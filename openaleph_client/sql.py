import logging
from datetime import datetime
from functools import lru_cache
from sqlalchemy import create_engine, update, case, select, func
from sqlalchemy.engine import Engine
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.exc import (
    DatabaseError,
    DisconnectionError,
    OperationalError,
    ResourceClosedError,
    TimeoutError,
)
from sqlalchemy.dialects.postgresql import insert

from openaleph_client.api import ingest_upload
from openaleph_client.crawldir import ingest_upload
from openaleph_client.util import get_opal_agent_version, get_current_user
from openaleph_client.settings import FILE_BATCH_SIZE, INVENTORY_TABLE_NAME, DATABASE_URI

log = logging.getLogger(__name__)


EXCEPTIONS = (
    DatabaseError,
    DisconnectionError,
    OperationalError,
    ResourceClosedError,
    TimeoutError,
)
try:
    from psycopg import DatabaseError, OperationalError

    EXCEPTIONS = (DatabaseError, OperationalError, *EXCEPTIONS)
except ImportError:
    try:
        from psycopg2 import DatabaseError, OperationalError

        EXCEPTIONS = (DatabaseError, OperationalError, *EXCEPTIONS)
    except ImportError:
        pass


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = INVENTORY_TABLE_NAME

    id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    file_path = Column(String, nullable=False, unique=True)
    is_file = Column(Boolean, default=False)
    processed = Column(Boolean, default=False)
    processed_at = Column(DateTime, nullable=True)
    is_processing = Column(Boolean, default=False)
    to_skip = Column(Boolean, default=False)
    failed = Column(Boolean, default=False)
    opal_agent = Column(String, nullable=True)
    entity_id = Column(String, nullable=True)
    user = Column(String, nullable=True)

    def __repr__(self):
        return (
            f"<Files(id={self.id}, file_path='{self.file_path}', is_file={self.is_file}, "
            f"processed={self.processed}, processed_at={self.processed_at},"
            f"to_skip={self.to_skip}, failed={self.failed},"
            f"opal_agent='{self.opal_agent}', entity_id={self.entity_id},"
            f"user={self.user}>"
        )


def adjust_psycopg3_uri(database_uri: str) -> str:
        """Adjust PostgreSQL URI to use psycopg3 dialect if psycopg is available."""
        if database_uri.startswith(("postgresql://", "postgres://")):
            try:
                import psycopg  # noqa: F401

                # Use psycopg3 dialect for better performance and compatibility
                if database_uri.startswith("postgresql://"):
                    return database_uri.replace(
                        "postgresql://", "postgresql+psycopg://", 1
                    )
                elif database_uri.startswith("postgres://"):
                    return database_uri.replace(
                        "postgres://", "postgresql+psycopg://", 1
                    )
            except ImportError:
                # Fall back to psycopg2 if psycopg3 is not available
                pass
        return database_uri


def get_db_conn() -> Engine:
    database_uri = adjust_psycopg3_uri(DATABASE_URI)

    config = {}
    config.setdefault("pool_size", 1)
    if database_uri.startswith("postgresql+psycopg://"):
            config.setdefault("max_overflow", 5)
            config.setdefault("pool_timeout", 60)
            config.setdefault("pool_recycle", 3600)
            config.setdefault("pool_pre_ping", True) 
    engine = create_engine(f"{database_uri}", **config)
    Base.metadata.create_all(engine)

    return engine


def batch_store(values):
    engine = get_db_conn()
    conn = engine.connect()
    tx = conn.begin()
    try:
        istmt = insert(File).values(values)
        stmt = istmt.on_conflict_do_update(
            constraint=f"{INVENTORY_TABLE_NAME}_file_path_key",
            set_=dict(
                # file_path=istmt.excluded.file_path,
                # is_file=istmt.excluded.is_file,
                # processed=istmt.excluded.processed,
                # processed_at=istmt.excluded.processed_at,
                # to_skip=istmt.excluded.to_skip,
                # failed=istmt.excluded.failed,
                # entity_id=istmt.excluded.entity_id,
                opal_agent=istmt.excluded.opal_agent,
                user=istmt.excluded.user,
            ),
        )
        conn.execute(stmt)
        tx.commit()
        log.info(f"Processed a batch of files (max size: {FILE_BATCH_SIZE:,})")
    except EXCEPTIONS:
        tx.rollback()
        log.exception("Database error storing file paths")
        exit()
    finally:
        conn.close()


def batch_sync(values):
    engine = get_db_conn()
    conn = engine.connect()
    tx = conn.begin()
    try:
        file_paths = [value["file_path"] for value in values]
        is_file_case = case(
            *[(File.file_path == row["file_path"], row["is_file"]) for row in values],
            else_=File.is_file,
        )
        entity_id_case = case(
            *[(File.file_path == row["file_path"], row["entity_id"]) for row in values],
            else_=File.entity_id,
        )
        processed_at_case = case(
            *[(File.file_path == row["file_path"], row["processed_at"]) for row in values],
            else_=File.processed_at,
        )
        processed_case = case(
            *[(File.file_path == row["file_path"], True) for row in values],
            else_=File.processed,
        )

        stmt = (
            update(File)
            .where(File.file_path.in_(file_paths))
            .values(
                is_file=is_file_case,
                entity_id=entity_id_case,
                processed_at=processed_at_case,
                processed=processed_case,
            )
        )
        result = conn.execute(stmt)
        tx.commit()
        log.info(f"Processed a batch of files (max size: {FILE_BATCH_SIZE:,})")

        existing_paths = {
        row[0]
        for row in conn.execute(
            select(File.file_path).where(File.file_path.in_(file_paths))
        )
        }
        missing = set(file_paths) - existing_paths
        log.info(f"Updated {result.rowcount} rows.")
        for m in missing:
            log.error(f"No entry found with file_path='{m}' — skipping.")
    except EXCEPTIONS:
        tx.rollback()
        log.exception("Database error updating file paths")
    finally:
        conn.close()


def get_batch(batch_size):
    engine = get_db_conn()
    conn = engine.connect()
    tx = conn.begin()

    files_batch = {}
    try:
        stmt = (
            select(File)
            .where(File.processed.is_(False), File.to_skip.is_(False), File.is_processing.is_(False))
            .limit(batch_size)
        )

        result = conn.execute(stmt)

        if result:
            files_batch = [{"file_path": file.file_path, "is_file": file.is_file} for file in result]
            log.info(f"Retrieved {len(files_batch)} rows from the inventory. Marking...")

            file_paths = [file_obj["file_path"] for file_obj in files_batch]
            log.info(file_paths)
            is_processing_case = case(
                *[(File.file_path == row["file_path"], row["is_processing"]) for row in file_paths],
                else_=File.is_processing,
            )
            stmt = (
                update(File)
                .where(File.file_path.in_(file_paths))
                .values(
                    is_processing=is_processing_case,
                )
            )
            update_result = conn.execute(stmt)
            if update_result:
                log.info(f"Marked {update_result.rowcount} files as 'processing'.")
            else:
                log.info(f"{len(file_paths)} may have not been marked as 'processing' and may be processed twice.")
        tx.commit()
    except EXCEPTIONS:
        tx.rollback()
        log.exception("Database error getting file paths")
    finally:
        conn.close()
        return files_batch


def mark_processed(file_path, entity_id):
    engine = get_db_conn()
    conn = engine.connect()
    tx = conn.begin()

    opal_agent_version = get_opal_agent_version()
    current_user = get_current_user()
    try:
        stmt = (
            update(File)
            .where(File.file_path == file_path)
            .values(
                entity_id=entity_id,
                processed=True,
                processed_at=datetime.now(),
                # updating this in case it has previously failed
                failed=True,
                opal_agent=opal_agent_version,
                user=current_user
            )
        )
        result = conn.execute(stmt)
        tx.commit()
        if result:
            log.info(f"Marked as processed: {file_path}.")
    except EXCEPTIONS:
        tx.rollback()
        log.exception(f"Database error marking as processed: {file_path}")
    finally:
        conn.close()


def mark_failed(file_path):
    engine = get_db_conn()
    conn = engine.connect()
    tx = conn.begin()

    opal_agent_version = get_opal_agent_version()
    current_user = get_current_user()
    try:
        stmt = (
            update(File)
            .where(File.file_path == file_path)
            .values(
                processed=False,
                processed_at=datetime.now(),
                failed=True,
                opal_agent=opal_agent_version,
                user=current_user
            )
        )
        result = conn.execute(stmt)
        tx.commit()
        if result:
            log.info(f"Marked as failed: {file_path}.")
    except EXCEPTIONS:
        tx.rollback()
        log.exception(f"Database error marking as failed: {file_path}")
    finally:
        conn.close()


@lru_cache
def get_or_create_directory_id(file_path, upload_path, collection_id, index):
    engine = get_db_conn()
    conn = engine.connect()
    tx = conn.begin()

    directory_entity_id = None
    try:
        stmt = (
            select(File.entity_id)
            .where(File.file_path == file_path)
            .limit(1)
        )

        result = conn.execute(stmt).scalar_one_or_none()

        if result is None or result == "" or result is False or result == 'false':
            # get the parent_id
            # recursively create the parent dirs, if needed
            if "/" not in file_path:
                # foreign_id = path relative to root dir
                foreign_id = file_path
                # the directory is in the root_dir, parents_id = None
                parent_id = None
                try:
                    entity_id = ingest_upload(upload_path, parent_id, foreign_id, collection_id, index)
                    if entity_id:
                        mark_processed(file_path, entity_id)
                except Exception as e:
                    log.error(f"[Collection {collection_id}] Failed to upload {upload_path}. Error: {e}")
                    mark_failed(file_path)
            else:
                # recursively call function with the path to the parent dir
                parent_dir_path = "/".join(file_path.split("/")[:-1])
                parent_dir_upload_path = "/".join(file_path.split("/")[:-1])
                directory_entity_id = get_or_create_directory_id(parent_dir_path, parent_dir_upload_path, collection_id, index)
        else:
            directory_entity_id = result
        tx.commit()
    except EXCEPTIONS:
        tx.rollback()
        log.exception(f"Database error getting or creating parent dir(s): {file_path}")
        mark_failed(file_path)
    finally:
        conn.close()
        return directory_entity_id


def count_not_processed():
    engine = get_db_conn()
    conn = engine.connect()
    tx = conn.begin()
    try:
        stmt = (
            select(func.count())
            .select_from(File.__table__)
            .where(File.processed.is_(False), File.to_skip.is_(False))
        )

        result = conn.execute(stmt)

        if result:
            log.info(f"{result} files left to process")
            return result
        
        tx.commit()
    except EXCEPTIONS:
        tx.rollback()
        log.exception("Database error getting the number of unprocessed files")
    finally:
        conn.close()