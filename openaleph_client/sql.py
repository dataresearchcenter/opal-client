import os
import logging
from sqlalchemy import create_engine
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
    to_skip = Column(Boolean, default=False)
    failed = Column(Boolean, default=False)
    opal_agent = Column(String, nullable=True)
    entity_id = Column(String, default=False)
    user = Column(String, default=False)

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
