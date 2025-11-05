import os
import pwd
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from importlib.metadata import version, PackageNotFoundError

from openaleph_client.util import get_or_create_state_file_path, adjust_psycopg3_uri
from openaleph_client.sql import get_db_conn, File

log = logging.getLogger(__name__)

FILE_COUNT = 1 

def log_progress():
    if FILE_COUNT % 1000 == 0:
        log.info(f"Processed {FILE_COUNT:,} files")

def build_inventory(path: str):
    database_uri = os.environ.get("OPAL_CLIENT_DB", "postgresql://opal:opal@localhost/opal")
    database_uri = adjust_psycopg3_uri(database_uri)

    conn = get_db_conn(database_uri)

    # this works for installed packages
    # fallback: parse pyproject.toml or add __version__ to __init_.py
    try:
        opal_agent_version = version("openaleph-client")
    except PackageNotFoundError:
        opal_agent_version = None   
    
    with Session(conn) as session:
        session.begin()

        def _traverse_and_store(path, session):
            with os.scandir(path) as files_iterator:
                for file_path in files_iterator:
                    current_user = pwd.getpwuid(os.getuid()).pw_name
                    global FILE_COUNT

                    if file_path.is_file(follow_symlinks=True):
                            # add a file
                            new_file_path = File(
                                file_path=os.path.normpath(file_path.path),
                                is_file=True,
                                processed=False,
                                processed_at=datetime.now(),
                                to_skip=False,
                                failed=False,
                                opal_agent=opal_agent_version,
                                user=current_user
                            )
                            try:
                                session.add(new_file_path)
                            except:
                                log.error(f"Could not commit: {new_file_path}")
                                session.rollback()
                            else:
                                session.commit()
                                log_progress()
                                FILE_COUNT += 1
                    elif file_path.is_dir(follow_symlinks=True):
                        with Session(conn) as session:
                            # add a dir; recursively call with path=dir
                            new_dir_path = File(
                                file_path=os.path.normpath(file_path.path),
                                is_file=False,
                                processed=False,
                                processed_at=datetime.now(),
                                to_skip=False,
                                failed=False,
                                opal_agent=opal_agent_version,
                                user=current_user
                            )
                            try:
                                session.add(new_dir_path)
                            except:
                                log.error(f"Could not commit: {new_dir_path}")
                                session.rollback()
                            else:
                                session.commit()
                                log_progress()
                                FILE_COUNT += 1
                        _traverse_and_store(file_path, session)
        
        _traverse_and_store(path, session)