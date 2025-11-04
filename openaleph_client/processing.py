import os
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from importlib.metadata import version, PackageNotFoundError

from openaleph_client.util import get_or_create_state_file_path
from openaleph_client.sql import get_db_conn, File


def build_inventory(path: str, state_file: str | None):
    db_file_path = get_or_create_state_file_path(path, state_file)
    conn = get_db_conn(db_file_path)

    # this works for installed packages
    # fallback: parse pyproject.toml or add __version__ to __init_.py
    try:
        opal_agent_version = version("openaleph-client")
    except PackageNotFoundError:
        opal_agent_version = None

    def _traverse_and_store(path):
        with os.scandir(path) as files_iterator:
            for file_path in files_iterator:
                if file_path.is_file(follow_symlinks=True):
                    with Session(conn) as session:
                        # add a file
                        new_file_path = File(
                            file_path=os.path.normpath(file_path.path),
                            processed=False,
                            processed_at=datetime.now(),
                            to_skip=False,
                            failed=False,
                            opal_agent=opal_agent_version,
                        )
                        try:
                            session.add(new_file_path)
                            session.commit()
                        except IntegrityError:
                            session.rollback()
                elif file_path.is_dir(follow_symlinks=True):
                    with Session(conn) as session:
                        # add a dir; recursively call with path=dir
                        new_dir_path = File(
                            file_path=os.path.normpath(file_path.path),
                            processed=False,
                            processed_at=datetime.now(),
                            to_skip=False,
                            failed=False,
                            opal_agent=opal_agent_version,
                        )
                        try:
                            session.add(new_dir_path)
                            session.commit()
                        except IntegrityError:
                            session.rollback()
                    _traverse_and_store(file_path)
    
    _traverse_and_store(path)