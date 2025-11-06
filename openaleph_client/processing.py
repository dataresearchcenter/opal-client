import os
import pwd
import logging
from datetime import datetime
from pathlib import Path
from importlib.metadata import version, PackageNotFoundError

from openaleph_client.sql import batch_store
from openaleph_client.settings import FILE_BATCH_SIZE

log = logging.getLogger(__name__)
FILE_COUNT = 1 


def build_inventory(path: str):
    # this works for installed packages
    # fallback: parse pyproject.toml or add __version__ to __init_.py
    try:
        opal_agent_version = version("openaleph-client")
    except PackageNotFoundError:
        opal_agent_version = None   
    
    values = []
    current_user = pwd.getpwuid(os.getuid()).pw_name

    def _traverse_and_store(path, values):
        with os.scandir(path) as files_iterator:
            for file_path in files_iterator:
                global FILE_COUNT
                if FILE_COUNT % FILE_BATCH_SIZE == 0:
                    batch_store(values)
                    log.info(f"Iterated through {FILE_COUNT:,} files")
                    values = []

                if file_path.is_file(follow_symlinks=True):
                        values.append({
                            "file_path": os.path.normpath(file_path.path),
                            "is_file": True,
                            "processed": False,
                            "processed_at": datetime.now(),
                            "to_skip": False,
                            "failed": False,
                            "opal_agent": opal_agent_version,
                            "user": current_user
                        })
                        FILE_COUNT += 1
                elif file_path.is_dir(follow_symlinks=True):
                    values.append({
                        "file_path": os.path.normpath(file_path.path),
                        "is_file": False,
                        "processed": False,
                        "processed_at": datetime.now(),
                        "to_skip": False,
                        "failed": False,
                        "opal_agent": opal_agent_version,
                        "user": current_user
                    })
                    FILE_COUNT += 1
                    _traverse_and_store(file_path, values)
    
    _traverse_and_store(path, values)
    
    if values:
        batch_store(values)
        log.info(f"Iterated through {FILE_COUNT:,} files")