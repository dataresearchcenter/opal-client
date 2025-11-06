import os
import pwd
import logging
from datetime import datetime
from pathlib import Path
from importlib.metadata import version, PackageNotFoundError

from openaleph_client.sql import batch_store
from openaleph_client.settings import FILE_BATCH_SIZE

log = logging.getLogger(__name__)


def _build_file_obj(file_path, is_file):
    # this works for installed packages
    # fallback: parse pyproject.toml or add __version__ to __init_.py
    try:
        opal_agent_version = version("openaleph-client")
    except PackageNotFoundError:
        opal_agent_version = None   

    current_user = pwd.getpwuid(os.getuid()).pw_name

    return {
                "file_path": os.path.normpath(file_path.path),
                "is_file": True if is_file else False,
                "processed": False,
                "processed_at": datetime.now(),
                "to_skip": False,
                "failed": False,
                "opal_agent": opal_agent_version,
                "user": current_user
            }


def _traverse_get_file_obj(path: str | os.DirEntry[str]):
    with os.scandir(path) as files_iterator:
        for file_path in files_iterator:
            try:
                if file_path.is_file(follow_symlinks=True):
                    yield _build_file_obj(file_path, is_file=True)
                elif file_path.is_dir(follow_symlinks=True):
                    yield from _traverse_get_file_obj(file_path)
                    yield _build_file_obj(file_path, is_file=False)
            except PermissionError:
                log.error(f"Permission denied: {os.path.normpath(file_path.path)}")


def build_inventory(path: str): 
    values = []
    FILE_COUNT = 1 
    
    for file_obj in _traverse_get_file_obj(path):
        if FILE_COUNT % FILE_BATCH_SIZE == 0:
            batch_store(values)
            log.info(f"Iterated through {FILE_COUNT:,} files")
            values = []
        
        values.append(file_obj)
        FILE_COUNT +=1

    if values:
        batch_store(values)
        log.info(f"Iterated through {FILE_COUNT:,} files")