import os
import csv
import pwd
import logging
from datetime import datetime
from importlib.metadata import version, PackageNotFoundError

from openaleph_client.sql import batch_store, batch_sync
from openaleph_client.settings import FILE_BATCH_SIZE

log = logging.getLogger(__name__)


def _build_initial_file_obj(file_path, is_file):
    try:
        opal_agent_version = version("openaleph-client")
    except PackageNotFoundError:
        opal_agent_version = None   

    current_user = pwd.getpwuid(os.getuid()).pw_name

    return {
                "file_path": os.path.normpath(file_path.path),
                "is_file": True if is_file else False,
                "processed": False,
                "processed_at": None,
                "to_skip": False,
                "failed": False,
                "entity_id": None,
                "opal_agent": opal_agent_version,
                "user": current_user
            }


def _build_synced_file_obj(file_path, is_file, file_entity_id, processed_at):
    return {
        "file_path": file_path,
        "is_file": True if is_file.lower() == True else False,
        "entity_id": file_entity_id,
        "processed_at": datetime.strptime(processed_at, "%Y-%m-%d %H:%M:%S.%f"),
        "processed": True
    }


def _traverse_get_file_obj(path: str | os.DirEntry[str]):
    with os.scandir(path) as files_iterator:
        for file_path in files_iterator:
            try:
                if file_path.is_file(follow_symlinks=True):
                    yield _build_initial_file_obj(file_path, is_file=True)
                elif file_path.is_dir(follow_symlinks=True):
                    yield from _traverse_get_file_obj(file_path)
                    yield _build_initial_file_obj(file_path, is_file=False)
            except PermissionError:
                log.error(f"Permission denied: {os.path.normpath(file_path.path)}")
    

def build_inventory(path: str): 
    values = []
    file_count = 1 
    
    for file_obj in _traverse_get_file_obj(path):
        if file_count % FILE_BATCH_SIZE == 0:
            batch_store(values)
            log.info(f"Iterated through {file_count:,} files")
            values = []
        
        values.append(file_obj)
        file_count +=1

    if values:
        batch_store(values)
        log.info(f"Iterated through {file_count:,} files")

def sync(processed, ignore, allow):
    if not any([processed, ignore, allow]):
        log.error("Could not sync inventory. Add a file to sync against by suing either --processed, --ignore or --allow.")
        return

    if processed:
        _, file_extension = os.path.splitext(processed)
        if file_extension != ".csv":
            log.error("The file containing processed files must be CSV.")
            return

        values = []
        file_count = 1 

        with open(processed, "r") as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                if file_count % FILE_BATCH_SIZE == 0:
                    # batch_store(values)
                    log.info(f"Synced {file_count:,} files")
                    values = []
                values.append(_build_synced_file_obj(
                    row["file_path"],
                    row["is_file"],
                    row["entity_id"],
                    row["processed_at"]
                ))
                file_count +=1

            if values:
                batch_sync(values)
                log.info(f"Synced {file_count:,} files")


