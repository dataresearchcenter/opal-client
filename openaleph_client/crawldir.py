import logging
import threading
import os
import hashlib
from pathlib import Path
from typing import cast, Optional, Dict

from openaleph_client.api import AlephAPI, ingest_upload
from openaleph_client.settings import FILE_BATCH_SIZE
from openaleph_client.sql import get_batch, get_or_create_directory_id, mark_failed, mark_processed, count_not_processed

log = logging.getLogger(__name__)


def consume(root_path, values, collection_id, index):
    """Worker thread: upload files, skipping those already processed."""    
    uploaded = 0
    failed = 0
    
    for file_path in values:
        parent_id = None
        # build upload path relative to the root dir 
        upload_path = root_path / Path(file_path)
        # foreign_id = path relative to root dir
        foreign_id = file_path
        # parent_id = the FTM entity_id of the parent dir
        # or None for the files in the root dir
        if "/" not in foreign_id:
            parent_id = None
        else:
            # all files that aren't in the root dir
            # need to have a parent ID
            # if none could be retrieved, mark as failed
            parent_path = "/".join(file_path.split("/")[:-1])
            parent_id = get_or_create_directory_id(parent_path, upload_path, collection_id, index)
            if not parent_id:
                log.info(f"[Collection {collection_id}] Could not upload {upload_path}. Failed to create parent dir.")
                mark_failed(file_path)
                failed += 1
                continue
        # upload to OpenAleph
        try:
            entity_id = ingest_upload(upload_path, parent_id, foreign_id, collection_id, index)
            if entity_id:
                mark_processed(file_path, entity_id)
                uploaded += 1
        except Exception as e:
            log.error(f"Failed to upload {upload_path}. Error: {e}")
            mark_failed(file_path)
            failed += 1

    log.info(f"Processed a batch of {FILE_BATCH_SIZE}. Uploaded: {uploaded}. Failed: {failed}")
    return (uploaded, failed)


def crawl_dir(
    api: AlephAPI,
    path: str,
    foreign_id: str,
    config: Dict,
    index: bool = True,
    parallel: int = 1,
):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use.
    language: language hint for the documents
    """
    log.info("Starting new crawl")

    collection = api.load_collection_by_foreign_id(foreign_id, config)
    collection_id = cast(str, collection.get("id"))

    while True:
        not_processed = count_not_processed()
        
        # all the files have been processed
        if not not_processed: 
            log.info("All files have been processed. Crawldir complete.")
            break
        
        values = get_batch(FILE_BATCH_SIZE * parallel)
        
        if not values:
            log.error("Error getting files from inventory")
            exit()
        
        consumers = []
        for idx in range(max(1, parallel)):
            values_batch = values[idx*FILE_BATCH_SIZE: idx*FILE_BATCH_SIZE+FILE_BATCH_SIZE]
            consumer = threading.Thread(target=consume, args=(path, values_batch, collection_id, index), daemon=True)
            consumers.append(consumer)
            consumer.start()

        # Block until all file upload queue consumers are done.
        for consumer in consumers:
            consumer.join()

    log.info(f"Crawldir complete.")
    
