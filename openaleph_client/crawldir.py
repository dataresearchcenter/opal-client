import logging
import threading
from fnmatch import fnmatch
import sqlite3
import signal
import sys
import os
import hashlib
from queue import Queue
from pathlib import Path
from typing import cast, Optional, Dict, List

from openaleph_client.api import AlephAPI
from openaleph_client.errors import AlephException
from openaleph_client.util import get_or_create_state_file_path, backoff
from openaleph_client.sql import get_db_conn

log = logging.getLogger(__name__)


def get_failed_file_path(target_dir: Path, state_file: Path) -> Path:
    """Get the path for the failed files list, using same logic as state file."""
    failed_filename = ".openaleph-failed.txt"

    # If state file is in target directory, put failed file there too
    if state_file.parent == target_dir:
        return target_dir / failed_filename
    else:
        # Use same directory as state file (temp dir)
        path_hash = hashlib.sha256(str(target_dir.resolve()).encode()).hexdigest()[:16]
        return state_file.parent / f"openaleph_failed_{path_hash}.txt"


class CrawlDirectory(object):
    def __init__(
        self,
        api: AlephAPI,
        collection: Dict,
        path: Path,
        index: bool = True,
    ):
        self.api = api
        self.index = index
        self.collection = collection
        self.collection_id = cast(str, collection.get("id"))
        self.root = path
        self._db_conn: sqlite3.Connection = None
        self._db_lock = threading.Lock()
        self.queue: Queue = Queue()
        self.scan_queue: Queue = Queue()
        self.ignore_patterns: List[str] = []

    def is_ignored(self, path: Path) -> bool:
        rel = str(path.relative_to(self.root))
        for pat in self.ignore_patterns:
            p = pat if isinstance(pat, str) else str(pat)
            if p.endswith("/") and path.is_dir():
                prefix = p.rstrip("/")
                if rel == prefix or rel.startswith(prefix + "/"):
                    return True
            elif fnmatch(rel, p):
                return True
        return False

    def crawl(self):
        while not self.scan_queue.empty():
            path, parent_id = self.scan_queue.get()
            id = None
            foreign_id = self.get_foreign_id(Path(path))
            if foreign_id is not None:
                id = self.backoff_ingest_upload(path, parent_id, foreign_id)
            self.scandir(path, id, parent_id)
            self.scan_queue.task_done()

    def consume(self):
        """Worker thread: upload files, skipping those already processed."""
        while True:
            path, parent_id = self.queue.get()
            # Poison‐pill sentinel
            if path is None:
                self.queue.task_done()
                break

            rel = str(Path(path).relative_to(self.root))
            with self._db_lock:
                with self._db_conn:
                    cur = self._db_conn.execute("SELECT 1 FROM processed WHERE path = ?", (rel,))
                    if cur.fetchone():
                        self.queue.task_done()
                        log.info("Skipping [%s->%s]: %s", self.collection_id, parent_id, rel)
                        continue  # if in db skip

            log.info("Upload [%s->%s]: %s", self.collection_id, parent_id, rel)
            result = self.backoff_ingest_upload(path, parent_id, self.get_foreign_id(Path(path)))
            with self._db_lock:
                with self._db_conn:
                    if result:
                        self._db_conn.execute("INSERT OR IGNORE INTO processed(path) VALUES(?)", (rel,))
                    else:
                        self._db_conn.execute("INSERT OR IGNORE INTO failed(path) VALUES(?)", (rel,))
            self.queue.task_done()

    def scandir(self, path: Path, id: Optional[str], parent_id: str):
        """
        Walk `path`, send directories to scan_queue
        and files to queue, skipping .openalephignore entries
        """
        with os.scandir(path) as it:
            for entry in it:
                child_path = Path(entry.path)
                if self.is_ignored(child_path):
                    continue
                if entry.is_dir():
                    self.scan_queue.put((child_path, id))
                else:
                    self.queue.put((child_path, id))

    def get_foreign_id(self, path: Path) -> Optional[str]:
        if path == self.root:
            if path.is_dir():
                return None
            return path.name

        # path.is_relative_to is still a bit new, so opting for something... older
        try:
            return str(path.relative_to(self.root))
        except ValueError:
            return None

    def backoff_ingest_upload(self, path: Path, parent_id: str, foreign_id: str) -> Optional[str]:
        try_number = 1
        while True:
            try:
                return self.ingest_upload(Path(path), parent_id, foreign_id)
            except AlephException as err:
                if err.transient and try_number < self.api.retries:
                    try_number += 1
                    backoff(err, try_number)
                else:
                    log.error(err.message)
                    return None
            except Exception:
                log.exception("Failed [%s]: %s", self.collection_id, path)
                return None

    def ingest_upload(self, path: Path, parent_id: str, foreign_id: str) -> str:
        metadata = {
            "foreign_id": foreign_id,
            "file_name": path.name,
        }
        if parent_id is not None:
            metadata["parent_id"] = parent_id
        result = self.api.ingest_upload(
            self.collection_id,
            path,
            metadata=metadata,
            index=self.index,
        )
        if "id" not in result and not hasattr(result, "id"):
            raise AlephException("Upload failed")
        return result["id"]


def crawl_dir(
    api: AlephAPI,
    path: str,
    foreign_id: str,
    config: Dict,
    index: bool = True,
    parallel: int = 1,
    resume: bool = False,
    state_file: Optional[str] = None
):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use.
    language: language hint for the documents
    """
    # shut down gracefully on sigint
    def _save_and_exit(signum, frame):
        with crawler._db_lock:
            crawler._db_conn.commit()
        log.info(f"\nState saved to {db_file_path}. Exiting.")
        sys.exit(1)
    signal.signal(signal.SIGINT, _save_and_exit)

    # SQLite DB to store crawl state, with fallback for read-only directories
    # if state_file:
    #     db_file = Path(state_file)
    # else:
    #     db_file = get_state_file_path(root)
    db_file_path = get_or_create_state_file_path(path, state_file)

    if not resume and db_file_path.exists():
        os.remove(db_file_path)
    
    conn = get_db_conn(db_file_path)
    with conn:
        conn.execute("CREATE TABLE IF NOT EXISTS processed (path TEXT PRIMARY KEY)")
        conn.execute("CREATE TABLE IF NOT EXISTS failed (path TEXT PRIMARY KEY)")

    # Log the state file location for user reference
    log.info(f"Using state file: {db_file_path}")
    if resume:
        log.info("Resuming crawl from existing state")
    else:
        log.info("Starting new crawl")

    collection = api.load_collection_by_foreign_id(foreign_id, config)

    root = Path(path).resolve()

    crawler = CrawlDirectory(api, collection, root, index=index)
    crawler._db_conn = conn

    # Read dot ignore file
    ignore_file = root / ".openalephignore"
    patterns = [".openaleph_crawl_state.db", ".openalephignore", ".openaleph-failed.txt"]
    if ignore_file.exists():
        for line in ignore_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            patterns.append(line)
    crawler.ignore_patterns = patterns
    crawler.scan_queue.put((root, None))
    consumers = []

    # Use one thread to produce using scandir and at least one to consume
    # files for upload.
    producer = threading.Thread(target=crawler.crawl, daemon=True)
    producer.start()
    for i in range(max(1, parallel)):
        consumer = threading.Thread(target=crawler.consume, daemon=True)
        consumers.append(consumer)
        consumer.start()

    # Block until the producer is done with queueing the tree.
    producer.join()

    # Block until the file upload queue is drained.
    crawler.queue.join()

    # Poison the queue to signal end to each consumer.
    for consumer in consumers:
        crawler.queue.put((None, None))

    # Block until all file upload queue consumers are done.
    for consumer in consumers:
        consumer.join()

    # final report
    with conn:
        cur = conn.execute("SELECT COUNT(*) FROM processed")
        total_ok = cur.fetchone()[0]
        cur = conn.execute("SELECT COUNT(*) FROM failed")
        total_fail = cur.fetchone()[0]

    log.info(f"Crawldir complete.")
    log.info(f"Uploaded (including prev. sessions if resumed): {total_ok}")
    log.info(f"Failed: {total_fail}")
    log.info(f"State file location: {db_file_path}")
    log.info("To resume this crawl, use: --resume --state-file " + str(db_file_path))

    # If any failures, write them to failed files list
    if total_fail:
        failed_file = get_failed_file_path(root, db_file_path)
        with open(failed_file, "w", encoding="utf-8") as fp:
            for row in conn.execute("SELECT path FROM failed ORDER BY path"):
                fp.write(f"{row[0]}\n")
        log.info(f"List of failed files written to {failed_file}")
