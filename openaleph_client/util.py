import time
import random
import logging
import hashlib
import tempfile
from pathlib import Path
from typing import Dict
from banal import ensure_list

from openaleph_client.errors import AlephException

log = logging.getLogger(__name__)


def backoff(err, failures: int):
    """Implement a random, growing delay between external service retries."""
    sleep = (2 ** max(1, failures)) + random.random()
    log.warning("Error: %s, back-off: %.2fs", err, sleep)
    time.sleep(sleep)


def prop_push(properties: Dict, prop: str, value):
    values = ensure_list(properties.get(prop))
    values.extend(ensure_list(value))
    properties[prop] = values


def get_or_create_state_file_path(root_path: str, state_file: str | None) -> Path:
    """Get the path for the state file, falling back to temp dir if target is read-only."""
    if state_file:
        return Path(state_file)

    root_dir = Path(root_path).resolve()
    state_filename = ".openaleph_crawl_state.db"

    # Try to create state file in target directory first
    try:
        state_file_path = root_dir / state_filename
        # Test if we can write to the directory
        test_file = root_dir / ".openaleph_write_test"
        test_file.touch()
        test_file.unlink()
        return state_file_path
    except (PermissionError, OSError):
        # Fall back to temp directory with a unique name based on target path
        path_hash = hashlib.sha256(str(root_dir.resolve()).encode()).hexdigest()[:16]
        temp_dir = Path(tempfile.gettempdir())
        fallback_file_path = temp_dir / f"openaleph_crawl_state_{path_hash}.db"
        log.warning(f"Cannot write to target directory, using fallback state file: {fallback_file_path}")
        return fallback_file_path


def adjust_psycopg3_uri(database_uri: str) -> str:
        """Adjust PostgreSQL URI to use psycopg3 dialect if psycopg is available."""
        if database_uri.startswith(("postgresql://", "postgres://")):
            try:
                import psycopg  # noqa: F401
                log.info("Using psycopg3")

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
                log.info("Using psycopg2")
                pass
        return database_uri