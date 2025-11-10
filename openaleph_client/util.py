import os
import pwd
import time
import random
import logging
from typing import Dict
from banal import ensure_list
from importlib.metadata import version, PackageNotFoundError

from openaleph_client.errors import AlephException

log = logging.getLogger(__name__)


def backoff(err, failures: int):
    """Implement a random, growing delay between external service retries."""
    sleep = (2 ** max(1, failures)) + random.random()
    log.warning(f"Error: {err}, back-off: {sleep:.2f}s")
    time.sleep(sleep)


def prop_push(properties: Dict, prop: str, value):
    values = ensure_list(properties.get(prop))
    values.extend(ensure_list(value))
    properties[prop] = values

def get_opal_agent_version():
    try:
        return version("openaleph-client")
    except PackageNotFoundError:
        return None

def get_current_user():
    return pwd.getpwuid(os.getuid()).pw_name