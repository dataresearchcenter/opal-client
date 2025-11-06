import os

# OpenAleph client API settings
HOST = os.environ.get("MEMORIOUS_ALEPH_HOST")
HOST = os.environ.get("OPENALEPH_HOST", HOST)
HOST = os.environ.get("OPAL_HOST", HOST)
HOST = "localhost"

API_KEY = os.environ.get("MEMORIOUS_ALEPH_API_KEY")
API_KEY = os.environ.get("OPENALEPH_API_KEY", API_KEY)
API_KEY = os.environ.get("OPAL_API_KEY", API_KEY)

MAX_TRIES = int(os.environ.get("OPENALEPH_MAX_TRIES", 5))
MEMORIOUS_RATE_LIMIT = int(os.environ.get("OPENALEPH_MEMORIOUS_RATE_LIMIT", 120))

FILE_BATCH_SIZE = 1000