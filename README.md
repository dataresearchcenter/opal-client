# OpenAleph

Python client for the OpenAleph data API.

## Installation

```bash
pip install openaleph-client
```

## Command-Line Interface

_All commands share the same global options:_

```bash
openaleph --host URL --api-key KEY [--retries N] <command> [options]
```

- `--host`     OpenAleph API host URL (default from `OPAL_HOST` env var)
- `--api-key`  API key for authentication (default from `OPAL_API_KEY` env var)
- `--retries`  Number of retry attempts on server failure (default: 5)

### `crawldir`

Recursively upload the contents of a folder to a collection, with optional pause/resume:

```bash
openaleph crawldir -f <foreign-id> [--resume] [--parallel N] [--noindex] [--casefile] [-l LANG] <path>
```

- `-f, --foreign-id`    Foreign-ID of the target collection (required)
- `--resume`            Resume from an existing state database; omit to start fresh (this will delete the state file!)
- `-p, --parallel N`    Number of parallel upload threads (default: 1)
- `-i, --noindex`       Skip indexing on ingest
- `--casefile`          Treat files as case files
- `-l, --language LANG` Language hints (ISO 639; repeatable)

### `fetchdir`

Download all entities in a collection (or a single entity) into a folder tree:

```bash
openaleph fetchdir -f <foreign-id> [-e <entity-id>] [-p <path>] [--overwrite]
```

### Other commands

- `reingest`         Re-ingest all documents in a collection
- `reindex`          Re-index all entities in a collection
- `delete`           Delete a collection and its contents
- `flush`            Delete all contents of a collection
- `write-entity`     Index a single entity from stdin
- `write-entities`   Bulk-index entities from stdin
- `stream-entities`  Stream entities to stdout
- `entitysets`       List entity sets
- `entitysetitems`   List items in an entity set
- `make-list`        Create a new list entity set

---

## State Persistence

When running **crawldir**, OpenAleph maintains a small SQLite database file in your crawl root:

```
<crawl-root>/.openaleph_crawl_state.db
```

- **Purpose**: track which files have already been successfully uploaded.
- **Resume support**:
  - Passing `--resume` skips any files recorded in this DB.
  - Omitting `--resume` deletes any existing state DB and starts fresh.
- **Thread-safe**: uploads are recorded under a lock to support parallel threads.
- **Update datasets later**: The db file stays in the directory, allowing you to update your local repository at any time and only sync the new files to OpenAleph.

---

## Ignore File

You can create a file named:

```
<crawl-root>/.openalephignore
```

and list glob patterns for any files or directories you want to skip entirely:

```text
# Skip hidden files
.*

# Common junk
.DS_Store
Thumbs.db

# Temporary directories
tmp/
build/

# Log files
*.log
```

- Patterns are matched against the **relative path** of each file or folder.
- A pattern ending in `/` only matches directories (and their contents).
- Blank lines and lines beginning with `#` are ignored.
- Anything matched here is never enqueued or uploaded.
- the `.openalephignore` file itself is ignored by default, and so is the state file

## Final Report

After a crawl completes, OpenAleph will print a summary to the console. If any failures occurred, by default a file is written to:

`<crawl-root>/.openaleph-failed.txt`

It contains one relative path per line for each file that could not be uploaded permanently. You can inspect this file to retry or investigate failures.
