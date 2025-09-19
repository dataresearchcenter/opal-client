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
- `--version`  Show the current version and exit

### `crawldir`

Recursively upload the contents of a folder to a collection, with optional pause/resume:

```bash
openaleph crawldir -f <foreign-id> [--resume] [--state-file PATH] [--parallel N] [--noindex] [--casefile] [-l LANG] <path>
```

- `-f, --foreign-id`     Foreign-ID of the target collection (required)
- `--resume`             Resume from an existing state database; omit to start fresh (this will delete the state file!)
- `--state-file PATH`    Path to state file (for resuming from custom locations)
- `-p, --parallel N`     Number of parallel upload threads (default: 1)
- `-i, --noindex`        Skip indexing on ingest
- `--casefile`           Treat files as case files
- `-l, --language LANG`  Language hints (ISO 639; repeatable)

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

When running **crawldir**, OpenAleph maintains a small SQLite database file to track upload progress:

### Default Behavior (Writable Directories)

For directories where you have write permissions, the state file is created in your crawl root:

```
<crawl-root>/.openaleph_crawl_state.db
```

### Read-Only Directory Support

When crawling **read-only directories** (e.g., mounted filesystems, archived data), OpenAleph automatically detects the lack of write permissions and creates the state file in your system's temporary directory with a unique name:

```
/tmp/openaleph_crawl_state_<hash>.db
```

The hash is based on the target directory path, ensuring multiple crawls of different read-only directories don't conflict.

### Key Features

- **Purpose**: track which files have already been successfully uploaded.
- **Resume support**:
  - Passing `--resume` skips any files recorded in this DB.
  - Omitting `--resume` deletes any existing state DB and starts fresh.
- **Custom state files**: Use `--state-file PATH` to specify a custom location for the state database.
- **Thread-safe**: uploads are recorded under a lock to support parallel threads.
- **Update datasets later**: The db file persists, allowing you to update your local repository at any time and only sync new files to OpenAleph.
- **Clear logging**: OpenAleph logs the exact state file location and provides resume commands for easy reference.

### Usage Examples

**Standard crawl (writable directory):**
```bash
openaleph crawldir -f my_collection /path/to/data
# State file: /path/to/data/.openaleph_crawl_state.db

# Resume later:
openaleph crawldir --resume -f my_collection /path/to/data
```

**Read-only directory crawl:**
```bash
openaleph crawldir -f my_collection /readonly/mount/data
# Output: Using state file: /tmp/openaleph_crawl_state_a1b2c3d4.db
# Output: To resume this crawl, use: --resume --state-file /tmp/openaleph_crawl_state_a1b2c3d4.db

# Resume later:
openaleph crawldir --resume --state-file /tmp/openaleph_crawl_state_a1b2c3d4.db -f my_collection /readonly/mount/data
```

**Custom state file location:**
```bash
openaleph crawldir --state-file ~/my_crawl_state.db -f my_collection /any/path
```

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

After a crawl completes, OpenAleph will print a summary to the console including:
- Number of files successfully uploaded
- Number of failed uploads
- State file location for future resume operations

### Failed Files Log

If any failures occurred, a file is written containing the relative paths of files that could not be uploaded:

**For writable directories:**
```
<crawl-root>/.openaleph-failed.txt
```

**For read-only directories:**
```
/tmp/openaleph_failed_<hash>.txt
```

The failed files list contains one relative path per line for each file that could not be uploaded permanently. You can inspect this file to retry or investigate failures.
