# openaleph

Command-line client for OpenAleph. It can be used to bulk import document sets via
the API, without direct access to the server. It requires an active API client
to perform uploads.

## Installation

Install using `pip`.

```bash
pip install openaleph
```

## Usage

Refer to the [OpenAleph documentation](https://openaleph.org/docs/) for an introduction on how to use `openaleph`, e.g. to crawl a local file directory, or to stream entities.


## Command-Line Interface

The `openaleph` CLI provides a suite of commands to ingest, fetch, and manage collections and entities in your OpenAleph instance. Configure global options once per invocation, then choose one of the subcommands below.

### Global Options
```
openaleph [OPTIONS] <COMMAND> [ARGS]...
```
- `--host TEXT`    : OpenAleph API host URL (default from `OPENALEPH_HOST` setting)
- `--api-key TEXT` : API key for authentication (default from `OPENALEPH_API_KEY`)
- `-r, --retries N`: Number of retry attempts on server failure
- `-h, --help`     : Show help message and exit

---

### `openaleph crawldir`
Recursively crawl a local directory, upload files to a collection, and optionally pause/resume progress.

**Usage**
```bash
openaleph crawldir -f <foreign-id> [OPTIONS] PATH
```
**Required**
- `PATH` : Directory to ingest
- `-f, --foreign-id TEXT` : Target collection's `foreign_id`

**Options**
- `--resume`         : Resume from an existing state database in `PATH`
- `-p, --parallel N` : Number of parallel upload threads (default: 1)
- `--nojunk`         : Skip common junk files (hidden/system files)
- `--noindex`        : Ingest without running post-upload indexing
- `--casefile`       : Treat uploads as case-file records
- `-l, --language`   : ISO 639 language hint (can repeat)

---

### `openaleph fetchdir`
Download the contents of a collection or entity from OpenAleph into a local folder structure.

**Usage**
```bash
openaleph fetchdir [OPTIONS]
```
**Options** (one of)
- `-f, --foreign-id TEXT` : Download entire collection by foreign ID
- `-e, --entity-id TEXT`  : Download single entity by its ID
- `-p, --prefix PATH`     : Destination root path (default: current directory)
- `--overwrite`           : Overwrite existing files

---

### `openaleph reingest`
Re-trigger ingestion of all documents in a collection.

**Usage**
```bash
openaleph reingest -f <foreign-id> [--index]
```
- `--index` : Run indexing on each document after re-ingest

---

### `openaleph reindex`
Re-run the indexing process for all entities in a collection.

**Usage**
```bash
openaleph reindex -f <foreign-id> [--flush]
```
- `--flush` : Delete existing indexed entities before reindexing

---

### `openaleph delete`
Delete an entire collection and its contents.

**Usage**
```bash
openaleph delete -f <foreign-id> [--sync]
```
- `--sync` : Wait for deletion to complete before exiting

---

### `openaleph flush`
Remove all entities from a collection without deleting the collection itself.

**Usage**
```bash
openaleph flush -f <foreign-id> [--sync]
```
- `--sync` : Wait for flush to complete

---

### `openaleph write-entity`
Index a single entity from JSON input.

**Usage**
```bash
openaleph write-entity -f <foreign-id> [OPTIONS]
```
- `-i, --infile` : File or `-` for STDIN (default)

---

### `openaleph delete-entity`
Delete a single entity by ID.

**Usage**
```bash
openaleph delete-entity <entity-id>
```

---

### `openaleph write-entities`
Bulk index entities from a JSON‑stream input.

**Usage**
```bash
openaleph write-entities -f <foreign-id> [OPTIONS]
```
**Options**
- `-i, --infile`       : File or `-` for STDIN
- `-e, --entityset ID` : Add to an existing entity set
- `-c, --chunksize N`  : Number of entities per API call (default: 1000)
- `--force`            : Continue on server errors
- `--unsafe`           : Allow references to archive hashes
- `--cleaned`          : Disable server-side type validation

---

### `openaleph stream-entities`
Stream entities from the server to JSON lines on STDOUT.

**Usage**
```bash
openaleph stream-entities -f <foreign-id> [OPTIONS]
```
**Options**
- `-o, --outfile FILE`    : Output file or `-` for STDOUT
- `-s, --schema SCHEMA`   : Filter by schema (can repeat)
- `-p, --publisher`       : Include publisher metadata

---

### `openaleph entitysets`
List or stream entity sets for a collection.

**Usage**
```bash
openaleph entitysets [-f <foreign-id>] [-t <type>]
```
- `-f, --foreign-id` : Only sets in this collection
- `-t, --type`       : Filter by set type

---

### `openaleph entitysetitems`
List items within a named entity set.

**Usage**
```bash
openaleph entitysetitems <entityset-id>
```

---

### `openaleph make-list`
Create a new entity set of type `list` with a label and optional summary.

**Usage**
```bash
openaleph make-list -f <foreign-id> <label> [--summary TEXT]
```

---
For more details on any command:
```bash
openaleph <command> --help
```


