# AGENTS.md

This file provides guidance to Qoder (qoder.com) when working with code in this repository.

## Build & Run

```bash
# Build binary
go build -o mysql-pitr .

# Run directly without building
go run . <command> [flags]

# Build and verify all packages compile
go build ./...

# Tidy dependencies
go mod tidy
```

There are no tests yet. If adding tests, run them with:
```bash
go test ./...
# Single package
go test ./internal/backup/...
```

## Architecture

The tool implements MySQL PITR (Point-In-Time Recovery) as a CLI with four subcommands. The data flow is:

```
backup  → xtrabackup → tar.gz → S3 (s3prefix/<backup-id>/backup.tar.gz)
                               S3 (s3prefix/<backup-id>/backup.meta.json)

binlog  → mysqlbinlog --read-from-remote-server → S3 (s3prefix/binlogs/<binlog-file>)

restore → S3 backup.tar.gz → extract → xtrabackup --prepare → copy-back → MySQL start
        → S3 binlogs → mysqlbinlog stdin | mysql stdin  (piped, no local temp files)

list    → scan S3 prefix for backup.meta.json files → display table or JSON
```

### Package responsibilities

- **`cmd/root.go`** — All CLI flag definitions and cobra command wiring. Global S3 flags (`--s3-*`) are parsed here and passed to `s3store.New()`. Each subcommand constructs its internal runner and calls `.Run(ctx)`.

- **`internal/s3store`** — Thin wrapper around `aws-sdk-go-v2/service/s3`. Supports custom endpoints (MinIO/Ceph) via `Config.Endpoint` and path-style addressing. Credential resolution order: explicit flags → `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env → SDK default chain.

- **`internal/backup`** — Calls `xtrabackup --backup`, streams the output directory through `tar.gz` via an `io.Pipe` directly into S3 (no double-buffering to disk). After upload writes `backup.meta.json` with binlog file/position and GTID set extracted from `xtrabackup_binlog_info`.

- **`internal/binlog`** — Runs `mysqlbinlog --read-from-remote-server --raw --stop-never` writing raw binlog files to a temp directory. A goroutine polls every 30 s, uploads completed (rotated) files to S3, and deletes local copies. On shutdown it uploads the in-progress file.

- **`internal/restore`** — Orchestrates the full restore sequence: resolves the best backup (latest `FinishTime ≤ TargetTime`), downloads and extracts the tar.gz, runs `xtrabackup --prepare` then `--copy-back`, stops/starts MySQL via configurable shell commands, then applies binlogs. Binlog apply pipes: `S3 objects → binlog.PipeReader → mysqlbinlog stdin → mysql stdin`.

### S3 key layout

```
<s3-prefix>/
  full-<timestamp>/
    backup.tar.gz
    backup.meta.json
  binlogs/
    binlog.000001
    binlog.000002
    ...
```

`s3-prefix` defaults to `mysql-pitr/` and is set globally via `--s3-prefix`.

### Password handling

Passwords are never logged. The backup command appends `--password=<val>` to the xtrabackup args slice but logs `args[:len(args)-1]` to omit it. The binlog command filters `--password=` args before logging.
