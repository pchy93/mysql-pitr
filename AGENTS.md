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
backup  → xtrabackup --stream=xbstream → compression → S3 multipart upload
                                            S3 (s3prefix/<backup-id>/backup.xb.zst)
                                            S3 (s3prefix/<backup-id>/backup.meta.json)

binlog  → mysqlbinlog --read-from-remote-server → temp dir → parallel upload workers
                                                       S3 (s3prefix/binlogs/<binlog-file>)

restore → S3 stream → decompress → xbstream -x → xtrabackup --prepare → copy-back
        → S3 binlogs (parallel download) → mysqlbinlog | mysql

list    → scan S3 prefix for backup.meta.json files → display table or JSON
```

### Package responsibilities

- **`cmd/root.go`** — All CLI flag definitions and cobra command wiring. Global S3 flags (`--s3-*`) are parsed here and passed to `s3store.New()`. Each subcommand constructs its internal runner and calls `.Run(ctx)`.

- **`internal/s3store`** — Wrapper around `aws-sdk-go-v2/service/s3`. Supports custom endpoints (MinIO/Ceph) via `Config.Endpoint` and path-style addressing. Includes `multipart.go` for streaming uploads with 16MB part size.

- **`internal/backup`** — Uses `xtrabackup --stream=xbstream` for streaming backup directly to S3 without intermediate disk writes. Supports compression (zstd/gzip/none). Writes `backup.meta.json` with binlog position, GTID set, compression type, and size.

- **`internal/backup/compression.go`** — Compression wrappers using `klauspost/compress/zstd` (default, 3-5x faster than gzip) and standard `compress/gzip`.

- **`internal/binlog`** — Runs `mysqlbinlog --read-from-remote-server --raw --stop-never`. Uses worker pool for parallel uploads. Flush interval is 10 seconds (reduced from 30s for lower latency). On shutdown uploads remaining files.

- **`internal/restore`** — Streaming download via `S3 → decompressor → xbstream -x`. Supports three copy methods: `copy` (standard), `move` (--move-back, fast but empties backup), `hardlink` (reflink/hardlink for same filesystem). Parallel binlog download with configurable workers.

### S3 key layout

```
<s3-prefix>/
  full-<timestamp>/
    backup.xb.zst        # or .xb.gz / .xb depending on compression
    backup.meta.json
  binlogs/
    binlog.000001
    binlog.000002
    ...
```

`s3-prefix` defaults to `mysql-pitr/` and is set globally via `--s3-prefix`.

### Performance optimizations

| Optimization | Implementation | Benefit |
|--------------|----------------|---------|
| Streaming backup | `xtrabackup --stream=xbstream` → pipe → S3 | No intermediate disk I/O |
| Streaming restore | S3 → pipe → `xbstream -x` | No intermediate disk I/O |
| Zstd compression | `klauspost/compress/zstd` | 3-5x faster than gzip |
| S3 multipart | 16MB parts, streaming writer | Efficient large file upload |
| Binlog parallel upload | Worker pool (default 4) | Concurrent uploads |
| Copy-method options | move/hardlink/reflink | 20-30% faster restore |

### Key CLI flags

**backup:**
- `--compression` — none/gzip/zstd (default: zstd)
- `--parallel` — parallel threads (default: 4)

**binlog:**
- `--compression` — none/gzip/zstd (default: none)
- `--parallel` — upload workers (default: 4)

**restore:**
- `--parallel` — download threads (default: 4)
- `--copy-method` — copy/move/hardlink (default: copy)

### Password handling

Passwords are never logged. All commands filter `--password=` args before logging via `filterPassword()` helper.
