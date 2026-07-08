# lsmkv

A zero-dependency, log-structured merge-tree (LSM) key-value store written in
pure Go (standard library only). `lsmkv` implements the core architecture behind
production engines like LevelDB and RocksDB — a skip-list memtable, a
write-ahead log for durability, on-disk SSTables with Bloom filters and sparse
indexes, and size-tiered compaction — using **no third-party packages**.

[![CI](https://github.com/EdgarOrtegaRamirez/lsmkv/actions/workflows/ci.yml/badge.svg)](https://github.com/EdgarOrtegaRamirez/lsmkv/actions/workflows/ci.yml)

## Why

Most Go LSM-tree implementations wrap or port existing C++ engines. `lsmkv` is a
from-scratch, readable, dependency-free implementation intended for learning,
embedding, and experimentation. The entire storage path — skip list, Bloom
filter, varint coding, SSTable block format, WAL, merge iterator, compaction —
is hand-written and lives in a single small module you can read end to end.

- **Zero dependencies.** `go.mod` has no `require` directives. Builds anywhere Go
  1.25+ runs, with no network access needed at build time.
- **Durable.** Every mutation is appended to a write-ahead log and fsynced before
  the caller returns. Crash recovery replays the WAL into a fresh memtable.
- **Ordered.** Keys are stored sorted; `Scan` returns a contiguous, deduplicated
  range in ascending key order with tombstones filtered out.
- **Compact.** Size-tiered compaction merges the oldest SSTables, keeping only the
  newest version of each key. A full `Compact()` discards tombstones entirely.

## Architecture

```
                    ┌──────────────┐
   Put/Delete ───▶  │   Memtable   │  (in-memory skip list, newest writes)
                    │  + WAL fsync │
                    └──────┬───────┘
                       flush (size threshold or explicit)
                            ▼
                    ┌──────────────┐
                    │   SSTable    │  (immutable, on disk, sorted)
                    │ data blocks  │
                    │ sparse index │
                    │ bloom filter │
                    └──────┬───────┘
                       compaction (size-tiered)
                            ▼
                    ┌──────────────┐
                    │ merged SST   │  (newest version per key; tombstones
                    │              │   dropped on full compaction)
                    └──────────────┘
```

**Read path** checks the memtable → immutable memtable → SSTables (newest first),
keeping the record with the highest sequence number. A per-SSTable Bloom filter
skips tables that definitely do not contain the key. Tombstones shadow deleted
keys; a `Get` returns `ErrNotFound` if the newest surviving record is a tombstone.

### On-disk layout

| File              | Pattern            | Purpose                                  |
|-------------------|--------------------|------------------------------------------|
| Write-ahead log   | `wal-NNNNNN.log`   | durable mutation stream, replayed on open|
| SSTable           | `NNNNNN.sst`       | immutable sorted run of key/value records|

Each SSTable is laid out as: **data blocks** → **sparse index** (one entry per
block, key = first key in block) → **Bloom filter** → **footer** (offsets +
64-bit magic). Records use length-prefixed varint coding with a sequence number
and type tag (`Put` / `Delete`).

## Quick start

```bash
go install github.com/EdgarOrtegaRamirez/lsmkv/cmd/lsmkv@latest
```

### CLI

```bash
# Store and retrieve values.
lsmkv put  ./mydb hello world
lsmkv get  ./mydb hello          # → world

# Binary values via stdin.
echo -n 'raw bytes' | lsmkv put ./mydb blob -

# Delete and scan.
lsmkv delete ./mydb hello
lsmkv scan   ./mydb              # all keys, ascending
lsmkv scan   ./mydb b 10         # from key "b", up to 10 results

# Maintenance and introspection.
lsmkv stats   ./mydb             # memtable/SSTable/WAL counts
lsmkv flush   ./mydb             # force memtable → SSTable
lsmkv compact ./mydb            # full compaction (drops tombstones)
lsmkv version
```

### As a library

```go
package main

import (
	"fmt"
	"log"

	"github.com/EdgarOrtegaRamirez/lsmkv/internal/engine"
)

func main() {
	eng, err := engine.Open("./mydb", engine.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer eng.Close()

	if err := eng.Put([]byte("greeting"), []byte("hello, world")); err != nil {
		log.Fatal(err)
	}

	val, err := eng.Get([]byte("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(val)) // hello, world

	// Ordered range scan (tombstones filtered out).
	pairs, _ := eng.Scan(nil, 0)
	for _, kv := range pairs {
		fmt.Printf("%s = %s\n", kv.Key, kv.Value)
	}

	eng.Delete([]byte("greeting"))
	eng.Compact() // reclaim space, drop tombstones
}
```

### Tuning

```go
opts := engine.Options{
	MemtableSize:       16 << 20, // 16 MiB before a flush is triggered
	MaxTables:          8,        // partial compaction trigger
	MergeFactor:       4,        // tables merged per partial compaction
	BloomFalsePositive: 0.01,    // 1% target false-positive rate
	SyncWrites:        true,     // fsync WAL after every mutation
}
eng, _ := engine.Open("./mydb", opts)
```

## Project layout

```
internal/
  skiplist/   probabilistic skip list — the in-memory memtable
  bloom/      Bloom filter with double hashing (Kirsch-Mitzenmacher)
  coding/     shared record format (key, value, seq, type) with varint coding
  sstable/    immutable sorted-string table: writer, reader, block index
  wal/        append-only write-ahead log with fsync and replay
  iterator/   k-way merge iterator + deduplicating/tombstone-filtering wrapper
  engine/     the LSM coordinator: write path, read path, compaction, recovery
cmd/lsmkv/   command-line interface
```

## Testing

```bash
go test ./...          # all packages
go test ./internal/engine -run CrashRecovery -v
go vet ./...
```

The test suite covers the happy path and error cases for every component:
skip-list iteration and tombstones, Bloom filter false-positive rate, WAL
append/replay/crash-recovery, merge-iterator ordering and deduplication,
multi-block SSTable reads, and engine-level persistence, crash recovery,
compaction tombstone dropping, large values, and reopen-after-compaction.

## Security notes

- The store is a **single-process, local-file** engine. It does not expose a
  network surface; do not expose the data directory to untrusted callers.
- **Validate keys at the application boundary.** Empty keys are rejected; keys
  and values are arbitrary byte slices and are not interpreted.
- The data directory holds durable WAL and SSTable files. Protect it with
  standard filesystem permissions (`0600`/`0700`); the engine creates files with
  restrictive modes but does not override an existing directory's permissions.
- There is no built-in encryption or authentication. For sensitive data, encrypt
  at the filesystem layer or wrap values before writing.

## Limitations

- Single-writer process (no multi-process locking). Concurrent access from one
  process is safe: writes are mutex-guarded, reads take a shared lock.
- No range tombstones — deletes are per-key only.
- No transactions or snapshots.
- Size-tiered compaction (not leveled). Write amplification is higher than a
  leveled design but the implementation is far simpler and easier to follow.

## License

MIT — see [LICENSE](LICENSE).
