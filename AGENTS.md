# AGENTS.md

Guidance for AI agents (and humans) working on this repository.

## Project

`lsmkv` is a zero-dependency, log-structured merge-tree key-value store in Go.
The entire storage stack is hand-written using only the Go standard library —
there are **no third-party module dependencies**. Keep it that way: do not add
`require` directives to `go.mod`. If a feature seems to need an external
package, implement it from scratch in `internal/` instead.

## Build & test

```bash
go build ./...     # must pass clean
go vet ./...       # must pass clean
go test ./...      # must pass
```

All three are run by CI (`.github/workflows/ci.yml`) on every push. A PR that
fails any of them must not be merged.

## Layout

```
internal/skiplist/   memtable (probabilistic skip list)
internal/bloom/      Bloom filter (double hashing)
internal/coding/     record format + varint coding
internal/sstable/    immutable sorted-string table (writer/reader, blocks, index)
internal/wal/        write-ahead log (append, fsync, replay)
internal/iterator/   k-way merge + dedup iterator
internal/engine/     the LSM coordinator (write/read/compact/recover)
cmd/lsmkv/           CLI
```

## Conventions

- **Package boundaries are strict.** `internal/engine` is the only package that
  imports the others; the storage primitives (`skiplist`, `bloom`, `sstable`,
  `wal`, `iterator`) do not import `engine`.
- **Errors are wrapped with context** at package boundaries (`fmt.Errorf("...
  %w", err)`). Sentinel errors (`ErrNotFound`) live in `engine`.
- **No `print`/`fmt.Println` in library code** — only in `cmd/lsmkv`. Tests may
  use `t.Logf`.
- **No bare `recover`/`catch`-all.** Handle expected errors explicitly; let
  unexpected ones propagate.
- **Mutex discipline:** the engine guards `memtable`, `immu`, `tables`, `wal`,
  `seq` with `e.mu`. Read paths take the lock for the duration of the snapshot
  they need; do not hold the lock across file I/O in new code.
- **Sequence numbers are monotonic and assigned under the lock.** The newest
  sequence number wins on read; tombstones shadow deletes.

## SSTable format

Data blocks → sparse index (one entry per block) → Bloom filter → footer
(48 bytes: indexOffset, indexLength, bloomOffset, bloomLength, count, maxSeq,
64-bit magic). Records: length-prefixed key, length-prefixed value, varint
sequence number, 1-byte type (`0` = Put, `1` = Delete). Keys within a table
**must be sorted ascending** — the writer does not re-sort. The `maxSeq` field
in the footer lets the engine restore its monotonic sequence counter in O(1)
on recovery without scanning all records.

## WAL recovery

On `Open`, all `wal-*.log` files are replayed in numeric order into a fresh
memtable, then consolidated into a single new WAL. The WAL writer is kept open
across recovery (reopening truncates). Do not call `wal.NewWriter` on an
existing consolidated file.

## Skip-list iterator convention

Iterators are positioned **before** the first element; the first `Next()` call
returns the first entry. This matches the SSTable reader iterator and the merge
iterator. Do not change this without updating all call sites.

## Adding a feature

1. Write the implementation in the appropriate `internal/` package.
2. Add tests in `<pkg>_test.go` covering the happy path and an error case.
3. `go build ./... && go vet ./... && go test ./...` must all pass.
4. Update `README.md` if the user-facing surface changes.
5. Commit with a conventional prefix: `feat:`, `fix:`, `refactor:`, `test:`,
   `docs:`, `chore:`, `ci:`, `security:`.

## Do not

- Add external dependencies.
- Introduce `init()` functions with side effects.
- Commit secrets, API keys, or the `.env` file.
- Push code that fails `go vet` or any test.
