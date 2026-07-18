// Package engine implements the log-structured merge-tree key-value store. It
// coordinates an in-memory skip-list memtable, a write-ahead log for
// durability, immutable on-disk SSTables, and size-tiered compaction.
//
// Write path: a mutation is assigned a monotonically increasing sequence
// number, appended to the active WAL, and inserted into the memtable. When the
// memtable exceeds a size threshold it is frozen into an immutable memtable, a
// fresh WAL is created for new writes, and the immutable is flushed to a new
// SSTable. Once the SSTable is durable the immutable's WAL is deleted.
//
// Read path: the memtable is consulted first (it holds the newest writes), then
// the immutable memtable, then SSTables from newest to oldest. A Bloom filter
// per SSTable skips tables that definitely do not contain the key. The newest
// sequence number for a key wins; tombstones shadow deleted keys.
//
// Compaction: when the number of SSTables exceeds a threshold, the oldest
// several are merged into one, keeping only the newest version of each key. A
// full compaction (Compact) merges every table and discards tombstones whose
// keys have no surviving live version.
package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/EdgarOrtegaRamirez/lsmkv/internal/coding"
	"github.com/EdgarOrtegaRamirez/lsmkv/internal/iterator"
	"github.com/EdgarOrtegaRamirez/lsmkv/internal/skiplist"
	"github.com/EdgarOrtegaRamirez/lsmkv/internal/sstable"
	"github.com/EdgarOrtegaRamirez/lsmkv/internal/wal"
)

// Default tuning constants.
const (
	defaultMemtableSize = 4 * 1024 * 1024 // 4 MiB
	defaultMaxTables    = 8               // trigger partial compaction at this count
	defaultMergeFactor  = 4               // tables merged per partial compaction
	defaultBloomFP      = 0.01
)

// Options configures an Engine.
type Options struct {
	// MemtableSize is the approximate byte threshold at which the memtable is
	// flushed to an SSTable.
	MemtableSize int
	// MaxTables triggers a partial compaction once the SSTable count reaches it.
	MaxTables int
	// MergeFactor is how many of the oldest tables a partial compaction merges.
	MergeFactor int
	// BloomFalsePositive is the target false-positive rate for SSTable filters.
	BloomFalsePositive float64
	// SyncWrites fsyncs the WAL after every mutation. If false, fsync happens on
	// memtable flush. Default true.
	SyncWrites bool
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() Options {
	return Options{
		MemtableSize:       defaultMemtableSize,
		MaxTables:          defaultMaxTables,
		MergeFactor:        defaultMergeFactor,
		BloomFalsePositive: defaultBloomFP,
		SyncWrites:         true,
	}
}

// Engine is a log-structured merge-tree key-value store.
type Engine struct {
	dir  string
	opts Options
	mu   sync.Mutex

	memtable *skiplist.SkipList
	immu     *skiplist.SkipList // immutable memtable being flushed (nil if none)

	wal  *wal.Writer
	walN int // active WAL generation number

	tables     []*sstable.Reader // sorted newest-first (highest number first)
	nextTableN int
	nextWalN   int
	seq        uint64

	closed bool
}

// Stats reports engine metrics.
type Stats struct {
	MemtableBytes int
	MemtableCount int
	HasImmutable  bool
	SSTableCount  int
	NextSeq       uint64
	ActiveWAL     int
	WALFiles      int
	SSTableFiles  int
}

// Open opens (or creates) a store at dir and recovers any committed state.
func Open(dir string, opts Options) (*Engine, error) {
	if opts.MemtableSize <= 0 {
		opts.MemtableSize = defaultMemtableSize
	}
	if opts.MaxTables <= 0 {
		opts.MaxTables = defaultMaxTables
	}
	if opts.MergeFactor <= 0 {
		opts.MergeFactor = defaultMergeFactor
	}
	if opts.BloomFalsePositive <= 0 || opts.BloomFalsePositive >= 1 {
		opts.BloomFalsePositive = defaultBloomFP
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: create dir: %w", err)
	}

	e := &Engine{
		dir:  dir,
		opts: opts,
		seq:  0,
	}

	if err := e.recover(); err != nil {
		return nil, fmt.Errorf("engine: recover: %w", err)
	}
	return e, nil
}

// recover loads SSTables, replays WALs into the memtable, and consolidates to a
// single fresh WAL.
func (e *Engine) recover() error {
	// 1. Load SSTables.
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return err
	}
	var sstNums []int
	for _, en := range entries {
		name := en.Name()
		if strings.HasSuffix(name, ".sst") {
			n, err := strconv.Atoi(strings.TrimSuffix(name, ".sst"))
			if err == nil {
				sstNums = append(sstNums, n)
			}
		}
	}
	sort.Ints(sstNums)
	maxSeq := uint64(0)
	for i := len(sstNums) - 1; i >= 0; i-- { // newest first
		n := sstNums[i]
		r, err := sstable.Open(filepath.Join(e.dir, fmt.Sprintf("%06d.sst", n)))
		if err != nil {
			return fmt.Errorf("open sst %d: %w", n, err)
		}
		e.tables = append(e.tables, r)
		if ms := r.MaxSeq(); ms > maxSeq {
			maxSeq = ms
		}
	}
	if len(sstNums) > 0 {
		e.nextTableN = sstNums[len(sstNums)-1] + 1
	}

	// 2. Replay WALs into memtable.
	e.memtable = skiplist.New()
	var walNums []int
	maxWalN := 0
	for _, en := range entries {
		name := en.Name()
		if strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log") {
			mid := strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log")
			n, err := strconv.Atoi(mid)
			if err == nil {
				walNums = append(walNums, n)
				if n > maxWalN {
					maxWalN = n
				}
			}
		}
	}
	sort.Ints(walNums)
	for _, n := range walNums {
		records, err := wal.Replay(filepath.Join(e.dir, fmt.Sprintf("wal-%06d.log", n)))
		if err != nil {
			return fmt.Errorf("replay wal %d: %w", n, err)
		}
		for _, r := range records {
			e.memtable.Insert(skiplist.Entry{
				Key: r.Key, Value: r.Value, Seq: r.Seq, Type: skiplist.EntryType(r.Type),
			})
			if r.Seq > maxSeq {
				maxSeq = r.Seq
			}
		}
	}
	e.seq = maxSeq
	e.nextWalN = maxWalN + 1

	// 3. Consolidate: write the recovered memtable to a fresh WAL, delete old WALs.
	// The writer is kept open so subsequent writes append to the same file
	// (NewWriter truncates, so we must not reopen the consolidated file).
	newWalN := e.nextWalN
	newWalPath := filepath.Join(e.dir, fmt.Sprintf("wal-%06d.log", newWalN))
	w, err := wal.NewWriter(newWalPath)
	if err != nil {
		return err
	}
	it := e.memtable.Iterator(nil)
	for it.Next() {
		ent := it.Entry()
		if err := w.Append(ent.Key, ent.Value, ent.Seq, byte(ent.Type)); err != nil {
			_ = w.Close()
			return err
		}
	}
	if err := w.Sync(); err != nil {
		_ = w.Close()
		return err
	}
	// Delete old WAL files (data now consolidated into the fresh one).
	for _, n := range walNums {
		_ = os.Remove(filepath.Join(e.dir, fmt.Sprintf("wal-%06d.log", n)))
	}
	e.wal = w // keep open for appends
	e.walN = newWalN
	e.nextWalN = newWalN + 1
	return nil
}

// Put stores key=value.
func (e *Engine) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	return e.write(key, value, coding.TypePut)
}

// Delete marks key as deleted (writes a tombstone).
func (e *Engine) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	return e.write(key, nil, coding.TypeDelete)
}

func (e *Engine) write(key, value []byte, rtype byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("engine: closed")
	}
	e.seq++
	seq := e.seq
	if err := e.wal.Append(key, value, seq, rtype); err != nil {
		return fmt.Errorf("engine: wal append: %w", err)
	}
	if e.opts.SyncWrites {
		if err := e.wal.Sync(); err != nil {
			return fmt.Errorf("engine: wal sync: %w", err)
		}
	}
	e.memtable.Insert(skiplist.Entry{
		Key: key, Value: value, Seq: seq, Type: skiplist.EntryType(rtype),
	})

	if e.memtable.ApproxSize() >= e.opts.MemtableSize {
		if err := e.maybeFlushLocked(); err != nil {
			return err
		}
	}
	return nil
}

// maybeFlushLocked freezes the memtable and flushes it. Requires e.mu held.
func (e *Engine) maybeFlushLocked() error {
	if e.memtable.Len() == 0 {
		return nil
	}
	// Freeze memtable into immutable, start a new memtable + new WAL.
	e.immu = e.memtable
	immuWalN := e.walN
	e.memtable = skiplist.New()

	newWalN := e.nextWalN
	newWalPath := filepath.Join(e.dir, fmt.Sprintf("wal-%06d.log", newWalN))
	newW, err := wal.NewWriter(newWalPath)
	if err != nil {
		// Roll back: restore memtable.
		e.memtable = e.immu
		e.immu = nil
		return fmt.Errorf("engine: new wal: %w", err)
	}
	if e.opts.SyncWrites {
		_ = newW.Sync()
	}
	oldWal := e.wal
	e.wal = newW
	e.walN = newWalN
	e.nextWalN = newWalN + 1

	// Flush immutable to a new SSTable.
	if err := e.flushImmutable(); err != nil {
		return err
	}

	// Close the old (immutable's) WAL and delete it; its data is now durable.
	_ = oldWal.Close()
	_ = os.Remove(filepath.Join(e.dir, fmt.Sprintf("wal-%06d.log", immuWalN)))
	e.immu = nil

	// Trigger compaction if too many tables.
	if len(e.tables) >= e.opts.MaxTables {
		_ = e.compactPartialLocked()
	}
	return nil
}

// flushImmutable writes the immutable memtable to a new SSTable. Requires e.mu.
func (e *Engine) flushImmutable() error {
	if e.immu == nil {
		return nil
	}
	n := e.nextTableN
	e.nextTableN++
	path := filepath.Join(e.dir, fmt.Sprintf("%06d.sst", n))
	// Estimate key count for the bloom filter.
	est := e.immu.Len()
	w, err := sstable.NewWriter(path, est)
	if err != nil {
		return err
	}
	it := e.immu.Iterator(nil)
	for it.Next() {
		ent := it.Entry()
		if err := w.Add(ent.Key, ent.Value, ent.Seq, byte(ent.Type)); err != nil {
			_ = w.Close()
			_ = os.Remove(path)
			return err
		}
	}
	if err := w.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	r, err := sstable.Open(path)
	if err != nil {
		_ = os.Remove(path)
		return err
	}
	// Insert at front (newest first).
	e.tables = append([]*sstable.Reader{r}, e.tables...)
	return nil
}

// Get returns the value for key, or ErrNotFound if absent/deleted.
var ErrNotFound = errors.New("engine: not found")

// Get returns the value for the newest live version of key.
func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, errors.New("engine: closed")
	}
	// Snapshot the table list and immutable reference under the lock.
	immu := e.immu
	mem := e.memtable
	tables := make([]*sstable.Reader, len(e.tables))
	copy(tables, e.tables)
	e.mu.Unlock()

	// Look up the key in each source, keeping the record with the highest
	// sequence number. The memtable and immutable memtable always hold newer
	// sequence numbers than the SSTables, so checking them first is an
	// optimisation; correctness comes from comparing sequence numbers.
	var best coding.Record
	found := false

	// 1. Active memtable.
	if ent, ok := mem.Get(key); ok {
		best = coding.Record{Key: ent.Key, Value: ent.Value, Seq: ent.Seq, Type: byte(ent.Type)}
		found = true
	}
	// 2. Immutable memtable (if present).
	if immu != nil {
		if ent, ok := immu.Get(key); ok && (!found || ent.Seq > best.Seq) {
			best = coding.Record{Key: ent.Key, Value: ent.Value, Seq: ent.Seq, Type: byte(ent.Type)}
			found = true
		}
	}
	// 3. SSTables: collect the newest-seq record across all tables. File-number
	// order is not sequence order (compacted tables hold old seqs), so we cannot
	// stop at the first hit.
	for _, t := range tables {
		rec, ok := t.Get(key)
		if !ok {
			continue
		}
		if !found || rec.Seq > best.Seq {
			best = rec
			found = true
		}
	}
	if !found {
		return nil, ErrNotFound
	}
	if best.Type == coding.TypeDelete {
		return nil, ErrNotFound
	}
	return best.Value, nil
}

// Scan returns up to limit live key/value pairs with keys >= start (ascending).
// A limit <= 0 means no limit.
func (e *Engine) Scan(start []byte, limit int) ([]KV, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, errors.New("engine: closed")
	}
	its := []iterator.Iterator{}
	if e.immu != nil {
		its = append(its, iterator.FromSkipList(e.immu.Iterator(start)))
	}
	its = append(its, iterator.FromSkipList(e.memtable.Iterator(start)))
	for _, t := range e.tables {
		its = append(its, t.NewIterator(start))
	}
	e.mu.Unlock()

	merge := iterator.NewMerge(its...)
	dedup := iterator.NewDedup(merge)
	var out []KV
	for dedup.Next() {
		r := dedup.Record()
		out = append(out, KV{Key: r.Key, Value: r.Value})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// KV is a key/value pair returned by Scan.
type KV struct {
	Key   []byte
	Value []byte
}

// Flush forces the active memtable to be flushed to an SSTable.
func (e *Engine) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("engine: closed")
	}
	return e.maybeFlushLocked()
}

// Compact performs a full compaction: merges every SSTable into one, discarding
// tombstones whose keys have no live version. The memtable is flushed first.
func (e *Engine) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return errors.New("engine: closed")
	}
	if err := e.maybeFlushLocked(); err != nil {
		return err
	}
	return e.compactFullLocked()
}

// compactPartialLocked merges the oldest MergeFactor tables into one, keeping
// tombstones (safe partial compaction). Requires e.mu.
func (e *Engine) compactPartialLocked() error {
	if len(e.tables) < e.opts.MergeFactor {
		return nil
	}
	// tables are newest-first; the oldest are at the end.
	n := e.opts.MergeFactor
	oldest := e.tables[len(e.tables)-n:]
	rest := e.tables[:len(e.tables)-n]

	merged, err := e.mergeTables(oldest, true) // keepTombstones = true
	if err != nil {
		return err
	}
	// Remove old readers, add merged (place at front since it's newest file num,
	// but its data spans old seqs; ordering by file num keeps it discoverable).
	for _, t := range oldest {
		_ = t.Close()
		_ = os.Remove(t.Path())
	}
	e.tables = append(rest, merged)
	// Re-sort newest-first by file number.
	sort.Slice(e.tables, func(i, j int) bool {
		return tableNumFromPath(e.tables[i].Path()) > tableNumFromPath(e.tables[j].Path())
	})
	return nil
}

// compactFullLocked merges all tables into one, dropping tombstones. Requires e.mu.
func (e *Engine) compactFullLocked() error {
	if len(e.tables) == 0 {
		return nil
	}
	merged, err := e.mergeTables(e.tables, false) // keepTombstones = false
	if err != nil {
		return err
	}
	for _, t := range e.tables {
		_ = t.Close()
		_ = os.Remove(t.Path())
	}
	e.tables = []*sstable.Reader{merged}
	return nil
}

// mergeTables merges the given readers into a single new SSTable. When
// keepTombstones is false, tombstones for keys with no live version are dropped
// (full compaction); when true, tombstones are retained (partial compaction).
func (e *Engine) mergeTables(readers []*sstable.Reader, keepTombstones bool) (*sstable.Reader, error) {
	its := make([]iterator.Iterator, 0, len(readers))
	totalCount := 0
	for _, r := range readers {
		its = append(its, r.NewIterator(nil))
		totalCount += r.Count()
	}
	merge := iterator.NewMerge(its...)

	n := e.nextTableN
	e.nextTableN++
	path := filepath.Join(e.dir, fmt.Sprintf("%06d.sst", n))
	w, err := sstable.NewWriter(path, max1(totalCount))
	if err != nil {
		return nil, err
	}

	// Collapse to newest version per key.
	var lastKey []byte
	written := 0
	for merge.Next() {
		r := merge.Record()
		if lastKey != nil && bytesEqual(r.Key, lastKey) {
			continue // older version, skip
		}
		lastKey = append(lastKey[:0], r.Key...)
		if r.Type == coding.TypeDelete && !keepTombstones {
			// Tombstone with no live version: drop entirely.
			continue
		}
		if err := w.Add(r.Key, r.Value, r.Seq, r.Type); err != nil {
			_ = w.Close()
			_ = os.Remove(path)
			return nil, err
		}
		written++
	}
	if err := w.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	_ = written
	r, err := sstable.Open(path)
	if err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return r, nil
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// tableNumFromPath parses the numeric prefix from an SSTable filename.
func tableNumFromPath(path string) int {
	base := filepath.Base(path)
	name := strings.TrimSuffix(base, ".sst")
	n, err := strconv.Atoi(name)
	if err != nil {
		return -1
	}
	return n
}

// Stats returns current engine metrics.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	entries, _ := os.ReadDir(e.dir)
	walCount, sstCount := 0, 0
	for _, en := range entries {
		if strings.HasPrefix(en.Name(), "wal-") && strings.HasSuffix(en.Name(), ".log") {
			walCount++
		}
		if strings.HasSuffix(en.Name(), ".sst") {
			sstCount++
		}
	}
	return Stats{
		MemtableBytes: e.memtable.ApproxSize(),
		MemtableCount: e.memtable.Len(),
		HasImmutable:  e.immu != nil,
		SSTableCount:  len(e.tables),
		NextSeq:       e.seq + 1,
		ActiveWAL:     e.walN,
		WALFiles:      walCount,
		SSTableFiles:  sstCount,
	}
}

// Close flushes the memtable and releases resources.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	if err := e.maybeFlushLocked(); err != nil {
		_ = e.wal.Close()
		return err
	}
	for _, t := range e.tables {
		_ = t.Close()
	}
	return e.wal.Close()
}
