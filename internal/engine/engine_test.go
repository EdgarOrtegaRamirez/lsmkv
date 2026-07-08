package engine

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestEnginePutGetDelete(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	for i, k := range keys {
		if err := eng.Put([]byte(k), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	for i, k := range keys {
		got, err := eng.Get([]byte(k))
		if err != nil {
			t.Errorf("Get %s: %v", k, err)
			continue
		}
		want := fmt.Sprintf("v%d", i)
		if string(got) != want {
			t.Errorf("Get %s = %q, want %q", k, got, want)
		}
	}

	// Delete one and verify.
	if err := eng.Delete([]byte("charlie")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := eng.Get([]byte("charlie")); err != ErrNotFound {
		t.Errorf("Get after delete: err = %v, want ErrNotFound", err)
	}
	// Others still present.
	if _, err := eng.Get([]byte("bravo")); err != nil {
		t.Errorf("Get bravo after deleting charlie: %v", err)
	}
}

func TestEngineScan(t *testing.T) {
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	defer eng.Close()
	for _, k := range []string{"c", "a", "b", "e", "d"} {
		_ = eng.Put([]byte(k), []byte(k+"-val"))
	}
	// Delete one to ensure tombstones are filtered from scans.
	_ = eng.Delete([]byte("b"))

	pairs, err := eng.Scan(nil, 0)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := []string{"a", "c", "d", "e"}
	if len(pairs) != len(want) {
		t.Fatalf("scan returned %d, want %d: %+v", len(pairs), len(want), pairs)
	}
	for i, k := range want {
		if string(pairs[i].Key) != k {
			t.Errorf("scan[%d] key = %q, want %q", i, pairs[i].Key, k)
		}
	}

	// Scan with start key.
	pairs2, _ := eng.Scan([]byte("c"), 0)
	if len(pairs2) != 3 || string(pairs2[0].Key) != "c" {
		t.Errorf("scan from 'c' = %+v", pairs2)
	}

	// Scan with limit.
	pairs3, _ := eng.Scan(nil, 2)
	if len(pairs3) != 2 {
		t.Errorf("scan limit 2 returned %d, want 2", len(pairs3))
	}
}

func TestEnginePersistence(t *testing.T) {
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	for i := 0; i < 50; i++ {
		_ = eng.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	eng2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer eng2.Close()
	for i := 0; i < 50; i++ {
		got, err := eng2.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil {
			t.Errorf("Get k%03d after reopen: %v", i, err)
			continue
		}
		if string(got) != fmt.Sprintf("v%d", i) {
			t.Errorf("Get k%03d = %q, want v%d", i, got, i)
		}
	}
}

func TestEngineCrashRecovery(t *testing.T) {
	// Simulate a crash: write data WITHOUT closing (memtable not flushed), then
	// reopen. The WAL must replay the unflushed writes.
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	for i := 0; i < 20; i++ {
		if err := eng.Put([]byte(fmt.Sprintf("key%02d", i)), []byte(fmt.Sprintf("val%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Force a WAL sync to mimic durable writes, then drop the engine without
	// flushing the memtable (simulate crash).
	eng.mu.Lock()
	_ = eng.wal.Sync()
	eng.mu.Unlock()
	// Do NOT call Close; just abandon the engine (files remain on disk).

	eng2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer eng2.Close()
	for i := 0; i < 20; i++ {
		got, err := eng2.Get([]byte(fmt.Sprintf("key%02d", i)))
		if err != nil {
			t.Errorf("Get key%02d after crash recovery: %v", i, err)
			continue
		}
		if string(got) != fmt.Sprintf("val%d", i) {
			t.Errorf("Get key%02d = %q, want val%d", i, got, i)
		}
	}
}

func TestEngineReputAfterDelete(t *testing.T) {
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	defer eng.Close()
	_ = eng.Put([]byte("k"), []byte("v1"))
	_ = eng.Delete([]byte("k"))
	_ = eng.Put([]byte("k"), []byte("v2"))
	got, err := eng.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after re-put: %v", err)
	}
	if string(got) != "v2" {
		t.Errorf("Get = %q, want v2", got)
	}
}

func TestEngineCompaction(t *testing.T) {
	// Use a tiny memtable so many flushes create many SSTables, triggering
	// compaction.
	opts := DefaultOptions()
	opts.MemtableSize = 1024 // 1 KiB
	opts.MaxTables = 4
	opts.MergeFactor = 2
	dir := t.TempDir()
	eng, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Write enough keys with enough data to force multiple flushes.
	for i := 0; i < 200; i++ {
		if err := eng.Put([]byte(fmt.Sprintf("k%04d", i)), make([]byte, 100)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	// All keys must still be readable after compactions.
	for i := 0; i < 200; i++ {
		got, err := eng.Get([]byte(fmt.Sprintf("k%04d", i)))
		if err != nil {
			t.Errorf("Get k%04d after compaction: %v", i, err)
			continue
		}
		if len(got) != 100 {
			t.Errorf("Get k%04d len = %d, want 100", i, len(got))
		}
	}
	// Full compaction should drop tombstones and reduce table count.
	before := eng.Stats().SSTableCount
	if err := eng.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := eng.Stats().SSTableCount
	if after > before {
		t.Errorf("table count after full compaction = %d, before = %d", after, before)
	}
	// Data still readable.
	for i := 0; i < 200; i++ {
		if _, err := eng.Get([]byte(fmt.Sprintf("k%04d", i))); err != nil {
			t.Errorf("Get k%04d after full compaction: %v", i, err)
		}
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEngineCompactionDropsTombstones(t *testing.T) {
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	defer eng.Close()
	_ = eng.Put([]byte("k"), []byte("v"))
	_ = eng.Flush()
	_ = eng.Delete([]byte("k"))
	_ = eng.Flush()
	// Full compaction should remove the tombstone entirely.
	if err := eng.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if _, err := eng.Get([]byte("k")); err != ErrNotFound {
		t.Errorf("Get after compaction: err = %v, want ErrNotFound", err)
	}
	// Reopen and verify the key is still gone (tombstone was dropped, not just
	// shadowed in memory).
	eng2, _ := Open(dir, DefaultOptions())
	defer eng2.Close()
	if _, err := eng2.Get([]byte("k")); err != ErrNotFound {
		t.Errorf("Get after reopen: err = %v, want ErrNotFound", err)
	}
}

func TestEngineEmptyKey(t *testing.T) {
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	defer eng.Close()
	if err := eng.Put([]byte(""), []byte("v")); err == nil {
		t.Error("Put with empty key should error")
	}
}

func TestEngineLargeValue(t *testing.T) {
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	defer eng.Close()
	val := make([]byte, 1<<16) // 64 KiB
	for i := range val {
		val[i] = byte(i)
	}
	if err := eng.Put([]byte("big"), val); err != nil {
		t.Fatalf("Put large: %v", err)
	}
	got, err := eng.Get([]byte("big"))
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Errorf("large value mismatch: got %d bytes, want %d", len(got), len(val))
	}
}

func TestEngineReopenCompactedData(t *testing.T) {
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	for i := 0; i < 30; i++ {
		_ = eng.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	_ = eng.Compact()
	_ = eng.Close()

	eng2, _ := Open(dir, DefaultOptions())
	defer eng2.Close()
	for i := 0; i < 30; i++ {
		got, err := eng2.Get([]byte(fmt.Sprintf("k%02d", i)))
		if err != nil {
			t.Errorf("Get k%02d: %v", i, err)
			continue
		}
		if string(got) != fmt.Sprintf("v%d", i) {
			t.Errorf("Get k%02d = %q, want v%d", i, got, i)
		}
	}
}

// TestEngineSeqRecoveryOverwrite verifies that the monotonic sequence counter
// is restored from SSTables on reopen, so a new write to an existing key
// (assigned a sequence number higher than any in the old SSTables) correctly
// shadows the older version. Without maxSeq recovery this test fails: the new
// write gets seq=1 and the old SSTable entry (seq>1) wins on Get.
func TestEngineSeqRecoveryOverwrite(t *testing.T) {
	dir := t.TempDir()
	eng, _ := Open(dir, DefaultOptions())
	// Write several keys so the memtable flushes to an SSTable with seqs 1..N.
	for i := 0; i < 10; i++ {
		_ = eng.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("old"))
	}
	_ = eng.Flush()
	_ = eng.Close()

	// Reopen — seq must be restored to 10, not reset to 0.
	eng2, _ := Open(dir, DefaultOptions())
	// Overwrite a key that exists in the SSTable. The new write must get
	// seq=11 (> 10), shadowing the old value.
	if err := eng2.Put([]byte("k05"), []byte("new")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	got, err := eng2.Get([]byte("k05"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "new" {
		t.Errorf("Get after overwrite = %q, want %q (seq not recovered from SSTable)", got, "new")
	}
	_ = eng2.Close()

	// Reopen again and verify the overwrite survived (it was in the WAL).
	eng3, _ := Open(dir, DefaultOptions())
	defer eng3.Close()
	got2, err := eng3.Get([]byte("k05"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got2) != "new" {
		t.Errorf("Get after reopen = %q, want %q", got2, "new")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
