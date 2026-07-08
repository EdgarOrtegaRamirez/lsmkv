package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/EdgarOrtegaRamirez/lsmkv/internal/coding"
)

func TestSSTableWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")
	w, err := NewWriter(path, 10)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	entries := []struct {
		key, val string
		seq      uint64
		typ      byte
	}{
		{"alpha", "A", 1, coding.TypePut},
		{"bravo", "B", 2, coding.TypePut},
		{"charlie", "C", 3, coding.TypePut},
		{"delta", "D", 4, coding.TypeDelete},
		{"echo", "E", 5, coding.TypePut},
	}
	for _, e := range entries {
		if err := w.Add([]byte(e.key), []byte(e.val), e.seq, e.typ); err != nil {
			t.Fatalf("Add %s: %v", e.key, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if r.Count() != len(entries) {
		t.Errorf("Count = %d, want %d", r.Count(), len(entries))
	}

	// Point gets.
	for _, e := range entries {
		rec, ok := r.Get([]byte(e.key))
		if !ok {
			t.Errorf("Get(%q): not found", e.key)
			continue
		}
		if rec.Seq != e.seq {
			t.Errorf("Get(%q) seq = %d, want %d", e.key, rec.Seq, e.seq)
		}
		if e.typ == coding.TypePut && string(rec.Value) != e.val {
			t.Errorf("Get(%q) value = %q, want %q", e.key, rec.Value, e.val)
		}
	}

	// Missing key.
	if _, ok := r.Get([]byte("zzz")); ok {
		t.Error("Get(zzz) should be absent")
	}

	// Iteration.
	it := r.NewIterator(nil)
	got := 0
	for it.Next() {
		got++
	}
	if got != len(entries) {
		t.Errorf("iterator yielded %d records, want %d", got, len(entries))
	}
	if it.Err() != nil {
		t.Errorf("iterator err: %v", it.Err())
	}

	// Iteration from a start key.
	it2 := r.NewIterator([]byte("charlie"))
	got2 := 0
	for it2.Next() {
		got2++
	}
	if got2 != 3 { // charlie, delta, echo
		t.Errorf("iterator from 'charlie' yielded %d, want 3", got2)
	}
}

func TestSSTableBloomRejection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "t.sst")
	w, _ := NewWriter(path, 4)
	for i, k := range []string{"a", "b", "c", "d"} {
		_ = w.Add([]byte(k), []byte("v"), uint64(i+1), coding.TypePut)
	}
	_ = w.Close()

	r, _ := Open(path)
	defer r.Close()
	// A key definitely not in the table should be rejected by the bloom filter.
	if r.MayContain([]byte("definitely-absent-key-xyz")) {
		// Bloom can false-positive, but with 4 keys and 1% target this is
		// extremely unlikely; treat as a soft check.
		t.Logf("bloom false-positive on absent key (acceptable but rare)")
	}
	// Keys in the table must pass the bloom filter.
	for _, k := range []string{"a", "b", "c", "d"} {
		if !r.MayContain([]byte(k)) {
			t.Errorf("bloom rejected present key %q", k)
		}
	}
}

func TestSSTableMultiBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.sst")
	w, _ := NewWriter(path, 1000)
	// Add enough records to span multiple 4KB blocks. Keys must be sorted.
	for i := 0; i < 200; i++ {
		key := []byte("key" + padInt(i)) // key0000..key0199, sorted ascending
		val := make([]byte, 64)
		if err := w.Add(key, val, uint64(i+1), coding.TypePut); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	if r.Count() != 200 {
		t.Errorf("Count = %d, want 200", r.Count())
	}
	// Verify a few point gets across the key space.
	for _, i := range []int{0, 50, 100, 199} {
		key := []byte("key" + padInt(i))
		rec, ok := r.Get(key)
		if !ok {
			t.Errorf("Get(%d) not found", i)
			continue
		}
		if rec.Seq != uint64(i+1) {
			t.Errorf("Get(%d) seq = %d, want %d", i, rec.Seq, i+1)
		}
	}
	// Full iteration count.
	it := r.NewIterator(nil)
	n := 0
	for it.Next() {
		n++
	}
	if n != 200 {
		t.Errorf("iterated %d, want 200", n)
	}
}

func padInt(i int) string {
	s := []byte{0, 0, 0, 0}
	x := i
	for j := 3; j >= 0; j-- {
		s[j] = byte('0' + (x % 10))
		x /= 10
	}
	return string(s)
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
