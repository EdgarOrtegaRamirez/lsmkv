package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/EdgarOrtegaRamirez/lsmkv/internal/coding"
)

func TestWALAppendReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	records := []struct {
		key, val string
		seq      uint64
		typ      byte
	}{
		{"k1", "v1", 1, coding.TypePut},
		{"k2", "v2", 2, coding.TypePut},
		{"k3", "", 3, coding.TypeDelete},
		{"k4", "v4", 4, coding.TypePut},
	}
	for _, r := range records {
		if err := w.Append([]byte(r.key), []byte(r.val), r.seq, r.typ); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != len(records) {
		t.Fatalf("replayed %d records, want %d", len(got), len(records))
	}
	for i, r := range records {
		if string(got[i].Key) != r.key {
			t.Errorf("rec[%d] key = %q, want %q", i, got[i].Key, r.key)
		}
		if string(got[i].Value) != r.val {
			t.Errorf("rec[%d] value = %q, want %q", i, got[i].Value, r.val)
		}
		if got[i].Seq != r.seq {
			t.Errorf("rec[%d] seq = %d, want %d", i, got[i].Seq, r.seq)
		}
		if got[i].Type != r.typ {
			t.Errorf("rec[%d] type = %d, want %d", i, got[i].Type, r.typ)
		}
	}
}

func TestWALTornRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, _ := NewWriter(path)
	_ = w.Append([]byte("good"), []byte("v"), 1, coding.TypePut)
	_ = w.Sync()
	// Append a complete second record.
	_ = w.Append([]byte("good2"), []byte("v2"), 2, coding.TypePut)
	_ = w.Sync()
	_ = w.Close()

	// Truncate the file to simulate a torn final write (drop last few bytes).
	st, _ := os.Stat(path)
	if err := os.Truncate(path, st.Size()-3); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	got, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	// The first complete record must survive; the torn tail is dropped.
	if len(got) < 1 {
		t.Fatalf("expected at least 1 record, got %d", len(got))
	}
	if string(got[0].Key) != "good" {
		t.Errorf("first record key = %q, want good", got[0].Key)
	}
}

func TestWALEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.log")
	w, _ := NewWriter(path)
	_ = w.Close()
	got, err := Replay(path)
	if err != nil {
		t.Fatalf("Replay empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty WAL replayed %d records, want 0", len(got))
	}
}
