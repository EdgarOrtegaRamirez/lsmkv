package skiplist

import (
	"bytes"
	"testing"
)

func TestSkipListInsertGet(t *testing.T) {
	s := New()
	entries := []Entry{
		{Key: []byte("apple"), Value: []byte("1"), Seq: 1, Type: TypePut},
		{Key: []byte("banana"), Value: []byte("2"), Seq: 2, Type: TypePut},
		{Key: []byte("cherry"), Value: []byte("3"), Seq: 3, Type: TypePut},
	}
	for _, e := range entries {
		s.Insert(e)
	}
	if s.Len() != 3 {
		t.Errorf("Len = %d, want 3", s.Len())
	}
	for _, e := range entries {
		got, ok := s.Get(e.Key)
		if !ok {
			t.Errorf("Get(%q) not found", e.Key)
			continue
		}
		if !bytes.Equal(got.Value, e.Value) {
			t.Errorf("Get(%q) value = %q, want %q", e.Key, got.Value, e.Value)
		}
	}
	if _, ok := s.Get([]byte("missing")); ok {
		t.Error("Get(missing) should be absent")
	}
}

func TestSkipListNewestVersion(t *testing.T) {
	s := New()
	// Insert three versions of the same key; the newest seq must win.
	s.Insert(Entry{Key: []byte("k"), Value: []byte("v1"), Seq: 1, Type: TypePut})
	s.Insert(Entry{Key: []byte("k"), Value: []byte("v3"), Seq: 3, Type: TypePut})
	s.Insert(Entry{Key: []byte("k"), Value: []byte("v2"), Seq: 2, Type: TypePut})

	got, ok := s.Get([]byte("k"))
	if !ok {
		t.Fatal("Get(k) not found")
	}
	if string(got.Value) != "v3" {
		t.Errorf("newest value = %q, want v3", got.Value)
	}
	if got.Seq != 3 {
		t.Errorf("newest seq = %d, want 3", got.Seq)
	}
}

func TestSkipListIteratorOrder(t *testing.T) {
	s := New()
	keys := []string{"echo", "alpha", "charlie", "bravo", "delta"}
	for i, k := range keys {
		s.Insert(Entry{Key: []byte(k), Value: []byte(k), Seq: uint64(i + 1), Type: TypePut})
	}
	it := s.Iterator(nil)
	got := []string{}
	for it.Next() {
		got = append(got, string(it.Entry().Key))
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if len(got) != len(want) {
		t.Fatalf("iterator yielded %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("order[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSkipListIteratorStart(t *testing.T) {
	s := New()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		s.Insert(Entry{Key: []byte(k), Value: []byte(k), Seq: 1, Type: TypePut})
	}
	it := s.Iterator([]byte("c"))
	got := []string{}
	for it.Next() {
		got = append(got, string(it.Entry().Key))
	}
	want := []string{"c", "d", "e"}
	if len(got) != len(want) {
		t.Fatalf("from 'c' yielded %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("order[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestSkipListTombstone(t *testing.T) {
	s := New()
	s.Insert(Entry{Key: []byte("k"), Value: nil, Seq: 5, Type: TypeDelete})
	got, ok := s.Get([]byte("k"))
	if !ok {
		t.Fatal("Get(k) not found")
	}
	if !got.IsTombstone() {
		t.Error("expected tombstone")
	}
}

func TestSkipListApproxSize(t *testing.T) {
	s := New()
	before := s.ApproxSize()
	s.Insert(Entry{Key: []byte("k"), Value: []byte("vvvv"), Seq: 1, Type: TypePut})
	after := s.ApproxSize()
	if after <= before {
		t.Errorf("ApproxSize did not grow: %d -> %d", before, after)
	}
}
