package iterator

import (
	"bytes"
	"testing"

	"github.com/EdgarOrtegaRamirez/lsmkv/internal/coding"
	"github.com/EdgarOrtegaRamirez/lsmkv/internal/skiplist"
)

// sliceIter is a test Iterator backed by a slice of records already in
// (key asc, seq desc) order.
type sliceIter struct {
	recs []coding.Record
	i    int
}

func newSliceIter(recs []coding.Record) *sliceIter { return &sliceIter{recs: recs, i: -1} }
func (s *sliceIter) Next() bool {
	s.i++
	return s.i < len(s.recs)
}
func (s *sliceIter) Record() coding.Record {
	if s.i < 0 || s.i >= len(s.recs) {
		return coding.Record{}
	}
	return s.recs[s.i]
}

func TestMergeIterator(t *testing.T) {
	a := newSliceIter([]coding.Record{
		{Key: []byte("a"), Value: []byte("a1"), Seq: 1, Type: coding.TypePut},
		{Key: []byte("c"), Value: []byte("c1"), Seq: 1, Type: coding.TypePut},
		{Key: []byte("e"), Value: []byte("e1"), Seq: 1, Type: coding.TypePut},
	})
	b := newSliceIter([]coding.Record{
		{Key: []byte("b"), Value: []byte("b1"), Seq: 1, Type: coding.TypePut},
		{Key: []byte("c"), Value: []byte("c2"), Seq: 2, Type: coding.TypePut},
		{Key: []byte("d"), Value: []byte("d1"), Seq: 1, Type: coding.TypePut},
	})
	m := NewMerge(a, b)
	var keys []string
	var seqs []uint64
	for m.Next() {
		keys = append(keys, string(m.Record().Key))
		seqs = append(seqs, m.Record().Seq)
	}
	wantKeys := []string{"a", "b", "c", "c", "d", "e"}
	wantSeqs := []uint64{1, 1, 2, 1, 1, 1} // for "c", newest (seq2) comes first
	if len(keys) != len(wantKeys) {
		t.Fatalf("merged %d, want %d: %v", len(keys), len(wantKeys), keys)
	}
	for i := range wantKeys {
		if keys[i] != wantKeys[i] {
			t.Errorf("key[%d] = %q, want %q", i, keys[i], wantKeys[i])
		}
		if seqs[i] != wantSeqs[i] {
			t.Errorf("seq[%d] = %d, want %d", i, seqs[i], wantSeqs[i])
		}
	}
}

func TestDedupIterator(t *testing.T) {
	a := newSliceIter([]coding.Record{
		{Key: []byte("k"), Value: []byte("old"), Seq: 1, Type: coding.TypePut},
	})
	b := newSliceIter([]coding.Record{
		{Key: []byte("k"), Value: nil, Seq: 3, Type: coding.TypeDelete},
	})
	c := newSliceIter([]coding.Record{
		{Key: []byte("k"), Value: []byte("mid"), Seq: 2, Type: coding.TypePut},
	})
	m := NewMerge(a, b, c)
	d := NewDedup(m)
	if d.Next() {
		t.Error("deleted key should yield no live record")
	}

	// Now a live newest version.
	a2 := newSliceIter([]coding.Record{
		{Key: []byte("k"), Value: []byte("old"), Seq: 1, Type: coding.TypePut},
	})
	b2 := newSliceIter([]coding.Record{
		{Key: []byte("k"), Value: []byte("new"), Seq: 5, Type: coding.TypePut},
	})
	m2 := NewMerge(a2, b2)
	d2 := NewDedup(m2)
	if !d2.Next() {
		t.Fatal("expected one live record")
	}
	if !bytes.Equal(d2.Record().Value, []byte("new")) {
		t.Errorf("value = %q, want new", d2.Record().Value)
	}
	if d2.Next() {
		t.Error("should yield only one record per key")
	}
}

func TestMergeFromSkipList(t *testing.T) {
	s1 := skiplist.New()
	s1.Insert(skiplist.Entry{Key: []byte("x"), Value: []byte("1"), Seq: 1, Type: skiplist.TypePut})
	s1.Insert(skiplist.Entry{Key: []byte("z"), Value: []byte("1"), Seq: 1, Type: skiplist.TypePut})
	s2 := skiplist.New()
	s2.Insert(skiplist.Entry{Key: []byte("y"), Value: []byte("2"), Seq: 2, Type: skiplist.TypePut})

	m := NewMerge(FromSkipList(s1.Iterator(nil)), FromSkipList(s2.Iterator(nil)))
	var got []string
	for m.Next() {
		got = append(got, string(m.Record().Key))
	}
	want := []string{"x", "y", "z"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
