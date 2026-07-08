// Package iterator provides k-way merging of sorted record streams and a
// deduplicating wrapper that collapses multiple versions of a key to the
// newest, filtering tombstones for point/range scans.
package iterator

import (
	"github.com/EdgarOrtegaRamirez/lsmkv/internal/coding"
	"github.com/EdgarOrtegaRamirez/lsmkv/internal/skiplist"
)

// Iterator is the common interface for forward record streams. Implementations
// must yield records in ascending key order and, for a given key, in
// descending sequence-number order (newest version first).
type Iterator interface {
	Next() bool
	Record() coding.Record
}

// skiplistIter adapts a skiplist.Iterator to the Iterator interface.
type skiplistIter struct {
	it *skiplist.Iterator
}

// FromSkipList wraps a skiplist.Iterator as an Iterator.
func FromSkipList(it *skiplist.Iterator) Iterator { return skiplistIter{it: it} }

func (s skiplistIter) Next() bool { return s.it.Next() }
func (s skiplistIter) Record() coding.Record {
	e := s.it.Entry()
	return coding.Record{Key: e.Key, Value: e.Value, Seq: e.Seq, Type: byte(e.Type)}
}

// compareKeys is byte-wise lexicographic comparison.
func compareKeys(a, b []byte) int {
	la, lb := len(a), len(b)
	n := la
	if lb < n {
		n = lb
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if la < lb {
		return -1
	}
	if la > lb {
		return 1
	}
	return 0
}

// MergeIterator merges multiple sorted Iterators into a single stream ordered
// by ascending key and, within a key, descending sequence number. It emits
// every version of every key; use DedupIterator to collapse to the live version.
type MergeIterator struct {
	its   []Iterator
	heads []coding.Record
	ok    []bool
	cur   coding.Record
	valid bool
}

// NewMerge returns a MergeIterator over the given iterators. Iterators that are
// already positioned before their first element are expected; the merge calls
// Next on each to prime the heads.
func NewMerge(its ...Iterator) *MergeIterator {
	m := &MergeIterator{
		its:   its,
		heads: make([]coding.Record, len(its)),
		ok:    make([]bool, len(its)),
	}
	for i := range its {
		m.advance(i)
	}
	return m
}

func (m *MergeIterator) advance(i int) {
	if m.its[i].Next() {
		m.heads[i] = m.its[i].Record()
		m.ok[i] = true
	} else {
		m.ok[i] = false
	}
}

// Next advances to the next record in merged order and reports whether one is
// available.
func (m *MergeIterator) Next() bool {
	// Find the smallest key; among equal keys pick the newest (largest) seq.
	best := -1
	for i := 0; i < len(m.ok); i++ {
		if !m.ok[i] {
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		c := compareKeys(m.heads[i].Key, m.heads[best].Key)
		if c < 0 {
			best = i
		} else if c == 0 && m.heads[i].Seq > m.heads[best].Seq {
			// Same key, newer version wins.
			best = i
		}
	}
	if best < 0 {
		m.valid = false
		return false
	}
	m.cur = m.heads[best]
	m.valid = true
	m.advance(best)
	return true
}

// Record returns the current record.
func (m *MergeIterator) Record() coding.Record { return m.cur }

// Valid reports whether the iterator is positioned on a record.
func (m *MergeIterator) Valid() bool { return m.valid }

// DedupIterator wraps a MergeIterator (or any Iterator ordered key-asc,
// seq-desc) and yields only the newest version of each key, skipping
// tombstones. This is the view used by point lookups and range scans.
type DedupIterator struct {
	src     Iterator
	cur     coding.Record
	valid   bool
	started bool
	lastKey []byte
}

// NewDedup wraps src, collapsing duplicate keys and filtering tombstones.
func NewDedup(src Iterator) *DedupIterator { return &DedupIterator{src: src} }

// Next advances to the next live (non-tombstone, newest-version) record.
func (d *DedupIterator) Next() bool {
	for d.src.Next() {
		r := d.src.Record()
		if d.started && compareKeys(r.Key, d.lastKey) == 0 {
			// Older version of an already-emitted key; skip.
			continue
		}
		d.lastKey = append(d.lastKey[:0], r.Key...)
		d.started = true
		if r.Type == coding.TypeDelete {
			// Tombstone: key is deleted, do not emit, and keep skipping older
			// versions of this key.
			continue
		}
		d.cur = r
		d.valid = true
		return true
	}
	d.valid = false
	return false
}

// Record returns the current record.
func (d *DedupIterator) Record() coding.Record { return d.cur }

// Valid reports whether the iterator is positioned on a record.
func (d *DedupIterator) Valid() bool { return d.valid }
