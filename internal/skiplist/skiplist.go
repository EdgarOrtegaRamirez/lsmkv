// Package skiplist implements a probabilistic skip list, the in-memory
// sorted structure used as the LSM-tree memtable.
//
// A skip list provides O(log n) expected-time insert, lookup, and ordered
// iteration without the rebalancing cost of a self-balancing tree. Each node
// holds a key, value, monotonically increasing sequence number, and a type
// tag distinguishing puts from tombstones (deletes).
package skiplist

import (
	"math/rand/v2"
	"sync"
)

// EntryType distinguishes puts from tombstones.
type EntryType uint8

const (
	// TypePut is a regular key/value insertion.
	TypePut EntryType = 0
	// TypeDelete is a tombstone marking the key as deleted.
	TypeDelete EntryType = 1
)

// Entry is a single key/value record stored in the skip list.
type Entry struct {
	Key   []byte
	Value []byte
	Seq   uint64
	Type  EntryType
}

// IsTombstone reports whether the entry is a delete marker.
func (e Entry) IsTombstone() bool { return e.Type == TypeDelete }

// MaxLevel is the maximum number of forward pointers any node may have.
// With p=0.25, MaxLevel=20 supports ~10^12 elements comfortably.
const (
	MaxLevel = 20
	p        = 0.25
)

// node is an internal skip-list node.
type node struct {
	entry   Entry
	forward []*node
}

// SkipList is a concurrent-safe skip list keyed by (key, seq) so that newer
// writes shadow older ones during iteration.
type SkipList struct {
	mu    sync.RWMutex
	head  *node
	level int
	count int
	bytes int // approximate memory footprint
	rng   *rand.Rand
}

// New returns an empty skip list.
func New() *SkipList {
	return &SkipList{
		head:  &node{forward: make([]*node, MaxLevel)},
		level: 1,
		rng:   rand.New(rand.NewPCG(0xC0FFEE, 0x5EED)),
	}
}

// randomLevel returns a geometrically distributed level in [1, MaxLevel].
func (s *SkipList) randomLevel() int {
	lvl := 1
	for lvl < MaxLevel && s.rng.Float64() < p {
		lvl++
	}
	return lvl
}

// compareKeys compares two byte slices lexicographically.
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

// Insert adds an entry. If an entry with the same key and sequence number
// already exists it is replaced; otherwise the new entry is inserted in key
// order. Entries with equal keys are ordered by descending sequence number so
// that the newest version is visited first by iterators.
func (s *SkipList) Insert(e Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*node, MaxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && less(x.forward[i].entry, e) {
			x = x.forward[i]
		}
		update[i] = x
	}

	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	n := &node{entry: e, forward: make([]*node, lvl)}
	for i := 0; i < lvl; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}
	s.count++
	s.bytes += len(e.Key) + len(e.Value) + 16 // +16 for seq/type overhead
}

// less defines the skip-list ordering: ascending key, then descending seq.
func less(a, b Entry) bool {
	c := compareKeys(a.Key, b.Key)
	if c != 0 {
		return c < 0
	}
	// Same key: newer (larger) seq comes first.
	return a.Seq > b.Seq
}

// Get returns the newest entry for key, or false if none exists.
func (s *SkipList) Get(key []byte) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil {
			c := compareKeys(x.forward[i].entry.Key, key)
			if c < 0 {
				x = x.forward[i]
			} else if c == 0 {
				// First match at this level is the newest seq for the key.
				return x.forward[i].entry, true
			} else {
				break
			}
		}
	}
	return Entry{}, false
}

// Len returns the number of entries.
func (s *SkipList) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count
}

// ApproxSize returns an approximate memory footprint in bytes.
func (s *SkipList) ApproxSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytes
}

// Iterator returns a forward iterator positioned before the first entry whose
// key is greater than or equal to start. Pass nil for start to begin at the
// head. The iterator is not safe for concurrent mutation; callers should
// snapshot or freeze the memtable before iterating.
//
// The iterator follows the "positioned before first" convention: the first
// call to Next advances to and reports the first matching entry.
func (s *SkipList) Iterator(start []byte) *Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && compareKeys(x.forward[i].entry.Key, start) < 0 {
			x = x.forward[i]
		}
	}
	// x is the predecessor of the first node with key >= start; Next advances
	// from x to that first matching node.
	return &Iterator{cur: x}
}

// Iterator yields entries in ascending key order. For keys with multiple
// versions, the newest sequence number is visited first. It is positioned
// before the first entry; call Next to advance.
type Iterator struct {
	cur *node
}

// Next advances the iterator to the next entry and reports whether one is
// available. The first call reports the first matching entry.
func (it *Iterator) Next() bool {
	if it.cur == nil {
		return false
	}
	it.cur = it.cur.forward[0]
	return it.cur != nil
}

// Entry returns the current entry. Must be called only after Next returns true.
func (it *Iterator) Entry() Entry {
	if it.cur == nil {
		return Entry{}
	}
	return it.cur.entry
}

// Valid reports whether the iterator is positioned on a valid entry.
func (it *Iterator) Valid() bool { return it.cur != nil }
