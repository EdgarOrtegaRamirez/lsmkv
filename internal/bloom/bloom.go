// Package bloom implements a standard Bloom filter used by SSTables to
// avoid unnecessary disk reads for keys that are definitely not present.
//
// The filter uses double hashing derived from a single 64-bit FNV-1a hash,
// giving k well-distributed probes without k independent hash functions.
package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// Filter is a Bloom filter over byte-slice keys.
type Filter struct {
	bits    []uint64 // bit array stored as 64-bit words
	numBits uint64
	k       int
}

// New returns a filter sized for n expected elements at a target false-positive
// rate. The bit count is derived from the optimal formula
// m = -n*ln(p) / (ln(2)^2) and k = (m/n)*ln(2).
func New(n int, fpRate float64) *Filter {
	if n <= 0 {
		n = 1
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}
	ln2 := math.Ln2
	m := uint64(float64(n) * -math.Log(fpRate) / (ln2 * ln2))
	if m < 8 {
		m = 8
	}
	// Round up to nearest multiple of 64 for word alignment.
	words := (m + 63) / 64
	m = words * 64
	k := int(float64(m) / float64(n) * ln2)
	if k < 1 {
		k = 1
	}
	return &Filter{bits: make([]uint64, words), numBits: m, k: k}
}

// doubleHash returns two 32-bit hashes derived from one FNV-1a 64-bit hash.
func doubleHash(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(key)
	b := h.Sum(nil)
	hi := binary.BigEndian.Uint32(b[0:4])
	lo := binary.BigEndian.Uint32(b[4:8])
	return hi, lo
}

// Add inserts key into the filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := doubleHash(key)
	for i := 0; i < f.k; i++ {
		pos := (uint64(h1) + uint64(i)*uint64(h2)) % f.numBits
		word := pos / 64
		bit := pos % 64
		f.bits[word] |= 1 << bit
	}
}

// MayContain reports whether key may be present. A false result is definitive;
// a true result may be a false positive.
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := doubleHash(key)
	for i := 0; i < f.k; i++ {
		pos := (uint64(h1) + uint64(i)*uint64(h2)) % f.numBits
		word := pos / 64
		bit := pos % 64
		if f.bits[word]&(1<<bit) == 0 {
			return false
		}
	}
	return true
}

// MarshalBinary encodes the filter for persistence in an SSTable footer.
func (f *Filter) MarshalBinary() []byte {
	out := make([]byte, 12+len(f.bits)*8)
	binary.BigEndian.PutUint64(out[0:8], f.numBits)
	binary.BigEndian.PutUint32(out[8:12], uint32(f.k))
	for i, w := range f.bits {
		binary.BigEndian.PutUint64(out[12+i*8:], w)
	}
	return out
}

// UnmarshalBinary decodes a filter previously produced by MarshalBinary.
func UnmarshalBinary(data []byte) (*Filter, error) {
	if len(data) < 12 {
		return nil, errCorrupt("bloom: short data")
	}
	numBits := binary.BigEndian.Uint64(data[0:8])
	k := int(binary.BigEndian.Uint32(data[8:12]))
	words := int(numBits / 64)
	if len(data) < 12+words*8 {
		return nil, errCorrupt("bloom: truncated bit array")
	}
	bits := make([]uint64, words)
	for i := 0; i < words; i++ {
		bits[i] = binary.BigEndian.Uint64(data[12+i*8:])
	}
	return &Filter{bits: bits, numBits: numBits, k: k}, nil
}

type errMsg string

func (e errMsg) Error() string  { return string(e) }
func errCorrupt(s string) error { return errMsg(s) }
