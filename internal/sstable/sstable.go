// Package sstable implements an immutable sorted string table: an on-disk file
// of key/value records sorted by key (and by descending sequence number within
// a key). Each table is organised into fixed-size data blocks, fronted by a
// sparse index (first key of every block) and a Bloom filter, and terminated by
// a fixed-size footer that locates the index and filter.
//
// File layout:
//
//	[data block 0][data block 1]...[data block N]
//	[index block]
//	[bloom block]
//	[footer: 40 bytes]
//
// Footer (fixed 40 bytes, big-endian):
//
//	indexOffset  uint64
//	indexLength  uint64
//	bloomOffset  uint64
//	bloomLength  uint32
//	entryCount   uint32
//	magic        uint64  (0x4C534D535354424C)
package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/EdgarOrtegaRamirez/lsmkv/internal/bloom"
	"github.com/EdgarOrtegaRamirez/lsmkv/internal/coding"
)

// Magic identifies an LSMKV SSTable file.
const Magic uint64 = 0x4C534D535354424C // "LSMSSTBL"

const (
	footerSize  = 48
	blockTarget = 4096 // flush a data block once it reaches this many bytes
)

var (
	// ErrCorrupt is returned for malformed tables.
	ErrCorrupt = errors.New("sstable: corrupt file")
	// ErrBadMagic indicates an missing or wrong footer magic.
	ErrBadMagic = errors.New("sstable: bad magic number")
)

// indexEntry locates one data block within the table.
type indexEntry struct {
	key    []byte // first key in the block
	offset int64
	length int
}

// Writer builds an SSTable. Records must be added in ascending key order; for a
// given key, the newest (largest) sequence number must be added first.
type Writer struct {
	w        *bufio.Writer
	f        *os.File
	offset   int64
	curBlock []byte
	index    []indexEntry
	bf       *bloom.Filter
	count    int
	minKey   []byte
	maxKey   []byte
	maxSeq   uint64
	closed   bool
}

// NewWriter creates a Writer writing to path. expectedKeys sizes the Bloom
// filter; pass a rough estimate of the number of keys.
func NewWriter(path string, expectedKeys int) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{
		w:      bufio.NewWriterSize(f, 64*1024),
		f:      f,
		bf:     bloom.New(expectedKeys, 0.01),
		offset: 0,
	}, nil
}

// Add appends a record. Records must be added in sorted order (ascending key,
// descending sequence within a key).
func (w *Writer) Add(key, value []byte, seq uint64, rtype byte) error {
	if w.closed {
		return errors.New("sstable: writer closed")
	}
	if len(w.minKey) == 0 {
		w.minKey = append(w.minKey[:0], key...)
	}
	w.maxKey = append(w.maxKey[:0], key...)
	w.bf.Add(key)
	w.count++
	if seq > w.maxSeq {
		w.maxSeq = seq
	}

	// Append the record to the current data block buffer.
	w.curBlock = coding.EncodeRecord(w.curBlock, key, value, seq, rtype)
	if len(w.curBlock) >= blockTarget {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

// flushBlock writes the current data block and records its index entry. The
// index key is the first key in the block, captured before any records were
// appended; we re-derive it here by decoding the first record.
func (w *Writer) flushBlock() error {
	if len(w.curBlock) == 0 {
		return nil
	}
	first, _, err := coding.DecodeRecord(w.curBlock, 0)
	if err != nil {
		return err
	}
	keyCopy := append([]byte(nil), first.Key...)

	n, err := w.w.Write(w.curBlock)
	if err != nil {
		return err
	}
	w.index = append(w.index, indexEntry{key: keyCopy, offset: w.offset, length: n})
	w.offset += int64(n)
	w.curBlock = w.curBlock[:0]
	return nil
}

// Close finalises the table: flushes the last block, writes the index, the
// bloom filter, and the footer.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.flushBlock(); err != nil {
		_ = w.f.Close()
		return err
	}

	// Index block: uint32 count, then per entry {keyLen uint16, key, offset uint64, length uint32}.
	indexOffset := w.offset
	var idx []byte
	idx = binary.AppendUvarint(idx, uint64(len(w.index)))
	for _, e := range w.index {
		idx = binary.AppendUvarint(idx, uint64(len(e.key)))
		idx = append(idx, e.key...)
		idx = binary.BigEndian.AppendUint64(idx, uint64(e.offset))
		idx = binary.BigEndian.AppendUint32(idx, uint32(e.length))
	}
	n, err := w.w.Write(idx)
	if err != nil {
		_ = w.f.Close()
		return err
	}
	indexLength := int64(n)
	w.offset += indexLength

	// Bloom block.
	bloomOffset := w.offset
	bloomData := w.bf.MarshalBinary()
	n, err = w.w.Write(bloomData)
	if err != nil {
		_ = w.f.Close()
		return err
	}
	bloomLength := uint32(n)
	w.offset += int64(n)

	// Footer (48 bytes): indexOffset, indexLength, bloomOffset, bloomLength,
	// count, maxSeq, magic.
	var footer [footerSize]byte
	binary.BigEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.BigEndian.PutUint64(footer[8:16], uint64(indexLength))
	binary.BigEndian.PutUint64(footer[16:24], uint64(bloomOffset))
	binary.BigEndian.PutUint32(footer[24:28], bloomLength)
	binary.BigEndian.PutUint32(footer[28:32], uint32(w.count))
	binary.BigEndian.PutUint64(footer[32:40], w.maxSeq)
	binary.BigEndian.PutUint64(footer[40:48], Magic)
	if _, err := w.w.Write(footer[:]); err != nil {
		_ = w.f.Close()
		return err
	}

	if err := w.w.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// Reader provides read access to an SSTable. The index and bloom filter are
// loaded into memory on Open; data blocks are read on demand.
type Reader struct {
	f      *os.File
	path   string
	size   int64
	index  []indexEntry
	bf     *bloom.Filter
	count  int
	minKey []byte
	maxSeq uint64
}

// Open opens an existing SSTable at path and loads its metadata.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() < footerSize {
		_ = f.Close()
		return nil, ErrCorrupt
	}

	// Read footer.
	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, st.Size()-footerSize); err != nil && !errors.Is(err, io.EOF) {
		_ = f.Close()
		return nil, err
	}
	if binary.BigEndian.Uint64(footer[40:48]) != Magic {
		_ = f.Close()
		return nil, ErrBadMagic
	}
	indexOffset := int64(binary.BigEndian.Uint64(footer[0:8]))
	indexLength := int64(binary.BigEndian.Uint64(footer[8:16]))
	bloomOffset := int64(binary.BigEndian.Uint64(footer[16:24]))
	bloomLength := int(binary.BigEndian.Uint32(footer[24:28]))
	count := int(binary.BigEndian.Uint32(footer[28:32]))
	maxSeq := binary.BigEndian.Uint64(footer[32:40])

	r := &Reader{f: f, path: path, size: st.Size(), count: count, maxSeq: maxSeq}

	// Read index block.
	idxBuf := make([]byte, indexLength)
	if _, err := f.ReadAt(idxBuf, indexOffset); err != nil && !errors.Is(err, io.EOF) {
		_ = f.Close()
		return nil, err
	}
	if err := r.parseIndex(idxBuf); err != nil {
		_ = f.Close()
		return nil, err
	}

	// Read bloom block.
	bloomBuf := make([]byte, bloomLength)
	if _, err := f.ReadAt(bloomBuf, bloomOffset); err != nil && !errors.Is(err, io.EOF) {
		_ = f.Close()
		return nil, err
	}
	bf, err := bloom.UnmarshalBinary(bloomBuf)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r.bf = bf

	if len(r.index) > 0 {
		r.minKey = r.index[0].key
		// maxKey is the last key in the last block; decode it lazily is costly,
		// so approximate with the last index key's block. We keep minKey only.
	}
	return r, nil
}

func (r *Reader) parseIndex(buf []byte) error {
	off := 0
	n, m := binary.Uvarint(buf[off:])
	if m <= 0 {
		return ErrCorrupt
	}
	off += m
	count := int(n)
	r.index = make([]indexEntry, 0, count)
	for i := 0; i < count; i++ {
		kl, m := binary.Uvarint(buf[off:])
		if m <= 0 {
			return ErrCorrupt
		}
		off += m
		if off+int(kl) > len(buf) {
			return ErrCorrupt
		}
		key := append([]byte(nil), buf[off:off+int(kl)]...)
		off += int(kl)
		if off+12 > len(buf) {
			return ErrCorrupt
		}
		offset := int64(binary.BigEndian.Uint64(buf[off:]))
		off += 8
		length := int(binary.BigEndian.Uint32(buf[off:]))
		off += 4
		r.index = append(r.index, indexEntry{key: key, offset: offset, length: length})
	}
	return nil
}

// Count returns the number of records in the table.
func (r *Reader) Count() int { return r.count }

// MaxSeq returns the highest sequence number of any record in the table.
// It is stored in the footer and read in O(1) on Open; the engine uses it to
// restore its monotonic sequence counter after recovery.
func (r *Reader) MaxSeq() uint64 { return r.maxSeq }

// MinKey returns the smallest key in the table (or nil if empty).
func (r *Reader) MinKey() []byte { return r.minKey }

// Path returns the file path of the table.
func (r *Reader) Path() string { return r.path }

// Close releases the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

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

// MayContain consults the Bloom filter. A false result means the key is
// definitely absent from this table.
func (r *Reader) MayContain(key []byte) bool {
	if r.bf == nil {
		return true
	}
	return r.bf.MayContain(key)
}

// findBlock returns the index of the data block that should contain key, or -1
// if key is smaller than the first block's first key.
func (r *Reader) findBlock(key []byte) int {
	lo, hi := 0, len(r.index)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		c := compareKeys(key, r.index[mid].key)
		if c < 0 {
			hi = mid - 1
		} else if c > 0 {
			lo = mid + 1
		} else {
			return mid
		}
	}
	return hi // last block whose first key <= key
}

// Get returns the newest record for key, or false if absent.
func (r *Reader) Get(key []byte) (coding.Record, bool) {
	if !r.MayContain(key) {
		return coding.Record{}, false
	}
	bi := r.findBlock(key)
	if bi < 0 {
		return coding.Record{}, false
	}
	block, err := r.readBlock(bi)
	if err != nil {
		return coding.Record{}, false
	}
	off := 0
	var best coding.Record
	found := false
	for off < len(block) {
		rec, n, err := coding.DecodeRecord(block, off)
		if err != nil {
			break
		}
		off += n
		c := compareKeys(rec.Key, key)
		if c == 0 {
			if !found || rec.Seq > best.Seq {
				best = rec
				found = true
			}
		} else if c > 0 {
			// Past the key; records are sorted.
			break
		}
	}
	return best, found
}

func (r *Reader) readBlock(i int) ([]byte, error) {
	e := r.index[i]
	buf := make([]byte, e.length)
	if _, err := r.f.ReadAt(buf, e.offset); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf, nil
}

// Iterator yields records in ascending key order. For keys with multiple
// versions the newest sequence number is visited first.
type Iterator struct {
	r        *Reader
	blockIdx int
	block    []byte
	off      int
	cur      coding.Record
	valid    bool
	err      error
	started  bool
	startKey []byte
}

// NewIterator returns an iterator positioned before the first record whose key
// is greater than or equal to start. Pass nil for start to scan from the
// beginning.
func (r *Reader) NewIterator(start []byte) *Iterator {
	it := &Iterator{r: r, startKey: start}
	if len(r.index) == 0 {
		return it
	}
	if start == nil {
		it.blockIdx = 0
	} else {
		it.blockIdx = r.findBlock(start)
		if it.blockIdx < 0 {
			it.blockIdx = 0
		}
	}
	return it
}

// Next advances to the next record and reports whether one is available.
func (it *Iterator) Next() bool {
	for {
		if it.err != nil {
			return false
		}
		if it.block == nil {
			if it.blockIdx >= len(it.r.index) {
				return false
			}
			b, err := it.r.readBlock(it.blockIdx)
			if err != nil {
				it.err = err
				return false
			}
			it.block = b
			it.off = 0
		}
		if it.off >= len(it.block) {
			it.block = nil
			it.blockIdx++
			continue
		}
		rec, n, err := coding.DecodeRecord(it.block, it.off)
		if err != nil {
			it.err = err
			return false
		}
		it.off += n
		if it.startKey != nil && compareKeys(rec.Key, it.startKey) < 0 {
			continue
		}
		// Past the start key; stop filtering further.
		it.startKey = nil
		it.cur = rec
		it.valid = true
		return true
	}
}

// Record returns the current record. Only valid after Next returns true.
func (it *Iterator) Record() coding.Record { return it.cur }

// Valid reports whether the iterator is positioned on a record.
func (it *Iterator) Valid() bool { return it.valid }

// Err returns any error encountered during iteration.
func (it *Iterator) Err() error { return it.err }
