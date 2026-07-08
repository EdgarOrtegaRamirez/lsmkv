// Package coding defines the on-disk record format shared by SSTables and the
// write-ahead log. A record encodes a single key/value mutation tagged with a
// monotonically increasing sequence number and a type byte distinguishing puts
// from tombstones.
//
// Record layout (all integers big-endian varint unless noted):
//
//	key_len   varint
//	key       [key_len]bytes
//	value_len varint
//	value     [value_len]bytes
//	seq       varint
//	type      1 byte   (0 = put, 1 = delete)
package coding

import (
	"encoding/binary"
	"errors"
)

// TypePut and TypeDelete tag records as inserts or tombstones.
const (
	TypePut    byte = 0
	TypeDelete byte = 1
)

// ErrCorrupt is returned when a record cannot be decoded.
var ErrCorrupt = errors.New("coding: corrupt record")

// EncodeRecord writes a single record to dst and returns the appended slice.
func EncodeRecord(dst, key, value []byte, seq uint64, rtype byte) []byte {
	dst = appendVarint(dst, uint64(len(key)))
	dst = append(dst, key...)
	dst = appendVarint(dst, uint64(len(value)))
	dst = append(dst, value...)
	dst = appendVarint(dst, seq)
	dst = append(dst, rtype)
	return dst
}

// Record holds a decoded record's fields.
type Record struct {
	Key   []byte
	Value []byte
	Seq   uint64
	Type  byte
}

// DecodeRecord parses one record from src starting at offset and returns the
// decoded record and the number of bytes consumed.
func DecodeRecord(src []byte, off int) (Record, int, error) {
	start := off
	klen, n, err := readVarint(src, off)
	if err != nil {
		return Record{}, 0, err
	}
	off += n
	if off+int(klen) > len(src) {
		return Record{}, 0, ErrCorrupt
	}
	key := src[off : off+int(klen)]
	off += int(klen)

	vlen, n, err := readVarint(src, off)
	if err != nil {
		return Record{}, 0, err
	}
	off += n
	if off+int(vlen) > len(src) {
		return Record{}, 0, ErrCorrupt
	}
	val := src[off : off+int(vlen)]
	off += int(vlen)

	seq, n, err := readVarint(src, off)
	if err != nil {
		return Record{}, 0, err
	}
	off += n
	if off+1 > len(src) {
		return Record{}, 0, ErrCorrupt
	}
	rtype := src[off]
	off++

	return Record{Key: key, Value: val, Seq: seq, Type: rtype}, off - start, nil
}

// RecordSize returns the encoded size of a record without allocating.
func RecordSize(key, value []byte, seq uint64) int {
	return varintLen(uint64(len(key))) + len(key) +
		varintLen(uint64(len(value))) + len(value) +
		varintLen(seq) + 1
}

func appendVarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func readVarint(src []byte, off int) (uint64, int, error) {
	if off >= len(src) {
		return 0, 0, ErrCorrupt
	}
	v, n := binary.Uvarint(src[off:])
	if n <= 0 {
		return 0, 0, ErrCorrupt
	}
	return v, n, nil
}

func varintLen(v uint64) int {
	n := 0
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n + 1
}
