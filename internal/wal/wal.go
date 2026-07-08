// Package wal implements an append-only write-ahead log. Every mutation is
// appended to the WAL and optionally fsynced before being applied to the
// in-memory memtable, so that a crash leaves a replayable record of all
// acknowledged writes.
//
// Each record is framed for crash safety:
//
//	length    varint   (number of bytes in the record body)
//	body      [length]bytes   coding.EncodeRecord payload
//	crc32     4 bytes (IEEE CRC-32 of body)
//
// On replay a truncated or checksum-mismatched trailing record is treated as a
// torn write from a crash and ignored; all earlier records are returned.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/EdgarOrtegaRamirez/lsmkv/internal/coding"
)

// Writer appends framed records to a WAL file.
type Writer struct {
	f *os.File
	w *bufio.Writer
}

// NewWriter creates or truncates a WAL at path.
func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, w: bufio.NewWriterSize(f, 64*1024)}, nil
}

// Append writes a framed record. It does not fsync; call Sync to flush to disk.
func (w *Writer) Append(key, value []byte, seq uint64, rtype byte) error {
	body := coding.EncodeRecord(nil, key, value, seq, rtype)
	var frame []byte
	frame = binary.AppendUvarint(frame, uint64(len(body)))
	frame = append(frame, body...)
	frame = binary.BigEndian.AppendUint32(frame, crc32.ChecksumIEEE(body))
	if _, err := w.w.Write(frame); err != nil {
		return err
	}
	return nil
}

// Sync flushes buffered data and fsyncs the underlying file.
func (w *Writer) Sync() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close flushes and closes the WAL.
func (w *Writer) Close() error {
	if err := w.w.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// Record holds a decoded WAL entry.
type Record = coding.Record

// ErrShortRead is returned by Replay only for internal signalling; Replay
// swallows it for the trailing torn record.
var ErrShortRead = errors.New("wal: short read")

// Replay reads all complete, checksum-valid records from the WAL at path. A
// trailing torn record (truncated or bad CRC) is ignored, modelling recovery
// from a crash that interrupted a write.
func Replay(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 64*1024)
	var out []Record
	for {
		lenVal, err := binary.ReadUvarint(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Truncated length prefix: torn write, stop.
			break
		}
		if lenVal == 0 || lenVal > 1<<28 {
			// Implausibly large frame: corruption, stop.
			break
		}
		body := make([]byte, lenVal)
		if _, err := io.ReadFull(r, body); err != nil {
			break // torn write
		}
		var crc [4]byte
		if _, err := io.ReadFull(r, crc[:]); err != nil {
			break // torn write
		}
		if binary.BigEndian.Uint32(crc[:]) != crc32.ChecksumIEEE(body) {
			break // checksum mismatch: torn write
		}
		rec, _, err := coding.DecodeRecord(body, 0)
		if err != nil {
			break
		}
		out = append(out, rec)
	}
	return out, nil
}
