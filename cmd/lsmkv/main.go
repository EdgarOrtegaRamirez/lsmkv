// Command lsmkv is a CLI for the LSM-tree key-value store.
//
// Usage:
//
//	lsmkv put    <db> <key> <value>   store a key/value pair
//	lsmkv get    <db> <key>           retrieve a value
//	lsmkv delete <db> <key>           delete a key
//	lsmkv scan   <db> [start] [limit] scan keys in ascending order
//	lsmkv stats  <db>                 show engine statistics
//	lsmkv flush  <db>                 flush the memtable to an SSTable
//	lsmkv compact <db>                fully compact all SSTables
//	lsmkv version                     print version information
//
// Values are read from stdin when <value> is "-" (useful for binary data).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/EdgarOrtegaRamirez/lsmkv/internal/engine"
)

const version = "0.1.0"

func usage() {
	fmt.Fprintln(os.Stderr, `lsmkv — log-structured merge-tree key-value store

Usage:
  lsmkv put     <db> <key> <value>   store a key/value pair
  lsmkv get     <db> <key>           retrieve a value
  lsmkv delete  <db> <key>           delete a key
  lsmkv scan    <db> [start] [limit] scan keys in ascending order
  lsmkv stats   <db>                 show engine statistics
  lsmkv flush   <db>                 flush the memtable to an SSTable
  lsmkv compact <db>                fully compact all SSTables
  lsmkv version                      print version information

Values are read from stdin when <value> is "-".`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "version":
		fmt.Printf("lsmkv %s\n", version)
	case "put":
		cmdPut(args)
	case "get":
		cmdGet(args)
	case "delete":
		cmdDelete(args)
	case "scan":
		cmdScan(args)
	case "stats":
		cmdStats(args)
	case "flush":
		cmdFlush(args)
	case "compact":
		cmdCompact(args)
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
}

func mustOpen(db string) *engine.Engine {
	eng, err := engine.Open(db, engine.DefaultOptions())
	if err != nil {
		fatal("open %s: %v", db, err)
	}
	return eng
}

func cmdPut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, "usage: lsmkv put <db> <key> <value>"); fs.PrintDefaults() }
	_ = fs.Parse(args)
	if fs.NArg() < 3 {
		fs.Usage()
		os.Exit(2)
	}
	db := fs.Arg(0)
	key := fs.Arg(1)
	value := fs.Arg(2)
	if value == "-" {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			fatal("read stdin: %v", err)
		}
		value = string(b)
	}
	eng := mustOpen(db)
	defer eng.Close()
	if err := eng.Put([]byte(key), []byte(value)); err != nil {
		fatal("put: %v", err)
	}
}

func cmdGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, "usage: lsmkv get <db> <key>"); fs.PrintDefaults() }
	_ = fs.Parse(args)
	if fs.NArg() < 2 {
		fs.Usage()
		os.Exit(2)
	}
	db := fs.Arg(0)
	key := fs.Arg(1)
	eng := mustOpen(db)
	defer eng.Close()
	val, err := eng.Get([]byte(key))
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			os.Exit(1)
		}
		fatal("get: %v", err)
	}
	if _, err := os.Stdout.Write(val); err != nil {
		fatal("write: %v", err)
	}
	fmt.Println()
}

func cmdDelete(args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, "usage: lsmkv delete <db> <key>"); fs.PrintDefaults() }
	_ = fs.Parse(args)
	if fs.NArg() < 2 {
		fs.Usage()
		os.Exit(2)
	}
	db := fs.Arg(0)
	key := fs.Arg(1)
	eng := mustOpen(db)
	defer eng.Close()
	if err := eng.Delete([]byte(key)); err != nil {
		fatal("delete: %v", err)
	}
}

func cmdScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, "usage: lsmkv scan <db> [start] [limit]"); fs.PrintDefaults() }
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(2)
	}
	db := fs.Arg(0)
	var start []byte
	if fs.NArg() >= 2 {
		start = []byte(fs.Arg(1))
	}
	limit := 0
	if fs.NArg() >= 3 {
		n, err := strconv.Atoi(fs.Arg(2))
		if err != nil {
			fatal("limit: %v", err)
		}
		limit = n
	}
	eng := mustOpen(db)
	defer eng.Close()
	pairs, err := eng.Scan(start, limit)
	if err != nil {
		fatal("scan: %v", err)
	}
	for _, kv := range pairs {
		fmt.Printf("%s\t%s\n", kv.Key, kv.Value)
	}
}

func cmdStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, "usage: lsmkv stats <db>"); fs.PrintDefaults() }
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(2)
	}
	db := fs.Arg(0)
	eng := mustOpen(db)
	defer eng.Close()
	s := eng.Stats()
	fmt.Printf("memtable bytes: %d\n", s.MemtableBytes)
	fmt.Printf("memtable entries: %d\n", s.MemtableCount)
	fmt.Printf("immutable pending: %t\n", s.HasImmutable)
	fmt.Printf("sstable readers: %d\n", s.SSTableCount)
	fmt.Printf("sstable files: %d\n", s.SSTableFiles)
	fmt.Printf("wal files: %d\n", s.WALFiles)
	fmt.Printf("active wal: %d\n", s.ActiveWAL)
	fmt.Printf("next seq: %d\n", s.NextSeq)
}

func cmdFlush(args []string) {
	fs := flag.NewFlagSet("flush", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, "usage: lsmkv flush <db>"); fs.PrintDefaults() }
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(2)
	}
	db := fs.Arg(0)
	eng := mustOpen(db)
	defer eng.Close()
	if err := eng.Flush(); err != nil {
		fatal("flush: %v", err)
	}
}

func cmdCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprintln(os.Stderr, "usage: lsmkv compact <db>"); fs.PrintDefaults() }
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		fs.Usage()
		os.Exit(2)
	}
	db := fs.Arg(0)
	eng := mustOpen(db)
	defer eng.Close()
	if err := eng.Compact(); err != nil {
		fatal("compact: %v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "lsmkv: "+format+"\n", args...)
	os.Exit(1)
}
