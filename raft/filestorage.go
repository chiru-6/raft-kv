package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage persists Raft state in a directory:
//
//	state.json  HardState, replaced atomically (write temp file, fsync, rename)
//	log.dat     append-only log of checksummed records
//
// Each log record is:
//
//	crc32 (4) | length (4) | index (8) | term (8) | command (length-16)
//
// The CRC covers everything after itself. On startup a torn or corrupt tail
// (e.g. power loss mid-write) is detected and cut off; every record before it
// was fsynced and is kept.
type FileStorage struct {
	mu      sync.Mutex
	dir     string
	sync    bool
	log     *os.File
	offsets []int64 // offsets[i] = file offset of the entry with Index i+1
	size    int64
}

const recordHeader = 4 + 4 + 8 + 8

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// OpenFileStorage opens (or creates) storage in dir. With fsync=false, Sync
// is a no-op: faster, but acknowledged entries can be lost on power failure.
// Keep it on outside benchmarks.
func OpenFileStorage(dir string, fsync bool) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "log.dat"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &FileStorage{dir: dir, sync: fsync, log: f}
	if err := s.scan(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// scan rebuilds the offset index and truncates any corrupt tail.
func (s *FileStorage) scan() error {
	if _, err := s.log.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(s.log, 1<<20)
	var off int64
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			break // clean EOF or torn header
		}
		sum := binary.BigEndian.Uint32(hdr[0:4])
		n := binary.BigEndian.Uint32(hdr[4:8])
		if n < 16 || n > 1<<30 {
			break
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			break
		}
		h := crc32.New(crcTable)
		h.Write(hdr[4:8])
		h.Write(body)
		if h.Sum32() != sum {
			break
		}
		if idx := binary.BigEndian.Uint64(body[0:8]); idx != uint64(len(s.offsets))+1 {
			break
		}
		s.offsets = append(s.offsets, off)
		off += int64(8 + n)
	}
	s.size = off
	if err := s.log.Truncate(off); err != nil {
		return err
	}
	_, err := s.log.Seek(off, io.SeekStart)
	return err
}

func (s *FileStorage) Load() (HardState, []LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var hs HardState
	b, err := os.ReadFile(filepath.Join(s.dir, "state.json"))
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return hs, nil, err
	default:
		if err := json.Unmarshal(b, &hs); err != nil {
			return hs, nil, fmt.Errorf("raft: corrupt state.json: %w", err)
		}
	}

	entries := make([]LogEntry, 0, len(s.offsets))
	if _, err := s.log.Seek(0, io.SeekStart); err != nil {
		return hs, nil, err
	}
	r := bufio.NewReaderSize(io.LimitReader(s.log, s.size), 1<<20)
	hdr := make([]byte, 8)
	for range s.offsets {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return hs, nil, err
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr[4:8]))
		if _, err := io.ReadFull(r, body); err != nil {
			return hs, nil, err
		}
		e := LogEntry{
			Index: binary.BigEndian.Uint64(body[0:8]),
			Term:  binary.BigEndian.Uint64(body[8:16]),
		}
		if len(body) > 16 {
			e.Command = body[16:]
		}
		entries = append(entries, e)
	}
	if _, err := s.log.Seek(s.size, io.SeekStart); err != nil {
		return hs, nil, err
	}
	return hs, entries, nil
}

func (s *FileStorage) SetHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, "state.json.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if s.sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, "state.json")); err != nil {
		return err
	}
	if s.sync {
		return syncDir(s.dir)
	}
	return nil
}

func (s *FileStorage) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var buf []byte
	offsets := make([]int64, 0, len(entries))
	off := s.size
	for i, e := range entries {
		if want := uint64(len(s.offsets)+i) + 1; e.Index != want {
			return fmt.Errorf("raft: non-contiguous append: got index %d, want %d", e.Index, want)
		}
		n := 16 + len(e.Command)
		rec := make([]byte, 8+n)
		binary.BigEndian.PutUint32(rec[4:8], uint32(n))
		binary.BigEndian.PutUint64(rec[8:16], e.Index)
		binary.BigEndian.PutUint64(rec[16:24], e.Term)
		copy(rec[24:], e.Command)
		binary.BigEndian.PutUint32(rec[0:4], crc32.Checksum(rec[4:], crcTable))
		buf = append(buf, rec...)
		offsets = append(offsets, off)
		off += int64(len(rec))
	}
	if _, err := s.log.Write(buf); err != nil {
		return err
	}
	s.offsets = append(s.offsets, offsets...)
	s.size = off
	return nil
}

func (s *FileStorage) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index == 0 {
		return fmt.Errorf("raft: cannot truncate index 0")
	}
	if index > uint64(len(s.offsets)) {
		return nil
	}
	off := s.offsets[index-1]
	if err := s.log.Truncate(off); err != nil {
		return err
	}
	if _, err := s.log.Seek(off, io.SeekStart); err != nil {
		return err
	}
	s.offsets = s.offsets[:index-1]
	s.size = off
	return nil
}

// Sync fsyncs the log. It does not hold the storage lock while flushing, so
// the leader can keep appending new entries during a slow fsync; that is what
// lets many proposals share one fsync (group commit).
func (s *FileStorage) Sync() error {
	if !s.sync {
		return nil
	}
	s.mu.Lock()
	f := s.log
	s.mu.Unlock()
	return f.Sync()
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
