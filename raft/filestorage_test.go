package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func entries(from, to, term uint64) []LogEntry {
	var out []LogEntry
	for i := from; i <= to; i++ {
		out = append(out, LogEntry{Index: i, Term: term, Command: []byte(fmt.Sprintf("cmd-%d", i))})
	}
	return out
}

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	hs := HardState{Term: 7, VotedFor: "n2"}
	if err := s.SetHardState(hs); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entries(1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.TruncateFrom(6); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entries(6, 8, 2)); err != nil {
		t.Fatal(err)
	}
	// A leader no-op has an empty command.
	if err := s.Append([]LogEntry{{Index: 9, Term: 3}}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s2, err := OpenFileStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	gotHS, got, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	want := append(append(entries(1, 5, 1), entries(6, 8, 2)...), LogEntry{Index: 9, Term: 3})
	if gotHS != hs {
		t.Fatalf("hard state = %+v, want %+v", gotHS, hs)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entries mismatch:\n got %+v\nwant %+v", got, want)
	}
	// Appending after reopen continues where the log left off.
	if err := s2.Append(entries(10, 10, 3)); err != nil {
		t.Fatal(err)
	}
}

func TestFileStorageRecoversFromTornWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStorage(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(entries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Simulate a crash halfway through writing entry 6, then a flipped bit
	// would be just as bad: chop the last record in half.
	path := filepath.Join(dir, "log.dat")
	fi, _ := os.Stat(path)
	if err := os.Truncate(path, fi.Size()-5); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenFileStorage(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	_, got, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, entries(1, 4, 1)) {
		t.Fatalf("after torn write got %d entries, want the 4 intact ones", len(got))
	}
	if err := s2.Append(entries(5, 6, 2)); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
}

func TestFileStorageDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenFileStorage(dir, false)
	s.Append(entries(1, 3, 1))
	s.Close()

	path := filepath.Join(dir, "log.dat")
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xff // flip a bit in the last record
	os.WriteFile(path, b, 0o644)

	s2, _ := OpenFileStorage(dir, false)
	defer s2.Close()
	_, got, _ := s2.Load()
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2 (corrupt record dropped)", len(got))
	}
}

// A node backed by FileStorage keeps its log across a restart.
func TestNodeWithFileStorage(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Node, <-chan ApplyMsg, *FileStorage) {
		fs, err := OpenFileStorage(dir, true)
		if err != nil {
			t.Fatal(err)
		}
		n, ch, err := NewNode(Config{ID: "solo", Storage: fs, ElectionTimeoutMin: 50e6})
		if err != nil {
			t.Fatal(err)
		}
		return n, ch, fs
	}
	n, ch, fs := open()
	var last uint64
	for i := 0; i < 5; {
		if idx, _, ok, _ := n.Propose([]byte(fmt.Sprint(i))); ok {
			last = idx
			i++
		}
	}
	for m := range ch {
		if m.Index == last {
			break
		}
	}
	n.Stop()
	fs.Close()

	n2, ch2, fs2 := open()
	defer func() { n2.Stop(); fs2.Close() }()
	var cmds []string
	for m := range ch2 {
		if len(m.Command) > 0 {
			cmds = append(cmds, string(m.Command))
		}
		if m.Index == last {
			break
		}
	}
	if !reflect.DeepEqual(cmds, []string{"0", "1", "2", "3", "4"}) {
		t.Fatalf("replayed %v", cmds)
	}
}
