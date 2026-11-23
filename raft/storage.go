package raft

import (
	"fmt"
	"sync"
)

// Storage persists a node's HardState and log. Raft calls it while holding its
// lock and before replying to any RPC that depends on the change, which is
// what makes the algorithm safe across crashes.
type Storage interface {
	// Load returns the persisted state. Entries start at index 1 and are contiguous.
	Load() (HardState, []LogEntry, error)
	SetHardState(HardState) error
	// Append adds entries that directly follow the current last entry. The
	// write need not be durable until Sync returns.
	Append(entries []LogEntry) error
	// TruncateFrom removes every entry with Index >= index.
	TruncateFrom(index uint64) error
	// Sync makes every previous Append and TruncateFrom durable. It may run
	// concurrently with Append; it covers at least the writes that completed
	// before it was called.
	Sync() error
	Close() error
}

// MemoryStorage keeps everything in memory. Reusing the same MemoryStorage
// for a new Node simulates a crash and restart with intact disk state.
type MemoryStorage struct {
	mu      sync.Mutex
	hs      HardState
	entries []LogEntry
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) Load() (HardState, []LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, append([]LogEntry(nil), m.entries...), nil
}

func (m *MemoryStorage) SetHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	return nil
}

func (m *MemoryStorage) Append(entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range entries {
		if want := uint64(len(m.entries)) + 1; e.Index != want {
			return fmt.Errorf("raft: non-contiguous append: got index %d, want %d", e.Index, want)
		}
		m.entries = append(m.entries, e)
	}
	return nil
}

func (m *MemoryStorage) TruncateFrom(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index == 0 {
		return fmt.Errorf("raft: cannot truncate index 0")
	}
	if index-1 < uint64(len(m.entries)) {
		m.entries = m.entries[:index-1]
	}
	return nil
}

func (m *MemoryStorage) Sync() error  { return nil }
func (m *MemoryStorage) Close() error { return nil }
