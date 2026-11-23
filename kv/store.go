// Package kv is a linearizable key-value state machine replicated with Raft.
//
// Every operation, reads included, goes through the Raft log, so a client
// always sees the effect of every write that completed before its request
// began. Each command carries (ClientID, Seq), and the store remembers the
// last Seq per client, so a request retried after a timeout or leader change
// is applied at most once.
package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/chiru-6/raft-kv/raft"
)

// Op is the kind of operation.
type Op string

const (
	OpGet    Op = "get"
	OpPut    Op = "put"
	OpDelete Op = "delete"
	OpCAS    Op = "cas" // compare-and-swap
)

// Command is what gets written to the Raft log.
type Command struct {
	Op       Op     `json:"op"`
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	Expected string `json:"expected,omitempty"` // CAS: required current value
	// ExpectAbsent makes a CAS succeed only if the key does not exist.
	ExpectAbsent bool   `json:"expectAbsent,omitempty"`
	ClientID     string `json:"clientId,omitempty"`
	Seq          uint64 `json:"seq,omitempty"`
}

// Result is what an operation returns.
type Result struct {
	Value string `json:"value,omitempty"`
	Found bool   `json:"found"`
	// OK is false when a CAS did not match.
	OK bool `json:"ok"`
}

var (
	// ErrLeadershipLost means the entry was overwritten by a new leader; retry.
	ErrLeadershipLost = errors.New("kv: leadership lost before commit")
)

// NotLeaderError is returned when a request reaches a follower.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "kv: not leader (leader unknown)"
	}
	return "kv: not leader; leader is " + e.Leader
}

type applied struct {
	term   uint64
	result Result
}

// Store applies committed commands to an in-memory map.
type Store struct {
	node *raft.Node

	mu        sync.Mutex
	data      map[string]string
	lastSeq   map[string]uint64 // clientID -> highest applied seq
	lastRes   map[string]Result // clientID -> result of that seq
	waiters   map[uint64]chan applied
	lastIndex uint64
	done      chan struct{}
}

// NewStore starts applying entries from applyCh. node is used to propose.
func NewStore(node *raft.Node, applyCh <-chan raft.ApplyMsg) *Store {
	s := &Store{
		node:    node,
		data:    map[string]string{},
		lastSeq: map[string]uint64{},
		lastRes: map[string]Result{},
		waiters: map[uint64]chan applied{},
		done:    make(chan struct{}),
	}
	go s.run(applyCh)
	return s
}

// Done is closed when the apply channel closes (the node stopped).
func (s *Store) Done() <-chan struct{} { return s.done }

// Node returns the underlying Raft node.
func (s *Store) Node() *raft.Node { return s.node }

// Execute replicates cmd and returns its result once it is applied.
func (s *Store) Execute(ctx context.Context, cmd Command) (Result, error) {
	switch cmd.Op {
	case OpGet, OpPut, OpDelete, OpCAS:
	default:
		return Result{}, fmt.Errorf("kv: unknown op %q", cmd.Op)
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return Result{}, err
	}

	// Holding s.mu across Propose guarantees the waiter is registered before
	// the apply loop (which also takes s.mu) can see the entry.
	s.mu.Lock()
	index, term, isLeader, err := s.node.Propose(b)
	if err != nil || !isLeader {
		s.mu.Unlock()
		if err != nil {
			return Result{}, err
		}
		return Result{}, &NotLeaderError{Leader: s.node.Leader()}
	}
	ch := make(chan applied, 1)
	s.waiters[index] = ch
	s.mu.Unlock()

	select {
	case a := <-ch:
		if a.term != term {
			return Result{}, ErrLeadershipLost
		}
		return a.result, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return Result{}, ctx.Err()
	case <-s.done:
		return Result{}, raft.ErrStopped
	}
}

// StaleGet reads this replica's local copy without going through Raft. It is
// fast but may return data that is out of date.
func (s *Store) StaleGet(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	return v, ok
}

// Len returns the number of keys on this replica.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

func (s *Store) run(applyCh <-chan raft.ApplyMsg) {
	defer close(s.done)
	for msg := range applyCh {
		s.mu.Lock()
		res := s.apply(msg.Command)
		s.lastIndex = msg.Index
		if ch, ok := s.waiters[msg.Index]; ok {
			ch <- applied{term: msg.Term, result: res}
			delete(s.waiters, msg.Index)
		}
		s.mu.Unlock()
	}
}

// apply runs one command against the map. Called with s.mu held.
func (s *Store) apply(raw []byte) Result {
	if len(raw) == 0 {
		return Result{} // leader no-op
	}
	var cmd Command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		// A corrupt command must be skipped identically on every replica.
		return Result{}
	}
	if cmd.ClientID != "" && cmd.Op != OpGet {
		if cmd.Seq <= s.lastSeq[cmd.ClientID] {
			return s.lastRes[cmd.ClientID] // duplicate: don't apply twice
		}
	}

	var res Result
	switch cmd.Op {
	case OpGet:
		res.Value, res.Found = s.data[cmd.Key]
		res.OK = true
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		res.OK = true
	case OpDelete:
		_, res.Found = s.data[cmd.Key]
		delete(s.data, cmd.Key)
		res.OK = true
	case OpCAS:
		cur, exists := s.data[cmd.Key]
		res.Value, res.Found = cur, exists
		if (cmd.ExpectAbsent && !exists) || (!cmd.ExpectAbsent && exists && cur == cmd.Expected) {
			s.data[cmd.Key] = cmd.Value
			res.OK = true
		}
	default:
		return Result{} // rejected by Execute; ignore identically everywhere
	}

	if cmd.ClientID != "" && cmd.Op != OpGet {
		s.lastSeq[cmd.ClientID] = cmd.Seq
		s.lastRes[cmd.ClientID] = res
	}
	return res
}
