package raft

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Handler is the receiving side of the Raft RPCs. *Node implements it.
type Handler interface {
	HandleRequestVote(*RequestVoteRequest) *RequestVoteResponse
	HandleAppendEntries(*AppendEntriesRequest) *AppendEntriesResponse
}

// ErrUnreachable is returned by MemNetwork when a message is dropped.
var ErrUnreachable = errors.New("raft: peer unreachable")

// MemNetwork is an in-process network for tests and demos. It can disconnect
// nodes, split the cluster into partitions, drop messages at random and add
// random latency, which is how the test suite exercises Raft under failure.
type MemNetwork struct {
	mu       sync.Mutex
	handlers map[string]Handler
	down     map[string]bool
	group    map[string]int // partition id per node; nodes talk only within a group
	dropRate float64
	maxDelay time.Duration
	rng      *rand.Rand
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{
		handlers: map[string]Handler{},
		down:     map[string]bool{},
		group:    map[string]int{},
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Register attaches (or replaces, after a restart) the handler for id.
func (nw *MemNetwork) Register(id string, h Handler) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.handlers[id] = h
}

// Disconnect isolates id from everyone; Connect undoes it.
func (nw *MemNetwork) Disconnect(id string) { nw.set(func() { nw.down[id] = true }) }
func (nw *MemNetwork) Connect(id string)    { nw.set(func() { delete(nw.down, id) }) }

// Partition splits the cluster: nodes can only reach nodes in the same group.
func (nw *MemNetwork) Partition(groups ...[]string) {
	nw.set(func() {
		nw.group = map[string]int{}
		for g, ids := range groups {
			for _, id := range ids {
				nw.group[id] = g + 1
			}
		}
	})
}

// Heal removes all partitions and disconnections.
func (nw *MemNetwork) Heal() {
	nw.set(func() {
		nw.group = map[string]int{}
		nw.down = map[string]bool{}
	})
}

// SetUnreliable makes the network drop a fraction of messages and delay
// the rest by up to maxDelay.
func (nw *MemNetwork) SetUnreliable(dropRate float64, maxDelay time.Duration) {
	nw.set(func() { nw.dropRate, nw.maxDelay = dropRate, maxDelay })
}

func (nw *MemNetwork) set(f func()) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	f()
}

func (nw *MemNetwork) reachable(from, to string) bool {
	return !nw.down[from] && !nw.down[to] && nw.group[from] == nw.group[to]
}

// deliver decides the fate of one message: delivered (after a delay) or not.
func (nw *MemNetwork) deliver(ctx context.Context, from, to string) (Handler, error) {
	nw.mu.Lock()
	h, ok := nw.handlers[to]
	okLink := ok && nw.reachable(from, to)
	drop := nw.dropRate > 0 && nw.rng.Float64() < nw.dropRate
	var delay time.Duration
	if nw.maxDelay > 0 {
		delay = time.Duration(nw.rng.Int63n(int64(nw.maxDelay)))
	}
	nw.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if !okLink || drop {
		// Behave like a real network: the caller waits for its timeout.
		<-ctx.Done()
		return nil, ErrUnreachable
	}
	return h, nil
}

// Transport returns the Transport that node `from` uses to send.
func (nw *MemNetwork) Transport(from string) Transport { return &memTransport{nw: nw, from: from} }

type memTransport struct {
	nw   *MemNetwork
	from string
}

// clone deep-copies a message the way a real wire would, so nodes never
// share memory.
func clone[T any](v *T) *T {
	b, _ := json.Marshal(v)
	out := new(T)
	_ = json.Unmarshal(b, out)
	return out
}

func (t *memTransport) RequestVote(ctx context.Context, peer string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	h, err := t.nw.deliver(ctx, t.from, peer)
	if err != nil {
		return nil, err
	}
	resp := h.HandleRequestVote(clone(req))
	if _, err := t.nw.deliver(ctx, peer, t.from); err != nil {
		return nil, err
	}
	return clone(resp), nil
}

func (t *memTransport) AppendEntries(ctx context.Context, peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	h, err := t.nw.deliver(ctx, t.from, peer)
	if err != nil {
		return nil, err
	}
	resp := h.HandleAppendEntries(clone(req))
	if _, err := t.nw.deliver(ctx, peer, t.from); err != nil {
		return nil, err
	}
	return clone(resp), nil
}
