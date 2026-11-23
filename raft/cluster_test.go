package raft

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testCluster runs N nodes on a MemNetwork and checks the core safety
// property on every apply: no two nodes ever apply different commands at the
// same index.
type testCluster struct {
	t        *testing.T
	net      *MemNetwork
	ids      []string
	preVote  bool
	mu       sync.Mutex
	nodes    map[string]*Node
	storages map[string]*MemoryStorage
	applied  map[string][]ApplyMsg // per node, in order, since its last start
	global   map[uint64][]byte     // index -> command, across all nodes
	failure  error
	wg       sync.WaitGroup
}

func newTestCluster(t *testing.T, n int, preVote bool) *testCluster {
	t.Helper()
	c := &testCluster{
		t: t, net: NewMemNetwork(), preVote: preVote,
		nodes: map[string]*Node{}, storages: map[string]*MemoryStorage{},
		applied: map[string][]ApplyMsg{}, global: map[uint64][]byte{},
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.storages[id] = NewMemoryStorage()
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *testCluster) peersOf(id string) []string {
	var out []string
	for _, p := range c.ids {
		if p != id {
			out = append(out, p)
		}
	}
	return out
}

func (c *testCluster) start(id string) {
	node, applyCh, err := NewNode(Config{
		ID:                 id,
		Peers:              c.peersOf(id),
		Transport:          c.net.Transport(id),
		Storage:            c.storages[id],
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  30 * time.Millisecond,
		RPCTimeout:         80 * time.Millisecond,
		DisablePreVote:     !c.preVote,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[id] = node
	c.applied[id] = nil
	c.mu.Unlock()
	c.net.Register(id, node)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for msg := range applyCh {
			c.recordApply(id, msg)
		}
	}()
}

func (c *testCluster) recordApply(id string, msg ApplyMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if want := uint64(len(c.applied[id])) + 1; msg.Index != want && c.failure == nil {
		c.failure = fmt.Errorf("%s applied index %d out of order (want %d)", id, msg.Index, want)
	}
	c.applied[id] = append(c.applied[id], msg)
	if prev, ok := c.global[msg.Index]; ok {
		if !bytes.Equal(prev, msg.Command) && c.failure == nil {
			c.failure = fmt.Errorf("SAFETY VIOLATION: index %d applied as %q and %q", msg.Index, prev, msg.Command)
		}
	} else {
		c.global[msg.Index] = msg.Command
	}
}

// crash stops a node and cuts it off. Its storage survives.
func (c *testCluster) crash(id string) {
	c.net.Disconnect(id)
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n != nil {
		n.Stop()
	}
}

func (c *testCluster) restart(id string) {
	c.crash(id)
	c.start(id)
	c.net.Connect(id)
}

func (c *testCluster) shutdown() {
	for _, id := range c.ids {
		c.crash(id)
	}
	c.wg.Wait()
	c.checkSafety()
}

func (c *testCluster) checkSafety() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		c.t.Fatal(c.failure)
	}
}

func (c *testCluster) node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// waitLeader waits until exactly one leader exists among `among` (default all live nodes).
func (c *testCluster) waitLeader(among ...string) string {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		leadersByTerm := map[uint64][]string{}
		var top uint64
		for _, id := range among {
			if n := c.node(id); n != nil {
				if s := n.Status(); s.State == "leader" {
					leadersByTerm[s.Term] = append(leadersByTerm[s.Term], id)
					if s.Term > top {
						top = s.Term
					}
				}
			}
		}
		for term, ls := range leadersByTerm {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
		}
		if len(leadersByTerm) > 0 {
			return leadersByTerm[top][0]
		}
	}
	c.t.Fatalf("no leader elected among %v", among)
	return ""
}

func (c *testCluster) noLeader(among ...string) {
	c.t.Helper()
	for _, id := range among {
		if n := c.node(id); n != nil && n.Status().State == "leader" {
			c.t.Fatalf("%s is leader but should not be", id)
		}
	}
}

// committedOn reports how many nodes have applied cmd at index.
func (c *testCluster) committedOn(index uint64, cmd []byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, msgs := range c.applied {
		if uint64(len(msgs)) >= index && bytes.Equal(msgs[index-1].Command, cmd) {
			count++
		}
	}
	return count
}

// one submits cmd via whichever node is leader and waits until at least
// `expected` nodes have applied it. Returns the index.
func (c *testCluster) one(cmd string, expected int) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			n := c.node(id)
			if n == nil {
				continue
			}
			idx, _, ok, err := n.Propose([]byte(cmd))
			if err != nil || !ok {
				continue
			}
			wait := time.Now().Add(2 * time.Second)
			for time.Now().Before(wait) {
				if c.committedOn(idx, []byte(cmd)) >= expected {
					return idx
				}
				time.Sleep(20 * time.Millisecond)
			}
			// Not committed: this "leader" may be stale (e.g. stuck in a
			// minority partition). Try the other nodes too.
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("command %q never reached %d nodes", cmd, expected)
	return 0
}

func (c *testCluster) term(id string) uint64 { return c.node(id).Status().Term }
