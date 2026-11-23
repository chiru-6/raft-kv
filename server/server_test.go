package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chiru-6/raft-kv/client"
	"github.com/chiru-6/raft-kv/server"
)

type testCluster struct {
	t     *testing.T
	ids   []string
	peers map[string]string
	addrs []string
	dir   string
	mu    sync.Mutex
	nodes map[string]*server.Node
}

func startCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, peers: map[string]string{}, nodes: map[string]*server.Node{}, dir: t.TempDir()}
	lns := map[string]net.Listener{}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c.ids = append(c.ids, id)
		lns[id] = ln
		c.peers[id] = "http://" + ln.Addr().String()
		c.addrs = append(c.addrs, ln.Addr().String())
	}
	for _, id := range c.ids {
		c.start(id, lns[id])
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) start(id string, ln net.Listener) {
	c.t.Helper()
	if ln == nil {
		var err error
		// Rebind the same port after a restart.
		for i := 0; i < 50; i++ {
			ln, err = net.Listen("tcp", c.peers[id][len("http://"):])
			if err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			c.t.Fatal(err)
		}
	}
	node, err := server.StartNode(server.NodeConfig{
		ID: id, Peers: c.peers, Listener: ln,
		DataDir: filepath.Join(c.dir, id), Fsync: false,
		ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[id] = node
	c.mu.Unlock()
}

func (c *testCluster) stop(id string) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n != nil {
		n.Close()
	}
}

func (c *testCluster) leader() string {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for id, n := range c.nodes {
			if n.Raft.Status().State == "leader" {
				c.mu.Unlock()
				return id
			}
		}
		c.mu.Unlock()
		time.Sleep(25 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return ""
}

func ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCRUDAndRedirect(t *testing.T) {
	c := startCluster(t, 3)
	leader := c.leader()

	// Point the client at a follower only: it must be redirected.
	var follower string
	for i, id := range c.ids {
		if id != leader {
			follower = c.addrs[i]
			break
		}
	}
	cl := client.New([]string{follower})
	ctx := ctx(t)

	if err := cl.Put(ctx, "users/42/name", "Chirandan"); err != nil {
		t.Fatal(err)
	}
	v, ok, err := cl.Get(ctx, "users/42/name")
	if err != nil || !ok || v != "Chirandan" {
		t.Fatalf("get = %q %v %v", v, ok, err)
	}
	if _, ok, _ := cl.Get(ctx, "missing"); ok {
		t.Fatal("missing key found")
	}

	swapped, cur, _, err := cl.CAS(ctx, "users/42/name", "wrong", false, "x")
	if err != nil || swapped || cur != "Chirandan" {
		t.Fatalf("CAS with wrong expected: swapped=%v cur=%q err=%v", swapped, cur, err)
	}
	swapped, _, _, err = cl.CAS(ctx, "users/42/name", "Chirandan", false, "Chiru")
	if err != nil || !swapped {
		t.Fatalf("CAS should succeed: %v %v", swapped, err)
	}
	swapped, _, _, _ = cl.CAS(ctx, "lock", "", true, "owner-1")
	again, _, _, _ := cl.CAS(ctx, "lock", "", true, "owner-2")
	if !swapped || again {
		t.Fatalf("expectAbsent CAS: first=%v second=%v", swapped, again)
	}

	existed, err := cl.Delete(ctx, "users/42/name")
	if err != nil || !existed {
		t.Fatalf("delete: %v %v", existed, err)
	}
	if _, ok, _ := cl.Get(ctx, "users/42/name"); ok {
		t.Fatal("key still present after delete")
	}

	// Followers eventually hold the same data (stale read from each node).
	time.Sleep(200 * time.Millisecond)
	for _, a := range c.addrs {
		v, ok, err := client.New([]string{a}).StaleGet(ctx, "lock")
		if err != nil || !ok || v != "owner-1" {
			t.Errorf("stale read on %s: %q %v %v", a, v, ok, err)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := startCluster(t, 3)
	cl := client.New(c.addrs)
	ctx := ctx(t)
	for i := 0; i < 10; i++ {
		if err := cl.Put(ctx, fmt.Sprint("k", i), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	old := c.leader()
	c.stop(old)

	start := time.Now()
	if err := cl.Put(ctx, "after", "failover"); err != nil {
		t.Fatal(err)
	}
	t.Logf("first write after killing leader %s took %v", old, time.Since(start).Round(time.Millisecond))
	for i := 0; i < 10; i++ {
		v, _, err := cl.Get(ctx, fmt.Sprint("k", i))
		if err != nil || v != fmt.Sprint(i) {
			t.Fatalf("k%d = %q, %v", i, v, err)
		}
	}

	// The old leader restarts from disk and catches up.
	c.start(old, nil)
	time.Sleep(500 * time.Millisecond)
	idx := map[string]int{}
	for i, id := range c.ids {
		idx[id] = i
	}
	v, ok, err := client.New([]string{c.addrs[idx[old]]}).StaleGet(ctx, "after")
	if err != nil || !ok || v != "failover" {
		t.Fatalf("restarted node did not catch up: %q %v %v", v, ok, err)
	}
}

// Many clients increment one counter with CAS while the leader is killed and
// restarted. Every successful CAS must be reflected exactly once.
func TestConcurrentCASCounterUnderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	c := startCluster(t, 3)
	ctx := ctx(t)
	if _, _, _, err := client.New(c.addrs).CAS(ctx, "counter", "", true, "0"); err != nil {
		t.Fatal(err)
	}

	var successes atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := client.New(c.addrs)
			for {
				select {
				case <-stop:
					return
				default:
				}
				cur, _, err := cl.Get(ctx, "counter")
				if err != nil {
					continue
				}
				n, _ := strconv.Atoi(cur)
				ok, _, _, err := cl.CAS(ctx, "counter", cur, false, strconv.Itoa(n+1))
				if err == nil && ok {
					successes.Add(1)
				}
			}
		}()
	}

	time.Sleep(700 * time.Millisecond)
	victim := c.leader()
	c.stop(victim)
	time.Sleep(1200 * time.Millisecond)
	c.start(victim, nil)
	time.Sleep(700 * time.Millisecond)
	close(stop)
	wg.Wait()

	v, _, err := client.New(c.addrs).Get(ctx, "counter")
	if err != nil {
		t.Fatal(err)
	}
	if got := int64(mustAtoi(t, v)); got != successes.Load() {
		t.Fatalf("counter = %d but %d CAS operations reported success", got, successes.Load())
	}
	t.Logf("counter reached %d across a leader failure", successes.Load())
}

// The same (client ID, seq) sent twice must only be applied once.
func TestRetriedWriteAppliedOnce(t *testing.T) {
	c := startCluster(t, 3)
	leader := c.leader()
	base := c.peers[leader]
	ctx := ctx(t)
	cl := client.New(c.addrs)
	cl.Put(ctx, "n", "0")

	send := func() server.Response {
		body, _ := json.Marshal(server.CASRequest{Expected: "0", Value: "1"})
		req, _ := http.NewRequest(http.MethodPost, base+"/cas/n", bytes.NewReader(body))
		req.Header.Set("X-Client-ID", "retry-test")
		req.Header.Set("X-Seq", "1")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var r server.Response
		json.NewDecoder(resp.Body).Decode(&r)
		return r
	}
	first := send()
	cl.Put(ctx, "n", "0") // someone resets the value in between
	second := send()      // a retry of the first request
	if !first.OK || !second.OK {
		t.Fatalf("retry should report the original result: %+v %+v", first, second)
	}
	if v, _, _ := cl.Get(ctx, "n"); v != "0" {
		t.Fatalf("retried CAS was applied twice: n = %q", v)
	}
}

func TestStatusEndpoint(t *testing.T) {
	c := startCluster(t, 3)
	leader := c.leader()
	time.Sleep(200 * time.Millisecond) // let followers hear a heartbeat
	st, err := client.New(c.addrs).Status(ctx(t), c.addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	if st["id"] != "n1" || st["leaderUrl"] != c.peers[leader] {
		t.Fatalf("unexpected status %v", st)
	}
}

func mustAtoi(t *testing.T, s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
