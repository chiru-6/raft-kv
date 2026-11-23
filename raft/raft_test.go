package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := c.waitLeader()
	term := c.term(leader)

	// With no failures, leadership and term must stay put.
	time.Sleep(time.Second)
	if l := c.waitLeader(); l != leader || c.term(l) != term {
		t.Fatalf("leadership changed without failures: %s/t%d -> %s/t%d", leader, term, l, c.term(l))
	}
	for _, id := range c.ids {
		if got := c.node(id).Leader(); got != leader {
			t.Errorf("%s thinks leader is %q, want %q", id, got, leader)
		}
	}
}

func TestReElection(t *testing.T) {
	c := newTestCluster(t, 3, true)
	l1 := c.waitLeader()

	c.net.Disconnect(l1)
	l2 := c.waitLeader(c.peersOf(l1)...)
	if l2 == l1 {
		t.Fatal("disconnected leader was re-elected")
	}

	// The old leader rejoins and must not disturb the new one.
	c.net.Connect(l1)
	if l := c.waitLeader(); l != l2 {
		t.Fatalf("leader changed after old leader rejoined: %s -> %s", l2, l)
	}

	// Without a quorum nobody can be elected.
	var others []string
	for _, id := range c.ids {
		if id != l2 {
			others = append(others, id)
		}
	}
	c.net.Disconnect(l2)
	c.net.Disconnect(others[0])
	time.Sleep(time.Second)
	c.noLeader(others[1])

	c.net.Connect(others[0])
	c.waitLeader(others...)
	c.net.Connect(l2)
	c.waitLeader()
}

func TestBasicAgreement(t *testing.T) {
	c := newTestCluster(t, 5, true)
	c.waitLeader()
	var last uint64
	for i := 0; i < 20; i++ {
		idx := c.one(fmt.Sprintf("cmd-%d", i), 5)
		if idx <= last {
			t.Fatalf("index went backwards: %d after %d", idx, last)
		}
		last = idx
	}
}

func TestFollowerFailureAndCatchUp(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := c.waitLeader()
	c.one("a", 3)

	follower := c.peersOf(leader)[0]
	c.net.Disconnect(follower)
	for i := 0; i < 50; i++ {
		c.one(fmt.Sprintf("while-away-%d", i), 2)
	}
	c.net.Connect(follower)
	idx := c.one("after-rejoin", 3) // only succeeds once the follower has caught up
	if got := c.node(follower).Status().LastApplied; got < idx {
		t.Fatalf("follower applied %d, want >= %d", got, idx)
	}
}

func TestNoCommitWithoutQuorum(t *testing.T) {
	c := newTestCluster(t, 5, true)
	leader := c.waitLeader()
	c.one("before", 5)

	followers := c.peersOf(leader)
	for _, f := range followers[:3] {
		c.net.Disconnect(f)
	}
	idx, _, ok, _ := c.node(leader).Propose([]byte("lonely"))
	if !ok {
		t.Fatal("leader refused proposal")
	}
	time.Sleep(time.Second)
	if n := c.committedOn(idx, []byte("lonely")); n > 0 {
		t.Fatalf("entry committed on %d nodes without a quorum", n)
	}

	c.net.Heal()
	c.one("after-heal", 5)
}

// A leader in a minority partition accepts entries it can never commit. After
// the partition heals, the majority's history must win everywhere.
func TestDivergentLogsAreRepaired(t *testing.T) {
	c := newTestCluster(t, 5, true)
	old := c.waitLeader()
	c.one("common", 5)

	peers := c.peersOf(old)
	minority := []string{old, peers[0]}
	majority := peers[1:]
	c.net.Partition(minority, majority)
	for i := 0; i < 20; i++ {
		c.node(old).Propose([]byte(fmt.Sprintf("doomed-%d", i)))
	}
	c.waitLeader(majority...)
	for i := 0; i < 20; i++ {
		c.one(fmt.Sprintf("winner-%d", i), 3)
	}

	c.net.Heal()
	idx := c.one("final", 5)
	// Every node now has an identical log up to idx (checked continuously by
	// recordApply); make sure no doomed entry survived anywhere.
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, msgs := range c.applied {
		for _, m := range msgs[:idx] {
			if len(m.Command) >= 6 && string(m.Command[:6]) == "doomed" {
				t.Fatalf("%s applied uncommitted entry %q", id, m.Command)
			}
		}
	}
}

func TestPersistenceAcrossFullRestart(t *testing.T) {
	c := newTestCluster(t, 3, true)
	c.waitLeader()
	for i := 0; i < 10; i++ {
		c.one(fmt.Sprintf("v%d", i), 3)
	}
	terms := map[string]uint64{}
	for _, id := range c.ids {
		terms[id] = c.term(id)
	}
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
		c.net.Connect(id)
		if got := c.term(id); got < terms[id] {
			t.Fatalf("%s forgot its term: %d < %d", id, got, terms[id])
		}
	}
	c.waitLeader()
	// Old entries are replayed (and checked against the pre-crash history),
	// and the cluster keeps making progress.
	c.one("after-restart", 3)
}

// Without Pre-Vote, an isolated follower keeps starting elections, inflating
// its term, and forces the healthy leader to step down when it rejoins.
// With Pre-Vote, none of that happens.
func TestPreVotePreventsDisruption(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		t.Run(fmt.Sprintf("preVote=%v", preVote), func(t *testing.T) {
			c := newTestCluster(t, 3, preVote)
			leader := c.waitLeader()
			c.one("x", 3)
			term := c.term(leader)

			isolated := c.peersOf(leader)[0]
			c.net.Disconnect(isolated)
			time.Sleep(1500 * time.Millisecond)
			isoTerm := c.term(isolated)
			c.net.Connect(isolated)
			time.Sleep(time.Second)
			newLeader := c.waitLeader()
			c.one("y", 3)

			if preVote {
				if isoTerm != term {
					t.Errorf("isolated node's term grew from %d to %d despite Pre-Vote", term, isoTerm)
				}
				if newLeader != leader || c.term(newLeader) != term {
					t.Errorf("healthy leader was disrupted: %s/t%d -> %s/t%d", leader, term, newLeader, c.term(newLeader))
				}
			} else {
				if isoTerm <= term {
					t.Errorf("expected term inflation without Pre-Vote (term %d, isolated %d)", term, isoTerm)
				}
				if c.term(newLeader) <= term {
					t.Errorf("expected rejoin to force a new term without Pre-Vote")
				}
			}
		})
	}
}

// Randomized stress: lossy, slow network, nodes crashing and restarting, and
// clients proposing the whole time. Safety is checked on every apply; at the
// end the cluster must heal and agree.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test is slow")
	}
	c := newTestCluster(t, 5, true)
	c.waitLeader()
	c.net.SetUnreliable(0.1, 15*time.Millisecond)

	stop := make(chan struct{})
	var proposed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				for _, id := range c.ids {
					if n := c.node(id); n != nil {
						if _, _, ok, _ := n.Propose([]byte(fmt.Sprintf("w%d-%d", w, i))); ok {
							proposed.Add(1)
						}
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	rng := rand.New(rand.NewSource(1))
	for round := 0; round < 12; round++ {
		victim := c.ids[rng.Intn(len(c.ids))]
		switch rng.Intn(3) {
		case 0:
			c.restart(victim)
		case 1:
			c.net.Disconnect(victim)
			time.Sleep(200 * time.Millisecond)
			c.net.Connect(victim)
		case 2:
			p := rng.Perm(5)
			c.net.Partition([]string{c.ids[p[0]], c.ids[p[1]]}, []string{c.ids[p[2]], c.ids[p[3]], c.ids[p[4]]})
			time.Sleep(300 * time.Millisecond)
			c.net.Heal()
		}
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	c.net.SetUnreliable(0, 0)
	c.net.Heal()
	c.waitLeader()
	c.one("settled", 5)
	c.checkSafety()
	t.Logf("proposals accepted by a leader: %d, committed log length: %d", proposed.Load(), len(c.global))
}
