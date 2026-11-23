package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ErrStopped is returned by operations on a stopped node.
var ErrStopped = errors.New("raft: node stopped")

// Node is one member of a Raft cluster.
//
// Concurrency model: a single mutex guards all Raft state. Background
// goroutines are
//   - ticker:      fires elections and heartbeats
//   - replicator:  one per peer; sends AppendEntries when signalled
//   - applier:     delivers committed entries to the apply channel
//   - syncer:      fsyncs the leader's new entries in batches (group commit)
//
// RPCs are always sent with the lock released, and every response is checked
// against the term it was sent in, so stale replies are ignored.
type Node struct {
	cfg     Config
	applyCh chan ApplyMsg

	mu          sync.Mutex
	state       State
	currentTerm uint64
	votedFor    string
	log         []LogEntry // log[0] is a sentinel {Index:0, Term:0}
	commitIndex uint64
	lastApplied uint64
	leaderID    string

	// syncedIndex is the last log index known to be durable on this node.
	// The leader counts itself toward a quorum only up to syncedIndex, so it
	// can append without waiting for fsync and let one fsync cover many
	// proposals. truncGen invalidates an fsync that raced with a truncation.
	syncedIndex uint64
	truncGen    uint64
	syncCh      chan struct{}

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionDeadline  time.Time
	lastLeaderContact time.Time // last time we heard from a valid leader
	lastHeartbeat     time.Time
	electionGen       uint64 // bumped to cancel in-flight elections

	applyCond   *sync.Cond
	replicateCh map[string]chan struct{}
	stopCh      chan struct{}
	stopped     bool
	wg          sync.WaitGroup
}

// NewNode creates a node, loads persisted state and starts it. Committed
// entries are delivered on the returned channel in index order; the caller
// must keep draining it.
func NewNode(cfg Config) (*Node, <-chan ApplyMsg, error) {
	cfg.setDefaults()
	if cfg.ID == "" || cfg.Transport == nil && len(cfg.Peers) > 0 {
		return nil, nil, fmt.Errorf("raft: config needs ID and Transport")
	}
	hs, entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, nil, fmt.Errorf("raft: load storage: %w", err)
	}
	n := &Node{
		cfg:         cfg,
		applyCh:     make(chan ApplyMsg, 256),
		state:       Follower,
		currentTerm: hs.Term,
		votedFor:    hs.VotedFor,
		log:         append([]LogEntry{{}}, entries...),
		nextIndex:   map[string]uint64{},
		matchIndex:  map[string]uint64{},
		replicateCh: map[string]chan struct{}{},
		stopCh:      make(chan struct{}),
		syncCh:      make(chan struct{}, 1),
	}
	n.syncedIndex = n.lastIndex() // loaded from disk
	n.applyCond = sync.NewCond(&n.mu)
	n.resetElectionTimer()

	for _, p := range cfg.Peers {
		ch := make(chan struct{}, 1)
		n.replicateCh[p] = ch
		n.wg.Add(1)
		go n.replicator(p, ch)
	}
	n.wg.Add(3)
	go n.ticker()
	go n.applier()
	go n.syncer()
	n.cfg.Logf("[%s] started: term=%d log=%d entries", cfg.ID, hs.Term, len(entries))
	return n, n.applyCh, nil
}

// Stop shuts the node down and waits for its goroutines. The apply channel is
// closed afterwards. Safe to call more than once.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
	close(n.applyCh)
}

// Propose appends a command to the leader's log. It returns the index and
// term the entry will have if it commits. isLeader is false (and nothing is
// appended) when this node is not the leader; callers should retry against
// Leader(). Being proposed does not mean committed: watch the apply channel.
func (n *Node) Propose(cmd []byte) (index, term uint64, isLeader bool, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, 0, false, ErrStopped
	}
	if n.state != Leader {
		return 0, 0, false, nil
	}
	e := LogEntry{Index: n.lastIndex() + 1, Term: n.currentTerm, Command: cmd}
	if err := n.cfg.Storage.Append([]LogEntry{e}); err != nil {
		return 0, 0, true, err
	}
	n.log = append(n.log, e)
	n.signalSync()
	n.triggerReplication()
	return e.Index, e.Term, true, nil
}

// Leader returns the ID of the current leader as far as this node knows ("" if unknown).
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:           n.cfg.ID,
		State:        n.state.String(),
		Term:         n.currentTerm,
		Leader:       n.leaderID,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
}

// ---------------------------------------------------------------------------
// Helpers (call with mu held)

func (n *Node) lastIndex() uint64 { return n.log[len(n.log)-1].Index }
func (n *Node) lastTerm() uint64  { return n.log[len(n.log)-1].Term }
func (n *Node) quorum() int       { return (len(n.cfg.Peers)+1)/2 + 1 }

func (n *Node) randomTimeout() time.Duration {
	lo, hi := n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Int63n(int64(hi-lo)))
}

func (n *Node) resetElectionTimer() {
	n.electionDeadline = time.Now().Add(n.randomTimeout())
}

func (n *Node) persistHardState() {
	if err := n.cfg.Storage.SetHardState(HardState{Term: n.currentTerm, VotedFor: n.votedFor}); err != nil {
		// Continuing without durable state would break Raft's safety guarantees.
		panic(fmt.Sprintf("raft: persist hard state: %v", err))
	}
}

// becomeFollower steps down. A higher term clears votedFor.
func (n *Node) becomeFollower(term uint64, leader string) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistHardState()
	}
	if n.state != Follower {
		n.cfg.Logf("[%s] -> follower (term %d)", n.cfg.ID, n.currentTerm)
	}
	n.state = Follower
	n.leaderID = leader
	n.electionGen++ // cancel any election in progress
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.cfg.ID
	n.electionGen++
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	n.cfg.Logf("[%s] -> leader (term %d)", n.cfg.ID, n.currentTerm)
	// A no-op in the new term lets the leader commit entries left over from
	// earlier terms (Raft §5.4.2 forbids counting replicas of old-term entries).
	e := LogEntry{Index: n.lastIndex() + 1, Term: n.currentTerm}
	if err := n.cfg.Storage.Append([]LogEntry{e}); err != nil {
		panic(fmt.Sprintf("raft: append no-op: %v", err))
	}
	n.log = append(n.log, e)
	n.signalSync()
	n.triggerReplication()
}

func (n *Node) signalSync() {
	select {
	case n.syncCh <- struct{}{}:
	default:
	}
}

func (n *Node) triggerReplication() {
	n.lastHeartbeat = time.Now()
	for _, ch := range n.replicateCh {
		select {
		case ch <- struct{}{}:
		default: // already signalled
		}
	}
}

// maybeAdvanceCommit moves commitIndex to the highest index stored on a
// quorum, counting only entries from the current term (Raft §5.4.2).
func (n *Node) maybeAdvanceCommit() {
	if n.state != Leader {
		return
	}
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			break // earlier entries are older terms too
		}
		count := 0
		if n.syncedIndex >= idx {
			count = 1 // our own durable copy
		}
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			n.applyCond.Broadcast()
			return
		}
	}
}

func (n *Node) logUpToDate(lastIndex, lastTerm uint64) bool {
	if lastTerm != n.lastTerm() {
		return lastTerm > n.lastTerm()
	}
	return lastIndex >= n.lastIndex()
}

// ---------------------------------------------------------------------------
// Background loops

func (n *Node) ticker() {
	defer n.wg.Done()
	tick := time.NewTicker(n.cfg.HeartbeatInterval / 4)
	defer tick.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-tick.C:
		}
		n.mu.Lock()
		now := time.Now()
		switch {
		case n.state == Leader:
			if now.Sub(n.lastHeartbeat) >= n.cfg.HeartbeatInterval {
				n.triggerReplication()
			}
		case now.After(n.electionDeadline):
			n.resetElectionTimer()
			n.campaign(!n.cfg.DisablePreVote)
		}
		n.mu.Unlock()
	}
}

// campaign starts a pre-vote (pre=true) or a real election. Called with mu held.
//
// Pre-Vote: before bumping its term, a node asks whether it *could* win. A
// node cut off by a partition keeps failing the pre-vote, so it never inflates
// its term and never forces a healthy leader to step down when it rejoins.
func (n *Node) campaign(pre bool) {
	var term uint64
	if pre {
		n.state = PreCandidate
		term = n.currentTerm + 1
	} else {
		n.state = Candidate
		n.currentTerm++
		n.votedFor = n.cfg.ID
		n.persistHardState()
		term = n.currentTerm
	}
	n.leaderID = ""
	n.electionGen++
	gen := n.electionGen
	n.cfg.Logf("[%s] starting %s for term %d", n.cfg.ID, map[bool]string{true: "pre-vote", false: "election"}[pre], term)

	votes := 1
	won := func() {
		if pre {
			n.campaign(false)
		} else {
			n.becomeLeader()
		}
	}
	if votes >= n.quorum() {
		won()
		return
	}
	req := &RequestVoteRequest{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
		PreVote:      pre,
	}
	for _, p := range n.cfg.Peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(ctx, peer, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped || gen != n.electionGen {
				return // this election is over
			}
			if resp.Term > n.currentTerm && !resp.VoteGranted {
				n.becomeFollower(resp.Term, "")
				return
			}
			if !resp.VoteGranted {
				return
			}
			votes++
			if votes == n.quorum() {
				won()
			}
		}(p)
	}
}

// replicator sends AppendEntries to one peer whenever signalled. Keeping one
// RPC in flight per peer keeps ordering simple; batching (up to
// MaxEntriesPerAppend) recovers throughput under load.
func (n *Node) replicator(peer string, signal <-chan struct{}) {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-signal:
		}
		for n.sendAppend(peer) {
			// keep going while the follower is behind
			select {
			case <-n.stopCh:
				return
			default:
			}
		}
	}
}

// sendAppend sends one AppendEntries to peer. It returns true if the peer is
// still behind and another round should be sent immediately.
func (n *Node) sendAppend(peer string) bool {
	n.mu.Lock()
	if n.state != Leader || n.stopped {
		n.mu.Unlock()
		return false
	}
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}
	prev := next - 1
	end := n.lastIndex()
	if max := prev + uint64(n.cfg.MaxEntriesPerAppend); end > max {
		end = max
	}
	entries := append([]LogEntry(nil), n.log[prev+1:end+1]...)
	req := &AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  n.log[prev].Term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
	resp, err := n.cfg.Transport.AppendEntries(ctx, peer, req)
	cancel()
	if err != nil {
		return false // retried on the next heartbeat
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.state != Leader || n.currentTerm != term {
		return false
	}
	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term, "")
		n.resetElectionTimer()
		return false
	}
	if resp.Success {
		match := prev + uint64(len(entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
		}
		if match+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = match + 1
		}
		n.maybeAdvanceCommit()
		return n.nextIndex[peer] <= n.lastIndex()
	}

	if resp.ConflictIndex == 0 {
		return false // rejected without a hint (e.g. the peer is shutting down)
	}
	// Fast backtracking: jump past the whole conflicting term.
	newNext := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		for i := n.lastIndex(); i > 0; i-- {
			if n.log[i].Term == resp.ConflictTerm {
				newNext = i + 1
				break
			}
			if n.log[i].Term < resp.ConflictTerm {
				break
			}
		}
	}
	if newNext < 1 {
		newNext = 1
	}
	if newNext > n.lastIndex()+1 {
		newNext = n.lastIndex() + 1
	}
	n.nextIndex[peer] = newNext
	return true
}

// syncer makes the leader's appended entries durable. Proposals that arrive
// while an fsync is running are covered by the next one, so under load a
// single fsync commits a whole batch.
func (n *Node) syncer() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.syncCh:
		}
		n.mu.Lock()
		target, gen := n.lastIndex(), n.truncGen
		needed := target > n.syncedIndex
		n.mu.Unlock()
		if !needed {
			continue
		}
		if err := n.cfg.Storage.Sync(); err != nil {
			panic(fmt.Sprintf("raft: sync: %v", err))
		}
		n.mu.Lock()
		if gen == n.truncGen && target > n.syncedIndex {
			n.syncedIndex = target
			n.maybeAdvanceCommit()
		}
		if n.lastIndex() > n.syncedIndex {
			n.signalSync() // more arrived during the fsync
		}
		n.mu.Unlock()
	}
}

func (n *Node) applier() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		for n.lastApplied >= n.commitIndex && !n.stopped {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		batch := append([]LogEntry(nil), n.log[n.lastApplied+1:n.commitIndex+1]...)
		n.mu.Unlock()

		for _, e := range batch {
			select {
			case n.applyCh <- ApplyMsg{Index: e.Index, Term: e.Term, Command: e.Command}:
			case <-n.stopCh:
				return
			}
			n.mu.Lock()
			n.lastApplied = e.Index
			n.mu.Unlock()
		}
	}
}

// ---------------------------------------------------------------------------
// RPC handlers

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := &RequestVoteResponse{Term: n.currentTerm}
	if n.stopped {
		return resp
	}

	if req.PreVote {
		// Grant only if a real election could succeed: the candidate's term
		// would be newer, its log is current, and we have not heard from a
		// live leader recently (so we are not interrupting a healthy cluster).
		heardFromLeader := n.state == Leader ||
			(n.leaderID != "" && time.Since(n.lastLeaderContact) < n.cfg.ElectionTimeoutMin)
		resp.VoteGranted = req.Term > n.currentTerm &&
			n.logUpToDate(req.LastLogIndex, req.LastLogTerm) &&
			!heardFromLeader
		return resp
	}

	if req.Term < n.currentTerm {
		return resp
	}
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, "")
	}
	resp.Term = n.currentTerm
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && n.logUpToDate(req.LastLogIndex, req.LastLogTerm) {
		n.votedFor = req.CandidateID
		n.persistHardState()
		n.resetElectionTimer()
		resp.VoteGranted = true
	}
	return resp
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := &AppendEntriesResponse{Term: n.currentTerm}
	if n.stopped || req.Term < n.currentTerm {
		return resp
	}
	// A valid leader for this term exists: follow it.
	if req.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(req.Term, req.LeaderID)
	}
	n.leaderID = req.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionTimer()
	resp.Term = n.currentTerm

	// Consistency check on the entry just before the new ones.
	if req.PrevLogIndex > n.lastIndex() {
		resp.ConflictIndex = n.lastIndex() + 1
		return resp
	}
	if t := n.log[req.PrevLogIndex].Term; t != req.PrevLogTerm {
		resp.ConflictTerm = t
		i := req.PrevLogIndex
		for i > 1 && n.log[i-1].Term == t {
			i--
		}
		resp.ConflictIndex = i
		return resp
	}

	// Append new entries, deleting any conflicting suffix first. Entries we
	// already have are skipped, so a delayed duplicate RPC can never truncate
	// entries that a newer RPC already added.
	for i, e := range req.Entries {
		if e.Index <= n.lastIndex() {
			if n.log[e.Index].Term == e.Term {
				continue
			}
			if e.Index <= n.commitIndex {
				panic(fmt.Sprintf("raft: [%s] leader tried to overwrite committed index %d", n.cfg.ID, e.Index))
			}
			if err := n.cfg.Storage.TruncateFrom(e.Index); err != nil {
				panic(fmt.Sprintf("raft: truncate: %v", err))
			}
			n.log = n.log[:e.Index]
			n.truncGen++
			if n.syncedIndex >= e.Index {
				n.syncedIndex = e.Index - 1
			}
		}
		rest := req.Entries[i:]
		if err := n.cfg.Storage.Append(rest); err != nil {
			panic(fmt.Sprintf("raft: append: %v", err))
		}
		n.log = append(n.log, rest...)
		break
	}

	// Everything we are about to acknowledge must be on disk first.
	if n.lastIndex() > n.syncedIndex {
		if err := n.cfg.Storage.Sync(); err != nil {
			panic(fmt.Sprintf("raft: sync: %v", err))
		}
		n.syncedIndex = n.lastIndex()
	}

	if req.LeaderCommit > n.commitIndex {
		lastNew := req.PrevLogIndex + uint64(len(req.Entries))
		c := req.LeaderCommit
		if lastNew < c {
			c = lastNew
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Broadcast()
		}
	}
	resp.Success = true
	return resp
}
