// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014).
//
// It covers leader election (with the Pre-Vote extension from §9.6 of the
// Raft thesis), log replication with fast conflict backtracking, and durable
// state through a pluggable Storage. The network is abstracted behind a
// Transport so the same code runs over HTTP in production and over an
// in-memory, fault-injecting network in tests.
package raft

import (
	"context"
	"time"
)

// State is the role a node currently plays.
type State int

const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// LogEntry is one replicated command. Index 0 is a sentinel and never holds
// a real command. An empty Command is a leader no-op.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"`
}

// HardState is the state Raft must persist before answering any RPC.
type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"votedFor"`
}

// ApplyMsg is delivered on the apply channel once an entry is committed.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// RequestVoteRequest is sent by (pre-)candidates.
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
	// PreVote asks "would you vote for me?" without anyone changing state.
	PreVote bool `json:"preVote"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"voteGranted"`
}

// AppendEntriesRequest replicates log entries and doubles as a heartbeat.
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex uint64     `json:"prevLogIndex"`
	PrevLogTerm  uint64     `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries,omitempty"`
	LeaderCommit uint64     `json:"leaderCommit"`
}

// AppendEntriesResponse carries ConflictTerm/ConflictIndex so the leader can
// skip a whole term of mismatched entries per round trip instead of one entry.
type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictTerm  uint64 `json:"conflictTerm,omitempty"`
	ConflictIndex uint64 `json:"conflictIndex,omitempty"`
}

// Transport sends RPCs to peers. Implementations must be safe for concurrent use.
type Transport interface {
	RequestVote(ctx context.Context, peer string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}

// Config configures a Node.
type Config struct {
	ID        string
	Peers     []string // IDs of the other members (excluding ID)
	Transport Transport
	Storage   Storage

	// ElectionTimeoutMin/Max bound the randomized election timeout.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval must be well below ElectionTimeoutMin.
	HeartbeatInterval time.Duration
	// RPCTimeout bounds each outgoing RPC.
	RPCTimeout time.Duration
	// MaxEntriesPerAppend caps the batch size of one AppendEntries.
	MaxEntriesPerAppend int
	// DisablePreVote turns off the Pre-Vote phase (used in tests to show why it exists).
	DisablePreVote bool

	Logf func(format string, args ...any)
}

func (c *Config) setDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = c.ElectionTimeoutMin / 6
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = c.ElectionTimeoutMin
	}
	if c.MaxEntriesPerAppend == 0 {
		c.MaxEntriesPerAppend = 1024
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	if c.Storage == nil {
		c.Storage = NewMemoryStorage()
	}
}

// Status is a point-in-time snapshot of a node, for monitoring and tests.
type Status struct {
	ID           string `json:"id"`
	State        string `json:"state"`
	Term         uint64 `json:"term"`
	Leader       string `json:"leader"`
	CommitIndex  uint64 `json:"commitIndex"`
	LastApplied  uint64 `json:"lastApplied"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}
