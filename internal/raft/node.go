package raft

import (
	"sync"
	"time"
)

type NodeConfig struct {
	ID          int
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
}

type Node struct {
	mu          sync.Mutex
	cfg         NodeConfig
	role        Role
	term        uint64
	votedFor    int
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64
	progress    map[int]*Progress // per-peer replication progress (Task 17)
	proposeCh   chan struct{}      // signals leader's replication loop
}

func NewNode(cfg NodeConfig) *Node {
	return &Node{
		cfg:       cfg,
		role:      RoleFollower,
		log:       []LogEntry{{Term: 0, Index: 0, Type: EntryNoop}},
		progress:  make(map[int]*Progress),
		proposeCh: make(chan struct{}, 64),
	}
}

func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term
}

func (n *Node) setRole(r Role) {
	n.mu.Lock()
	n.role = r
	n.mu.Unlock()
}

func (n *Node) LastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return uint64(len(n.log) - 1)
}

func (n *Node) LastLogTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// ID returns the configured node ID.
func (n *Node) ID() int { return n.cfg.ID }

// BecomeCandidate increments the term, votes for self, transitions to Candidate.
func (n *Node) BecomeCandidate() {
	n.mu.Lock()
	n.term++
	n.role = RoleCandidate
	n.votedFor = n.cfg.ID
	n.mu.Unlock()
}

// BecomeLeader transitions to Leader. Initializes per-peer progress to the
// tail of the log.
func (n *Node) BecomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.role = RoleLeader
	lastIdx := uint64(len(n.log) - 1)
	for pid := range n.progress {
		n.progress[pid] = &Progress{NextIndex: lastIdx + 1, MatchIndex: 0}
	}
}

// StepDown returns the node to Follower with the given higher term.
func (n *Node) StepDown(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term > n.term {
		n.term = term
	}
	n.role = RoleFollower
	n.votedFor = -1
}
