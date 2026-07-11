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
