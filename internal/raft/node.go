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
}

func NewNode(cfg NodeConfig) *Node {
	return &Node{
		cfg:  cfg,
		role: RoleFollower,
		log:  []LogEntry{{Term: 0, Index: 0, Type: EntryNoop}},
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
