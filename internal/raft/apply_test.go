package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAppliesCommittedEntries(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	n.log = []LogEntry{
		{Term: 0, Index: 0, Type: EntryNoop},
		{Term: 1, Index: 1, Type: EntryCommand, Data: []byte("op1")},
	}
	n.commitIndex = 1

	var (
		mu   sync.Mutex
		seen []uint64
	)
	applier := NewApplier(n, func(idx uint64, cmd []byte) {
		mu.Lock()
		seen = append(seen, idx)
		mu.Unlock()
		_ = cmd
	})

	stop := make(chan struct{})
	go applier.Run()
	time.Sleep(50 * time.Millisecond)
	close(stop)
	applier.Stop()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uint64{1}, seen)
}
