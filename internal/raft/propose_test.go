package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProposeAppendsToLeaderLog(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	n.role = RoleLeader
	n.term = 1

	done := make(chan error, 1)
	go func() { done <- n.Propose(context.Background(), []byte("x")) }()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("propose did not return")
	}
	assert.Len(t, n.log, 2)
	assert.Equal(t, []byte("x"), n.log[1].Data)
}

func TestProposeFromFollowerReturnsError(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	err := n.Propose(context.Background(), []byte("x"))
	assert.Error(t, err)
}
