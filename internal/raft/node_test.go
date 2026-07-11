package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewNodeIsFollower(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	assert.Equal(t, RoleFollower, n.Role())
	assert.Equal(t, uint64(0), n.CurrentTerm())
}
