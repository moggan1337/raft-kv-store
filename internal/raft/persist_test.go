package raft

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistRecover(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	n.term = 7
	n.votedFor = 2

	require.NoError(t, n.Persist(path))

	n2 := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	require.NoError(t, n2.Recover(path))

	assert.Equal(t, uint64(7), n2.term)
	assert.Equal(t, 2, n2.votedFor)
}

func TestRecoverMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing.json")

	n := NewNode(NodeConfig{ID: 1})
	assert.NoError(t, n.Recover(path))
	assert.Equal(t, uint64(0), n.term)
}

func TestPersistRecoversLogEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	n := NewNode(NodeConfig{ID: 1})
	n.role = RoleLeader
	n.term = 5
	n.log = []LogEntry{
		{Term: 0, Index: 0, Type: EntryNoop},
		{Term: 5, Index: 1, Type: EntryCommand, Data: []byte("hello")},
		{Term: 5, Index: 2, Type: EntryCommand, Data: []byte("world")},
	}
	n.commitIndex = 2

	require.NoError(t, n.Persist(path))

	n2 := NewNode(NodeConfig{ID: 1})
	require.NoError(t, n2.Recover(path))
	assert.Equal(t, uint64(5), n2.term)
	assert.Equal(t, RoleFollower, n2.role, "role should be reset to Follower on Recover")
	assert.Equal(t, uint64(2), n2.LastLogIndex())
}
