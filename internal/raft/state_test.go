package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateEncodeDecode(t *testing.T) {
	ps := PersistentState{
		CurrentTerm: 7,
		VotedFor:    3,
		Log: []LogEntry{
			{Term: 1, Index: 1, Type: EntryNoop},
			{Term: 2, Index: 2, Type: EntryCommand, Data: []byte("k")},
		},
	}
	b, err := ps.Encode()
	assert.NoError(t, err)

	var got PersistentState
	assert.NoError(t, got.Decode(b))
	assert.Equal(t, ps, got)
}

func TestRoleString(t *testing.T) {
	assert.Equal(t, "follower", RoleFollower.String())
	assert.Equal(t, "candidate", RoleCandidate.String())
	assert.Equal(t, "leader", RoleLeader.String())
}
