package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBecomeCandidateIncrementsTerm(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	n.becomeCandidate()
	assert.Equal(t, RoleCandidate, n.Role())
	assert.Equal(t, uint64(1), n.CurrentTerm())
	assert.Equal(t, 1, n.votedFor)
}

func TestHandleVoteRequestGrantsForSameTermNewerLog(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	n.log = append(n.log, LogEntry{Term: 1, Index: 1, Type: EntryCommand})

	resp := n.HandleVoteRequest(VoteRequest{Term: 2, CandidateID: 2, LastLogIndex: 1, LastLogTerm: 1})
	assert.True(t, resp.VoteGranted)
	assert.Equal(t, uint64(2), n.CurrentTerm())
}

func TestHandleVoteRequestRejectsStaleTerm(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	n.term = 5

	resp := n.HandleVoteRequest(VoteRequest{Term: 3, CandidateID: 2})
	assert.False(t, resp.VoteGranted)
}
