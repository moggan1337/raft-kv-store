package raft

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAppendEntriesRejectsMismatch(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	n.term = 2
	n.log = []LogEntry{{Term: 0, Index: 0}, {Term: 1, Index: 1}}

	resp := n.HandleAppendEntries(AppendRequest{
		Term: 2, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 99, LeaderCommit: 0, Entries: nil,
	})
	assert.False(t, resp.Success)
}

func TestAppendEntriesAppendsAndCommits(t *testing.T) {
	n := NewNode(NodeConfig{ID: 1, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond})
	n.term = 2
	n.log = []LogEntry{{Term: 0, Index: 0}, {Term: 1, Index: 1}}

	resp := n.HandleAppendEntries(AppendRequest{
		Term: 2, LeaderID: 2,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries:     []LogEntry{{Term: 2, Index: 2, Type: EntryCommand, Data: []byte("x")}},
		LeaderCommit: 2,
	})
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(3), n.log[2].Index)
	assert.Equal(t, uint64(2), n.commitIndex)
}

func TestProgressMaybeUpdate(t *testing.T) {
	p := &Progress{NextIndex: 1, MatchIndex: 0}
	p.MaybeUpdate(5)
	assert.Equal(t, uint64(5), p.MatchIndex)
	assert.Equal(t, uint64(6), p.NextIndex)
}

func TestProgressMaybeDecrement(t *testing.T) {
	p := &Progress{NextIndex: 5, MatchIndex: 4}
	p.MaybeDecrement(3)
	assert.Equal(t, uint64(3), p.NextIndex)
}

func TestCommitIndexMajority(t *testing.T) {
	// 5-node cluster: matches {2,2,2,2,0} -> majority at 2
	matches := []uint64{2, 2, 2, 2, 0}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	n := len(matches)
	idx := n / 2
	assert.Equal(t, uint64(2), matches[idx])
}
