package raft

import (
	"context"
	"sort"
	"time"
)

type Transport interface {
	SendAppendEntries(ctx context.Context, peerID int, req AppendRequest) (AppendResponse, error)
	SendRequestVote(ctx context.Context, peerID int, req VoteRequest) (VoteResponse, error)
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	n.role = RoleLeader
	// On leader election, nextIndex initialized to len(log).
	for pid := range n.progress {
		n.progress[pid] = &Progress{NextIndex: uint64(len(n.log)), MatchIndex: 0}
	}
	n.mu.Unlock()
}

// LeaderLoop is the main leader-side replication loop.
// It ticks every Heartbeat interval and triggers a replicate to each peer.
func (n *Node) LeaderLoop(ctx context.Context, t Transport, peerIDs []int) {
	tick := time.NewTicker(n.cfg.Heartbeat)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			n.mu.Lock()
			if n.role != RoleLeader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			for _, pid := range peerIDs {
				go n.replicateOnce(ctx, t, pid)
			}
		}
	}
}

func (n *Node) replicateOnce(ctx context.Context, tr Transport, peerID int) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	progress, ok := n.progress[peerID]
	if !ok {
		progress = &Progress{NextIndex: uint64(len(n.log)), MatchIndex: 0}
		n.progress[peerID] = progress
	}
	prevIdx := progress.NextIndex - 1
	prevTerm := uint64(0)
	if prevIdx > 0 && prevIdx < uint64(len(n.log)) {
		prevTerm = n.log[prevIdx].Term
	}
	var entries []LogEntry
	if progress.NextIndex < uint64(len(n.log)) {
		entries = append(entries, n.log[progress.NextIndex:]...)
	}
	commit := n.commitIndex
	term := n.term
	n.mu.Unlock()

	resp, err := tr.SendAppendEntries(ctx, peerID, AppendRequest{
		Term: term, LeaderID: n.cfg.ID,
		PrevLogIndex: prevIdx, PrevLogTerm: prevTerm,
		Entries: entries, LeaderCommit: commit,
	})
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.term = resp.Term
		n.role = RoleFollower
		n.votedFor = -1
		return
	}
	if resp.Success {
		progress.MaybeUpdate(resp.MatchIndex)
	} else if prevIdx > 0 {
		progress.MaybeDecrement(prevIdx)
	}
	// Try to advance commit index
	n.advanceCommit()
}

// advanceCommit is called on each successful replicate to update the
// leader's commitIndex based on a majority of matchIndex values.
func (n *Node) advanceCommit() {
	// Collect all matchIndex values (peers + leader)
	matches := make([]uint64, 0, len(n.progress)+1)
	matches = append(matches, uint64(len(n.log)-1))
	for _, p := range n.progress {
		matches = append(matches, p.MatchIndex)
	}
	// Find the majority matchIndex
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	mid := len(matches) / 2
	median := matches[mid]
	if median > n.commitIndex && median < uint64(len(n.log)) && n.log[median].Term == n.term {
		n.commitIndex = median
	}
}
