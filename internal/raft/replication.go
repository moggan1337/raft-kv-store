package raft

import "sort"

type AppendRequest struct {
	Term         uint64
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendResponse struct {
	Term       uint64
	Success    bool
	MatchIndex uint64
}

func (n *Node) HandleAppendEntries(req AppendRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.term {
		return AppendResponse{Term: n.term, Success: false}
	}
	if req.Term > n.term {
		n.term = req.Term
		n.role = RoleFollower
		n.votedFor = -1
	}
	if req.PrevLogIndex >= uint64(len(n.log)) {
		return AppendResponse{Term: n.term, Success: false, MatchIndex: uint64(len(n.log) - 1)}
	}
	if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		return AppendResponse{Term: n.term, Success: false, MatchIndex: req.PrevLogIndex - 1}
	}
	for i, e := range req.Entries {
		idx := req.PrevLogIndex + 1 + uint64(i)
		if int(idx) < len(n.log) {
			if n.log[idx].Term != e.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, e)
			}
		} else {
			n.log = append(n.log, e)
		}
	}
	if req.LeaderCommit > n.commitIndex {
		last := uint64(len(n.log) - 1)
		if req.LeaderCommit < last {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = last
		}
	}
	return AppendResponse{Term: n.term, Success: true, MatchIndex: uint64(len(n.log) - 1)}
}

// Per-peer replication progress (Task 17+18)
type Progress struct {
	NextIndex  uint64
	MatchIndex uint64
}

func (p *Progress) MaybeUpdate(matched uint64) {
	if matched > p.MatchIndex {
		p.MatchIndex = matched
		p.NextIndex = matched + 1
	}
}

func (p *Progress) MaybeDecrement(rejected uint64) {
	if p.NextIndex > 1 && rejected < p.NextIndex {
		p.NextIndex = rejected
	}
}

// Leader-side quorum commit advancement (Task 18).
// Returns the new commit index that the leader can safely advance to.
// Rule: leader can commit an entry from its current term if a majority
// of peers have it (Figure 2 from the Raft paper).
func (n *Node) ComputeCommitIndex(peerMatches map[int]uint64, leaderLastIndex uint64) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Collect match indices: leader's own log + each peer's match
	indices := make([]uint64, 0, len(peerMatches)+1)
	indices = append(indices, leaderLastIndex)
	for _, m := range peerMatches {
		indices = append(indices, m)
	}
	// Sort descending
	sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })
	// Median (majority) index
	mid := len(indices) / 2
	median := indices[mid]
	// Only commit if it's from the current term (safety)
	if median > n.commitIndex && median < uint64(len(n.log)) {
		if n.log[median].Term == n.term {
			return median
		}
	}
	return n.commitIndex
}
