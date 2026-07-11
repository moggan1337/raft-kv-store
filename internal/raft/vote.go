package raft

func (n *Node) becomeCandidate() {
	n.mu.Lock()
	n.term++
	n.role = RoleCandidate
	n.votedFor = n.cfg.ID
	n.mu.Unlock()
}

type VoteRequest struct {
	Term         uint64
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

func (n *Node) HandleVoteRequest(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.term {
		return VoteResponse{Term: n.term, VoteGranted: false}
	}
	if req.Term > n.term {
		n.term = req.Term
		n.role = RoleFollower
		n.votedFor = -1
	}
	lastIdx := uint64(len(n.log) - 1)
	lastTerm := uint64(0)
	if lastIdx > 0 {
		lastTerm = n.log[lastIdx].Term
	}
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
	if (n.votedFor == -1 || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		return VoteResponse{Term: n.term, VoteGranted: true}
	}
	return VoteResponse{Term: n.term, VoteGranted: false}
}
