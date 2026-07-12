package network

import "github.com/moggan1337/raft-kv-store/internal/raft"

func fakeAppend() raft.AppendRequest {
	return raft.AppendRequest{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []raft.LogEntry{
			{Term: 1, Index: 2, Type: raft.EntryCommand, Data: []byte("x")},
		},
		LeaderCommit: 1,
	}
}

func fakeVote() raft.VoteRequest {
	return raft.VoteRequest{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
}
