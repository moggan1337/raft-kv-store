package network

import (
	"context"

	"github.com/moggan1337/raft-kv-store/internal/raft"
	pb "github.com/moggan1337/raft-kv-store/internal/rpc"
)

// GRPCTransport implements raft.Transport over gRPC. It opens a
// persistent connection to each peer and forwards RPC calls.
type GRPCTransport struct {
	peers map[int]*Peer
}

func NewGRPCTransport(peers map[int]*Peer) *GRPCTransport {
	return &GRPCTransport{peers: peers}
}

func (t *GRPCTransport) peer(id int) (*Peer, bool) {
	if t == nil {
		return nil, false
	}
	p, ok := t.peers[id]
	return p, ok
}

func (t *GRPCTransport) SendAppendEntries(ctx context.Context, peerID int, req raft.AppendRequest) (raft.AppendResponse, error) {
	p, ok := t.peer(peerID)
	if !ok {
		return raft.AppendResponse{}, ErrNoSuchPeer
	}
	entries := make([]*pb.Entry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = &pb.Entry{
			Term:  e.Term,
			Index: e.Index,
			Type:  uint32(e.Type),
			Data:  e.Data,
		}
	}
	resp, err := p.AppendEntries(ctx, &pb.AppendRequest{
		Term:         req.Term,
		LeaderId:     int32(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return raft.AppendResponse{}, err
	}
	return raft.AppendResponse{
		Term:       resp.Term,
		Success:    resp.Success,
		MatchIndex: resp.MatchIndex,
	}, nil
}

func (t *GRPCTransport) SendRequestVote(ctx context.Context, peerID int, req raft.VoteRequest) (raft.VoteResponse, error) {
	p, ok := t.peer(peerID)
	if !ok {
		return raft.VoteResponse{}, ErrNoSuchPeer
	}
	resp, err := p.RequestVote(ctx, req.Term, req.CandidateID, req.LastLogIndex, req.LastLogTerm)
	if err != nil {
		return raft.VoteResponse{}, err
	}
	return raft.VoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

var ErrNoSuchPeer = peerNotFound{}

type peerNotFound struct{}

func (peerNotFound) Error() string { return "no such peer" }
