package network

import (
	"context"
	"time"

	pb "github.com/moggan1337/raft-kv-store/internal/rpc"
	"google.golang.org/grpc"
)

type Peer struct {
	ID   int
	Addr string
	conn *grpc.ClientConn
	c    pb.RaftClient
}

func DialPeer(id int, addr string) (*Peer, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(2*time.Second))
	if err != nil {
		return nil, err
	}
	return &Peer{ID: id, Addr: addr, conn: conn, c: pb.NewRaftClient(conn)}, nil
}

func (p *Peer) Close() error { return p.conn.Close() }

func (p *Peer) RequestVote(ctx context.Context, term uint64, candidateID int, lastIdx, lastTerm uint64) (*pb.VoteResponse, error) {
	return p.c.RequestVote(ctx, &pb.VoteRequest{
		Term: term, CandidateId: int32(candidateID), LastLogIndex: lastIdx, LastLogTerm: lastTerm,
	})
}

func (p *Peer) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	return p.c.AppendEntries(ctx, req)
}
