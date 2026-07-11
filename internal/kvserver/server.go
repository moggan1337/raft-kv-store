package kvserver

import (
	"context"
	"fmt"

	pb "github.com/moggan1337/raft-kv-store/internal/rpc"
)

type Proposer interface {
	Propose(ctx context.Context, cmd []byte) error
}

type Server struct {
	pb.UnimplementedKVServer
	prop Proposer
	// In-memory state machine backing — in production this would be the
	// real applier (statemachine.MemKV).
	store map[string][]byte
}

func New(prop Proposer) *Server {
	return &Server{prop: prop, store: make(map[string][]byte)}
}

func (s *Server) Get(_ context.Context, req *pb.Key) (*pb.Value, error) {
	if v, ok := s.store[req.K]; ok {
		return &pb.Value{Found: true, V: v}, nil
	}
	return &pb.Value{Found: false}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.prop == nil {
		return &pb.PutResponse{Ok: false}, nil
	}
	cmd := []byte(fmt.Sprintf(`{"op":"put","k":%q,"v":%q}`, req.K, req.V))
	if err := s.prop.Propose(ctx, cmd); err != nil {
		return &pb.PutResponse{Ok: false, LeaderHint: err.Error()}, nil
	}
	// In a real impl we'd wait for commit before returning. For now,
	// optimistically apply locally — the applier will overwrite on commit.
	s.store[req.K] = []byte(req.V)
	return &pb.PutResponse{Ok: true}, nil
}
