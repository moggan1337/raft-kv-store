package kvserver

import (
	"context"
	"fmt"

	"github.com/moggan1337/raft-kv-store/internal/statemachine"
	pb "github.com/moggan1337/raft-kv-store/internal/rpc"
)

// Proposer submits a command to the Raft log. The command bytes
// represent a state machine operation (e.g. `{"op":"put","k":"x","v":"y"}`).
type Proposer interface {
	Propose(ctx context.Context, cmd []byte) error
}

// Server implements the client-facing KV gRPC service. The state machine
// is the source of truth for reads; the Server reads from it on every Get.
// Writes go through Raft (via Proposer) and the applier eventually
// updates the state machine, so reads on a leader see committed writes.
type Server struct {
	pb.UnimplementedKVServer
	prop Proposer
	sm   *statemachine.MemKV
}

func New(prop Proposer, sm *statemachine.MemKV) *Server {
	return &Server{prop: prop, sm: sm}
}

func (s *Server) Get(_ context.Context, req *pb.Key) (*pb.Value, error) {
	v := s.sm.Get(req.K)
	if v == nil {
		return &pb.Value{Found: false}, nil
	}
	return &pb.Value{Found: true, V: v}, nil
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.prop == nil {
		return &pb.PutResponse{Ok: false}, nil
	}
	cmd := []byte(fmt.Sprintf(`{"op":"put","k":%q,"v":%q}`, req.K, req.V))
	if err := s.prop.Propose(ctx, cmd); err != nil {
		return &pb.PutResponse{Ok: false, LeaderHint: err.Error()}, nil
	}
	// Optimistic local apply: the applier will eventually write the
	// same value, but doing it here too lets us return Ok immediately
	// without waiting for the applier to tick.
	_, _ = s.sm.Apply(cmd)
	return &pb.PutResponse{Ok: true}, nil
}
