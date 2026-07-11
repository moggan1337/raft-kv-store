package network

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/moggan1337/raft-kv-store/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type fakeRaftServer struct {
	pb.UnimplementedRaftServer
	lastVote *pb.VoteRequest
}

func (f *fakeRaftServer) RequestVote(_ context.Context, r *pb.VoteRequest) (*pb.VoteResponse, error) {
	f.lastVote = r
	return &pb.VoteResponse{Term: r.Term, VoteGranted: true}, nil
}

func TestPeerRequestVote(t *testing.T) {
	srv := &fakeRaftServer{}
	lis := bufconn.Listen(1024)
	s := grpc.NewServer()
	pb.RegisterRaftServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(dialer), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()

	c := pb.NewRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.RequestVote(ctx, &pb.VoteRequest{Term: 5, CandidateId: 1})
	require.NoError(t, err)
	assert.True(t, resp.VoteGranted)
	assert.NotNil(t, srv.lastVote)
}
