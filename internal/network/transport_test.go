package network

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/moggan1337/raft-kv-store/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type fakeTransportServer struct {
	pb.UnimplementedRaftServer
}

func (f *fakeTransportServer) AppendEntries(_ context.Context, r *pb.AppendRequest) (*pb.AppendResponse, error) {
	return &pb.AppendResponse{
		Term:       r.Term,
		Success:    true,
		MatchIndex: r.PrevLogIndex + uint64(len(r.Entries)),
	}, nil
}

func (f *fakeTransportServer) RequestVote(_ context.Context, r *pb.VoteRequest) (*pb.VoteResponse, error) {
	return &pb.VoteResponse{Term: r.Term, VoteGranted: true}, nil
}

func setupGRPCTransport(t *testing.T, peerID int) *GRPCTransport {
	srv := &fakeTransportServer{}
	lis := bufconn.Listen(1024)
	s := grpc.NewServer()
	pb.RegisterRaftServer(s, srv)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(func() { s.Stop(); lis.Close() })

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn := mustConn(t, dialer)
	p := &Peer{ID: peerID, Addr: "bufnet", conn: conn, c: pb.NewRaftClient(conn)}
	return NewGRPCTransport(map[int]*Peer{peerID: p})
}

func mustConn(t *testing.T, dialer func(context.Context, string) (net.Conn, error)) *grpc.ClientConn {
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(dialer), grpc.WithInsecure())
	require.NoError(t, err)
	return conn
}

func TestGRPCTransportSendAppendEntries(t *testing.T) {
	tr := setupGRPCTransport(t, 1)
	resp, err := tr.SendAppendEntries(context.Background(), 1, fakeAppend())
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, uint64(2), resp.MatchIndex)
}

func TestGRPCTransportSendRequestVote(t *testing.T) {
	tr := setupGRPCTransport(t, 1)
	resp, err := tr.SendRequestVote(context.Background(), 1, fakeVote())
	require.NoError(t, err)
	assert.True(t, resp.VoteGranted)
}

func TestGRPCTransportUnknownPeer(t *testing.T) {
	tr := setupGRPCTransport(t, 1)
	_, err := tr.SendAppendEntries(context.Background(), 99, fakeAppend())
	assert.Error(t, err)
}
