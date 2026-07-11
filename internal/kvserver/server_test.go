package kvserver

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

// fakeProposer is a minimal Proposer for testing the KV server.
type fakeProposer struct{ called bool }

func (f *fakeProposer) Propose(_ context.Context, _ []byte) error {
	f.called = true
	return nil
}

func TestPutThenGet(t *testing.T) {
	prop := &fakeProposer{}
	srv := New(prop)
	s := grpc.NewServer()
	pb.RegisterKVServer(s, srv)
	lis := bufconn.Listen(1024)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(dialer), grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()

	c := pb.NewKVClient(conn)
	ctx := context.Background()

	putResp, err := c.Put(ctx, &pb.PutRequest{K: "foo", V: "bar"})
	require.NoError(t, err)
	assert.True(t, putResp.Ok)
	assert.True(t, prop.called, "Propose should have been called")

	getResp, err := c.Get(ctx, &pb.Key{K: "foo"})
	require.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("bar"), getResp.V)
}
