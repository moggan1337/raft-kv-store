//go:build integration
// +build integration

package tests

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pb "github.com/moggan1337/raft-kv-store/internal/rpc"
)

// findFreePort returns an available TCP port by listening on :0.
func findFreePort(t *testing.T) int {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}

// waitForServer polls the given TCP address until a connection succeeds
// or the timeout elapses. Returns true on success.
func waitForServer(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

type binaryHandle struct {
	cmd     *exec.Cmd
	dataDir string
	addr    string
}

// spawnBinary starts ./bin/raftkvd with a fresh data dir and a free
// listen port. Returns once the gRPC port is accepting connections.
func spawnBinary(t *testing.T, addr string, dataDir string) *binaryHandle {
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	binPath, err := filepath.Abs("../bin/raftkvd")
	require.NoError(t, err)
	require.FileExists(t, binPath, "build the binary first: make build")

	cmd := exec.Command(binPath,
		"-listen", addr,
		"-data", dataDir,
		"-debug",
	)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	require.NoError(t, cmd.Start())

	// Forward binary output to test output for visibility
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				t.Log("STDOUT:", string(buf[:n]))
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				t.Log("STDERR:", string(buf[:n]))
			}
			if err != nil {
				return
			}
		}
	}()

	if !waitForServer(addr, 10*time.Second) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("raftkvd did not start listening on %s within 10s", addr)
	}

	t.Cleanup(func() {
		// Best-effort cleanup
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	return &binaryHandle{cmd: cmd, dataDir: dataDir, addr: addr}
}

func dialKV(t *testing.T, addr string) pb.KVClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(5*time.Second))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return pb.NewKVClient(conn)
}

func TestEndToEndSingleNode_PutGetPersist(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in -short mode")
	}

	port := findFreePort(t)
	addr := "127.0.0.1:" + itoa(port)
	dataDir := t.TempDir()

	// === Phase 1: spawn, put, get, shut down ===
	h1 := spawnBinary(t, addr, dataDir)

	// Give the single-node cluster a moment to elect itself.
	// The binary's election loop ticks every ~50ms; it becomes
	// leader within ~200ms in practice.
	time.Sleep(500 * time.Millisecond)

	client1 := dialKV(t, addr)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	putResp, err := client1.Put(ctx1, &pb.PutRequest{K: "alpha", V: "first"})
	require.NoError(t, err)
	assert.True(t, putResp.Ok, "first Put should succeed (we are leader)")

	// Poll Get until the applier applies the entry (ticker is 10ms,
	// but the binary needs to commit + apply — give it some slack).
	var got *pb.Value
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client1.Get(ctx1, &pb.Key{K: "alpha"})
		require.NoError(t, err)
		if resp.Found {
			got = resp
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NotNil(t, got, "applier did not apply the Put within 5s")
	assert.Equal(t, []byte("first"), got.V)

	// Shut down gracefully. SIGTERM triggers final persist.
	require.NoError(t, h1.cmd.Process.Signal(syscall.SIGTERM))
	done := make(chan error, 1)
	go func() { done <- h1.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = h1.cmd.Process.Kill()
		t.Fatal("raftkvd did not exit within 5s of SIGTERM")
	}

	// === Phase 2: spawn again with same data dir, verify the value persists ===
	_ = spawnBinary(t, addr, dataDir) // h2 reference (t.Cleanup handles shutdown)
	time.Sleep(500 * time.Millisecond)

	client2 := dialKV(t, addr)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	// The applier should rebuild the state machine by replaying
	// the recovered log entries. Poll for the value to appear.
	var restored *pb.Value
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client2.Get(ctx2, &pb.Key{K: "alpha"})
		require.NoError(t, err)
		if resp.Found {
			restored = resp
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NotNil(t, restored, "value 'alpha' not recovered after restart within 5s")
	assert.Equal(t, []byte("first"), restored.V, "recovered value must match")

	// Bonus: also put a second value and verify it lands.
	putResp2, err := client2.Put(ctx2, &pb.PutRequest{K: "beta", V: "second"})
	require.NoError(t, err)
	assert.True(t, putResp2.Ok)

	deadline = time.Now().Add(5 * time.Second)
	var beta *pb.Value
	for time.Now().Before(deadline) {
		resp, err := client2.Get(ctx2, &pb.Key{K: "beta"})
		require.NoError(t, err)
		if resp.Found {
			beta = resp
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NotNil(t, beta)
	assert.Equal(t, []byte("second"), beta.V)
}

func itoa(i int) string {
	// minimal local int-to-string (avoids importing strconv in hot path)
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
