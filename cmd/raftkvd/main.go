package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/moggan1337/raft-kv-store/internal/config"
	"github.com/moggan1337/raft-kv-store/internal/kvserver"
	"github.com/moggan1337/raft-kv-store/internal/logging"
	"github.com/moggan1337/raft-kv-store/internal/network"
	"github.com/moggan1337/raft-kv-store/internal/raft"
	"github.com/moggan1337/raft-kv-store/internal/statemachine"
	pb "github.com/moggan1337/raft-kv-store/internal/rpc"
)

func main() {
	var (
		configPath = flag.String("config", "", "path to YAML config (optional)")
		debug      = flag.Bool("debug", false, "enable debug logging")
		listenAddr = flag.String("listen", "", "override listen_addr (e.g. 127.0.0.1:0 for ephemeral)")
		dataDir    = flag.String("data", "", "override data_dir")
	)
	flag.Parse()

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config load: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir data dir: %v\n", err)
		os.Exit(1)
	}
	if *listenAddr != "" {
		cfg.ListenAddr = *listenAddr
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}

	// Logger
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := logging.New(level, os.Stderr).With("node_id", cfg.NodeID)
	logger.Info("raftkvd starting", "data_dir", cfg.DataDir, "listen", cfg.ListenAddr, "version", raft.Version)

	// Recover persistent state
	statePath := filepath.Join(cfg.DataDir, "state.json")
	node := raft.NewNode(raft.NodeConfig{
		ID:          cfg.NodeID,
		ElectionMin: cfg.ElectionMin,
		ElectionMax: cfg.ElectionMax,
		Heartbeat:   cfg.Heartbeat,
	})
	if err := node.Recover(statePath); err != nil {
		logger.Error("recover state failed", "err", err)
		os.Exit(1)
	}
	logger.Info("state recovered", "term", node.CurrentTerm(), "log_len", node.LastLogIndex()+1)

	// State machine + applier
	sm := statemachine.NewMemKV()
	applier := raft.NewApplier(node, func(_ uint64, cmd []byte) {
		_, err := sm.Apply(cmd)
		if err != nil {
			logger.Warn("apply op failed", "err", err)
		}
	})
	go applier.Run()

	// Periodic state persistence (after each successful log append)
	go persistLoop(logger, node, statePath)

	// Connect to peers (gRPC)
	peers := make(map[int]*network.Peer)
	peerIDs := make([]int, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peer, err := network.DialPeer(p.ID, p.URL)
		if err != nil {
			logger.Warn("peer dial failed", "id", p.ID, "url", p.URL, "err", err)
			continue
		}
		peers[p.ID] = peer
		peerIDs = append(peerIDs, p.ID)
		logger.Info("peer connected", "id", p.ID, "url", p.URL)
	}
	transport := network.NewGRPCTransport(peers)

	// Election loop
	go electionLoop(logger, node, transport, peerIDs)

	// Leader replication loop (only runs while leader)
	go leaderLoop(logger, node, transport, peerIDs)

	// gRPC server (Raft + KV)
	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("listen failed", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}
	gsrv := grpc.NewServer()

	// Raft service: implement the gRPC RaftServer by delegating to the Node
	// and persisting state after each mutation.
	pb.RegisterRaftServer(gsrv, &raftService{node: node, logger: logger, statePath: statePath})

	// KV service
	pb.RegisterKVServer(gsrv, kvserver.New(&proposerAdapter{node: node}, sm))

	go func() {
		logger.Info("gRPC server listening", "addr", cfg.ListenAddr)
		if err := gsrv.Serve(lis); err != nil {
			logger.Error("gRPC serve failed", "err", err)
			os.Exit(1)
		}
	}()

	// Block on signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutdown signal received", "signal", sig.String())

	applier.Stop()
	gsrv.GracefulStop()

	// Final persist
	if err := node.Persist(statePath); err != nil {
		logger.Error("final persist failed", "err", err)
	}
	for _, p := range peers {
		_ = p.Close()
	}
	logger.Info("raftkvd shutdown complete")
}

// proposerAdapter implements kvserver.Proposer by delegating to node.Propose.
// The proposer's Propose call returns immediately if the node is not the
// leader; the leader's actual commit is async via the applier.
type proposerAdapter struct {
	node *raft.Node
}

func (p *proposerAdapter) Propose(ctx context.Context, cmd []byte) error {
	return p.node.Propose(ctx, cmd)
}

// raftService is the gRPC Raft server. It implements the RaftServer
// interface generated from raft.proto, delegating each call to the
// Node's in-process handler.
type raftService struct {
	pb.UnimplementedRaftServer
	node    *raft.Node
	logger  *slog.Logger
	statePath string
}

func (s *raftService) RequestVote(ctx context.Context, r *pb.VoteRequest) (*pb.VoteResponse, error) {
	resp := s.node.HandleVoteRequest(raft.VoteRequest{
		Term:         r.Term,
		CandidateID:  int(r.CandidateId),
		LastLogIndex: r.LastLogIndex,
		LastLogTerm:  r.LastLogTerm,
	})
	s.persist("request_vote")
	return &pb.VoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (s *raftService) AppendEntries(ctx context.Context, r *pb.AppendRequest) (*pb.AppendResponse, error) {
	entries := make([]raft.LogEntry, 0, len(r.Entries))
	for _, e := range r.Entries {
		entries = append(entries, raft.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Type:  raft.EntryType(e.Type),
			Data:  e.Data,
		})
	}
	resp := s.node.HandleAppendEntries(raft.AppendRequest{
		Term:         r.Term,
		LeaderID:     int(r.LeaderId),
		PrevLogIndex: r.PrevLogIndex,
		PrevLogTerm:  r.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: r.LeaderCommit,
	})
	s.persist("append_entries")
	return &pb.AppendResponse{
		Term:       resp.Term,
		Success:    resp.Success,
		MatchIndex: resp.MatchIndex,
	}, nil
}

func (s *raftService) InstallSnapshot(_ context.Context, _ *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	return &pb.SnapshotResponse{Term: 0}, nil // TODO: real impl
}

func (s *raftService) persist(reason string) {
	if err := s.node.Persist(s.statePath); err != nil {
		s.logger.Warn("persist failed", "reason", reason, "err", err)
	}
}

// electionLoop runs an election timer that becomes a candidate when no
// heartbeat is received within ElectionMin..ElectionMax.
func electionLoop(logger *slog.Logger, node *raft.Node, transport *network.GRPCTransport, peerIDs []int) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	lastHeartbeat := time.Now()
	for range t.C {
		if node.Role() == raft.RoleLeader {
			continue
		}
		// Random timeout
		min := 150 * time.Millisecond
		max := 300 * time.Millisecond
		timeout := min + time.Duration(time.Now().UnixNano()%int64(max-min))
		if time.Since(lastHeartbeat) < timeout {
			continue
		}
		// Become candidate
		node.BecomeCandidate()
		logger.Info("became candidate", "term", node.CurrentTerm())
		// Request votes
		lastIdx := node.LastLogIndex()
		lastTerm := node.LastLogTerm()
		votes := 1 // vote for self
		votesNeeded := len(peerIDs)/2 + 1
		for _, pid := range peerIDs {
			resp, err := transport.SendRequestVote(context.Background(), pid, raft.VoteRequest{
				Term:         node.CurrentTerm(),
				CandidateID:  node.ID(),
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				logger.Warn("request vote failed", "peer", pid, "err", err)
				continue
			}
			if resp.VoteGranted {
				votes++
			}
			if resp.Term > node.CurrentTerm() {
				logger.Info("stale term from vote, stepping down", "peer", pid, "term", resp.Term)
				node.StepDown(resp.Term)
			}
		}
		if votes >= votesNeeded {
			node.BecomeLeader()
			logger.Info("became leader", "term", node.CurrentTerm(), "votes", votes)
		}
		lastHeartbeat = time.Now()
	}
}

// leaderLoop runs AppendEntries replication when this node is the leader.
func leaderLoop(logger *slog.Logger, node *raft.Node, transport *network.GRPCTransport, peerIDs []int) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		if node.Role() != raft.RoleLeader {
			continue
		}
		for _, pid := range peerIDs {
			transport.SendAppendEntries(context.Background(), pid, raft.AppendRequest{
				Term:         node.CurrentTerm(),
				LeaderID:     node.ID(),
				PrevLogIndex: node.LastLogIndex(),
				PrevLogTerm:  node.LastLogTerm(),
				Entries:      nil,
				LeaderCommit: node.CommitIndex(),
			})
		}
	}
}

// persistLoop persists state every 1 second. Simple and safe; can be
// optimized to only persist after mutations.
func persistLoop(logger *slog.Logger, node *raft.Node, statePath string) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for range t.C {
		if err := node.Persist(statePath); err != nil {
			logger.Warn("periodic persist failed", "err", err)
		}
	}
}
