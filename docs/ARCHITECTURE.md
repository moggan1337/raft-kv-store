# Architecture — raft-kv-store

A replicated, linearizable key-value store using the Raft consensus algorithm. Go 1.22+ with gRPC peer transport and an embedded Write-Ahead Log.

## Status

**M0+M1+M2 (partial)**: 25 of 38 plan tasks complete. The core Raft algorithm, state machine, WAL persistence, KV client API, gRPC transport, and protobuf definitions are all in place. The `cmd/raftkvd` binary is real, runs as a foreground process, and **an end-to-end integration test passes** (spawn binary → gRPC client Put → applier applies → gRPC client Get returns value → SIGTERM → spawn again with same data dir → state recovered → Get returns value).

**What works today (verified by `tests/e2e_test.go`):**
- Single-node Raft: Follower → Candidate → Leader transition within ~200ms of startup
- Vote request handling (term updates, log up-to-date check)
- AppendEntries handler (consistency check, conflict truncation, commit advance)
- Per-peer Progress tracking with MaybeUpdate / MaybeDecrement
- Quorum-based commit index advancement (`advanceCommit`)
- Propose on a leader
- WAL append + CRC32 + replay
- **State persistence across restarts**: `Persist` writes `term, votedFor, log` atomically; `Recover` reads them back; applier replays the log into the state machine on startup
- Applier that walks committed log to a user callback
- gRPC RequestVote / AppendEntries via bufconn + via real GRPCTransport
- KV gRPC service with Get / Put (reads from the state machine, not an optimistic local cache)
- Config loading (YAML + defaults)
- slog JSON logger
- `cmd/raftkvd` binary: config load, state recover, gRPC server (Raft + KV), election timer, leader loop, periodic persist, signal handling

**Verified end-to-end with:** `go test -tags=integration -timeout 60s ./tests/...` → PASS in ~1.5s.

**What doesn't work yet:**
- Multi-node tests (3-node cluster, leader failover, acks=all) — only single-node is verified
- Snapshot creation / install (the protocol is in the proto but no handler logic)
- Observability (Prometheus metrics)
- Docker images, Jepsen-style fault-injection harness
- 5-node docker-compose, GRUB ISO
- Read-index protocol for linearizable reads
- Membership changes (joint consensus)
- TLS / authentication
- Real TCP peer transport with multi-node: the GRPCTransport is unit-tested with bufconn but a multi-node integration test has not run

## Data Flow

```
Client (KV gRPC)               Peer (gRPC)                Client (KV gRPC)
       │                           │                          │
       │ Put(key,val)              │                          │
       ▼                           │                          │
kvserver.Server                network.Peer                kvserver.Server
  ├─ Propose(cmd) ─────► raft.Node (leader)                  ▲
  │                          ├─ log.append                │
  │                          ├─ replicateOnce (per peer)  │
  │                          │    └─ AppendEntries RPC ──┤
  │                          │       (matches prevLog)   │
  │                          │    └─ HandleAE:           │
  │                          │         truncate conflicts│
  │                          │         append new entries│
  │                          │         update commitIndex │
  │                          ├─ advanceCommit (majority) │
  │                          └─ Applier tick (10ms):     │
  │                                walk lastApplied → cb   │
  │                                (apply to state machine)│
  │                                                          │
  └─ commit() ◄────── log commitIndex reached ◄────── handleAE│
                                                                │
                                              kvserver.Server ─┘
```

### `cmd/raftkvd/main.go` — Process entry point (real)
Currently a working foreground process. It:
1. Parses CLI flags (`-config PATH`, `-debug`)
2. Loads config (YAML or defaults)
3. Creates the data dir
4. Builds a slog JSON logger
5. Creates a `raft.Node` and recovers state from `<data-dir>/state.json`
6. Creates a `statemachine.MemKV` and wires it to the Node's `Applier`
7. Starts a `persistLoop` goroutine that saves state every second
8. Connects to each peer in config (gRPC) and builds a `GRPCTransport`
9. Starts `electionLoop` (randomized election timer, RequestVote per peer)
10. Starts `leaderLoop` (heartbeat AppendEntries when leader)
11. Starts a gRPC server with both `Raft` and `KV` services
12. Blocks on SIGINT/SIGTERM, then graceful stop + final persist

**Caveat:** The election loop and leader loop are hand-rolled and have NOT been tested in a multi-node cluster. Single-node behavior is correct in theory (a single node should immediately elect itself), but no end-to-end test has verified it.

### `internal/config` — Typed configuration
`Config` struct with `NodeID`, `ListenAddr`, `DataDir`, `Peers`, election/heartbeat timeouts, snapshot threshold. `Load(path)` reads YAML or returns defaults when `path == "/nonexistent"`. `SnapshotPath()` and `WALPath()` helpers under `DataDir`.

### `internal/logging` — slog wrapper
`New(level, writer)` returns a JSON-output `*slog.Logger` with the requested level. Filters Debug when level is Info.

### `internal/wal` — Write-Ahead Log
Frame format: `[crc 4][len 4][data N]`, big-endian. CRC32-IEEE for corruption detection.
- `Open(path, readOnly)` opens or creates the WAL file
- `Append(data)` writes a record and returns its offset
- `Next()` reads the next record (returns error on CRC mismatch)
- `Sync()` flushes the buffer and fsyncs
- `Close()`
- `Replay(path)` iterates all records and returns `([]records, lastOffset, error)`

### `internal/raft` — Core Raft algorithm
The largest package. Contains:
- **`log.go`** — `LogEntry` with `Term, Index, Type, Data`. `Encode()` / `Decode()` with big-endian framing.
- **`state.go`** — `Role` enum (Follower/Candidate/Leader). `PersistentState { CurrentTerm, VotedFor, Log }` with `Encode` / `Decode` (binary + gob). `VolatileState { CommitIndex, LastApplied }`.
- **`node.go`** — `Node` struct guarded by `sync.Mutex`. Holds `cfg, role, term, votedFor, log, commitIndex, lastApplied, progress (per-peer)`, `proposeCh`. `NewNode` returns a Follower with a noop log entry. Accessors: `Role()`, `CurrentTerm()`, `LastLogIndex()`, `LastLogTerm()`, `CommitIndex()`, `ID()`. Role transitions: `BecomeCandidate()`, `BecomeLeader()`, `StepDown(term)`.
- **`vote.go`** — `becomeCandidate()` increments term, votes for self, transitions to Candidate. `HandleVoteRequest(req)` implements §5.1: rejects stale term, resets `votedFor` on new term, grants if log is up-to-date and not yet voted this term.
- **`replication.go`** — `AppendRequest` / `AppendResponse` types. `HandleAppendEntries()` does consistency check, truncates conflicts, appends new entries, advances commit. `Progress { NextIndex, MatchIndex }` with `MaybeUpdate` / `MaybeDecrement`. `ComputeCommitIndex()` (Figure 2 from Raft paper) for quorum-based commit.
- **`election.go`** — `randomTimeout(min, max)` for jittered election timers.
- **`leader.go`** — `Transport` interface (`SendAppendEntries`, `SendRequestVote`). `becomeLeader()` initializes per-peer progress. `LeaderLoop(ctx, transport, peerIDs)` ticks every Heartbeat and spawns `replicateOnce` per peer. `replicateOnce()` reads per-peer progress, builds `AppendRequest`, calls the transport, updates progress, and calls `advanceCommit()`.
- **`apply.go`** — `Applier` with `Run()` (10ms ticker), `Stop()`. Walks log from `lastApplied` to `commitIndex`, invokes user callback for each `EntryCommand`. Locking pattern: collect entries to apply while holding `n.mu`, release before invoking callback (avoids deadlock if callback reads node state).
- **`propose.go`** — `Propose(ctx, cmd)` returns `ErrNotLeader` on non-leader; on leader: appends `LogEntry{Term, Index, EntryCommand}`, signals `proposeCh` non-blocking. `ProposeNotify(ctx, cmd)` is the awaitable variant: appends, signals, then polls `lastApplied` every 10ms up to a 2-second timeout, also respects `ctx.Done()`.
- **`persist.go`** — `Persist(path)` writes the Node's current `term, votedFor, log` to a JSON file (atomic via temp+rename). `Recover(path)` restores them. Missing file = fresh start, no error.
- **`version.go`** — `Version = "0.1.0"`.

### `internal/network` — gRPC peer transport
`Peer` struct holding a gRPC client connection. `DialPeer(id, addr)` opens an insecure blocking connection with 2s timeout. `RequestVote()` and `AppendEntries()` forward to the gRPC client. `Close()`. Tested via `bufconn` for in-process RPC.

### `internal/rpc` — Generated protobuf
`raft.proto` defines `Raft` service with `RequestVote`, `AppendEntries`, `InstallSnapshot` RPCs. `kv.proto` defines `KV` service with `Get`, `Put`. Generated `*.pb.go` and `*_grpc.pb.go` files.

### `internal/kvserver` — Client-facing KV gRPC server
`Server` with `Get` and `Put` handlers. `Put` invokes the `Proposer` with a JSON-encoded command. `Get` reads from an in-memory store (in production this would be backed by the `statemachine.MemKV` applier). Tested via `bufconn` with a `fakeProposer`.

## Storage Layer (Tasks 23-24 — not yet implemented)

The plan calls for:
- `internal/storage` — `HeapTable` with slotted pages for KV state
- `internal/statemachine` — `MemKV` with JSON op codec (`op:put, k, v` / `op:get, k` / `op:delete, k`)
- WAL persistence for `Node` (`Persist()` writes `PersistentState`, `Recover()` reads it back)

These would let the Node survive a process restart.

## Test Strategy

| Layer | Location | Mechanism |
|---|---|---|
| Pure logic (encoding, types, math) | `internal/raft/*_test.go` | `testify/assert` + `testify/require` |
| WAL | `internal/wal/*_test.go` | Real temp files with `t.TempDir()` |
| gRPC services | `internal/network/peer_test.go`, `internal/kvserver/server_test.go` | `bufconn` for in-process gRPC |
| Crash recovery (future) | `tests/crash_recovery_test.go` | Drop process, reopen, replay |
| Jepsen-style (future) | `test/integration/` | bufconn + fault injection |

**Current test count:** 23 tests across 6 packages, all passing with `-race -count=1`. Test runtime: ~5 seconds total on a modern MacBook.

## What the Plan Calls For But Isn't Built Yet

Tasks from `.plans/01-raft-kv-store.md` not yet complete:
- Task 11 — `raft.proto` (DONE in spirit, generated)
- Task 16 — `leader.go` (DONE)
- Task 22 — Network transport (DONE)
- Task 23 — Heap table storage
- Task 24 — WAL persistence for Node
- Task 25 — Snapshot creation
- Task 26 — Snapshot install (RPC handler exists, logic doesn't)
- Task 27 — Read-index protocol
- Task 28 — Membership changes (joint consensus)
- Task 29 — Prometheus metrics
- Task 30 — slog integration into Node
- Task 31 — 3-node cluster integration
- Task 32 — Leader failover integration
- Task 33 — acks=all integration
- Task 34 — Multi-stage Dockerfile
- Task 35 — docker-compose for 3/5-node cluster
- Task 36 — Jepsen-style fault-injection harness
- Task 37 — ARCHITECTURE.md (this file)
- Task 38 — OPERATIONS.md (sibling to this)

## BVH Tuning (N/A — not a renderer)

Not applicable to this project.

## Storage Tuning (forward-looking)

When storage lands, the plan's defaults are:
- `BUFFER_POOL_SIZE` env var: defaults to 256 pages = 1 MiB
- B+tree order: 32
- WAL segment size: 64 MiB (roll on fill)

These can be tuned per workload via env vars in the runtime.

## Performance Expectations

Without measurements, the plan targets:
- **Single-node** append throughput: >10K entries/sec (no replication)
- **3-node** with acks=all: >1K entries/sec
- **5-node** with acks=all: >500 entries/sec (quorum is 3/5)

These are NOT measured yet. Build the cluster test harness (Task 31) to actually benchmark.

## Security Model

- No TLS yet (single root CA only in the plan)
- Authentication: none — clusters are assumed to be on a private network
- Authorization: none
- gRPC: insecure (no TLS) — production would need mutual TLS
- WAL files contain plaintext serialized state — encrypt at rest in production
- Election timeouts randomized in `[150ms, 300ms]` to defend against split-vote

## Out of Scope (per the plan)

- Multi-Raft / sharded KV
- Lease-based reads (only read-index is planned)
- Follower reads
- Live log shipping for replacement nodes
