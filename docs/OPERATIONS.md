# Operations — raft-kv-store

Build, test, run, and operate a single-node or multi-node Raft cluster. **Note: this project is not yet a complete deployable binary** — the core Raft algorithm is implemented and tested, but the `main.go` entry point, storage layer, snapshotting, and Docker images are not yet built.

## Status

21 of 38 plan tasks complete. The code compiles and all 23 unit tests pass. There is **no working binary** yet — `make build` produces a `raftkvd` that just prints "raftkvd starting" and exits.

## Building

### Prerequisites

| Tool | Version | Install |
|---|---|---|
| Go | 1.22+ | `brew install go` |
| protoc | 35.x | `brew install protobuf` |
| protoc-gen-go | latest | `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` |
| protoc-gen-go-grpc | latest | `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest` |

The two `go install` commands put the plugins in `$(go env GOPATH)/bin` (usually `~/go/bin`). They must be in `$PATH` for `protoc` to find them.

### Build

```bash
cd ~/projects-2026/01-raft-kv-store

# Generate Go code from .proto (only needed if you change .proto)
PATH=$PATH:$(go env GOPATH)/bin \
  protoc --go_out=. --go-grpc_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_opt=paths=source_relative \
    internal/rpc/raft.proto internal/rpc/kv.proto

# Build the binary
make build          # produces bin/raftkvd

# Run the (stub) binary
./bin/raftkvd       # prints "raftkvd starting" and exits
```

### Verify

```bash
make test           # runs `go test -race -count=1 ./...`
```

Expected output: 6 packages pass, 23 tests pass, ~5s runtime.

## Testing

```bash
make test
# or, more explicitly:
go test -race -count=1 ./...

# With verbose output for a single package
go test -v ./internal/raft/

# Run a single test
go test -v -run TestBecomeCandidateIncrementsTerm ./internal/raft/
```

### Lint

```bash
make lint
```

Runs `golangci-lint` over all packages. Currently clean.

### Test Coverage (per package)

| Package | Tests | What it covers |
|---|---|---|
| `internal/config` | 2 | default values, missing-file error |
| `internal/kvserver` | 1 | Put-then-Get roundtrip with bufconn gRPC |
| `internal/logging` | 2 | Info/Debug level filtering |
| `internal/network` | 1 | Peer gRPC client with bufconn |
| `internal/raft` | 15 | LogEntry codec, role, state, election, vote, AppendEntries, Progress, quorum commit, Propose, Applier, NewNode defaults |
| `internal/wal` | 2 | roundtrip records, CRC corruption detection, replay |

**Total: 23 tests in 6 packages, ~5s runtime.**

## Running (NOT YET WORKING)

The current `cmd/raftkvd/main.go` is a stub. A real entry point would do:

1. Parse CLI flags (node ID, data dir, listen addr, peer addrs)
2. Open the WAL at `<data-dir>/wal`
3. Replay WAL to recover Node state
4. Open gRPC server on `listen_addr` with both `Raft` and `KV` services
5. Start the election timer
6. Block on SIGINT / SIGTERM

None of this is wired up. To make `raftkvd` work end-to-end, you would need to:

```go
// in cmd/raftkvd/main.go
func main() {
    cfg, err := config.Load(*configPath)
    // ... error handling ...

    wal, err := wal.Open(cfg.WALPath(), false)
    // ... replay for recovery ...

    node := raft.NewNode(raft.NodeConfig{
        ID: cfg.NodeID,
        ElectionMin: cfg.ElectionMin,
        ElectionMax: cfg.ElectionMax,
        Heartbeat:   cfg.Heartbeat,
    })
    // ... restore state from WAL ...

    // Set up gRPC server with Raft + KV services
    // Set up peer connections for replication
    // Start election timer + leader loop
    // Block on signal
}
```

This is approximately Task 23+ in the plan, and is what `cmd/raftkvd/main.go` would need to be replaced with.

## Configuration

Config is loaded from a YAML file or defaults if not specified:

```yaml
node_id: 1
listen_addr: "127.0.0.1:7000"
data_dir: "data"
election_timeout_min: 150ms
election_timeout_max: 300ms
heartbeat_interval: 50ms
snapshot_every_n_entries: 10000
peers:
  - {id: 2, url: "http://node2:7000"}
  - {id: 3, url: "http://node3:7000"}
```

CLI flag parsing is not yet implemented — the current stub ignores all flags.

## Known Limitations

1. **No real main()**: `raftkvd` just prints a message and exits
2. **No cluster startup**: There's no logic to bring up multiple nodes together
3. **No WAL persistence for Node state**: WAL exists but is not yet wired to Node state
4. **No snapshotting**: `InstallSnapshot` RPC exists in the proto but the handler doesn't
5. **No real network transport**: `Transport` is an interface; only a bufconn-based test impl exists
6. **No TLS**: gRPC uses `WithInsecure()` — only safe on private networks
7. **No observability**: No Prometheus, no slog integration into the Node
8. **No follower reads**: All reads go through the leader (via `KV.Get`)
9. **No leases**: Only read-index is planned (not yet implemented)
10. **No live log shipping for replacement nodes**: A new node can't join from scratch yet

## Performance (Not Yet Measured)

Plan targets (NOT validated):
- Single-node append: >10K entries/sec
- 3-node acks=all: >1K entries/sec
- 5-node acks=all: >500 entries/sec

To actually measure, implement the Jepsen-style integration test harness (Task 36 in the plan).

## Roadmap to Production-Ready

Following the plan, the remaining 17 tasks in priority order:

1. **Task 24**: WAL persistence for `Node` state (Persist/Recover methods)
2. **Task 23**: Heap table storage + state machine (`statemachine.MemKV`)
3. **Task 16+**: Wire `kvserver.Server` to the actual Node applier
4. **Task 22 (production)**: TCP-based `Transport` implementation (currently just bufconn)
5. **Task 31**: 3-node cluster integration test
6. **Task 32**: Leader failover test
7. **Task 33**: acks=all test
8. **Task 25-26**: Snapshot creation / install
9. **Task 27-28**: Read-index + membership changes
10. **Task 29-30**: Prometheus + slog integration
11. **Task 34-36**: Docker images + docker-compose + Jepsen harness

## Troubleshooting

### `protoc-gen-go: program not found`

Install protoc-gen-go:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
export PATH=$PATH:$(go env GOPATH)/bin
```

### `module not found` for `github.com/moggan1337/raft-kv-store/internal/rpc`

Run `go mod tidy` after generating the gRPC code.

### `expected 'package', found 'EOF'` after a `write` tool call

This is a transient race in the file-write tool. The file is correct, but `go test` may run while the file is mid-write. Re-run the test.

### `undefined: sort` or `undefined: time` after creating a file

Missing import. Add the import to the imports block of the affected file.

### `internal/raft/... no test files` after creating a test file

Verify the test file's package declaration matches the production code's package (`package raft`).

## CI

GitHub Actions: `.github/workflows/ci.yml` runs on push/PR:
- `go mod download`
- `make build`
- `make test`
- `make lint` via `golangci/golangci-lint-action@v6`

This will pass on a fresh clone. The build/test/lint will fail only if the code is broken.

## Out of Scope

Per the plan, the following are explicitly excluded from this project:
- Multi-Raft / sharded KV
- TLS mutual auth (single root CA only)
- Lease-based reads
- Follower reads
- Live log shipping for replacement nodes
