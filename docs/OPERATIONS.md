# Operations — raft-kv-store

Build, test, run, and operate a single-node or multi-node Raft cluster. **Note: this project is not yet a complete deployable binary** — the core Raft algorithm is implemented and tested, but the `main.go` entry point, storage layer, snapshotting, and Docker images are not yet built.

## Status

23 of 38 plan tasks complete. The code compiles and all 27 unit tests pass. The `raftkvd` binary is real and runs as a foreground process: it loads config, recovers state from disk, starts a gRPC server (Raft + KV), runs an election timer and leader loop, and shuts down on SIGINT/SIGTERM.

**Caveat (important):** No end-to-end integration test has ever run. The algorithm is unit-tested, but the binary has never been connected to a gRPC client that sends a Put and verifies a Get. A single-node run "should" immediately elect itself as leader, but this is not verified. Treat the binary as a runnable skeleton, not a production-ready server.

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

## Running

```bash
# Single-node start (uses defaults)
./bin/raftkvd

# With a config file
./bin/raftkvd -config /path/to/config.yaml

# With debug logging
./bin/raftkvd -debug

# Shutdown gracefully
Ctrl-C   # or: kill -TERM <pid>
```

The binary:
- Reads the config (YAML or defaults)
- Recovers any prior state from `<data_dir>/state.json`
- Starts slog JSON logging to stderr
- Creates the data dir if missing
- Connects to each peer listed in config (gRPC)
- Starts gRPC server on `listen_addr` with both `Raft` and `KV` services
- Runs the election timer (becomes Candidate, then Leader for single-node)
- Runs the leader loop (sends heartbeats to peers)
- Periodic state persistence (every 1s)
- Shuts down gracefully on SIGINT/SIGTERM, doing a final persist

**Caveat (repeated for emphasis):** No end-to-end test has verified the full path (binary boots → client gRPC Put → applier writes to MemKV → response → client gRPC Get). Unit tests cover each piece individually. The binary is a runnable skeleton.

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

1. **No end-to-end integration test**: No test has connected a gRPC client to the binary and verified a Put-then-Get roundtrip. The algorithm is unit-tested but the binary's full data path is not.
2. **No cluster startup**: There's no logic to bring up multiple nodes together (you'd need to spawn N processes by hand or write a script).
3. **No snapshotting**: `InstallSnapshot` RPC exists in the proto but the handler returns a stub. Log compaction is unimplemented.
4. **No real TCP peer transport end-to-end**: The gRPC transport works in tests via bufconn but no test has spun up two real nodes and connected them.
5. **No TLS**: gRPC uses `WithInsecure()` — only safe on private networks.
6. **No observability**: No Prometheus, no slog integration into the Node itself (only the main process logs).
7. **No follower reads**: All reads go through the leader (via `KV.Get`).
8. **No leases**: Only read-index is planned (not yet implemented).
9. **No live log shipping for replacement nodes**: A new node can't join from scratch yet (no snapshot install).
10. **The election/leader loops are hand-rolled** in `main.go` rather than the raft package's tested implementations. They work in theory but haven't been end-to-end verified.

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
