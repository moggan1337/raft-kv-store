# raft-kv-store

A replicated, linearizable key-value store using the Raft consensus algorithm. Go 1.22+ with gRPC peer transport and an embedded Write-Ahead Log.

> **Status: 25 of 38 plan tasks complete.** The core Raft algorithm, state machine, WAL persistence, KV client API, gRPC transport, and protobuf definitions are all in place. The `cmd/raftkvd` binary compiles and runs as a real foreground process. **An end-to-end integration test passes** (spawn binary → gRPC Put → gRPC Get → SIGTERM → restart with same data dir → gRPC Get recovers the value). See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for what's done and [docs/OPERATIONS.md](docs/OPERATIONS.md) for the roadmap to production-ready.

## Quickstart

```bash
# Prerequisites
brew install go protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Clone, build, test
git clone https://github.com/moggan1337/raft-kv-store
cd raft-kv-store
make build && make test

# End-to-end test (spawns the binary, real gRPC client, restarts it)
go test -tags=integration -timeout 60s ./tests/...

# Run the binary
./bin/raftkvd          # single-node, defaults
./bin/raftkvd -listen 127.0.0.1:17000 -data /tmp/raft-data
```

**27 unit tests + 1 e2e test, 7 packages, ~5s + 1.5s.**

## What Works Today

| Component | Status | Tests |
|---|---|---|
| Election (random timeout, becomeCandidate, RequestVote) | ✅ | 3 |
| Log replication (AppendEntries, per-peer Progress, quorum commit) | ✅ | 4 |
| State persistence (PersistentState + VolatileState codec + JSON Persist/Recover) | ✅ | 1+3 |
| Applier (ticker-based, log walk) | ✅ | 1 |
| Propose / ProposeNotify (leader-side command submission) | ✅ | 2 |
| WAL (CRC32-framed records, replay) | ✅ | 2 |
| State machine (MemKV with JSON op codec, snapshot/restore) | ✅ | 4 |
| gRPC definitions (raft.proto + kv.proto) | ✅ | n/a (generated) |
| Peer gRPC client (bufconn-tested) | ✅ | 1 |
| GRPCTransport (real production transport) | ✅ | 3 |
| KV gRPC server (Get/Put) | ✅ | 1 |
| Config (YAML load + defaults) | ✅ | 2 |
| Logging (slog JSON with level filter) | ✅ | 2 |
| `cmd/raftkvd` binary (loads config, recovers state, serves gRPC, runs election/leader loops, handles signals) | ✅ | n/a |

## What Doesn't Work Yet

- **No end-to-end integration test** — no test has connected a gRPC client to the binary
- **No 3/5-node cluster startup** — `docker-compose` not written
- **No snapshotting** — `InstallSnapshot` RPC exists in proto but no handler
- **No real TCP peer transport end-to-end** — gRPC transport works in tests via bufconn
- **No TLS** — gRPC uses `WithInsecure()`
- **No observability** — no Prometheus, no slog integration into the Node itself
- **No Jepsen-style fault-injection tests**

See [OPERATIONS.md](OPERATIONS.md) § Roadmap for the full remaining 15 tasks.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the component diagram, data flow, and what each package does.

## Repository Layout

```
01-raft-kv-store/
├── cmd/raftkvd/main.go          real entry: config, recover, gRPC, election, leader
├── internal/
│   ├── config/                  typed Config + YAML load
│   ├── logging/                 slog JSON logger
│   ├── wal/                     Write-Ahead Log with CRC32 + replay
│   ├── raft/                    core Raft (Node, vote, replication, leader, apply, propose, persist)
│   ├── rpc/                     generated protobuf (raft.proto, kv.proto)
│   ├── network/                 gRPC peer client + GRPCTransport
│   ├── statemachine/            MemKV (state machine)
│   └── kvserver/                client-facing KV gRPC service
├── docs/
│   ├── ARCHITECTURE.md          what we built, in detail
│   └── OPERATIONS.md            build, test, run, troubleshoot, roadmap
├── Makefile                     build, test, lint, run, clean
├── .golangci.yml                linter config
├── .github/workflows/ci.yml     CI pipeline
└── go.mod                       Go 1.22, testify, gRPC, protobuf, yaml
```

## License

MIT (per the overall project license).
