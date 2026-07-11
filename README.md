# raft-kv-store

A replicated, linearizable key-value store using the Raft consensus algorithm. Go 1.22+ with gRPC peer transport and an embedded Write-Ahead Log.

> **Status: 21 of 38 plan tasks complete.** The core Raft algorithm (election, log replication, quorum commit) is implemented and tested, but the `main.go` entry point, storage layer, snapshotting, and Docker images are not yet built. See [ARCHITECTURE.md](ARCHITECTURE.md) for what's done and [OPERATIONS.md](OPERATIONS.md) for the roadmap to production-ready.

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
```

**23 tests, 6 packages, ~5s runtime.**

## What Works Today

| Component | Status | Tests |
|---|---|---|
| Election (random timeout, becomeCandidate, RequestVote) | ✅ | 3 |
| Log replication (AppendEntries, per-peer Progress, quorum commit) | ✅ | 4 |
| State persistence (PersistentState + VolatileState codec) | ✅ | 1 |
| Applier (ticker-based, log walk) | ✅ | 1 |
| Propose / ProposeNotify (leader-side command submission) | ✅ | 2 |
| WAL (CRC32-framed records, replay) | ✅ | 2 |
| gRPC definitions (raft.proto + kv.proto) | ✅ | n/a (generated) |
| Peer gRPC client (bufconn-tested) | ✅ | 1 |
| KV gRPC server (Get/Put) | ✅ | 1 |
| Config (YAML load + defaults) | ✅ | 2 |
| Logging (slog JSON with level filter) | ✅ | 2 |

## What Doesn't Work Yet

- **No real `main()`**: `raftkvd` just prints a stub message
- **No 3/5-node cluster startup** — `docker-compose` not written
- **No storage layer** — `HeapTable` not implemented
- **No WAL persistence for Node** — restart loses state
- **No snapshotting** — `InstallSnapshot` RPC exists in proto but no handler
- **No TLS** — gRPC uses `WithInsecure()`
- **No observability** — no Prometheus, no slog integration
- **No Jepsen-style fault-injection tests**

See [OPERATIONS.md](OPERATIONS.md) § Roadmap for the full remaining 17 tasks.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the component diagram, data flow, and what each package does.

## Repository Layout

```
01-raft-kv-store/
├── cmd/raftkvd/main.go          (stub: prints "raftkvd starting")
├── internal/
│   ├── config/                  typed Config + YAML load
│   ├── logging/                 slog JSON logger
│   ├── wal/                     Write-Ahead Log with CRC32 + replay
│   ├── raft/                    core Raft (Node, vote, replication, leader, apply, propose)
│   ├── rpc/                     generated protobuf (raft.proto, kv.proto)
│   ├── network/                 gRPC peer client
│   └── kvserver/                client-facing KV gRPC service
├── ARCHITECTURE.md              what we built, in detail
├── OPERATIONS.md                build, test, run, troubleshoot, roadmap
├── Makefile                     build, test, lint, run, clean
├── .golangci.yml                linter config
├── .github/workflows/ci.yml     CI pipeline
└── go.mod                       Go 1.22, testify, gRPC, protobuf, yaml
```

## License

MIT (per the overall project license).
