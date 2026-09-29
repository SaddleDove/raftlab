# Developer guide

## Requirements

Go 1.22 or newer, and optionally `make` and `staticcheck`. There are no module dependencies, so nothing needs downloading.

## Getting started

```bash
git clone https://github.com/SaddleDove/raftlab
cd raftlab

go test ./...
go run ./cmd/raftlab gate-m0
sh scripts/gates.sh
```

`scripts/gates.sh` runs every gate plus the supporting checks and rewrites `docs/PROGRESS.md`. That file is evidence, and it is regenerated rather than edited.

## Where things live

- `internal/raft` is the protocol. It must not import `time`, `net`, the simulator, the checker, the transport or the storage package.
- `internal/sim` is the deterministic world the protocol runs in.
- `internal/storage` holds the WAL and the in-memory store.
- `internal/check` holds the history, the KV model and the two searches.
- `internal/unsafe` names the broken variants.
- `cmd/raftlab` is the CLI, including the gates.

## Rules

Advance one milestone at a time. A gate that does not pass stops the milestone.

When a test fails intermittently, print the seed and the shrunk scenario and commit it under `reports/failures/`. Do not add a sleep, a retry or a looser threshold to make a flake go away.

When an invariant fires, change the protocol. Do not edit the invariant.

Replay stability is itself a test. The same seed, node count and run length must produce the same canonical event log, so two runs are compared by hash.

Each actor's random stream is derived from the seed and tagged with the actor's identity. Adding a node must not perturb the streams of nodes that already existed.

Keep comments scarce and pin exported behaviour with tests instead.

## Common tasks

```bash
go run ./cmd/raftlab gate-m0 -seeds 3 -until 200
go run ./cmd/raftlab gate-m5 -seeds 200
go run ./cmd/importcheck
go test ./internal/storage -run Torn -count=1
go test -race ./...
```

## Continuous integration

`.github/workflows/ci.yml` runs gofmt, the import check, `go vet`, the test suite with the race detector, and every gate from M0 to M8. The suite finishes in well under a minute, so there is no reason to gate only the first milestone.

## Adding a fault kind

Declare it in `DeclaredFaults`, implement the matching branch in `Matrix`, and add it to `WiredFaults`. `internal/sim/faults_test.go` asserts that the two lists are consistent and that the unwired set is documented, so the split cannot drift.

## Adding a variant

Add the case to `raft.Bug` and its name in `AllBugs`, wire the behaviour behind a `Config.Bug` check, and write a pair of tests in `internal/raft/catch.go` style: one asserting the correct implementation does not exhibit the bug, one asserting the broken one does. `gate-m5` requires every name in `AllBugs` to be caught.
