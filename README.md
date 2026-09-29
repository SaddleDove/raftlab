# RaftLab

[![ci](https://github.com/SaddleDove/raftlab/actions/workflows/ci.yml/badge.svg)](https://github.com/SaddleDove/raftlab/actions/workflows/ci.yml)

Consensus code that ships with its own adversary.

Getting a Raft implementation to converge on a quiet network is not the hard part. RaftLab is built around the rest of the question, which is whether the thing still holds when the network is not quiet, and whether you would know if it did not. So the repository carries three things beyond the protocol: a deterministic simulator with a virtual clock, a fault injector, and a set of checkers that turn correctness into a test signal. A fourth thing keeps the checkers honest, a set of deliberately broken Raft variants that every checker is required to catch.

## What is in here

- A Raft core that can only see the world through injected interfaces, so every source of nondeterminism is controllable
- A single-threaded event simulator with a virtual clock, per-actor random streams and byte-identical replay from a seed
- A fault matrix that corrupts the network and the nodes on a schedule
- Nine runtime invariants evaluated on every state change, send, persist and apply
- A client history and two linearizability search strategies that have to agree
- Eight bug variants wired into the core behind a test-only switch, each one paired with an invariant that must fire
- A write-ahead log with CRC32-C framing, so a torn tail is recovered as a valid prefix and a bad checksum is a hard error

It is a lab, not a library. Nothing here is meant to be embedded in a production system, and the README is not claiming a performance or maturity story. If you want a hardened, deployed consensus implementation, use etcd or HashiCorp Raft. If you want to see one being broken on purpose until the checks catch it, keep reading.

## Quick start

Go 1.22 or newer. There are no module dependencies.

```bash
git clone https://github.com/SaddleDove/raftlab
cd raftlab

go test ./...
go run ./cmd/raftlab gate-m0
go run ./cmd/raftlab gate-m5 -seeds 200
sh scripts/gates.sh      # regenerates docs/PROGRESS.md
```

`make ci` runs the import check, `go vet`, the test suite with the race detector, and all nine gates. `make evidence` rewrites `docs/PROGRESS.md`.

## Gates

Each milestone has a command that either passes or does not, and the milestone after it is not started until it does. Raw output for the current tree lives in [`docs/PROGRESS.md`](docs/PROGRESS.md), which is produced by `scripts/gates.sh` rather than written by hand.

| Gate | Command | What it asserts |
|------|---------|-----------------|
| M0 | `raftlab gate-m0` | Three silent nodes, same seed, SHA-256 of the canonical event log identical across two runs |
| M1 | `raftlab gate-m1` | 1000 seeds, invariants I1–I3 held, a leader converged within 20 election timeouts |
| M2 | `raftlab gate-m2` | A quiet-network KV history is linearizable |
| M3 | `raftlab gate-m3` | 1000 crash and restart seeds, I7 held |
| M4 | `raftlab gate-m4` | A partitioned minority acknowledges zero writes, timeline written to HTML |
| M5 | `raftlab gate-m5` | Random fault seeding across 3 and 5 nodes, plus all eight variants caught, plus 50 replays |
| M6 | `raftlab gate-m6` | A lagging node catches up through InstallSnapshot after compaction |
| M7 | `raftlab gate-m7` | Joint consensus, add and remove, dual quorum enforced |
| M8 | `raftlab gate-m8` | ReadIndex and lease reads included in the linearizable history |

The whole suite runs in well under a minute, which is why CI runs all of it rather than just M0.

## Architecture rules

Three rules hold the design together.

1. `internal/raft` observes the world only through `Transport`, `Clock`, `LogStore`, `StateMachine`, `Timeouts` and `Rand`. It never imports the simulator, the checker, the transport or the storage packages, and it contains no call to `time.Now` or `time.Sleep`.
2. All nondeterminism enters through those interfaces. The simulator supplies a virtual clock, a single-threaded scheduler and per-node random streams derived from one seed.
3. Dependencies point one way. The simulator drives the node; the node does not know the simulator exists.

`cmd/importcheck` walks the import graph and fails the build when a rule is broken, and `internal/raft/import_test.go` enforces the same list from inside the test suite.

## Determinism

Every event is ordered by virtual time, then by kind, then by insertion sequence, and each actor draws from its own derived stream. Adding a node does not perturb the streams of existing nodes. The event log is canonicalised and hashed, so byte-identical replay is a test rather than a hope, and `gate-m0` is that test.

## Faults

Sixteen fault kinds are declared. Ten are wired into the matrix and can be triggered by a rule; the other six are enum values with a name and no implementation, and the docs say so rather than implying otherwise. `internal/sim/faults_test.go` asserts the split, so the two lists cannot drift apart quietly.

| Wired | Kind | What it does |
|-------|------|--------------|
| yes | `delay_const` | fixed extra latency on an edge |
| yes | `delay_uniform` | latency drawn from a range |
| yes | `delay_heavy` | heavy-tailed latency |
| yes | `drop` | message loss with a probability |
| yes | `dup` | duplicate delivery |
| yes | `reorder` | holds a message back so a later send overtakes it |
| yes | `partition_sym` | group against group, both directions |
| yes | `partition_asym` | group against group, one direction |
| yes | `crash_stop` | node stops and stays down |
| yes | `crash_restart` | node stops and comes back from its store |
| no | `bitflip` | wire corruption; outside the model Raft assumes |
| no | `torn_write` | covered at the storage layer by `internal/storage` tests |
| no | `slow_disk` | needs a cost model for `Sync` |
| no | `election_storm` | needs scheduler-level burst scheduling |
| no | `clock_skew` | needs per-node clocks rather than one virtual clock |
| no | `stale_rejoin` | needs per-node storage forks on restart |

Rules can be composed and expire at a virtual time, so a scenario is a list of intervals rather than a single switch.

## Invariants

The monitor is not a post-processing step. It watches the same events the simulator sees and reports a violation the moment one happens.

| Code | Invariant |
|------|-----------|
| I1 | at most one leader per term |
| I2 | terms never decrease, in memory or on disk |
| I3 | a vote is persisted before the response is sent |
| I4 | same index and term implies the same command |
| I5 | a leader's log contains every committed entry |
| I6 | no index is applied with two different commands |
| I7 | committed entries survive crashes |
| I8 | a minority partition acknowledges no writes |
| I9 | apply never runs ahead of commit |

A violation means the protocol gets changed. The invariant does not get relaxed, and a flaky test does not get a sleep or a retry.

## Linearizability

Client operations are recorded as intervals and checked against a reference KV model. Two search strategies decide a history.

- `order` backtracks over the orders consistent with real-time precedence. Complete, and the one that runs on every history.
- `bitmask` searches breadth-first over subsets. Complete and heuristic-free, which makes it a useful second opinion, but its state space is two to the number of operations, so it is capped at 16 operations.

`CrossCheck` runs both when the history fits the cap and reports which strategies actually ran. Above the cap the verdict comes from `order` alone and says so, in the checker name and in the detail field. A large history is never silently relabelled as a cross-check. This matters because the gates print the checker that decided them, and `gate-m8` histories are past the cap.

Both strategies share one KV model, so agreement between them rules out a search bug but not a modelling bug. That limitation is worth saying out loud once.

## Variants

`internal/unsafe` names eight classic Raft bugs: not persisting `votedFor`, not persisting a new term, ignoring a higher term in AppendEntries, committing an old-term entry on a bare majority, skipping the log up-to-date check when granting a vote, applying before commit, counting the leader twice toward quorum, and failing to reset the election timer. `gate-m5` requires all eight to be caught with a named violating invariant, and reports any that slip through.

The switch itself is deliberate and worth flagging for readers: the variants are wired into the core through `Config.Bug`, because that keeps one code path under one simulator. The cost is that the shipped node contains test-only branches. `Bug` is never set outside tests and the gates, and `internal/unsafe` exists to name them. A restructure into variant wiring behind a hook interface would remove the branches; it has not been done.

## Layout

```
internal/raft/         election, replication, persistence, snapshots, membership, invariants
internal/storage/      WAL with CRC32-C frames, in-memory store for tests
internal/sim/          virtual clock, scheduler, network, cluster, fault matrix
internal/transport/    TCP transport with the same fault-control surface
internal/check/        client history, KV model, the two linearizability searches
internal/unsafe/       names for the eight deliberately broken variants
cmd/raftlab/           gates, single-run simulator, TCP smoke
cmd/importcheck/       import graph enforcement
docs/                  architecture, interfaces, developer guide, gate evidence
reports/failures/      shrunk failing seeds, written when a gate fails
```

## Design notes

An invariant that fires gets the protocol changed, not the checker. A gate that fails does not get a longer timeout. A flaky test prints its seed and its shrunk scenario, and shrinks are kept as files rather than retried away. Liveness is reported and not asserted, because timing assertions on a simulated scheduler are a way to write tests that fail for the wrong reason.

## License

MIT. See [LICENSE](LICENSE).

Written by [Bow Arkwright](https://github.com/SaddleDove).
