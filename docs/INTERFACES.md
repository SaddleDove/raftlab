# Interfaces

The protocol core observes the world through these interfaces and nothing else.

## Transport

```go
type Transport interface {
    Send(msg Message)
}
```

`sim.Network` implements it deterministically with a virtual clock and fault rules. `transport.TCP` implements it over real sockets with the same fault-control surface, so a scenario written against the simulator can be pointed at a real cluster.

## Clock

```go
type Clock interface {
    Now() Instant
}
```

The simulator advances the clock when it pops an event, so time is a function of the schedule rather than of the wall. The core is not allowed to call `time.Now`.

## LogStore

```go
type LogStore interface {
    HardState() HardState
    SetHardState(hs HardState) error
    LastIndex() Index
    FirstIndex() Index
    Term(i Index) (Term, error)
    Entries(lo, hi Index) ([]LogEntry, error)
    Append(entries []LogEntry) error
    TruncateSuffix(from Index) error
    TruncatePrefix(to Index) error
    Sync() error
    Snapshot() SnapshotMeta
    ApplySnapshot(meta SnapshotMeta) error
}
```

`storage.Memory` is the test implementation. `storage.WAL` frames every mutation as a checksummed record, recovers a torn tail as a valid prefix, and returns `ErrCorruptWAL` on a checksum mismatch rather than guessing.

## StateMachine

```go
type StateMachine interface {
    Apply(index Index, command []byte) []byte
    Snapshot() []byte
    Restore(snap []byte)
}
```

`NopSM` is used by the silent-node gates. `raft.KV` backs the linearizability work: gets, puts and compare-and-swap over a plain map, with a deterministic snapshot encoding.

## Timeouts and randomness

```go
type Timeouts interface {
    ResetElection(d Instant)
    ResetHeartbeat(d Instant)
    StopHeartbeat()
}

type Rand interface {
    Intn(n int) int
}
```

Timeouts become scheduled events, and randomness comes from a per-node stream derived from the seed, which is what makes elections jitter without making replay unstable.

## Observer

```go
type Observer interface {
    OnState(id NodeID, role Role, hs HardState, lastApplied Index)
    OnSend(msg Message)
    OnPersist(id NodeID, hs HardState)
    OnApply(id NodeID, index Index, command []byte)
    OnLog(id NodeID, e LogEntry)
    OnViolation(v Violation)
}
```

The observer is how the monitor and the event log see the protocol without the protocol knowing about them.

## CLI

```text
raftlab gate-m0 … gate-m8      milestone gates
raftlab sim -n 3 -seed 1       one simulation, hash and event count
raftlab demo-kv -seed 1        KV history, linearizability verdict
raftlab cluster -n 5 -port …   bind a TCP cluster and exit
```

## Faults

Sixteen kinds are declared and ten are wired into the matrix. A rule carries a kind, an optional edge, a probability, delay parameters, a group pair, a target node and an expiry time. See the table in the README, and `WiredFaults()` for the authoritative list.

## Invariants

| Code | Name |
|------|------|
| I1 | election safety, at most one leader per term |
| I2 | term monotonic |
| I3 | vote persisted before it is sent |
| I4 | log matching |
| I5 | leader completeness |
| I6 | state machine safety |
| I7 | committed entries are durable |
| I8 | no write is acknowledged by a minority |
| I9 | apply is never ahead of commit |

## Variants

Eight, each paired with the invariant that must catch it. See `internal/unsafe` for the names and `gate-m5` for the evidence.

## Linearizability checkers

`check.CrossCheck` runs `OrderKV` and `BitmaskKV` and requires agreement. `BitmaskKV` is capped at `BitmaskLimit` (16) operations because its search is exponential; above the cap the verdict comes from `OrderKV` alone and `Verdict.Checker` reports `order` rather than a cross-check. Both share the KV model in `check/order.go`.
