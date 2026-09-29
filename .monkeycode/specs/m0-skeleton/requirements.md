# Requirements Document

## Introduction

M0 delivers the skeleton: interfaces, deterministic simulator, WAL, invariant monitor, and CI. The gate is byte-identical replay of three silent nodes.

## Glossary

- **Silent node**: a Raft node with no client proposals; only election/heartbeat timers fire
- **Canonical event log**: newline-joined deterministic serialization of simulation events
- **Seed**: root entropy from which per-actor RNG streams are derived

## Requirements

### Requirement 1

**User Story:** AS a protocol engineer, I want a Raft node that depends only on injected interfaces, so that all nondeterminism is controllable.

#### Acceptance Criteria

1. THE `internal/raft` package SHALL compile against Transport, Clock, LogStore, StateMachine, Timeouts, and Rand only
2. WHEN CI runs importcheck, THE system SHALL reject imports of `time`, `net`, `internal/sim`, `internal/check`, `internal/unsafe`, `internal/transport`, and `internal/storage` from `internal/raft`
3. THE Raft node SHALL contain no `time.Now` or `time.Sleep` call sites

### Requirement 2

**User Story:** AS a tester, I want a deterministic simulator, so that the same seed replays identically.

#### Acceptance Criteria

1. WHEN two simulations run with the same seed, node count, and `RunUntil`, THE canonical event logs SHALL be byte-identical
2. THE scheduler SHALL be single-threaded and order events by (time, kind, sequence)
3. THE network, scheduler, and each node SHALL use independently derived RNG streams

### Requirement 3

**User Story:** AS a tester, I want a WAL with checksums, so that torn writes are recovered as a valid prefix or rejected loudly.

#### Acceptance Criteria

1. WHEN a WAL record is truncated mid-write, THE opener SHALL recover the valid prefix and discard the torn tail
2. WHEN a WAL record checksum mismatches, THE opener SHALL return `ErrCorruptWAL`
3. THE memory LogStore SHALL implement the same LogStore contract for tests

### Requirement 4

**User Story:** AS a tester, I want invariants monitored from step zero, so that violations surface immediately.

#### Acceptance Criteria

1. THE monitor SHALL observe role/term/vote/commit/apply events
2. THE monitor SHALL record I1, I2, I3, I9 on the M0 silent-node path
3. WHEN a violation is raised, THE event log SHALL contain a `violate` entry

### Requirement 5

**User Story:** AS a maintainer, I want a gate command, so that M0 is mechanically checkable.

#### Acceptance Criteria

1. WHEN `raftlab gate-m0` runs, THE process SHALL simulate 3 silent nodes for at least seeds 1, 2, and 7
2. THE process SHALL replay each seed twice and compare SHA-256 of the canonical logs
3. IF any seed mismatches, THE process SHALL exit non-zero
