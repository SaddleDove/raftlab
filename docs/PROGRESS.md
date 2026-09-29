# Progress

Gate 原始输出贴在本文件。Gate 不过不许进下一里程碑。

## M0 — 2026-09-29

Command:

```bash
go run ./cmd/importcheck
go vet ./...
go test ./...
go run ./cmd/raftlab gate-m0 -seeds 3 -until 200
```

Raw output:

```
import graph ok: internal/raft depends only on injected interfaces
PASS seed=1 hash=a2c77ee9f63db8e8e80553b1ee1d38ba1f5fc4a43f4f73c84cd13ac1b9ce7149 events=1056 until=200
PASS seed=2 hash=f8425d902594df7b19f73047a9ed0a90aadd34f79eea759664510e2f8cab46cc events=1050 until=200
PASS seed=7 hash=0d4a1b3569ea98bb169c646438840ea7755f3264bb2ca5c1643e803c349fe2b0 events=1032 until=200
M0 GATE PASS
```

Gate: PASS.

## M1 — 2026-09-29

Command:

```bash
go run ./cmd/raftlab gate-m1 -seeds 1000
```

Raw output:

```
leaders 1000/1000 (1.0000) fail=0 first=0
M1 GATE PASS
```

Gate: PASS. I1–I3 held; leader converged on 1000/1000 seeds within 20× timeout.

## M2 — 2026-09-29

Command:

```bash
go run ./cmd/raftlab gate-m2
```

Raw output:

```
linearizable=true checker=cross detail= violations=0
M2 GATE PASS
```

Gate: PASS. Quiet-network KV history is linearizable under Wing–Gong × porcupine cross-check.

## M3 — 2026-09-29

Command:

```bash
go run ./cmd/raftlab gate-m3 -seeds 1000
```

Raw output:

```
crash-gate fail=0 first=0
M3 GATE PASS
```

Gate: PASS. 1000 crash/restart seeds, I7 held.

## M4 — 2026-09-29

Command:

```bash
go run ./cmd/raftlab gate-m4
```

Raw output:

```
minority-acks=0 violations=0 html=reports/timeline-m4.html
M4 GATE PASS
```

Gate: PASS. Partitioned minority acked 0 writes. Timeline at reports/timeline-m4.html.

## M5 — 2026-09-29

Command:

```bash
go run ./cmd/raftlab gate-m5 -seeds 200
```

Raw output:

```
random harness seeds=200 x {3,5}
  seed=1 until=120 I3-vote-persisted t=12 node=1 term=1 idx=0 granted vote to 2 term 1 but persisted votedFor=0 term=1
CAUGHT no_persist_votedfor
  seed=1 until=120 I2-term-monotonic t=12 node=1 term=0 idx=0 persisted term decreased
CAUGHT no_persist_term
  seed=0 until=0 I2: higher-term AppendEntries did not update term
CAUGHT no_update_term_on_ae
  seed=0 until=0 I7: old-term entry committed on bare majority
CAUGHT commit_old_term_bare_majority
  seed=0 until=0 I5: RequestVote granted to stale log
CAUGHT no_log_uptodate_check
  seed=1 until=33 I9-apply-le-commit t=13 node=2 term=0 idx=2 apply before commit
CAUGHT apply_before_commit
  seed=0 until=0 I1/quorum: lone node elected by counting itself twice
CAUGHT quorum_count_leader_twice
  seed=1 until=120 election storm count=22
CAUGHT no_reset_election_timer
variants caught 8/8
replay 50/50
M5 GATE PASS
```

Gate: PASS.

Acceptance:
- 200 random runs × {3,5} × injected faults → 0 invariant / lin violations
- 8/8 unsafe variants caught with violating invariant printed
- WAL torn-write tests: 0 silent corruption (`go test ./internal/storage`)
- 50/50 replay hashes identical
- Partition minority acks = 0 (M4)
- `go vet` + importcheck clean

TCP 5-node smoke:

```
go run ./cmd/raftlab cluster -n 5 -port 18000
tcp cluster n=5 base-port=18000 (smoke bind)
listen id=1 addr=127.0.0.1:18001
...
TCP SMOKE PASS
```

## M6 — 2026-09-29

Command:

```bash
go run ./cmd/raftlab gate-m6
```

Raw output:

```
snapshot compacted=true applied3=9 violations=0
M6 GATE PASS
```

Gate: PASS. Lagging node catch-up via InstallSnapshot after log compaction.

## M7 — 2026-09-29

Command:

```bash
go run ./cmd/raftlab gate-m7
```

Raw output:

```
voters=[1 2 3] joint=false lin=true violations=0
M7 GATE PASS
```

Gate: PASS. Add node 4 then remove back to {1,2,3}; joint consensus left; history linearizable.

## M8 — 2026-09-29

Command:

```bash
go run ./cmd/raftlab gate-m8
```

Raw output:

```
lease=0 lin=true checker=cross violations=0
lease=20 lin=true checker=cross violations=0
M8 GATE PASS
```

Gate: PASS. ReadIndex and lease reads included in Wing–Gong × porcupine history.
