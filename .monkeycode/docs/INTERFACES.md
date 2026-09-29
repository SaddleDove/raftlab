# 接口定义

## Raft 核心依赖

`internal/raft` 只通过以下接口观察世界。

### Transport

```go
type Transport interface {
    Send(msg Message)
}
```

两套实现：`sim.Network`（确定性、虚拟时钟、故障规则）和后续 `transport/tcp`（真 socket，同一套故障控制 API）。

### Clock

```go
type Clock interface {
    Now() Instant
}
```

仿真时钟由调度器推进。核心代码禁止 `time.Now`。

### LogStore

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

实现：`storage.Memory`（测试）、`storage.WAL`（校验和 + fsync 策略）。

### StateMachine

```go
type StateMachine interface {
    Apply(index Index, command []byte) []byte
    Snapshot() []byte
    Restore(snap []byte)
}
```

M0 使用 `NopSM`。M2 接入 KV。M6 快照压缩；M8 Get 走 ReadIndex / lease。

### Timeouts / Rand

超时由仿真调度器注入为时钟事件。随机数由独立派生流提供，用于选举抖动。

## CLI

```text
raftlab gate-m0 … gate-m8
raftlab sim -n 3 -seed 1 -until 200
```

## 故障矩阵（14 种，可组合）

延迟（常数/均匀/重尾）、丢包、重复、乱序、位翻转、对称分区、非对称分区、崩溃停止、崩溃恢复、写一半崩溃、慢盘、选举期消息风暴、时钟偏移、带着陈旧日志重新加入。

## 不变量（运行时 9 条）

| 编号 | 名称 |
|------|------|
| I1 | 选举安全（每任期至多一个 leader） |
| I2 | 任期单调 |
| I3 | 投票先落盘再回复 |
| I4 | 日志匹配 |
| I5 | Leader 完备性 |
| I6 | 状态机安全 |
| I7 | 已提交不丢失 |
| I8 | 无脑裂写入 |
| I9 | apply ≤ commit |

## 变异清单（8）

不持久化 votedFor / 不回写新任期 / 收到更高任期 AppendEntries 不更新任期 / 依赖旧任期条目在裸多数上提交 / RequestVote 不做日志新旧检查 / 提交前就 apply / quorum 把 leader 算两次 / 选举定时器不重置。
