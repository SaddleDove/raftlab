# RaftLab

能写出在安静网络上跑的 Raft 不算本事。RaftLab 的交付物是故障注入器 + 不变量判定器：把「它能工作」变成可测量的断言。

确定性仿真（种子可重放）、故障矩阵、线性一致性检查器（与独立实现交叉验证）、以及一组故意写错的 Raft 变体——检查器必须抓出来。

## 快速开始

```bash
go test ./...
go run ./cmd/raftlab gate-m0
go run ./cmd/raftlab gate-m1 -seeds 1000
go run ./cmd/raftlab gate-m5 -seeds 200
make ci
```

## 架构铁律

`internal/raft` 只看得到 `Transport` / `Clock` / `LogStore` / `StateMachine` 四个接口。
不允许 import 仿真层，不允许出现 `time.Now` / `time.Sleep`（CI 强制检查 import 图）。
所有不确定性从这几个接口注入。

## 里程碑

| Gate | 命令 | 通过条件 |
|------|------|----------|
| M0 | `go run ./cmd/raftlab gate-m0` | 3 个静默节点，同种子重放哈希字节一致 |
| M1 | `go run ./cmd/raftlab gate-m1` | 1000 种子 I1–I3，leader 收敛 |
| M2 | `go run ./cmd/raftlab gate-m2` | 无故障线性一致 |
| M3 | `go run ./cmd/raftlab gate-m3` | 1000 崩溃种子 I7 |
| M4 | `go run ./cmd/raftlab gate-m4` | I8 + 时间线 HTML |
| M5 | `go run ./cmd/raftlab gate-m5` | 全故障矩阵 + 8 变体全抓 |
| M6 | `go run ./cmd/raftlab gate-m6` | 快照压缩 + InstallSnapshot 追赶 |
| M7 | `go run ./cmd/raftlab gate-m7` | Joint 成员变更，双法定人数 |
| M8 | `go run ./cmd/raftlab gate-m8` | ReadIndex / lease 线性一致读 |

Gate 不过不许进下一个。间歇性失败打印种子和收缩场景，不加 sleep / 重试 / 放宽阈值。不变量报警就改协议，不改不变量。

## 许可证

MIT
