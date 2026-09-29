# 开发指南

## 环境要求

- Go 1.22 或更高
- Make（可选）

## 快速开始

```bash
git clone <repo>
cd raftlab
go test ./...
go run ./cmd/raftlab gate-m0
```

## 项目结构说明

- `internal/raft` 协议核心，CI 禁止 import `time`/`net`/`sim`
- `internal/sim` 确定性仿真
- `internal/storage` WAL 与内存存储
- `internal/check` 线性一致与收缩（M2+）
- `internal/unsafe` 故意写错的变体（M5）
- `cmd/raftlab` gate 与 demo
- `cmd/importcheck` import 图检查
- `docs/PROGRESS.md` 每个 gate 的原始输出必须贴在这里

## 开发规范

- 严格按里程碑推进，gate 不过不许进下一个
- 测试间歇性失败就打印种子和收缩场景
- 禁止用 sleep / 重试 / 放宽阈值「修好」flaky
- 不变量报警就改协议，不许改不变量
- 同种子同场景事件日志必须字节级一致，这条本身是测试（跑两遍比哈希）
- 每个参与者独立派生随机流，加节点不扰动既有流
- 代码注释保持最少；导出 API 的行为用测试钉死

## 常见任务

跑 gates：

```bash
go run ./cmd/raftlab gate-m0 -seeds 3 -until 200
go run ./cmd/raftlab gate-m1 -seeds 1000
go run ./cmd/raftlab gate-m2
go run ./cmd/raftlab gate-m3 -seeds 1000
go run ./cmd/raftlab gate-m4
go run ./cmd/raftlab gate-m5 -seeds 200
go run ./cmd/raftlab gate-m6
go run ./cmd/raftlab gate-m7
go run ./cmd/raftlab gate-m8
```

检查 import 图：

```bash
go run ./cmd/importcheck
```

WAL 撕裂写：

```bash
go test ./internal/storage -run Torn -count=1
```

## 构建与发布

```bash
make ci
```

包含 importcheck、vet、单元测试、gate-m0。`-race` 与 staticcheck 在完整 M5 验收启用。
