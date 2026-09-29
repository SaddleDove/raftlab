package raft

type Transport interface {
	Send(msg Message)
}

type Clock interface {
	Now() Instant
}

type Timeouts interface {
	ResetElection(d Instant)
	ResetHeartbeat(d Instant)
	StopHeartbeat()
}

type Rand interface {
	Intn(n int) int
}

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

type StateMachine interface {
	Apply(index Index, command []byte) []byte
	Snapshot() []byte
	Restore(snap []byte)
}

type Observer interface {
	OnState(id NodeID, role Role, hs HardState, lastApplied Index)
	OnSend(msg Message)
	OnPersist(id NodeID, hs HardState)
	OnApply(id NodeID, index Index, command []byte)
	OnLog(id NodeID, e LogEntry)
	OnViolation(v Violation)
}

type NopObserver struct{}

func (NopObserver) OnState(NodeID, Role, HardState, Index) {}
func (NopObserver) OnSend(Message)                         {}
func (NopObserver) OnPersist(NodeID, HardState)            {}
func (NopObserver) OnApply(NodeID, Index, []byte)          {}
func (NopObserver) OnLog(NodeID, LogEntry)                 {}
func (NopObserver) OnViolation(Violation)                  {}

type NopSM struct{}

func (NopSM) Apply(Index, []byte) []byte { return nil }
func (NopSM) Snapshot() []byte           { return nil }
func (NopSM) Restore([]byte)             {}
