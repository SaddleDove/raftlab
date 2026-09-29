package storage

import (
	"fmt"

	"raftlab/internal/raft"
)

type Memory struct {
	hs            raft.HardState
	snapshotIndex raft.Index
	snapshotTerm  raft.Term
	snapshot      []byte
	snapVoters    []raft.NodeID
	snapVotersOld []raft.NodeID
	ents          []raft.LogEntry
}

func NewMemory() *Memory {
	return &Memory{}
}

func (m *Memory) HardState() raft.HardState { return m.hs }

func (m *Memory) SetHardState(hs raft.HardState) error {
	m.hs = hs
	return nil
}

func (m *Memory) LastIndex() raft.Index {
	if n := len(m.ents); n > 0 {
		return m.ents[n-1].Index
	}
	return m.snapshotIndex
}

func (m *Memory) FirstIndex() raft.Index {
	return m.snapshotIndex + 1
}

func (m *Memory) Term(i raft.Index) (raft.Term, error) {
	if i == 0 {
		return 0, nil
	}
	if i == m.snapshotIndex {
		return m.snapshotTerm, nil
	}
	if i < m.FirstIndex() {
		return 0, raft.ErrCompacted
	}
	if i > m.LastIndex() {
		return 0, raft.ErrUnavailable
	}
	off := i - m.FirstIndex()
	return m.ents[off].Term, nil
}

func (m *Memory) Entries(lo, hi raft.Index) ([]raft.LogEntry, error) {
	if lo > hi {
		return nil, raft.ErrUnavailable
	}
	if lo <= m.snapshotIndex {
		return nil, raft.ErrCompacted
	}
	last := m.LastIndex()
	if lo > last+1 || hi > last+1 {
		return nil, raft.ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}
	start := lo - m.FirstIndex()
	end := hi - m.FirstIndex()
	return raft.CloneEntries(m.ents[start:end]), nil
}

func (m *Memory) Append(entries []raft.LogEntry) error {
	for _, e := range entries {
		if e.Index == 0 {
			return fmt.Errorf("storage: append index 0")
		}
		last := m.LastIndex()
		if e.Index <= m.snapshotIndex {
			return raft.ErrCompacted
		}
		if e.Index <= last {
			if err := m.TruncateSuffix(e.Index); err != nil {
				return err
			}
			last = m.LastIndex()
		}
		if e.Index != last+1 {
			return fmt.Errorf("storage: append gap want %d got %d", last+1, e.Index)
		}
		m.ents = append(m.ents, e.Clone())
	}
	return nil
}

func (m *Memory) TruncateSuffix(from raft.Index) error {
	if from <= m.snapshotIndex {
		return raft.ErrCompacted
	}
	last := m.LastIndex()
	if from > last+1 {
		return raft.ErrUnavailable
	}
	if from == last+1 {
		return nil
	}
	keep := from - m.FirstIndex()
	m.ents = m.ents[:keep]
	return nil
}

func (m *Memory) TruncatePrefix(to raft.Index) error {
	if to <= m.snapshotIndex {
		return nil
	}
	last := m.LastIndex()
	if to > last {
		return raft.ErrUnavailable
	}
	term, err := m.Term(to)
	if err != nil {
		return err
	}
	off := to - m.snapshotIndex
	if int(off) > len(m.ents) {
		return raft.ErrUnavailable
	}
	m.ents = append([]raft.LogEntry(nil), m.ents[off:]...)
	m.snapshotIndex = to
	m.snapshotTerm = term
	return nil
}

func (m *Memory) ApplySnapshot(meta raft.SnapshotMeta) error {
	keep := make([]raft.LogEntry, 0, len(m.ents))
	for _, e := range m.ents {
		if e.Index > meta.LastIndex {
			keep = append(keep, e)
		}
	}
	m.ents = keep
	m.snapshotIndex = meta.LastIndex
	m.snapshotTerm = meta.LastTerm
	m.snapshot = append([]byte(nil), meta.Data...)
	m.snapVoters = raft.CloneIDs(meta.Voters)
	m.snapVotersOld = raft.CloneIDs(meta.VotersOld)
	if m.hs.Commit < meta.LastIndex {
		m.hs.Commit = meta.LastIndex
	}
	return nil
}

func (m *Memory) Snapshot() raft.SnapshotMeta {
	return raft.SnapshotMeta{
		LastIndex: m.snapshotIndex,
		LastTerm:  m.snapshotTerm,
		Data:      append([]byte(nil), m.snapshot...),
		Voters:    raft.CloneIDs(m.snapVoters),
		VotersOld: raft.CloneIDs(m.snapVotersOld),
	}
}

func (m *Memory) Sync() error { return nil }

func (m *Memory) Clone() *Memory {
	out := &Memory{
		hs:            m.hs,
		snapshotIndex: m.snapshotIndex,
		snapshotTerm:  m.snapshotTerm,
		snapVoters:    raft.CloneIDs(m.snapVoters),
		snapVotersOld: raft.CloneIDs(m.snapVotersOld),
	}
	if len(m.snapshot) > 0 {
		out.snapshot = append([]byte(nil), m.snapshot...)
	}
	out.ents = raft.CloneEntries(m.ents)
	return out
}
