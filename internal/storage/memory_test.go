package storage

import (
	"testing"

	"raftlab/internal/raft"
)

func TestMemoryAppendAndTerm(t *testing.T) {
	m := NewMemory()
	ents := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}
	if err := m.Append(ents); err != nil {
		t.Fatal(err)
	}
	if m.LastIndex() != 2 {
		t.Fatalf("last %d", m.LastIndex())
	}
	term, err := m.Term(2)
	if err != nil || term != 1 {
		t.Fatalf("term %d err %v", term, err)
	}
	got, err := m.Entries(1, 3)
	if err != nil || len(got) != 2 {
		t.Fatalf("entries %v %v", got, err)
	}
}

func TestMemoryTruncateSuffix(t *testing.T) {
	m := NewMemory()
	_ = m.Append([]raft.LogEntry{
		{Term: 1, Index: 1},
		{Term: 2, Index: 2},
		{Term: 2, Index: 3},
	})
	if err := m.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if m.LastIndex() != 1 {
		t.Fatalf("last %d", m.LastIndex())
	}
}

func TestMemoryOverwriteConflict(t *testing.T) {
	m := NewMemory()
	_ = m.Append([]raft.LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
	})
	if err := m.Append([]raft.LogEntry{{Term: 2, Index: 2, Command: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	term, _ := m.Term(2)
	if term != 2 {
		t.Fatalf("term %d", term)
	}
	if m.LastIndex() != 2 {
		t.Fatalf("last %d", m.LastIndex())
	}
}

func TestMemoryHardState(t *testing.T) {
	m := NewMemory()
	hs := raft.HardState{Term: 3, VotedFor: 2, Commit: 1}
	if err := m.SetHardState(hs); err != nil {
		t.Fatal(err)
	}
	if m.HardState() != hs {
		t.Fatalf("hs %+v", m.HardState())
	}
}

func TestMemoryApplySnapshotKeepsSuffix(t *testing.T) {
	m := NewMemory()
	_ = m.Append([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
	})
	if err := m.ApplySnapshot(raft.SnapshotMeta{
		LastIndex: 2,
		LastTerm:  1,
		Data:      []byte("snap"),
		Voters:    []raft.NodeID{1, 2, 3},
	}); err != nil {
		t.Fatal(err)
	}
	if m.FirstIndex() != 3 || m.LastIndex() != 3 {
		t.Fatalf("first=%d last=%d", m.FirstIndex(), m.LastIndex())
	}
	snap := m.Snapshot()
	if snap.LastIndex != 2 || string(snap.Data) != "snap" || len(snap.Voters) != 3 {
		t.Fatalf("snap %+v", snap)
	}
}
