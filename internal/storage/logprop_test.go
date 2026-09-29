package storage

import (
	"testing"

	"raftlab/internal/raft"
)

func TestLogAppendIndexMonotonic(t *testing.T) {
	m := NewMemory()
	for i := 1; i <= 32; i++ {
		if err := m.Append([]raft.LogEntry{{Term: 1, Index: raft.Index(i), Command: []byte{byte(i)}}}); err != nil {
			t.Fatal(err)
		}
		if m.LastIndex() != raft.Index(i) {
			t.Fatalf("last %d want %d", m.LastIndex(), i)
		}
	}
}

func TestLogTruncateThenAppend(t *testing.T) {
	m := NewMemory()
	for i := 1; i <= 8; i++ {
		_ = m.Append([]raft.LogEntry{{Term: 1, Index: raft.Index(i)}})
	}
	_ = m.TruncateSuffix(5)
	if err := m.Append([]raft.LogEntry{{Term: 2, Index: 5}}); err != nil {
		t.Fatal(err)
	}
	term, err := m.Term(5)
	if err != nil || term != 2 {
		t.Fatalf("term %d %v", term, err)
	}
	if m.LastIndex() != 5 {
		t.Fatalf("last %d", m.LastIndex())
	}
}

func TestWALByteTruncation(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if err := w.Append([]raft.LogEntry{{Term: 1, Index: raft.Index(i), Command: []byte("x")}}); err != nil {
			t.Fatal(err)
		}
	}
	path := w.Path()
	_ = w.Close()
	b := mustRead(t, path)
	for cut := len(b); cut > 0; cut -= 3 {
		if err := writeFile(path, b[:cut]); err != nil {
			t.Fatal(err)
		}
		w2, err := OpenWAL(dir)
		if err == raft.ErrCorruptWAL {
			continue
		}
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		_ = w2.LastIndex()
		_ = w2.Close()
	}
}
