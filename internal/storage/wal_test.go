package storage

import (
	"os"
	"path/filepath"
	"testing"

	"raftlab/internal/raft"
)

func TestWALReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SetHardState(raft.HardState{Term: 2, VotedFor: 1, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("put a 1")},
		{Term: 2, Index: 2, Command: []byte("put b 2")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	hs := w2.HardState()
	if hs.Term != 2 || hs.VotedFor != 1 || hs.Commit != 1 {
		t.Fatalf("hs %+v", hs)
	}
	if w2.LastIndex() != 2 {
		t.Fatalf("last %d", w2.LastIndex())
	}
	ents, err := w2.Entries(1, 3)
	if err != nil || len(ents) != 2 {
		t.Fatalf("ents %v %v", ents, err)
	}
}

func TestWALTornWriteRecoversPrefix(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]raft.LogEntry{{Term: 1, Index: 1, Command: []byte("ok")}}); err != nil {
		t.Fatal(err)
	}
	path := w.Path()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	good := st.Size()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x42, 0x4C, 0x41, 0x52, 0x02, 0x10, 0x00}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("torn write should recover prefix, got %v", err)
	}
	defer w2.Close()
	if w2.LastIndex() != 1 {
		t.Fatalf("last %d", w2.LastIndex())
	}
	st2, _ := os.Stat(path)
	if st2.Size() != good {
		t.Fatalf("expected truncate to %d, got %d", good, st2.Size())
	}
}

func TestWALCorruptChecksumRejected(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]raft.LogEntry{{Term: 1, Index: 1, Command: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	path := w.Path()
	_ = w.Close()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 14 {
		t.Fatal("short wal")
	}
	b[len(b)-1] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = OpenWAL(dir)
	if err != raft.ErrCorruptWAL {
		t.Fatalf("want ErrCorruptWAL, got %v", err)
	}
}

func TestWALSnapshotVotersRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	})
	if err := w.ApplySnapshot(raft.SnapshotMeta{
		LastIndex: 2,
		LastTerm:  1,
		Data:      []byte("kv"),
		Voters:    []raft.NodeID{1, 2, 4},
		VotersOld: []raft.NodeID{1, 2, 3},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	snap := w2.Snapshot()
	if snap.LastIndex != 2 || string(snap.Data) != "kv" {
		t.Fatalf("snap %+v", snap)
	}
	if len(snap.Voters) != 3 || snap.Voters[2] != 4 {
		t.Fatalf("voters %v", snap.Voters)
	}
	if len(snap.VotersOld) != 3 {
		t.Fatalf("old %v", snap.VotersOld)
	}
}

func TestWALPath(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if filepath.Base(w.Path()) != "raft.wal" {
		t.Fatalf("path %s", w.Path())
	}
}
