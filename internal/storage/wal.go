package storage

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"raftlab/internal/raft"
)

const (
	walMagic     uint32 = 0x52414C42
	recHardState byte   = 1
	recEntry     byte   = 2
	recTruncSuf  byte   = 3
	recTruncPre  byte   = 4
	recSnapshot  byte   = 5
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type WAL struct {
	mem  *Memory
	f    *os.File
	path string
	sync bool
}

type WALOption func(*WAL)

func SyncOnWrite(v bool) WALOption {
	return func(w *WAL) { w.sync = v }
}

func OpenWAL(dir string, opts ...WALOption) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "raft.wal")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	w := &WAL{mem: NewMemory(), f: f, path: path, sync: true}
	for _, o := range opts {
		o(w)
	}
	if err := w.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Sync()
	if c := w.f.Close(); err == nil {
		err = c
	}
	w.f = nil
	return err
}

func (w *WAL) HardState() raft.HardState            { return w.mem.HardState() }
func (w *WAL) LastIndex() raft.Index                { return w.mem.LastIndex() }
func (w *WAL) FirstIndex() raft.Index               { return w.mem.FirstIndex() }
func (w *WAL) Term(i raft.Index) (raft.Term, error) { return w.mem.Term(i) }
func (w *WAL) Entries(lo, hi raft.Index) ([]raft.LogEntry, error) {
	return w.mem.Entries(lo, hi)
}
func (w *WAL) Snapshot() raft.SnapshotMeta { return w.mem.Snapshot() }

func (w *WAL) SetHardState(hs raft.HardState) error {
	payload := encodeHardState(hs)
	if err := w.appendRecord(recHardState, payload); err != nil {
		return err
	}
	return w.mem.SetHardState(hs)
}

func (w *WAL) Append(entries []raft.LogEntry) error {
	for _, e := range entries {
		if err := w.appendRecord(recEntry, encodeEntry(e)); err != nil {
			return err
		}
	}
	return w.mem.Append(entries)
}

func (w *WAL) TruncateSuffix(from raft.Index) error {
	if err := w.appendRecord(recTruncSuf, encodeIndex(from)); err != nil {
		return err
	}
	return w.mem.TruncateSuffix(from)
}

func (w *WAL) TruncatePrefix(to raft.Index) error {
	if err := w.appendRecord(recTruncPre, encodeIndex(to)); err != nil {
		return err
	}
	return w.mem.TruncatePrefix(to)
}

func (w *WAL) ApplySnapshot(meta raft.SnapshotMeta) error {
	if err := w.appendRecord(recSnapshot, encodeSnapshot(meta)); err != nil {
		return err
	}
	return w.mem.ApplySnapshot(meta)
}

func (w *WAL) Sync() error {
	if w.f == nil {
		return nil
	}
	return w.f.Sync()
}

func (w *WAL) Path() string { return w.path }

func (w *WAL) replay() error {
	buf, err := io.ReadAll(w.f)
	if err != nil {
		return err
	}
	off := 0
	good := 0
	for off < len(buf) {
		kind, payload, n, status := decodeRecord(buf[off:])
		if status == recTorn {
			if err := w.f.Truncate(int64(good)); err != nil {
				return err
			}
			if _, err := w.f.Seek(int64(good), io.SeekStart); err != nil {
				return err
			}
			return nil
		}
		if status == recCorrupt {
			return raft.ErrCorruptWAL
		}
		if err := w.applyRecord(kind, payload); err != nil {
			return err
		}
		off += n
		good = off
	}
	if _, err := w.f.Seek(int64(good), io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (w *WAL) applyRecord(kind byte, payload []byte) error {
	switch kind {
	case recHardState:
		hs, err := decodeHardState(payload)
		if err != nil {
			return raft.ErrCorruptWAL
		}
		return w.mem.SetHardState(hs)
	case recEntry:
		e, err := decodeEntry(payload)
		if err != nil {
			return raft.ErrCorruptWAL
		}
		return w.mem.Append([]raft.LogEntry{e})
	case recTruncSuf:
		idx, err := decodeIndex(payload)
		if err != nil {
			return raft.ErrCorruptWAL
		}
		return w.mem.TruncateSuffix(idx)
	case recTruncPre:
		idx, err := decodeIndex(payload)
		if err != nil {
			return raft.ErrCorruptWAL
		}
		return w.mem.TruncatePrefix(idx)
	case recSnapshot:
		meta, err := decodeSnapshot(payload)
		if err != nil {
			return raft.ErrCorruptWAL
		}
		return w.mem.ApplySnapshot(meta)
	default:
		return raft.ErrCorruptWAL
	}
}

func (w *WAL) appendRecord(kind byte, payload []byte) error {
	rec := encodeRecord(kind, payload)
	if _, err := w.f.Write(rec); err != nil {
		return err
	}
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func encodeRecord(kind byte, payload []byte) []byte {
	crcSrc := make([]byte, 1+len(payload))
	crcSrc[0] = kind
	copy(crcSrc[1:], payload)
	sum := crc32.Checksum(crcSrc, crcTable)
	out := make([]byte, 4+1+4+4+len(payload))
	binary.LittleEndian.PutUint32(out[0:4], walMagic)
	out[4] = kind
	binary.LittleEndian.PutUint32(out[5:9], uint32(len(payload)))
	binary.LittleEndian.PutUint32(out[9:13], sum)
	copy(out[13:], payload)
	return out
}

const recOK = 0
const recTorn = 1
const recCorrupt = 2

func decodeRecord(b []byte) (kind byte, payload []byte, n int, status int) {
	header := 13
	if len(b) < header {
		return 0, nil, 0, recTorn
	}
	magic := binary.LittleEndian.Uint32(b[0:4])
	if magic != walMagic {
		return 0, nil, 0, recCorrupt
	}
	kind = b[4]
	plen := binary.LittleEndian.Uint32(b[5:9])
	sum := binary.LittleEndian.Uint32(b[9:13])
	need := header + int(plen)
	if len(b) < need {
		return 0, nil, 0, recTorn
	}
	payload = b[header:need]
	crcSrc := make([]byte, 1+len(payload))
	crcSrc[0] = kind
	copy(crcSrc[1:], payload)
	if crc32.Checksum(crcSrc, crcTable) != sum {
		return 0, nil, 0, recCorrupt
	}
	return kind, payload, need, recOK
}

func encodeHardState(hs raft.HardState) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint64(b[0:8], uint64(hs.Term))
	binary.LittleEndian.PutUint64(b[8:16], uint64(hs.VotedFor))
	binary.LittleEndian.PutUint64(b[16:24], uint64(hs.Commit))
	return b
}

func decodeHardState(b []byte) (raft.HardState, error) {
	if len(b) != 24 {
		return raft.HardState{}, raft.ErrCorruptWAL
	}
	return raft.HardState{
		Term:     raft.Term(binary.LittleEndian.Uint64(b[0:8])),
		VotedFor: raft.NodeID(binary.LittleEndian.Uint64(b[8:16])),
		Commit:   raft.Index(binary.LittleEndian.Uint64(b[16:24])),
	}, nil
}

func encodeEntry(e raft.LogEntry) []byte {
	b := make([]byte, 16+4+len(e.Command))
	binary.LittleEndian.PutUint64(b[0:8], uint64(e.Term))
	binary.LittleEndian.PutUint64(b[8:16], uint64(e.Index))
	binary.LittleEndian.PutUint32(b[16:20], uint32(len(e.Command)))
	copy(b[20:], e.Command)
	return b
}

func decodeEntry(b []byte) (raft.LogEntry, error) {
	if len(b) < 20 {
		return raft.LogEntry{}, raft.ErrCorruptWAL
	}
	n := binary.LittleEndian.Uint32(b[16:20])
	if len(b) != 20+int(n) {
		return raft.LogEntry{}, raft.ErrCorruptWAL
	}
	e := raft.LogEntry{
		Term:  raft.Term(binary.LittleEndian.Uint64(b[0:8])),
		Index: raft.Index(binary.LittleEndian.Uint64(b[8:16])),
	}
	if n > 0 {
		e.Command = append([]byte(nil), b[20:]...)
	}
	return e, nil
}

func encodeIndex(i raft.Index) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func decodeIndex(b []byte) (raft.Index, error) {
	if len(b) != 8 {
		return 0, raft.ErrCorruptWAL
	}
	return raft.Index(binary.LittleEndian.Uint64(b)), nil
}

func encodeSnapshot(meta raft.SnapshotMeta) []byte {
	b := make([]byte, 20+len(meta.Data)+8+8*len(meta.Voters)+8*len(meta.VotersOld))
	binary.LittleEndian.PutUint64(b[0:8], uint64(meta.LastIndex))
	binary.LittleEndian.PutUint64(b[8:16], uint64(meta.LastTerm))
	binary.LittleEndian.PutUint32(b[16:20], uint32(len(meta.Data)))
	copy(b[20:], meta.Data)
	off := 20 + len(meta.Data)
	binary.LittleEndian.PutUint32(b[off:off+4], uint32(len(meta.Voters)))
	off += 4
	for _, id := range meta.Voters {
		binary.LittleEndian.PutUint64(b[off:off+8], uint64(id))
		off += 8
	}
	binary.LittleEndian.PutUint32(b[off:off+4], uint32(len(meta.VotersOld)))
	off += 4
	for _, id := range meta.VotersOld {
		binary.LittleEndian.PutUint64(b[off:off+8], uint64(id))
		off += 8
	}
	return b[:off]
}

func decodeSnapshot(b []byte) (raft.SnapshotMeta, error) {
	if len(b) < 20 {
		return raft.SnapshotMeta{}, raft.ErrCorruptWAL
	}
	n := binary.LittleEndian.Uint32(b[16:20])
	if len(b) < 20+int(n) {
		return raft.SnapshotMeta{}, raft.ErrCorruptWAL
	}
	meta := raft.SnapshotMeta{
		LastIndex: raft.Index(binary.LittleEndian.Uint64(b[0:8])),
		LastTerm:  raft.Term(binary.LittleEndian.Uint64(b[8:16])),
	}
	if n > 0 {
		meta.Data = append([]byte(nil), b[20:20+n]...)
	}
	off := 20 + int(n)
	if off == len(b) {
		return meta, nil
	}
	voters, next, err := decodeIDList(b, off)
	if err != nil {
		return raft.SnapshotMeta{}, err
	}
	old, next, err := decodeIDList(b, next)
	if err != nil {
		return raft.SnapshotMeta{}, err
	}
	if next != len(b) {
		return raft.SnapshotMeta{}, raft.ErrCorruptWAL
	}
	meta.Voters = voters
	meta.VotersOld = old
	return meta, nil
}

func decodeIDList(b []byte, off int) ([]raft.NodeID, int, error) {
	if off+4 > len(b) {
		return nil, 0, raft.ErrCorruptWAL
	}
	n := int(binary.LittleEndian.Uint32(b[off : off+4]))
	off += 4
	if n < 0 || off+8*n > len(b) {
		return nil, 0, raft.ErrCorruptWAL
	}
	if n == 0 {
		return nil, off, nil
	}
	ids := make([]raft.NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = raft.NodeID(binary.LittleEndian.Uint64(b[off : off+8]))
		off += 8
	}
	return ids, off, nil
}
