package raft

import "testing"

type stubTransport struct{ sent []Message }

func (s *stubTransport) Send(m Message) { s.sent = append(s.sent, m.Clone()) }

type stubClock struct{ n Instant }

func (s *stubClock) Now() Instant { return s.n }

type stubTO struct {
	elec Instant
	hb   Instant
	stop int
}

func (s *stubTO) ResetElection(d Instant)  { s.elec = d }
func (s *stubTO) ResetHeartbeat(d Instant) { s.hb = d }
func (s *stubTO) StopHeartbeat()           { s.stop++ }

type stubRand struct{ v int }

func (s stubRand) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return s.v % n
}

type memStore struct {
	hs            HardState
	ents          []LogEntry
	snap          SnapshotMeta
	snapshotIndex Index
	snapshotTerm  Term
}

func (m *memStore) HardState() HardState { return m.hs }
func (m *memStore) SetHardState(hs HardState) error {
	m.hs = hs
	return nil
}
func (m *memStore) LastIndex() Index {
	if n := len(m.ents); n > 0 {
		return m.ents[n-1].Index
	}
	return m.snapshotIndex
}
func (m *memStore) FirstIndex() Index {
	return m.snapshotIndex + 1
}
func (m *memStore) Term(i Index) (Term, error) {
	if i == 0 {
		return 0, nil
	}
	if i == m.snapshotIndex {
		return m.snapshotTerm, nil
	}
	if i < m.FirstIndex() {
		return 0, ErrCompacted
	}
	for _, e := range m.ents {
		if e.Index == i {
			return e.Term, nil
		}
	}
	return 0, ErrUnavailable
}
func (m *memStore) Entries(lo, hi Index) ([]LogEntry, error) {
	var out []LogEntry
	for _, e := range m.ents {
		if e.Index >= lo && e.Index < hi {
			out = append(out, e.Clone())
		}
	}
	return out, nil
}
func (m *memStore) Append(entries []LogEntry) error {
	m.ents = append(m.ents, CloneEntries(entries)...)
	return nil
}
func (m *memStore) TruncateSuffix(from Index) error {
	kept := m.ents[:0]
	for _, e := range m.ents {
		if e.Index < from {
			kept = append(kept, e)
		}
	}
	m.ents = kept
	return nil
}
func (m *memStore) TruncatePrefix(Index) error { return nil }
func (m *memStore) Sync() error                { return nil }
func (m *memStore) Snapshot() SnapshotMeta {
	out := m.snap
	out.LastIndex = m.snapshotIndex
	out.LastTerm = m.snapshotTerm
	if len(m.snap.Data) > 0 {
		out.Data = append([]byte(nil), m.snap.Data...)
	}
	out.Voters = CloneIDs(m.snap.Voters)
	out.VotersOld = CloneIDs(m.snap.VotersOld)
	return out
}
func (m *memStore) ApplySnapshot(meta SnapshotMeta) error {
	keep := make([]LogEntry, 0, len(m.ents))
	for _, e := range m.ents {
		if e.Index > meta.LastIndex {
			keep = append(keep, e)
		}
	}
	m.ents = keep
	m.snapshotIndex = meta.LastIndex
	m.snapshotTerm = meta.LastTerm
	m.snap = SnapshotMeta{
		LastIndex: meta.LastIndex,
		LastTerm:  meta.LastTerm,
		Data:      append([]byte(nil), meta.Data...),
		Voters:    CloneIDs(meta.Voters),
		VotersOld: CloneIDs(meta.VotersOld),
	}
	if m.hs.Commit < meta.LastIndex {
		m.hs.Commit = meta.LastIndex
	}
	return nil
}

func testNode(t *testing.T) (*Node, *stubTransport, *memStore, *stubTO) {
	t.Helper()
	tr := &stubTransport{}
	sto := &memStore{}
	to := &stubTO{}
	n, err := NewNode(Config{
		ID:              1,
		Peers:           []NodeID{2, 3},
		ElectionTickMin: 10,
		ElectionTickMax: 20,
		HeartbeatTick:   2,
	}, tr, &stubClock{}, sto, NopSM{}, to, stubRand{v: 3})
	if err != nil {
		t.Fatal(err)
	}
	return n, tr, sto, to
}

func TestNewNodeSchedulesElection(t *testing.T) {
	_, _, _, to := testNode(t)
	if to.elec < 10 || to.elec > 20 {
		t.Fatalf("election timeout %d", to.elec)
	}
}

func TestTickStartsElection(t *testing.T) {
	n, tr, sto, _ := testNode(t)
	n.Step(Message{Type: MsgTickElection, To: 1, From: 1})
	if n.Role() != Candidate {
		t.Fatalf("role %s", n.Role())
	}
	if n.Term() != 1 {
		t.Fatalf("term %d", n.Term())
	}
	if sto.hs.VotedFor != 1 {
		t.Fatalf("votedFor not persisted: %d", sto.hs.VotedFor)
	}
	if len(tr.sent) != 2 {
		t.Fatalf("want 2 vote reqs, got %d", len(tr.sent))
	}
}

func TestSingleNodeElection(t *testing.T) {
	tr := &stubTransport{}
	sto := &memStore{}
	to := &stubTO{}
	n, err := NewNode(Config{
		ID:              1,
		ElectionTickMin: 5,
		ElectionTickMax: 5,
		HeartbeatTick:   1,
	}, tr, &stubClock{}, sto, NopSM{}, to, stubRand{})
	if err != nil {
		t.Fatal(err)
	}
	n.Step(Message{Type: MsgTickElection, To: 1, From: 1})
	if n.Role() != Leader {
		t.Fatalf("single node should be leader, got %s", n.Role())
	}
}

func electSingle(t *testing.T, cfg Config, sm StateMachine, sto *memStore) (*Node, *stubTransport, *stubClock) {
	t.Helper()
	if sto == nil {
		sto = &memStore{}
	}
	if sm == nil {
		sm = NopSM{}
	}
	tr := &stubTransport{}
	clk := &stubClock{}
	n, err := NewNode(cfg, tr, clk, sto, sm, &stubTO{}, stubRand{})
	if err != nil {
		t.Fatal(err)
	}
	n.Step(Message{Type: MsgTickElection, To: cfg.ID, From: cfg.ID})
	for _, p := range cfg.Peers {
		n.Step(Message{Type: MsgRequestVoteResp, From: p, To: cfg.ID, Term: n.Term(), VoteGranted: true})
	}
	if n.Role() != Leader {
		t.Fatalf("role %s", n.Role())
	}
	return n, tr, clk
}

func TestSnapshotCompactsLog(t *testing.T) {
	sto := &memStore{}
	kv := NewKV()
	n, _, _ := electSingle(t, Config{
		ID: 1, ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
		SnapshotThreshold: 2,
	}, kv, sto)
	n.Step(Message{Type: MsgClientPropose, From: 9, To: 1, ClientID: 1, Seq: 1, Command: EncodePut("a", "1")})
	n.Step(Message{Type: MsgClientPropose, From: 9, To: 1, ClientID: 1, Seq: 2, Command: EncodePut("b", "2")})
	if n.SnapshotMeta().LastIndex < 2 {
		t.Fatalf("snap index %d first=%d", n.SnapshotMeta().LastIndex, sto.FirstIndex())
	}
	v, ok := kv.Get("a")
	if !ok || v != "1" {
		t.Fatalf("kv a=%q ok=%v", v, ok)
	}
}

func TestInstallSnapshotRestoresFollower(t *testing.T) {
	tr := &stubTransport{}
	sto := &memStore{}
	kv := NewKV()
	n, err := NewNode(Config{
		ID: 2, Peers: []NodeID{1, 3},
		ElectionTickMin: 10, ElectionTickMax: 10, HeartbeatTick: 2,
	}, tr, &stubClock{}, sto, kv, &stubTO{}, stubRand{})
	if err != nil {
		t.Fatal(err)
	}
	snapKV := NewKV()
	snapKV.Apply(1, EncodePut("k", "v"))
	n.Step(Message{
		Type:              MsgInstallSnapshot,
		From:              1,
		To:                2,
		Term:              1,
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
		Snapshot:          snapKV.Snapshot(),
		Voters:            []NodeID{1, 2, 3},
	})
	if n.Applied() != 3 {
		t.Fatalf("applied %d", n.Applied())
	}
	v, ok := kv.Get("k")
	if !ok || v != "v" {
		t.Fatalf("restored kv k=%q ok=%v", v, ok)
	}
	if sto.FirstIndex() != 4 {
		t.Fatalf("first %d", sto.FirstIndex())
	}
}

func TestLeaderSendsSnapshotWhenCompacted(t *testing.T) {
	sto := &memStore{}
	n, tr, _ := electSingle(t, Config{
		ID: 1, Peers: []NodeID{2},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
		SnapshotThreshold: 1,
	}, NewKV(), sto)
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	n.Step(Message{Type: MsgClientPropose, From: 9, To: 1, ClientID: 1, Seq: 1, Command: EncodePut("a", "1")})
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	if n.SnapshotMeta().LastIndex == 0 {
		t.Fatalf("expected compaction")
	}
	n.nextIndex[2] = 1
	tr.sent = nil
	n.Step(Message{Type: MsgTickHeartbeat, From: 1, To: 1})
	found := false
	for _, m := range tr.sent {
		if m.Type == MsgInstallSnapshot && m.To == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("want install snapshot, sent %#v", tr.sent)
	}
}

func TestJointConfRequiresDualQuorum(t *testing.T) {
	sto := &memStore{}
	n, _, _ := electSingle(t, Config{
		ID: 1, Peers: []NodeID{2},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
	}, NopSM{}, sto)
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	n.Step(Message{Type: MsgClientPropose, From: 9, To: 1, ClientID: 1, Seq: 1, Command: EncodeJoint([]NodeID{1, 3})})
	if !n.Conf().InJoint() {
		t.Fatalf("expected joint %+v", n.Conf())
	}
	before := n.Commit()
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	if n.Commit() != before {
		t.Fatalf("old majority alone must not commit, commit %d -> %d", before, n.Commit())
	}
	n.Step(Message{Type: MsgAppendEntriesResp, From: 3, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	if n.Commit() <= before {
		t.Fatalf("dual quorum should commit, commit %d", n.Commit())
	}
}

func TestLeaveJointAfterCommit(t *testing.T) {
	sto := &memStore{}
	n, _, _ := electSingle(t, Config{
		ID: 1, Peers: []NodeID{2},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
	}, NopSM{}, sto)
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	n.Step(Message{Type: MsgClientPropose, From: 9, To: 1, ClientID: 1, Seq: 1, Command: EncodeJoint([]NodeID{1, 2, 3})})
	if !n.Conf().InJoint() {
		t.Fatalf("want joint after propose, conf %+v", n.Conf())
	}
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	n.Step(Message{Type: MsgAppendEntriesResp, From: 3, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	if n.Conf().InJoint() {
		t.Fatalf("leave should apply, conf %+v", n.Conf())
	}
	want := []NodeID{1, 2, 3}
	if !idsEqual(n.Conf().Voters, want) {
		t.Fatalf("voters %v", n.Conf().Voters)
	}
}

func TestReadIndexWaitsForHeartbeatQuorum(t *testing.T) {
	kv := NewKV()
	sto := &memStore{}
	n, tr, _ := electSingle(t, Config{
		ID: 1, Peers: []NodeID{2, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
	}, kv, sto)
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	n.Step(Message{Type: MsgClientPropose, From: 9, To: 1, ClientID: 1, Seq: 1, Command: EncodePut("k", "v")})
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	tr.sent = nil
	n.Step(Message{Type: MsgClientRead, From: 9, To: 1, ClientID: 2, Seq: 2, Command: EncodeGet("k")})
	for _, m := range tr.sent {
		if m.Type == MsgClientReadResp && m.Ok {
			t.Fatalf("read served before quorum heartbeat")
		}
	}
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	ok := false
	for _, m := range tr.sent {
		if m.Type == MsgClientReadResp && m.Ok && string(m.Result) == "v" {
			ok = true
		}
	}
	if !ok {
		t.Fatalf("want linearizable read, sent %#v", tr.sent)
	}
}

func TestReadLeaseServesLocally(t *testing.T) {
	kv := NewKV()
	sto := &memStore{}
	n, tr, clk := electSingle(t, Config{
		ID: 1, Peers: []NodeID{2},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
		ReadLease: 10,
	}, kv, sto)
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	n.Step(Message{Type: MsgClientPropose, From: 9, To: 1, ClientID: 1, Seq: 1, Command: EncodePut("k", "v")})
	n.Step(Message{Type: MsgAppendEntriesResp, From: 2, To: 1, Term: n.Term(), Success: true, MatchIndex: n.LastIndex()})
	clk.n = 3
	tr.sent = nil
	n.Step(Message{Type: MsgClientRead, From: 9, To: 1, ClientID: 2, Seq: 2, Command: EncodeGet("k")})
	ok := false
	for _, m := range tr.sent {
		if m.Type == MsgClientReadResp && m.Ok && string(m.Result) == "v" {
			ok = true
		}
	}
	if !ok {
		t.Fatalf("lease read failed, sent %#v", tr.sent)
	}
}
