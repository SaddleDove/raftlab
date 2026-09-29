package raft

func CatchQuorumDoubleCount() bool {
	tr := &catchTr{}
	sto := &catchStore{}
	to := &catchTO{}
	n, err := NewNode(Config{
		ID: 1, Peers: []NodeID{2, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
		Bug: BugQuorumCountLeaderTwice,
	}, tr, catchClk{}, sto, NopSM{}, to, catchRnd{})
	if err != nil {
		return false
	}
	n.Step(Message{Type: MsgTickElection, From: 1, To: 1})
	return n.Role() == Leader
}

func CatchCorrectQuorumNoLoneLeader() bool {
	tr := &catchTr{}
	n, err := NewNode(Config{
		ID: 1, Peers: []NodeID{2, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
	}, tr, catchClk{}, &catchStore{}, NopSM{}, &catchTO{}, catchRnd{})
	if err != nil {
		return false
	}
	n.Step(Message{Type: MsgTickElection, From: 1, To: 1})
	return n.Role() != Leader
}

func CatchStaleVoteGranted() bool {
	tr := &catchTr{}
	sto := &catchStore{ents: []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}}
	n, err := NewNode(Config{
		ID: 2, Peers: []NodeID{1, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
		Bug: BugNoLogUpToDateCheck,
	}, tr, catchClk{}, sto, NopSM{}, &catchTO{}, catchRnd{})
	if err != nil {
		return false
	}
	tr.sent = nil
	n.Step(Message{
		Type: MsgRequestVote, From: 3, To: 2, Term: 1,
		LastLogIndex: 0, LastLogTerm: 0,
	})
	for _, m := range tr.sent {
		if m.Type == MsgRequestVoteResp && m.VoteGranted {
			return true
		}
	}
	return false
}

func CatchCorrectRejectsStaleVote() bool {
	tr := &catchTr{}
	sto := &catchStore{ents: []LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
	}}
	n, err := NewNode(Config{
		ID: 2, Peers: []NodeID{1, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
	}, tr, catchClk{}, sto, NopSM{}, &catchTO{}, catchRnd{})
	if err != nil {
		return false
	}
	n.Step(Message{
		Type: MsgRequestVote, From: 3, To: 2, Term: 1,
		LastLogIndex: 0, LastLogTerm: 0,
	})
	for _, m := range tr.sent {
		if m.Type == MsgRequestVoteResp && m.VoteGranted {
			return false
		}
	}
	return true
}

func CatchOldTermBareMajority() bool {
	tr := &catchTr{}
	sto := &catchStore{ents: []LogEntry{
		{Term: 1, Index: 1, Command: []byte("old")},
	}}
	n, err := NewNode(Config{
		ID: 1, Peers: []NodeID{2, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
		Bug: BugCommitOldTermOnBareMajority,
	}, tr, catchClk{}, sto, NopSM{}, &catchTO{}, catchRnd{})
	if err != nil {
		return false
	}
	n.Step(Message{Type: MsgTickElection, From: 1, To: 1})
	n.Step(Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: n.Term(), VoteGranted: true})
	if n.Role() != Leader {
		return false
	}
	n.matchIndex[2] = 1
	n.advanceCommit()
	return n.Commit() >= 1
}

func CatchNoUpdateTermOnAE() bool {
	tr := &catchTr{}
	n, err := NewNode(Config{
		ID: 1, Peers: []NodeID{2, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
		Bug: BugNoUpdateTermOnAE,
	}, tr, catchClk{}, &catchStore{}, NopSM{}, &catchTO{}, catchRnd{})
	if err != nil {
		return false
	}
	n.Step(Message{Type: MsgTickElection, From: 1, To: 1})
	old := n.Term()
	n.Step(Message{
		Type:         MsgAppendEntries,
		From:         2,
		To:           1,
		Term:         old + 5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	})
	return n.Term() == old
}

func CatchCorrectUpdatesTermOnAE() bool {
	tr := &catchTr{}
	n, err := NewNode(Config{
		ID: 1, Peers: []NodeID{2, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
	}, tr, catchClk{}, &catchStore{}, NopSM{}, &catchTO{}, catchRnd{})
	if err != nil {
		return false
	}
	n.Step(Message{Type: MsgTickElection, From: 1, To: 1})
	old := n.Term()
	n.Step(Message{
		Type:         MsgAppendEntries,
		From:         2,
		To:           1,
		Term:         old + 5,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 0,
	})
	return n.Term() == old+5
}

func CatchCorrectHoldsOldTerm() bool {
	tr := &catchTr{}
	sto := &catchStore{ents: []LogEntry{
		{Term: 1, Index: 1, Command: []byte("old")},
	}}
	n, err := NewNode(Config{
		ID: 1, Peers: []NodeID{2, 3},
		ElectionTickMin: 5, ElectionTickMax: 5, HeartbeatTick: 1,
	}, tr, catchClk{}, sto, NopSM{}, &catchTO{}, catchRnd{})
	if err != nil {
		return false
	}
	n.Step(Message{Type: MsgTickElection, From: 1, To: 1})
	n.Step(Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: n.Term(), VoteGranted: true})
	if n.Role() != Leader {
		return false
	}
	n.matchIndex[2] = 1
	n.advanceCommit()
	return n.Commit() < 1 || func() bool {
		t, _ := n.termOf(n.Commit())
		return t == n.Term()
	}()
}

type catchTr struct{ sent []Message }

func (c *catchTr) Send(m Message) { c.sent = append(c.sent, m.Clone()) }

type catchClk struct{}

func (catchClk) Now() Instant { return 0 }

type catchTO struct{ elec, hb Instant }

func (c *catchTO) ResetElection(d Instant)  { c.elec = d }
func (c *catchTO) ResetHeartbeat(d Instant) { c.hb = d }
func (c *catchTO) StopHeartbeat()           {}

type catchRnd struct{}

func (catchRnd) Intn(n int) int { return 0 }

type catchStore struct {
	hs            HardState
	ents          []LogEntry
	snapshotIndex Index
	snapshotTerm  Term
	snap          SnapshotMeta
}

func (m *catchStore) HardState() HardState { return m.hs }
func (m *catchStore) SetHardState(hs HardState) error {
	m.hs = hs
	return nil
}
func (m *catchStore) LastIndex() Index {
	if n := len(m.ents); n > 0 {
		return m.ents[n-1].Index
	}
	return m.snapshotIndex
}
func (m *catchStore) FirstIndex() Index { return m.snapshotIndex + 1 }
func (m *catchStore) Term(i Index) (Term, error) {
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
func (m *catchStore) Entries(lo, hi Index) ([]LogEntry, error) {
	var out []LogEntry
	for _, e := range m.ents {
		if e.Index >= lo && e.Index < hi {
			out = append(out, e.Clone())
		}
	}
	return out, nil
}
func (m *catchStore) Append(entries []LogEntry) error {
	m.ents = append(m.ents, CloneEntries(entries)...)
	return nil
}
func (m *catchStore) TruncateSuffix(from Index) error {
	kept := m.ents[:0]
	for _, e := range m.ents {
		if e.Index < from {
			kept = append(kept, e)
		}
	}
	m.ents = kept
	return nil
}
func (m *catchStore) TruncatePrefix(Index) error { return nil }
func (m *catchStore) Sync() error                { return nil }
func (m *catchStore) Snapshot() SnapshotMeta {
	out := m.snap
	out.LastIndex = m.snapshotIndex
	out.LastTerm = m.snapshotTerm
	return out
}
func (m *catchStore) ApplySnapshot(meta SnapshotMeta) error {
	keep := make([]LogEntry, 0, len(m.ents))
	for _, e := range m.ents {
		if e.Index > meta.LastIndex {
			keep = append(keep, e)
		}
	}
	m.ents = keep
	m.snapshotIndex = meta.LastIndex
	m.snapshotTerm = meta.LastTerm
	m.snap = meta
	return nil
}
