package raft

type Node struct {
	cfg Config
	tr  Transport
	clk Clock
	sto LogStore
	sm  StateMachine
	to  Timeouts
	rnd Rand
	obs Observer

	role        Role
	term        Term
	votedFor    NodeID
	leader      NodeID
	commitIndex Index
	lastApplied Index

	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index
	votes      map[NodeID]struct{}
	pending    map[Index]Message
	conf       ConfState
	reads      []readWait
	leaseUntil Instant
	hbAcks     map[NodeID]struct{}
}

type readWait struct {
	msg   Message
	index Index
}

func NewNode(cfg Config, tr Transport, clk Clock, sto LogStore, sm StateMachine, to Timeouts, rnd Rand) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if tr == nil || clk == nil || sto == nil || sm == nil || to == nil || rnd == nil {
		return nil, errf("node: nil dependency")
	}
	hs := sto.HardState()
	n := &Node{
		cfg:         cfg,
		tr:          tr,
		clk:         clk,
		sto:         sto,
		sm:          sm,
		to:          to,
		rnd:         rnd,
		obs:         cfg.observer(),
		role:        Follower,
		term:        hs.Term,
		votedFor:    hs.VotedFor,
		commitIndex: hs.Commit,
		lastApplied: sto.FirstIndex() - 1,
		nextIndex:   make(map[NodeID]Index),
		matchIndex:  make(map[NodeID]Index),
		votes:       make(map[NodeID]struct{}),
		pending:     make(map[Index]Message),
		hbAcks:      make(map[NodeID]struct{}),
		conf:        confFromConfig(cfg),
	}
	if meta := sto.Snapshot(); meta.LastIndex > 0 {
		if len(meta.Data) > 0 {
			sm.Restore(meta.Data)
		}
		if n.lastApplied < meta.LastIndex {
			n.lastApplied = meta.LastIndex
		}
		if n.commitIndex < meta.LastIndex {
			n.commitIndex = meta.LastIndex
		}
	}
	n.reloadConf()
	if n.lastApplied > n.commitIndex {
		n.lastApplied = n.commitIndex
	}
	n.resetElection()
	n.emitState()
	return n, nil
}

func (n *Node) ID() NodeID     { return n.cfg.ID }
func (n *Node) Role() Role     { return n.role }
func (n *Node) Term() Term     { return n.term }
func (n *Node) Leader() NodeID { return n.leader }
func (n *Node) Commit() Index  { return n.commitIndex }
func (n *Node) Applied() Index { return n.lastApplied }
func (n *Node) VotedFor() NodeID {
	return n.votedFor
}

func (n *Node) HardState() HardState {
	return HardState{Term: n.term, VotedFor: n.votedFor, Commit: n.commitIndex}
}

func (n *Node) LastIndex() Index  { return n.sto.LastIndex() }
func (n *Node) FirstIndex() Index { return n.sto.FirstIndex() }
func (n *Node) Conf() ConfState   { return n.conf }
func (n *Node) SnapshotMeta() SnapshotMeta {
	return n.sto.Snapshot()
}

func confFromConfig(cfg Config) ConfState {
	if cfg.Join {
		return ConfState{}
	}
	v := []NodeID{cfg.ID}
	v = append(v, cfg.Peers...)
	return ConfState{Voters: sortIDs(v)}
}

func (n *Node) Step(m Message) {
	switch m.Type {
	case MsgTickElection:
		n.tickElection()
		return
	case MsgTickHeartbeat:
		n.tickHeartbeat()
		return
	}

	if m.Term > n.term {
		if n.cfg.Bug == BugNoUpdateTermOnAE && m.Type == MsgAppendEntries {
		} else {
			n.becomeFollower(m.Term, None)
		}
	}

	switch m.Type {
	case MsgRequestVote:
		n.onRequestVote(m)
	case MsgRequestVoteResp:
		n.onRequestVoteResp(m)
	case MsgAppendEntries:
		n.onAppendEntries(m)
	case MsgAppendEntriesResp:
		n.onAppendEntriesResp(m)
	case MsgClientPropose:
		n.onPropose(m)
	case MsgInstallSnapshot:
		n.onInstallSnapshot(m)
	case MsgInstallSnapshotResp:
		n.onInstallSnapshotResp(m)
	case MsgClientRead:
		n.onRead(m)
	}
	n.maybeApply()
}

func (n *Node) tickElection() {
	if n.role == Leader {
		return
	}
	if !n.conf.Has(n.cfg.ID) {
		n.resetElection()
		return
	}
	n.startElection()
}

func (n *Node) tickHeartbeat() {
	if n.role != Leader {
		return
	}
	n.broadcastAppend()
	n.to.ResetHeartbeat(n.cfg.HeartbeatTick)
}

func (n *Node) startElection() {
	n.role = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leader = None
	n.votes = map[NodeID]struct{}{}
	if n.conf.Has(n.cfg.ID) {
		n.votes[n.cfg.ID] = struct{}{}
	}
	n.persist()
	n.resetElection()
	n.emitState()

	lastIdx := n.sto.LastIndex()
	lastTerm := LastTerm(n.sto)
	for _, p := range n.conf.AllPeers(n.cfg.ID) {
		n.send(Message{
			Type:         MsgRequestVote,
			From:         n.cfg.ID,
			To:           p,
			Term:         n.term,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		})
	}
	if n.hasJointQuorum(func(id NodeID) bool {
		_, ok := n.votes[id]
		return ok
	}) {
		n.becomeLeader()
	}
}

func (n *Node) becomeFollower(term Term, leader NodeID) {
	termChanged := term != n.term
	roleChanged := n.role != Follower
	leaderChanged := n.leader != leader
	n.role = Follower
	if termChanged {
		n.term = term
		n.votedFor = None
	}
	n.leader = leader
	n.to.StopHeartbeat()
	n.leaseUntil = 0
	n.failReads()
	if termChanged || roleChanged {
		n.persist()
	}
	if n.cfg.Bug != BugNoResetElectionTimer {
		n.resetElection()
	}
	if termChanged || roleChanged || leaderChanged {
		n.emitState()
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.cfg.ID
	last := n.sto.LastIndex()
	n.nextIndex = make(map[NodeID]Index)
	n.matchIndex = make(map[NodeID]Index)
	for _, p := range n.conf.AllPeers(n.cfg.ID) {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.hbAcks = map[NodeID]struct{}{}
	n.leaseUntil = 0
	noop := LogEntry{Term: n.term, Index: last + 1}
	_ = n.sto.Append([]LogEntry{noop})
	_ = n.sto.Sync()
	n.obs.OnLog(n.cfg.ID, noop)
	n.to.ResetHeartbeat(n.cfg.HeartbeatTick)
	n.emitState()
	n.broadcastAppend()
	n.advanceCommit()
}

func (n *Node) onRequestVote(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgRequestVoteResp, From: n.cfg.ID, To: m.From, Term: n.term, VoteGranted: false})
		return
	}
	grant := false
	if (n.votedFor == None || n.votedFor == m.From) && n.logUpToDate(m) {
		grant = true
		n.votedFor = m.From
		n.persist()
		if n.cfg.Bug != BugNoResetElectionTimer {
			n.resetElection()
		}
	}
	n.send(Message{Type: MsgRequestVoteResp, From: n.cfg.ID, To: m.From, Term: n.term, VoteGranted: grant})
}

func (n *Node) logUpToDate(m Message) bool {
	if n.cfg.Bug == BugNoLogUpToDateCheck {
		return true
	}
	lastIdx := n.sto.LastIndex()
	lastTerm, _ := n.termOf(lastIdx)
	if m.LastLogTerm > lastTerm {
		return true
	}
	if m.LastLogTerm == lastTerm && m.LastLogIndex >= lastIdx {
		return true
	}
	return false
}

func (n *Node) onRequestVoteResp(m Message) {
	if n.role != Candidate || m.Term != n.term {
		return
	}
	if !m.VoteGranted {
		return
	}
	if n.votes == nil {
		n.votes = map[NodeID]struct{}{n.cfg.ID: {}}
	}
	n.votes[m.From] = struct{}{}
	if n.hasJointQuorum(func(id NodeID) bool {
		_, ok := n.votes[id]
		return ok
	}) {
		n.becomeLeader()
	}
}

func (n *Node) voteCount() int {
	return n.countIn(n.conf.Voters, n.votes)
}

func (n *Node) quorum() int {
	if n.cfg.Bug == BugQuorumCountLeaderTwice {
		return majority(len(n.conf.Voters) + 1)
	}
	return majority(len(n.conf.Voters))
}

func (n *Node) countIn(group []NodeID, set map[NodeID]struct{}) int {
	c := 0
	for _, id := range group {
		if _, ok := set[id]; ok {
			c++
		}
	}
	if n.cfg.Bug == BugQuorumCountLeaderTwice && containsID(group, n.cfg.ID) {
		c++
	}
	return c
}

func (n *Node) hasJointQuorum(match func(NodeID) bool) bool {
	inC := 0
	for _, id := range n.conf.Voters {
		if match(id) {
			inC++
		}
	}
	if n.cfg.Bug == BugQuorumCountLeaderTwice && containsID(n.conf.Voters, n.cfg.ID) {
		inC++
	}
	if inC < majority(len(n.conf.Voters)) {
		return false
	}
	if len(n.conf.Outgoing) == 0 {
		return true
	}
	outC := 0
	for _, id := range n.conf.Outgoing {
		if match(id) {
			outC++
		}
	}
	return outC >= majority(len(n.conf.Outgoing))
}

func (n *Node) onAppendEntries(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgAppendEntriesResp, From: n.cfg.ID, To: m.From, Term: n.term, Success: false})
		return
	}
	if n.cfg.Bug == BugNoUpdateTermOnAE && m.Term > n.term {
		return
	}
	n.becomeFollower(m.Term, m.From)

	prevOK := true
	if m.PrevLogIndex > 0 {
		t, err := n.termOf(m.PrevLogIndex)
		if err != nil || t != m.PrevLogTerm {
			prevOK = false
		}
	}
	if !prevOK {
		n.send(Message{Type: MsgAppendEntriesResp, From: n.cfg.ID, To: m.From, Term: n.term, Success: false, MatchIndex: n.sto.LastIndex()})
		return
	}
	if len(m.Entries) > 0 {
		n.appendEntries(m.Entries)
	}
	if m.LeaderCommit > n.commitIndex {
		lastNew := n.sto.LastIndex()
		if m.LeaderCommit < lastNew {
			lastNew = m.LeaderCommit
		}
		if lastNew > n.commitIndex {
			n.commitIndex = lastNew
			n.persistCommit()
			t, _ := n.termOf(n.commitIndex)
			n.obsCommit(n.commitIndex, t)
		}
	}
	n.send(Message{
		Type:       MsgAppendEntriesResp,
		From:       n.cfg.ID,
		To:         m.From,
		Term:       n.term,
		Success:    true,
		MatchIndex: n.sto.LastIndex(),
	})
}

func (n *Node) appendEntries(ents []LogEntry) {
	first := n.sto.FirstIndex()
	for _, e := range ents {
		if e.Index < first {
			continue
		}
		t, err := n.termOf(e.Index)
		if err == nil && t != e.Term {
			_ = n.sto.TruncateSuffix(e.Index)
			break
		}
		if err != nil {
			break
		}
	}
	last := n.sto.LastIndex()
	toAppend := make([]LogEntry, 0, len(ents))
	for _, e := range ents {
		if e.Index < first {
			continue
		}
		if e.Index <= last {
			t, err := n.termOf(e.Index)
			if err == nil && t == e.Term {
				continue
			}
		}
		toAppend = append(toAppend, e)
	}
	if len(toAppend) > 0 {
		_ = n.sto.Append(toAppend)
		_ = n.sto.Sync()
		for _, e := range toAppend {
			n.obs.OnLog(n.cfg.ID, e)
		}
		n.reloadConf()
	}
}

func (n *Node) onAppendEntriesResp(m Message) {
	if n.role != Leader || m.Term != n.term {
		return
	}
	if !m.Success {
		first := n.sto.FirstIndex()
		if n.nextIndex[m.From] > first {
			n.nextIndex[m.From]--
			n.sendAppend(m.From)
		} else {
			n.sendSnapshot(m.From)
		}
		return
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
	}
	n.nextIndex[m.From] = n.matchIndex[m.From] + 1
	n.hbAcks[m.From] = struct{}{}
	n.maybeGrantLease()
	n.advanceCommit()
	n.flushReads()
}

func (n *Node) advanceCommit() {
	if n.role != Leader {
		return
	}
	prev := n.commitIndex
	last := n.sto.LastIndex()
	for i := n.commitIndex + 1; i <= last; i++ {
		if n.cfg.Bug != BugCommitOldTermOnBareMajority {
			t, err := n.termOf(i)
			if err != nil || t != n.term {
				continue
			}
		}
		idx := i
		if n.hasJointQuorum(func(id NodeID) bool {
			if id == n.cfg.ID {
				return true
			}
			return n.matchIndex[id] >= idx
		}) {
			n.commitIndex = i
			t, _ := n.termOf(i)
			n.obsCommit(i, t)
		}
	}
	if n.commitIndex != prev {
		n.persistCommit()
	}
}

func (n *Node) onPropose(m Message) {
	resp := Message{
		Type:       MsgClientProposeResp,
		From:       n.cfg.ID,
		To:         m.From,
		Term:       n.term,
		ClientID:   m.ClientID,
		Seq:        m.Seq,
		LeaderHint: n.leader,
	}
	if n.role != Leader {
		resp.NotLeader = true
		n.send(resp)
		return
	}
	cmd := append([]byte(nil), m.Command...)
	if IsConfChange(cmd) {
		joint, leave, voters, _, ok := ParseConfEx(cmd)
		if !ok || (joint && (n.conf.Pending || n.conf.InJoint())) || (leave && !n.conf.InJoint()) {
			resp.Ok = false
			n.send(resp)
			return
		}
		if joint {
			cmd = EncodeJointWithOld(voters, n.conf.Voters)
		}
	}
	e := LogEntry{
		Term:    n.term,
		Index:   n.sto.LastIndex() + 1,
		Command: cmd,
	}
	_ = n.sto.Append([]LogEntry{e})
	_ = n.sto.Sync()
	n.obs.OnLog(n.cfg.ID, e)
	n.pending[e.Index] = m
	if IsConfChange(e.Command) {
		n.reloadConf()
		n.syncPeerProgress()
	}
	if n.cfg.Bug == BugApplyBeforeCommit && !IsConfChange(e.Command) {
		res := n.sm.Apply(e.Index, e.Command)
		n.lastApplied = e.Index
		n.obs.OnApply(n.cfg.ID, e.Index, e.Command)
		n.replyPropose(e.Index, res)
	}
	n.broadcastAppend()
	n.advanceCommit()
}

func (n *Node) maybeApply() {
	if n.cfg.Bug == BugApplyBeforeCommit {
		return
	}
	first := n.sto.FirstIndex()
	if first > 0 && n.lastApplied+1 < first {
		n.lastApplied = first - 1
	}
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		ents, err := n.sto.Entries(n.lastApplied, n.lastApplied+1)
		if err != nil || len(ents) == 0 {
			n.lastApplied--
			return
		}
		cmd := ents[0].Command
		if IsConfChange(cmd) {
			n.obs.OnApply(n.cfg.ID, ents[0].Index, cmd)
			n.replyPropose(ents[0].Index, []byte("ok"))
			if !n.conf.Has(n.cfg.ID) && n.role == Leader {
				n.becomeFollower(n.term, None)
			}
			continue
		}
		res := n.sm.Apply(ents[0].Index, cmd)
		n.obs.OnApply(n.cfg.ID, ents[0].Index, cmd)
		n.replyPropose(ents[0].Index, res)
	}
	n.flushReads()
	n.maybeSnapshot()
	n.maybeLeaveJoint()
}

func (n *Node) replyPropose(index Index, result []byte) {
	m, ok := n.pending[index]
	if !ok {
		return
	}
	delete(n.pending, index)
	n.send(Message{
		Type:       MsgClientProposeResp,
		From:       n.cfg.ID,
		To:         m.From,
		Term:       n.term,
		ClientID:   m.ClientID,
		Seq:        m.Seq,
		Ok:         true,
		Result:     result,
		LeaderHint: n.cfg.ID,
	})
}

func (n *Node) broadcastAppend() {
	n.hbAcks = map[NodeID]struct{}{n.cfg.ID: {}}
	for _, p := range n.conf.AllPeers(n.cfg.ID) {
		n.sendAppend(p)
	}
}

func (n *Node) sendAppend(to NodeID) {
	ni := n.nextIndex[to]
	if ni == 0 {
		ni = 1
	}
	first := n.sto.FirstIndex()
	if ni < first {
		n.sendSnapshot(to)
		return
	}
	prev := ni - 1
	prevTerm, err := n.termOf(prev)
	if err != nil {
		n.sendSnapshot(to)
		return
	}
	hi := n.sto.LastIndex() + 1
	ents, err := n.sto.Entries(ni, hi)
	if err != nil {
		n.sendSnapshot(to)
		return
	}
	n.send(Message{
		Type:         MsgAppendEntries,
		From:         n.cfg.ID,
		To:           to,
		Term:         n.term,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) termOf(i Index) (Term, error) {
	if i == 0 {
		return 0, nil
	}
	return n.sto.Term(i)
}

func (n *Node) persist() {
	hs := HardState{Term: n.term, VotedFor: n.votedFor, Commit: n.commitIndex}
	if n.cfg.Bug == BugNoPersistVotedFor {
		hs.VotedFor = None
	}
	if n.cfg.Bug == BugNoPersistTerm {
		old := n.sto.HardState()
		hs.Term = old.Term
	}
	_ = n.sto.SetHardState(hs)
	_ = n.sto.Sync()
	n.obs.OnPersist(n.cfg.ID, hs)
}

func (n *Node) persistCommit() {
	hs := n.sto.HardState()
	hs.Commit = n.commitIndex
	_ = n.sto.SetHardState(hs)
	n.obs.OnPersist(n.cfg.ID, hs)
}

func (n *Node) resetElection() {
	span := int(n.cfg.ElectionTickMax - n.cfg.ElectionTickMin + 1)
	d := n.cfg.ElectionTickMin
	if span > 0 {
		d += Instant(n.rnd.Intn(span))
	}
	n.to.ResetElection(d)
}

func (n *Node) send(m Message) {
	n.obs.OnSend(m)
	n.tr.Send(m)
}

func (n *Node) emitState() {
	n.obs.OnState(n.cfg.ID, n.role, n.HardState(), n.lastApplied)
}

func (n *Node) obsCommit(index Index, term Term) {
	type cobs interface {
		OnCommit(NodeID, Index, Term)
	}
	if c, ok := n.obs.(cobs); ok {
		c.OnCommit(n.cfg.ID, index, term)
	}
}

func (n *Node) restoreSnapshotLocked() {
	meta := n.sto.Snapshot()
	if meta.LastIndex == 0 {
		return
	}
	if len(meta.Data) > 0 {
		n.sm.Restore(meta.Data)
	}
	if n.lastApplied < meta.LastIndex {
		n.lastApplied = meta.LastIndex
	}
	if n.commitIndex < meta.LastIndex {
		n.commitIndex = meta.LastIndex
	}
	if len(meta.Voters) > 0 {
		n.conf.Voters = CloneIDs(meta.Voters)
		n.conf.Outgoing = CloneIDs(meta.VotersOld)
		n.conf.Pending = len(meta.VotersOld) > 0
	}
}

func (n *Node) RestoreFromStore() {
	hs := n.sto.HardState()
	n.term = hs.Term
	n.votedFor = hs.VotedFor
	n.commitIndex = hs.Commit
	n.role = Follower
	n.leader = None
	n.lastApplied = n.sto.FirstIndex() - 1
	n.restoreSnapshotLocked()
	n.reloadConf()
	if n.lastApplied > n.commitIndex {
		n.lastApplied = n.commitIndex
	}
	n.resetElection()
	n.emitState()
	n.maybeApply()
}

func (n *Node) sendSnapshot(to NodeID) {
	meta := n.sto.Snapshot()
	if meta.LastIndex == 0 && n.lastApplied > 0 {
		n.compact(n.lastApplied)
		meta = n.sto.Snapshot()
	}
	if meta.LastIndex == 0 {
		return
	}
	n.send(Message{
		Type:              MsgInstallSnapshot,
		From:              n.cfg.ID,
		To:                to,
		Term:              n.term,
		LastIncludedIndex: meta.LastIndex,
		LastIncludedTerm:  meta.LastTerm,
		Snapshot:          meta.Data,
		Voters:            CloneIDs(meta.Voters),
		VotersOld:         CloneIDs(meta.VotersOld),
	})
}

func (n *Node) onInstallSnapshot(m Message) {
	if m.Term < n.term {
		n.send(Message{Type: MsgInstallSnapshotResp, From: n.cfg.ID, To: m.From, Term: n.term, Success: false})
		return
	}
	n.becomeFollower(m.Term, m.From)
	cur := n.sto.Snapshot()
	if m.LastIncludedIndex <= cur.LastIndex || m.LastIncludedIndex <= n.lastApplied {
		n.send(Message{
			Type:              MsgInstallSnapshotResp,
			From:              n.cfg.ID,
			To:                m.From,
			Term:              n.term,
			Success:           true,
			MatchIndex:        n.sto.LastIndex(),
			LastIncludedIndex: maxIndex(cur.LastIndex, n.lastApplied),
		})
		return
	}
	meta := SnapshotMeta{
		LastIndex: m.LastIncludedIndex,
		LastTerm:  m.LastIncludedTerm,
		Data:      append([]byte(nil), m.Snapshot...),
		Voters:    CloneIDs(m.Voters),
		VotersOld: CloneIDs(m.VotersOld),
	}
	if len(meta.Voters) == 0 {
		meta.Voters = CloneIDs(n.conf.Voters)
		meta.VotersOld = CloneIDs(n.conf.Outgoing)
	}
	_ = n.sto.ApplySnapshot(meta)
	_ = n.sto.Sync()
	if len(meta.Data) > 0 {
		n.sm.Restore(meta.Data)
	}
	n.lastApplied = meta.LastIndex
	if n.commitIndex < meta.LastIndex {
		n.commitIndex = meta.LastIndex
	}
	n.reloadConf()
	n.persistCommit()
	n.send(Message{
		Type:              MsgInstallSnapshotResp,
		From:              n.cfg.ID,
		To:                m.From,
		Term:              n.term,
		Success:           true,
		MatchIndex:        meta.LastIndex,
		LastIncludedIndex: meta.LastIndex,
	})
}

func (n *Node) onInstallSnapshotResp(m Message) {
	if n.role != Leader || m.Term != n.term || !m.Success {
		return
	}
	idx := m.LastIncludedIndex
	if idx == 0 {
		idx = m.MatchIndex
	}
	if idx > n.matchIndex[m.From] {
		n.matchIndex[m.From] = idx
	}
	n.nextIndex[m.From] = n.matchIndex[m.From] + 1
	n.hbAcks[m.From] = struct{}{}
	n.maybeGrantLease()
	n.advanceCommit()
	n.flushReads()
}

func (n *Node) maybeSnapshot() {
	th := n.cfg.SnapshotThreshold
	if th == 0 || n.lastApplied == 0 {
		return
	}
	meta := n.sto.Snapshot()
	if n.lastApplied <= meta.LastIndex {
		return
	}
	if n.lastApplied-meta.LastIndex < th {
		return
	}
	n.compact(n.lastApplied)
}

func (n *Node) compact(index Index) {
	if index == 0 {
		return
	}
	term, err := n.termOf(index)
	if err != nil {
		return
	}
	meta := SnapshotMeta{
		LastIndex: index,
		LastTerm:  term,
		Data:      n.sm.Snapshot(),
		Voters:    CloneIDs(n.conf.Voters),
		VotersOld: CloneIDs(n.conf.Outgoing),
	}
	_ = n.sto.ApplySnapshot(meta)
	_ = n.sto.Sync()
}

func (n *Node) applyConf(joint, leave bool, voters, outgoing []NodeID) {
	if leave {
		n.conf.Outgoing = nil
		n.conf.Pending = false
	} else if joint {
		if len(outgoing) > 0 {
			n.conf.Outgoing = sortIDs(outgoing)
		} else if len(n.conf.Voters) > 0 {
			n.conf.Outgoing = CloneIDs(n.conf.Voters)
		}
		n.conf.Voters = sortIDs(voters)
		n.conf.Pending = len(n.conf.Outgoing) > 0
	}
	if n.role == Leader {
		n.syncPeerProgress()
	}
}

func (n *Node) maybeLeaveJoint() {
	if n.role != Leader || !n.conf.InJoint() {
		return
	}
	if n.logHasConfLeave() {
		return
	}
	idx := n.latestJointIndex()
	if idx == 0 || idx > n.commitIndex {
		return
	}
	e := LogEntry{
		Term:    n.term,
		Index:   n.sto.LastIndex() + 1,
		Command: EncodeLeaveJoint(),
	}
	_ = n.sto.Append([]LogEntry{e})
	_ = n.sto.Sync()
	n.obs.OnLog(n.cfg.ID, e)
	n.reloadConf()
	n.broadcastAppend()
	n.advanceCommit()
}

func (n *Node) latestJointIndex() Index {
	if snap := n.sto.Snapshot(); len(snap.VotersOld) > 0 {
		return snap.LastIndex
	}
	lo := n.sto.FirstIndex()
	hi := n.sto.LastIndex() + 1
	if lo >= hi {
		return 0
	}
	ents, err := n.sto.Entries(lo, hi)
	if err != nil {
		return 0
	}
	var idx Index
	for _, e := range ents {
		joint, leave, _, ok := ParseConf(e.Command)
		if !ok {
			continue
		}
		if joint {
			idx = e.Index
		}
		if leave {
			idx = 0
		}
	}
	return idx
}

func (n *Node) logHasConfLeave() bool {
	lo := n.lastApplied + 1
	hi := n.sto.LastIndex() + 1
	if lo >= hi {
		return false
	}
	ents, err := n.sto.Entries(lo, hi)
	if err != nil {
		return false
	}
	for _, e := range ents {
		_, leave, _, ok := ParseConf(e.Command)
		if ok && leave {
			return true
		}
	}
	return false
}

func (n *Node) onRead(m Message) {
	if n.role != Leader {
		n.send(Message{
			Type:       MsgClientReadResp,
			From:       n.cfg.ID,
			To:         m.From,
			Term:       n.term,
			ClientID:   m.ClientID,
			Seq:        m.Seq,
			NotLeader:  true,
			LeaderHint: n.leader,
		})
		return
	}
	idx := n.commitIndex
	if n.currentTermCommitted() && n.leaseValid() && n.lastApplied >= idx {
		n.serveRead(m, idx)
		return
	}
	n.reads = append(n.reads, readWait{msg: m, index: idx})
	n.broadcastAppend()
}

func (n *Node) serveRead(m Message, index Index) {
	res := n.sm.Apply(n.lastApplied, m.Command)
	n.send(Message{
		Type:       MsgClientReadResp,
		From:       n.cfg.ID,
		To:         m.From,
		Term:       n.term,
		ClientID:   m.ClientID,
		Seq:        m.Seq,
		Ok:         true,
		Result:     res,
		LeaderHint: n.cfg.ID,
		ReadIndex:  index,
	})
}

func (n *Node) flushReads() {
	if n.role != Leader || len(n.reads) == 0 {
		return
	}
	if !n.currentTermCommitted() {
		return
	}
	confirmed := n.leaseValid() || n.hasJointQuorum(func(id NodeID) bool {
		_, ok := n.hbAcks[id]
		return ok
	})
	if !confirmed {
		return
	}
	remain := n.reads[:0]
	for _, w := range n.reads {
		if n.lastApplied >= w.index {
			n.serveRead(w.msg, w.index)
		} else {
			remain = append(remain, w)
		}
	}
	n.reads = remain
}

func (n *Node) failReads() {
	if len(n.reads) == 0 {
		return
	}
	for _, w := range n.reads {
		n.send(Message{
			Type:       MsgClientReadResp,
			From:       n.cfg.ID,
			To:         w.msg.From,
			Term:       n.term,
			ClientID:   w.msg.ClientID,
			Seq:        w.msg.Seq,
			NotLeader:  true,
			LeaderHint: n.leader,
		})
	}
	n.reads = nil
}

func (n *Node) leaseValid() bool {
	if n.cfg.ReadLease <= 0 || n.role != Leader {
		return false
	}
	return n.clk.Now() < n.leaseUntil
}

func (n *Node) maybeGrantLease() {
	if n.cfg.ReadLease <= 0 || n.role != Leader {
		return
	}
	if n.hasJointQuorum(func(id NodeID) bool {
		_, ok := n.hbAcks[id]
		return ok
	}) {
		n.leaseUntil = n.clk.Now() + n.cfg.ReadLease
	}
}

func (n *Node) currentTermCommitted() bool {
	if n.commitIndex == 0 {
		return false
	}
	t, err := n.termOf(n.commitIndex)
	return err == nil && t == n.term
}

func (n *Node) reloadConf() {
	meta := n.sto.Snapshot()
	if len(meta.Voters) > 0 {
		n.conf.Voters = sortIDs(meta.Voters)
		n.conf.Outgoing = sortIDs(meta.VotersOld)
		n.conf.Pending = len(meta.VotersOld) > 0
	} else {
		n.conf = confFromConfig(n.cfg)
	}
	lo := n.sto.FirstIndex()
	hi := n.sto.LastIndex() + 1
	if lo >= hi {
		return
	}
	ents, err := n.sto.Entries(lo, hi)
	if err != nil {
		return
	}
	for _, e := range ents {
		joint, leave, voters, outgoing, ok := ParseConfEx(e.Command)
		if ok {
			n.applyConf(joint, leave, voters, outgoing)
		}
	}
}

func (n *Node) syncPeerProgress() {
	if n.role != Leader {
		return
	}
	first := n.sto.FirstIndex()
	if first == 0 {
		first = 1
	}
	for _, p := range n.conf.AllPeers(n.cfg.ID) {
		if _, ok := n.nextIndex[p]; !ok {
			n.nextIndex[p] = first
			n.matchIndex[p] = 0
		}
	}
}
