package raft

import "fmt"

type InvCode int

const (
	I1ElectionSafety InvCode = iota + 1
	I2TermMonotonic
	I3VotePersisted
	I4LogMatching
	I5LeaderCompleteness
	I6StateMachineSafety
	I7CommittedDurable
	I8NoSplitBrainWrite
	I9ApplyLeCommit
)

func (c InvCode) String() string {
	switch c {
	case I1ElectionSafety:
		return "I1-election-safety"
	case I2TermMonotonic:
		return "I2-term-monotonic"
	case I3VotePersisted:
		return "I3-vote-persisted"
	case I4LogMatching:
		return "I4-log-matching"
	case I5LeaderCompleteness:
		return "I5-leader-completeness"
	case I6StateMachineSafety:
		return "I6-state-machine-safety"
	case I7CommittedDurable:
		return "I7-committed-durable"
	case I8NoSplitBrainWrite:
		return "I8-no-split-brain-write"
	case I9ApplyLeCommit:
		return "I9-apply-le-commit"
	default:
		return fmt.Sprintf("I%d", int(c))
	}
}

type Violation struct {
	Code   InvCode
	At     Instant
	Node   NodeID
	Detail string
	Term   Term
	Index  Index
}

func (v Violation) String() string {
	return fmt.Sprintf("%s t=%d node=%d term=%d idx=%d %s", v.Code, v.At, v.Node, v.Term, v.Index, v.Detail)
}

type Monitor struct {
	clk Clock
	obs Observer

	leaders    map[Term]NodeID
	lastTerm   map[NodeID]Term
	lastHS     map[NodeID]HardState
	applied    map[NodeID]Index
	logAt      map[NodeID]map[Index]logSlot
	commitAt   map[Index]Term
	appliedCmd map[Index]string
	violations []Violation
}

type logSlot struct {
	Term Term
	Cmd  string
}

func NewMonitor(clk Clock, obs Observer) *Monitor {
	if obs == nil {
		obs = NopObserver{}
	}
	return &Monitor{
		clk:        clk,
		obs:        obs,
		leaders:    map[Term]NodeID{},
		lastTerm:   map[NodeID]Term{},
		lastHS:     map[NodeID]HardState{},
		applied:    map[NodeID]Index{},
		logAt:      map[NodeID]map[Index]logSlot{},
		commitAt:   map[Index]Term{},
		appliedCmd: map[Index]string{},
	}
}

func (m *Monitor) now() Instant {
	if m.clk == nil {
		return 0
	}
	return m.clk.Now()
}

func (m *Monitor) Violations() []Violation { return append([]Violation(nil), m.violations...) }

func (m *Monitor) Note(v Violation) { m.raise(v) }

func (m *Monitor) raise(v Violation) {
	v.At = m.now()
	m.violations = append(m.violations, v)
	m.obs.OnViolation(v)
}

func (m *Monitor) OnState(id NodeID, role Role, hs HardState, lastApplied Index) {
	if prev, ok := m.lastTerm[id]; ok && hs.Term < prev {
		m.raise(Violation{Code: I2TermMonotonic, Node: id, Term: hs.Term, Detail: fmt.Sprintf("term decreased %d -> %d", prev, hs.Term)})
	}
	m.lastTerm[id] = hs.Term
	m.lastHS[id] = hs
	m.applied[id] = lastApplied

	if lastApplied > hs.Commit {
		m.raise(Violation{Code: I9ApplyLeCommit, Node: id, Index: lastApplied, Detail: fmt.Sprintf("applied %d > commit %d", lastApplied, hs.Commit)})
	}

	if role == Leader {
		if other, ok := m.leaders[hs.Term]; ok && other != id {
			m.raise(Violation{Code: I1ElectionSafety, Node: id, Term: hs.Term, Detail: fmt.Sprintf("two leaders in term %d: %d and %d", hs.Term, other, id)})
		} else {
			m.leaders[hs.Term] = id
		}
	}
}

func (m *Monitor) OnSend(msg Message) {
	if msg.Type == MsgRequestVoteResp && msg.VoteGranted {
		hs := m.lastHS[msg.From]
		if hs.VotedFor != msg.To || hs.Term != msg.Term {
			m.raise(Violation{
				Code:   I3VotePersisted,
				Node:   msg.From,
				Term:   msg.Term,
				Detail: fmt.Sprintf("granted vote to %d term %d but persisted votedFor=%d term=%d", msg.To, msg.Term, hs.VotedFor, hs.Term),
			})
		}
	}
}

func (m *Monitor) OnPersist(id NodeID, hs HardState) {
	prev := m.lastHS[id]
	if hs.Term < prev.Term {
		m.raise(Violation{Code: I2TermMonotonic, Node: id, Term: hs.Term, Detail: "persisted term decreased"})
	}
	m.lastHS[id] = hs
}

func (m *Monitor) OnApply(id NodeID, index Index, command []byte) {
	key := string(command)
	if prev, ok := m.appliedCmd[index]; ok && prev != key {
		m.raise(Violation{
			Code:   I6StateMachineSafety,
			Node:   id,
			Index:  index,
			Detail: fmt.Sprintf("index %d applied %q vs %q", index, key, prev),
		})
	}
	m.appliedCmd[index] = key
	hs := m.lastHS[id]
	if index > hs.Commit {
		m.raise(Violation{Code: I9ApplyLeCommit, Node: id, Index: index, Detail: "apply before commit"})
	}
}

func (m *Monitor) OnLog(id NodeID, e LogEntry) {
	if m.logAt[id] == nil {
		m.logAt[id] = map[Index]logSlot{}
	}
	slot := logSlot{Term: e.Term, Cmd: string(e.Command)}
	if prev, ok := m.logAt[id][e.Index]; ok && prev.Term == e.Term && prev.Cmd != slot.Cmd {
		m.raise(Violation{Code: I4LogMatching, Node: id, Index: e.Index, Term: e.Term, Detail: "same index+term different command"})
	}
	m.logAt[id][e.Index] = slot
	for other, logs := range m.logAt {
		if other == id {
			continue
		}
		if t, ok := logs[e.Index]; ok && t.Term == e.Term && t.Cmd != slot.Cmd {
			m.raise(Violation{
				Code:   I4LogMatching,
				Node:   id,
				Index:  e.Index,
				Term:   e.Term,
				Detail: fmt.Sprintf("conflict with node %d at index %d term %d", other, e.Index, e.Term),
			})
		}
	}
}

func (m *Monitor) NoteCommit(index Index, term Term) {
	if t, ok := m.commitAt[index]; ok && t != term {
		m.raise(Violation{Code: I7CommittedDurable, Index: index, Term: term, Detail: fmt.Sprintf("commit term changed %d -> %d", t, term)})
	}
	m.commitAt[index] = term
}

func (m *Monitor) OnCommit(id NodeID, index Index, term Term) {
	m.NoteCommit(index, term)
}

func (m *Monitor) OnViolation(v Violation) {}
