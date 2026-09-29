package raft

import "fmt"

type NodeID uint64
type Term uint64
type Index uint64
type Instant int64

const None NodeID = 0

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

type HardState struct {
	Term     Term
	VotedFor NodeID
	Commit   Index
}

type LogEntry struct {
	Term    Term
	Index   Index
	Command []byte
}

func (e LogEntry) Clone() LogEntry {
	out := LogEntry{Term: e.Term, Index: e.Index}
	if len(e.Command) > 0 {
		out.Command = append([]byte(nil), e.Command...)
	}
	return out
}

func CloneEntries(ents []LogEntry) []LogEntry {
	if len(ents) == 0 {
		return nil
	}
	out := make([]LogEntry, len(ents))
	for i := range ents {
		out[i] = ents[i].Clone()
	}
	return out
}

type MsgType int

const (
	MsgTickElection MsgType = iota + 1
	MsgTickHeartbeat
	MsgRequestVote
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp
	MsgInstallSnapshot
	MsgInstallSnapshotResp
	MsgClientPropose
	MsgClientProposeResp
	MsgClientRead
	MsgClientReadResp
)

func (t MsgType) String() string {
	switch t {
	case MsgTickElection:
		return "tick_election"
	case MsgTickHeartbeat:
		return "tick_heartbeat"
	case MsgRequestVote:
		return "request_vote"
	case MsgRequestVoteResp:
		return "request_vote_resp"
	case MsgAppendEntries:
		return "append_entries"
	case MsgAppendEntriesResp:
		return "append_entries_resp"
	case MsgInstallSnapshot:
		return "install_snapshot"
	case MsgInstallSnapshotResp:
		return "install_snapshot_resp"
	case MsgClientPropose:
		return "client_propose"
	case MsgClientProposeResp:
		return "client_propose_resp"
	case MsgClientRead:
		return "client_read"
	case MsgClientReadResp:
		return "client_read_resp"
	default:
		return "unknown"
	}
}

type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term Term

	LastLogIndex Index
	LastLogTerm  Term
	VoteGranted  bool

	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index

	Success    bool
	MatchIndex Index

	LastIncludedIndex Index
	LastIncludedTerm  Term
	Snapshot          []byte
	Voters            []NodeID
	VotersOld         []NodeID

	ClientID   uint64
	Seq        uint64
	Command    []byte
	Ok         bool
	Result     []byte
	NotLeader  bool
	LeaderHint NodeID
	ReadIndex  Index
}

func (m Message) Clone() Message {
	out := m
	out.Entries = CloneEntries(m.Entries)
	if len(m.Snapshot) > 0 {
		out.Snapshot = append([]byte(nil), m.Snapshot...)
	}
	if len(m.Command) > 0 {
		out.Command = append([]byte(nil), m.Command...)
	}
	if len(m.Result) > 0 {
		out.Result = append([]byte(nil), m.Result...)
	}
	out.Voters = CloneIDs(m.Voters)
	out.VotersOld = CloneIDs(m.VotersOld)
	return out
}

func (m Message) Canonical() string {
	ents := 0
	sum := uint64(0)
	for _, e := range m.Entries {
		ents++
		sum += uint64(e.Term)
		sum += uint64(e.Index)
		sum += uint64(len(e.Command))
		for _, b := range e.Command {
			sum += uint64(b)
		}
	}
	vg, ok, nl := 0, 0, 0
	if m.VoteGranted {
		vg = 1
	}
	if m.Success {
		ok = 1
	}
	if m.NotLeader {
		nl = 1
	}
	return fmt.Sprintf("typ=%s from=%d to=%d term=%d lli=%d llt=%d vg=%d pli=%d plt=%d ents=%d esum=%d lc=%d ok=%d mi=%d sni=%d snt=%d sn=%d cid=%d seq=%d nl=%d",
		m.Type, m.From, m.To, m.Term,
		m.LastLogIndex, m.LastLogTerm, vg,
		m.PrevLogIndex, m.PrevLogTerm, ents, sum,
		m.LeaderCommit, ok, m.MatchIndex,
		m.LastIncludedIndex, m.LastIncludedTerm, len(m.Snapshot),
		m.ClientID, m.Seq, nl)
}

type Bug int

const (
	BugNone Bug = iota
	BugNoPersistVotedFor
	BugNoPersistTerm
	BugNoUpdateTermOnAE
	BugCommitOldTermOnBareMajority
	BugNoLogUpToDateCheck
	BugApplyBeforeCommit
	BugQuorumCountLeaderTwice
	BugNoResetElectionTimer
)

func (b Bug) String() string {
	switch b {
	case BugNone:
		return "none"
	case BugNoPersistVotedFor:
		return "no_persist_votedfor"
	case BugNoPersistTerm:
		return "no_persist_term"
	case BugNoUpdateTermOnAE:
		return "no_update_term_on_ae"
	case BugCommitOldTermOnBareMajority:
		return "commit_old_term_bare_majority"
	case BugNoLogUpToDateCheck:
		return "no_log_uptodate_check"
	case BugApplyBeforeCommit:
		return "apply_before_commit"
	case BugQuorumCountLeaderTwice:
		return "quorum_count_leader_twice"
	case BugNoResetElectionTimer:
		return "no_reset_election_timer"
	default:
		return "unknown"
	}
}

func AllBugs() []Bug {
	return []Bug{
		BugNoPersistVotedFor,
		BugNoPersistTerm,
		BugNoUpdateTermOnAE,
		BugCommitOldTermOnBareMajority,
		BugNoLogUpToDateCheck,
		BugApplyBeforeCommit,
		BugQuorumCountLeaderTwice,
		BugNoResetElectionTimer,
	}
}

type SnapshotMeta struct {
	LastIndex Index
	LastTerm  Term
	Data      []byte
	Voters    []NodeID
	VotersOld []NodeID
}

func CloneIDs(ids []NodeID) []NodeID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]NodeID, len(ids))
	copy(out, ids)
	return out
}

func idsEqual(a, b []NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
