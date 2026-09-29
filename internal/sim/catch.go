package sim

import (
	"fmt"

	"raftlab/internal/check"
	"raftlab/internal/raft"
)

type CatchResult struct {
	Caught     bool
	Bug        raft.Bug
	Seed       uint64
	Until      raft.Instant
	Violation  string
	Leaders    int
	CommitLone bool
}

func CatchBug(b raft.Bug) CatchResult {
	switch b {
	case raft.BugQuorumCountLeaderTwice:
		if raft.CatchQuorumDoubleCount() && raft.CatchCorrectQuorumNoLoneLeader() {
			return CatchResult{Bug: b, Caught: true, Seed: 0, Violation: "I1/quorum: lone node elected by counting itself twice"}
		}
	case raft.BugNoLogUpToDateCheck:
		if raft.CatchStaleVoteGranted() && raft.CatchCorrectRejectsStaleVote() {
			return CatchResult{Bug: b, Caught: true, Seed: 0, Violation: "I5: RequestVote granted to stale log"}
		}
	case raft.BugCommitOldTermOnBareMajority:
		if raft.CatchOldTermBareMajority() && raft.CatchCorrectHoldsOldTerm() {
			return CatchResult{Bug: b, Caught: true, Seed: 0, Violation: "I7: old-term entry committed on bare majority"}
		}
	case raft.BugNoUpdateTermOnAE:
		if raft.CatchNoUpdateTermOnAE() && raft.CatchCorrectUpdatesTermOnAE() {
			return CatchResult{Bug: b, Caught: true, Seed: 0, Violation: "I2: higher-term AppendEntries did not update term"}
		}
	}
	for seed := uint64(1); seed <= 30; seed++ {
		if r := catchOnce(b, seed); r.Caught {
			return r
		}
	}
	return CatchResult{Bug: b, Caught: false}
}

func catchOnce(b raft.Bug, seed uint64) CatchResult {
	out := CatchResult{Bug: b, Seed: seed}
	cfg := DefaultClusterConfig(3, seed)
	cfg.Bug = b
	cfg.UseKV = true
	c, err := NewCluster(cfg)
	if err != nil {
		return out
	}
	switch b {
	case raft.BugQuorumCountLeaderTwice:
		c.Crash(2)
		c.Crash(3)
		c.RunUntil(80)
		env := c.env(1)
		if env != nil && env.n.Role() == raft.Leader {
			out.Caught = true
			out.Until = c.Now()
			out.Violation = "lone node elected by counting itself twice"
			return out
		}
	case raft.BugNoResetElectionTimer:
		c.RunUntil(120)
		if c.ElectionCount() > 8 {
			out.Caught = true
			out.Until = c.Now()
			out.Violation = fmt.Sprintf("election storm count=%d", c.ElectionCount())
			return out
		}
	case raft.BugNoUpdateTermOnAE:
		if lid := c.WaitLeader(80); lid != 0 {
			var rest []raft.NodeID
			for i := 1; i <= 3; i++ {
				id := raft.NodeID(i)
				if id != lid {
					rest = append(rest, id)
				}
			}
			c.Partition([]raft.NodeID{lid}, rest, false)
			c.RunUntil(c.Now() + 80)
			c.Heal()
			c.RunUntil(c.Now() + 80)
			if c.MaxLeaders() > 1 {
				out.Caught = true
				out.Until = c.Now()
				out.Violation = fmt.Sprintf("concurrent leaders seen=%d", c.MaxLeaders())
				return out
			}
		}
	case raft.BugNoLogUpToDateCheck:
		c.Crash(3)
		if c.WaitLeader(80) != 0 {
			c.Propose("k", "committed")
			c.RunUntil(c.Now() + 40)
		}
		_ = c.Restart(3)
		c.Crash(1)
		c.RunUntil(c.Now() + 80)
	case raft.BugCommitOldTermOnBareMajority:
		if c.WaitLeader(80) != 0 {
			c.Propose("a", "1")
			c.RunUntil(c.Now() + 8)
			c.Crash(c.Leader())
			c.RunUntil(c.Now() + 50)
			if id := c.Leader(); id != 0 {
				c.Propose("b", "2")
				c.RunUntil(c.Now() + 30)
			}
		}
	case raft.BugApplyBeforeCommit:
		if c.WaitLeader(80) != 0 {
			c.Propose("k", "v")
			c.RunUntil(c.Now() + 20)
		}
	case raft.BugNoPersistVotedFor, raft.BugNoPersistTerm:
		c.RunUntil(60)
		c.Crash(1)
		_ = c.Restart(1)
		c.RunUntil(c.Now() + 60)
	default:
		c.RunUntil(100)
	}
	out.Until = c.Now()
	out.Leaders = len(c.Leaders())
	if out.Leaders > 1 {
		out.Caught = true
		out.Violation = fmt.Sprintf("concurrent leaders %v", c.Leaders())
		return out
	}
	if vs := c.Monitor().Violations(); len(vs) > 0 {
		out.Caught = true
		out.Violation = vs[0].String()
		return out
	}
	v := check.CrossCheck(c.History())
	if !v.Linearizable {
		out.Caught = true
		out.Violation = "lin " + v.Detail
		return out
	}
	return out
}
