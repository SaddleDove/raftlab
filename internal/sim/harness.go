package sim

import (
	"fmt"

	"raftlab/internal/check"
	"raftlab/internal/raft"
)

type HarnessReport struct {
	Seeds      int
	N          int
	Fail       int
	LinFail    int
	ReplayFail int
	Caught     map[string]bool
	FirstFail  uint64
	Detail     string
}

func RunElectionGate(seeds int, n int, until raft.Instant) HarnessReport {
	rep := HarnessReport{Seeds: seeds, N: n, Caught: map[string]bool{}}
	timeout := raft.Instant(20)
	bound := 20 * timeout
	ok := 0
	for i := 1; i <= seeds; i++ {
		cfg := DefaultClusterConfig(n, uint64(i))
		cfg.RunUntil = until
		c, err := NewCluster(cfg)
		if err != nil {
			rep.Fail++
			rep.Detail = err.Error()
			continue
		}
		r := c.Run()
		bad := false
		for _, v := range r.Violations {
			if v.Code == raft.I1ElectionSafety || v.Code == raft.I2TermMonotonic || v.Code == raft.I3VotePersisted {
				bad = true
				rep.Detail = v.String()
			}
		}
		if r.FirstLeaderAt == 0 || r.FirstLeaderAt > bound {
			bad = true
			if rep.Detail == "" {
				rep.Detail = fmt.Sprintf("seed %d no leader within %d, first=%d", i, bound, r.FirstLeaderAt)
			}
		} else {
			ok++
		}
		if bad {
			rep.Fail++
			if rep.FirstFail == 0 {
				rep.FirstFail = uint64(i)
			}
		}
	}
	rep.Detail = fmt.Sprintf("leaders %d/%d (%.4f) fail=%d first=%d %s", ok, seeds, float64(ok)/float64(seeds), rep.Fail, rep.FirstFail, rep.Detail)
	return rep
}

func RunLinearizableQuiet(seed uint64, n int) (check.Verdict, []raft.Violation, error) {
	cfg := DefaultClusterConfig(n, seed)
	cfg.UseKV = true
	cfg.Silent = false
	cfg.RunUntil = 80
	c, err := NewCluster(cfg)
	if err != nil {
		return check.Verdict{}, nil, err
	}
	if c.WaitLeader(80) == raft.None {
		return check.Verdict{Linearizable: false, Detail: "no leader"}, c.mon.Violations(), nil
	}
	for i := 0; i < 8; i++ {
		c.Propose(fmt.Sprintf("k%d", i%3), fmt.Sprintf("v%d", i))
		c.RunUntil(c.Now() + 20)
	}
	c.RunUntil(c.Now() + 40)
	return check.CrossCheck(c.History()), c.mon.Violations(), nil
}

func RunCrashGate(seeds int) HarnessReport {
	rep := HarnessReport{Seeds: seeds, N: 3, Caught: map[string]bool{}}
	for i := 1; i <= seeds; i++ {
		cfg := DefaultClusterConfig(3, uint64(i*17+3))
		cfg.UseKV = true
		cfg.RunUntil = 40
		c, err := NewCluster(cfg)
		if err != nil {
			rep.Fail++
			continue
		}
		if c.WaitLeader(60) == 0 {
			rep.Fail++
			rep.Detail = fmt.Sprintf("seed %d no leader", i)
			continue
		}
		c.Propose("x", "1")
		c.RunUntil(c.Now() + 30)
		id := raft.NodeID((i % 3) + 1)
		c.Crash(id)
		c.RunUntil(c.Now() + 20)
		_ = c.Restart(id)
		c.RunUntil(c.Now() + 40)
		for _, v := range c.mon.Violations() {
			if v.Code == raft.I7CommittedDurable || v.Code == raft.I6StateMachineSafety || v.Code == raft.I1ElectionSafety {
				rep.Fail++
				rep.FirstFail = uint64(i)
				rep.Detail = v.String()
				break
			}
		}
	}
	return rep
}

func RunPartitionGate(seed uint64) (*Cluster, error) {
	cfg := DefaultClusterConfig(3, seed)
	cfg.UseKV = true
	c, err := NewCluster(cfg)
	if err != nil {
		return nil, err
	}
	if c.WaitLeader(80) == 0 {
		return c, fmt.Errorf("no leader")
	}
	lid := c.Leader()
	var others []raft.NodeID
	for i := 1; i <= 3; i++ {
		id := raft.NodeID(i)
		if id != lid {
			others = append(others, id)
		}
	}
	c.Partition([]raft.NodeID{lid}, others, false)
	c.ProposeTo(lid, "k", "minority")
	c.RunUntil(c.Now() + 40)
	return c, nil
}

func RunSnapshotGate(seed uint64) (*Cluster, error) {
	cfg := DefaultClusterConfig(3, seed)
	cfg.UseKV = true
	cfg.SnapshotThreshold = 3
	c, err := NewCluster(cfg)
	if err != nil {
		return nil, err
	}
	if c.WaitLeader(80) == raft.None {
		return c, fmt.Errorf("no leader")
	}
	lag := raft.NodeID(3)
	if c.Leader() == lag {
		lag = 2
	}
	c.Crash(lag)
	for i := 0; i < 8; i++ {
		c.Propose("k", fmt.Sprintf("v%d", i))
		c.RunUntil(c.Now() + 12)
	}
	if !c.Compacted() {
		return c, fmt.Errorf("no snapshot after writes")
	}
	if err := c.Restart(lag); err != nil {
		return c, err
	}
	c.RunUntil(c.Now() + 80)
	env := c.env(lag)
	if env == nil || env.n.Applied() < 4 {
		applied := raft.Index(0)
		if env != nil {
			applied = env.n.Applied()
		}
		return c, fmt.Errorf("lagging node not caught up applied=%d", applied)
	}
	return c, nil
}

func RunMembershipGate(seed uint64) (*Cluster, error) {
	cfg := DefaultClusterConfig(3, seed)
	cfg.UseKV = true
	c, err := NewCluster(cfg)
	if err != nil {
		return nil, err
	}
	if c.WaitLeader(80) == raft.None {
		return c, fmt.Errorf("no leader")
	}
	c.Propose("k", "before")
	c.RunUntil(c.Now() + 20)
	if err := c.AddNode(4); err != nil {
		return c, err
	}
	c.ChangeConfig([]raft.NodeID{1, 2, 3, 4})
	c.RunUntil(c.Now() + 80)
	if !c.WaitNotJoint(c.Now() + 80) {
		return c, fmt.Errorf("still in joint after add")
	}
	voters := c.ConfVoters()
	if len(voters) != 4 {
		return c, fmt.Errorf("want 4 voters got %v", voters)
	}
	c.Propose("k", "after-add")
	c.RunUntil(c.Now() + 30)
	c.ChangeConfig([]raft.NodeID{1, 2, 3})
	c.RunUntil(c.Now() + 80)
	if !c.WaitNotJoint(c.Now() + 80) {
		return c, fmt.Errorf("still in joint after remove")
	}
	voters = c.ConfVoters()
	if len(voters) != 3 {
		return c, fmt.Errorf("want 3 voters got %v", voters)
	}
	c.Propose("k", "after-remove")
	c.RunUntil(c.Now() + 40)
	return c, nil
}

func RunReadIndexGate(seed uint64, lease raft.Instant) (check.Verdict, []raft.Violation, error) {
	cfg := DefaultClusterConfig(3, seed)
	cfg.UseKV = true
	cfg.ReadLease = lease
	c, err := NewCluster(cfg)
	if err != nil {
		return check.Verdict{}, nil, err
	}
	if c.WaitLeader(80) == raft.None {
		return check.Verdict{Linearizable: false, Detail: "no leader"}, c.mon.Violations(), nil
	}
	for i := 0; i < 6; i++ {
		c.Propose("k", fmt.Sprintf("v%d", i))
		c.RunUntil(c.Now() + 15)
		c.Get("k")
		c.RunUntil(c.Now() + 15)
		c.Get("missing")
		c.RunUntil(c.Now() + 10)
	}
	c.RunUntil(c.Now() + 40)
	reads := 0
	for _, op := range c.History().Ops {
		if op.Kind == check.OpGet && op.Ok {
			reads++
		}
	}
	if reads == 0 {
		return check.Verdict{Linearizable: false, Detail: "no successful reads"}, c.mon.Violations(), nil
	}
	return check.CrossCheck(c.History()), c.mon.Violations(), nil
}

func ApplyRandomFaults(c *Cluster, rnd *Stream, until raft.Instant) {
	n := c.cfg.N
	pick := func() raft.NodeID { return raft.NodeID(rnd.Intn(n) + 1) }
	kind := rnd.Intn(7)
	switch kind {
	case 0:
		c.net.Matrix().Add(Rule{Kind: FDrop, P: 0.1, Until: until})
	case 1:
		c.net.Matrix().Add(Rule{Kind: FDelayUniform, Delay: 1, Delay2: 5, Until: until})
	case 2:
		a := pick()
		var rest []raft.NodeID
		for i := 1; i <= n; i++ {
			if raft.NodeID(i) != a {
				rest = append(rest, raft.NodeID(i))
			}
		}
		c.Partition([]raft.NodeID{a}, rest, rnd.Intn(2) == 0)
	case 3:
		c.Crash(pick())
	case 4:
		c.net.Matrix().Add(Rule{Kind: FDup, P: 0.05, Until: until})
	case 5:
		c.net.Matrix().Add(Rule{Kind: FReorder, P: 0.1, Until: until})
	default:
		c.net.Matrix().Add(Rule{Kind: FDelayConst, Delay: 3, Until: until})
	}
}
