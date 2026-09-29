package sim

import (
	"testing"

	"raftlab/internal/raft"
)

func TestLeaderConverges(t *testing.T) {
	cfg := DefaultClusterConfig(3, 1)
	cfg.RunUntil = 80
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := c.Run()
	if r.LeaderID == 0 {
		t.Fatal("no leader")
	}
	if r.FirstLeaderAt == 0 || r.FirstLeaderAt > 20*20 {
		t.Fatalf("slow converge %d", r.FirstLeaderAt)
	}
	for _, v := range r.Violations {
		if v.Code == raft.I1ElectionSafety || v.Code == raft.I2TermMonotonic || v.Code == raft.I3VotePersisted {
			t.Fatal(v)
		}
	}
}

func TestElectionGateSmall(t *testing.T) {
	rep := RunElectionGate(20, 3, 200)
	if rep.Fail > 0 {
		t.Fatal(rep.Detail)
	}
}
