package sim

import (
	"testing"

	"raftlab/internal/check"
)

func TestQuietLinearizable(t *testing.T) {
	v, viol, err := RunLinearizableQuiet(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(viol) > 0 {
		t.Fatal(viol)
	}
	if !v.Linearizable {
		t.Fatal(v)
	}
	_ = check.CrossCheck
}

func TestPartitionMinorityNoAck(t *testing.T) {
	c, err := RunPartitionGate(7)
	if err != nil {
		t.Fatal(err)
	}
	if c.MinorityAcks() != 0 {
		t.Fatalf("minority acks %d", c.MinorityAcks())
	}
}

func TestSnapshotCatchUp(t *testing.T) {
	c, err := RunSnapshotGate(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Monitor().Violations()) > 0 {
		t.Fatal(c.Monitor().Violations())
	}
}

func TestMembershipAddRemove(t *testing.T) {
	c, err := RunMembershipGate(5)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Monitor().Violations()) > 0 {
		t.Fatal(c.Monitor().Violations())
	}
	v := check.CrossCheck(c.History())
	if !v.Linearizable {
		t.Fatal(v)
	}
}

func TestReadIndexLinearizable(t *testing.T) {
	v, viol, err := RunReadIndexGate(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(viol) > 0 {
		t.Fatal(viol)
	}
	if !v.Linearizable {
		t.Fatal(v)
	}
}

func TestReadLeaseLinearizable(t *testing.T) {
	v, viol, err := RunReadIndexGate(2, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(viol) > 0 {
		t.Fatal(viol)
	}
	if !v.Linearizable {
		t.Fatal(v)
	}
}
