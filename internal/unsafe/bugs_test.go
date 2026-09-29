package unsafe_test

import (
	"testing"

	"raftlab/internal/check"
	"raftlab/internal/raft"
	"raftlab/internal/sim"
	"raftlab/internal/unsafe"
)

func TestAllVariantsNamed(t *testing.T) {
	if len(unsafe.Names()) != 8 {
		t.Fatalf("want 8 got %d", len(unsafe.Names()))
	}
}

func TestVariantsCaught(t *testing.T) {
	for _, b := range raft.AllBugs() {
		r := sim.CatchBug(b)
		if !r.Caught {
			t.Errorf("missed %s", b)
			continue
		}
		t.Logf("caught %s seed=%d %s", b, r.Seed, r.Violation)
	}
}

func TestLinearizabilityIndependent(t *testing.T) {
	h := check.History{Ops: []check.Op{
		{Kind: check.OpPut, Key: "k", Value: "1", CallAt: 1, ReturnAt: 2, Ok: true, Seq: 1},
	}}
	if !check.CrossCheck(h).Linearizable {
		t.Fatal("put should be lin")
	}
}
