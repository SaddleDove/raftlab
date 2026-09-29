package check

import (
	"testing"

	"raftlab/internal/raft"
)

func TestExhaustive3I1(t *testing.T) {
	if err := Exhaustive3(4); err != nil {
		t.Fatal(err)
	}
}

func TestCrossCheckEmpty(t *testing.T) {
	v := CrossCheck(History{})
	if !v.Linearizable {
		t.Fatal(v)
	}
}

func TestOrderDetectsBadGet(t *testing.T) {
	h := History{Ops: []Op{
		{Kind: OpPut, Key: "a", Value: "1", CallAt: 1, ReturnAt: 2, Ok: true, Seq: 1},
		{Kind: OpGet, Key: "a", Result: "2", CallAt: 3, ReturnAt: 4, Ok: true, Seq: 2},
	}}
	v := OrderKV(h)
	if v.Linearizable {
		t.Fatal("expected non-linearizable get")
	}
}

func TestBitmaskDetectsBadGet(t *testing.T) {
	h := History{Ops: []Op{
		{Kind: OpPut, Key: "a", Value: "1", CallAt: 1, ReturnAt: 2, Ok: true, Seq: 1},
		{Kind: OpGet, Key: "a", Result: "2", CallAt: 3, ReturnAt: 4, Ok: true, Seq: 2},
	}}
	if BitmaskKV(h).Linearizable {
		t.Fatal("bitmask accepted a non-linearizable get")
	}
}

func TestBitmaskRefusesLongHistory(t *testing.T) {
	h := History{}
	for i := 0; i < BitmaskLimit+1; i++ {
		h.Ops = append(h.Ops, Op{Kind: OpPut, Key: "k", Value: "v", CallAt: raft.Instant(i), ReturnAt: raft.Instant(i + 1), Ok: true, Seq: uint64(i)})
	}
	v := BitmaskKV(h)
	if v.Linearizable {
		t.Fatal("bitmask must not decide a history past its limit")
	}
	if v.Checker != "bitmask" {
		t.Fatalf("checker %q", v.Checker)
	}
}

func TestCrossCheckReportsWhichCheckersRan(t *testing.T) {
	small := History{Ops: []Op{
		{Kind: OpPut, Key: "a", Value: "1", CallAt: 1, ReturnAt: 2, Ok: true, Seq: 1},
	}}
	v := CrossCheckKV(small)
	if len(v.Checkers) != 2 {
		t.Fatalf("small history should be decided twice, got %v", v.Checkers)
	}
	big := History{}
	for i := 0; i < BitmaskLimit+1; i++ {
		big.Ops = append(big.Ops, Op{Kind: OpPut, Key: "k", Value: "v", CallAt: raft.Instant(i), ReturnAt: raft.Instant(i + 1), Ok: true, Seq: uint64(i)})
	}
	v = CrossCheckKV(big)
	if len(v.Checkers) != 1 || v.Checker != "order" {
		t.Fatalf("long history must be decided once and say so, got %q %v", v.Checker, v.Checkers)
	}
	if v.Detail == "" {
		t.Fatal("long history verdict must explain the skipped checker")
	}
}

func TestOrderOkPutGet(t *testing.T) {
	h := History{Ops: []Op{
		{Kind: OpPut, Key: "a", Value: "1", CallAt: 1, ReturnAt: 2, Ok: true, Seq: 1},
		{Kind: OpGet, Key: "a", Result: "1", CallAt: 3, ReturnAt: 4, Ok: true, Seq: 2},
	}}
	v := CrossCheck(h)
	if !v.Linearizable {
		t.Fatalf("%+v", v)
	}
}
