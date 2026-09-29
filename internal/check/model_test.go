package check

import "testing"

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

func TestWingGongDetectsBadGet(t *testing.T) {
	h := History{Ops: []Op{
		{Kind: OpPut, Key: "a", Value: "1", CallAt: 1, ReturnAt: 2, Ok: true, Seq: 1},
		{Kind: OpGet, Key: "a", Result: "2", CallAt: 3, ReturnAt: 4, Ok: true, Seq: 2},
	}}
	v := WingGongKV(h)
	if v.Linearizable {
		t.Fatal("expected non-linearizable get")
	}
}

func TestWingGongOkPutGet(t *testing.T) {
	h := History{Ops: []Op{
		{Kind: OpPut, Key: "a", Value: "1", CallAt: 1, ReturnAt: 2, Ok: true, Seq: 1},
		{Kind: OpGet, Key: "a", Result: "1", CallAt: 3, ReturnAt: 4, Ok: true, Seq: 2},
	}}
	v := CrossCheck(h)
	if !v.Linearizable {
		t.Fatalf("%+v", v)
	}
}
