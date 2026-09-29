package sim

import "testing"

// The docs have to state the wired set, not the declared set: this test is the
// reason the two can no longer drift apart silently.
func TestFaultWiringIsExplicit(t *testing.T) {
	declared := DeclaredFaults()
	wired := WiredFaults()
	unwired := UnwiredFaults()

	if len(declared) != 16 {
		t.Fatalf("declared %d fault kinds, want 16", len(declared))
	}
	if len(wired) != 10 {
		t.Fatalf("wired %d fault kinds, want 10", len(wired))
	}
	if len(declared) != len(wired)+len(unwired) {
		t.Fatal("declared must split into wired and unwired")
	}
	inWired := map[FaultKind]bool{}
	for _, k := range wired {
		inWired[k] = true
		if k.String() == "unknown" {
			t.Fatalf("wired kind %d has no name", k)
		}
	}
	for _, k := range unwired {
		if inWired[k] {
			t.Fatalf("%s appears in both lists", k)
		}
	}
	if len(wired) == len(declared) {
		t.Fatal("expected unwired kinds; update the docs if they are all implemented now")
	}
}

// A reorder rule must actually change when a message arrives.
func TestReorderRuleDelaysDelivery(t *testing.T) {
	matrix := NewMatrix()
	if p := matrix.ReorderP(0, 1, 2); p != 0 {
		t.Fatalf("idle matrix reorder probability %v", p)
	}
	matrix.Add(Rule{Kind: FReorder, P: 1, Until: 100})
	if p := matrix.ReorderP(0, 1, 2); p != 1 {
		t.Fatalf("reorder probability %v", p)
	}
	if p := matrix.ReorderP(101, 1, 2); p != 0 {
		t.Fatalf("expired rule still active: %v", p)
	}
}
