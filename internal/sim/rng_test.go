package sim

import "testing"

func TestDeriveIndependent(t *testing.T) {
	a := Derive(1, 100)
	b := Derive(1, 101)
	same := 0
	for i := 0; i < 32; i++ {
		if a.Uint64() == b.Uint64() {
			same++
		}
	}
	if same == 32 {
		t.Fatal("derived streams identical")
	}
}

func TestStreamDeterministic(t *testing.T) {
	a := NewStream(42)
	b := NewStream(42)
	for i := 0; i < 16; i++ {
		if a.Uint64() != b.Uint64() {
			t.Fatal("same seed diverged")
		}
	}
}
