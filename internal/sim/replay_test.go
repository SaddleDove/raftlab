package sim

import "testing"

func TestThreeSilentNodesReplayByteIdentical(t *testing.T) {
	seeds := []uint64{1, 2, 7, 42, 99}
	for _, seed := range seeds {
		cfg := DefaultClusterConfig(3, seed)
		cfg.Silent = true
		cfg.RunUntil = 200
		h1, h2, r1, r2, err := Replay(cfg)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if h1 != h2 {
			t.Fatalf("seed %d hash mismatch\n%s\n%s\nevents %d vs %d", seed, h1, h2, r1.Events, r2.Events)
		}
		if r1.Events == 0 {
			t.Fatalf("seed %d produced empty event log", seed)
		}
	}
}

func TestReplayCanonicalStable(t *testing.T) {
	cfg := DefaultClusterConfig(3, 1)
	cfg.RunUntil = 80
	c1, err := NewCluster(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r1 := c1.Run()
	canon := c1.Log().Canonical()
	c2, err := NewCluster(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r2 := c2.Run()
	if r1.Hash != r2.Hash {
		t.Fatalf("hash %s vs %s", r1.Hash, r2.Hash)
	}
	if canon != c2.Log().Canonical() {
		t.Fatal("canonical event log mismatch")
	}
}
