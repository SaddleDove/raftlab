package sim

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"raftlab/internal/raft"
)

func ShrinkUntil(cfg ClusterConfig, failUntil raft.Instant) (raft.Instant, error) {
	lo, hi := raft.Instant(1), failUntil
	best := failUntil
	for lo <= hi {
		mid := (lo + hi) / 2
		c := cfg
		c.RunUntil = mid
		cl, err := NewCluster(c)
		if err != nil {
			return 0, err
		}
		r := cl.Run()
		if len(r.Violations) > 0 {
			best = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return best, nil
}

func WriteFailure(dir string, seed uint64, until raft.Instant, hash, detail string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	name := filepath.Join(dir, fmt.Sprintf("seed-%d.txt", seed))
	body := strings.Join([]string{
		fmt.Sprintf("seed=%d", seed),
		fmt.Sprintf("until=%d", until),
		fmt.Sprintf("hash=%s", hash),
		detail,
	}, "\n")
	return os.WriteFile(name, []byte(body+"\n"), 0o644)
}
