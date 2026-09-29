package raft

import (
	"errors"
	"fmt"
)

var (
	ErrCompacted   = errors.New("raft: log compacted")
	ErrUnavailable = errors.New("raft: log unavailable")
	ErrTornRecord  = errors.New("raft: torn wal record")
	ErrCorruptWAL  = errors.New("raft: corrupt wal")
	ErrNotLeader   = errors.New("raft: not leader")
	ErrDropped     = errors.New("raft: dropped")
	ErrInvariant   = errors.New("raft: invariant violated")
)

func errf(format string, args ...interface{}) error {
	return fmt.Errorf(format, args...)
}
