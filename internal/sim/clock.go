package sim

import "raftlab/internal/raft"

type Clock struct {
	now raft.Instant
}

func NewClock() *Clock { return &Clock{} }

func (c *Clock) Now() raft.Instant { return c.now }

func (c *Clock) Advance(to raft.Instant) {
	if to > c.now {
		c.now = to
	}
}
