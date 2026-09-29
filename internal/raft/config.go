package raft

type Config struct {
	ID                NodeID
	Peers             []NodeID
	ElectionTickMin   Instant
	ElectionTickMax   Instant
	HeartbeatTick     Instant
	SnapshotThreshold Index
	ReadLease         Instant
	Join              bool
	Bug               Bug
	Observer          Observer
}

func (c Config) Validate() error {
	if c.ID == None {
		return errf("config: id required")
	}
	if c.ElectionTickMin <= 0 {
		return errf("config: ElectionTickMin must be > 0")
	}
	if c.ElectionTickMax < c.ElectionTickMin {
		return errf("config: ElectionTickMax < ElectionTickMin")
	}
	if c.HeartbeatTick <= 0 {
		return errf("config: HeartbeatTick must be > 0")
	}
	seen := map[NodeID]struct{}{c.ID: {}}
	for _, p := range c.Peers {
		if p == None || p == c.ID {
			return errf("config: invalid peer %d", p)
		}
		if _, ok := seen[p]; ok {
			return errf("config: duplicate peer %d", p)
		}
		seen[p] = struct{}{}
	}
	return nil
}

func (c Config) Quorum() int {
	return (len(c.Peers)+1)/2 + 1
}

func (c Config) observer() Observer {
	if c.Observer == nil {
		return NopObserver{}
	}
	return c.Observer
}
