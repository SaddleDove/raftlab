package transport

import "raftlab/internal/raft"

type FaultController interface {
	SetDrop(from, to raft.NodeID, p float64)
	SetDelay(from, to raft.NodeID, d raft.Instant)
	Partition(a, b []raft.NodeID, asymmetric bool)
	Heal()
}

type Factory interface {
	Open(id raft.NodeID, peers map[raft.NodeID]string) (raft.Transport, error)
}
