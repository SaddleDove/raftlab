package sim

import "raftlab/internal/raft"

type FaultKind int

const (
	FDelayConst FaultKind = iota + 1
	FDelayUniform
	FDelayHeavy
	FDrop
	FDup
	FReorder
	FBitFlip
	FPartitionSym
	FPartitionAsym
	FCrashStop
	FCrashRestart
	FTornWrite
	FSlowDisk
	FElectionStorm
	FClockSkew
	FStaleRejoin
)

func (k FaultKind) String() string {
	switch k {
	case FDelayConst:
		return "delay_const"
	case FDelayUniform:
		return "delay_uniform"
	case FDelayHeavy:
		return "delay_heavy"
	case FDrop:
		return "drop"
	case FDup:
		return "dup"
	case FReorder:
		return "reorder"
	case FBitFlip:
		return "bitflip"
	case FPartitionSym:
		return "partition_sym"
	case FPartitionAsym:
		return "partition_asym"
	case FCrashStop:
		return "crash_stop"
	case FCrashRestart:
		return "crash_restart"
	case FTornWrite:
		return "torn_write"
	case FSlowDisk:
		return "slow_disk"
	case FElectionStorm:
		return "election_storm"
	case FClockSkew:
		return "clock_skew"
	case FStaleRejoin:
		return "stale_rejoin"
	default:
		return "unknown"
	}
}

type Rule struct {
	Kind   FaultKind
	From   raft.NodeID
	To     raft.NodeID
	P      float64
	Delay  raft.Instant
	Delay2 raft.Instant
	Until  raft.Instant
	GroupA []raft.NodeID
	GroupB []raft.NodeID
	Node   raft.NodeID
}

type Matrix struct {
	rules []Rule
}

func NewMatrix() *Matrix { return &Matrix{} }

func (m *Matrix) Add(r Rule) { m.rules = append(m.rules, r) }

func (m *Matrix) Clear() { m.rules = nil }

func (m *Matrix) Active(now raft.Instant) []Rule {
	out := make([]Rule, 0, len(m.rules))
	for _, r := range m.rules {
		if r.Until == 0 || now < r.Until {
			out = append(out, r)
		}
	}
	return out
}

func (m *Matrix) DropP(now raft.Instant, from, to raft.NodeID) float64 {
	p := 0.0
	for _, r := range m.Active(now) {
		if r.Kind != FDrop {
			continue
		}
		if matchEdge(r, from, to) && r.P > p {
			p = r.P
		}
	}
	return p
}

func (m *Matrix) Delay(now raft.Instant, from, to raft.NodeID, rnd *Stream) raft.Instant {
	var d raft.Instant
	for _, r := range m.Active(now) {
		if !matchEdge(r, from, to) {
			continue
		}
		switch r.Kind {
		case FDelayConst:
			if r.Delay > d {
				d = r.Delay
			}
		case FDelayUniform:
			span := int(r.Delay2 - r.Delay + 1)
			if span < 1 {
				span = 1
			}
			x := r.Delay + raft.Instant(rnd.Intn(span))
			if x > d {
				d = x
			}
		case FDelayHeavy:
			u := rnd.Float64()
			if u < 0.001 {
				u = 0.001
			}
			x := raft.Instant(float64(r.Delay) / u)
			if r.Delay2 > 0 && x > r.Delay2 {
				x = r.Delay2
			}
			if x > d {
				d = x
			}
		}
	}
	return d
}

func (m *Matrix) DupP(now raft.Instant, from, to raft.NodeID) float64 {
	p := 0.0
	for _, r := range m.Active(now) {
		if r.Kind == FDup && matchEdge(r, from, to) && r.P > p {
			p = r.P
		}
	}
	return p
}

func (m *Matrix) Partitioned(now raft.Instant, from, to raft.NodeID) bool {
	for _, r := range m.Active(now) {
		switch r.Kind {
		case FPartitionSym:
			if inGroup(r.GroupA, from) && inGroup(r.GroupB, to) {
				return true
			}
			if inGroup(r.GroupB, from) && inGroup(r.GroupA, to) {
				return true
			}
		case FPartitionAsym:
			if inGroup(r.GroupA, from) && inGroup(r.GroupB, to) {
				return true
			}
		}
	}
	return false
}

func (m *Matrix) Crashed(now raft.Instant, id raft.NodeID) bool {
	for _, r := range m.Active(now) {
		if (r.Kind == FCrashStop || r.Kind == FCrashRestart) && r.Node == id {
			return true
		}
	}
	return false
}

func matchEdge(r Rule, from, to raft.NodeID) bool {
	if r.From != 0 && r.From != from {
		return false
	}
	if r.To != 0 && r.To != to {
		return false
	}
	return true
}

func inGroup(g []raft.NodeID, id raft.NodeID) bool {
	for _, x := range g {
		if x == id {
			return true
		}
	}
	return false
}
