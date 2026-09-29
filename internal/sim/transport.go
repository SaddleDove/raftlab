package sim

import (
	"fmt"

	"raftlab/internal/raft"
)

type inbox func(raft.Message)

type Transport struct {
	id      raft.NodeID
	net     *Network
	deliver inbox
}

type Network struct {
	sched   *Scheduler
	matrix  *Matrix
	rng     *Stream
	log     *Log
	nodes   map[raft.NodeID]*Transport
	baseLat raft.Instant
}

func NewNetwork(sched *Scheduler, matrix *Matrix, rng *Stream, log *Log, baseLat raft.Instant) *Network {
	if matrix == nil {
		matrix = NewMatrix()
	}
	if baseLat <= 0 {
		baseLat = 1
	}
	return &Network{
		sched:   sched,
		matrix:  matrix,
		rng:     rng,
		log:     log,
		nodes:   map[raft.NodeID]*Transport{},
		baseLat: baseLat,
	}
}

func (n *Network) Attach(id raft.NodeID, deliver inbox) *Transport {
	t := &Transport{id: id, net: n, deliver: deliver}
	n.nodes[id] = t
	return t
}

func (n *Network) Detach(id raft.NodeID) { delete(n.nodes, id) }

func (t *Transport) Send(msg raft.Message) {
	t.net.inject(msg)
}

func (n *Network) inject(msg raft.Message) {
	now := n.sched.Now()
	if n.matrix.Crashed(now, msg.From) || n.matrix.Crashed(now, msg.To) {
		n.note(EvDrop, now, msg, "crashed")
		return
	}
	if n.matrix.Partitioned(now, msg.From, msg.To) {
		n.note(EvDrop, now, msg, "partition")
		return
	}
	if n.rng.Float64() < n.matrix.DropP(now, msg.From, msg.To) {
		n.note(EvDrop, now, msg, "drop")
		return
	}
	d := n.baseLat + n.matrix.Delay(now, msg.From, msg.To, n.rng)
	if d < 1 {
		d = 1
	}
	if p := n.matrix.ReorderP(now, msg.From, msg.To); p > 0 && n.rng.Float64() < p {
		// Hold the message back so a later send on the same edge overtakes it.
		extra := 1 + n.rng.Intn(3)
		n.note(EvReorder, now, msg, fmt.Sprintf("hold=%d", extra))
		d += raft.Instant(extra)
	}
	n.enqueue(now+d, msg)
	if n.rng.Float64() < n.matrix.DupP(now, msg.From, msg.To) {
		n.enqueue(now+d+1, msg.Clone())
		n.note(EvDup, now, msg, "dup")
	}
}

func (n *Network) enqueue(at raft.Instant, msg raft.Message) {
	cloned := msg.Clone()
	n.sched.Schedule(at, KindDeliver, func() {
		now := n.sched.Now()
		dst, ok := n.nodes[cloned.To]
		if !ok || dst.deliver == nil {
			n.note(EvDrop, now, cloned, "no-dst")
			return
		}
		if n.matrix.Crashed(now, cloned.To) {
			n.note(EvDrop, now, cloned, "dst-crashed")
			return
		}
		n.note(EvDeliver, now, cloned, cloned.Canonical())
		dst.deliver(cloned)
	})
}

func (n *Network) note(kind EventKind, at raft.Instant, msg raft.Message, extra string) {
	if n.log == nil {
		return
	}
	n.log.Append(at, kind, fmt.Sprintf("%s %s", extra, msg.Canonical()))
}

func (n *Network) Matrix() *Matrix { return n.matrix }
