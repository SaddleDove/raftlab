package sim

import "raftlab/internal/raft"

type Timeouts struct {
	id    raft.NodeID
	sched *Scheduler
	clk   *Clock
	step  func(raft.Message)
	elec  uint64
	hb    uint64
}

func NewTimeouts(id raft.NodeID, sched *Scheduler, clk *Clock, step func(raft.Message)) *Timeouts {
	return &Timeouts{id: id, sched: sched, clk: clk, step: step}
}

func (t *Timeouts) ResetElection(d raft.Instant) {
	if t.elec != 0 {
		t.sched.Cancel(t.elec)
	}
	at := t.clk.Now() + d
	id := t.id
	t.elec = t.sched.Schedule(at, KindTimer, func() {
		t.step(raft.Message{Type: raft.MsgTickElection, To: id, From: id})
	})
}

func (t *Timeouts) ResetHeartbeat(d raft.Instant) {
	if t.hb != 0 {
		t.sched.Cancel(t.hb)
	}
	at := t.clk.Now() + d
	id := t.id
	t.hb = t.sched.Schedule(at, KindTimer, func() {
		t.step(raft.Message{Type: raft.MsgTickHeartbeat, To: id, From: id})
	})
}

func (t *Timeouts) StopHeartbeat() {
	if t.hb != 0 {
		t.sched.Cancel(t.hb)
		t.hb = 0
	}
}

func (t *Timeouts) StopElection() {
	if t.elec != 0 {
		t.sched.Cancel(t.elec)
		t.elec = 0
	}
}
