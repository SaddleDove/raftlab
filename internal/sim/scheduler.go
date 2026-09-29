package sim

import (
	"container/heap"
	"fmt"

	"raftlab/internal/raft"
)

type Kind int

const (
	KindTimer Kind = iota + 1
	KindDeliver
	KindCrash
	KindRestart
	KindClient
	KindScript
)

func (k Kind) String() string {
	switch k {
	case KindTimer:
		return "timer"
	case KindDeliver:
		return "deliver"
	case KindCrash:
		return "crash"
	case KindRestart:
		return "restart"
	case KindClient:
		return "client"
	case KindScript:
		return "script"
	default:
		return "unknown"
	}
}

type EventFn func()

type item struct {
	at    raft.Instant
	seq   uint64
	kind  Kind
	fn    EventFn
	token uint64
	dead  bool
}

type pq []*item

func (p pq) Len() int { return len(p) }
func (p pq) Less(i, j int) bool {
	if p[i].at != p[j].at {
		return p[i].at < p[j].at
	}
	if p[i].kind != p[j].kind {
		return p[i].kind < p[j].kind
	}
	return p[i].seq < p[j].seq
}
func (p pq) Swap(i, j int)       { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x interface{}) { *p = append(*p, x.(*item)) }
func (p *pq) Pop() interface{} {
	old := *p
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*p = old[:n-1]
	return x
}

type Scheduler struct {
	clk   *Clock
	q     pq
	seq   uint64
	tok   uint64
	log   *Log
	steps uint64
}

func NewScheduler(clk *Clock, log *Log) *Scheduler {
	s := &Scheduler{clk: clk, log: log}
	heap.Init(&s.q)
	return s
}

func (s *Scheduler) Now() raft.Instant { return s.clk.Now() }

func (s *Scheduler) Schedule(at raft.Instant, kind Kind, fn EventFn) uint64 {
	s.tok++
	s.seq++
	it := &item{at: at, seq: s.seq, kind: kind, fn: fn, token: s.tok}
	heap.Push(&s.q, it)
	return s.tok
}

func (s *Scheduler) Cancel(token uint64) {
	for _, it := range s.q {
		if it.token == token {
			it.dead = true
			return
		}
	}
}

func (s *Scheduler) Empty() bool {
	for _, it := range s.q {
		if !it.dead {
			return false
		}
	}
	return true
}

func (s *Scheduler) Step() bool {
	for s.q.Len() > 0 {
		it := heap.Pop(&s.q).(*item)
		if it.dead {
			continue
		}
		s.clk.Advance(it.at)
		s.steps++
		if s.log != nil {
			s.log.Append(s.clk.Now(), EvAdvance, fmt.Sprintf("kind=%s seq=%d", it.kind, it.seq))
		}
		it.fn()
		return true
	}
	return false
}

func (s *Scheduler) RunUntil(until raft.Instant) {
	for s.q.Len() > 0 {
		next := s.peekLive()
		if next == nil || next.at > until {
			s.clk.Advance(until)
			return
		}
		s.Step()
	}
	s.clk.Advance(until)
}

func (s *Scheduler) peekLive() *item {
	for s.q.Len() > 0 {
		if s.q[0].dead {
			heap.Pop(&s.q)
			continue
		}
		return s.q[0]
	}
	return nil
}

func (s *Scheduler) Steps() uint64 { return s.steps }
