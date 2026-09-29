package check

import (
	"fmt"

	"raftlab/internal/raft"
	"raftlab/internal/storage"
)

type stubTO struct{ elec, hb raft.Instant }

func (s *stubTO) ResetElection(d raft.Instant)  { s.elec = d }
func (s *stubTO) ResetHeartbeat(d raft.Instant) { s.hb = d }
func (s *stubTO) StopHeartbeat()                {}

type stubClk struct{ n raft.Instant }

func (s *stubClk) Now() raft.Instant { return s.n }

type stubRnd struct{ i int }

func (s *stubRnd) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	s.i++
	return s.i % n
}

type bufTr struct{ ch chan raft.Message }

func (b *bufTr) Send(m raft.Message) {
	b.ch <- m.Clone()
}

func Exhaustive3(maxSteps int) error {
	if maxSteps <= 0 {
		maxSteps = 6
	}
	type st struct {
		ticks [3]int
		depth int
	}
	queue := []st{{}}
	seen := 0
	for len(queue) > 0 && seen < 2000 {
		cur := queue[0]
		queue = queue[1:]
		seen++
		if err := runInterleaving(cur.ticks, cur.depth); err != nil {
			return err
		}
		if cur.depth >= maxSteps {
			continue
		}
		for i := 0; i < 3; i++ {
			n := cur
			n.ticks[i]++
			n.depth++
			queue = append(queue, n)
		}
	}
	return nil
}

func runInterleaving(ticks [3]int, depth int) error {
	ch := make(chan raft.Message, 64)
	tr := &bufTr{ch: ch}
	clk := &stubClk{}
	nodes := make([]*raft.Node, 3)
	for i := 0; i < 3; i++ {
		id := raft.NodeID(i + 1)
		peers := []raft.NodeID{}
		for j := 1; j <= 3; j++ {
			if raft.NodeID(j) != id {
				peers = append(peers, raft.NodeID(j))
			}
		}
		n, err := raft.NewNode(raft.Config{
			ID: id, Peers: peers,
			ElectionTickMin: 1, ElectionTickMax: 1, HeartbeatTick: 1,
		}, tr, clk, storage.NewMemory(), raft.NopSM{}, &stubTO{}, &stubRnd{i: i + 1})
		if err != nil {
			return err
		}
		nodes[i] = n
	}
	mon := raft.NewMonitor(clk, raft.NopObserver{})
	deliver := func() {
		for {
			select {
			case m := <-ch:
				to := int(m.To) - 1
				if to >= 0 && to < 3 {
					nodes[to].Step(m)
					mon.OnSend(m)
				}
			default:
				return
			}
		}
	}
	order := []int{}
	for i, t := range ticks {
		for k := 0; k < t; k++ {
			order = append(order, i)
		}
	}
	if depth == 0 {
		order = []int{0, 1, 2}
	}
	for _, i := range order {
		nodes[i].Step(raft.Message{Type: raft.MsgTickElection, From: raft.NodeID(i + 1), To: raft.NodeID(i + 1)})
		mon.OnState(nodes[i].ID(), nodes[i].Role(), nodes[i].HardState(), nodes[i].Applied())
		deliver()
		leaders := map[raft.Term]int{}
		for _, n := range nodes {
			if n.Role() == raft.Leader {
				leaders[n.Term()]++
				if leaders[n.Term()] > 1 {
					return fmt.Errorf("I1 violated term %d", n.Term())
				}
			}
		}
	}
	_ = depth
	return nil
}
