package transport

import (
	"testing"
	"time"

	"raftlab/internal/raft"
)

func TestTCPLoopback(t *testing.T) {
	a, err := Listen(1, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	got := make(chan raft.Message, 1)
	a.OnRecv(func(m raft.Message) { got <- m })

	b, err := Listen(2, "127.0.0.1:0", map[raft.NodeID]string{1: a.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.Send(raft.Message{Type: raft.MsgRequestVote, From: 2, To: 1, Term: 3})
	select {
	case m := <-got:
		if m.From != 2 || m.Term != 3 {
			t.Fatalf("%+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}
