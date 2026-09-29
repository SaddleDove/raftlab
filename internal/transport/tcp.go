package transport

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"

	"raftlab/internal/raft"
)

func init() {
	gob.Register(raft.Message{})
	gob.Register(raft.LogEntry{})
}

type TCP struct {
	id      raft.NodeID
	ln      net.Listener
	peers   map[raft.NodeID]string
	inbox   chan raft.Message
	mu      sync.Mutex
	conns   map[raft.NodeID]net.Conn
	handler func(raft.Message)
	closed  bool
}

func Listen(id raft.NodeID, addr string, peers map[raft.NodeID]string) (*TCP, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	t := &TCP{
		id:    id,
		ln:    ln,
		peers: peers,
		inbox: make(chan raft.Message, 256),
		conns: map[raft.NodeID]net.Conn{},
	}
	go t.accept()
	return t, nil
}

func (t *TCP) Addr() string {
	return t.ln.Addr().String()
}

func (t *TCP) OnRecv(fn func(raft.Message)) { t.handler = fn }

func (t *TCP) Send(msg raft.Message) {
	addr, ok := t.peers[msg.To]
	if !ok {
		return
	}
	t.mu.Lock()
	c, ok := t.conns[msg.To]
	t.mu.Unlock()
	if !ok {
		nc, err := net.Dial("tcp", addr)
		if err != nil {
			return
		}
		t.mu.Lock()
		t.conns[msg.To] = nc
		t.mu.Unlock()
		c = nc
		go t.readLoop(nc)
	}
	enc := gob.NewEncoder(c)
	_ = enc.Encode(msg)
}

func (t *TCP) accept() {
	for {
		c, err := t.ln.Accept()
		if err != nil {
			return
		}
		go t.readLoop(c)
	}
}

func (t *TCP) readLoop(c net.Conn) {
	dec := gob.NewDecoder(c)
	for {
		var m raft.Message
		if err := dec.Decode(&m); err != nil {
			if err != io.EOF {
				_ = c.Close()
			}
			return
		}
		if t.handler != nil {
			t.handler(m)
		} else {
			select {
			case t.inbox <- m:
			default:
			}
		}
	}
}

func (t *TCP) Recv() <-chan raft.Message { return t.inbox }

func (t *TCP) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	for _, c := range t.conns {
		_ = c.Close()
	}
	return t.ln.Close()
}

func LocalPeers(n int, basePort int) map[raft.NodeID]string {
	out := map[raft.NodeID]string{}
	for i := 1; i <= n; i++ {
		out[raft.NodeID(i)] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}
	return out
}
