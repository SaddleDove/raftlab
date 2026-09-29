package sim

import (
	"fmt"

	"raftlab/internal/check"
	"raftlab/internal/raft"
	"raftlab/internal/storage"
)

type ClusterConfig struct {
	N                 int
	Seed              uint64
	ElectionTickMin   raft.Instant
	ElectionTickMax   raft.Instant
	HeartbeatTick     raft.Instant
	BaseLatency       raft.Instant
	RunUntil          raft.Instant
	Bug               raft.Bug
	Silent            bool
	UseKV             bool
	SnapshotThreshold raft.Index
	ReadLease         raft.Instant
}

func DefaultClusterConfig(n int, seed uint64) ClusterConfig {
	return ClusterConfig{
		N:               n,
		Seed:            seed,
		ElectionTickMin: 10,
		ElectionTickMax: 20,
		HeartbeatTick:   2,
		BaseLatency:     1,
		RunUntil:        200,
		Silent:          true,
	}
}

type nodeEnv struct {
	id   raft.NodeID
	n    *raft.Node
	sto  raft.LogStore
	sm   raft.StateMachine
	to   *Timeouts
	tr   *Transport
	live bool
	step func(raft.Message)
	rcfg raft.Config
}

type Cluster struct {
	cfg        ClusterConfig
	clk        *Clock
	sched      *Scheduler
	net        *Network
	log        *Log
	mon        *raft.Monitor
	nodes      []*nodeEnv
	rng        *Stream
	hist       check.History
	clientN    uint64
	seq        uint64
	acked      int
	minOK      int
	roleOf     map[raft.NodeID]raft.Role
	maxLeaders int
}

type runObserver struct {
	c *Cluster
}

func (o runObserver) OnState(id raft.NodeID, role raft.Role, hs raft.HardState, lastApplied raft.Index) {
	o.c.mon.OnState(id, role, hs, lastApplied)
	o.c.log.Append(o.c.clk.Now(), EvState, fmt.Sprintf("id=%d role=%s term=%d vote=%d commit=%d applied=%d", id, role, hs.Term, hs.VotedFor, hs.Commit, lastApplied))
	if o.c.roleOf == nil {
		o.c.roleOf = map[raft.NodeID]raft.Role{}
	}
	o.c.roleOf[id] = role
	n := 0
	for _, r := range o.c.roleOf {
		if r == raft.Leader {
			n++
		}
	}
	if n > o.c.maxLeaders {
		o.c.maxLeaders = n
	}
}

func (o runObserver) OnSend(msg raft.Message) {
	o.c.mon.OnSend(msg)
	if msg.Type == raft.MsgClientProposeResp || msg.Type == raft.MsgClientReadResp {
		o.c.onClientResp(msg)
	}
}

func (o runObserver) OnPersist(id raft.NodeID, hs raft.HardState) {
	o.c.mon.OnPersist(id, hs)
	o.c.log.Append(o.c.clk.Now(), EvPersist, fmt.Sprintf("id=%d term=%d vote=%d commit=%d", id, hs.Term, hs.VotedFor, hs.Commit))
}

func (o runObserver) OnLog(id raft.NodeID, e raft.LogEntry) {
	o.c.mon.OnLog(id, e)
}

func (o runObserver) OnCommit(id raft.NodeID, index raft.Index, term raft.Term) {
	o.c.mon.OnCommit(id, index, term)
}

func (o runObserver) OnApply(id raft.NodeID, index raft.Index, command []byte) {
	o.c.mon.OnApply(id, index, command)
	o.c.log.Append(o.c.clk.Now(), EvApply, fmt.Sprintf("id=%d idx=%d cmd=%x", id, index, command))
}

func (o runObserver) OnViolation(v raft.Violation) {
	o.c.log.Append(o.c.clk.Now(), EvViolate, v.String())
}

func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	if cfg.N < 1 {
		return nil, fmt.Errorf("cluster: n>=1")
	}
	if cfg.ElectionTickMin == 0 {
		cfg.ElectionTickMin = 10
	}
	if cfg.ElectionTickMax == 0 {
		cfg.ElectionTickMax = 20
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = 2
	}
	clk := NewClock()
	lg := NewLog()
	sched := NewScheduler(clk, lg)
	root := NewStream(cfg.Seed)
	netRNG := Derive(cfg.Seed, 1)
	net := NewNetwork(sched, NewMatrix(), netRNG, lg, cfg.BaseLatency)
	c := &Cluster{
		cfg:    cfg,
		clk:    clk,
		sched:  sched,
		net:    net,
		log:    lg,
		rng:    root,
		roleOf: map[raft.NodeID]raft.Role{},
	}
	c.mon = raft.NewMonitor(clk, runObserver{c})
	ids := make([]raft.NodeID, cfg.N)
	for i := 0; i < cfg.N; i++ {
		ids[i] = raft.NodeID(i + 1)
	}
	for i := 0; i < cfg.N; i++ {
		id := ids[i]
		peers := make([]raft.NodeID, 0, cfg.N-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		var sm raft.StateMachine = raft.NopSM{}
		if cfg.UseKV {
			sm = raft.NewKV()
		}
		env := &nodeEnv{id: id, sto: storage.NewMemory(), sm: sm, live: true}
		var node *raft.Node
		step := func(m raft.Message) {
			if env.live && node != nil {
				node.Step(m)
			}
		}
		tr := net.Attach(id, step)
		to := NewTimeouts(id, sched, clk, step)
		rnd := Derive(cfg.Seed, uint64(100+id))
		rcfg := raft.Config{
			ID:                id,
			Peers:             peers,
			ElectionTickMin:   cfg.ElectionTickMin,
			ElectionTickMax:   cfg.ElectionTickMax,
			HeartbeatTick:     cfg.HeartbeatTick,
			SnapshotThreshold: cfg.SnapshotThreshold,
			ReadLease:         cfg.ReadLease,
			Bug:               cfg.Bug,
			Observer:          runObserver{c},
		}
		var err error
		node, err = raft.NewNode(rcfg, tr, clk, env.sto, sm, to, rnd)
		if err != nil {
			return nil, err
		}
		env.n = node
		env.to = to
		env.tr = tr
		env.step = step
		env.rcfg = rcfg
		c.nodes = append(c.nodes, env)
	}
	return c, nil
}

func (c *Cluster) Run() *Result {
	until := c.cfg.RunUntil
	if until <= 0 {
		until = 200
	}
	c.sched.RunUntil(until)
	return c.result()
}

func (c *Cluster) RunUntil(until raft.Instant) *Result {
	c.sched.RunUntil(until)
	return c.result()
}

func (c *Cluster) Log() *Log              { return c.log }
func (c *Cluster) Monitor() *raft.Monitor { return c.mon }
func (c *Cluster) Hash() string           { return c.log.Hash() }
func (c *Cluster) History() check.History { return c.hist.Clone() }
func (c *Cluster) Net() *Network          { return c.net }
func (c *Cluster) Now() raft.Instant      { return c.clk.Now() }
func (c *Cluster) Scheduler() *Scheduler  { return c.sched }

func (c *Cluster) Leader() raft.NodeID {
	for _, env := range c.nodes {
		if env.live && env.n.Role() == raft.Leader {
			return env.id
		}
	}
	return raft.None
}

func (c *Cluster) Leaders() []raft.NodeID {
	var out []raft.NodeID
	for _, env := range c.nodes {
		if env.live && env.n.Role() == raft.Leader {
			out = append(out, env.id)
		}
	}
	return out
}

func (c *Cluster) FirstLeaderAt() raft.Instant {
	for _, e := range c.log.Events() {
		if e.Kind == EvState && containsRoleLeader(e.Line) {
			return e.At
		}
	}
	return 0
}

func containsRoleLeader(s string) bool {
	return len(s) > 0 && (indexOf(s, "role=leader") >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func (c *Cluster) ElectionCount() int {
	n := 0
	for _, e := range c.log.Events() {
		if e.Kind == EvState && indexOf(e.Line, "role=candidate") >= 0 {
			n++
		}
	}
	return n
}

func (c *Cluster) Crash(id raft.NodeID) {
	env := c.env(id)
	if env == nil || !env.live {
		return
	}
	env.live = false
	env.to.StopHeartbeat()
	env.to.StopElection()
	c.net.Detach(id)
	c.log.Append(c.clk.Now(), EvCrash, fmt.Sprintf("id=%d", id))
}

func (c *Cluster) Restart(id raft.NodeID) error {
	env := c.env(id)
	if env == nil {
		return fmt.Errorf("no node %d", id)
	}
	env.live = true
	step := func(m raft.Message) {
		if env.live && env.n != nil {
			env.n.Step(m)
		}
	}
	env.step = step
	env.tr = c.net.Attach(id, step)
	env.to = NewTimeouts(id, c.sched, c.clk, step)
	n, err := raft.NewNode(env.rcfg, env.tr, c.clk, env.sto, env.sm, env.to, Derive(c.cfg.Seed, uint64(100+id)))
	if err != nil {
		return err
	}
	env.n = n
	n.RestoreFromStore()
	c.log.Append(c.clk.Now(), EvRestart, fmt.Sprintf("id=%d", id))
	return nil
}

func (c *Cluster) Partition(a, b []raft.NodeID, asym bool) {
	kind := FPartitionSym
	if asym {
		kind = FPartitionAsym
	}
	c.net.Matrix().Add(Rule{Kind: kind, GroupA: a, GroupB: b})
	c.log.Append(c.clk.Now(), EvFault, fmt.Sprintf("partition asym=%v a=%v b=%v", asym, a, b))
}

func (c *Cluster) Heal() {
	c.net.Matrix().Clear()
	c.log.Append(c.clk.Now(), EvFault, "heal")
}

func (c *Cluster) Step() bool { return c.sched.Step() }

func (c *Cluster) WaitLeader(deadline raft.Instant) raft.NodeID {
	for c.clk.Now() < deadline && c.Leader() == raft.None {
		if !c.sched.Step() {
			break
		}
	}
	return c.Leader()
}

func (c *Cluster) WaitNotJoint(deadline raft.Instant) bool {
	for c.clk.Now() < deadline {
		lid := c.Leader()
		if lid != raft.None {
			if env := c.env(lid); env != nil && !env.n.Conf().InJoint() && len(env.n.Conf().Voters) > 0 {
				return true
			}
		}
		if !c.sched.Step() {
			break
		}
	}
	return false
}

func (c *Cluster) WaitApplied(index raft.Index, deadline raft.Instant) bool {
	need := c.cfg.N/2 + 1
	for c.clk.Now() < deadline {
		n := 0
		for _, env := range c.nodes {
			if env.live && env.n.Applied() >= index {
				n++
			}
		}
		if n >= need {
			return true
		}
		if !c.sched.Step() {
			break
		}
	}
	return false
}

func (c *Cluster) ProposeTo(id raft.NodeID, key, val string) uint64 {
	c.seq++
	seq := c.seq
	cid := uint64(1000)
	cmd := raft.EncodePut(key, val)
	c.hist.Record(check.Op{
		Kind:   check.OpPut,
		Client: cid,
		Seq:    seq,
		Key:    key,
		Value:  val,
		CallAt: c.clk.Now(),
	})
	msg := raft.Message{
		Type:     raft.MsgClientPropose,
		From:     raft.NodeID(cid),
		To:       id,
		ClientID: cid,
		Seq:      seq,
		Command:  cmd,
	}
	env := c.env(id)
	if env != nil && env.live {
		env.n.Step(msg)
	}
	c.log.Append(c.clk.Now(), EvClient, fmt.Sprintf("put %s=%s seq=%d to=%d", key, val, seq, id))
	return seq
}

func (c *Cluster) Propose(key, val string) uint64 {
	c.seq++
	seq := c.seq
	cid := uint64(1000)
	cmd := raft.EncodePut(key, val)
	c.hist.Record(check.Op{
		Kind:   check.OpPut,
		Client: cid,
		Seq:    seq,
		Key:    key,
		Value:  val,
		CallAt: c.clk.Now(),
	})
	leader := c.Leader()
	if leader == raft.None {
		leader = 1
	}
	msg := raft.Message{
		Type:     raft.MsgClientPropose,
		From:     raft.NodeID(cid),
		To:       leader,
		ClientID: cid,
		Seq:      seq,
		Command:  cmd,
	}
	env := c.env(leader)
	if env != nil && env.live {
		env.n.Step(msg)
	}
	c.log.Append(c.clk.Now(), EvClient, fmt.Sprintf("put %s=%s seq=%d to=%d", key, val, seq, leader))
	return seq
}

func (c *Cluster) onClientResp(msg raft.Message) {
	for i := range c.hist.Ops {
		op := &c.hist.Ops[i]
		if op.Seq == msg.Seq && op.Client == msg.ClientID && op.ReturnAt == 0 {
			op.ReturnAt = c.clk.Now()
			op.Ok = msg.Ok && !msg.NotLeader
			op.Result = string(msg.Result)
			if op.Ok {
				c.acked++
				if c.inMinority(raft.NodeID(0)) {
					c.minOK++
					c.mon.Note(raft.Violation{
						Code:   raft.I8NoSplitBrainWrite,
						Node:   msg.From,
						Detail: fmt.Sprintf("minority ack seq=%d", msg.Seq),
					})
				}
			}
			return
		}
	}
}

func (c *Cluster) inMinority(_ raft.NodeID) bool {
	rules := c.net.Matrix().Active(c.clk.Now())
	for _, r := range rules {
		if r.Kind == FPartitionSym || r.Kind == FPartitionAsym {
			n := c.cfg.N
			if len(r.GroupA) < n/2+1 {
				leader := c.Leader()
				if leader != raft.None && inGroup(r.GroupA, leader) {
					return true
				}
			}
			if len(r.GroupB) < n/2+1 {
				leader := c.Leader()
				if leader != raft.None && inGroup(r.GroupB, leader) {
					return true
				}
			}
		}
	}
	return false
}

func (c *Cluster) MinorityAcks() int { return c.minOK }
func (c *Cluster) Acks() int         { return c.acked }
func (c *Cluster) MaxLeaders() int   { return c.maxLeaders }
func (c *Cluster) N() int            { return len(c.nodes) }

func (c *Cluster) Node(id raft.NodeID) *raft.Node {
	env := c.env(id)
	if env == nil {
		return nil
	}
	return env.n
}

func (c *Cluster) Compacted() bool {
	for _, env := range c.nodes {
		if env.n.SnapshotMeta().LastIndex > 0 {
			return true
		}
	}
	return false
}

func (c *Cluster) ConfVoters() []raft.NodeID {
	if lid := c.Leader(); lid != raft.None {
		return c.env(lid).n.Conf().Voters
	}
	if len(c.nodes) == 0 {
		return nil
	}
	return c.nodes[0].n.Conf().Voters
}

func (c *Cluster) Get(key string) uint64 {
	c.seq++
	seq := c.seq
	cid := uint64(1000)
	cmd := raft.EncodeGet(key)
	c.hist.Record(check.Op{
		Kind:   check.OpGet,
		Client: cid,
		Seq:    seq,
		Key:    key,
		CallAt: c.clk.Now(),
	})
	leader := c.Leader()
	if leader == raft.None {
		leader = 1
	}
	msg := raft.Message{
		Type:     raft.MsgClientRead,
		From:     raft.NodeID(cid),
		To:       leader,
		ClientID: cid,
		Seq:      seq,
		Command:  cmd,
	}
	env := c.env(leader)
	if env != nil && env.live {
		env.n.Step(msg)
	}
	c.log.Append(c.clk.Now(), EvClient, fmt.Sprintf("get %s seq=%d to=%d", key, seq, leader))
	return seq
}

func (c *Cluster) ProposeRaw(cmd []byte) uint64 {
	c.seq++
	seq := c.seq
	cid := uint64(1000)
	leader := c.Leader()
	if leader == raft.None {
		leader = 1
	}
	msg := raft.Message{
		Type:     raft.MsgClientPropose,
		From:     raft.NodeID(cid),
		To:       leader,
		ClientID: cid,
		Seq:      seq,
		Command:  cmd,
	}
	env := c.env(leader)
	if env != nil && env.live {
		env.n.Step(msg)
	}
	c.log.Append(c.clk.Now(), EvClient, fmt.Sprintf("raw seq=%d to=%d cmd=%s", seq, leader, cmd))
	return seq
}

func (c *Cluster) ChangeConfig(voters []raft.NodeID) uint64 {
	return c.ProposeRaw(raft.EncodeJoint(voters))
}

func (c *Cluster) AddNode(id raft.NodeID) error {
	if c.env(id) != nil {
		return fmt.Errorf("node %d exists", id)
	}
	var sm raft.StateMachine = raft.NopSM{}
	if c.cfg.UseKV {
		sm = raft.NewKV()
	}
	env := &nodeEnv{id: id, sto: storage.NewMemory(), sm: sm, live: true}
	var node *raft.Node
	step := func(m raft.Message) {
		if env.live && node != nil {
			node.Step(m)
		}
	}
	tr := c.net.Attach(id, step)
	to := NewTimeouts(id, c.sched, c.clk, step)
	rnd := Derive(c.cfg.Seed, uint64(100+id))
	peers := make([]raft.NodeID, 0, len(c.nodes))
	for _, e := range c.nodes {
		peers = append(peers, e.id)
	}
	rcfg := raft.Config{
		ID:                id,
		Peers:             peers,
		ElectionTickMin:   c.cfg.ElectionTickMin,
		ElectionTickMax:   c.cfg.ElectionTickMax,
		HeartbeatTick:     c.cfg.HeartbeatTick,
		SnapshotThreshold: c.cfg.SnapshotThreshold,
		ReadLease:         c.cfg.ReadLease,
		Join:              true,
		Bug:               c.cfg.Bug,
		Observer:          runObserver{c},
	}
	var err error
	node, err = raft.NewNode(rcfg, tr, c.clk, env.sto, sm, to, rnd)
	if err != nil {
		return err
	}
	env.n = node
	env.to = to
	env.tr = tr
	env.step = step
	env.rcfg = rcfg
	c.nodes = append(c.nodes, env)
	c.cfg.N = len(c.nodes)
	c.log.Append(c.clk.Now(), EvFault, fmt.Sprintf("add id=%d", id))
	return nil
}

func (c *Cluster) env(id raft.NodeID) *nodeEnv {
	for _, e := range c.nodes {
		if e.id == id {
			return e
		}
	}
	return nil
}

type Result struct {
	Seed          uint64
	Hash          string
	Events        int
	Until         raft.Instant
	Violations    []raft.Violation
	Leaders       map[raft.NodeID]raft.Role
	LeaderID      raft.NodeID
	FirstLeaderAt raft.Instant
	Elections     int
	History       check.History
	MinorityAcks  int
}

func (c *Cluster) result() *Result {
	roles := map[raft.NodeID]raft.Role{}
	for _, env := range c.nodes {
		roles[env.id] = env.n.Role()
	}
	return &Result{
		Seed:          c.cfg.Seed,
		Hash:          c.log.Hash(),
		Events:        c.log.Len(),
		Until:         c.clk.Now(),
		Violations:    c.mon.Violations(),
		Leaders:       roles,
		LeaderID:      c.Leader(),
		FirstLeaderAt: c.FirstLeaderAt(),
		Elections:     c.ElectionCount(),
		History:       c.hist.Clone(),
		MinorityAcks:  c.minOK,
	}
}

func Replay(cfg ClusterConfig) (h1, h2 string, r1, r2 *Result, err error) {
	c1, err := NewCluster(cfg)
	if err != nil {
		return "", "", nil, nil, err
	}
	r1 = c1.Run()
	c2, err := NewCluster(cfg)
	if err != nil {
		return "", "", nil, nil, err
	}
	r2 = c2.Run()
	return r1.Hash, r2.Hash, r1, r2, nil
}
