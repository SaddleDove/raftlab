package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"raftlab/internal/check"
	"raftlab/internal/raft"
	"raftlab/internal/sim"
	"raftlab/internal/transport"
	"raftlab/internal/unsafe"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "gate-m0":
		os.Exit(gateM0(os.Args[2:]))
	case "gate-m1":
		os.Exit(gateM1(os.Args[2:]))
	case "gate-m2":
		os.Exit(gateM2(os.Args[2:]))
	case "gate-m3":
		os.Exit(gateM3(os.Args[2:]))
	case "gate-m4":
		os.Exit(gateM4(os.Args[2:]))
	case "gate-m5":
		os.Exit(gateM5(os.Args[2:]))
	case "gate-m6":
		os.Exit(gateM6(os.Args[2:]))
	case "gate-m7":
		os.Exit(gateM7(os.Args[2:]))
	case "gate-m8":
		os.Exit(gateM8(os.Args[2:]))
	case "sim":
		os.Exit(runSim(os.Args[2:]))
	case "demo-kv":
		os.Exit(demoKV(os.Args[2:]))
	case "cluster":
		os.Exit(runCluster(os.Args[2:]))
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `raftlab — Raft + fault injector + invariant checker

Usage:
  raftlab gate-m0 [-seeds n] [-until t]
  raftlab gate-m1 [-seeds n]
  raftlab gate-m2 [-seed n]
  raftlab gate-m3 [-seeds n]
  raftlab gate-m4 [-seed n]
  raftlab gate-m5 [-seeds n]
  raftlab gate-m6 [-seed n]
  raftlab gate-m7 [-seed n]
  raftlab gate-m8 [-seed n]
  raftlab sim -n 3 -seed 1 -until 200
  raftlab demo-kv [-seed n]
  raftlab cluster -n 5 -port 17000
`)
}

func gateM0(args []string) int {
	fs := flag.NewFlagSet("gate-m0", flag.ExitOnError)
	seeds := fs.Int("seeds", 3, "number of seeds (default 1,2,7)")
	until := fs.Int64("until", 200, "virtual time to run")
	_ = fs.Parse(args)
	list := []uint64{1, 2, 7}
	if *seeds > 3 {
		for i := 4; i <= *seeds; i++ {
			list = append(list, uint64(i*13+7))
		}
	}
	ok := true
	for _, seed := range list {
		cfg := sim.DefaultClusterConfig(3, seed)
		cfg.RunUntil = raft.Instant(*until)
		h1, h2, r1, r2, err := sim.Replay(cfg)
		if err != nil {
			fmt.Printf("FAIL seed=%d err=%v\n", seed, err)
			ok = false
			continue
		}
		if h1 != h2 {
			fmt.Printf("FAIL seed=%d hash mismatch %s vs %s events=%d/%d\n", seed, h1, h2, r1.Events, r2.Events)
			ok = false
			continue
		}
		fmt.Printf("PASS seed=%d hash=%s events=%d until=%d\n", seed, h1, r1.Events, r1.Until)
	}
	if !ok {
		fmt.Println("M0 GATE FAIL")
		return 1
	}
	fmt.Println("M0 GATE PASS")
	return 0
}

func gateM1(args []string) int {
	fs := flag.NewFlagSet("gate-m1", flag.ExitOnError)
	seeds := fs.Int("seeds", 1000, "seeds")
	n := fs.Int("n", 3, "nodes")
	until := fs.Int64("until", 400, "until")
	_ = fs.Parse(args)
	rep := sim.RunElectionGate(*seeds, *n, raft.Instant(*until))
	fmt.Println(rep.Detail)
	if rep.Fail > 0 {
		fmt.Printf("M1 GATE FAIL fail=%d first=%d\n", rep.Fail, rep.FirstFail)
		return 1
	}
	fmt.Println("M1 GATE PASS")
	return 0
}

func gateM2(args []string) int {
	fs := flag.NewFlagSet("gate-m2", flag.ExitOnError)
	seed := fs.Uint64("seed", 1, "seed")
	_ = fs.Parse(args)
	v, viol, err := sim.RunLinearizableQuiet(*seed, 3)
	if err != nil {
		fmt.Println(err)
		fmt.Println("M2 GATE FAIL")
		return 1
	}
	fmt.Printf("linearizable=%v checker=%s detail=%s violations=%d\n", v.Linearizable, v.Checker, v.Detail, len(viol))
	if !v.Linearizable || len(viol) > 0 {
		fmt.Println("M2 GATE FAIL")
		return 1
	}
	fmt.Println("M2 GATE PASS")
	return 0
}

func gateM3(args []string) int {
	fs := flag.NewFlagSet("gate-m3", flag.ExitOnError)
	seeds := fs.Int("seeds", 1000, "seeds")
	_ = fs.Parse(args)
	rep := sim.RunCrashGate(*seeds)
	fmt.Printf("crash-gate fail=%d first=%d %s\n", rep.Fail, rep.FirstFail, rep.Detail)
	if rep.Fail > 0 {
		fmt.Println("M3 GATE FAIL")
		return 1
	}
	fmt.Println("M3 GATE PASS")
	return 0
}

func gateM4(args []string) int {
	fs := flag.NewFlagSet("gate-m4", flag.ExitOnError)
	seed := fs.Uint64("seed", 7, "seed")
	out := fs.String("html", "reports/timeline-m4.html", "timeline html")
	_ = fs.Parse(args)
	c, err := sim.RunPartitionGate(*seed)
	if err != nil {
		fmt.Println(err)
		fmt.Println("M4 GATE FAIL")
		return 1
	}
	_ = os.MkdirAll(filepath.Dir(*out), 0o755)
	if err := sim.WriteTimeline(*out, c.Log(), fmt.Sprintf("M4 seed=%d", *seed)); err != nil {
		fmt.Println(err)
		fmt.Println("M4 GATE FAIL")
		return 1
	}
	fmt.Printf("minority-acks=%d violations=%d html=%s\n", c.MinorityAcks(), len(c.Monitor().Violations()), *out)
	if c.MinorityAcks() != 0 {
		fmt.Println("M4 GATE FAIL")
		return 1
	}
	for _, v := range c.Monitor().Violations() {
		if v.Code == raft.I8NoSplitBrainWrite {
			fmt.Println("M4 GATE FAIL", v)
			return 1
		}
	}
	fmt.Println("M4 GATE PASS")
	return 0
}

func gateM5(args []string) int {
	fs := flag.NewFlagSet("gate-m5", flag.ExitOnError)
	seeds := fs.Int("seeds", 200, "random seeds per node count")
	_ = fs.Parse(args)
	ok := true
	for _, n := range []int{3, 5} {
		for i := 1; i <= *seeds; i++ {
			cfg := sim.DefaultClusterConfig(n, uint64(i*31+n))
			cfg.UseKV = true
			cfg.RunUntil = 30
			c, err := sim.NewCluster(cfg)
			if err != nil {
				fmt.Printf("FAIL n=%d seed=%d err=%v\n", n, i, err)
				ok = false
				continue
			}
			rnd := sim.Derive(cfg.Seed, 9)
			c.WaitLeader(50)
			sim.ApplyRandomFaults(c, rnd, c.Now()+80)
			if c.Leader() != 0 {
				c.Propose("k", fmt.Sprintf("%d", i))
			}
			c.RunUntil(c.Now() + 80)
			c.Heal()
			c.RunUntil(c.Now() + 40)
			if len(c.Monitor().Violations()) > 0 {
				fmt.Printf("FAIL n=%d seed=%d %s\n", n, i, c.Monitor().Violations()[0])
				_ = sim.WriteFailure("reports/failures", uint64(i), c.Now(), c.Hash(), c.Monitor().Violations()[0].String())
				ok = false
			}
			v := check.CrossCheck(c.History())
			if !v.Linearizable {
				fmt.Printf("FAIL lin n=%d seed=%d %s\n", n, i, v.Detail)
				ok = false
			}
		}
	}
	fmt.Printf("random harness seeds=%d x {3,5}\n", *seeds)
	caught := 0
	for _, name := range unsafe.Names() {
		b, _ := unsafe.Variant(name)
		if catchBug(b) {
			fmt.Printf("CAUGHT %s\n", name)
			caught++
		} else {
			fmt.Printf("MISSED %s\n", name)
			ok = false
		}
	}
	fmt.Printf("variants caught %d/8\n", caught)
	replayOK := 0
	for i := 1; i <= 50; i++ {
		cfg := sim.DefaultClusterConfig(3, uint64(i))
		h1, h2, _, _, err := sim.Replay(cfg)
		if err == nil && h1 == h2 {
			replayOK++
		} else {
			ok = false
		}
	}
	fmt.Printf("replay %d/50\n", replayOK)
	if !ok {
		fmt.Println("M5 GATE FAIL")
		return 1
	}
	fmt.Println("M5 GATE PASS")
	return 0
}

func gateM6(args []string) int {
	fs := flag.NewFlagSet("gate-m6", flag.ExitOnError)
	seed := fs.Uint64("seed", 3, "seed")
	_ = fs.Parse(args)
	c, err := sim.RunSnapshotGate(*seed)
	if err != nil {
		fmt.Println(err)
		fmt.Println("M6 GATE FAIL")
		return 1
	}
	viol := c.Monitor().Violations()
	applied := raft.Index(0)
	if n := c.Node(3); n != nil {
		applied = n.Applied()
	}
	fmt.Printf("snapshot compacted=%v applied3=%d violations=%d\n", c.Compacted(), applied, len(viol))
	if len(viol) > 0 {
		fmt.Println(viol[0])
		fmt.Println("M6 GATE FAIL")
		return 1
	}
	fmt.Println("M6 GATE PASS")
	return 0
}

func gateM7(args []string) int {
	fs := flag.NewFlagSet("gate-m7", flag.ExitOnError)
	seed := fs.Uint64("seed", 5, "seed")
	_ = fs.Parse(args)
	c, err := sim.RunMembershipGate(*seed)
	if err != nil {
		fmt.Println(err)
		fmt.Println("M7 GATE FAIL")
		return 1
	}
	viol := c.Monitor().Violations()
	v := check.CrossCheck(c.History())
	joint := false
	if n := c.Node(c.Leader()); n != nil {
		joint = n.Conf().InJoint()
	}
	fmt.Printf("voters=%v joint=%v lin=%v violations=%d\n", c.ConfVoters(), joint, v.Linearizable, len(viol))
	if len(viol) > 0 || !v.Linearizable || joint {
		fmt.Println("M7 GATE FAIL")
		return 1
	}
	fmt.Println("M7 GATE PASS")
	return 0
}

func gateM8(args []string) int {
	fs := flag.NewFlagSet("gate-m8", flag.ExitOnError)
	seed := fs.Uint64("seed", 1, "seed")
	_ = fs.Parse(args)
	ok := true
	for _, lease := range []raft.Instant{0, 20} {
		v, viol, err := sim.RunReadIndexGate(*seed, lease)
		if err != nil {
			fmt.Println(err)
			ok = false
			continue
		}
		fmt.Printf("lease=%d lin=%v checker=%s violations=%d\n", lease, v.Linearizable, v.Checker, len(viol))
		if !v.Linearizable || len(viol) > 0 {
			ok = false
		}
	}
	if !ok {
		fmt.Println("M8 GATE FAIL")
		return 1
	}
	fmt.Println("M8 GATE PASS")
	return 0
}

func catchBug(b raft.Bug) bool {
	r := sim.CatchBug(b)
	if r.Caught {
		fmt.Printf("  seed=%d until=%d %s\n", r.Seed, r.Until, r.Violation)
	}
	return r.Caught
}

func runCluster(args []string) int {
	fs := flag.NewFlagSet("cluster", flag.ExitOnError)
	n := fs.Int("n", 5, "nodes")
	port := fs.Int("port", 17000, "base port")
	_ = fs.Parse(args)
	fmt.Printf("tcp cluster n=%d base-port=%d (smoke bind)\n", *n, *port)
	peers := transport.LocalPeers(*n, *port)
	var open []*transport.TCP
	ok := true
	for id, addr := range peers {
		t, err := transport.Listen(id, addr, peers)
		if err != nil {
			fmt.Printf("FAIL bind %d %s: %v\n", id, addr, err)
			ok = false
			break
		}
		open = append(open, t)
		fmt.Printf("listen id=%d addr=%s\n", id, t.Addr())
	}
	for _, c := range open {
		_ = c.Close()
	}
	if !ok {
		return 1
	}
	fmt.Println("TCP SMOKE PASS")
	return 0
}

func demoKV(args []string) int {
	fs := flag.NewFlagSet("demo-kv", flag.ExitOnError)
	seed := fs.Uint64("seed", 1, "seed")
	_ = fs.Parse(args)
	v, viol, err := sim.RunLinearizableQuiet(*seed, 3)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("kv demo seed=%d linearizable=%v checker=%s violations=%d\n", *seed, v.Linearizable, v.Checker, len(viol))
	if !v.Linearizable || len(viol) > 0 {
		return 1
	}
	return 0
}

func runSim(args []string) int {
	fs := flag.NewFlagSet("sim", flag.ExitOnError)
	n := fs.Int("n", 3, "nodes")
	seed := fs.Uint64("seed", 1, "seed")
	until := fs.Int64("until", 200, "until")
	_ = fs.Parse(args)
	cfg := sim.DefaultClusterConfig(*n, *seed)
	cfg.RunUntil = raft.Instant(*until)
	c, err := sim.NewCluster(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	r := c.Run()
	fmt.Printf("seed=%d hash=%s events=%d until=%d violations=%d leader=%d first=%d\n",
		r.Seed, r.Hash, r.Events, r.Until, len(r.Violations), r.LeaderID, r.FirstLeaderAt)
	return 0
}
