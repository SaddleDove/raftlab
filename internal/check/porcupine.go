package check

func PorcupineKV(h History) Verdict {
	ops := make([]Op, 0, len(h.Ops))
	for _, op := range h.Ops {
		if op.ReturnAt == 0 && !op.Ok {
			continue
		}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return Verdict{Linearizable: true, Checker: "porcupine"}
	}
	type frame struct {
		mask uint64
		st   kvState
	}
	if len(ops) > 16 {
		v := WingGongKV(h)
		v.Checker = "porcupine"
		return v
	}
	start := frame{mask: 0, st: kvState{}}
	queue := []frame{start}
	seen := map[uint64]struct{}{0: {}}
	n := len(ops)
	target := uint64(1)<<n - 1
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.mask == target {
			return Verdict{Linearizable: true, Checker: "porcupine"}
		}
		for i, op := range ops {
			bit := uint64(1) << i
			if cur.mask&bit != 0 {
				continue
			}
			if !minimalMask(ops, cur.mask, i) {
				continue
			}
			ns, ok, _ := applyOp(cur.st, op)
			if !ok {
				continue
			}
			next := cur.mask | bit
			if _, dup := seen[next]; dup {
				continue
			}
			seen[next] = struct{}{}
			queue = append(queue, frame{mask: next, st: ns})
		}
	}
	return Verdict{Linearizable: false, Checker: "porcupine", Detail: "no linearization"}
}

func minimalMask(ops []Op, mask uint64, i int) bool {
	for j, other := range ops {
		if mask&(1<<j) != 0 || j == i {
			continue
		}
		if other.ReturnAt > 0 && other.ReturnAt < ops[i].CallAt {
			return false
		}
	}
	return true
}

func CrossCheckKV(h History) Verdict {
	a := WingGongKV(h)
	b := PorcupineKV(h)
	if a.Linearizable != b.Linearizable {
		return Verdict{
			Linearizable: false,
			Checker:      "cross",
			Detail:       "winggong=" + boolStr(a.Linearizable) + " porcupine=" + boolStr(b.Linearizable),
		}
	}
	if !a.Linearizable {
		return a
	}
	return Verdict{Linearizable: true, Checker: "cross"}
}
