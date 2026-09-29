package check

// BitmaskLimit is the largest history BitmaskKV will decide. The search is
// breadth-first over subsets, so its state space is 2^n; above the limit the
// caller must decide with OrderKV instead. CrossCheckKV enforces this and
// reports which checkers actually ran.
const BitmaskLimit = 16

// BitmaskKV decides linearizability by breadth-first search over subsets of
// operations. It is complete within the limit and has no pruning heuristics,
// which is what makes it a useful second opinion on a small history.
func BitmaskKV(h History) Verdict {
	ops := usableOps(h)
	if len(ops) == 0 {
		return Verdict{Linearizable: true, Checker: "bitmask"}
	}
	if len(ops) > BitmaskLimit {
		return Verdict{
			Linearizable: false,
			Checker:      "bitmask",
			Detail:       "history too long for the exhaustive checker",
		}
	}
	type frame struct {
		mask uint64
		st   kvState
	}
	start := frame{mask: 0, st: kvState{}}
	queue := []frame{start}
	seen := map[uint64]struct{}{0: {}}
	target := uint64(1)<<len(ops) - 1
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur.mask == target {
			return Verdict{Linearizable: true, Checker: "bitmask"}
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
	return Verdict{Linearizable: false, Checker: "bitmask", Detail: "no linearization"}
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
