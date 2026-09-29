package check

import (
	"fmt"
	"sort"
)

// kvState is the reference state the checkers linearize against: a plain map.
type kvState map[string]string

func cloneState(s kvState) kvState {
	out := make(kvState, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

// applyOp returns the state after op and whether op is permitted from s.
// Both checkers share this model on purpose: they are two search strategies
// over one definition of correct KV behaviour, so a wrong model is invisible
// to both of them. The cross-check buys agreement between searches, not
// independence from the model.
func applyOp(s kvState, op Op) (kvState, bool, string) {
	ns := cloneState(s)
	switch op.Kind {
	case OpGet:
		v := ns[op.Key]
		if op.Ok && v != op.Result {
			return s, false, fmt.Sprintf("get %s got %q model %q", op.Key, op.Result, v)
		}
		return ns, true, v
	case OpPut:
		ns[op.Key] = op.Value
		return ns, true, "ok"
	case OpCAS:
		cur := ns[op.Key]
		if cur == op.Old {
			ns[op.Key] = op.Value
			if op.Ok && op.Result == "fail" {
				return s, false, "cas should succeed"
			}
			return ns, true, "ok"
		}
		if op.Ok && op.Result == "ok" {
			return s, false, "cas should fail"
		}
		return ns, true, "fail"
	default:
		return ns, true, ""
	}
}

// OrderKV decides linearizability by backtracking over the orders that respect
// real-time precedence: an operation that returned before another was called
// must be placed first. The search is complete, but unpicked-prefix pruning is
// a heuristic, so a large history can be expensive.
func OrderKV(h History) Verdict {
	ops := usableOps(h)
	if len(ops) == 0 {
		return Verdict{Linearizable: true, Checker: "order"}
	}
	sort.SliceStable(ops, func(i, j int) bool {
		if ops[i].CallAt != ops[j].CallAt {
			return ops[i].CallAt < ops[j].CallAt
		}
		return ops[i].Seq < ops[j].Seq
	})
	if search(ops, 0, kvState{}, make([]bool, len(ops))) {
		return Verdict{Linearizable: true, Checker: "order"}
	}
	return Verdict{Linearizable: false, Checker: "order", Detail: "no linearization"}
}

func search(ops []Op, done int, st kvState, used []bool) bool {
	if done == len(ops) {
		return true
	}
	for i, op := range ops {
		if used[i] {
			continue
		}
		if !minimal(ops, used, i) {
			continue
		}
		ns, ok, _ := applyOp(st, op)
		if !ok {
			continue
		}
		used[i] = true
		if search(ops, done+1, ns, used) {
			return true
		}
		used[i] = false
	}
	return false
}

// minimal reports whether op i can be placed next: nothing that must precede it
// is still unplaced.
func minimal(ops []Op, used []bool, i int) bool {
	for j, other := range ops {
		if used[j] || j == i {
			continue
		}
		if other.ReturnAt > 0 && other.ReturnAt < ops[i].CallAt {
			return false
		}
	}
	return true
}
