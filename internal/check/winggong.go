package check

import (
	"fmt"
	"sort"
)

type kvState map[string]string

func cloneState(s kvState) kvState {
	out := make(kvState, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

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

func WingGongKV(h History) Verdict {
	ops := make([]Op, 0, len(h.Ops))
	for _, op := range h.Ops {
		if op.ReturnAt == 0 && !op.Ok {
			continue
		}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return Verdict{Linearizable: true, Checker: "winggong"}
	}
	sort.SliceStable(ops, func(i, j int) bool {
		if ops[i].CallAt != ops[j].CallAt {
			return ops[i].CallAt < ops[j].CallAt
		}
		return ops[i].Seq < ops[j].Seq
	})
	if search(ops, 0, kvState{}, make([]bool, len(ops))) {
		return Verdict{Linearizable: true, Checker: "winggong"}
	}
	return Verdict{Linearizable: false, Checker: "winggong", Detail: "no linearization"}
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
