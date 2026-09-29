package check

import (
	"fmt"
	"strings"
)

// Verdict is the result of a linearizability check. Checker names the
// strategy that produced it; Checkers lists every strategy that ran and agreed.
type Verdict struct {
	Linearizable bool
	Detail       string
	Checker      string
	Checkers     []string
}

func (v Verdict) String() string {
	s := fmt.Sprintf("linearizable=%v checker=%s", v.Linearizable, v.Checker)
	if len(v.Checkers) > 1 {
		s += " checkers=" + strings.Join(v.Checkers, "+")
	}
	if v.Detail != "" {
		s += " detail=" + v.Detail
	}
	return s
}

// usableOps drops calls that neither returned nor were acknowledged: they carry
// no ordering information.
func usableOps(h History) []Op {
	ops := make([]Op, 0, len(h.Ops))
	for _, op := range h.Ops {
		if op.ReturnAt == 0 && !op.Ok {
			continue
		}
		ops = append(ops, op)
	}
	return ops
}

// Order runs the backtracking checker.
func Order(h History) Verdict { return OrderKV(h) }

// Bitmask runs the exhaustive subset checker.
func Bitmask(h History) Verdict { return BitmaskKV(h) }

// CrossCheck runs both checkers and reports disagreement as a failure. The two
// search strategies share a KV model, so agreement rules out a search bug, not
// a modelling bug.
//
// The exhaustive checker is capped at BitmaskLimit operations. Histories above
// the cap are decided by the backtracking checker alone, and the verdict says
// so in Checker/Checkers and Detail. Nothing is relabelled.
func CrossCheck(h History) Verdict { return CrossCheckKV(h) }

func CrossCheckKV(h History) Verdict {
	n := len(usableOps(h))
	if n > BitmaskLimit {
		v := OrderKV(h)
		v.Checker = "order"
		v.Checkers = []string{"order"}
		if v.Detail == "" {
			v.Detail = fmt.Sprintf("only %d/%d ops decided by one checker", n, n)
		}
		v.Detail = fmt.Sprintf("history has %d usable ops > bitmask limit %d; exhaustive checker skipped (%s)", n, BitmaskLimit, v.Detail)
		return v
	}
	a := OrderKV(h)
	b := BitmaskKV(h)
	if a.Linearizable != b.Linearizable {
		return Verdict{
			Linearizable: false,
			Checker:      "cross",
			Checkers:     []string{"order", "bitmask"},
			Detail:       "order=" + boolStr(a.Linearizable) + " bitmask=" + boolStr(b.Linearizable),
		}
	}
	if !a.Linearizable {
		return Verdict{
			Linearizable: false,
			Checker:      "cross",
			Checkers:     []string{"order", "bitmask"},
			Detail:       a.Detail,
		}
	}
	return Verdict{
		Linearizable: true,
		Checker:      "cross",
		Checkers:     []string{"order", "bitmask"},
		Detail:       fmt.Sprintf("both checkers agreed on %d ops", n),
	}
}

func boolStr(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
