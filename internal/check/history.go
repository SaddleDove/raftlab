package check

import "raftlab/internal/raft"

type OpKind int

const (
	OpGet OpKind = iota + 1
	OpPut
	OpCAS
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpCAS:
		return "cas"
	default:
		return "unknown"
	}
}

type Op struct {
	Kind     OpKind
	Client   uint64
	Seq      uint64
	Key      string
	Value    string
	Old      string
	CallAt   raft.Instant
	ReturnAt raft.Instant
	Ok       bool
	Result   string
}

type History struct {
	Ops []Op
}

func (h *History) Record(op Op) { h.Ops = append(h.Ops, op) }

func (h *History) Clone() History {
	out := History{Ops: make([]Op, len(h.Ops))}
	copy(out.Ops, h.Ops)
	return out
}
