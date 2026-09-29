package sim

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"raftlab/internal/raft"
)

type EventKind int

const (
	EvAdvance EventKind = iota + 1
	EvDeliver
	EvDrop
	EvDup
	EvReorder
	EvCrash
	EvRestart
	EvTimer
	EvClient
	EvState
	EvApply
	EvPersist
	EvViolate
	EvFault
)

func (k EventKind) String() string {
	switch k {
	case EvAdvance:
		return "advance"
	case EvDeliver:
		return "deliver"
	case EvDrop:
		return "drop"
	case EvDup:
		return "dup"
	case EvReorder:
		return "reorder"
	case EvCrash:
		return "crash"
	case EvRestart:
		return "restart"
	case EvTimer:
		return "timer"
	case EvClient:
		return "client"
	case EvState:
		return "state"
	case EvApply:
		return "apply"
	case EvPersist:
		return "persist"
	case EvViolate:
		return "violate"
	case EvFault:
		return "fault"
	default:
		return "unknown"
	}
}

type Event struct {
	Seq  uint64
	At   raft.Instant
	Kind EventKind
	Line string
}

func (e Event) Canonical() string {
	return fmt.Sprintf("%d %d %s %s", e.Seq, e.At, e.Kind, e.Line)
}

type Log struct {
	evs []Event
	seq uint64
}

func NewLog() *Log { return &Log{} }

func (l *Log) Append(at raft.Instant, kind EventKind, line string) Event {
	l.seq++
	e := Event{Seq: l.seq, At: at, Kind: kind, Line: line}
	l.evs = append(l.evs, e)
	return e
}

func (l *Log) Events() []Event { return l.evs }

func (l *Log) Canonical() string {
	var b strings.Builder
	for i, e := range l.evs {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(e.Canonical())
	}
	return b.String()
}

func (l *Log) Hash() string {
	sum := sha256.Sum256([]byte(l.Canonical()))
	return hex.EncodeToString(sum[:])
}

func (l *Log) Len() int { return len(l.evs) }
