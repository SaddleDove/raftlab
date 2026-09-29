package unsafe

import "raftlab/internal/raft"

func Variant(name string) (raft.Bug, bool) {
	for _, b := range raft.AllBugs() {
		if b.String() == name {
			return b, true
		}
	}
	return raft.BugNone, name == raft.BugNone.String()
}

func Names() []string {
	out := make([]string, 0, 8)
	for _, b := range raft.AllBugs() {
		out = append(out, b.String())
	}
	return out
}
