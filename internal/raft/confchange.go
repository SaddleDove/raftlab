package raft

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	cmdConfJoint = "cfgjoint"
	cmdConfLeave = "cfgleave"
)

type ConfState struct {
	Voters   []NodeID
	Outgoing []NodeID
	Pending  bool
}

func EncodeJoint(voters []NodeID) []byte {
	return EncodeJointWithOld(voters, nil)
}

func EncodeJointWithOld(voters, old []NodeID) []byte {
	s := cmdConfJoint + " " + joinIDs(voters)
	if len(old) > 0 {
		s += ":" + joinIDs(old)
	}
	return []byte(s)
}

func joinIDs(ids []NodeID) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatUint(uint64(id), 10)
	}
	return strings.Join(parts, ",")
}

func parseIDs(body string) ([]NodeID, bool) {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil, true
	}
	var ids []NodeID
	for _, p := range strings.Split(body, ",") {
		if p == "" {
			continue
		}
		v, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			return nil, false
		}
		ids = append(ids, NodeID(v))
	}
	return ids, true
}

func EncodeLeaveJoint() []byte {
	return []byte(cmdConfLeave)
}

func ParseConf(cmd []byte) (joint bool, leave bool, voters []NodeID, ok bool) {
	joint, leave, voters, _, ok = ParseConfEx(cmd)
	return
}

func ParseConfEx(cmd []byte) (joint bool, leave bool, voters, outgoing []NodeID, ok bool) {
	s := string(cmd)
	if s == cmdConfLeave {
		return false, true, nil, nil, true
	}
	if strings.HasPrefix(s, cmdConfJoint+" ") {
		body := strings.TrimPrefix(s, cmdConfJoint+" ")
		newPart, oldPart := body, ""
		if i := strings.IndexByte(body, ':'); i >= 0 {
			newPart, oldPart = body[:i], body[i+1:]
		}
		voters, ok = parseIDs(newPart)
		if !ok {
			return false, false, nil, nil, false
		}
		outgoing, ok = parseIDs(oldPart)
		if !ok {
			return false, false, nil, nil, false
		}
		return true, false, voters, outgoing, true
	}
	return false, false, nil, nil, false
}

func IsConfChange(cmd []byte) bool {
	_, _, _, ok := ParseConf(cmd)
	return ok
}

func containsID(ids []NodeID, id NodeID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

func majority(n int) int {
	if n <= 0 {
		return 1
	}
	return n/2 + 1
}

func (c ConfState) InJoint() bool { return len(c.Outgoing) > 0 }

func (c ConfState) AllPeers(self NodeID) []NodeID {
	seen := map[NodeID]struct{}{}
	var out []NodeID
	add := func(id NodeID) {
		if id == None || id == self {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	for _, id := range c.Voters {
		add(id)
	}
	for _, id := range c.Outgoing {
		add(id)
	}
	return out
}

func (c ConfState) Has(id NodeID) bool {
	return containsID(c.Voters, id) || containsID(c.Outgoing, id)
}

func sortIDs(ids []NodeID) []NodeID {
	out := CloneIDs(ids)
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func maxIndex(a, b Index) Index {
	if a > b {
		return a
	}
	return b
}

func FormatIDs(ids []NodeID) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
}
