package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

type KV struct {
	m map[string]string
}

func NewKV() *KV { return &KV{m: map[string]string{}} }

func (k *KV) Apply(_ Index, command []byte) []byte {
	if len(command) == 0 {
		return nil
	}
	s := string(command)
	parts := strings.SplitN(s, " ", 3)
	if len(parts) == 0 {
		return []byte("err")
	}
	switch parts[0] {
	case "Get":
		if len(parts) < 2 {
			return []byte("err")
		}
		v, ok := k.m[parts[1]]
		if !ok {
			return []byte("")
		}
		return []byte(v)
	case "Put":
		if len(parts) < 3 {
			return []byte("err")
		}
		k.m[parts[1]] = parts[2]
		return []byte("ok")
	case "CAS":
		fields := strings.SplitN(s, " ", 4)
		if len(fields) < 4 {
			return []byte("err")
		}
		cur := k.m[fields[1]]
		if cur == fields[2] {
			k.m[fields[1]] = fields[3]
			return []byte("ok")
		}
		return []byte("fail")
	default:
		return []byte("err")
	}
}

func (k *KV) Snapshot() []byte {
	keys := make([]string, 0, len(k.m))
	for key := range k.m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	_ = binary.Write(&b, binary.LittleEndian, uint32(len(keys)))
	for _, key := range keys {
		val := k.m[key]
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(key)))
		b.WriteString(key)
		_ = binary.Write(&b, binary.LittleEndian, uint32(len(val)))
		b.WriteString(val)
	}
	return b.Bytes()
}

func (k *KV) Restore(snap []byte) {
	k.m = map[string]string{}
	if len(snap) < 4 {
		return
	}
	r := bytes.NewReader(snap)
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return
	}
	for i := uint32(0); i < n; i++ {
		var kl uint32
		if err := binary.Read(r, binary.LittleEndian, &kl); err != nil {
			return
		}
		kb := make([]byte, kl)
		if _, err := r.Read(kb); err != nil {
			return
		}
		var vl uint32
		if err := binary.Read(r, binary.LittleEndian, &vl); err != nil {
			return
		}
		vb := make([]byte, vl)
		if _, err := r.Read(vb); err != nil {
			return
		}
		k.m[string(kb)] = string(vb)
	}
}

func (k *KV) Get(key string) (string, bool) {
	v, ok := k.m[key]
	return v, ok
}

func EncodePut(key, val string) []byte {
	return []byte(fmt.Sprintf("Put %s %s", key, val))
}

func EncodeGet(key string) []byte {
	return []byte("Get " + key)
}
