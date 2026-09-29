package raft

func LastTerm(sto LogStore) Term {
	i := sto.LastIndex()
	if i == 0 {
		return 0
	}
	t, err := sto.Term(i)
	if err != nil {
		return 0
	}
	return t
}
