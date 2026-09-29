package check

type Verdict struct {
	Linearizable bool
	Detail       string
	Checker      string
}

func WingGong(h History) Verdict { return WingGongKV(h) }

func Porcupine(h History) Verdict { return PorcupineKV(h) }

func CrossCheck(h History) Verdict { return CrossCheckKV(h) }

func boolStr(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
