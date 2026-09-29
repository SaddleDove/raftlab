package sim

type Stream struct {
	s uint64
}

func NewStream(seed uint64) *Stream {
	if seed == 0 {
		seed = 0x9E3779B97F4A7C15
	}
	return &Stream{s: seed}
}

func Derive(seed uint64, tag uint64) *Stream {
	return NewStream(splitmix(seed ^ (tag * 0x9E3779B97F4A7C15)))
}

func (r *Stream) Uint64() uint64 {
	r.s += 0x9E3779B97F4A7C15
	z := r.s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

func (r *Stream) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.Uint64() % uint64(n))
}

func (r *Stream) Float64() float64 {
	return float64(r.Uint64()>>11) / (1 << 53)
}

func splitmix(z uint64) uint64 {
	z += 0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}
