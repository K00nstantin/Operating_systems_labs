package syncprim

import "sync/atomic"

type Barrier struct {
	n     uint32
	count atomic.Uint32
	sense atomic.Uint32
}

func NewBarrier(n int) *Barrier {
	if n <= 0 {
		panic("n <= 0")
	}
	return &Barrier{n: uint32(n)}
}

func (b *Barrier) Wait() {
	s := b.sense.Load()
	if b.count.Add(1) == b.n {
		b.count.Store(0)
		b.sense.Store(s ^ 1)
	} else {
		for b.sense.Load() == s {
		}
	}
}

func (b *Barrier) WaitLeader(prep func()) {
	s := b.sense.Load()
	if b.count.Add(1) == b.n {
		prep()
		b.count.Store(0)
		b.sense.Store(s ^ 1)
	} else {
		for b.sense.Load() == s {
		}
	}
}

func (b *Barrier) Count() uint32 { return b.count.Load() }
func (b *Barrier) Sense() uint32 { return b.sense.Load() }
