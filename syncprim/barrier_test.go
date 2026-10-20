package syncprim

import (
	"runtime"
	"sync/atomic"
	"testing"
)

func TestBarrierReusable(t *testing.T) {
	const N = 8
	const Rounds = 1000

	b1 := NewBarrier(N)
	b2 := NewBarrier(N) // Защита от убегания в следующий раунд

	var counter atomic.Int64
	var done atomic.Int32
	var failed atomic.Bool

	for i := 0; i < N; i++ {
		go func() {
			for r := 0; r < Rounds; r++ {
				counter.Add(1)
				b1.Wait()

				want := int64((r + 1) * N)
				if counter.Load() != want {
					failed.Store(true)
				}

				b2.Wait()
			}
			done.Add(1)
		}()
	}

	for done.Load() != N {
		runtime.Gosched()
	}

	if failed.Load() {
		t.Fatal("Барьер сломался: обнаружено прохождение до того, как все потоки дошли")
	}
}

func TestBarrierLeader(t *testing.T) {
	const N = 8
	const Rounds = 1000

	b1 := NewBarrier(N)
	b2 := NewBarrier(N)

	var prepCount atomic.Int32
	var done atomic.Int32
	var failed atomic.Bool

	for i := 0; i < N; i++ {
		go func() {
			for r := 0; r < Rounds; r++ {
				b1.WaitLeader(func() {
					prepCount.Add(1)
				})

				if prepCount.Load() != int32(r+1) {
					failed.Store(true)
				}
				b2.Wait()
			}
			done.Add(1)
		}()
	}

	for done.Load() != N {
		runtime.Gosched()
	}

	if failed.Load() {
		t.Fatal("Лидер отработал неверное количество раз")
	}
}
