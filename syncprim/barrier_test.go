package syncprim

import (
	"runtime"
	"sync/atomic"
	"testing"
)

// Тест 1: Проверка обычного барьера
func TestBarrierReusable(t *testing.T) {
	const N = 8
	const Rounds = 1000 // Уменьшено для -race, иначе будет считаться 10 минут

	b1 := NewBarrier(N)
	b2 := NewBarrier(N) // Второй барьер: не даем потокам убежать в следующий раунд

	var counter atomic.Int64
	var done atomic.Int32
	var failed atomic.Bool

	for i := 0; i < N; i++ {
		go func() {
			for r := 0; r < Rounds; r++ {
				counter.Add(1)
				b1.Wait() // Ждем, пока все сделают Add(1)

				want := int64((r + 1) * N)
				if counter.Load() != want {
					failed.Store(true)
				}

				b2.Wait() // Ждем, пока все проверят counter, ТОЛЬКО ПОТОМ идем на новый круг
			}
			done.Add(1)
		}()
	}

	// Главный поток ждет завершения. Gosched отдает процессор рабочим горутинам.
	for done.Load() != N {
		runtime.Gosched()
	}

	if failed.Load() {
		t.Fatal("Барьер сломался: обнаружено прохождение до того, как все потоки дошли")
	}
}

// Тест 2: Проверка барьера с лидером
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
					prepCount.Add(1) // Это выполняет только лидер
				})

				// После барьера проверяем: лидер отработал ровно 1 раз за раунд?
				if prepCount.Load() != int32(r+1) {
					failed.Store(true)
				}

				b2.Wait() // Синхронизация перед следующим раундом
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
