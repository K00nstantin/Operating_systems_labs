package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"operating_systems/syncprim"
)

//go:embed index.html
var indexHTML []byte

// --- Событие, которое уходит в браузер ---

type Event struct {
	Type   string `json:"type"`
	Thread int    `json:"thread"`
	State  string `json:"state"`
	Round  int    `json:"round"`
	N      int    `json:"n"`
	Count  uint32 `json:"count"`
	Sense  uint32 `json:"sense"`
}

// --- Хаб: следит за последним состоянием и рассылает события ---

type Hub struct {
	mu     sync.Mutex
	subs   map[chan []byte]struct{}
	n      int
	round  int
	count  uint32
	sense  uint32
	states []string
}

func NewHub(n int) *Hub {
	h := &Hub{
		subs:   make(map[chan []byte]struct{}),
		n:      n,
		states: make([]string, n),
	}
	for i := range h.states {
		h.states[i] = "idle"
	}
	return h
}

func (h *Hub) Add(ch chan []byte) {
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) Remove(ch chan []byte) {
	h.mu.Lock()
	delete(h.subs, ch)
	h.mu.Unlock()
}

// Broadcast обновляет внутреннее состояние и шлёт событие всем подписчикам.
func (h *Hub) Broadcast(e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Обновляем "последнее известное" состояние.
	switch e.Type {
	case "reset":
		h.round = 0
		h.count = 0
		h.sense = 0
		for i := range h.states {
			h.states[i] = "idle"
		}
	case "state":
		if e.Thread >= 0 && e.Thread < len(h.states) {
			h.states[e.Thread] = e.State
		}
		if e.State == "work" {
			h.round = e.Round
		}
	case "snapshot":
		h.count = e.Count
		h.sense = e.Sense
	}

	data, _ := json.Marshal(e)
	for ch := range h.subs {
		select {
		case ch <- data:
		default:
			// Медленный подписчик — пропускаем, чтобы не блокировать остальных.
		}
	}
}

// InitEventLocked — стартовое событие для нового подписчика.
func (h *Hub) InitEventLocked() []byte {
	e := Event{
		Type:  "reset",
		N:     h.n,
		Round: h.round,
		Count: h.count,
		Sense: h.sense,
	}
	data, _ := json.Marshal(e)
	return data
}

// SSEHandler — HTTP-обработчик для /events.
func (h *Hub) SSEHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := make(chan []byte, 128)
	h.Add(ch)
	defer h.Remove(ch)

	// Отправляем текущее состояние новому подписчику.
	h.mu.Lock()
	initData := h.InitEventLocked()
	states := make([]string, len(h.states))
	copy(states, h.states)
	h.mu.Unlock()

	fmt.Fprintf(w, "data: %s\n\n", initData)
	// Досылаем состояния всех потоков, чтобы клиент сразу видел картину.
	for i, st := range states {
		e := Event{Type: "state", Thread: i, State: st}
		if st == "work" {
			e.Round = h.round
		}
		data, _ := json.Marshal(e)
		fmt.Fprintf(w, "data: %s\n\n", data)
	}
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprintf(w, ": ping\n\n")
			flusher.Flush()
		case msg := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

// --- Демо: N горутин, rounds раундов ---

func runDemo(hub *Hub, n, rounds int) {
	b := syncprim.NewBarrier(n)
	hub.Broadcast(Event{Type: "reset", N: n})

	// Тикер «снимков» для count/sense.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				hub.Broadcast(Event{
					Type:  "snapshot",
					Count: b.Count(),
					Sense: b.Sense(),
				})
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				// 1. Полезная работа — разной длины, чтобы приходили вразнобой.
				hub.Broadcast(Event{Type: "state", Thread: id, State: "work", Round: r})
				time.Sleep(time.Duration(800+id*400) * time.Millisecond)

				// 2. Пришли к барьеру.
				hub.Broadcast(Event{Type: "state", Thread: id, State: "arrive", Round: r})

				// 3. Барьер с лидером.
				b.WaitLeader(func() {
					hub.Broadcast(Event{Type: "state", Thread: id, State: "prep", Round: r})
					time.Sleep(1500 * time.Millisecond) // имитация подготовки
				})

				// 4. Прошли барьер.
				hub.Broadcast(Event{Type: "state", Thread: id, State: "pass", Round: r})
				time.Sleep(600 * time.Millisecond)
			}
			hub.Broadcast(Event{Type: "state", Thread: id, State: "done"})
		}(i)
	}

	wg.Wait()
	close(stop)
	<-done
}

// --- main ---

func main() {
	const (
		n      = 5
		rounds = 2
	)

	hub := NewHub(n)

	// Демо-цикл: прогон → пауза 2с → снова.
	go func() {
		for {
			runDemo(hub, n, rounds)
			hub.Broadcast(Event{Type: "finished"})
			time.Sleep(5 * time.Second)
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	http.HandleFunc("/events", hub.SSEHandler)

	log.Println("http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
