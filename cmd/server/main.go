package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"operating_systems/syncprim"
)

//go:embed index.html
var indexHTML []byte

//go:embed gauss.html
var gaussHTML []byte

type Event struct {
	Type       string      `json:"type"`
	Thread     int         `json:"thread"`
	State      string      `json:"state"`
	Round      int         `json:"round"`
	N          int         `json:"n"`
	Count      uint32      `json:"count"`
	Sense      uint32      `json:"sense"`
	Matrix     [][]float64 `json:"matrix,omitempty"`
	Row        int         `json:"row,omitempty"`
	Col        int         `json:"col,omitempty"`
	ActiveRows []int       `json:"activeRows,omitempty"`
}

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

func (h *Hub) Broadcast(e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch e.Type {
	case "reset":
		h.round = 0
		h.count = 0
		h.sense = 0
		for i := range h.states {
			h.states[i] = "idle"
		}
	case "state", "gauss_state":
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
		}
	}
}

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

	h.mu.Lock()
	initData := h.InitEventLocked()
	states := make([]string, len(h.states))
	copy(states, h.states)
	h.mu.Unlock()

	fmt.Fprintf(w, "data: %s\n\n", initData)
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

// --- Демо 1: Абстрактный барьер ---
func runDemo(hub *Hub, n, rounds int) {
	b := syncprim.NewBarrier(n)
	hub.Broadcast(Event{Type: "reset", N: n})

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
				hub.Broadcast(Event{Type: "snapshot", Count: b.Count(), Sense: b.Sense()})
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				hub.Broadcast(Event{Type: "state", Thread: id, State: "work", Round: r})
				time.Sleep(time.Duration(800+id*400) * time.Millisecond)

				hub.Broadcast(Event{Type: "state", Thread: id, State: "arrive", Round: r})

				b.WaitLeader(func() {
					hub.Broadcast(Event{Type: "state", Thread: id, State: "prep", Round: r})
					time.Sleep(1500 * time.Millisecond)
				})

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

// --- Демо 2: Метод Гаусса ---
func runGaussDemo(hub *Hub, n int) {
	b := syncprim.NewBarrier(n)

	matrix := make([][]float64, n)
	for i := range matrix {
		matrix[i] = make([]float64, n+1)
		for j := range matrix[i] {
			matrix[i][j] = float64(rand.Intn(9) + 1)
		}
	}

	copyMatrix := func(m [][]float64) [][]float64 {
		c := make([][]float64, len(m))
		for i := range m {
			c[i] = make([]float64, len(m[i]))
			copy(c[i], m[i])
		}
		return c
	}

	hub.Broadcast(Event{Type: "gauss_init", Matrix: copyMatrix(matrix)})
	time.Sleep(5 * time.Second)

	var matrixMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(id int) {
			defer wg.Done()
			for col := 0; col < n-1; col++ {

				b.WaitLeader(func() {
					hub.Broadcast(Event{Type: "gauss_state", Thread: id, State: "prep"})

					matrixMu.Lock()
					maxRow := col
					for i := col + 1; i < n; i++ {
						if math.Abs(matrix[i][col]) > math.Abs(matrix[maxRow][col]) {
							maxRow = i
						}
					}
					matrix[col], matrix[maxRow] = matrix[maxRow], matrix[col]
					pivot := matrix[col][col]
					for j := col; j <= n; j++ {
						matrix[col][j] /= pivot
					}
					matrixMu.Unlock()

					hub.Broadcast(Event{Type: "gauss_prep", Col: col, Matrix: copyMatrix(matrix)})
					time.Sleep(3 * time.Second)
				})

				hub.Broadcast(Event{Type: "gauss_state", Thread: id, State: "work"})

				var myRows []int
				for r := col + 1 + id; r < n; r += n {
					myRows = append(myRows, r)
				}

				if len(myRows) > 0 {
					// ЗАДЕРЖКА 100МС: Даем браузеру время отрисовать предыдущую строку
					time.Sleep(100 * time.Millisecond)

					hub.Broadcast(Event{
						Type:       "gauss_work_start",
						Thread:     id,
						ActiveRows: myRows,
						Col:        col,
					})

					// Потоки "работают" параллельно (3 секунды)
					time.Sleep(3 * time.Second)

					matrixMu.Lock()
					for _, r := range myRows {
						mult := matrix[r][col]
						for j := col; j <= n; j++ {
							matrix[r][j] -= mult * matrix[col][j]
						}
					}
					snap := copyMatrix(matrix)
					matrixMu.Unlock()

					hub.Broadcast(Event{
						Type:       "gauss_matrix",
						Col:        col,
						Matrix:     snap,
						ActiveRows: myRows,
					})
				}

				hub.Broadcast(Event{Type: "gauss_state", Thread: id, State: "idle"})
				time.Sleep(1 * time.Second)
			}
		}(i)
	}
	wg.Wait()
	hub.Broadcast(Event{Type: "gauss_finished"})
	time.Sleep(3 * time.Second)
}

func main() {
	const n = 5

	// Хаб 1
	hub1 := NewHub(n)
	go func() {
		for {
			runDemo(hub1, n, 2)
			hub1.Broadcast(Event{Type: "finished"})
			time.Sleep(5 * time.Second)
		}
	}()

	// Хаб 2
	hub2 := NewHub(n)
	go func() {
		for {
			runGaussDemo(hub2, n)
			time.Sleep(5 * time.Second)
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	http.HandleFunc("/events", hub1.SSEHandler)

	http.HandleFunc("/gauss", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(gaussHTML)
	})
	http.HandleFunc("/gauss-events", hub2.SSEHandler)

	log.Println("Абстрактная визуализация: http://localhost:8080/")
	log.Println("Метод Гаусса:            http://localhost:8080/gauss")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
