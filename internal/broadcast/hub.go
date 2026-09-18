package broadcast

import (
	"encoding/json"
	"sync"

	"github.com/alen/trading-price-api/pkg/types"
)

// Client adalah interface untuk subscriber downstream WebSocket.
type Client interface {
	// WriteJSON mengirim pesan ke client; mengembalikan error kalau client sudah putus.
	WriteJSON(v any) error
	// Close menutup koneksi client.
	Close() error
}

// Hub melakukan pub/sub dari upstream ke semua WebSocket client downstream.
// Client bisa subscribe simbol tertentu (default: semua).
type Hub struct {
	mu      sync.RWMutex
	clients map[Client]struct{}
	// per-client filter simbol; empty = terima semua
	subs map[Client]map[string]struct{}
}

// NewHub membuat hub baru.
func NewHub() *Hub {
	return &Hub{
		clients: make(map[Client]struct{}),
		subs:    make(map[Client]map[string]struct{}),
	}
}

// Register mendaftarkan client baru (terima semua simbol).
func (h *Hub) Register(c Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
	h.subs[c] = nil
}

// Unregister melepas client.
func (h *Hub) Unregister(c Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
	delete(h.subs, c)
}

// Subscribe membuat client hanya menerima simbol tertentu.
func (h *Hub) Subscribe(c Client, symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[c] == nil {
		h.subs[c] = make(map[string]struct{})
	}
	h.subs[c][symbol] = struct{}{}
}

// Unsubscribe menghapus filter simbol client.
func (h *Hub) Unsubscribe(c Client, symbol string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[c] == nil {
		return
	}
	delete(h.subs[c], symbol)
	if len(h.subs[c]) == 0 {
		h.subs[c] = nil
	}
}

// PublishTick mengirim tick ke semua client yang relevan.
func (h *Hub) PublishTick(tick types.Tick) {
	payload, _ := json.Marshal(map[string]any{
		"type": "tick",
		"data": tick,
	})

	h.mu.RLock()
	defer h.mu.RUnlock()

	for c := range h.clients {
		if subs := h.subs[c]; subs != nil {
			if _, ok := subs[tick.Symbol]; !ok {
				continue
			}
		}
		if err := c.WriteJSON(json.RawMessage(payload)); err != nil {
			// putus async supaya tidak blokir broadcast
			go h.drop(c)
		}
	}
}

// PublishCandle mengirim candle yang baru ditutup ke semua client.
func (h *Hub) PublishCandle(c types.Candle) {
	payload, _ := json.Marshal(map[string]any{
		"type": "candle",
		"data": c,
	})

	h.mu.RLock()
	defer h.mu.RUnlock()

	for c := range h.clients {
		if err := c.WriteJSON(json.RawMessage(payload)); err != nil {
			go h.drop(c)
		}
	}
}

// ClientCount mengembalikan jumlah client terdaftar.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) drop(c Client) {
	h.Unregister(c)
	_ = c.Close()
}
