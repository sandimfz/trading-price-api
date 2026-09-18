package wsapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/alen/trading-price-api/internal/apikey"
	"github.com/alen/trading-price-api/internal/broadcast"
)

// ClientMessage adalah bentuk pesan yang diterima dari client downstream.
type ClientMessage struct {
	Action  string   `json:"action"`            // subscribe | unsubscribe
	Symbols []string `json:"symbols,omitempty"` // daftar simbol (format internal)
}

// SubManager adalah interface ref-counting subscription (subscription.Manager).
type SubManager interface {
	Subscribe(symbol string) error
	Unsubscribe(symbol string) error
}

// wsClient membungkus koneksi downstream + key yang terautentikasi.
type wsClient struct {
	conn  *websocket.Conn
	key   *apikey.Key
	subs  map[string]struct{}
	subMu sync.Mutex
	hub   *broadcast.Hub
	up    SubManager
	log   *slog.Logger
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Handler adalah http.Handler untuk endpoint WebSocket eksternal.
type Handler struct {
	hub        *broadcast.Hub
	up         SubManager
	keySvc     *apikey.KeyService
	log        *slog.Logger
	writeWait  time.Duration
	pongWait   time.Duration
	pingPeriod time.Duration
	// allowedOrigins adalah daftar origin yang diizinkan; kosong = tolak semua
	// kecuali "AllowAllOrigins()" di-set eksplisit untuk development.
	allowedOrigins []string
	allowAll       bool
	// activeConns melacak koneksi aktif per key untuk enforce max_concurrent_ws_conn.
	connMu sync.Mutex
	conns  map[string]int
}

// AllowAllOrigins mengizinkan semua origin (hanya untuk development).
func (h *Handler) AllowAllOrigins() *Handler {
	h.allowAll = true
	return h
}

// WithAllowedOrigins membatasi origin yang boleh connect (format https://host).
func (h *Handler) WithAllowedOrigins(origins ...string) *Handler {
	h.allowedOrigins = origins
	return h
}

// NewHandler membuat WebSocket handler.
func NewHandler(hub *broadcast.Hub, up SubManager, keySvc *apikey.KeyService, log *slog.Logger) *Handler {
	return &Handler{
		hub:        hub,
		up:         up,
		keySvc:     keySvc,
		log:        log,
		writeWait:  10 * time.Second,
		pongWait:   60 * time.Second,
		pingPeriod: 50 * time.Second,
		conns:      make(map[string]int),
	}
}

func (h *Handler) originAllowed(r *http.Request) bool {
	if h.allowAll {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		// koneksi non-browser tanpa Origin: izinkan hanya kalau tidak ada whitelist
		return len(h.allowedOrigins) == 0
	}
	for _, o := range h.allowedOrigins {
		if o == origin {
			return true
		}
	}
	return false
}

// ServeHTTP meng-upgrade koneksi dan memvalidasi API key via query param.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.originAllowed(r) {
		http.Error(w, `{"error":"origin not allowed"}`, http.StatusForbidden)
		return
	}

	key := r.URL.Query().Get("api_key")
	if key == "" {
		http.Error(w, `{"error":"missing api_key query parameter"}`, http.StatusUnauthorized)
		return
	}

	k, err := h.keySvc.Resolve(r.Context(), key)
	if err != nil {
		http.Error(w, `{"error":"invalid api key"}`, http.StatusUnauthorized)
		return
	}

	if !h.acquireConn(k) {
		http.Error(w, `{"error":"max concurrent connections reached"}`, http.StatusTooManyRequests)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.releaseConn(k)
		h.log.Warn("ws upgrade failed", "err", err)
		return
	}

	c := &wsClient{
		conn: conn,
		key:  k,
		subs: make(map[string]struct{}),
		hub:  h.hub,
		up:   h.up,
		log:  h.log,
	}
	h.hub.Register(c)

	conn.SetReadDeadline(time.Now().Add(h.pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(h.pongWait))
	})

	h.log.Info("ws client connected", "prefix", k.Prefix, "tier", k.Tier)
	go h.writePump(c)
	h.readPump(c)
	h.releaseConn(k)
}

// acquireConn mengecek dan menambah hitungan koneksi aktif per key.
func (h *Handler) acquireConn(k *apikey.Key) bool {
	h.connMu.Lock()
	defer h.connMu.Unlock()

	if h.conns[k.ID] >= k.MaxConcurrentWS {
		return false
	}
	h.conns[k.ID]++
	return true
}

// releaseConn mengurangi hitungan koneksi aktif per key.
func (h *Handler) releaseConn(k *apikey.Key) {
	h.connMu.Lock()
	defer h.connMu.Unlock()

	if h.conns[k.ID] <= 1 {
		delete(h.conns, k.ID)
		return
	}
	h.conns[k.ID]--
}

func (h *Handler) readPump(c *wsClient) {
	defer func() {
		h.hub.Unregister(c)
		c.cleanupSubscriptions()
		_ = c.conn.Close()
		h.log.Info("ws client disconnected", "prefix", c.key.Prefix)
	}()

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			return
		}

		var msg ClientMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			_ = c.conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"invalid message format"}`))
			continue
		}

		switch msg.Action {
		case "subscribe":
			h.handleSubscribe(c, msg.Symbols)
		case "unsubscribe":
			h.handleUnsubscribe(c, msg.Symbols)
		default:
			_ = c.conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"unknown action"}`))
		}
	}
}

func (h *Handler) handleSubscribe(c *wsClient, symbols []string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	if len(c.subs) >= c.key.MaxSymbolsPerKey {
		_ = c.conn.WriteMessage(websocket.TextMessage, []byte(`{"error":"max symbols reached for this tier"}`))
		return
	}

	for _, sym := range symbols {
		if _, ok := c.subs[sym]; ok {
			continue
		}
		if len(c.subs) >= c.key.MaxSymbolsPerKey {
			break
		}
		if err := c.up.Subscribe(sym); err != nil {
			continue
		}
		c.subs[sym] = struct{}{}
		c.hub.Subscribe(c, sym)
	}

	_ = c.conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribed","symbols":`+jsonList(c.subs)+`}`))
}

func (h *Handler) handleUnsubscribe(c *wsClient, symbols []string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	for _, sym := range symbols {
		if _, ok := c.subs[sym]; !ok {
			continue
		}
		_ = c.up.Unsubscribe(sym)
		delete(c.subs, sym)
		c.hub.Unsubscribe(c, sym)
	}
}

// cleanupSubscriptions melepas semua simbol yang disubscribe client ini.
func (c *wsClient) cleanupSubscriptions() {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	for sym := range c.subs {
		_ = c.up.Unsubscribe(sym)
		delete(c.subs, sym)
	}
}

// writePump mengirim ping periodik untuk menjaga koneksi.
func (h *Handler) writePump(c *wsClient) {
	ticker := time.NewTicker(h.pingPeriod)
	defer ticker.Stop()

	for range ticker.C {
		c.conn.SetWriteDeadline(time.Now().Add(h.writeWait))
		if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
			return
		}
	}
}

// WriteJSON mengimplementasikan broadcast.Client.
func (c *wsClient) WriteJSON(v any) error {
	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return c.conn.WriteMessage(websocket.TextMessage, mustMarshal(v))
}

// Close mengimplementasikan broadcast.Client.
func (c *wsClient) Close() error {
	return c.conn.Close()
}

func jsonList(set map[string]struct{}) string {
	list := make([]string, 0, len(set))
	for s := range set {
		list = append(list, s)
	}
	b, _ := json.Marshal(list)
	return string(b)
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte(`{"error":"marshal failed"}`)
	}
	return b
}
