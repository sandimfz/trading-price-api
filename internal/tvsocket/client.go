package tvsocket

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/alen/trading-price-api/pkg/types"
)

const authToken = "unauthorized_user_token"

// qsdMessage adalah bentuk pesan data quote dari upstream.
// Format asli: {"m":"qsd","p":["<session_id>",{"n":"SYMBOL","s":"ok","v":{...}}]}
// Elemen pertama p adalah string session id, elemen berikutnya objek simbol.
type qsdMessage struct {
	M string            `json:"m"`
	P []json.RawMessage `json:"p"`
}

type qsdItem struct {
	N string `json:"n"`
	S string `json:"s"`
	V struct {
		// LP pointer supaya bisa dibedakan "tidak ada" vs harga 0.
		LP   *float64 `json:"lp"`
		CH   *float64 `json:"ch"`
		CHP  *float64 `json:"chp"`
		High *float64 `json:"high_price"`
		Low  *float64 `json:"low_price"`
	} `json:"v"`
}

// Client adalah koneksi WebSocket ke upstream TradingView.
type Client struct {
	mu        sync.Mutex
	conn      *websocket.Conn
	sessionID string
	url       string
	origin    string
	tickCh    chan<- types.Tick
	// pending berisi simbol yang disubscribe sebelum koneksi siap;
	// dikirim ulang setelah connect dan setelah reconnect.
	pending map[string]struct{}
}

// NewClient membuat client upstream; tickCh menerima Tick hasil parse qsd.
func NewClient(url, origin string, tickCh chan<- types.Tick) *Client {
	return &Client{
		url:     url,
		origin:  origin,
		tickCh:  tickCh,
		pending: make(map[string]struct{}),
	}
}

// Connect melakukan dial, auth, dan membuat session quote.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	header := map[string][]string{"Origin": {c.origin}}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.url, header)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}

	c.conn = conn
	c.sessionID = "qs_" + randomHex(6)

	if err := c.send(`{"m":"set_auth_token","p":["` + authToken + `"]}`); err != nil {
		conn.Close()
		return fmt.Errorf("send auth token: %w", err)
	}
	if err := c.send(fmt.Sprintf(`{"m":"quote_create_session","p":["%s"]}`, c.sessionID)); err != nil {
		conn.Close()
		return fmt.Errorf("create session: %w", err)
	}
	if err := c.send(fmt.Sprintf(`{"m":"quote_set_fields","p":["%s","lp","ch","chp","high_price","low_price","last_tick"]}`, c.sessionID)); err != nil {
		conn.Close()
		return fmt.Errorf("set fields: %w", err)
	}

	// kirim ulang semua simbol pending (termasuk setelah reconnect).
	// Delay kecil antar simbol untuk menghindari throttle upstream.
	for sym := range c.pending {
		if err := c.send(fmt.Sprintf(`{"m":"quote_add_symbols","p":["%s","%s"]}`, c.sessionID, sym)); err != nil {
			conn.Close()
			return fmt.Errorf("resubscribe %s: %w", sym, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

// SubscribeSymbol menambahkan simbol ke session upstream.
// Kalau belum connect, simbol disimpan pending dan dikirim saat connect.
func (c *Client) SubscribeSymbol(symbol string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pending[symbol] = struct{}{}
	if c.conn == nil {
		return nil
	}
	return c.send(fmt.Sprintf(`{"m":"quote_add_symbols","p":["%s","%s"]}`, c.sessionID, symbol))
}

// UnsubscribeSymbol menghapus simbol dari session upstream.
func (c *Client) UnsubscribeSymbol(symbol string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pending, symbol)
	if c.conn == nil {
		return nil
	}
	return c.send(fmt.Sprintf(`{"m":"quote_remove_symbols","p":["%s","%s"]}`, c.sessionID, symbol))
}

// Close menutup koneksi upstream.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ReadLoop membaca frame dari upstream sampai koneksi putus.
// Tiap frame dicek: ping di-echo, qsd di-parse jadi Tick.
// Frame ~m~{len}~m~ bisa terpecah antar websocket message, jadi semua
// message dialirkan ke satu io.Pipe agar scanner tidak kehilangan sisa frame.
func (c *Client) ReadLoop(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("tvsocket: read loop panic recovered: %v\n", r)
		}
	}()

	pr, pw := io.Pipe()
	defer pw.Close()

	go func() {
		for {
			c.mu.Lock()
			conn := c.conn
			c.mu.Unlock()
			if conn == nil {
				return
			}

			_, raw, err := conn.ReadMessage()
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			if _, err := pw.Write(raw); err != nil {
				return
			}
		}
	}()

	scanner := NewFrameScanner(bufio.NewReader(pr))
	for scanner.Scan() {
		token := scanner.Bytes()
		if isPing(token) {
			c.echo(token)
			continue
		}
		c.handlePayload(token)
	}
}

func (c *Client) echo(token []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.WriteMessage(websocket.TextMessage, []byte(echoPing(token)))
	}
}

func (c *Client) handlePayload(payload []byte) {
	var msg qsdMessage
	if err := json.Unmarshal(payload, &msg); err != nil || msg.M != "qsd" {
		return
	}

	now := time.Now().Unix()
	for _, rawItem := range msg.P {
		var item qsdItem
		if err := json.Unmarshal(rawItem, &item); err != nil {
			continue // elemen bukan objek simbol (misal session id string)
		}
		symbol := stripSessionPrefix(item.N, c.sessionID)
		if symbol == "" {
			continue
		}
		// pesan qsd awal hanya berisi metrics (misal {"metrics_loaded":false})
		// tanpa harga — skip supaya tidak menimpa harga terakhir dengan 0.
		if item.V.LP == nil {
			continue
		}
		tick := types.Tick{
			Symbol:    symbol,
			LastPrice: *item.V.LP,
			Change:    deref(item.V.CH),
			ChangePct: deref(item.V.CHP),
			High:      deref(item.V.High),
			Low:       deref(item.V.Low),
			Timestamp: now,
		}
		select {
		case c.tickCh <- tick:
		default:
			// channel penuh, drop tick — jangan blokir read loop
		}
	}
}

func deref(f *float64) float64 {
	if f == nil {
		return 0
	}
	return *f
}

// stripSessionPrefix menghapus prefix "qs_xxx~" dari nama simbol qsd.
func stripSessionPrefix(name, sessionID string) string {
	prefix := sessionID + "~"
	if strings.HasPrefix(name, prefix) {
		return strings.TrimPrefix(name, prefix)
	}
	return name
}

func (c *Client) send(payload string) error {
	if c.conn == nil {
		return fmt.Errorf("tvsocket: not connected")
	}
	framed := frameMessage(payload)
	return c.conn.WriteMessage(websocket.TextMessage, []byte(framed))
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
