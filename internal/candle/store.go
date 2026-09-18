package candle

import (
	"sync"

	"github.com/alen/trading-price-api/pkg/types"
)

// ringTickSize adalah jumlah tick mentah yang disimpan per simbol.
const ringTickSize = 3600

// Store adalah penyimpanan in-memory: ring buffer tick terbaru + candle yang sudah ditutup.
type Store struct {
	mu      sync.RWMutex
	ticks   map[string][]types.Tick              // symbol -> ring buffer
	tickIdx map[string]int                       // symbol -> index tulis berikutnya
	candles map[string]map[string][]types.Candle // symbol -> interval -> closed candles (terbaru di akhir)
}

// NewStore membuat store baru.
func NewStore() *Store {
	return &Store{
		ticks:   make(map[string][]types.Tick),
		tickIdx: make(map[string]int),
		candles: make(map[string]map[string][]types.Candle),
	}
}

// PushTick menambah tick ke ring buffer simbol.
func (s *Store) PushTick(tick types.Tick) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ring, ok := s.ticks[tick.Symbol]
	if !ok {
		ring = make([]types.Tick, ringTickSize)
		s.ticks[tick.Symbol] = ring
	}

	idx := s.tickIdx[tick.Symbol]
	ring[idx] = tick
	s.tickIdx[tick.Symbol] = (idx + 1) % ringTickSize
}

// RecentTicks mengembalikan tick terbaru secara berurutan (paling lama dulu).
func (s *Store) RecentTicks(symbol string, n int) []types.Tick {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ring, ok := s.ticks[symbol]
	if !ok {
		return nil
	}
	if n <= 0 || n > ringTickSize {
		n = ringTickSize
	}

	idx := s.tickIdx[symbol]
	out := make([]types.Tick, 0, n)
	for i := 0; i < n; i++ {
		idx = (idx - 1 + ringTickSize) % ringTickSize
		t := ring[idx]
		if t.Timestamp == 0 {
			break
		}
		out = append(out, t)
	}
	// balik supaya urut kronologis
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// AddCandle menyimpan candle yang sudah ditutup.
func (s *Store) AddCandle(c types.Candle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	byInterval, ok := s.candles[c.Symbol]
	if !ok {
		byInterval = make(map[string][]types.Candle)
		s.candles[c.Symbol] = byInterval
	}
	byInterval[c.Interval] = append(byInterval[c.Interval], c)
}

// RecentCandles mengembalikan n candle terakhir untuk simbol+interval (terbaru di akhir).
func (s *Store) RecentCandles(symbol, interval string, n int) []types.Candle {
	s.mu.RLock()
	defer s.mu.RUnlock()

	byInterval, ok := s.candles[symbol]
	if !ok {
		return nil
	}
	list := byInterval[interval]
	if n <= 0 || n > len(list) {
		n = len(list)
	}
	out := make([]types.Candle, n)
	copy(out, list[len(list)-n:])
	return out
}
