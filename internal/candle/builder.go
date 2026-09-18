package candle

import (
	"sync"
	"time"

	"github.com/alen/trading-price-api/pkg/types"
)

// Intervals adalah daftar interval candle yang dibangun.
var Intervals = []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}

// Builder mengagregasi Tick menjadi candle OHLC per simbol + interval.
type Builder struct {
	mu       sync.Mutex
	active   map[string]map[string]*types.Candle
	onClosed func(types.Candle)
}

// NewBuilder membuat builder; onClosed dipanggil saat candle ditutup.
func NewBuilder(onClosed func(types.Candle)) *Builder {
	return &Builder{
		active:   make(map[string]map[string]*types.Candle),
		onClosed: onClosed,
	}
}

// Run adalah consumer loop, dijalankan sebagai goroutine terpisah.
func (b *Builder) Run(tickCh <-chan types.Tick) {
	for tick := range tickCh {
		b.handleTick(tick)
	}
}

func (b *Builder) handleTick(tick types.Tick) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.active[tick.Symbol]; !ok {
		b.active[tick.Symbol] = make(map[string]*types.Candle)
	}

	for _, interval := range Intervals {
		start := IntervalStart(tick.Timestamp, interval)
		c, exists := b.active[tick.Symbol][interval]

		if !exists || c.Timestamp != start {
			if exists {
				b.onClosed(*c) // interval lewat, tutup candle lama
			}
			b.active[tick.Symbol][interval] = &types.Candle{
				Symbol:    tick.Symbol,
				Interval:  interval,
				Open:      tick.LastPrice,
				High:      tick.LastPrice,
				Low:       tick.LastPrice,
				Close:     tick.LastPrice,
				Timestamp: start,
			}
			continue
		}

		c.High = max(c.High, tick.LastPrice)
		c.Low = min(c.Low, tick.LastPrice)
		c.Close = tick.LastPrice
	}
}

// CloseAll menutup semua candle aktif (dipanggil saat shutdown).
func (b *Builder) CloseAll() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, byInterval := range b.active {
		for _, c := range byInterval {
			b.onClosed(*c)
		}
	}
	b.active = make(map[string]map[string]*types.Candle)
}

// IntervalStart menghitung bucket start (Unix seconds) untuk interval tertentu.
func IntervalStart(ts int64, interval string) int64 {
	t := time.Unix(ts, 0).UTC()

	var start time.Time
	switch interval {
	case "1m":
		start = t.Truncate(time.Minute)
	case "5m":
		start = t.Truncate(5 * time.Minute)
	case "15m":
		start = t.Truncate(15 * time.Minute)
	case "30m":
		start = t.Truncate(30 * time.Minute)
	case "1h":
		start = t.Truncate(time.Hour)
	case "4h":
		start = t.Truncate(4 * time.Hour)
	case "1d":
		start = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	default:
		start = t
	}
	return start.Unix()
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
