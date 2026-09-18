package candle

import (
	"sync"
	"testing"
	"time"

	"github.com/alen/trading-price-api/pkg/types"
)

func TestIntervalStart(t *testing.T) {
	// 2026-08-15 10:37:45 UTC
	ts := time.Date(2026, 8, 15, 10, 37, 45, 0, time.UTC).Unix()

	tests := []struct {
		interval string
		want     string
	}{
		{"1m", "2026-08-15 10:37:00"},
		{"5m", "2026-08-15 10:35:00"},
		{"15m", "2026-08-15 10:30:00"},
		{"30m", "2026-08-15 10:30:00"},
		{"1h", "2026-08-15 10:00:00"},
		{"4h", "2026-08-15 08:00:00"},
		{"1d", "2026-08-15 00:00:00"},
	}

	for _, tt := range tests {
		t.Run(tt.interval, func(t *testing.T) {
			got := time.Unix(IntervalStart(ts, tt.interval), 0).UTC().Format("2006-01-02 15:04:05")
			if got != tt.want {
				t.Fatalf("IntervalStart(%s) = %s, want %s", tt.interval, got, tt.want)
			}
		})
	}
}

func TestBuilderAggregatesOHLC(t *testing.T) {
	var mu sync.Mutex
	var closed []types.Candle
	b := NewBuilder(func(c types.Candle) {
		mu.Lock()
		closed = append(closed, c)
		mu.Unlock()
	})

	// tick dalam bucket 1m yang sama (10:37:00 UTC)
	base := time.Date(2026, 8, 15, 10, 37, 30, 0, time.UTC).Unix()
	ticks := []types.Tick{
		{Symbol: "FOREXCOM:XAUUSD", LastPrice: 100.0, Timestamp: base},
		{Symbol: "FOREXCOM:XAUUSD", LastPrice: 105.0, Timestamp: base + 1},
		{Symbol: "FOREXCOM:XAUUSD", LastPrice: 95.0, Timestamp: base + 2},
		{Symbol: "FOREXCOM:XAUUSD", LastPrice: 102.0, Timestamp: base + 3},
	}

	ch := make(chan types.Tick, len(ticks))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Run(ch)
	}()
	for _, t := range ticks {
		ch <- t
	}

	// tick di bucket berikutnya menutup candle 1m sebelumnya
	next := time.Date(2026, 8, 15, 10, 38, 0, 0, time.UTC).Unix()
	ch <- types.Tick{Symbol: "FOREXCOM:XAUUSD", LastPrice: 110.0, Timestamp: next}
	close(ch)
	wg.Wait() // pastikan semua tick sudah diproses sebelum dicek

	mu.Lock()
	defer mu.Unlock()
	if len(closed) == 0 {
		t.Fatal("tidak ada candle yang ditutup")
	}

	// cari candle interval 1m yang ditutup
	var c *types.Candle
	for i := range closed {
		if closed[i].Interval == "1m" {
			c = &closed[i]
			break
		}
	}
	if c == nil {
		t.Fatal("candle 1m tidak ditemukan")
	}

	if c.Open != 100.0 || c.High != 105.0 || c.Low != 95.0 || c.Close != 102.0 {
		t.Fatalf("OHLC salah: %+v", c)
	}
}

func TestBuilderConcurrentTicks(t *testing.T) {
	// simulasi banyak goroutine push tick secara paralel
	b := NewBuilder(func(types.Candle) {})

	const workers = 8
	const ticksPerWorker = 1000

	done := make(chan struct{})
	for w := 0; w < workers; w++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < ticksPerWorker; i++ {
				b.handleTick(types.Tick{
					Symbol:    "FOREXCOM:XAUUSD",
					LastPrice: 100.0 + float64(i),
					Timestamp: time.Now().Unix(),
				})
			}
		}()
	}
	for w := 0; w < workers; w++ {
		<-done
	}

	b.mu.Lock()
	active := len(b.active["FOREXCOM:XAUUSD"])
	b.mu.Unlock()
	if active != len(Intervals) {
		t.Fatalf("active intervals = %d, want %d", active, len(Intervals))
	}
}
