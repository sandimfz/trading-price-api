package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Result adalah hasil pengecekan rate limit.
type Result struct {
	Allowed   bool
	Limit     int
	Remaining int
	ResetAt   time.Time
}

// Limiter adalah interface token bucket per key.
type Limiter interface {
	// Allow mengecek apakah key masih punya kuota pada waktu sekarang.
	Allow(ctx context.Context, key string, limit int) (Result, error)
}

// tokenBucket adalah state bucket per key.
type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

// MemLimiter adalah token bucket in-memory (per instance).
// Untuk scale horizontal, ganti dengan implementasi Redis (Lua script).
type MemLimiter struct {
	mu    sync.Mutex
	state map[string]*tokenBucket
	now   func() time.Time
}

// NewMemLimiter membuat limiter in-memory.
func NewMemLimiter() *MemLimiter {
	return &MemLimiter{
		state: make(map[string]*tokenBucket),
		now:   time.Now,
	}
}

// Allow mengecek kuota key; limit = kapasitas bucket (token per menit).
func (m *MemLimiter) Allow(_ context.Context, key string, limit int) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	b, ok := m.state[key]
	if !ok {
		b = &tokenBucket{tokens: float64(limit), lastRefill: now}
		m.state[key] = b
	}

	// refill berdasarkan waktu berlalu
	elapsed := now.Sub(b.lastRefill).Seconds()
	rate := float64(limit) / 60.0 // token per detik
	b.tokens = minF(b.tokens+elapsed*rate, float64(limit))
	b.lastRefill = now

	res := Result{
		Limit:     limit,
		Remaining: int(b.tokens),
		ResetAt:   now.Add(time.Duration((float64(limit)-b.tokens)/rate) * time.Second),
	}
	if b.tokens >= 1 {
		b.tokens--
		res.Allowed = true
		res.Remaining = int(b.tokens)
		return res, nil
	}
	return res, nil
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
