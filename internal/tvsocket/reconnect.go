package tvsocket

import (
	"context"
	"math"
	"math/rand"
	"time"
)

const (
	reconnectBase = 1 * time.Second
	reconnectMax  = 60 * time.Second
)

// RunWithReconnect menjaga koneksi upstream tetap hidup dengan exponential backoff.
// Blocking sampai ctx di-cancel.
func (c *Client) RunWithReconnect(ctx context.Context) {
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.Connect(ctx); err != nil {
			delay := backoffDelay(attempt)
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			attempt++
			continue
		}

		attempt = 0     // reset saat konek sukses
		c.ReadLoop(ctx) // blocking sampai koneksi putus
		_ = c.Close()
	}
}

func backoffDelay(attempt int) time.Duration {
	delay := float64(reconnectBase) * math.Pow(2, float64(attempt))
	jitter := rand.Float64() * float64(time.Second)
	total := time.Duration(delay + jitter)
	if total > reconnectMax {
		return reconnectMax
	}
	return total
}
