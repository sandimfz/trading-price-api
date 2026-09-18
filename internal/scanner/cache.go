package scanner

import (
	"sync"
	"time"
)

type cacheEntry struct {
	value     Indicator
	expiresAt time.Time
}

// Cache adalah TTL cache sederhana (map + timestamp check), cukup untuk
// scope per-instance dan data short-lived.
type Cache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]cacheEntry
}

// NewCache membuat cache dengan TTL tertentu.
func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		ttl:     ttl,
		entries: make(map[string]cacheEntry),
	}
}

// Get mengambil value kalau belum expired.
func (c *Cache) Get(key string) (Indicator, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		return Indicator{}, false
	}
	return e.value, true
}

// Set menyimpan value dengan TTL dari now.
func (c *Cache) Set(key string, v Indicator) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = cacheEntry{value: v, expiresAt: time.Now().Add(c.ttl)}
}
