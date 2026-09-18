package apikey

import (
	"sync"
	"time"
)

type keyCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]keyCacheEntry
}

type keyCacheEntry struct {
	key       *Key
	expiresAt time.Time
}

func newKeyCache(ttl time.Duration) *keyCache {
	return &keyCache{
		ttl:     ttl,
		entries: make(map[string]keyCacheEntry),
	}
}

func (c *keyCache) Get(hashed string) (*Key, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	e, ok := c.entries[hashed]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.key, true
}

func (c *keyCache) Set(hashed string, k *Key, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[hashed] = keyCacheEntry{key: k, expiresAt: time.Now().Add(ttl)}
}

func (c *keyCache) Delete(hashed string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, hashed)
}
