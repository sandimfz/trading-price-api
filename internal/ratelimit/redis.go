package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLimiter adalah rate limiter fixed-window per menit berbasis Redis.
// Semua instance server berbagi satu counter, sehingga kuota konsisten
// meskipun traffic terdistribusi ke beberapa instance (multi-instance safe).
type RedisLimiter struct {
	rdb *redis.Client
}

// NewRedisLimiter membuat limiter Redis.
func NewRedisLimiter(rdb *redis.Client) *RedisLimiter {
	return &RedisLimiter{rdb: rdb}
}

// scriptIncr adalah script atomik: INCR, dan EXPIRE 60s hanya pada hit pertama.
const scriptIncr = `
local c = redis.call('INCR', KEYS[1])
if c == 1 then
  redis.call('EXPIRE', KEYS[1], 60)
end
return c`

// Allow mengecek kuota key pada window menit berjalan.
func (l *RedisLimiter) Allow(ctx context.Context, key string, limit int) (Result, error) {
	window := time.Now().Unix() / 60
	redisKey := fmt.Sprintf("rl:%s:%d", key, window)

	count, err := l.rdb.Eval(ctx, scriptIncr, []string{redisKey}).Int()
	if err != nil {
		return Result{}, fmt.Errorf("redis rate limit: %w", err)
	}

	res := Result{
		Limit:     limit,
		Remaining: limit - count,
		ResetAt:   time.Unix((window+1)*60, 0),
	}
	if res.Remaining < 0 {
		res.Remaining = 0
	}
	res.Allowed = count <= limit
	return res, nil
}
