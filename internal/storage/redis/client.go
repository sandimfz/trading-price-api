package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client membungkus go-redis client.
type Client struct {
	rdb *redis.Client
}

// Raw mengembalikan client go-redis bawaan (untuk komponen seperti rate limiter).
func (c *Client) Raw() *redis.Client {
	return c.rdb
}

// Connect membuat koneksi Redis.
func Connect(ctx context.Context, addr, password string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return &Client{rdb: rdb}, nil
}

// Close menutup koneksi Redis.
func (c *Client) Close() error {
	return c.rdb.Close()
}
