package redisx

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	RDB *redis.Client
}

func New(addr, password string, db int) *Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     50,
		MinIdleConns: 10,
	})
	return &Client{RDB: rdb}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.RDB.Ping(ctx).Err()
}
