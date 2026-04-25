package queue

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

func NewRedisClient(redisURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("redis: parse url: %w", err)
	}

	opts.PoolSize = 20 // minimum concurrency sum
	opts.MinIdleConns = 2

	return redis.NewClient(opts), nil
}
