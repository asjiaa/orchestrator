package queue

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/rl.lua
var rateLimitScript string

type RateLimiter struct {
	script *redis.Script
	client *redis.Client
	now    func() time.Time // injectable for tests
}

func NewRateLimiter(client *redis.Client) *RateLimiter {
	return &RateLimiter{
		script: redis.NewScript(rateLimitScript),
		client: client,
		now:    time.Now,
	}
}

// Test via clock override
func (rl *RateLimiter) SetClock(fn func() time.Time) {
	rl.now = fn
}

func (rl *RateLimiter) Allow(ctx context.Context, tenantID string, limitRPS int) (int64, bool, error) {
	key := fmt.Sprintf("rl:%s:%d", tenantID, rl.now().Unix())

	n, err := rl.script.Run(ctx, rl.client, []string{key}).Int64()
	if err != nil {
		return 0, false, fmt.Errorf("rate limit: run script tenant %s: %w", tenantID, err)
	}

	return n, n <= int64(limitRPS), nil
}
