package queue

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/concurrency.lua
var concurrencyCheckScript string

type ConcurrencyChecker struct {
	script *redis.Script
	client *redis.Client
}

func NewConcurrencyChecker(client *redis.Client) *ConcurrencyChecker {
	return &ConcurrencyChecker{
		script: redis.NewScript(concurrencyCheckScript),
		client: client,
	}
}

func (c *ConcurrencyChecker) Acquire(ctx context.Context, tenantID string, maxConcurrent int) (bool, error) {
	key := inflightCounterKey(tenantID)

	result, err := c.script.Run(ctx, c.client, []string{key}, maxConcurrent).Int64()
	if err != nil {
		return false, fmt.Errorf("concurrency check: run script tenant %s: %w", tenantID, err)
	}

	return result == 1, nil
}

func (c *ConcurrencyChecker) Release(ctx context.Context, tenantID string) error {
	if err := c.client.Decr(ctx, inflightCounterKey(tenantID)).Err(); err != nil {
		return fmt.Errorf("concurrency check: release tenant %s: %w", tenantID, err)
	}
	return nil
}
