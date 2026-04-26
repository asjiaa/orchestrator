package queue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/reaper.lua
var reaperScript string

const (
	visibilityTTL  = 60 * time.Second // job completion time limit
	dequeueTimeout = 5 * time.Second  // handle blocking via window on element move
)

// Queue implementation via Redis
type RedisQueue struct {
	client *redis.Client
	cc     *ConcurrencyChecker
	reaper *redis.Script
}

func NewRedisQueue(client *redis.Client, cc *ConcurrencyChecker) *RedisQueue {
	return &RedisQueue{
		client: client,
		cc:     cc,
		reaper: redis.NewScript(reaperScript),
	}
}

func (q *RedisQueue) Enqueue(ctx context.Context, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: marshal job: %w", err)
	}

	key := tenantQueueKey(job.TenantID) // tenant queue as job entrypoint
	if err := q.client.LPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("queue: enqueue to %s: %w", key, err)
	}

	return nil
}

func (q *RedisQueue) Dequeue(ctx context.Context) (*Job, error) {
	data, err := q.client.BLMove(ctx,
		keyDispatchReady,
		keyInflight,
		"LEFT", "RIGHT",
		dequeueTimeout,
	).Bytes()

	if errors.Is(err, redis.Nil) {
		return nil, ErrEmptyQueue // handle timeout on empty queue
	}
	if err != nil {
		return nil, fmt.Errorf("queue: dequeue: %w", err)
	}

	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("queue: unmarshal job: %w", err)
	}

	// Signal worker as active to reaper over expiry window
	visKey := visibilityKey(job.ID)
	if err := q.client.Set(ctx, visKey, job.TenantID, visibilityTTL).Err(); err != nil {
		return &job, fmt.Errorf("queue: set visibility key for %s: %w", job.ID, err)
	}

	return &job, nil
}

// Remove inflight job on process success
func (q *RedisQueue) Ack(ctx context.Context, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: marshal ack job: %w", err)
	}

	pipe := q.client.Pipeline()
	pipe.LRem(ctx, keyInflight, 1, data)
	pipe.Del(ctx, visibilityKey(job.ID))
	pipe.Decr(ctx, inflightCounterKey(job.TenantID))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("queue: ack job %s: %w", job.ID, err)
	}
	return nil
}

func (q *RedisQueue) Nack(ctx context.Context, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: marshal nack job: %w", err)
	}

	pipe := q.client.Pipeline() // requeue pipeline on failure
	pipe.LRem(ctx, keyInflight, 1, data)
	pipe.Del(ctx, visibilityKey(job.ID))
	pipe.Decr(ctx, inflightCounterKey(job.TenantID))
	pipe.LPush(ctx, tenantQueueKey(job.TenantID), data)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("queue: nack job %s: %w", job.ID, err)
	}

	return nil
}

func (q *RedisQueue) Depth(ctx context.Context, tenantID string) (int64, error) {
	n, err := q.client.LLen(ctx, tenantQueueKey(tenantID)).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: depth for tenant %s: %w", tenantID, err)
	}
	return n, nil
}

func (q *RedisQueue) MoveToReady(ctx context.Context, tenantID string, maxConcurrent int) error {
	acq, err := q.cc.Acquire(ctx, tenantID, maxConcurrent)
	if err != nil {
		return fmt.Errorf("queue: concurrency check tenant %s: %w", tenantID, err)
	}
	if !acq {
		return ErrAtConcurrencyLimit
	}

	result, err := q.client.LMove(ctx,
		tenantQueueKey(tenantID),
		keyDispatchReady,
		"LEFT", "RIGHT",
	).Result()

	// Drain handling
	if errors.Is(err, redis.Nil) {
		_ = q.cc.Release(ctx, tenantID)
		return nil
	}
	if err != nil {
		_ = q.cc.Release(ctx, tenantID)
		return fmt.Errorf("queue: lmove tenant %s to ready: %w", tenantID, err)
	}
	_ = result
	return nil
}

func (q *RedisQueue) ReaperRequeue(ctx context.Context, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: marshal reaper job: %w", err)
	}
	if err := q.reaper.Run(ctx, q.client, nil, data, job.TenantID).Err(); err != nil {
		return fmt.Errorf("queue: reaper requeue job %s: %w", job.ID, err)
	}
	return nil
}
