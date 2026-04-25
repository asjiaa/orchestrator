package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const idempotencyTTL = 24 * time.Hour // deduplication window

type IdempotencyStore struct {
	client *redis.Client
}

func NewIdempotencyStore(client *redis.Client) *IdempotencyStore {
	return &IdempotencyStore{client: client}
}

type ErrDuplicateRequest struct {
	ExistingJobID string
}

func (e *ErrDuplicateRequest) Error() string {
	return fmt.Sprintf("idempotency: duplicate request, existing job_id=%s", e.ExistingJobID)
}

func (s *IdempotencyStore) Claim(ctx context.Context, tenantID, clientKey, jobID string) error {
	key := idempotencyKey(tenantID, clientKey)

	set, err := s.client.SetNX(ctx, key, jobID, idempotencyTTL).Result()
	if err != nil {
		return fmt.Errorf("idempotency: claim key %s: %w", key, err)
	}
	if set {
		return nil // absent key
	}

	existing, err := s.client.Get(ctx, key).Result()

	if err != nil {
		return fmt.Errorf("idempotency: read existing key %s: %w", key, err)
	}

	return &ErrDuplicateRequest{ExistingJobID: existing}
}

func idempotencyKey(tenantID, clientKey string) string {
	return fmt.Sprintf("idempotency:%s:%s", tenantID, clientKey)
}
