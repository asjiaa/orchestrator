package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestIdempotencyStore(t *testing.T, mr *miniredis.Miniredis) *IdempotencyStore {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewIdempotencyStore(client)
}

func TestIdempotencyStore_FirstClaim(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newTestIdempotencyStore(t, mr)

	err := s.Claim(context.Background(), "tenant-a", "client-key-1", "job-id-1")
	if err != nil {
		t.Fatalf("first claim: want nil, got %v", err)
	}
}

func TestIdempotencyStore_DuplicateClaim(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newTestIdempotencyStore(t, mr)

	const originalJobID = "job-id-original"

	if err := s.Claim(context.Background(), "tenant-b", "client-key-2", originalJobID); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	err := s.Claim(context.Background(), "tenant-b", "client-key-2", "job-id-new")
	if err == nil {
		t.Fatal("second claim: want ErrDuplicateRequest, got nil")
	}

	var dup *ErrDuplicateRequest
	if !errors.As(err, &dup) {
		t.Fatalf("second claim: want *ErrDuplicateRequest, got %T: %v", err, err)
	}
	if dup.ExistingJobID != originalJobID {
		t.Errorf("duplicate: want ExistingJobID=%q, got %q", originalJobID, dup.ExistingJobID)
	}
}

func TestIdempotencyStore_DifferentKeysSameTenant(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newTestIdempotencyStore(t, mr)

	if err := s.Claim(context.Background(), "tenant-c", "key-A", "job-1"); err != nil {
		t.Fatalf("claim key-A: %v", err)
	}

	if err := s.Claim(context.Background(), "tenant-c", "key-B", "job-2"); err != nil {
		t.Fatalf("claim key-B: want nil, got %v", err)
	}
}

func TestIdempotencyStore_SameKeyDifferentTenants(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newTestIdempotencyStore(t, mr)

	const sharedClientKey = "shared-key"

	if err := s.Claim(context.Background(), "tenant-d", sharedClientKey, "job-d"); err != nil {
		t.Fatalf("tenant-d claim: %v", err)
	}

	if err := s.Claim(context.Background(), "tenant-e", sharedClientKey, "job-e"); err != nil {
		t.Fatalf("tenant-e claim: want nil, got %v", err)
	}
}

func TestIdempotencyStore_KeyHasTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newTestIdempotencyStore(t, mr)

	if err := s.Claim(context.Background(), "tenant-f", "key-ttl", "job-ttl"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	ttl := mr.TTL("idempotency:tenant-f:key-ttl")
	if ttl <= 0 {
		t.Errorf("want positive TTL on idempotency key, got %v", ttl)
	}
}

func TestIdempotencyStore_ExpiredKeyAllowsFreshClaim(t *testing.T) {
	mr := miniredis.RunT(t)
	s := newTestIdempotencyStore(t, mr)

	if err := s.Claim(context.Background(), "tenant-g", "key-exp", "job-old"); err != nil {
		t.Fatalf("initial claim: %v", err)
	}

	mr.FastForward(25 * 60 * 60 * 1e9) // 25 hours in nanoseconds

	err := s.Claim(context.Background(), "tenant-g", "key-exp", "job-new")
	if err != nil {
		t.Fatalf("post-expiry claim: want nil, got %v", err)
	}
}
