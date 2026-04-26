package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRateLimiter(t *testing.T, mr *miniredis.Miniredis, fixedTime time.Time) *RateLimiter {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rl := NewRateLimiter(client)
	rl.SetClock(func() time.Time { return fixedTime })
	return rl
}

func TestRateLimiter_FirstCall(t *testing.T) {
	mr := miniredis.RunT(t)
	rl := newTestRateLimiter(t, mr, time.Unix(1_000_000, 0))

	n, allowed, err := rl.Allow(context.Background(), "tenant-a", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 {
		t.Errorf("want count=1, got %d", n)
	}
	if !allowed {
		t.Error("want allowed=true on first call")
	}
}

func TestRateLimiter_AtLimit(t *testing.T) {
	mr := miniredis.RunT(t)
	rl := newTestRateLimiter(t, mr, time.Unix(1_000_000, 0))

	const limit = 5
	var n int64
	var allowed bool
	var err error

	for i := range limit {
		n, allowed, err = rl.Allow(context.Background(), "tenant-b", limit)
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
	}

	if n != limit {
		t.Errorf("want count=%d at limit, got %d", limit, n)
	}
	if !allowed {
		t.Errorf("want allowed=true at exactly the limit, got false")
	}
}

func TestRateLimiter_OverLimit(t *testing.T) {
	mr := miniredis.RunT(t)
	rl := newTestRateLimiter(t, mr, time.Unix(1_000_000, 0))

	const limit = 5
	for range limit {
		if _, _, err := rl.Allow(context.Background(), "tenant-c", limit); err != nil {
			t.Fatalf("setup call failed: %v", err)
		}
	}

	n, allowed, err := rl.Allow(context.Background(), "tenant-c", limit)
	if err != nil {
		t.Fatalf("over-limit call: unexpected error: %v", err)
	}
	if allowed {
		t.Errorf("want allowed=false over limit, got true (count=%d)", n)
	}
}

func TestRateLimiter_Increments(t *testing.T) {
	mr := miniredis.RunT(t)
	rl := newTestRateLimiter(t, mr, time.Unix(1_000_000, 0))

	const calls = 3
	for i := 1; i <= calls; i++ {
		n, _, err := rl.Allow(context.Background(), "tenant-d", 100)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if n != int64(i) {
			t.Errorf("call %d: want count=%d, got %d", i, i, n)
		}
	}
}

func TestRateLimiter_KeyHasTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	fixedTime := time.Unix(1_000_000, 0)
	rl := newTestRateLimiter(t, mr, fixedTime)

	if _, _, err := rl.Allow(context.Background(), "tenant-e", 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	key := fmt.Sprintf("rl:tenant-e:%d", fixedTime.Unix())
	ttl := mr.TTL(key)
	if ttl <= 0 {
		t.Errorf("want positive TTL on rate limit key, got %v", ttl)
	}
}

func TestRateLimiter_ResetsNextSecond(t *testing.T) {
	mr := miniredis.RunT(t)

	second1 := time.Unix(1_000_000, 0)
	second2 := time.Unix(1_000_001, 0)

	rl1 := newTestRateLimiter(t, mr, second1)
	const limit = 3
	for range limit {
		if _, _, err := rl1.Allow(context.Background(), "tenant-f", limit); err != nil {
			t.Fatalf("second-1 setup: %v", err)
		}
	}
	_, allowed, _ := rl1.Allow(context.Background(), "tenant-f", limit)
	if allowed {
		t.Fatal("want denied at end of second 1")
	}

	mr.FastForward(3 * time.Second)

	rl2 := newTestRateLimiter(t, mr, second2)
	n, allowed, err := rl2.Allow(context.Background(), "tenant-f", limit)
	if err != nil {
		t.Fatalf("second-2 call: %v", err)
	}
	if n != 1 {
		t.Errorf("want count=1 at new second, got %d", n)
	}
	if !allowed {
		t.Error("want allowed=true at new second after TTL expiry")
	}
}

func TestRateLimiter_TenantsIsolated(t *testing.T) {
	mr := miniredis.RunT(t)
	rl := newTestRateLimiter(t, mr, time.Unix(1_000_000, 0))

	const limit = 2
	for range limit + 1 {
		rl.Allow(context.Background(), "tenant-g", limit) //nolint:errcheck
	}

	n, allowed, err := rl.Allow(context.Background(), "tenant-h", limit)
	if err != nil {
		t.Fatalf("tenant-h: %v", err)
	}
	if n != 1 || !allowed {
		t.Errorf("tenant-h: want (1, true), got (%d, %v)", n, allowed)
	}
}
