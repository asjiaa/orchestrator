package queue

import (
	"context"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestConcurrencyChecker(t *testing.T, mr *miniredis.Miniredis) *ConcurrencyChecker {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewConcurrencyChecker(client)
}

func TestConcurrencyChecker_UnderCap(t *testing.T) {
	mr := miniredis.RunT(t)
	cc := newTestConcurrencyChecker(t, mr)

	acq, err := cc.Acquire(context.Background(), "tenant-a", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acq {
		t.Error("want acquire=true when under cap")
	}
}

func TestConcurrencyChecker_AtCap(t *testing.T) {
	mr := miniredis.RunT(t)
	cc := newTestConcurrencyChecker(t, mr)

	const cap = 3
	for i := range cap - 1 {
		acq, err := cc.Acquire(context.Background(), "tenant-b", cap)
		if err != nil {
			t.Fatalf("setup acquire %d: %v", i, err)
		}
		if !acq {
			t.Fatalf("setup acquire %d: want true, got false", i)
		}
	}

	acq, err := cc.Acquire(context.Background(), "tenant-b", cap)
	if err != nil {
		t.Fatalf("at-cap acquire: %v", err)
	}
	if !acq {
		t.Error("want acquire=true when taking the last available slot")
	}
}

func TestConcurrencyChecker_OverCap(t *testing.T) {
	mr := miniredis.RunT(t)
	cc := newTestConcurrencyChecker(t, mr)

	const cap = 2
	for i := range cap {
		if _, err := cc.Acquire(context.Background(), "tenant-c", cap); err != nil {
			t.Fatalf("setup acquire %d: %v", i, err)
		}
	}

	acq, err := cc.Acquire(context.Background(), "tenant-c", cap)
	if err != nil {
		t.Fatalf("over-cap acquire: %v", err)
	}
	if acq {
		t.Error("want acquire=false when over cap")
	}

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	val, err := client.Get(context.Background(), "inflight:tenant-c").Int64()
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if val != cap {
		t.Errorf("want counter=%d after rollback, got %d", cap, val)
	}
}

func TestConcurrencyChecker_Release(t *testing.T) {
	mr := miniredis.RunT(t)
	cc := newTestConcurrencyChecker(t, mr)

	const cap = 1

	acq, err := cc.Acquire(context.Background(), "tenant-d", cap)
	if err != nil || !acq {
		t.Fatalf("initial acquire: acq=%v err=%v", acq, err)
	}

	acq, err = cc.Acquire(context.Background(), "tenant-d", cap)
	if err != nil {
		t.Fatalf("over-cap acquire: %v", err)
	}
	if acq {
		t.Fatal("want acquire=false when cap full")
	}

	if err := cc.Release(context.Background(), "tenant-d"); err != nil {
		t.Fatalf("release: %v", err)
	}

	acq, err = cc.Acquire(context.Background(), "tenant-d", cap)
	if err != nil {
		t.Fatalf("post-release acquire: %v", err)
	}
	if !acq {
		t.Error("want acquire=true after release")
	}
}

func TestConcurrencyChecker_TenantsIsolated(t *testing.T) {
	mr := miniredis.RunT(t)
	cc := newTestConcurrencyChecker(t, mr)

	const cap = 2

	for range cap {
		cc.Acquire(context.Background(), "tenant-e", cap) //nolint:errcheck
	}
	acq, _ := cc.Acquire(context.Background(), "tenant-e", cap)
	if acq {
		t.Fatal("tenant-e should be at cap")
	}

	acq, err := cc.Acquire(context.Background(), "tenant-f", cap)
	if err != nil {
		t.Fatalf("tenant-f acquire: %v", err)
	}
	if !acq {
		t.Error("want acquire=true for tenant-f, independent of tenant-e")
	}
}

func TestConcurrencyChecker_ConcurrentCallers(t *testing.T) {
	mr := miniredis.RunT(t)
	cc := newTestConcurrencyChecker(t, mr)

	const (
		goroutines = 20
		cap        = 10
	)

	var (
		mu       sync.Mutex
		acquired int
		wg       sync.WaitGroup
	)

	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			acq, err := cc.Acquire(context.Background(), "tenant-g", cap)
			if err != nil {
				t.Errorf("concurrent acquire: %v", err)
				return
			}
			if acq {
				mu.Lock()
				acquired++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if acquired != cap {
		t.Errorf("want exactly %d successful acquires, got %d", cap, acquired)
	}
}
