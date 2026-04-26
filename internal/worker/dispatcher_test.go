package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	iqueue "github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/store"
)

// Panic all methods to catch call not to retrieve tenants
type stubStore struct {
	tenants []store.Tenant
}

func (s *stubStore) GetTenants(_ context.Context) ([]store.Tenant, error) {
	return s.tenants, nil
}

func (s *stubStore) GetJob(_ context.Context, _ string) (*store.Job, error) {
	panic("stubStore: GetJob called — not expected in dispatcher test")
}
func (s *stubStore) ListJobsByStatus(_ context.Context, _ string, _ store.JobStatus) ([]store.Job, error) {
	panic("stubStore: ListJobsByStatus called")
}
func (s *stubStore) CreateJob(_ context.Context, _ store.Job) error {
	panic("stubStore: CreateJob called")
}
func (s *stubStore) ClaimJob(_ context.Context, _, _, _ string) (bool, error) {
	panic("stubStore: ClaimJob called")
}
func (s *stubStore) RetryJob(_ context.Context, _, _ string) (*store.Job, error) {
	panic("stubStore: RetryJob called")
}
func (s *stubStore) FailJob(_ context.Context, _, _ string) (int, store.JobStatus, error) {
	panic("stubStore: FailJob called")
}
func (s *stubStore) GetStuckJobs(_ context.Context) ([]store.Job, error) {
	panic("stubStore: GetStuckJobs called")
}
func (s *stubStore) UpdateStatus(_ context.Context, _ string, _ store.JobStatus) error {
	panic("stubStore: UpdateStatus called")
}
func (s *stubStore) SetResultKey(_ context.Context, _, _ string) error {
	panic("stubStore: SetResultKey called")
}
func (s *stubStore) GetTenantByKeyHash(_ context.Context, _ string) (*store.Tenant, error) {
	panic("stubStore: GetTenantByKeyHash called")
}

func seedLane(t *testing.T, client *redis.Client, tenantID string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		job := iqueue.Job{
			ID:       fmt.Sprintf("%s-job-%04d", tenantID, i),
			TenantID: tenantID,
			Payload:  []byte(`{"input_key":"test"}`),
		}
		data, err := json.Marshal(job)
		if err != nil {
			t.Fatalf("seed: marshal job: %v", err)
		}
		// Emulate enqueue behavior
		if err := client.LPush(ctx, fmt.Sprintf("queue:tenant:%s", tenantID), data).Err(); err != nil {
			t.Fatalf("seed: lpush: %v", err)
		}
	}
}

func countByTenant(t *testing.T, client *redis.Client) map[string]int {
	t.Helper()
	ctx := context.Background()

	entries, err := client.LRange(ctx, "queue:dispatch_ready", 0, -1).Result()
	if err != nil {
		t.Fatalf("count: lrange dispatch_ready: %v", err)
	}

	counts := make(map[string]int)
	for _, raw := range entries {
		var job iqueue.Job
		if err := json.Unmarshal([]byte(raw), &job); err != nil {
			t.Fatalf("count: unmarshal job: %v", err)
		}
		counts[job.TenantID]++
	}
	return counts
}

func TestDispatcherFairness(t *testing.T) {
	const (
		enterpriseID   = "enterprise-tenant-id"
		freeID         = "free-tenant-id"
		enterpriseJobs = 60
		freeJobs       = 20
		runDuration    = 5 * time.Second
		minFreeRatio   = 0.25 // free must receive ratio of dispatched jobs
	)

	tenants := []store.Tenant{
		{
			ID:             enterpriseID,
			Name:           "Enterprise",
			Plan:           "enterprise",
			DispatchWeight: 3,
			MaxConcurrent:  1000, // increase max concurrency limit
			RateLimitRPS:   200,
		},
		{
			ID:             freeID,
			Name:           "Free",
			Plan:           "free",
			DispatchWeight: 1,
			MaxConcurrent:  1000,
			RateLimitRPS:   5,
		},
	}

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	seedLane(t, client, enterpriseID, enterpriseJobs)
	seedLane(t, client, freeID, freeJobs)

	cc := iqueue.NewConcurrencyChecker(client)
	q := iqueue.NewRedisQueue(client, cc)
	s := &stubStore{tenants: tenants}
	d := NewDispatcher(s, q, cc)

	ctx, cancel := context.WithTimeout(context.Background(), runDuration)
	defer cancel()

	d.Run(ctx)

	counts := countByTenant(t, client)
	totalDispatched := counts[enterpriseID] + counts[freeID]

	t.Logf("dispatched: enterprise=%d free=%d total=%d",
		counts[enterpriseID], counts[freeID], totalDispatched)

	if totalDispatched == 0 {
		t.Fatal("fail: no jobs were dispatched at all — dispatcher may not be running")
	}

	freeCount := counts[freeID]
	if freeCount == 0 {
		t.Fatal("fail: free tenant received zero dispatched jobs — possible starvation")
	}

	freeRatio := float64(freeCount) / float64(totalDispatched)
	if freeRatio < minFreeRatio {
		t.Fatalf("fail: free tenant ratio %.2f is below minimum %.2f (enterprise=%d free=%d)",
			freeRatio, minFreeRatio, counts[enterpriseID], freeCount)
	}

	t.Logf("pass: free ratio %.2f >= %.2f", freeRatio, minFreeRatio)
}
