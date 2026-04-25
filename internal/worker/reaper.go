package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/store"
)

const (
	reaperInterval = 15 * time.Second
)

type Reaper struct {
	store store.Store
	queue queue.Queue
	log   *slog.Logger
}

func NewReaper(s store.Store, q queue.Queue) *Reaper {
	return &Reaper{
		store: s,
		queue: q,
		log:   slog.Default(),
	}
}

func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()

	r.log.Info("reaper started", "interval", reaperInterval)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reaper stopping")
			return
		case <-ticker.C:
			r.reap(ctx)
		}
	}
}

func (r *Reaper) reap(ctx context.Context) {
	stuck, err := r.store.GetStuckJobs(ctx)
	if err != nil {
		r.log.Error("reaper: get stuck jobs", "error", err)
		return
	}
	if len(stuck) == 0 {
		return
	}

	r.log.Info("reaper: found stuck jobs", "count", len(stuck))

	for _, orphan := range stuck {
		r.requeueOrphan(ctx, orphan)
	}
}

func (r *Reaper) requeueOrphan(ctx context.Context, orphan store.Job) {
	job, err := r.store.GetJob(ctx, orphan.ID)
	if err != nil {
		r.log.Error("reaper: get job",
			"job_id", orphan.ID,
			"tenant_id", orphan.TenantID,
			"error", err,
		)
		return
	}

	// Prevent reprocessing on terminal jobs
	switch job.Status {
	case store.StatusComplete, store.StatusDead:
		r.log.Info("reaper: skipping terminal job",
			"job_id", job.ID,
			"tenant_id", job.TenantID,
			"status", job.Status,
		)
		return
	}

	queueJob := queue.Job{
		ID:       job.ID,
		TenantID: job.TenantID,
		Payload:  job.Payload,
	}
	if err := r.queue.ReaperRequeue(ctx, queueJob); err != nil {
		r.log.Error("reaper: redis requeue",
			"job_id", job.ID,
			"tenant_id", job.TenantID,
			"attempt", job.Attempts,
			"error", err,
		)
		return
	}

	if err := r.store.UpdateStatus(ctx, job.ID, store.StatusPending); err != nil {
		r.log.Error("reaper: update status pending",
			"job_id", job.ID,
			"tenant_id", job.TenantID,
			"attempt", job.Attempts,
			"error", err,
		)
		return
	}

	r.log.Info("reaper: requeued orphan",
		"job_id", job.ID,
		"tenant_id", job.TenantID,
		"attempt", job.Attempts,
	)
}
