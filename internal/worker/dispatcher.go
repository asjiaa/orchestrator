package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/store"
)

const (
	dispatchInterval = 100 * time.Millisecond
	tenantRefresh    = 30 * time.Second
)

type tenantState struct {
	tenant  store.Tenant
	deficit int
}

type Dispatcher struct {
	store store.Store
	queue queue.Queue
	cc    *queue.ConcurrencyChecker
}

func NewDispatcher(s store.Store, q queue.Queue, cc *queue.ConcurrencyChecker) *Dispatcher {
	return &Dispatcher{store: s, queue: q, cc: cc}
}

func (d *Dispatcher) Run(ctx context.Context) {
	slog.InfoContext(ctx, "dispatcher started")

	states, err := d.loadTenants(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "dispatcher: initial tenant load failed", "error", err)
	}

	tickTimer := time.NewTicker(dispatchInterval)
	refreshTimer := time.NewTicker(tenantRefresh)
	defer tickTimer.Stop()
	defer refreshTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "dispatcher stopping")
			return

		case <-refreshTimer.C:
			fresh, err := d.loadTenants(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "dispatcher: tenant refresh failed", "error", err)
				continue // continue through stale state
			}
			states = d.mergeStates(states, fresh)
			slog.InfoContext(ctx, "dispatcher: tenants refreshed", "count", len(states))

		case <-tickTimer.C:
			d.tick(ctx, states)
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context, states []*tenantState) {
	for _, s := range states {
		for s.deficit >= 1 {
			depth, err := d.queue.Depth(ctx, s.tenant.ID)
			if err != nil {
				break
			}
			if depth == 0 {
				break
			}

			err = d.queue.MoveToReady(ctx, s.tenant.ID, s.tenant.MaxConcurrent)
			if errors.Is(err, queue.ErrAtConcurrencyLimit) {
				break
			}
			if err != nil {
				break
			}

			s.deficit--
			if s.deficit == 0 {
				s.deficit = s.tenant.DispatchWeight
				break
			}
		}
	}
}

func (d *Dispatcher) loadTenants(ctx context.Context) ([]*tenantState, error) {
	tenants, err := d.store.GetTenants(ctx)
	if err != nil {
		return nil, err
	}

	states := make([]*tenantState, len(tenants))
	for i, t := range tenants {
		states[i] = &tenantState{
			tenant:  t,
			deficit: t.DispatchWeight,
		}
	}
	return states, nil
}

// Preserve deficit counter
func (d *Dispatcher) mergeStates(old, fresh []*tenantState) []*tenantState {
	oldByID := make(map[string]*tenantState, len(old))
	for _, s := range old {
		oldByID[s.tenant.ID] = s
	}

	for _, s := range fresh {
		if prev, extg := oldByID[s.tenant.ID]; extg {
			s.deficit = prev.deficit
		}
	}
	return fresh
}
