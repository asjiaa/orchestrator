package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/asjiaa/orchestrator/internal/processor"
	"github.com/asjiaa/orchestrator/internal/queue"
	"github.com/asjiaa/orchestrator/internal/storage"
	"github.com/asjiaa/orchestrator/internal/store"
)

const (
	pollInterval      = 250 * time.Millisecond // sleep on empty dispatch
	processingTimeout = 45 * time.Second       // maximum job process length
)

type jobPayload struct {
	InputKey string `json:"input_key"`
}

type Worker struct {
	id        string // unique per-process
	queue     queue.Queue
	store     store.Store
	storage   *storage.Client
	processor processor.Processor
}

func New(
	id string,
	q queue.Queue,
	s store.Store,
	st *storage.Client,
	p processor.Processor,
) *Worker {
	return &Worker{
		id:        id,
		queue:     q,
		store:     s,
		storage:   st,
		processor: p,
	}
}

func (w *Worker) Run(ctx context.Context) {
	slog.InfoContext(ctx, "worker started")

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "worker stopping")
			return
		default:
		}

		job, err := w.queue.Dequeue(ctx)
		if errors.Is(err, queue.ErrEmptyQueue) {
			select {
			case <-ctx.Done():
				slog.InfoContext(ctx, "worker stopping")
				return
			case <-time.After(pollInterval):
				continue
			}
		}
		if err != nil {
			slog.ErrorContext(ctx, "dequeue error",
				"error", err,
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}

		w.process(ctx, job)
	}
}

func (w *Worker) RunN(ctx context.Context, n int) {
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			slog.InfoContext(ctx, "worker goroutine started", "id", id)
			w.Run(ctx)
			slog.InfoContext(ctx, "worker goroutine stopped", "id", id)
		}(i)
	}
	wg.Wait()
}

func (w *Worker) process(ctx context.Context, job *queue.Job) {
	log := slog.With("job_id", job.ID, "tenant_id", job.TenantID, "worker_id", w.id)

	claimed, err := w.store.ClaimJob(ctx, job.ID, job.TenantID, w.id)
	if err != nil {
		log.ErrorContext(ctx, "claim job", "error", err)
		if nackErr := w.queue.Nack(ctx, *job); nackErr != nil {
			log.ErrorContext(ctx, "nack after claim failure", "error", nackErr)
		}
		return
	}
	if !claimed {
		log.InfoContext(ctx, "job already claimed by another worker, discarding")
		return
	}

	log.Info("job claimed", "status", "processing")

	processCtx, cancel := context.WithTimeout(context.Background(), processingTimeout)
	defer cancel()

	result, err := w.runPipeline(processCtx, log, job)
	if err != nil {
		w.handleFailure(ctx, log, job, err)
		return
	}

	if err := w.store.SetResultKey(ctx, job.ID, result.Key); err != nil {
		log.ErrorContext(ctx, "set result key", "error", err)
		w.handleFailure(ctx, log, job, err)
		return
	}
	if err := w.store.UpdateStatus(ctx, job.ID, store.StatusComplete); err != nil {
		log.ErrorContext(ctx, "update status complete", "error", err)
		w.handleFailure(ctx, log, job, err)
		return
	}

	if err := w.queue.Ack(ctx, *job); err != nil {
		log.ErrorContext(ctx, "ack job", "error", err)
		return
	}

	log.InfoContext(ctx, "job complete", "result_key", result.Key)
}

func (w *Worker) handleFailure(ctx context.Context, log *slog.Logger, job *queue.Job, src error) {
	attempt, nextStatus, err := w.store.FailJob(ctx, job.ID, job.TenantID)
	if err != nil {
		log.ErrorContext(ctx, "fail job transition", "error", err)
		if nackErr := w.queue.Nack(ctx, *job); nackErr != nil {
			log.ErrorContext(ctx, "nack after failed transition", "error", nackErr)
		}
		return
	}

	log.ErrorContext(ctx, "job failure transition",
		"attempt", attempt,
		"status", nextStatus,
		"error", src,
	)

	if nextStatus == store.StatusPending {
		if nackErr := w.queue.Nack(ctx, *job); nackErr != nil {
			log.ErrorContext(ctx, "nack for retry", "attempt", attempt, "error", nackErr)
		}
		return
	}

	if ackErr := w.queue.Ack(ctx, *job); ackErr != nil {
		log.ErrorContext(ctx, "ack dead-letter job", "attempt", attempt, "error", ackErr)
	}
}

func (w *Worker) runPipeline(ctx context.Context, log *slog.Logger, job *queue.Job) (*pipelineResult, error) {
	var payload jobPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	input, err := w.storage.Get(ctx, payload.InputKey)
	if err != nil {
		return nil, fmt.Errorf("download input %s: %w", payload.InputKey, err)
	}
	defer input.Close()

	result, err := w.processor.Process(ctx, job.ID, input)
	if err != nil {
		return nil, fmt.Errorf("process: %w", err)
	}

	if err := w.storage.Put(ctx, result.Key, result.ContentType, result.Data); err != nil {
		return nil, fmt.Errorf("upload result %s: %w", result.Key, err)
	}

	return &pipelineResult{Key: result.Key}, nil
}

type pipelineResult struct {
	Key string
}
