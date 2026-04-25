package queue

import (
	"context"
	"errors"
)

var ErrEmptyQueue = errors.New("queue is empty")

var ErrAtConcurrencyLimit = errors.New("queue: tenant at concurrency limit")

type Job struct {
	ID       string
	TenantID string
	Payload  []byte
}

type Queue interface {
	Enqueue(ctx context.Context, job Job) error
	Dequeue(ctx context.Context) (*Job, error)
	Ack(ctx context.Context, job Job) error
	Nack(ctx context.Context, job Job) error
	Depth(ctx context.Context, tenantID string) (int64, error)
	MoveToReady(ctx context.Context, tenantID string, maxConcurrent int) error
	ReaperRequeue(ctx context.Context, job Job) error
}
