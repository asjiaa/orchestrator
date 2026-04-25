package store

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("not found")

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusComplete   JobStatus = "complete"
	StatusFailed     JobStatus = "failed"
	StatusDead       JobStatus = "dead"
)

type Job struct {
	ID                  string
	TenantID            string
	Status              JobStatus
	Attempts            int
	Payload             []byte
	InputKey            *string // source file object key
	ResultKey           *string // object key on successful processing
	WorkerID            *string // detect orphans from worker crash via instance
	CreatedAt           time.Time
	UpdatedAt           time.Time
	ProcessingStartedAt *time.Time
}

type Tenant struct {
	ID             string
	Name           string
	Plan           string
	RateLimitRPS   int
	MaxConcurrent  int
	DispatchWeight int
}

type Store interface {
	GetJob(ctx context.Context, jobID string) (*Job, error)
	ListJobsByStatus(ctx context.Context, tenantID string, status JobStatus) ([]Job, error)
	CreateJob(ctx context.Context, job Job) error
	ClaimJob(ctx context.Context, jobID string, tenantID string, workerID string) (bool, error)
	FailJob(ctx context.Context, jobID string, tenantID string) (int, JobStatus, error)
	GetStuckJobs(ctx context.Context) ([]Job, error)
	RetryJob(ctx context.Context, jobID string, tenantID string) (*Job, error)
	UpdateStatus(ctx context.Context, jobID string, status JobStatus) error
	SetResultKey(ctx context.Context, jobID string, resultKey string) error
	GetTenants(ctx context.Context) ([]Tenant, error)
	GetTenantByKeyHash(ctx context.Context, keyHash string) (*Tenant, error)
}
