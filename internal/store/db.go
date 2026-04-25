package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxAttempts = 3

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("store: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() {
	s.pool.Close()
}

func (s *PostgresStore) GetJob(ctx context.Context, jobID string) (*Job, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			id, tenant_id, status, attempts,
			payload, input_key, result_key, worker_id,
			created_at, updated_at, processing_started_at
		FROM jobs
		WHERE id = $1
	`, jobID)

	var j Job
	err := row.Scan(
		&j.ID, &j.TenantID, &j.Status, &j.Attempts,
		&j.Payload, &j.InputKey, &j.ResultKey, &j.WorkerID,
		&j.CreatedAt, &j.UpdatedAt, &j.ProcessingStartedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get job %s: %w", jobID, err)
	}
	return &j, nil
}

func (s *PostgresStore) ListJobsByStatus(ctx context.Context, tenantID string, status JobStatus) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id, tenant_id, status, attempts,
			payload, input_key, result_key, worker_id,
			created_at, updated_at
		FROM jobs
		WHERE tenant_id = $1
		AND   status = $2
		ORDER BY created_at DESC
	`, tenantID, status)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs tenant %s status %s: %w", tenantID, status, err)
	}
	defer rows.Close()

	jobs := make([]Job, 0)
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.ID, &j.TenantID, &j.Status, &j.Attempts,
			&j.Payload, &j.InputKey, &j.ResultKey, &j.WorkerID,
			&j.CreatedAt, &j.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("store: scan listed job: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate listed jobs: %w", err)
	}
	return jobs, nil
}

func (s *PostgresStore) CreateJob(ctx context.Context, job Job) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO jobs
			(id, tenant_id, status, payload, input_key, created_at, updated_at)
		VALUES
			($1, $2, $3, $4, $5, now(), now())
	`, job.ID, job.TenantID, StatusPending, job.Payload, job.InputKey)
	if err != nil {
		return fmt.Errorf("store: create job %s: %w", job.ID, err)
	}
	return nil
}

func (s *PostgresStore) ClaimJob(ctx context.Context, jobID string, tenantID string, workerID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET    status     = $1,
		       worker_id  = $2,
			   processing_started_at = now(),
		       updated_at = now()
		WHERE  id = $3
		AND    tenant_id = $4
		AND    status = $5
	`, StatusProcessing, workerID, jobID, tenantID, StatusPending)
	if err != nil {
		return false, fmt.Errorf("store: claim job %s tenant %s: %w", jobID, tenantID, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PostgresStore) FailJob(ctx context.Context, jobID string, tenantID string) (int, JobStatus, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET    attempts = attempts + 1,
		       status = CASE
		           WHEN attempts + 1 >= $3 THEN $1
		           ELSE $2
		       END,
		       worker_id = NULL,
		       updated_at = now()
		WHERE  id = $4
		AND    tenant_id = $5
		AND    status = $6
		RETURNING attempts, status
	`, StatusDead, StatusPending, maxAttempts, jobID, tenantID, StatusProcessing)

	var attempts int
	var status JobStatus
	if err := row.Scan(&attempts, &status); errors.Is(err, pgx.ErrNoRows) {
		return 0, "", fmt.Errorf("store: fail job %s: %w", jobID, ErrNotFound)
	} else if err != nil {
		return 0, "", fmt.Errorf("store: fail job %s: %w", jobID, err)
	}
	return attempts, status, nil
}

func (s *PostgresStore) GetStuckJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT id, tenant_id FROM jobs
        WHERE status = 'processing'
        AND   updated_at < now() - interval '75 seconds'
    `)
	if err != nil {
		return nil, fmt.Errorf("store: get stuck jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.TenantID); err != nil {
			return nil, fmt.Errorf("store: scan stuck job: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (s *PostgresStore) RetryJob(ctx context.Context, jobID string, tenantID string) (*Job, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET    attempts = 0,
		       status = $1,
		       worker_id = NULL,
		       updated_at = now()
		WHERE  id = $2
		AND    tenant_id = $3
		AND    status = $4
		RETURNING id, tenant_id, payload
	`, StatusPending, jobID, tenantID, StatusDead)

	var j Job
	if err := row.Scan(&j.ID, &j.TenantID, &j.Payload); errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, fmt.Errorf("store: retry job %s tenant %s: %w", jobID, tenantID, err)
	}
	return &j, nil
}

func (s *PostgresStore) UpdateStatus(ctx context.Context, jobID string, status JobStatus) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET    status     = $1,
		       updated_at = now()
		WHERE  id = $2
	`, status, jobID)
	if err != nil {
		return fmt.Errorf("store: update status %s %s: %w", jobID, status, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: update status %s: %w", jobID, ErrNotFound)
	}
	return nil
}

func (s *PostgresStore) SetResultKey(ctx context.Context, jobID string, resultKey string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET    result_key = $1,
		       updated_at = now()
		WHERE  id = $2
	`, resultKey, jobID)
	if err != nil {
		return fmt.Errorf("store: set result key %s: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: set result key %s: %w", jobID, ErrNotFound)
	}
	return nil
}

func (s *PostgresStore) GetTenants(ctx context.Context) ([]Tenant, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT id, name, plan, rate_limit_rps, max_concurrent, dispatch_weight
        FROM tenants
        ORDER BY id
    `)
	if err != nil {
		return nil, fmt.Errorf("store: get tenants: %w", err)
	}
	defer rows.Close()

	var tenants []Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(
			&t.ID, &t.Name, &t.Plan,
			&t.RateLimitRPS, &t.MaxConcurrent, &t.DispatchWeight,
		); err != nil {
			return nil, fmt.Errorf("store: scan tenant: %w", err)
		}
		tenants = append(tenants, t)
	}
	return tenants, rows.Err()
}

func (s *PostgresStore) GetTenantByKeyHash(ctx context.Context, keyHash string) (*Tenant, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			t.id, t.name, t.plan,
			t.rate_limit_rps, t.max_concurrent, t.dispatch_weight
		FROM   tenants  t
		JOIN   keys k ON k.tenant_id = t.id
		WHERE  k.hash = $1
	`, keyHash)

	var t Tenant
	err := row.Scan(
		&t.ID, &t.Name, &t.Plan,
		&t.RateLimitRPS, &t.MaxConcurrent, &t.DispatchWeight,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get tenant by key hash: %w", err)
	}
	return &t, nil
}
