# Orchestrator

[![Go](https://img.shields.io/badge/Go-%E2%89%A51.25-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Redis](https://img.shields.io/badge/Redis-%E2%89%A57.4-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-%E2%89%A516-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Engine%20%2B%20Compose%20v2-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Object Storage](https://img.shields.io/badge/Object%20Storage-MinIO%20(latest)%20%7C%20AWS%20S3-3ECF8E?logo=minio&logoColor=white)](https://min.io/)

Orchestrator is a self-hosted job processing service for image workloads, focused on **reliability** and **performance** under [high throughput](./scripts/load.js).

Orchestrator adopts [fault-tolerance](https://kafka.apache.org/24/streams/architecture/#local-state-stores) for **at-least-once delivery** and [multi-tenancy](https://pulsar.apache.org/docs/next/concepts-multi-tenancy/) support for **fair scheduling**, as **production queue semantics** inspired by open-source distributed event streamers like [Apache Kafka](https://kafka.apache.org/) and [Apache Pulsar](https://pulsar.apache.org/).

## Architecture

### Interfaces

Orchestrator keeps application boundaries explicit through three interfaces:

```go
type Queue interface {
    Enqueue(ctx context.Context, job Job) error
    Dequeue(ctx context.Context) (*Job, error)
    Ack(ctx context.Context, job Job) error
    Nack(ctx context.Context, job Job) error
    Depth(ctx context.Context, tenantID string) (int64, error)
    MoveToReady(ctx context.Context, tenantID string, maxConcurrent int) error
    ReaperRequeue(ctx context.Context, job Job) error
}
```

```go
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
```

```go
type Processor interface {
    Process(ctx context.Context, jobID string, input io.Reader) (Result, error)
}
```

At runtime:
- `internal/api` depends on `Store + Queue + Storage`
- `internal/worker` depends on `Store + Queue + Processor + Storage`
- concrete implementations are `PostgresStore`, `RedisQueue`, `GovipsProcessor`, and S3-compatible storage client

### Request Path

```mermaid
flowchart LR
    C[Client] --> A[POST /jobs]
    A --> M1[Auth middleware]
    M1 --> M2[Rate-limit middleware]
    M2 --> H[CreateJob handler]
    H --> DB1[(PostgreSQL jobs: pending)]
    H --> S3[(MinIO/AWS S3: input object)]
    H --> TL[["Redis tenant lane\nqueue:tenant:{tenant_id}"]]
    TL --> R[202 Accepted + job_id]
```

Design choices:
- **Middleware before writes**: auth + rate checks happen before DB/object/queue mutation, reducing bad writes under abusive traffic.
- **Database-first insertion**: job row is written before queue insertion so every queued payload has canonical metadata.
- **Tenant lane enqueue**: API does not enqueue directly into the global ready queue, instead it writes to per-tenant lanes to preserve fairness at dispatch time.
- **Idempotency support**: `X-Idempotency-Key` claims duplicates early and returns existing `job_id` to avoid duplicate work.

### Dispatch Process

```mermaid
flowchart LR
    subgraph Lanes[Per-tenant Redis lanes]
      T1[[tenant A lane]]
      T2[[tenant B lane]]
      T3[[tenant C lane]]
    end
    Lanes --> D[Dispatcher goroutine<br/>tick=100ms]
    D --> CC{Concurrency<br/>available?}
    CC -->|yes| RQ[[dispatch_ready queue]]
    CC -->|no| D
    RQ --> W[Worker Dequeue]
    W --> IF[[inflight queue + visibility key]]
    IF --> P[Process image]
    P -->|success| ACK[Ack and release inflight]
    P -->|error| NACK[Nack and requeue to tenant lane]
```

DRR (Deficit Round Robin) tracing:

```mermaid
sequenceDiagram
    participant D as Dispatcher
    participant A as Tenant A (weight=1)
    participant B as Tenant B (weight=2)
    participant C as Tenant C (weight=3)
    participant R as dispatch_ready
    D->>A: deficit=1, move 1 job
    A-->>R: +1
    D->>B: deficit=2, move up to 2 jobs
    B-->>R: +2
    D->>C: deficit=3, move up to 3 jobs
    C-->>R: +3
    Note over D: each tenant capped by max_concurrent
    Note over D: deficit replenished by dispatch_weight
```

Design choices:
- **Two-stage queueing**: tenant lane -> dispatch queue separates fairness policy from worker consumption.
- **Weighted fairness**: dispatcher uses per-tenant `dispatch_weight` as DRR credit, enabling plan-tier priority without starvation.
- **Concurrency guard at dispatch**: workers remain simple while per-tenant concurrency is enforced before entering ready queue.
- **At-least-once processing**: `BLMOVE` to inflight + `Ack/Nack` supports retry and crash recovery semantics.

### Storage Responsibilities

```mermaid
flowchart TD
    API[API Handler] --> PG1[(PostgreSQL)]
    API --> OS1[(MinIO/AWS S3)]
    API --> RD1[(Redis)]
    PG1 -->|job id + state| RD1

    W[Worker] --> RD1
    W --> OS1
    W --> PG1

    RP[Reaper] --> PG1
    RP --> RD1
```

Responsibility split:
- **PostgreSQL (source of truth)**: tenants, API keys, job lifecycle state (`pending/processing/complete/dead`), attempts, ownership, timestamps.
- **Redis (execution state)**: tenant lanes, dispatch queue, inflight queue, visibility keys, concurrency counters, rate-limit counters, idempotency keys.
- **MinIO/AWS S3 (payload state)**: immutable input objects and generated output objects (`input_key`, `result_key`).

Write ordering and reconciliation strategy:
- **Create path**: DB row (`pending`) -> object upload -> tenant-lane enqueue. Queue payload references an existing job ID rather than being the primary record.
- **Success path**: upload result object -> write `result_key` in DB -> mark `complete` -> `Ack` queue entry.
- **Failure path**: `FailJob` increments attempts and flips status (`pending` or `dead`) -> `Nack`/`Ack` queue accordingly.
- **Crash/stall recovery**: reaper scans stale `processing` rows from DB, requeues in Redis, then writes `pending` in DB for reconciliation.

## Features

### At-least-once delivery

Jobs are durably retried after transient failures or worker interruption. Duplicates are possible and handled at the application boundary.

See **Dispatch Process** and **Storage Responsibilities** for dequeue/inflight/ack-nack and reaper reconciliation details.

### Fair Multi-tenant scheduling

Each tenant continuously makes progress under load, while plan tiers receive proportionally higher dispatch share.

See **Request Path** and **Dispatch Process** for per-tenant laneing and DRR-weighted dispatch behavior.

### Graceful Failure Handling

Failures are bounded by explicit retry limits, transition into observable terminal states, and remain operator-recoverable.

See **Storage Responsibilities** for fail transitions, retry limits, and crash recovery write ordering.

## Usage

### Prerequisites

- [Docker Compose](https://docs.docker.com/compose/install)

### Local

#### 1) Start the stack

```bash
docker compose -f infra/compose.local.yaml up --build -d
```

This starts:

- `cache` (Redis) on `6379`
- `db` (Postgres) on `5432`
- `minio` (S3 API) on `9000`, console on `9001`
- `api` on `8080`
- `worker` (includes pprof listener on `6060` internally)

#### 2) Apply database schema

```bash
docker exec -i orchestrator-db-1 psql -U orchestrator -d orchestrator < db/schema.sql
```

#### 3) Seed tenants and API keys

```bash
docker compose -f infra/compose.local.yaml run --rm seed
```

If `seed` is not defined in your compose flow, run:

```bash
docker compose -f infra/compose.local.yaml run --rm \
  -e DATABASE_URL=postgres://orchestrator:orchestrator@db:5432/orchestrator \
  api go run ./cmd/seed
```

Seeded plaintext demo keys:

- `free-tenant-api-key-plaintext`
- `pro-tenant-api-key-plaintext`
- `enterprise-tenant-api-key-plaintext`

#### 4) Submit a job

```bash
curl -X POST "http://localhost:8080/jobs" \
  -H "Authorization: Bearer enterprise-tenant-api-key-plaintext" \
  -F "file=@scripts/assets/test.jpg"
```

Example response:

```json
{
  "job_id": "4f9adf34-97e8-4f7a-8ab4-949e99a6f94f",
  "status": "pending"
}
```

#### 5) Check job status

```bash
curl -H "Authorization: Bearer enterprise-tenant-api-key-plaintext" \
  "http://localhost:8080/jobs/<job_id>"
```

When complete, response includes a presigned `result_url`.

### Deployment

Production compose is defined in `infra/compose.prod.yaml` and expects runtime values from environment variables (typically a `.env` file in deployment).

#### 1) AWS console-side setup

Before bringing up `compose.prod`, create and collect:

- **S3 bucket** (e.g. `orchestrator-prod-assets`) in your target region
- **IAM credentials** for S3 object access (scoped to the bucket with operations: `GetObject`, `PutObject`, `ListBucket`)
- **PostgreSQL instance** (RDS or equivalent) and connection string
- **Redis endpoint** (ElastiCache/Valkey-compatible Redis or managed Redis)
- **Network access** so API/worker hosts can reach Redis, Postgres, and S3

#### 2) Configure environment variables

Create a deployment `.env` and supply:

- `REDIS_URL`
  - Redis connection URI used by both API and worker
  - Example: `redis://<host>:6379/0`
- `DATABASE_URL`
  - Postgres DSN used by both services
  - Example: `postgres://<user>:<pass>@<host>:5432/<db>?sslmode=require`
- `S3_BUCKET`
  - Bucket storing input and output objects
- `AWS_REGION`
  - Region containing the S3 bucket
- `AWS_ACCESS_KEY_ID`
  - IAM access key for object operations
- `AWS_SECRET_ACCESS_KEY`
  - IAM secret key paired with access key

Additional variables used by infrastructure services in `compose.prod`:

- `POSTGRES_USER`: `<user>` in Postgres DSN
- `POSTGRES_PASSWORD`: `<pass>` in Postgres DSN
- `POSTGRES_DB`: `<db>` in Postgres DSN
- `WORKER_CONCURRENCY`: number of worker goroutines processing jobs concurrently per worker container
- `VIPS_WORKERS`: libvips internal worker count used by image processing threads inside each container

**Security**

- Avoid committing plaintext secrets in repo-managed `.env` files.
- For ECS deployments, inject configuration through task-level environment files and secret sources (for example AWS Secrets Manager or SSM Parameter Store).
- Reference: [Pass environment variables to an Amazon ECS container](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/use-environment-file.html).

#### 3) Recommended worker constants

`compose.prod` pins `VIPS_WORKERS: "1"` to avoid libvips CPU oversubscription per container and exposes `WORKER_CONCURRENCY` for queue parallelism. Recommended starting points:

- **2 vCPU worker host**: `WORKER_CONCURRENCY=2`, `VIPS_WORKERS=1`
- **4 vCPU worker host**: `WORKER_CONCURRENCY=4`, `VIPS_WORKERS=1`
- **8 vCPU worker host**: `WORKER_CONCURRENCY=6-8`, `VIPS_WORKERS=1`

Rule of thumb: scale throughput first by worker replicas, then increase `WORKER_CONCURRENCY` conservatively while monitoring queue latency, CPU saturation, and job failure/retry rates.

#### 4) Bring up production compose

```bash
docker compose -f infra/compose.prod.yaml --env-file .env up --build -d
```

## Benchmarks

`scripts/load.js` validates both performance and queue semantics under mixed tenant traffic, fairness contention, and failure recovery.

Metrics with thresholds are used to verify:

- `enterprise_rate_limited`: enterprise traffic should not be unexpectedly throttled during normal load profile.
- `pro_rate_limited`: pro traffic should also pass steady-state load without unintended rate limiting.
- `free_burst_rate_limited`: free-tier burst traffic should be rate-limited, confirming tenant plan enforcement.
- `job_accepted`: a healthy portion of submissions should still be admitted under stress, indicating service remains usable.
- `job_end_to_end_load_ms`: total completion latency (submit -> terminal status) under mixed throughput load.
- `queue_latency_load_ms`: queue waiting time (`created_at -> processing_started_at`) under mixed throughput load.
- `job_end_to_end_fairness_ms`: completion latency under fairness-focused contention scenarios.
- `queue_latency_fairness_ms`: queue wait under fairness contention, used to detect starvation or scheduler imbalance.
- `fairness_free_ratio`: checks that free-tier jobs still complete at a minimum share during concurrent higher-tier traffic.
- `recovery_job_accepted`: verifies API admission remains healthy during recovery-phase scenarios (including worker interruption tests).

### How to run

```bash
# Baseline benchmark profile (all scenarios/thresholds from scripts/load.js)
k6 run scripts/load.js

# Recovery benchmark wrapper (includes worker SIGKILL fault injection)
./scripts/load.sh baseline
```
