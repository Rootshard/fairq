# fairq

A standalone, language-agnostic job distribution service that solves the noisy neighbor problem in multi-tenant queue-based systems. fairq sits between your producers and consumers, receiving jobs from input queues and routing them to the right output queues — while ensuring no single tenant can starve others of processing capacity.

---

## The Problem

In multi-tenant systems that share a single queue, one tenant sending a large volume of jobs causes backlogs for everyone else. Standard queues process messages in order — so if tenant A floods the queue with 1000 jobs, tenants B, C, and D wait behind them regardless of how few jobs they have.

fairq solves this by acting as a smart intermediary — tracking tenant activity and ensuring fair access to processing capacity across all tenants.

---

## How It Works

```
Producer (any language)
    ↓  writes job to Postgres, enqueues event to inbound queue
Input Queue (SQS or RabbitMQ)
    ↓  {job_id}
fairq — looks up job, updates status, routes to module queue
    ↓
Output Queue (SQS or RabbitMQ)
    ↓
Consumer (NestJS, Python, Go, anything)
```

Producers write a job record directly to Postgres, then enqueue a lightweight event `{job_id}` to the inbound queue. fairq picks up the event, looks up the full job, marks it as `processing`, decrements the tenant's pending counter in Redis, and forwards it to the correct output queue based on the job's `module` field.

fairq is protocol-agnostic — input can be SQS while output is RabbitMQ, or any combination.

---

## Inbound Event Format

Producers send a lightweight event to the inbound queue after writing the job to Postgres:

```json
{
  "job_id": "82dad7d8-b329-4b5d-afe8-7a965effe3dc"
}
```

The full job record in Postgres drives routing — fairq reads `module`, `tenant_id`, and `pattern` from there.

---

## Job Schema

```sql
CREATE TABLE jobs (
  job_id       TEXT PRIMARY KEY,
  status       TEXT NOT NULL,
  tenant_id    TEXT NOT NULL,
  module       TEXT NOT NULL,
  pattern      TEXT NOT NULL,
  queued_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  allocated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta         JSONB
);
```

| Field | Description |
|---|---|
| `job_id` | Unique identifier for the job |
| `status` | Current lifecycle status |
| `tenant_id` | Tenant this job belongs to |
| `module` | Determines which output queue the job is routed to |
| `pattern` | Passed as-is to the output queue for downstream consumer routing |
| `meta` | Arbitrary JSON payload forwarded to the consumer |

---

## Job Lifecycle

```
pending → processing → completed
                     ↘ failed
```

| Status | Description |
|---|---|
| `pending` | Job written to Postgres by producer, awaiting fairq pickup |
| `processing` | Job forwarded to output queue by fairq |
| `completed` | Downstream signalled completion |
| `failed` | Something went wrong during routing or processing |

---

## Module Config

Modules map a job's `module` field to an output queue. They are defined in [`internal/config/modules.go`](internal/config/modules.go) and their queue URLs are loaded from environment variables:

```
Currently configured modules:
  ingest  →  MODULE_INGEST_QUEUE_URL  (SQS)
  notify  →  MODULE_NOTIFY_QUEUE_URL  (RabbitMQ)
```

Adding a new module requires updating `modules.go` and setting the corresponding env var. Routing decisions are code-level configuration, not runtime configuration.

---

## API

### Get Job

```
GET /jobs/{jobID}
X-Tenant-ID: tenant_a
```

```json
{
  "job_id": "82dad7d8-b329-4b5d-afe8-7a965effe3dc",
  "status": "processing",
  "tenant_id": "tenant_a",
  "module": "notify",
  "pattern": "default",
  "queued_at": "2025-01-01T00:00:00Z",
  "updated_at": "2025-01-01T00:00:01Z",
  "allocated_at": "2025-01-01T00:00:01Z",
  "meta": {}
}
```

Tenants can only query their own jobs. A job belonging to a different tenant returns `404`.

---

## Phases of Implementation

### Phase 1 — Core Routing ✅ (in progress)
- Consume from SQS and/or RabbitMQ inbound queues (min 1, max 2)
- Persist job state to Postgres with lifecycle tracking
- Route jobs to output queues based on module → queue mapping
- Cross-protocol routing — SQS in, RMQ out and vice versa
- Pattern passthrough for downstream consumer differentiation
- Pending job count tracked in Redis per tenant
- REST endpoint to query job status (`GET /jobs/{jobID}`)
- Structured logging via `log/slog`

**Coming in Phase 1:**
- gRPC endpoint for downstream services to signal `completed` / `failed`

### Phase 2 — Tenant Hard Limits
Enforces a strict maximum number of concurrent jobs per tenant. If a tenant reaches their limit, new jobs enter a `waiting` state and are held until a slot becomes available.

### Phase 3 — Fair Queuing
Implements dynamic fairness inspired by AWS SQS Fair Queues. Instead of hard limits, fairq tracks each tenant's share of total processing capacity and deprioritizes noisy neighbors.

> **Phase 2 vs Phase 3 is configurable** via a `TENANT_MODE` flag (`hard_limit` or `fair_queue`).

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Go 1.25 |
| Input queues | AWS SQS, RabbitMQ |
| Output queues | AWS SQS, RabbitMQ |
| Job state | PostgreSQL |
| Tenant tracking | Redis |
| HTTP routing | chi |
| Config | Environment variables |

---

## Getting Started

### Prerequisites

- Go 1.21+
- Docker (for Postgres and RabbitMQ)
- Redis
- AWS credentials (if using SQS)

### 1. Start infrastructure

```bash
docker-compose up -d
```

This starts Postgres on `:5433` and RabbitMQ on `:5672` (management UI: `:15672`) with the `fairq` user pre-configured.

### 2. Create the jobs table

```bash
psql "postgres://postgres:postgres@localhost:5433/fairq?sslmode=disable" -c "
CREATE TABLE jobs (
  job_id       TEXT PRIMARY KEY,
  status       TEXT NOT NULL,
  tenant_id    TEXT NOT NULL,
  module       TEXT NOT NULL,
  pattern      TEXT NOT NULL,
  queued_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  allocated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta         JSONB
);"
```

### 3. Configure environment

```bash
cp .env.example .env
```

Edit `.env` as needed. At minimum you need one inbound queue set:

```env
PORT=8080

POSTGRES_DSN=postgres://postgres:postgres@localhost:5433/fairq?sslmode=disable

REDIS_ADDR=localhost:6379
REDIS_PASSWORD=

RABBITMQ_URL=amqp://fairq:fairq@localhost:5672/

# Inbound — set at least one
RMQ_INBOUND_QUEUE=fairq-inbound
# SQS_INBOUND_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/fairq-inbound

# Outbound — one per module in internal/config/modules.go
MODULE_NOTIFY_QUEUE_URL=fairq-notify
# MODULE_INGEST_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/fairq-ingest
```

### 4. Run

```bash
set -a && source .env && set +a
go run ./cmd/server/main.go
```

---

## Testing a Message End-to-End

Insert a job into Postgres:

```bash
psql "postgres://postgres:postgres@localhost:5433/fairq?sslmode=disable" -c "
INSERT INTO jobs (job_id, status, tenant_id, module, pattern, queued_at, updated_at, allocated_at)
VALUES (gen_random_uuid(), 'pending', 'tenant-001', 'notify', 'default', NOW(), NOW(), NOW())
RETURNING job_id;"
```

Publish an event to the inbound queue (replace the UUID):

```bash
curl -u fairq:fairq -X POST http://localhost:15672/api/exchanges/%2F/amq.default/publish \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {},
    "routing_key": "fairq-inbound",
    "payload": "{\"job_id\":\"<uuid-from-above>\"}",
    "payload_encoding": "string"
  }'
```

Check job status:

```bash
curl -H "X-Tenant-ID: tenant-001" http://localhost:8080/jobs/<uuid-from-above>
```

---

## Project Structure

```
fairq/
├── cmd/
│   └── server/
│       └── main.go          # entrypoint, wires all components
├── internal/
│   ├── model/               # Job, Status, Event types
│   ├── config/              # config loading, validation, module mapping
│   ├── db/                  # Postgres queries (get, update status)
│   ├── cache/               # Redis client, pending job counters
│   ├── consumer/            # SQS and RabbitMQ inbound consumers
│   ├── publisher/           # SQS and RabbitMQ publishers, routing logic
│   └── handler/             # HTTP handlers (GET /jobs/{jobID})
├── infra/
│   └── rabbitmq/            # RabbitMQ config and queue definitions
├── docker-compose.yml       # Local Postgres + RabbitMQ
└── .env.example
```

---

## License

MIT
