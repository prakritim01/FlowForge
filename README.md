# Distributed Task Queue

A production-style backend infrastructure project that implements a Redis-backed distributed background job queue with FastAPI, worker processes, retries, priority queues, delayed execution, dead-letter handling, status tracking, and live WebSocket monitoring.

This repository is designed as a professional engineering portfolio project. It is intentionally scoped like a simplified Celery/RQ system: small enough to understand in an interview, but complete enough to demonstrate real backend design decisions.

## Why This Project Matters

Most production systems eventually need background processing for slow, unreliable, or asynchronous work: emails, webhooks, report generation, imports, model inference, billing events, and cleanup jobs. A task queue sits at the center of those workflows and has to handle ordering, failures, retries, observability, and operational safety.

This project shows those concerns in a clean Python implementation with Redis as the queueing and state backend.

## Architecture Overview

```text
Client
  |
  | REST / WebSocket
  v
FastAPI API  -----> Redis
  |                 |-- priority lists: high, medium, low
  |                 |-- delayed sorted set
  |                 |-- job state records
  |                 |-- event stream/pubsub
  v
Dashboard

Worker processes
  |
  | poll Redis priority queues
  v
Task executor -> success, retry with backoff, or dead-letter
```

The API accepts job submissions and stores durable job metadata in Redis. Workers run separately with `python -m app.worker`, consume high-priority jobs before medium and low, update status through the job lifecycle, and publish events used by WebSocket clients and the dashboard.

## Features

- REST API for job submission, job lookup, recent jobs, queue stats, and health checks.
- Redis-backed priority queues: `high`, `medium`, and `low`.
- Delayed execution using a Redis sorted set.
- Worker process with configurable concurrency.
- Retry handling with exponential backoff.
- Dead-letter queue for permanently failed jobs.
- Job lifecycle states: `queued`, `delayed`, `running`, `succeeded`, `failed`, `retrying`, `dead`.
- Rich job metadata including timestamps, attempts, payload, priority, and error messages.
- WebSocket endpoint for live job updates.
- Browser dashboard at `/dashboard`.
- Structured JSON logging with `structlog`.
- Docker and Docker Compose setup.
- Pytest suite using isolated fake Redis.
- GitHub Actions CI for linting and tests.

## Tech Stack

- Python 3.11+
- FastAPI
- Redis
- WebSockets
- Docker / Docker Compose
- Pydantic v2
- Pytest
- Fakeredis
- Ruff
- Structlog

## Folder Structure

```text
distributed-task-queue/
  app/
    __init__.py
    main.py
    config.py
    models.py
    redis_client.py
    queue.py
    worker.py
    events.py
    dashboard.py
    logging_config.py
  tests/
    conftest.py
    test_api.py
    test_queue.py
    test_worker.py
    test_retries.py
    test_priority.py
  .github/
    workflows/
      ci.yml
  Dockerfile
  docker-compose.yml
  pyproject.toml
  README.md
  .env.example
  .gitignore
```

## Setup

Create a virtual environment and install the project:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

Start Redis locally:

```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

Run the API:

```bash
uvicorn app.main:app --reload
```

Run a worker in another terminal:

```bash
python -m app.worker
```

## Docker Setup

Run the full stack:

```bash
docker compose up --build
```

Services:

- API: `http://localhost:8000`
- Dashboard: `http://localhost:8000/dashboard`
- Redis: `localhost:6379`
- Worker: separate Compose service running `python -m app.worker`

## API Examples

Submit a job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome"
    },
    "priority": "high",
    "delay_seconds": 0,
    "max_retries": 3
  }'
```

Submit a delayed job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "task_name": "generate_report",
    "payload": {"report_id": "rpt_123"},
    "priority": "medium",
    "delay_seconds": 30,
    "max_retries": 2
  }'
```

Get job status:

```bash
curl http://localhost:8000/jobs/<job_id>
```

List recent jobs:

```bash
curl http://localhost:8000/jobs
```

Queue statistics:

```bash
curl http://localhost:8000/queues/stats
```

Health check:

```bash
curl http://localhost:8000/health
```

## Worker Behavior

Workers poll Redis queues in strict priority order:

1. `high`
2. `medium`
3. `low`

Before execution, a job is marked `running` and its attempt count is incremented. The included `SimulatedTaskExecutor` is intentionally deterministic and testable:

- Normal jobs succeed.
- Jobs with `task_name: "always_fail"` fail.
- Jobs with payload `{"fail": true}` fail.
- Jobs with payload `{"fail_until_attempt": 1}` fail once, then succeed.

This makes retry, backoff, and dead-letter behavior easy to validate without external services.

## Retries and Dead-Letter Queue

When a job fails and attempts remain, it is marked `retrying` and placed into the delayed sorted set using exponential backoff:

```text
delay = RETRY_BACKOFF_BASE_SECONDS * 2^(attempts - 1)
```

When retries are exhausted, the job is marked `dead` and its ID is pushed into the Redis dead-letter list. The final error message is retained in the job metadata.

## WebSocket and Dashboard

Live updates are published to Redis Pub/Sub and retained in a short Redis event list.

- WebSocket endpoint: `ws://localhost:8000/ws/jobs`
- Dashboard: `http://localhost:8000/dashboard`

The dashboard shows total jobs, queued jobs, running jobs, succeeded jobs, failed/retrying jobs, dead-letter jobs, and recent job events.

## Testing

Run the test suite:

```bash
pytest
```

Run linting:

```bash
ruff check .
```

The tests use `fakeredis`, so they do not require a running Redis instance.

## Configuration

Configuration is environment-driven. See `.env.example`:

```bash
REDIS_URL=redis://localhost:6379/0
WORKER_CONCURRENCY=4
WORKER_POLL_INTERVAL_SECONDS=0.5
RETRY_BACKOFF_BASE_SECONDS=2
RECENT_JOBS_LIMIT=100
EVENT_STREAM_LIMIT=200
LOG_LEVEL=INFO
```

## Future Improvements

- Add task registration with typed task handlers.
- Add worker heartbeats and worker registry.
- Add scheduled recurring jobs.
- Add Redis Streams support for stronger delivery semantics.
- Add OpenTelemetry tracing and Prometheus metrics.
- Add authentication and tenant-aware job isolation.
- Add a richer dashboard with filtering and replay controls.

## Portfolio Note

This is a portfolio backend infrastructure project. It focuses on clear architecture, operational behavior, testability, and interview-ready code rather than artificial scale or unnecessary framework complexity.

