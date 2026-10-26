# FlowForge — Distributed Task Queue

A distributed job execution platform built with **FastAPI**, **Redis**, **Python**, and **Docker Compose**. FlowForge exposes a REST API for submitting and inspecting jobs while independent workers consume jobs from the queue.

> **Project provenance:** FlowForge is an extended fork of an upstream open-source project. Preserve the upstream license and attribution, and document your own changes separately. Do not describe inherited functionality as newly implemented.

## Features

- REST API for creating and inspecting jobs
- Priority-based job processing (`high`, `medium`, `low`)
- FIFO ordering within the same priority
- Delayed job scheduling
- Retry handling with exponential backoff
- Dead-letter handling when retries are exhausted
- Configurable worker concurrency
- Queue statistics and health endpoints
- Interactive API documentation through Swagger UI
- Docker Compose setup for the API, Redis, and worker
- Automated tests for API, queue ordering, delayed jobs, retries, and worker behavior

## Architecture

```text
                  ┌──────────────────────┐
                  │       Client         │
                  │   Swagger / HTTP     │
                  └──────────┬───────────┘
                             │ REST
                             ▼
                  ┌──────────────────────┐
                  │    FastAPI service   │
                  │   POST /jobs         │
                  │   GET  /jobs         │
                  │   GET  /jobs/{id}    │
                  │   GET  /queues/stats │
                  │   GET  /health       │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │        Redis         │
                  │ queue / job state    │
                  └──────────┬───────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │   Worker processes   │
                  │ consume and execute  │
                  │ jobs / retry failures│
                  └──────────────────────┘
```

The API and worker run as separate services. Redis provides shared queue/state storage. Exact delivery guarantees and persistence behavior depend on the implementation and Redis configuration.

## Tech Stack

- **Python 3.11**
- **FastAPI** and **Uvicorn**
- **Redis 7**
- **Docker Compose**
- **Pytest** for automated tests

## Getting Started

### Prerequisites

Install:
- Docker Desktop with Docker Compose
- Git

### 1. Clone your fork

Replace the URL if you renamed the repository on GitHub.

```bash
git clone https://github.com/prakritim01/distributed-task-queue.git
cd distributed-task-queue
```

### 2. Start the services

```bash
docker compose up --build
```

Keep this terminal open while using the application.

### 3. Open the API documentation

Visit:

- Swagger UI: <http://localhost:8000/docs>
- OpenAPI schema: <http://localhost:8000/openapi.json>
- Health endpoint: <http://localhost:8000/health>
- Dashboard endpoint: <http://localhost:8000/dashboard>

The dashboard endpoint may return API data rather than a standalone web UI, depending on the implementation.

### 4. Stop the services

Press `Ctrl+C`, then run:

```bash
docker compose down
```

To remove containers and the default Compose network while keeping named volumes, use the command above. To also remove Compose-managed volumes, run `docker compose down -v` (this can delete stored data).

## Example: Create a Job

In Swagger UI, expand **POST `/jobs`**, select **Try it out**, and submit:

```json
{
  "task_name": "send_email",
  "payload": {
    "to": "prakriti@example.com",
    "subject": "Queue test",
    "message": "Hello from FlowForge!"
  },
  "priority": "high",
  "delay_seconds": 0,
  "max_retries": 3
}
```

This example submits a sample task to the queue. It does **not** imply that a real email will be sent unless an email handler has been implemented and configured.

A successful response looks like:

```json
{
  "job_id": "generated-job-id",
  "status": "queued"
}
```

Copy the returned `job_id` and use **GET `/jobs/{job_id}`** to inspect the job.

## API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/jobs` | Submit a new job |
| `GET` | `/jobs` | List jobs |
| `GET` | `/jobs/{job_id}` | Retrieve a job's status/details |
| `GET` | `/queues/stats` | Inspect queue statistics |
| `GET` | `/health` | Health check |
| `GET` | `/dashboard` | Dashboard-related endpoint |

Use `/docs` as the source of truth for request fields, response schemas, and validation behavior.

## Testing

The project test suite was run successfully in the development container:

```text
17 passed in 0.83s
```

The tests covered:
- Job creation, status retrieval, listing, queue statistics, and health endpoint
- Priority ordering and FIFO ordering within a priority
- Delayed jobs and promotion when due
- Retry scheduling and exponential backoff
- Dead-letter behavior after retries are exhausted
- Worker success and failure paths

### Run tests

If the test dependencies are installed in the active container, run:

```bash
docker compose exec api python -m pytest tests -v
```

If Pytest or test dependencies are missing after rebuilding, add the project's development/test dependencies to its dependency configuration and rebuild the image. Installing packages into a running container is temporary and may not persist after recreation.

## Configuration

The current Compose setup uses the following environment variables:

| Variable | Purpose |
|---|---|
| `REDIS_URL` | Redis connection URL (Compose default: `redis://redis:6379/0`) |
| `LOG_LEVEL` | API logging level |
| `WORKER_CONCURRENCY` | Worker concurrency setting (Compose default: `4`) |

Review `docker-compose.yml` and the application settings for the complete list of supported options.

## Development Workflow

```bash
# Start services
docker compose up --build

# Run tests
docker compose exec api python -m pytest tests -v

# View service status
docker compose ps

# View logs
docker compose logs -f api worker redis

# Stop services
docker compose down
```

## Reliability Concepts Demonstrated

- Priority scheduling
- FIFO ordering within equal priorities
- Delayed execution
- Retry policies and backoff
- Dead-letter handling
- API/worker separation
- Containerized local development
- Automated testing

These are implementation-level capabilities, not a claim of production certification. Before using the system in production, review authentication, authorization, rate limiting, input limits, Redis persistence, job idempotency, graceful shutdown, monitoring, and delivery guarantees.

## Roadmap

Potential future extensions (not claimed as implemented):
- Persistent PostgreSQL job history
- Job timeout handling
- A React dashboard with live updates
- Prometheus metrics and Grafana dashboards
- Authentication, authorization, and rate limiting
- GitHub Actions CI
- Load testing and throughput benchmarks

## Attribution and License

This repository is based on an upstream open-source project. Keep the original license and copyright notices, and add a clear upstream link here before publishing this README.

- **Upstream repository:** `https://github.com/noureddine-py/distributed-task-queue`
- **Your fork:** <https://github.com/prakritim01/distributed-task-queue>

Document your own modifications in a separate section, for example:

- Added: `Job cancellation through DELETE /jobs/{job_id}`
- Changed: `Redis transaction-based cancellation and worker claim checks`
- Tests added: `Existing test suite verified: 17 tests passed`

Do not remove required license notices. If the upstream project uses a license that requires preserving attribution or source disclosures, follow those terms.

## Screenshots

Add screenshots after capturing the running application:

- Swagger UI at `/docs`
- A successful `POST /jobs` response
- Job status from `GET /jobs/{job_id}`
- Queue statistics from `/queues/stats`

Suggested location: `docs/images/`.

---

**Status:** Local Docker Compose startup and the existing 17-test suite have been verified in the development environment. Feature claims above reflect the behavior tested so far; verify any additional behavior before advertising it.

