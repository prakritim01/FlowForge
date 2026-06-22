import httpx
import pytest


@pytest.mark.asyncio
async def test_create_job_returns_generated_id(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={
            "task_name": "send_email",
            "payload": {"to": "user@example.com"},
            "priority": "high",
            "delay_seconds": 0,
            "max_retries": 3,
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["job_id"]
    assert body["status"] == "queued"


@pytest.mark.asyncio
async def test_get_job_status(client: httpx.AsyncClient) -> None:
    created = (
        await client.post("/jobs", json={"task_name": "send_email", "payload": {}})
    ).json()

    response = await client.get(f"/jobs/{created['job_id']}")

    assert response.status_code == 200
    assert response.json()["status"] == "queued"


@pytest.mark.asyncio
async def test_invalid_priority_validation(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/jobs",
        json={"task_name": "send_email", "payload": {}, "priority": "urgent"},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_list_recent_jobs(client: httpx.AsyncClient) -> None:
    await client.post("/jobs", json={"task_name": "first", "payload": {}})
    await client.post("/jobs", json={"task_name": "second", "payload": {}})

    response = await client.get("/jobs")

    assert response.status_code == 200
    assert [job["task_name"] for job in response.json()] == ["second", "first"]


@pytest.mark.asyncio
async def test_queue_stats_endpoint(client: httpx.AsyncClient) -> None:
    await client.post("/jobs", json={"task_name": "high", "payload": {}, "priority": "high"})
    await client.post("/jobs", json={"task_name": "low", "payload": {}, "priority": "low"})

    response = await client.get("/queues/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["total_jobs"] == 2
    assert body["queued_jobs"] == 2
    assert body["queues"]["high"] == 1
    assert body["queues"]["low"] == 1


@pytest.mark.asyncio
async def test_health_endpoint(client: httpx.AsyncClient) -> None:
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "redis": "ok"}
