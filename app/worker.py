
from __future__ import annotations

import asyncio
import signal
from dataclasses import dataclass
from typing import Protocol

from redis import Redis

from app.config import get_settings
from app.logging_config import configure_logging, logger
from app.models import Job
from app.queue import (
    JobCancellationError,
    JobNotFoundError,
    JobQueue,
)
from app.redis_client import get_redis


class TaskExecutor(Protocol):
    async def execute(self, job: Job) -> None:
        ...


class SimulatedTaskExecutor:
    async def execute(self, job: Job) -> None:
        duration = float(job.payload.get("duration_seconds", 0.05))
        await asyncio.sleep(max(duration, 0))

        fail_until_attempt = int(
            job.payload.get("fail_until_attempt", 0)
        )

        if (
            job.payload.get("fail") is True
            or job.task_name == "always_fail"
        ):
            raise RuntimeError("Simulated task failure")

        if fail_until_attempt >= job.attempts:
            raise RuntimeError(
                "Simulated transient failure through "
                f"attempt {fail_until_attempt}"
            )


@dataclass
class Worker:
    queue: JobQueue
    executor: TaskExecutor
    concurrency: int
    poll_interval_seconds: float
    retry_backoff_base_seconds: int
    _stopping: bool = False

    async def process_next(self) -> bool:
        job = self.queue.dequeue()

        if job is None:
            return False

        # Claim the job before executing it. If cancellation or another
        # worker changed its state, do not execute the stale job object.
        try:
            job = self.queue.mark_running(job)
        except (JobCancellationError, JobNotFoundError):
            logger.info(
                "job_start_skipped",
                job_id=job.job_id,
                reason="job state changed before worker claim",
            )
            return True

        try:
            await self.executor.execute(job)

        except Exception as exc:  # noqa: BLE001
            error = str(exc)

            try:
                if job.attempts <= job.max_retries:
                    delay = (
                        self.retry_backoff_base_seconds
                        * (2 ** (job.attempts - 1))
                    )
                    self.queue.mark_failed_for_retry(
                        job,
                        error,
                        delay,
                    )
                else:
                    self.queue.mark_dead(job, error)

            except (JobCancellationError, JobNotFoundError):
                logger.warning(
                    "job_failure_transition_skipped",
                    job_id=job.job_id,
                )
                return True

            logger.warning(
                "job_failed",
                job_id=job.job_id,
                attempts=job.attempts,
                error=error,
            )
            return True

        try:
            self.queue.mark_succeeded(job)
        except (JobCancellationError, JobNotFoundError):
            logger.warning(
                "job_success_transition_skipped",
                job_id=job.job_id,
            )
            return True

        logger.info(
            "job_succeeded",
            job_id=job.job_id,
            attempts=job.attempts,
        )
        return True

    async def run(self) -> None:
        workers = [
            asyncio.create_task(self._run_loop(index))
            for index in range(self.concurrency)
        ]
        await asyncio.gather(*workers)

    async def _run_loop(self, index: int) -> None:
        logger.info("worker_loop_started", worker_index=index)

        while not self._stopping:
            processed = await self.process_next()

            if not processed:
                await asyncio.sleep(self.poll_interval_seconds)

        logger.info("worker_loop_stopped", worker_index=index)

    def stop(self) -> None:
        self._stopping = True


def build_worker(redis: Redis | None = None) -> Worker:
    settings = get_settings()
    redis_client = redis or get_redis()

    return Worker(
        queue=JobQueue(redis_client),
        executor=SimulatedTaskExecutor(),
        concurrency=settings.worker_concurrency,
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        retry_backoff_base_seconds=settings.retry_backoff_base_seconds,
    )


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    worker = build_worker()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, worker.stop)

    logger.info("worker_started", concurrency=settings.worker_concurrency)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
