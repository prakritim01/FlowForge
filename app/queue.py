from __future__ import annotations

from datetime import timedelta

from redis import Redis
from redis.exceptions import WatchError

from app.config import get_settings
from app.events import EventPublisher
from app.models import (
    PRIORITY_ORDER,
    Job,
    JobCreate,
    JobStatus,
    Priority,
    QueueStats,
    utc_now,
)


class JobNotFoundError(Exception):
    pass


class JobCancellationError(Exception):
    pass


class JobQueue:
    def __init__(self, redis: Redis):
        self.redis = redis
        self.settings = get_settings()
        self.events = EventPublisher(redis)

    def submit(self, request: JobCreate) -> Job:
        now = utc_now()
        status = (
            JobStatus.DELAYED
            if request.delay_seconds > 0
            else JobStatus.QUEUED
        )

        job = Job(
            task_name=request.task_name,
            payload=request.payload,
            priority=request.priority,
            status=status,
            max_retries=request.max_retries,
            created_at=now,
            updated_at=now,
        )

        self._save_job(job)
        self._remember_job(job.job_id)

        if request.delay_seconds > 0:
            run_at = now + timedelta(seconds=request.delay_seconds)
            self.redis.zadd(
                "dtq:delayed",
                {job.job_id: run_at.timestamp()},
            )
            self.events.publish(
                job.job_id,
                JobStatus.DELAYED,
                "Job scheduled for delayed execution",
            )
        else:
            self._push_queue(job.priority, job.job_id)
            self.events.publish(
                job.job_id,
                JobStatus.QUEUED,
                "Job queued",
            )

        return job

    def get(self, job_id: str) -> Job:
        raw = self.redis.get(self._job_key(job_id))
        if raw is None:
            raise JobNotFoundError(job_id)

        return Job.model_validate_json(raw)

    def cancel(self, job_id: str) -> Job:
        key = self._job_key(job_id)

        while True:
            with self.redis.pipeline() as pipe:
                try:
                    pipe.watch(key)
                    raw = pipe.get(key)

                    if raw is None:
                        raise JobNotFoundError(job_id)

                    job = Job.model_validate_json(raw)

                    if job.status not in (
                        JobStatus.QUEUED,
                        JobStatus.DELAYED,
                    ):
                        raise JobCancellationError(
                            f"Cannot cancel a job with status "
                            f"'{job.status}'"
                        )

                    job.status = JobStatus.CANCELLED
                    job.updated_at = utc_now()
                    job.finished_at = job.updated_at

                    pipe.multi()
                    pipe.set(key, job.model_dump_json())
                    pipe.zrem("dtq:delayed", job_id)

                    for priority in PRIORITY_ORDER:
                        pipe.lrem(
                            self._queue_key(priority),
                            0,
                            job_id,
                        )

                    pipe.execute()
                    break

                except WatchError:
                    continue

        self.events.publish(
            job.job_id,
            JobStatus.CANCELLED,
            "Job cancelled before execution",
        )

        return job

    def list_recent(self, limit: int = 50) -> list[Job]:
        ids = self.redis.lrange(
            "dtq:recent-jobs",
            0,
            limit - 1,
        )

        jobs: list[Job] = []

        for job_id in ids:
            try:
                jobs.append(self.get(job_id))
            except JobNotFoundError:
                continue

        return jobs

    def stats(self) -> QueueStats:
        jobs = self.list_recent(self.settings.recent_jobs_limit)
        counts = {status: 0 for status in JobStatus}

        for job in jobs:
            counts[JobStatus(job.status)] += 1

        queues = {
            priority.value: self.redis.llen(
                self._queue_key(priority)
            )
            for priority in PRIORITY_ORDER
        }

        return QueueStats(
            total_jobs=len(jobs),
            queued_jobs=sum(queues.values()),
            delayed_jobs=self.redis.zcard("dtq:delayed"),
            running_jobs=counts[JobStatus.RUNNING],
            succeeded_jobs=counts[JobStatus.SUCCEEDED],
            failed_jobs=(
                counts[JobStatus.FAILED]
                + counts[JobStatus.RETRYING]
            ),
            dead_letter_jobs=self.redis.llen("dtq:dead-letter"),
            queues=queues,
        )

    def promote_due_jobs(self) -> int:
        now_score = utc_now().timestamp()

        job_ids = self.redis.zrangebyscore(
            "dtq:delayed",
            "-inf",
            now_score,
        )

        promoted = 0

        for job_id in job_ids:
            key = self._job_key(job_id)

            while True:
                with self.redis.pipeline() as pipe:
                    try:
                        pipe.watch(key)

                        raw = pipe.get(key)
                        if raw is None:
                            pipe.multi()
                            pipe.zrem("dtq:delayed", job_id)
                            pipe.execute()
                            break

                        job = Job.model_validate_json(raw)

                        if job.status not in (
                            JobStatus.DELAYED,
                            JobStatus.RETRYING,
                        ):
                            pipe.multi()
                            pipe.zrem("dtq:delayed", job_id)
                            pipe.execute()
                            break

                        job.status = JobStatus.QUEUED
                        job.updated_at = utc_now()

                        pipe.multi()
                        pipe.set(key, job.model_dump_json())
                        pipe.zrem("dtq:delayed", job_id)
                        pipe.lpush(
                            self._queue_key(Priority(job.priority)),
                            job.job_id,
                        )
                        pipe.execute()

                        self.events.publish(
                            job.job_id,
                            JobStatus.QUEUED,
                            "Delayed job moved to ready queue",
                        )
                        promoted += 1
                        break

                    except WatchError:
                        continue

        return promoted

    def dequeue(self) -> Job | None:
        self.promote_due_jobs()

        for priority in PRIORITY_ORDER:
            job_id = self.redis.rpop(
                self._queue_key(priority)
            )

            if not job_id:
                continue

            try:
                job = self.get(job_id)
            except JobNotFoundError:
                continue

            if job.status != JobStatus.QUEUED:
                continue

            return job

        return None

    def mark_running(self, job: Job) -> Job:
        key = self._job_key(job.job_id)

        while True:
            with self.redis.pipeline() as pipe:
                try:
                    pipe.watch(key)
                    raw = pipe.get(key)

                    if raw is None:
                        raise JobNotFoundError(job.job_id)

                    current = Job.model_validate_json(raw)

                    if current.status != JobStatus.QUEUED:
                        raise JobCancellationError(
                            f"Cannot start a job with status "
                            f"'{current.status}'"
                        )

                    current.status = JobStatus.RUNNING
                    current.attempts += 1
                    current.started_at = utc_now()
                    current.updated_at = current.started_at
                    current.error_message = None

                    pipe.multi()
                    pipe.set(key, current.model_dump_json())
                    pipe.execute()
                    break

                except WatchError:
                    continue

        self.events.publish(
            current.job_id,
            JobStatus.RUNNING,
            "Job started",
        )

        return current

    def mark_succeeded(self, job: Job) -> Job:
        current = self._transition_from_running(
            job.job_id,
            JobStatus.SUCCEEDED,
            error=None,
        )
        current.finished_at = utc_now()
        current.updated_at = current.finished_at
        self._save_job(current)

        self.events.publish(
            current.job_id,
            JobStatus.SUCCEEDED,
            "Job completed successfully",
        )
        return current

    def mark_failed_for_retry(
        self,
        job: Job,
        error: str,
        delay_seconds: int,
    ) -> Job:
        current = self._transition_from_running(
            job.job_id,
            JobStatus.RETRYING,
            error=error,
        )

        run_at = current.updated_at + timedelta(
            seconds=delay_seconds
        )
        self.redis.zadd(
            "dtq:delayed",
            {current.job_id: run_at.timestamp()},
        )

        self.events.publish(
            current.job_id,
            JobStatus.RETRYING,
            f"Job failed and will retry in {delay_seconds} seconds",
        )
        return current

    def mark_dead(self, job: Job, error: str) -> Job:
        current = self._transition_from_running(
            job.job_id,
            JobStatus.DEAD,
            error=error,
        )
        current.finished_at = utc_now()
        current.updated_at = current.finished_at
        self._save_job(current)
        self.redis.lpush("dtq:dead-letter", current.job_id)

        self.events.publish(
            current.job_id,
            JobStatus.DEAD,
            "Job moved to dead-letter queue",
        )
        return current

    def mark_failed(self, job: Job, error: str) -> Job:
        current = self._transition_from_running(
            job.job_id,
            JobStatus.FAILED,
            error=error,
        )
        current.finished_at = utc_now()
        current.updated_at = current.finished_at
        self._save_job(current)

        self.events.publish(
            current.job_id,
            JobStatus.FAILED,
            "Job failed",
        )
        return current

    def _transition_from_running(
        self,
        job_id: str,
        new_status: JobStatus,
        error: str | None,
    ) -> Job:
        key = self._job_key(job_id)

        while True:
            with self.redis.pipeline() as pipe:
                try:
                    pipe.watch(key)
                    raw = pipe.get(key)

                    if raw is None:
                        raise JobNotFoundError(job_id)

                    current = Job.model_validate_json(raw)

                    if current.status != JobStatus.RUNNING:
                        raise JobCancellationError(
                            f"Cannot transition a job with status "
                            f"'{current.status}' to '{new_status}'"
                        )

                    current.status = new_status
                    current.error_message = error
                    current.updated_at = utc_now()

                    pipe.multi()
                    pipe.set(key, current.model_dump_json())
                    pipe.execute()
                    return current

                except WatchError:
                    continue

    def _save_job(self, job: Job) -> None:
        self.redis.set(
            self._job_key(job.job_id),
            job.model_dump_json(),
        )

    def _remember_job(self, job_id: str) -> None:
        pipe = self.redis.pipeline()
        pipe.lpush("dtq:recent-jobs", job_id)
        pipe.ltrim(
            "dtq:recent-jobs",
            0,
            self.settings.recent_jobs_limit - 1,
        )
        pipe.execute()

    def _push_queue(
        self,
        priority: Priority | str,
        job_id: str,
    ) -> None:
        self.redis.lpush(
            self._queue_key(Priority(priority)),
            job_id,
        )

    @staticmethod
    def _job_key(job_id: str) -> str:
        return f"dtq:job:{job_id}"

    @staticmethod
    def _queue_key(priority: Priority) -> str:
        return f"dtq:queue:{priority.value}"