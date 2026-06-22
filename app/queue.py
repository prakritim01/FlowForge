from __future__ import annotations

from datetime import timedelta

from redis import Redis

from app.config import get_settings
from app.events import EventPublisher
from app.models import PRIORITY_ORDER, Job, JobCreate, JobStatus, Priority, QueueStats, utc_now


class JobNotFoundError(Exception):
    pass


class JobQueue:
    def __init__(self, redis: Redis):
        self.redis = redis
        self.settings = get_settings()
        self.events = EventPublisher(redis)

    def submit(self, request: JobCreate) -> Job:
        now = utc_now()
        status = JobStatus.DELAYED if request.delay_seconds > 0 else JobStatus.QUEUED
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
            self.redis.zadd("dtq:delayed", {job.job_id: run_at.timestamp()})
            self.events.publish(
                job.job_id,
                JobStatus.DELAYED,
                "Job scheduled for delayed execution",
            )
        else:
            self._push_queue(job.priority, job.job_id)
            self.events.publish(job.job_id, JobStatus.QUEUED, "Job queued")
        return job

    def get(self, job_id: str) -> Job:
        raw = self.redis.get(self._job_key(job_id))
        if raw is None:
            raise JobNotFoundError(job_id)
        return Job.model_validate_json(raw)

    def list_recent(self, limit: int = 50) -> list[Job]:
        ids = self.redis.lrange("dtq:recent-jobs", 0, limit - 1)
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
            priority.value: self.redis.llen(self._queue_key(priority))
            for priority in PRIORITY_ORDER
        }
        queued_jobs = sum(queues.values())
        delayed_jobs = self.redis.zcard("dtq:delayed")
        return QueueStats(
            total_jobs=len(jobs),
            queued_jobs=queued_jobs,
            delayed_jobs=delayed_jobs,
            running_jobs=counts[JobStatus.RUNNING],
            succeeded_jobs=counts[JobStatus.SUCCEEDED],
            failed_jobs=counts[JobStatus.FAILED] + counts[JobStatus.RETRYING],
            dead_letter_jobs=self.redis.llen("dtq:dead-letter"),
            queues=queues,
        )

    def promote_due_jobs(self) -> int:
        now_score = utc_now().timestamp()
        job_ids = self.redis.zrangebyscore("dtq:delayed", "-inf", now_score)
        promoted = 0
        for job_id in job_ids:
            if not self.redis.zrem("dtq:delayed", job_id):
                continue
            try:
                job = self.get(job_id)
            except JobNotFoundError:
                continue
            job.status = JobStatus.QUEUED
            job.updated_at = utc_now()
            self._save_job(job)
            self._push_queue(job.priority, job.job_id)
            self.events.publish(job.job_id, JobStatus.QUEUED, "Delayed job moved to ready queue")
            promoted += 1
        return promoted

    def dequeue(self) -> Job | None:
        self.promote_due_jobs()
        for priority in PRIORITY_ORDER:
            job_id = self.redis.rpop(self._queue_key(priority))
            if not job_id:
                continue
            try:
                return self.get(job_id)
            except JobNotFoundError:
                continue
        return None

    def mark_running(self, job: Job) -> Job:
        job.status = JobStatus.RUNNING
        job.attempts += 1
        job.started_at = utc_now()
        job.updated_at = job.started_at
        job.error_message = None
        self._save_job(job)
        self.events.publish(job.job_id, JobStatus.RUNNING, "Job started")
        return job

    def mark_succeeded(self, job: Job) -> Job:
        job.status = JobStatus.SUCCEEDED
        job.finished_at = utc_now()
        job.updated_at = job.finished_at
        job.error_message = None
        self._save_job(job)
        self.events.publish(job.job_id, JobStatus.SUCCEEDED, "Job completed successfully")
        return job

    def mark_failed_for_retry(self, job: Job, error: str, delay_seconds: int) -> Job:
        job.status = JobStatus.RETRYING
        job.error_message = error
        job.updated_at = utc_now()
        self._save_job(job)
        run_at = job.updated_at + timedelta(seconds=delay_seconds)
        self.redis.zadd("dtq:delayed", {job.job_id: run_at.timestamp()})
        self.events.publish(
            job.job_id,
            JobStatus.RETRYING,
            f"Job failed and will retry in {delay_seconds} seconds",
        )
        return job

    def mark_dead(self, job: Job, error: str) -> Job:
        job.status = JobStatus.DEAD
        job.error_message = error
        job.finished_at = utc_now()
        job.updated_at = job.finished_at
        self._save_job(job)
        self.redis.lpush("dtq:dead-letter", job.job_id)
        self.events.publish(job.job_id, JobStatus.DEAD, "Job moved to dead-letter queue")
        return job

    def mark_failed(self, job: Job, error: str) -> Job:
        job.status = JobStatus.FAILED
        job.error_message = error
        job.finished_at = utc_now()
        job.updated_at = job.finished_at
        self._save_job(job)
        self.events.publish(job.job_id, JobStatus.FAILED, "Job failed")
        return job

    def _save_job(self, job: Job) -> None:
        self.redis.set(self._job_key(job.job_id), job.model_dump_json())

    def _remember_job(self, job_id: str) -> None:
        pipe = self.redis.pipeline()
        pipe.lpush("dtq:recent-jobs", job_id)
        pipe.ltrim("dtq:recent-jobs", 0, self.settings.recent_jobs_limit - 1)
        pipe.execute()

    def _push_queue(self, priority: Priority | str, job_id: str) -> None:
        self.redis.lpush(self._queue_key(Priority(priority)), job_id)

    @staticmethod
    def _job_key(job_id: str) -> str:
        return f"dtq:job:{job_id}"

    @staticmethod
    def _queue_key(priority: Priority) -> str:
        return f"dtq:queue:{priority.value}"
