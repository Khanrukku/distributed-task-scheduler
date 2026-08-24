"""
worker/worker.py
Worker node — pulls tasks from RabbitMQ, executes them concurrently,
sends heartbeats, handles retries with exponential backoff, and prevents
concurrent duplicate execution of the same task.

Concurrency model:
  - Worker instances run as asyncio tasks
  - asyncio.Semaphore limits concurrent execution slots
  - Heartbeat runs independently from task processing
  - asyncio.Lock protects local worker metadata
  - Redis ownership locks prevent duplicate concurrent task execution
"""

import asyncio
import uuid
from datetime import datetime, timezone

from models.task import Task, TaskStatus, WorkerInfo
from utils.redis_store import RedisStore
from scheduler.queue_manager import QueueManager
from worker.task_executor import execute_task


# Individual task execution is limited to 30 seconds.
# This Redis lock is intentionally longer so it also covers
# status updates and retry backoff for the default retry policy.
TASK_EXECUTION_LOCK_TTL = 120


class Worker:
    """
    A worker node in the distributed task-processing pool.

    Multiple Worker instances may run in the same process or across
    multiple machines/containers.

    A Redis ownership lock is acquired before executing each task so
    duplicate RabbitMQ deliveries cannot execute the same task
    concurrently on different workers.
    """

    def __init__(
        self,
        redis_store: RedisStore,
        queue_manager: QueueManager,
        max_concurrent: int = 2,
        heartbeat_interval: int = 5,
    ):
        if max_concurrent <= 0:
            raise ValueError(
                "max_concurrent must be greater than zero"
            )

        if heartbeat_interval <= 0:
            raise ValueError(
                "heartbeat_interval must be greater than zero"
            )

        self.worker_id = (
            f"worker-{str(uuid.uuid4())[:8]}"
        )

        self._store = redis_store
        self._queue = queue_manager

        self._semaphore = asyncio.Semaphore(
            max_concurrent
        )

        # Protects local WorkerInfo mutations.
        self._lock = asyncio.Lock()

        self._heartbeat_interval = (
            heartbeat_interval
        )

        self._running = False

        self._info = WorkerInfo(
            worker_id=self.worker_id
        )

        self._active_tasks: dict[
            str,
            asyncio.Task
        ] = {}

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self):
        """
        Register the worker and start heartbeat + queue consumption.
        """
        self._running = True

        await self._store.register_worker(
            self._info
        )

        print(
            f"[{self.worker_id}] Started"
        )

        await asyncio.gather(
            self._heartbeat_loop(),
            self._consume_loop(),
        )

    async def stop(self):
        """
        Stop accepting work, wait for local active tasks, and deregister.
        """
        self._running = False

        if self._active_tasks:
            print(
                f"[{self.worker_id}] Waiting for "
                f"{len(self._active_tasks)} active tasks..."
            )

            await asyncio.gather(
                *self._active_tasks.values(),
                return_exceptions=True,
            )

        await self._store.deregister_worker(
            self.worker_id
        )

        print(
            f"[{self.worker_id}] Stopped. "
            f"Done={self._info.tasks_done} "
            f"Failed={self._info.tasks_failed}"
        )

    # ── Heartbeat ──────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self):
        """
        Periodically refresh this worker's Redis heartbeat.

        Redis automatically expires the worker record if the process
        disappears and heartbeats stop.
        """
        while self._running:
            try:
                await self._store.update_worker(
                    self._info
                )

            except Exception as exc:
                print(
                    f"[{self.worker_id}] "
                    f"Heartbeat error: {exc}"
                )

            await asyncio.sleep(
                self._heartbeat_interval
            )

    # ── Task consumption ───────────────────────────────────────────────────────

    async def _consume_loop(self):
        """
        Consume tasks from RabbitMQ.
        """
        await self._queue.consume(
            self._handle_task
        )

    async def _handle_task(
        self,
        task: Task,
    ):
        """
        Handle one delivered task.

        Reliability protections:

          1. Semaphore limits local concurrency.
          2. Redis ownership lock prevents the same task from running
             concurrently on multiple workers.
          3. Terminal tasks are ignored if a duplicate delivery arrives.

        The RabbitMQ message is acknowledged only after this handler
        returns successfully.
        """
        async with self._semaphore:

            # ── Skip stale duplicate deliveries ───────────────────────────────

            stored_task = await self._store.get_task(
                task.id
            )

            if stored_task is not None:
                if stored_task.status in (
                    TaskStatus.COMPLETED,
                    TaskStatus.DEAD,
                ):
                    print(
                        f"[{self.worker_id}] Ignoring duplicate "
                        f"delivery for terminal task {task.id} "
                        f"({stored_task.status.value})"
                    )

                    return

                # Redis is the authoritative source for retry state.
                task.retries = stored_task.retries
                task.max_retries = (
                    stored_task.max_retries
                )

            # ── Acquire distributed execution lock ───────────────────────────

            lock_resource = (
                f"task-execution:{task.id}"
            )

            lock_token = await self._store.acquire_lock(
                lock_resource,
                ttl=TASK_EXECUTION_LOCK_TTL,
            )

            if lock_token is None:
                # Another worker is already processing this task.
                #
                # Returning successfully acknowledges this duplicate
                # RabbitMQ delivery while the legitimate execution
                # continues elsewhere.
                print(
                    f"[{self.worker_id}] Duplicate task "
                    f"{task.id} is already being processed; "
                    "skipping this delivery"
                )

                return

            try:
                asyncio_task = asyncio.create_task(
                    self._run_task(task)
                )

                self._active_tasks[
                    task.id
                ] = asyncio_task

                try:
                    await asyncio_task

                finally:
                    self._active_tasks.pop(
                        task.id,
                        None,
                    )

            finally:
                released = await self._store.release_lock(
                    lock_resource,
                    lock_token,
                )

                if not released:
                    # The lock may already have expired.
                    #
                    # Importantly, ownership-safe release prevents
                    # this worker from deleting a newer worker's lock.
                    print(
                        f"[{self.worker_id}] Execution lock "
                        f"for task {task.id} was no longer owned "
                        "when release was attempted"
                    )

    # ── Task execution ─────────────────────────────────────────────────────────

    async def _run_task(
        self,
        task: Task,
    ):
        """
        Execute one task with:

          - Redis status tracking
          - execution timeout
          - exponential-backoff retries
          - dead-letter routing after retry exhaustion
        """
        async with self._lock:
            self._info.status = "busy"
            self._info.current_task = task.id

            await self._store.update_worker(
                self._info
            )

        # Mark task as running.
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(
            timezone.utc
        )
        task.worker_id = self.worker_id

        await self._store.update_task(
            task
        )

        print(
            f"[{self.worker_id}] Running task "
            f"{task.id} ({task.name})"
        )

        try:
            result = await asyncio.wait_for(
                execute_task(task),
                timeout=30.0,
            )

            # ── Successful execution ──────────────────────────────────────────

            task.status = (
                TaskStatus.COMPLETED
            )

            task.completed_at = datetime.now(
                timezone.utc
            )

            task.result = result
            task.error = None

            await self._store.update_task(
                task
            )

            async with self._lock:
                self._info.tasks_done += 1

            print(
                f"[{self.worker_id}] "
                f"Task {task.id} completed ✓"
            )

        except asyncio.TimeoutError:
            await self._handle_failure(
                task,
                "Task timed out after 30s",
            )

        except Exception as exc:
            await self._handle_failure(
                task,
                str(exc),
            )

        finally:
            async with self._lock:
                self._info.status = "idle"
                self._info.current_task = None

                await self._store.update_worker(
                    self._info
                )

    # ── Failure / retry handling ───────────────────────────────────────────────

    async def _handle_failure(
        self,
        task: Task,
        error: str,
    ):
        """
        Apply application-level retry policy.

        Failed tasks are retried using exponential backoff.

        After retry exhaustion, the task is marked DEAD and explicitly
        published to the dead-letter queue.
        """
        task.retries += 1
        task.error = error

        if task.retries <= task.max_retries:

            backoff = 2 ** task.retries

            print(
                f"[{self.worker_id}] "
                f"Task {task.id} failed ({error}). "
                f"Retry {task.retries}/"
                f"{task.max_retries} "
                f"in {backoff}s"
            )

            task.status = (
                TaskStatus.QUEUED
            )

            await self._store.update_task(
                task
            )

            await asyncio.sleep(
                backoff
            )

            # Publish the next retry attempt.
            await self._queue.publish(
                task
            )

        else:
            print(
                f"[{self.worker_id}] "
                f"Task {task.id} exhausted retries "
                "→ dead-letter"
            )

            task.status = (
                TaskStatus.DEAD
            )

            await self._store.update_task(
                task
            )

            await self._queue.publish_to_dead_letter(
                task
            )

            async with self._lock:
                self._info.tasks_failed += 1
