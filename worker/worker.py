"""
worker/worker.py
Worker node — pulls tasks from queue, executes them concurrently,
sends heartbeats, handles retries with exponential backoff.

Concurrency model:
  - Each worker runs an async event loop
  - asyncio.Semaphore limits concurrent task slots
  - Heartbeat runs as a separate asyncio task (non-blocking)
  - Thread-safe state updates via asyncio locks
"""

import asyncio
import uuid
from datetime import datetime, timezone

from models.task import Task, TaskStatus, WorkerInfo
from utils.redis_store import RedisStore
from scheduler.queue_manager import QueueManager
from worker.task_executor import execute_task


class Worker:
    """
    A single worker node in the distributed pool.
    Multiple Worker instances can run on the same or different machines.
    """

    def __init__(
        self,
        redis_store: RedisStore,
        queue_manager: QueueManager,
        max_concurrent: int = 2,
        heartbeat_interval: int = 5,
    ):
        self.worker_id = f"worker-{str(uuid.uuid4())[:8]}"
        self._store = redis_store
        self._queue = queue_manager
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._lock = asyncio.Lock()
        self._heartbeat_interval = heartbeat_interval
        self._running = False
        self._info = WorkerInfo(worker_id=self.worker_id)
        self._active_tasks: dict[str, asyncio.Task] = {}

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self):
        self._running = True
        await self._store.register_worker(self._info)
        print(f"[{self.worker_id}] Started")

        # Run heartbeat and task consumption concurrently.
        await asyncio.gather(
            self._heartbeat_loop(),
            self._consume_loop(),
        )

    async def stop(self):
        self._running = False

        # Wait for active tasks to finish.
        if self._active_tasks:
            print(
                f"[{self.worker_id}] Waiting for "
                f"{len(self._active_tasks)} active tasks..."
            )
            await asyncio.gather(
                *self._active_tasks.values(),
                return_exceptions=True,
            )

        await self._store.deregister_worker(self.worker_id)

        print(
            f"[{self.worker_id}] Stopped. "
            f"Done={self._info.tasks_done} "
            f"Failed={self._info.tasks_failed}"
        )

    # ── Heartbeat ──────────────────────────────────────────────────────────────

    async def _heartbeat_loop(self):
        """
        Sends periodic heartbeats to Redis.
        If a worker dies, its key TTL expires and it is automatically
        removed from the active registry.
        """
        while self._running:
            try:
                await self._store.update_worker(self._info)
            except Exception as exc:
                print(f"[{self.worker_id}] Heartbeat error: {exc}")

            await asyncio.sleep(self._heartbeat_interval)

    # ── Task consumption ───────────────────────────────────────────────────────

    async def _consume_loop(self):
        """Pull tasks from the queue and process them with concurrency control."""
        await self._queue.consume(self._handle_task)

    async def _handle_task(self, task: Task):
        """
        Called by QueueManager for each incoming task.

        Uses a semaphore to enforce the max_concurrent limit and creates
        an asyncio task to execute the work.
        """
        async with self._semaphore:
            asyncio_task = asyncio.create_task(
                self._run_task(task)
            )

            self._active_tasks[task.id] = asyncio_task

            try:
                await asyncio_task
            finally:
                self._active_tasks.pop(task.id, None)

    async def _run_task(self, task: Task):
        """
        Execute a single task with:
          - status tracking in Redis
          - timeout enforcement
          - retry with exponential backoff
          - dead-letter routing after retry exhaustion
        """
        async with self._lock:
            self._info.status = "busy"
            self._info.current_task = task.id
            await self._store.update_worker(self._info)

        # Mark task as running.
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now(timezone.utc)
        task.worker_id = self.worker_id

        await self._store.update_task(task)

        print(
            f"[{self.worker_id}] Running task "
            f"{task.id} ({task.name})"
        )

        try:
            # Enforce task timeout.
            result = await asyncio.wait_for(
                execute_task(task),
                timeout=30.0,
            )

            # Success.
            task.status = TaskStatus.COMPLETED
            task.completed_at = datetime.now(timezone.utc)
            task.result = result

            await self._store.update_task(task)

            async with self._lock:
                self._info.tasks_done += 1

            print(
                f"[{self.worker_id}] Task "
                f"{task.id} completed ✓"
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
                await self._store.update_worker(self._info)

    async def _handle_failure(
        self,
        task: Task,
        error: str,
    ):
        """
        Retry logic with exponential backoff.

        After max_retries is exhausted, the task is moved to
        the dead-letter queue.
        """
        task.retries += 1
        task.error = error

        if task.retries <= task.max_retries:
            backoff = 2 ** task.retries

            print(
                f"[{self.worker_id}] Task {task.id} failed "
                f"({error}). Retry "
                f"{task.retries}/{task.max_retries} "
                f"in {backoff}s"
            )

            task.status = TaskStatus.QUEUED

            await self._store.update_task(task)
            await asyncio.sleep(backoff)
            await self._queue.publish(task)

        else:
            print(
                f"[{self.worker_id}] Task {task.id} "
                f"exhausted retries → dead-letter"
            )

            task.status = TaskStatus.DEAD

            await self._store.update_task(task)
            await self._queue.publish_to_dead_letter(task)

            async with self._lock:
                self._info.tasks_failed += 1
