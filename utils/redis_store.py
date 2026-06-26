"""
utils/redis_store.py
Redis-backed task store with atomic operations and deduplication.

Key design decisions:
  - SETNX (set-if-not-exists) for task deduplication — prevents duplicate submissions
  - Redis pipelines for atomic multi-key updates — prevents partial state writes
  - Sorted sets for priority queuing — O(log N) insert/pop by priority score
  - TTL on completed tasks — automatic cleanup without a separate GC process
"""

import json
import asyncio
import redis.asyncio as aioredis
from typing import Optional, List
from datetime import datetime

from models.task import Task, TaskStatus, WorkerInfo


TASK_KEY        = "task:{task_id}"
TASK_SET_KEY    = "tasks:all"           # sorted set: score = priority
WORKER_KEY      = "worker:{worker_id}"
WORKER_SET_KEY  = "workers:active"
LOCK_KEY        = "lock:{resource}"
TASK_TTL        = 3600                  # completed tasks expire after 1 hour


class RedisStore:
    """
    Central Redis store for task state and worker registry.
    All multi-key writes use pipelines for atomicity.
    """

    def __init__(self, redis_url: str):
        self._url = redis_url
        self._client: Optional[aioredis.Redis] = None

    async def connect(self):
        self._client = await aioredis.from_url(self._url, decode_responses=True)

    async def disconnect(self):
        if self._client:
            await self._client.aclose()

    # ── Task operations ────────────────────────────────────────────────────────

    async def save_task(self, task: Task) -> bool:
        """
        Atomically save task and add to priority queue.
        Returns False if task already exists (deduplication via SETNX).
        """
        key = TASK_KEY.format(task_id=task.id)
        serialized = json.dumps(task.to_dict())

        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.setnx(key, serialized)              # deduplicate
            await pipe.expire(key, TASK_TTL * 24)          # 24h TTL for pending
            await pipe.zadd(                               # priority sorted set
                TASK_SET_KEY,
                {task.id: task.priority.value}
            )
            results = await pipe.execute()

        return bool(results[0])  # True if newly created

    async def get_task(self, task_id: str) -> Optional[Task]:
        key = TASK_KEY.format(task_id=task_id)
        data = await self._client.get(key)
        if not data:
            return None
        return Task.from_dict(json.loads(data))

    async def update_task(self, task: Task):
        """Update task state atomically."""
        key = TASK_KEY.format(task_id=task.id)
        serialized = json.dumps(task.to_dict())
        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.set(key, serialized)
            # set shorter TTL once terminal state reached
            if task.status in (TaskStatus.COMPLETED, TaskStatus.DEAD):
                await pipe.expire(key, TASK_TTL)
                await pipe.zrem(TASK_SET_KEY, task.id)
            await pipe.execute()

    async def pop_next_task(self) -> Optional[str]:
        """
        Pop highest-priority task id from the queue.
        Uses ZPOPMAX for atomic pop — no two workers can get the same task.
        """
        result = await self._client.zpopmax(TASK_SET_KEY, count=1)
        if not result:
            return None
        task_id, _score = result[0]
        return task_id

    async def list_tasks(self, status: Optional[TaskStatus] = None) -> List[Task]:
        """List all tasks, optionally filtered by status."""
        # scan all task keys
        keys = []
        async for key in self._client.scan_iter("task:*"):
            keys.append(key)

        tasks = []
        for key in keys:
            data = await self._client.get(key)
            if data:
                task = Task.from_dict(json.loads(data))
                if status is None or task.status == status:
                    tasks.append(task)

        return sorted(tasks, key=lambda t: t.created_at, reverse=True)

    # ── Distributed lock ───────────────────────────────────────────────────────

    async def acquire_lock(self, resource: str, ttl: int = 10) -> bool:
        """
        Try to acquire a distributed lock using SET NX EX.
        Returns True if lock acquired, False if already held.
        This is used for leader election and critical section protection.
        """
        key = LOCK_KEY.format(resource=resource)
        result = await self._client.set(key, "1", nx=True, ex=ttl)
        return result is not None

    async def release_lock(self, resource: str):
        key = LOCK_KEY.format(resource=resource)
        await self._client.delete(key)

    # ── Worker registry ────────────────────────────────────────────────────────

    async def register_worker(self, worker: WorkerInfo):
        key = WORKER_KEY.format(worker_id=worker.worker_id)
        await self._client.set(key, json.dumps(worker.model_dump(mode="json")), ex=30)
        await self._client.sadd(WORKER_SET_KEY, worker.worker_id)

    async def update_worker(self, worker: WorkerInfo):
        key = WORKER_KEY.format(worker_id=worker.worker_id)
        worker.last_heartbeat = datetime.utcnow()
        await self._client.set(key, json.dumps(worker.model_dump(mode="json")), ex=30)

    async def deregister_worker(self, worker_id: str):
        key = WORKER_KEY.format(worker_id=worker_id)
        await self._client.delete(key)
        await self._client.srem(WORKER_SET_KEY, worker_id)

    async def list_workers(self) -> List[WorkerInfo]:
        worker_ids = await self._client.smembers(WORKER_SET_KEY)
        workers = []
        for wid in worker_ids:
            data = await self._client.get(WORKER_KEY.format(worker_id=wid))
            if data:
                workers.append(WorkerInfo(**json.loads(data)))
            else:
                # worker TTL expired — clean up set
                await self._client.srem(WORKER_SET_KEY, wid)
        return workers
