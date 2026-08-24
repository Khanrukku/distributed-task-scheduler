"""
utils/redis_store.py
Redis-backed task store with atomic operations and deduplication.

Key design decisions:
  - Lua scripting for atomic task creation + queue insertion
  - Redis sorted sets for priority queuing — O(log N) insert/pop by priority score
  - TTL on completed tasks — automatic cleanup without a separate GC process
  - Worker heartbeats stored with short-lived TTLs
"""

import json
import redis.asyncio as aioredis
from typing import Optional, List
from datetime import datetime, timezone

from models.task import Task, TaskStatus, WorkerInfo


TASK_KEY = "task:{task_id}"
TASK_SET_KEY = "tasks:all"          # sorted set: score = priority
WORKER_KEY = "worker:{worker_id}"
WORKER_SET_KEY = "workers:active"
LOCK_KEY = "lock:{resource}"
TASK_TTL = 3600                     # completed tasks expire after 1 hour


# Atomically:
# 1. Check whether the task already exists.
# 2. Create the task only if it does not exist.
# 3. Apply TTL.
# 4. Add the task to the priority queue.
#
# This prevents duplicate submissions from modifying queue state.
SAVE_TASK_SCRIPT = """
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end

redis.call(
    "SET",
    KEYS[1],
    ARGV[1],
    "EX",
    ARGV[2]
)

redis.call(
    "ZADD",
    KEYS[2],
    ARGV[3],
    ARGV[4]
)

return 1
"""


class RedisStore:
    """
    Central Redis store for task state and worker registry.
    """

    def __init__(self, redis_url: str):
        self._url = redis_url
        self._client: Optional[aioredis.Redis] = None

    async def connect(self):
        self._client = await aioredis.from_url(
            self._url,
            decode_responses=True,
        )

    async def disconnect(self):
        if self._client:
            await self._client.aclose()

    # ── Task operations ────────────────────────────────────────────────────────

    async def save_task(self, task: Task) -> bool:
        """
        Atomically save a new task and add it to the priority queue.

        Returns:
            True  -> task was newly created
            False -> task already existed

        Duplicate submissions do not refresh TTLs and do not modify
        the priority queue.
        """
        key = TASK_KEY.format(task_id=task.id)
        serialized = json.dumps(task.to_dict())

        result = await self._client.eval(
            SAVE_TASK_SCRIPT,
            2,
            key,
            TASK_SET_KEY,
            serialized,
            TASK_TTL * 24,
            task.priority.value,
            task.id,
        )

        return bool(result)

    async def get_task(self, task_id: str) -> Optional[Task]:
        key = TASK_KEY.format(task_id=task_id)
        data = await self._client.get(key)

        if not data:
            return None

        return Task.from_dict(json.loads(data))

    async def update_task(self, task: Task):
        """
        Update task state atomically.

        Terminal tasks receive a shorter TTL and are removed from
        the priority queue.
        """
        key = TASK_KEY.format(task_id=task.id)
        serialized = json.dumps(task.to_dict())

        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.set(key, serialized)

            if task.status in (TaskStatus.COMPLETED, TaskStatus.DEAD):
                await pipe.expire(key, TASK_TTL)
                await pipe.zrem(TASK_SET_KEY, task.id)

            await pipe.execute()

    async def pop_next_task(self) -> Optional[str]:
        """
        Pop the highest-priority task id from the queue.

        ZPOPMAX is atomic, preventing multiple workers from
        receiving the same queued task from this Redis structure.
        """
        result = await self._client.zpopmax(
            TASK_SET_KEY,
            count=1,
        )

        if not result:
            return None

        task_id, _score = result[0]
        return task_id

    async def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
    ) -> List[Task]:
        """
        List all tasks, optionally filtered by status.
        """
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

        return sorted(
            tasks,
            key=lambda task: task.created_at,
            reverse=True,
        )

    # ── Distributed lock ───────────────────────────────────────────────────────

    async def acquire_lock(
        self,
        resource: str,
        ttl: int = 10,
    ) -> bool:
        """
        Try to acquire a distributed lock using SET NX EX.

        Returns True if the lock is acquired, otherwise False.
        """
        key = LOCK_KEY.format(resource=resource)

        result = await self._client.set(
            key,
            "1",
            nx=True,
            ex=ttl,
        )

        return result is not None

    async def release_lock(self, resource: str):
        """
        Release the distributed lock for a resource.

        Note:
        This implementation currently deletes the lock key directly.
        A later hardening step will add ownership tokens so one client
        cannot accidentally release another client's lock.
        """
        key = LOCK_KEY.format(resource=resource)
        await self._client.delete(key)

    # ── Worker registry ────────────────────────────────────────────────────────

    async def register_worker(self, worker: WorkerInfo):
        key = WORKER_KEY.format(worker_id=worker.worker_id)

        await self._client.set(
            key,
            json.dumps(worker.model_dump(mode="json")),
            ex=30,
        )

        await self._client.sadd(
            WORKER_SET_KEY,
            worker.worker_id,
        )

    async def update_worker(self, worker: WorkerInfo):
        key = WORKER_KEY.format(worker_id=worker.worker_id)

        worker.last_heartbeat = datetime.now(timezone.utc)

        await self._client.set(
            key,
            json.dumps(worker.model_dump(mode="json")),
            ex=30,
        )

    async def deregister_worker(self, worker_id: str):
        key = WORKER_KEY.format(worker_id=worker_id)

        await self._client.delete(key)

        await self._client.srem(
            WORKER_SET_KEY,
            worker_id,
        )

    async def list_workers(self) -> List[WorkerInfo]:
        worker_ids = await self._client.smembers(WORKER_SET_KEY)

        workers = []

        for worker_id in worker_ids:
            data = await self._client.get(
                WORKER_KEY.format(worker_id=worker_id)
            )

            if data:
                workers.append(
                    WorkerInfo(**json.loads(data))
                )
            else:
                # Worker TTL expired — clean up stale registry membership.
                await self._client.srem(
                    WORKER_SET_KEY,
                    worker_id,
                )

        return workers
