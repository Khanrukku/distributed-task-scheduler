"""
utils/redis_store.py
Redis-backed task store with atomic operations, deduplication,
priority queuing, distributed locking, and worker heartbeats.

Key design decisions:
  - Lua scripting for atomic task creation + queue insertion
  - Redis sorted sets for priority queuing
  - Ownership tokens for safe distributed lock release
  - TTL on completed tasks for automatic cleanup
  - Worker heartbeat TTLs for stale-worker detection
"""

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import redis.asyncio as aioredis

from models.task import Task, TaskStatus, WorkerInfo


TASK_KEY = "task:{task_id}"
TASK_SET_KEY = "tasks:all"          # sorted set: score = priority
WORKER_KEY = "worker:{worker_id}"
WORKER_SET_KEY = "workers:active"
LOCK_KEY = "lock:{resource}"

TASK_TTL = 3600                     # completed tasks expire after 1 hour
PENDING_TASK_TTL = TASK_TTL * 24    # pending/non-terminal tasks kept for 24h


# ── Atomic task creation ──────────────────────────────────────────────────────
#
# The complete operation runs inside Redis as one atomic Lua script.
#
# If the task already exists:
#   - its TTL is NOT refreshed
#   - its priority is NOT modified
#   - it is NOT inserted into the queue again
#
# If it is new:
#   1. create task record
#   2. assign TTL
#   3. insert task into priority sorted set
#
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


# ── Safe distributed-lock release ─────────────────────────────────────────────
#
# A lock may only be deleted when its stored ownership token matches the token
# supplied by the caller.
#
# This prevents a worker whose lock expired from accidentally deleting a newer
# lock acquired by another worker.
#
RELEASE_LOCK_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end

return 0
"""


class RedisStore:
    """
    Central Redis store for task state and worker registry.

    Redis atomic operations and Lua scripts are used where multiple
    state changes must behave as one logical operation.
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
        Atomically create a task and insert it into the priority queue.

        Returns:
            True:
                Task was newly created.

            False:
                A task with this ID already existed.

        Duplicate submissions do not alter the existing task, refresh
        its TTL, or modify its queue priority.
        """
        key = TASK_KEY.format(task_id=task.id)
        serialized = json.dumps(task.to_dict())

        result = await self._client.eval(
            SAVE_TASK_SCRIPT,
            2,
            key,
            TASK_SET_KEY,
            serialized,
            PENDING_TASK_TTL,
            task.priority.value,
            task.id,
        )

        return bool(result)

    async def get_task(self, task_id: str) -> Optional[Task]:
        """
        Retrieve a task from Redis by ID.
        """
        key = TASK_KEY.format(task_id=task_id)

        data = await self._client.get(key)

        if not data:
            return None

        return Task.from_dict(
            json.loads(data)
        )

    async def update_task(self, task: Task):
        """
        Update task state atomically.

        Terminal tasks receive a shorter TTL and are removed from the
        priority sorted set.
        """
        key = TASK_KEY.format(task_id=task.id)
        serialized = json.dumps(task.to_dict())

        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.set(
                key,
                serialized,
            )

            if task.status in (
                TaskStatus.COMPLETED,
                TaskStatus.DEAD,
            ):
                await pipe.expire(
                    key,
                    TASK_TTL,
                )

                await pipe.zrem(
                    TASK_SET_KEY,
                    task.id,
                )

            await pipe.execute()

    async def pop_next_task(self) -> Optional[str]:
        """
        Atomically remove and return the highest-priority task ID.

        ZPOPMAX prevents two consumers using this Redis queue from
        popping the same queue entry.
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
        Return tasks currently stored in Redis.

        A TaskStatus may optionally be supplied to filter the results.
        """
        keys = []

        async for key in self._client.scan_iter("task:*"):
            keys.append(key)

        tasks = []

        for key in keys:
            data = await self._client.get(key)

            if not data:
                continue

            task = Task.from_dict(
                json.loads(data)
            )

            if status is None or task.status == status:
                tasks.append(task)

        return sorted(
            tasks,
            key=lambda task: task.created_at,
            reverse=True,
        )

    # ── Distributed locking ────────────────────────────────────────────────────

    async def acquire_lock(
        self,
        resource: str,
        ttl: int = 10,
    ) -> Optional[str]:
        """
        Attempt to acquire a distributed lock.

        A cryptographically-random UUID token identifies the lock owner.

        Redis SET NX EX guarantees that the lock is created only when
        the resource is currently unlocked.

        Args:
            resource:
                Logical name of the resource being protected.

            ttl:
                Lock lifetime in seconds. The TTL prevents a dead worker
                from leaving a permanent lock behind.

        Returns:
            Ownership token when the lock is successfully acquired.

            None when another worker already owns the lock.

        The returned token MUST be supplied to release_lock().
        """
        if ttl <= 0:
            raise ValueError("Lock TTL must be greater than zero.")

        key = LOCK_KEY.format(
            resource=resource
        )

        token = str(uuid.uuid4())

        acquired = await self._client.set(
            key,
            token,
            nx=True,
            ex=ttl,
        )

        if acquired is None:
            return None

        return token

    async def release_lock(
        self,
        resource: str,
        token: str,
    ) -> bool:
        """
        Release a distributed lock only when the caller still owns it.

        A Lua compare-and-delete operation is used so the ownership
        check and deletion occur atomically.

        This prevents the following race:

            Worker A acquires lock
                    ↓
            A's lock expires
                    ↓
            Worker B acquires new lock
                    ↓
            Worker A tries to release old lock
                    ↓
            B's lock must NOT be deleted

        Returns:
            True:
                Lock existed, token matched, and lock was released.

            False:
                Lock no longer existed or belonged to another owner.
        """
        if not token:
            return False

        key = LOCK_KEY.format(
            resource=resource
        )

        result = await self._client.eval(
            RELEASE_LOCK_SCRIPT,
            1,
            key,
            token,
        )

        return bool(result)

    # ── Worker registry ────────────────────────────────────────────────────────

    async def register_worker(
        self,
        worker: WorkerInfo,
    ):
        """
        Register a worker and create its heartbeat record.
        """
        key = WORKER_KEY.format(
            worker_id=worker.worker_id
        )

        await self._client.set(
            key,
            json.dumps(
                worker.model_dump(mode="json")
            ),
            ex=30,
        )

        await self._client.sadd(
            WORKER_SET_KEY,
            worker.worker_id,
        )

    async def update_worker(
        self,
        worker: WorkerInfo,
    ):
        """
        Refresh worker state and heartbeat timestamp.
        """
        key = WORKER_KEY.format(
            worker_id=worker.worker_id
        )

        worker.last_heartbeat = datetime.now(
            timezone.utc
        )

        await self._client.set(
            key,
            json.dumps(
                worker.model_dump(mode="json")
            ),
            ex=30,
        )

    async def deregister_worker(
        self,
        worker_id: str,
    ):
        """
        Remove a worker from the registry.
        """
        key = WORKER_KEY.format(
            worker_id=worker_id
        )

        await self._client.delete(key)

        await self._client.srem(
            WORKER_SET_KEY,
            worker_id,
        )

    async def list_workers(self) -> List[WorkerInfo]:
        """
        Return currently active workers.

        If a worker heartbeat key has expired, its stale ID is removed
        from the active-worker set.
        """
        worker_ids = await self._client.smembers(
            WORKER_SET_KEY
        )

        workers = []

        for worker_id in worker_ids:
            data = await self._client.get(
                WORKER_KEY.format(
                    worker_id=worker_id
                )
            )

            if data:
                workers.append(
                    WorkerInfo(
                        **json.loads(data)
                    )
                )

            else:
                # Heartbeat TTL expired — remove stale membership.
                await self._client.srem(
                    WORKER_SET_KEY,
                    worker_id,
                )

        return workers
