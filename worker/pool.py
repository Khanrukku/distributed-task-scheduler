"""
worker/pool.py
Worker pool — manages N concurrent worker instances.
Each worker runs its own async event loop for true concurrency.
"""

import asyncio
from typing import List
from utils.redis_store import RedisStore
from scheduler.queue_manager import QueueManager
from worker.worker import Worker


class WorkerPool:
    """
    Spins up and manages multiple Worker instances.
    All workers compete on the same queue (competing consumers pattern).
    """

    def __init__(
        self,
        redis_store:    RedisStore,
        queue_manager:  QueueManager,
        num_workers:    int = 4,
        max_concurrent: int = 2,
    ):
        self._store   = redis_store
        self._queue   = queue_manager
        self._workers: List[Worker] = [
            Worker(redis_store, queue_manager, max_concurrent)
            for _ in range(num_workers)
        ]

    async def start(self):
        print(f"[WorkerPool] Starting {len(self._workers)} workers...")
        await asyncio.gather(*[w.start() for w in self._workers])

    async def stop(self):
        print("[WorkerPool] Shutting down workers...")
        await asyncio.gather(*[w.stop() for w in self._workers])

    @property
    def worker_ids(self) -> List[str]:
        return [w.worker_id for w in self._workers]
