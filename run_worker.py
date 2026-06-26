"""
run_worker.py
Entrypoint to start a worker pool.
Run multiple instances of this on different machines for true distribution.
"""

import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

from utils.redis_store import RedisStore
from scheduler.queue_manager import QueueManager
from worker.pool import WorkerPool


async def main():
    redis_store = RedisStore(os.getenv("REDIS_URL", "redis://localhost:6379"))
    queue_manager = QueueManager(
        amqp_url          = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
        queue_name        = os.getenv("TASK_QUEUE", "task_queue"),
        dead_letter_queue = os.getenv("DEAD_LETTER_QUEUE", "dead_letter"),
    )

    await redis_store.connect()
    await queue_manager.connect()

    pool = WorkerPool(
        redis_store   = redis_store,
        queue_manager = queue_manager,
        num_workers   = int(os.getenv("MAX_WORKERS", 4)),
        max_concurrent = 2,
    )

    try:
        await pool.start()
    except KeyboardInterrupt:
        await pool.stop()
    finally:
        await redis_store.disconnect()
        await queue_manager.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
