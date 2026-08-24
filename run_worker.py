"""
run_worker.py

Entrypoint for starting the distributed worker pool.

Multiple instances of this process can be started on different
machines/containers to horizontally scale task execution.

Local concurrency capacity:

    num_workers × max_concurrent

This value is also used as the RabbitMQ prefetch count so the broker
can deliver enough messages to utilize the worker pool.
"""

import asyncio
import os

from dotenv import load_dotenv

from utils.redis_store import RedisStore
from scheduler.queue_manager import QueueManager
from worker.pool import WorkerPool


load_dotenv()


async def main():
    # ── Worker configuration ──────────────────────────────────────────────────

    num_workers = int(
        os.getenv(
            "MAX_WORKERS",
            "4",
        )
    )

    max_concurrent = int(
        os.getenv(
            "MAX_CONCURRENT_PER_WORKER",
            "2",
        )
    )

    if num_workers <= 0:
        raise ValueError(
            "MAX_WORKERS must be greater than zero"
        )

    if max_concurrent <= 0:
        raise ValueError(
            "MAX_CONCURRENT_PER_WORKER must be greater than zero"
        )

    # Maximum number of tasks this process is designed
    # to execute concurrently.
    total_concurrency = (
        num_workers * max_concurrent
    )

    # ── Infrastructure clients ────────────────────────────────────────────────

    redis_store = RedisStore(
        os.getenv(
            "REDIS_URL",
            "redis://localhost:6379",
        )
    )

    queue_manager = QueueManager(
        amqp_url=os.getenv(
            "RABBITMQ_URL",
            "amqp://guest:guest@localhost:5672/",
        ),
        queue_name=os.getenv(
            "TASK_QUEUE",
            "task_queue",
        ),
        dead_letter_queue=os.getenv(
            "DEAD_LETTER_QUEUE",
            "dead_letter",
        ),

        # Allow RabbitMQ to provide enough outstanding
        # messages to utilize the local worker capacity.
        prefetch_count=total_concurrency,
    )

    # ── Connect infrastructure ────────────────────────────────────────────────

    await redis_store.connect()
    await queue_manager.connect()

    # ── Create worker pool ────────────────────────────────────────────────────

    pool = WorkerPool(
        redis_store=redis_store,
        queue_manager=queue_manager,
        num_workers=num_workers,
        max_concurrent=max_concurrent,
    )

    print(
        "[WorkerRuntime] Configuration: "
        f"{num_workers} workers × "
        f"{max_concurrent} concurrent tasks "
        f"= {total_concurrency} task slots"
    )

    # ── Start worker runtime ──────────────────────────────────────────────────

    try:
        await pool.start()

    except KeyboardInterrupt:
        print(
            "\n[WorkerRuntime] Shutdown requested"
        )

    finally:
        await pool.stop()
        await redis_store.disconnect()
        await queue_manager.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
