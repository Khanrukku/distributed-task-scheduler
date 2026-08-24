"""
scheduler/queue_manager.py
RabbitMQ integration for reliable and concurrent task delivery.

Design:
  - Durable queues survive broker restarts
  - Persistent messages survive broker restarts
  - Manual acknowledgements remove messages only after successful processing
  - Dead-letter queue handles rejected/failed messages
  - RabbitMQ native priority queues support priority-based delivery
  - Configurable prefetch controls outstanding messages per consumer
  - Incoming messages are dispatched concurrently
"""

import asyncio
import json
from typing import Awaitable, Callable, Optional, Set

import aio_pika

from models.task import Task


# TaskPriority currently uses:
# LOW = 1
# MEDIUM = 5
# HIGH = 10
MAX_QUEUE_PRIORITY = 10


class QueueManager:
    """
    RabbitMQ-backed task queue manager.

    Responsible for:
      - declaring durable queues
      - publishing persistent task messages
      - priority-based task delivery
      - concurrent message dispatch
      - acknowledgement / rejection handling
      - dead-letter routing
    """

    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        dead_letter_queue: str,
        prefetch_count: int = 1,
    ):
        if prefetch_count <= 0:
            raise ValueError(
                "prefetch_count must be greater than zero"
            )

        self._url = amqp_url
        self._queue_name = queue_name
        self._dead_letter_queue = dead_letter_queue
        self._prefetch_count = prefetch_count

        self._connection: Optional[
            aio_pika.abc.AbstractRobustConnection
        ] = None

        self._channel: Optional[
            aio_pika.abc.AbstractChannel
        ] = None

        # Keeps strong references to in-flight processing tasks.
        self._inflight_tasks: Set[asyncio.Task] = set()

    # ── Connection lifecycle ──────────────────────────────────────────────────

    async def connect(self):
        """
        Connect to RabbitMQ and declare required queues.
        """
        self._connection = await aio_pika.connect_robust(
            self._url
        )

        self._channel = await self._connection.channel()

        await self._channel.set_qos(
            prefetch_count=self._prefetch_count
        )

        # Declare dead-letter queue first.
        await self._channel.declare_queue(
            self._dead_letter_queue,
            durable=True,
        )

        # Declare the main priority queue.
        #
        # Note:
        # There is intentionally NO x-message-ttl here.
        # Valid tasks are allowed to wait in the queue until a worker
        # is available instead of being dead-lettered simply because
        # they waited longer than an arbitrary time limit.
        await self._channel.declare_queue(
            self._queue_name,
            durable=True,
            arguments={
                "x-max-priority": MAX_QUEUE_PRIORITY,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key":
                    self._dead_letter_queue,
            },
        )

    async def disconnect(self):
        """
        Wait for in-flight message handlers and close RabbitMQ.
        """
        if self._inflight_tasks:
            await asyncio.gather(
                *self._inflight_tasks,
                return_exceptions=True,
            )

        if self._connection:
            await self._connection.close()

    # ── Publishing ─────────────────────────────────────────────────────────────

    async def publish(self, task: Task):
        """
        Publish a persistent task message with RabbitMQ priority.
        """
        if not self._channel:
            raise RuntimeError(
                "QueueManager not connected"
            )

        priority = int(task.priority.value)

        if priority < 0 or priority > MAX_QUEUE_PRIORITY:
            raise ValueError(
                f"Task priority must be between 0 and "
                f"{MAX_QUEUE_PRIORITY}, received {priority}."
            )

        message = aio_pika.Message(
            body=json.dumps(
                task.to_dict()
            ).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=priority,
            headers={
                "task_id": task.id,
                "retries": task.retries,
            },
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self._queue_name,
        )

    # ── Message processing ─────────────────────────────────────────────────────

    async def _process_message(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        handler: Callable[
            [Task],
            Awaitable[None],
        ],
    ):
        """
        Process one RabbitMQ message.

        Acknowledgement lifecycle:

          handler succeeds
              → ACK

          handler raises
              → reject / NACK with requeue=False
              → RabbitMQ dead-letter routing
        """
        async with message.process(
            requeue=False
        ):
            try:
                data = json.loads(
                    message.body.decode()
                )

                task = Task.from_dict(data)

                await handler(task)

            except Exception as exc:
                print(
                    "[QueueManager] Failed to process "
                    f"message: {exc}"
                )

                # Re-raise so message.process() rejects the
                # message rather than acknowledging it.
                raise

    # ── Consumption ────────────────────────────────────────────────────────────

    async def consume(
        self,
        handler: Callable[
            [Task],
            Awaitable[None],
        ],
    ):
        """
        Continuously consume messages from the main queue.

        Each delivery is dispatched into its own asyncio task,
        allowing multiple messages to be processed concurrently.

        Actual execution concurrency is bounded by:
          - RabbitMQ prefetch
          - the Worker's asyncio.Semaphore
        """
        if not self._channel:
            raise RuntimeError(
                "QueueManager not connected"
            )

        queue = await self._channel.get_queue(
            self._queue_name
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:

                processing_task = asyncio.create_task(
                    self._process_message(
                        message,
                        handler,
                    )
                )

                self._inflight_tasks.add(
                    processing_task
                )

                # Remove completed tasks from the tracking set.
                processing_task.add_done_callback(
                    self._inflight_tasks.discard
                )

    # ── Dead-letter publishing ────────────────────────────────────────────────

    async def publish_to_dead_letter(
        self,
        task: Task,
    ):
        """
        Explicitly publish a task to the dead-letter queue
        after application-level retry exhaustion.
        """
        if not self._channel:
            raise RuntimeError(
                "QueueManager not connected"
            )

        message = aio_pika.Message(
            body=json.dumps(
                task.to_dict()
            ).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={
                "reason": "max_retries_exhausted",
                "task_id": task.id,
                "retries": task.retries,
            },
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self._dead_letter_queue,
        )
