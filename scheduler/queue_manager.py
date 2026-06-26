"""
scheduler/queue_manager.py
RabbitMQ integration for reliable task delivery.

Design:
  - Durable queues survive broker restarts
  - Manual acknowledgements — tasks only removed from queue after successful processing
  - Dead-letter exchange (DLX) — failed/expired tasks routed to dead_letter queue
  - Prefetch count = 1 — each worker gets one task at a time (fair dispatch)
"""

import json
import asyncio
import aio_pika
from typing import Callable, Awaitable, Optional
from models.task import Task


class QueueManager:

    def __init__(self, amqp_url: str, queue_name: str, dead_letter_queue: str):
        self._url              = amqp_url
        self._queue_name       = queue_name
        self._dead_letter_queue = dead_letter_queue
        self._connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self._channel:    Optional[aio_pika.abc.AbstractChannel]          = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel    = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=1)   # fair dispatch

        # declare dead-letter queue first
        await self._channel.declare_queue(
            self._dead_letter_queue,
            durable=True
        )

        # declare main queue with DLX routing
        await self._channel.declare_queue(
            self._queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self._dead_letter_queue,
                "x-message-ttl": 60000,   # messages expire after 60s → DLQ
            }
        )

    async def disconnect(self):
        if self._connection:
            await self._connection.close()

    async def publish(self, task: Task):
        """Publish a task to the queue with priority header."""
        if not self._channel:
            raise RuntimeError("QueueManager not connected")

        message = aio_pika.Message(
            body=json.dumps(task.to_dict()).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # survive broker restart
            priority=task.priority.value,
            headers={"task_id": task.id, "retries": task.retries}
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self._queue_name
        )

    async def consume(self, handler: Callable[[Task], Awaitable[None]]):
        """
        Start consuming tasks from the queue.
        handler is called for each task — ack on success, nack on failure.
        nack with requeue=False sends to dead-letter queue after max retries.
        """
        if not self._channel:
            raise RuntimeError("QueueManager not connected")

        queue = await self._channel.get_queue(self._queue_name)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process(requeue=False):
                    try:
                        data = json.loads(message.body.decode())
                        task = Task.from_dict(data)
                        await handler(task)
                    except Exception as e:
                        # message.process(requeue=False) auto-nacks on exception
                        # → routes to dead-letter queue
                        print(f"[QueueManager] Failed to process message: {e}")
                        raise

    async def publish_to_dead_letter(self, task: Task):
        """Explicitly send exhausted task to dead-letter queue."""
        if not self._channel:
            raise RuntimeError("QueueManager not connected")

        message = aio_pika.Message(
            body=json.dumps(task.to_dict()).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={"reason": "max_retries_exhausted", "task_id": task.id}
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self._dead_letter_queue
        )
