"""
scheduler/queue_manager.py
RabbitMQ integration for reliable task delivery.

Design:
  - Durable queues survive broker restarts
  - Persistent messages survive broker restarts
  - Manual acknowledgements remove messages only after successful processing
  - Dead-letter queue handles rejected/expired messages
  - RabbitMQ native priority queues support priority-based delivery
  - Prefetch count = 1 provides fair dispatch between consumers
"""

import json
from typing import Awaitable, Callable, Optional

import aio_pika

from models.task import Task


# TaskPriority currently uses:
# LOW = 1
# MEDIUM = 5
# HIGH = 10
#
# RabbitMQ must explicitly be configured with x-max-priority
# or the priority field on published messages will not affect delivery order.
MAX_QUEUE_PRIORITY = 10


class QueueManager:
    """
    RabbitMQ-backed task queue manager.

    Responsible for:
      - declaring durable queues
      - publishing persistent task messages
      - priority-based task delivery
      - consuming and acknowledging tasks
      - dead-letter routing
    """

    def __init__(
        self,
        amqp_url: str,
        queue_name: str,
        dead_letter_queue: str,
    ):
        self._url = amqp_url
        self._queue_name = queue_name
        self._dead_letter_queue = dead_letter_queue

        self._connection: Optional[
            aio_pika.abc.AbstractRobustConnection
        ] = None

        self._channel: Optional[
            aio_pika.abc.AbstractChannel
        ] = None

    # ── Connection lifecycle ──────────────────────────────────────────────────

    async def connect(self):
        """
        Connect to RabbitMQ and declare the required queues.
        """
        self._connection = await aio_pika.connect_robust(
            self._url
        )

        self._channel = await self._connection.channel()

        # Limit the number of unacknowledged messages delivered
        # to each consumer.
        await self._channel.set_qos(
            prefetch_count=1
        )

        # Declare dead-letter queue first.
        await self._channel.declare_queue(
            self._dead_letter_queue,
            durable=True,
        )

        # Declare the primary task queue.
        #
        # x-max-priority is REQUIRED for RabbitMQ to actually
        # honor the `priority` field attached to messages.
        await self._channel.declare_queue(
            self._queue_name,
            durable=True,
            arguments={
                "x-max-priority": MAX_QUEUE_PRIORITY,

                "x-dead-letter-exchange": "",

                "x-dead-letter-routing-key":
                    self._dead_letter_queue,

                # Queue messages waiting longer than 60 seconds
                # are automatically dead-lettered.
                #
                # We will review whether this TTL is desirable
                # in a later reliability-hardening step.
                "x-message-ttl": 60000,
            },
        )

    async def disconnect(self):
        """
        Close the RabbitMQ connection.
        """
        if self._connection:
            await self._connection.close()

    # ── Publishing ─────────────────────────────────────────────────────────────

    async def publish(self, task: Task):
        """
        Publish a task to the main RabbitMQ queue.

        The message is persistent and carries the task priority.

        Priority values are interpreted by RabbitMQ because
        the queue is declared with x-max-priority.
        """
        if not self._channel:
            raise RuntimeError(
                "QueueManager not connected"
            )

        priority = int(task.priority.value)

        # Defensive validation in case task priority definitions
        # change in the future.
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

    # ── Consumption ────────────────────────────────────────────────────────────

    async def consume(
        self,
        handler: Callable[
            [Task],
            Awaitable[None],
        ],
    ):
        """
        Consume tasks from the main queue.

        Successful handler execution:
            message is acknowledged.

        Handler exception:
            message is negatively acknowledged with requeue=False,
            allowing RabbitMQ dead-letter routing to handle it.
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
                        # message.process(requeue=False) will reject
                        # the message when this exception escapes.
                        #
                        # Because the main queue has dead-letter
                        # configuration, RabbitMQ routes the rejected
                        # message to the dead-letter queue.
                        print(
                            "[QueueManager] Failed to "
                            f"process message: {exc}"
                        )

                        raise

    # ── Dead-letter publishing ─────────────────────────────────────────────────

    async def publish_to_dead_letter(
        self,
        task: Task,
    ):
        """
        Explicitly publish a task to the dead-letter queue after
        application-level retry exhaustion.
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
