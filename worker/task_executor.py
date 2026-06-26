"""
worker/task_executor.py
Pluggable task execution engine.
Register task handlers by name — the worker dispatches to the right handler.

This design mirrors how real distributed systems (Celery, Temporal, etc.)
separate task routing from task logic.
"""

import asyncio
from typing import Callable, Awaitable, Dict, Any
from models.task import Task


# ── Handler registry ──────────────────────────────────────────────────────────

TaskHandler = Callable[[Dict[str, Any]], Awaitable[Any]]
_REGISTRY:   Dict[str, TaskHandler] = {}


def register(name: str):
    """Decorator to register a task handler by name."""
    def decorator(fn: TaskHandler):
        _REGISTRY[name] = fn
        return fn
    return decorator


# ── Built-in task handlers ────────────────────────────────────────────────────

@register("sleep")
async def handle_sleep(payload: Dict[str, Any]) -> str:
    """Simulates a time-consuming task. Used for load testing."""
    duration = payload.get("duration", 1)
    await asyncio.sleep(duration)
    return f"Slept for {duration}s"


@register("compute")
async def handle_compute(payload: Dict[str, Any]) -> int:
    """CPU-bound simulation — sum of range."""
    n = payload.get("n", 1000)
    # run in executor to avoid blocking the event loop
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, lambda: sum(range(n)))
    return result


@register("echo")
async def handle_echo(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Returns the payload unchanged. Used for integration testing."""
    return payload


@register("fail")
async def handle_fail(payload: Dict[str, Any]):
    """Always fails — used to test retry and dead-letter logic."""
    raise RuntimeError(f"Intentional failure: {payload.get('reason', 'test')}")


# ── Dispatcher ────────────────────────────────────────────────────────────────

async def execute_task(task: Task) -> Any:
    """
    Dispatch task to the registered handler.
    Raises ValueError if handler not found — task will be retried/dead-lettered.
    """
    handler = _REGISTRY.get(task.name)
    if not handler:
        raise ValueError(f"No handler registered for task type '{task.name}'")
    return await handler(task.payload)
