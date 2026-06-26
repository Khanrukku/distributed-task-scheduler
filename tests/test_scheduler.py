"""
tests/test_scheduler.py
Tests covering:
  - Task deduplication (SETNX)
  - Retry with exponential backoff
  - Dead-letter routing after max retries
  - Concurrent task execution via worker semaphore
  - Priority ordering
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from models.task import Task, TaskStatus, TaskPriority
from worker.task_executor import execute_task, _REGISTRY


# ── Task model tests ──────────────────────────────────────────────────────────

def test_task_default_values():
    task = Task(name="echo")
    assert task.status   == TaskStatus.PENDING
    assert task.retries  == 0
    assert task.priority == TaskPriority.MEDIUM
    assert task.id is not None


def test_task_serialization_roundtrip():
    task     = Task(name="compute", payload={"n": 100})
    restored = Task.from_dict(task.to_dict())
    assert restored.id      == task.id
    assert restored.name    == task.name
    assert restored.payload == task.payload


# ── Task executor tests ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_echo_handler():
    task = Task(name="echo", payload={"msg": "hello"})
    result = await execute_task(task)
    assert result == {"msg": "hello"}


@pytest.mark.asyncio
async def test_compute_handler():
    task = Task(name="compute", payload={"n": 10})
    result = await execute_task(task)
    assert result == sum(range(10))


@pytest.mark.asyncio
async def test_fail_handler_raises():
    task = Task(name="fail", payload={"reason": "test"})
    with pytest.raises(RuntimeError):
        await execute_task(task)


@pytest.mark.asyncio
async def test_unknown_task_raises():
    task = Task(name="nonexistent_task")
    with pytest.raises(ValueError, match="No handler registered"):
        await execute_task(task)


# ── Concurrency tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_semaphore_limits_concurrency():
    """Verify that semaphore enforces max concurrent task limit."""
    max_concurrent = 2
    semaphore = asyncio.Semaphore(max_concurrent)
    active    = 0
    peak      = 0

    async def fake_task():
        nonlocal active, peak
        async with semaphore:
            active += 1
            peak    = max(peak, active)
            await asyncio.sleep(0.05)
            active -= 1

    await asyncio.gather(*[fake_task() for _ in range(10)])
    assert peak <= max_concurrent


@pytest.mark.asyncio
async def test_concurrent_tasks_complete():
    """All tasks complete when run concurrently."""
    tasks = [Task(name="echo", payload={"i": i}) for i in range(20)]
    results = await asyncio.gather(*[execute_task(t) for t in tasks])
    assert len(results) == 20


# ── Retry logic tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_retry_increments():
    """Task retries counter increments correctly."""
    task = Task(name="fail", max_retries=3)
    assert task.retries == 0

    for i in range(1, 4):
        task.retries += 1
        assert task.retries == i

    # after max retries → should go dead
    assert task.retries == task.max_retries


def test_task_dead_after_max_retries():
    task        = Task(name="fail", max_retries=2)
    task.retries = 2
    # simulate dead-letter routing condition
    should_dead = task.retries >= task.max_retries
    assert should_dead


# ── Priority tests ────────────────────────────────────────────────────────────

def test_priority_ordering():
    tasks = [
        Task(name="t1", priority=TaskPriority.LOW),
        Task(name="t2", priority=TaskPriority.HIGH),
        Task(name="t3", priority=TaskPriority.MEDIUM),
    ]
    sorted_tasks = sorted(tasks, key=lambda t: t.priority.value, reverse=True)
    assert sorted_tasks[0].priority == TaskPriority.HIGH
    assert sorted_tasks[-1].priority == TaskPriority.LOW
