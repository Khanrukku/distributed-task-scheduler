"""
api/main.py
FastAPI REST API for the distributed task scheduler.

Endpoints:
  POST /tasks          — submit a new task
  GET  /tasks          — list all tasks (optional ?status= filter)
  GET  /tasks/{id}     — get task status and result
  GET  /workers        — list active workers and their state
  GET  /health         — health check
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List

from models.task import Task, TaskStatus, TaskRequest, TaskResponse
from utils.redis_store import RedisStore
from scheduler.queue_manager import QueueManager


# ── App lifecycle ─────────────────────────────────────────────────────────────

redis_store   = RedisStore(os.getenv("REDIS_URL", "redis://localhost:6379"))
queue_manager = QueueManager(
    amqp_url          = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
    queue_name        = os.getenv("TASK_QUEUE", "task_queue"),
    dead_letter_queue = os.getenv("DEAD_LETTER_QUEUE", "dead_letter"),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await redis_store.connect()
    await queue_manager.connect()
    yield
    await redis_store.disconnect()
    await queue_manager.disconnect()


app = FastAPI(
    title       = "Distributed Task Scheduler",
    description = "High-throughput task scheduler with worker pool, Redis, and RabbitMQ",
    version     = "1.0.0",
    lifespan    = lifespan,
)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.post("/tasks", response_model=TaskResponse, status_code=201)
async def submit_task(request: TaskRequest):
    """
    Submit a new task to the scheduler.
    Deduplication via Redis SETNX — duplicate task IDs are rejected.
    """
    task = Task(
        name        = request.name,
        payload     = request.payload,
        priority    = request.priority,
        max_retries = request.max_retries,
    )

    created = await redis_store.save_task(task)
    if not created:
        raise HTTPException(status_code=409, detail=f"Task {task.id} already exists")

    task.status = TaskStatus.QUEUED
    await redis_store.update_task(task)
    await queue_manager.publish(task)

    return TaskResponse(
        task_id = task.id,
        status  = task.status,
        message = f"Task '{task.name}' queued successfully",
    )


@app.get("/tasks", response_model=List[Task])
async def list_tasks(status: Optional[TaskStatus] = Query(None)):
    """List all tasks, optionally filtered by status."""
    return await redis_store.list_tasks(status=status)


@app.get("/tasks/{task_id}", response_model=Task)
async def get_task(task_id: str):
    """Get full task details including status, result, and error."""
    task = await redis_store.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return task


@app.get("/workers")
async def list_workers():
    """List all active workers and their current state."""
    workers = await redis_store.list_workers()
    return {
        "total":   len(workers),
        "workers": [w.model_dump() for w in workers],
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "task-scheduler-api"}
