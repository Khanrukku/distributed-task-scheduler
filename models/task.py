"""
models/task.py
Core data models for the distributed task scheduler.
"""

import uuid
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any


class TaskStatus(str, Enum):
    PENDING   = "pending"
    QUEUED    = "queued"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    DEAD      = "dead"        # exhausted all retries → dead-letter queue


class TaskPriority(int, Enum):
    LOW    = 1
    MEDIUM = 5
    HIGH   = 10


class Task(BaseModel):
    id:            str            = Field(default_factory=lambda: str(uuid.uuid4()))
    name:          str
    payload:       Dict[str, Any] = Field(default_factory=dict)
    priority:      TaskPriority   = TaskPriority.MEDIUM
    status:        TaskStatus     = TaskStatus.PENDING
    retries:       int            = 0
    max_retries:   int            = 3
    created_at:    datetime       = Field(default_factory=datetime.utcnow)
    started_at:    Optional[datetime] = None
    completed_at:  Optional[datetime] = None
    error:         Optional[str]  = None
    worker_id:     Optional[str]  = None
    result:        Optional[Any]  = None

    def to_dict(self) -> dict:
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        return cls(**data)


class TaskRequest(BaseModel):
    """Incoming API request to submit a task."""
    name:        str
    payload:     Dict[str, Any] = Field(default_factory=dict)
    priority:    TaskPriority   = TaskPriority.MEDIUM
    max_retries: int            = 3


class TaskResponse(BaseModel):
    """API response after task submission."""
    task_id: str
    status:  TaskStatus
    message: str


class WorkerInfo(BaseModel):
    """Metadata about a running worker node."""
    worker_id:    str
    status:       str       = "idle"   # idle | busy
    current_task: Optional[str] = None
    tasks_done:   int       = 0
    tasks_failed: int       = 0
    started_at:   datetime  = Field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
