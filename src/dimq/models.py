from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


class TaskAttempt(BaseModel):
    attempt_number: int
    worker_id: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    status: TaskStatus = TaskStatus.RUNNING
    error: Optional[str] = None


class TaskRecord(BaseModel):
    task_id: str
    task_type: str
    payload: str  # JSON string
    status: TaskStatus = TaskStatus.PENDING
    attempts: List[TaskAttempt] = Field(default_factory=list)
    retry_count: int = 0
    created_at: datetime = Field(default_factory=datetime.now)
    result_payload: Optional[str] = None


class TaskConfig(BaseModel):
    name: str  # "module:function"
    max_retries: int = 3
    timeout_seconds: float = 60.0


class DimqConfig(BaseModel):
    endpoint: str = "tcp://0.0.0.0:5555"
    client_endpoint: Optional[str] = "tcp://0.0.0.0:5556"
    heartbeat_interval_seconds: int = 5
    heartbeat_timeout_missed: int = 3
    adaptive_window_seconds: float = 30.0
    adaptive_reprobe_seconds: float = 300.0
    tasks: List[TaskConfig] = Field(default_factory=list)
