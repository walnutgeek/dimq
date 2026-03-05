from dimq.models import (
    TaskStatus,
    TaskAttempt,
    TaskRecord,
    TaskConfig,
    DimqConfig,
)
from datetime import datetime


def test_task_config_defaults():
    cfg = TaskConfig(name="mod:func")
    assert cfg.max_retries == 3
    assert cfg.timeout_seconds == 60.0


def test_task_record_initial_state():
    rec = TaskRecord(task_id="t1", task_type="mod:func", payload='{"x": 1}')
    assert rec.status == TaskStatus.PENDING
    assert rec.attempts == []
    assert rec.retry_count == 0


def test_task_attempt_records():
    attempt = TaskAttempt(
        attempt_number=1,
        worker_id="w1",
        started_at=datetime.now(),
        status=TaskStatus.COMPLETED,
    )
    assert attempt.error is None


def test_dimq_config_from_dict():
    data = {
        "endpoint": "tcp://0.0.0.0:5555",
        "heartbeat_interval_seconds": 5,
        "heartbeat_timeout_missed": 3,
        "tasks": [
            {"name": "mod:func", "max_retries": 2, "timeout_seconds": 10},
        ],
    }
    cfg = DimqConfig(**data)
    assert cfg.endpoint == "tcp://0.0.0.0:5555"
    assert len(cfg.tasks) == 1
    assert cfg.tasks[0].max_retries == 2
