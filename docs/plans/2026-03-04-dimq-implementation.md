# DIMQ Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a distributed in-memory queue with adaptive worker parallelization over ZeroMQ.

**Architecture:** Orchestrator runs two ZMQ ROUTER sockets (worker + client). Workers connect, register with CPU count, and pull tasks. Orchestrator tracks throughput and adjusts per-worker parallelization factor. Tasks are plain Python functions with Pydantic-typed I/O, introspected via `inspect`.

**Tech Stack:** Python 3.12+, uv, pyzmq, pydantic, pyyaml, structlog, pytest, pytest-asyncio. Rust (pyo3, maturin, rayon) for LoadTask.

**Design doc:** `docs/plans/2026-03-04-dimq-design.md`

---

### Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/dimq/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

**Step 1: Create pyproject.toml**

```toml
[project]
name = "dimq"
version = "0.1.0"
description = "Distributed In-Memory Queue"
requires-python = ">=3.12"
dependencies = [
    "pyzmq>=26.0",
    "pydantic>=2.0",
    "pyyaml>=6.0",
    "structlog>=24.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "pytest-timeout>=2.3",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/dimq"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

**Step 2: Create package init**

```python
# src/dimq/__init__.py
```

Empty file.

**Step 3: Create test scaffolding**

```python
# tests/__init__.py
```

```python
# tests/conftest.py
import pytest
```

**Step 4: Install dependencies**

Run: `cd /Users/sergeyk/w/DIMQ && uv sync --all-extras`
Expected: Dependencies installed successfully.

**Step 5: Verify pytest runs**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest --co -q`
Expected: "no tests ran" (no errors)

**Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/ tests/
git commit -m "chore: scaffold project with uv, pytest, pyzmq, pydantic"
```

---

### Task 2: Shared Models

**Files:**
- Create: `src/dimq/models.py`
- Create: `tests/test_models.py`

**Step 1: Write the failing test**

```python
# tests/test_models.py
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
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_models.py -v`
Expected: FAIL (ImportError)

**Step 3: Write minimal implementation**

```python
# src/dimq/models.py
from __future__ import annotations

from datetime import datetime
from enum import Enum

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
    ended_at: datetime | None = None
    status: TaskStatus = TaskStatus.RUNNING
    error: str | None = None


class TaskRecord(BaseModel):
    task_id: str
    task_type: str
    payload: str  # JSON string
    status: TaskStatus = TaskStatus.PENDING
    attempts: list[TaskAttempt] = Field(default_factory=list)
    retry_count: int = 0
    created_at: datetime = Field(default_factory=datetime.now)
    result_payload: str | None = None


class TaskConfig(BaseModel):
    name: str  # "module:function"
    max_retries: int = 3
    timeout_seconds: float = 60.0


class DimqConfig(BaseModel):
    endpoint: str = "tcp://0.0.0.0:5555"
    client_endpoint: str = "tcp://0.0.0.0:5556"
    heartbeat_interval_seconds: int = 5
    heartbeat_timeout_missed: int = 3
    adaptive_window_seconds: float = 30.0
    adaptive_reprobe_seconds: float = 300.0
    tasks: list[TaskConfig] = Field(default_factory=list)
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_models.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add src/dimq/models.py tests/test_models.py
git commit -m "feat: add shared Pydantic models for tasks, config, and attempts"
```

---

### Task 3: Config Loading

**Files:**
- Create: `src/dimq/config.py`
- Create: `tests/test_config.py`

**Step 1: Write the failing test**

```python
# tests/test_config.py
import tempfile
from pathlib import Path

import yaml

from dimq.config import load_config


def test_load_config_from_yaml(tmp_path: Path):
    config_data = {
        "endpoint": "tcp://0.0.0.0:9999",
        "heartbeat_interval_seconds": 10,
        "heartbeat_timeout_missed": 5,
        "tasks": [
            {"name": "my_mod:my_func", "max_retries": 2, "timeout_seconds": 15},
        ],
    }
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(config_data))

    cfg = load_config(config_file)
    assert cfg.endpoint == "tcp://0.0.0.0:9999"
    assert cfg.heartbeat_interval_seconds == 10
    assert len(cfg.tasks) == 1
    assert cfg.tasks[0].name == "my_mod:my_func"
    assert cfg.tasks[0].timeout_seconds == 15


def test_load_config_defaults(tmp_path: Path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump({}))

    cfg = load_config(config_file)
    assert cfg.endpoint == "tcp://0.0.0.0:5555"
    assert cfg.heartbeat_interval_seconds == 5
    assert cfg.tasks == []
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_config.py -v`
Expected: FAIL (ImportError)

**Step 3: Write minimal implementation**

```python
# src/dimq/config.py
from pathlib import Path

import yaml

from dimq.models import DimqConfig


def load_config(path: Path) -> DimqConfig:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return DimqConfig(**data)
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_config.py -v`
Expected: All 2 tests PASS

**Step 5: Commit**

```bash
git add src/dimq/config.py tests/test_config.py
git commit -m "feat: add YAML config loading"
```

---

### Task 4: Task Introspection & Loading

**Files:**
- Create: `src/dimq/task.py`
- Create: `tests/test_task.py`
- Create: `tests/sample_tasks.py` (test fixtures)

**Step 1: Create sample task fixtures**

```python
# tests/sample_tasks.py
from pydantic import BaseModel


class EchoInput(BaseModel):
    message: str


class EchoOutput(BaseModel):
    echoed: str


def echo(input: EchoInput) -> EchoOutput:
    return EchoOutput(echoed=input.message)


async def async_echo(input: EchoInput) -> EchoOutput:
    return EchoOutput(echoed=input.message)


def bad_no_annotations(x):
    return x
```

**Step 2: Write the failing test**

```python
# tests/test_task.py
import pytest
from pydantic import BaseModel

from dimq.task import load_task_func, TaskFunc
from tests.sample_tasks import EchoInput, EchoOutput


def test_load_sync_task():
    tf = load_task_func("tests.sample_tasks:echo")
    assert tf.input_model is EchoInput
    assert tf.output_model is EchoOutput
    assert tf.is_async is False

    result = tf.func(EchoInput(message="hello"))
    assert result == EchoOutput(echoed="hello")


def test_load_async_task():
    tf = load_task_func("tests.sample_tasks:async_echo")
    assert tf.is_async is True


def test_load_task_bad_annotations():
    with pytest.raises(ValueError, match="type annotation"):
        load_task_func("tests.sample_tasks:bad_no_annotations")


def test_load_task_module_not_found():
    with pytest.raises(ImportError):
        load_task_func("nonexistent_module:func")


def test_load_task_func_not_found():
    with pytest.raises(AttributeError):
        load_task_func("tests.sample_tasks:nonexistent")
```

**Step 3: Run test to verify it fails**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_task.py -v`
Expected: FAIL (ImportError)

**Step 4: Write minimal implementation**

```python
# src/dimq/task.py
from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from typing import Any, Callable, get_type_hints

from pydantic import BaseModel


@dataclass
class TaskFunc:
    func: Callable
    input_model: type[BaseModel]
    output_model: type[BaseModel]
    is_async: bool


def load_task_func(task_name: str) -> TaskFunc:
    """Load a task function from 'module.path:function_name' string.

    Uses inspect to extract Pydantic input/output types and async status.
    """
    module_path, func_name = task_name.rsplit(":", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)

    hints = get_type_hints(func)
    params = inspect.signature(func).parameters

    # Get input type from first parameter
    param_names = list(params.keys())
    if not param_names or param_names[0] not in hints:
        raise ValueError(
            f"Task '{task_name}': first parameter must have a Pydantic type annotation"
        )
    input_model = hints[param_names[0]]

    # Get output type from return annotation
    if "return" not in hints:
        raise ValueError(
            f"Task '{task_name}': must have a return type annotation"
        )
    output_model = hints["return"]

    return TaskFunc(
        func=func,
        input_model=input_model,
        output_model=output_model,
        is_async=inspect.iscoroutinefunction(func),
    )
```

**Step 5: Run test to verify it passes**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_task.py -v`
Expected: All 5 tests PASS

**Step 6: Commit**

```bash
git add src/dimq/task.py tests/test_task.py tests/sample_tasks.py
git commit -m "feat: add task introspection - load functions and extract Pydantic types via inspect"
```

---

### Task 5: Adaptive Parallelization Logic

**Files:**
- Create: `src/dimq/adaptive.py`
- Create: `tests/test_adaptive.py`

**Step 1: Write the failing test**

```python
# tests/test_adaptive.py
import time

from dimq.adaptive import AdaptiveController


def test_initial_factor_from_cpu_count():
    ctrl = AdaptiveController(cpu_count=4, window_seconds=0.1)
    assert ctrl.factor == 4


def test_increase_on_throughput_gain():
    ctrl = AdaptiveController(cpu_count=1, window_seconds=0.05)
    # Window 1: 5 completions
    for _ in range(5):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    # Throughput > 0, should increase
    assert ctrl.factor == 2

    # Window 2: 15 completions (higher throughput)
    for _ in range(15):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == 3


def test_decrease_on_throughput_plateau():
    ctrl = AdaptiveController(cpu_count=2, window_seconds=0.05)

    # Window 1: establish baseline
    for _ in range(10):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    prev_factor = ctrl.factor

    # Window 2: same or lower throughput
    for _ in range(5):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == prev_factor - 1


def test_minimum_factor_is_one():
    ctrl = AdaptiveController(cpu_count=1, window_seconds=0.05)
    # No completions — throughput drops
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor >= 1
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_adaptive.py -v`
Expected: FAIL (ImportError)

**Step 3: Write minimal implementation**

```python
# src/dimq/adaptive.py
from __future__ import annotations

import time


class AdaptiveController:
    def __init__(
        self,
        cpu_count: int,
        window_seconds: float = 30.0,
        reprobe_seconds: float = 300.0,
    ):
        self.factor: int = cpu_count
        self.window_seconds = window_seconds
        self.reprobe_seconds = reprobe_seconds

        self._completions: int = 0
        self._window_start: float = time.monotonic()
        self._prev_throughput: float = 0.0
        self._steady: bool = False
        self._steady_since: float = 0.0

    def record_completion(self) -> None:
        self._completions += 1

    def evaluate(self) -> None:
        now = time.monotonic()
        elapsed = now - self._window_start
        if elapsed <= 0:
            return

        throughput = self._completions / elapsed

        if self._steady:
            # In steady mode, check if it's time to re-probe
            if now - self._steady_since >= self.reprobe_seconds:
                self.factor += 1
                self._steady = False
        else:
            if throughput > self._prev_throughput:
                self.factor += 1
            else:
                self.factor = max(1, self.factor - 1)
                self._steady = True
                self._steady_since = now

        self._prev_throughput = throughput
        self._completions = 0
        self._window_start = now
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_adaptive.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add src/dimq/adaptive.py tests/test_adaptive.py
git commit -m "feat: add adaptive parallelization controller with throughput-based scaling"
```

---

### Task 6: Orchestrator Core

**Files:**
- Create: `src/dimq/orchestrator.py`
- Create: `tests/test_orchestrator.py`

This is the largest component. The orchestrator manages:
- Worker registration and heartbeat tracking
- Task queue (FIFO) and dispatch
- Result storage and retry logic
- Two ZMQ ROUTER sockets (worker + client)

**Step 1: Write the failing test**

```python
# tests/test_orchestrator.py
import asyncio
import uuid

import pytest
import zmq
import zmq.asyncio

from dimq.config import load_config
from dimq.models import DimqConfig, TaskConfig, TaskStatus
from dimq.orchestrator import Orchestrator


@pytest.fixture
def config():
    return DimqConfig(
        endpoint="tcp://127.0.0.1:15555",
        client_endpoint="tcp://127.0.0.1:15556",
        heartbeat_interval_seconds=1,
        heartbeat_timeout_missed=3,
        tasks=[
            TaskConfig(name="tests.sample_tasks:echo", max_retries=2, timeout_seconds=5),
        ],
    )


@pytest.fixture
async def orchestrator(config):
    orch = Orchestrator(config)
    task = asyncio.create_task(orch.run())
    await asyncio.sleep(0.1)  # let it bind
    yield orch
    orch.shutdown()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.fixture
async def worker_socket(config):
    ctx = zmq.asyncio.Context()
    sock = ctx.socket(zmq.DEALER)
    worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"
    sock.setsockopt_string(zmq.IDENTITY, worker_id)
    sock.connect(config.endpoint)
    yield sock, worker_id
    sock.close()
    ctx.term()


@pytest.fixture
async def client_socket(config):
    ctx = zmq.asyncio.Context()
    sock = ctx.socket(zmq.DEALER)
    client_id = f"test-client-{uuid.uuid4().hex[:8]}"
    sock.setsockopt_string(zmq.IDENTITY, client_id)
    sock.connect(config.client_endpoint)
    yield sock, client_id
    sock.close()
    ctx.term()


@pytest.mark.timeout(5)
async def test_worker_registration(orchestrator, worker_socket):
    sock, worker_id = worker_socket
    # Send READY
    await sock.send_multipart([b"READY", worker_id.encode(), b"4"])
    await asyncio.sleep(0.1)
    assert worker_id in orchestrator.workers
    assert orchestrator.workers[worker_id].parallelization_factor == 4


@pytest.mark.timeout(5)
async def test_submit_and_dispatch(orchestrator, worker_socket, client_socket):
    w_sock, worker_id = worker_socket
    c_sock, client_id = client_socket

    # Register worker
    await w_sock.send_multipart([b"READY", worker_id.encode(), b"2"])
    await asyncio.sleep(0.1)

    # Client submits task
    task_id = "task-001"
    await c_sock.send_multipart([
        b"SUBMIT",
        b"tests.sample_tasks:echo",
        task_id.encode(),
        b'{"message": "hello"}',
    ])

    # Client receives ACK
    ack = await c_sock.recv_multipart()
    assert ack[0] == b"ACK"
    assert ack[1] == task_id.encode()

    # Worker receives TASK
    msg = await w_sock.recv_multipart()
    assert msg[0] == b"TASK"
    assert msg[1] == task_id.encode()
    assert msg[2] == b"tests.sample_tasks:echo"
    assert msg[3] == b'{"message": "hello"}'
    # msg[4] is parallelization_factor

    # Worker sends result
    await w_sock.send_multipart([
        b"RESULT",
        task_id.encode(),
        b"COMPLETED",
        b'{"echoed": "hello"}',
    ])
    await asyncio.sleep(0.1)

    # Client queries result
    await c_sock.send_multipart([b"RESULT", task_id.encode()])
    reply = await c_sock.recv_multipart()
    assert reply[0] == b"RESULT_REPLY"
    assert reply[1] == task_id.encode()
    assert reply[2] == b"COMPLETED"
    assert reply[3] == b'{"echoed": "hello"}'


@pytest.mark.timeout(5)
async def test_task_status_query(orchestrator, worker_socket, client_socket):
    w_sock, worker_id = worker_socket
    c_sock, _ = client_socket

    await w_sock.send_multipart([b"READY", worker_id.encode(), b"1"])
    await asyncio.sleep(0.1)

    task_id = "task-002"
    await c_sock.send_multipart([
        b"SUBMIT",
        b"tests.sample_tasks:echo",
        task_id.encode(),
        b'{"message": "hi"}',
    ])
    await c_sock.recv_multipart()  # ACK

    # Let dispatch happen
    await w_sock.recv_multipart()  # consume TASK
    await asyncio.sleep(0.05)

    # Query status while running
    await c_sock.send_multipart([b"STATUS", task_id.encode()])
    reply = await c_sock.recv_multipart()
    assert reply[0] == b"STATUS_REPLY"
    assert reply[1] == task_id.encode()
    assert reply[2] == b"RUNNING"
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_orchestrator.py -v`
Expected: FAIL (ImportError)

**Step 3: Write implementation**

```python
# src/dimq/orchestrator.py
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

import structlog
import zmq
import zmq.asyncio

from dimq.adaptive import AdaptiveController
from dimq.models import (
    DimqConfig,
    TaskAttempt,
    TaskRecord,
    TaskStatus,
)

logger = structlog.get_logger()


@dataclass
class WorkerState:
    worker_id: str
    cpu_count: int
    parallelization_factor: int
    active_tasks: set[str] = field(default_factory=set)
    last_heartbeat: float = field(default_factory=time.monotonic)
    adaptive: AdaptiveController | None = None

    @property
    def free_slots(self) -> int:
        return max(0, self.parallelization_factor - len(self.active_tasks))


class Orchestrator:
    def __init__(self, config: DimqConfig):
        self.config = config
        self.workers: dict[str, WorkerState] = {}
        self.tasks: dict[str, TaskRecord] = {}
        self.task_queue: deque[str] = deque()  # task_ids in FIFO order
        self._task_configs = {tc.name: tc for tc in config.tasks}
        self._running = False
        self._ctx: zmq.asyncio.Context | None = None

    def shutdown(self) -> None:
        self._running = False

    async def run(self) -> None:
        self._running = True
        self._ctx = zmq.asyncio.Context()

        worker_sock = self._ctx.socket(zmq.ROUTER)
        worker_sock.bind(self.config.endpoint)

        client_sock = self._ctx.socket(zmq.ROUTER)
        client_sock.bind(self.config.client_endpoint)

        logger.info("orchestrator.started",
                     worker_endpoint=self.config.endpoint,
                     client_endpoint=self.config.client_endpoint)

        poller = zmq.asyncio.Poller()
        poller.register(worker_sock, zmq.POLLIN)
        poller.register(client_sock, zmq.POLLIN)

        heartbeat_check_task = asyncio.create_task(self._heartbeat_checker())
        adaptive_task = asyncio.create_task(self._adaptive_evaluator())

        try:
            while self._running:
                events = dict(await poller.poll(timeout=100))

                if worker_sock in events:
                    frames = await worker_sock.recv_multipart()
                    identity = frames[0]
                    msg = frames[1:]
                    await self._handle_worker_msg(worker_sock, identity, msg)

                if client_sock in events:
                    frames = await client_sock.recv_multipart()
                    identity = frames[0]
                    msg = frames[1:]
                    await self._handle_client_msg(client_sock, identity, msg)

                await self._dispatch_tasks(worker_sock)
        finally:
            heartbeat_check_task.cancel()
            adaptive_task.cancel()
            worker_sock.close()
            client_sock.close()
            self._ctx.term()

    async def _handle_worker_msg(
        self, sock: zmq.asyncio.Socket, identity: bytes, msg: list[bytes]
    ) -> None:
        cmd = msg[0].decode()

        if cmd == "READY":
            worker_id = msg[1].decode()
            cpu_count = int(msg[2].decode())
            self.workers[worker_id] = WorkerState(
                worker_id=worker_id,
                cpu_count=cpu_count,
                parallelization_factor=cpu_count,
                adaptive=AdaptiveController(
                    cpu_count=cpu_count,
                    window_seconds=self.config.adaptive_window_seconds,
                    reprobe_seconds=self.config.adaptive_reprobe_seconds,
                ),
            )
            logger.info("worker.registered", worker_id=worker_id, cpu_count=cpu_count)

        elif cmd == "HEARTBEAT":
            worker_id = msg[1].decode()
            if worker_id in self.workers:
                self.workers[worker_id].last_heartbeat = time.monotonic()

        elif cmd == "RESULT":
            task_id = msg[1].decode()
            status_str = msg[2].decode()
            payload = msg[3].decode()
            await self._handle_task_result(task_id, status_str, payload)

    async def _handle_client_msg(
        self, sock: zmq.asyncio.Socket, identity: bytes, msg: list[bytes]
    ) -> None:
        cmd = msg[0].decode()

        if cmd == "SUBMIT":
            task_type = msg[1].decode()
            task_id = msg[2].decode()
            payload = msg[3].decode()

            record = TaskRecord(
                task_id=task_id,
                task_type=task_type,
                payload=payload,
            )
            self.tasks[task_id] = record
            self.task_queue.append(task_id)

            await sock.send_multipart([identity, b"ACK", task_id.encode()])
            logger.info("task.submitted", task_id=task_id, task_type=task_type)

        elif cmd == "STATUS":
            task_id = msg[1].decode()
            record = self.tasks.get(task_id)
            if record:
                await sock.send_multipart([
                    identity,
                    b"STATUS_REPLY",
                    task_id.encode(),
                    record.status.value.encode(),
                    json.dumps([a.model_dump(mode="json") for a in record.attempts]).encode(),
                ])
            else:
                await sock.send_multipart([
                    identity, b"STATUS_REPLY", task_id.encode(), b"NOT_FOUND", b"[]"
                ])

        elif cmd == "RESULT":
            task_id = msg[1].decode()
            record = self.tasks.get(task_id)
            if record and record.status == TaskStatus.COMPLETED:
                await sock.send_multipart([
                    identity,
                    b"RESULT_REPLY",
                    task_id.encode(),
                    b"COMPLETED",
                    (record.result_payload or "").encode(),
                ])
            elif record:
                await sock.send_multipart([
                    identity,
                    b"RESULT_REPLY",
                    task_id.encode(),
                    record.status.value.encode(),
                    b"",
                ])
            else:
                await sock.send_multipart([
                    identity, b"RESULT_REPLY", task_id.encode(), b"NOT_FOUND", b""
                ])

    async def _handle_task_result(
        self, task_id: str, status_str: str, payload: str
    ) -> None:
        record = self.tasks.get(task_id)
        if not record:
            return

        # Find which worker had this task and update
        for w in self.workers.values():
            if task_id in w.active_tasks:
                w.active_tasks.discard(task_id)
                if w.adaptive:
                    w.adaptive.record_completion()
                # Update attempt
                for attempt in reversed(record.attempts):
                    if attempt.worker_id == w.worker_id and attempt.ended_at is None:
                        attempt.ended_at = datetime.now()
                        attempt.status = TaskStatus(status_str)
                        break
                break

        if status_str == "COMPLETED":
            record.status = TaskStatus.COMPLETED
            record.result_payload = payload
            logger.info("task.completed", task_id=task_id)
        else:
            # TIMEOUT or ERROR — retry if possible
            task_cfg = self._task_configs.get(record.task_type)
            max_retries = task_cfg.max_retries if task_cfg else 3

            record.retry_count += 1
            if record.retry_count <= max_retries:
                record.status = TaskStatus.RETRYING
                self.task_queue.append(task_id)
                logger.info("task.retrying", task_id=task_id, retry=record.retry_count)
            else:
                record.status = TaskStatus.FAILED
                logger.info("task.failed", task_id=task_id, retries_exhausted=True)

    async def _dispatch_tasks(self, worker_sock: zmq.asyncio.Socket) -> None:
        if not self.task_queue:
            return

        for worker in self.workers.values():
            if not self.task_queue:
                break
            while worker.free_slots > 0 and self.task_queue:
                task_id = self.task_queue.popleft()
                record = self.tasks[task_id]
                record.status = TaskStatus.RUNNING
                record.attempts.append(
                    TaskAttempt(
                        attempt_number=len(record.attempts) + 1,
                        worker_id=worker.worker_id,
                        started_at=datetime.now(),
                    )
                )
                worker.active_tasks.add(task_id)

                await worker_sock.send_multipart([
                    worker.worker_id.encode(),
                    b"TASK",
                    task_id.encode(),
                    record.task_type.encode(),
                    record.payload.encode(),
                    str(worker.parallelization_factor).encode(),
                ])
                logger.info("task.dispatched", task_id=task_id, worker=worker.worker_id)

    async def _heartbeat_checker(self) -> None:
        timeout = (
            self.config.heartbeat_interval_seconds
            * self.config.heartbeat_timeout_missed
        )
        while self._running:
            await asyncio.sleep(self.config.heartbeat_interval_seconds)
            now = time.monotonic()
            dead_workers = []
            for wid, ws in self.workers.items():
                if now - ws.last_heartbeat > timeout:
                    dead_workers.append(wid)

            for wid in dead_workers:
                ws = self.workers.pop(wid)
                # Re-queue all in-flight tasks
                for task_id in ws.active_tasks:
                    record = self.tasks.get(task_id)
                    if record and record.status == TaskStatus.RUNNING:
                        await self._handle_task_result(task_id, "TIMEOUT", "")
                logger.warning("worker.dead", worker_id=wid, requeued=len(ws.active_tasks))

    async def _adaptive_evaluator(self) -> None:
        while self._running:
            await asyncio.sleep(self.config.adaptive_window_seconds)
            for ws in self.workers.values():
                if ws.adaptive:
                    ws.adaptive.evaluate()
                    ws.parallelization_factor = ws.adaptive.factor
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_orchestrator.py -v`
Expected: All 3 tests PASS

**Step 5: Commit**

```bash
git add src/dimq/orchestrator.py tests/test_orchestrator.py
git commit -m "feat: add orchestrator with ZMQ ROUTER, task dispatch, heartbeat, retry logic"
```

---

### Task 7: Worker Core

**Files:**
- Create: `src/dimq/worker.py`
- Create: `tests/test_worker.py`

**Step 1: Write the failing test**

```python
# tests/test_worker.py
import asyncio
import json
import uuid

import pytest
import zmq
import zmq.asyncio

from dimq.models import DimqConfig, TaskConfig
from dimq.worker import Worker


@pytest.fixture
def config():
    return DimqConfig(
        endpoint="tcp://127.0.0.1:16555",
        tasks=[
            TaskConfig(name="tests.sample_tasks:echo", max_retries=2, timeout_seconds=5),
        ],
    )


@pytest.fixture
async def mock_orchestrator(config):
    """A mock orchestrator that speaks the protocol."""
    ctx = zmq.asyncio.Context()
    sock = ctx.socket(zmq.ROUTER)
    sock.bind(config.endpoint)
    yield sock
    sock.close()
    ctx.term()


@pytest.mark.timeout(5)
async def test_worker_registers_on_start(mock_orchestrator, config):
    sock = mock_orchestrator
    worker = Worker(config)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.2)

    # Should receive READY
    frames = await sock.recv_multipart()
    identity = frames[0]
    assert frames[1] == b"READY"
    # frames[2] is worker_id, frames[3] is cpu_count

    worker.shutdown()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.timeout(5)
async def test_worker_executes_task(mock_orchestrator, config):
    sock = mock_orchestrator
    worker = Worker(config)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.2)

    # Receive READY
    frames = await sock.recv_multipart()
    identity = frames[0]
    worker_id = frames[2]

    # Send task to worker
    task_id = b"task-w1"
    await sock.send_multipart([
        identity,
        b"TASK",
        task_id,
        b"tests.sample_tasks:echo",
        b'{"message": "world"}',
        b"2",
    ])

    # Receive result
    frames = await sock.recv_multipart()
    assert frames[1] == b"RESULT"
    assert frames[2] == task_id
    assert frames[3] == b"COMPLETED"
    result = json.loads(frames[4])
    assert result["echoed"] == "world"

    worker.shutdown()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_worker.py -v`
Expected: FAIL (ImportError)

**Step 3: Write implementation**

```python
# src/dimq/worker.py
from __future__ import annotations

import asyncio
import os
import uuid
import json
import time
from concurrent.futures import ThreadPoolExecutor

import structlog
import zmq
import zmq.asyncio

from dimq.models import DimqConfig
from dimq.task import load_task_func, TaskFunc

logger = structlog.get_logger()


class Worker:
    def __init__(self, config: DimqConfig):
        self.config = config
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self.cpu_count = os.cpu_count() or 1
        self._running = False
        self._parallelization_factor = self.cpu_count
        self._active_tasks: set[str] = set()
        self._task_funcs: dict[str, TaskFunc] = {}
        self._executor = ThreadPoolExecutor(max_workers=self.cpu_count * 2)

        # Load task functions
        for tc in config.tasks:
            self._task_funcs[tc.name] = load_task_func(tc.name)
            logger.info("worker.task_loaded", task=tc.name)

        # Build timeout lookup
        self._timeouts = {tc.name: tc.timeout_seconds for tc in config.tasks}

    def shutdown(self) -> None:
        self._running = False

    async def run(self) -> None:
        self._running = True
        ctx = zmq.asyncio.Context()
        sock = ctx.socket(zmq.DEALER)
        sock.setsockopt_string(zmq.IDENTITY, self.worker_id)
        sock.connect(self.config.endpoint)

        # Register
        await sock.send_multipart([
            b"READY",
            self.worker_id.encode(),
            str(self.cpu_count).encode(),
        ])
        logger.info("worker.registered", worker_id=self.worker_id, cpu_count=self.cpu_count)

        heartbeat_task = asyncio.create_task(self._heartbeat_loop(sock))

        try:
            while self._running:
                try:
                    msg = await asyncio.wait_for(sock.recv_multipart(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                cmd = msg[0].decode()

                if cmd == "TASK":
                    task_id = msg[1].decode()
                    task_type = msg[2].decode()
                    payload = msg[3].decode()
                    new_factor = int(msg[4].decode())
                    self._parallelization_factor = new_factor

                    self._active_tasks.add(task_id)
                    asyncio.create_task(
                        self._execute_task(sock, task_id, task_type, payload)
                    )

                elif cmd == "IDLE":
                    new_factor = int(msg[1].decode())
                    self._parallelization_factor = new_factor

        finally:
            heartbeat_task.cancel()
            sock.close()
            ctx.term()

    async def _execute_task(
        self,
        sock: zmq.asyncio.Socket,
        task_id: str,
        task_type: str,
        payload: str,
    ) -> None:
        tf = self._task_funcs.get(task_type)
        if not tf:
            await sock.send_multipart([
                b"RESULT", task_id.encode(), b"ERROR",
                json.dumps({"error": f"Unknown task type: {task_type}"}).encode(),
            ])
            self._active_tasks.discard(task_id)
            return

        timeout = self._timeouts.get(task_type, 60.0)
        input_obj = tf.input_model.model_validate_json(payload)

        try:
            if tf.is_async:
                result = await asyncio.wait_for(
                    tf.func(input_obj), timeout=timeout
                )
            else:
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(self._executor, tf.func, input_obj),
                    timeout=timeout,
                )

            await sock.send_multipart([
                b"RESULT",
                task_id.encode(),
                b"COMPLETED",
                result.model_dump_json().encode(),
            ])
            logger.info("worker.task_completed", task_id=task_id)

        except asyncio.TimeoutError:
            await sock.send_multipart([
                b"RESULT", task_id.encode(), b"TIMEOUT",
                json.dumps({"error": "Task timed out"}).encode(),
            ])
            logger.warning("worker.task_timeout", task_id=task_id)

        except Exception as e:
            await sock.send_multipart([
                b"RESULT", task_id.encode(), b"ERROR",
                json.dumps({"error": str(e)}).encode(),
            ])
            logger.error("worker.task_error", task_id=task_id, error=str(e))

        finally:
            self._active_tasks.discard(task_id)

    async def _heartbeat_loop(self, sock: zmq.asyncio.Socket) -> None:
        while self._running:
            await asyncio.sleep(self.config.heartbeat_interval_seconds)
            await sock.send_multipart([
                b"HEARTBEAT",
                self.worker_id.encode(),
                str(len(self._active_tasks)).encode(),
            ])
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_worker.py -v`
Expected: All 2 tests PASS

**Step 5: Commit**

```bash
git add src/dimq/worker.py tests/test_worker.py
git commit -m "feat: add worker with task execution, timeout cancellation, heartbeat"
```

---

### Task 8: Integration Test — Full Round Trip

**Files:**
- Create: `tests/test_integration.py`

**Step 1: Write the integration test**

```python
# tests/test_integration.py
import asyncio
import json
import uuid

import pytest
import zmq
import zmq.asyncio

from dimq.models import DimqConfig, TaskConfig
from dimq.orchestrator import Orchestrator
from dimq.worker import Worker


@pytest.fixture
def config():
    return DimqConfig(
        endpoint="tcp://127.0.0.1:17555",
        client_endpoint="tcp://127.0.0.1:17556",
        heartbeat_interval_seconds=1,
        heartbeat_timeout_missed=3,
        tasks=[
            TaskConfig(name="tests.sample_tasks:echo", max_retries=2, timeout_seconds=5),
        ],
    )


@pytest.fixture
async def system(config):
    """Start orchestrator + worker together."""
    orch = Orchestrator(config)
    worker = Worker(config)

    orch_task = asyncio.create_task(orch.run())
    await asyncio.sleep(0.1)
    worker_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.2)

    yield orch, worker

    worker.shutdown()
    orch.shutdown()
    worker_task.cancel()
    orch_task.cancel()
    for t in [worker_task, orch_task]:
        try:
            await t
        except asyncio.CancelledError:
            pass


@pytest.fixture
async def client_socket(config):
    ctx = zmq.asyncio.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.setsockopt_string(zmq.IDENTITY, f"client-{uuid.uuid4().hex[:8]}")
    sock.connect(config.client_endpoint)
    yield sock
    sock.close()
    ctx.term()


@pytest.mark.timeout(10)
async def test_full_round_trip(system, client_socket):
    orch, worker = system
    sock = client_socket

    # Submit task
    task_id = "integration-001"
    await sock.send_multipart([
        b"SUBMIT",
        b"tests.sample_tasks:echo",
        task_id.encode(),
        b'{"message": "integration test"}',
    ])

    # ACK
    ack = await sock.recv_multipart()
    assert ack[0] == b"ACK"

    # Wait for processing
    await asyncio.sleep(0.5)

    # Get result
    await sock.send_multipart([b"RESULT", task_id.encode()])
    reply = await sock.recv_multipart()
    assert reply[0] == b"RESULT_REPLY"
    assert reply[2] == b"COMPLETED"
    result = json.loads(reply[3])
    assert result["echoed"] == "integration test"


@pytest.mark.timeout(10)
async def test_multiple_tasks(system, client_socket):
    orch, worker = system
    sock = client_socket

    # Submit 5 tasks
    for i in range(5):
        task_id = f"multi-{i:03d}"
        await sock.send_multipart([
            b"SUBMIT",
            b"tests.sample_tasks:echo",
            task_id.encode(),
            json.dumps({"message": f"msg-{i}"}).encode(),
        ])
        ack = await sock.recv_multipart()
        assert ack[0] == b"ACK"

    # Wait for all to complete
    await asyncio.sleep(1.0)

    # Check all results
    for i in range(5):
        task_id = f"multi-{i:03d}"
        await sock.send_multipart([b"RESULT", task_id.encode()])
        reply = await sock.recv_multipart()
        assert reply[2] == b"COMPLETED"
        result = json.loads(reply[3])
        assert result["echoed"] == f"msg-{i}"
```

**Step 2: Run the integration test**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_integration.py -v`
Expected: All 2 tests PASS

**Step 3: Run full test suite**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest -v`
Expected: All tests PASS

**Step 4: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: add full round-trip integration tests"
```

---

### Task 9: CLI Entry Points

**Files:**
- Create: `src/dimq/cli.py`
- Modify: `pyproject.toml` (add scripts entry)

**Step 1: Write CLI module**

```python
# src/dimq/cli.py
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from dimq.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description="DIMQ - Distributed In-Memory Queue")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Orchestrator
    orch_parser = subparsers.add_parser("orchestrator", help="Run the orchestrator")
    orch_parser.add_argument("--config", type=Path, required=True)

    # Worker
    worker_parser = subparsers.add_parser("worker", help="Run a worker")
    worker_parser.add_argument("--config", type=Path, required=True)
    worker_parser.add_argument("--endpoint", type=str, default=None,
                                help="Override orchestrator endpoint")

    args = parser.parse_args()
    config = load_config(args.config)

    if args.command == "orchestrator":
        from dimq.orchestrator import Orchestrator
        orch = Orchestrator(config)
        asyncio.run(orch.run())

    elif args.command == "worker":
        if args.endpoint:
            config.endpoint = args.endpoint
        from dimq.worker import Worker
        worker = Worker(config)
        asyncio.run(worker.run())


if __name__ == "__main__":
    main()
```

**Step 2: Add scripts entry to pyproject.toml**

Add to `[project.scripts]`:

```toml
[project.scripts]
dimq = "dimq.cli:main"
```

**Step 3: Verify CLI help works**

Run: `cd /Users/sergeyk/w/DIMQ && uv run dimq --help`
Expected: Shows help with `orchestrator` and `worker` subcommands

**Step 4: Commit**

```bash
git add src/dimq/cli.py pyproject.toml
git commit -m "feat: add CLI entry points for orchestrator and worker"
```

---

### Task 10: LoadTask Rust Extension

**Files:**
- Create: `load_task/Cargo.toml`
- Create: `load_task/pyproject.toml`
- Create: `load_task/src/lib.rs`
- Create: `tests/test_load_task.py`

**Step 1: Create Cargo.toml**

```toml
[package]
name = "load_task"
version = "0.1.0"
edition = "2021"

[lib]
name = "load_task"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.22", features = ["extension-module"] }
rayon = "1.10"
sha2 = "0.10"
tempfile = "3"
```

**Step 2: Create load_task/pyproject.toml**

```toml
[build-system]
requires = ["maturin>=1.0,<2.0"]
build-backend = "maturin"

[project]
name = "load_task"
version = "0.1.0"
requires-python = ">=3.12"

[tool.maturin]
features = ["pyo3/extension-module"]
```

**Step 3: Write the Rust implementation**

```rust
// load_task/src/lib.rs
use pyo3::prelude::*;
use rayon::prelude::*;
use sha2::{Sha256, Digest};
use std::time::{Duration, Instant};
use std::io::Write;

#[pyfunction]
#[pyo3(signature = (duration_seconds=1.0, concurrency=1, cpu_load=0.7, io_load=0.2, memory_mb=50))]
fn run(
    py: Python<'_>,
    duration_seconds: f64,
    concurrency: usize,
    cpu_load: f64,
    io_load: f64,
    memory_mb: usize,
) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        let start = Instant::now();
        let total_duration = Duration::from_secs_f64(duration_seconds);
        let cpu_duration = Duration::from_secs_f64(duration_seconds * cpu_load);
        let io_duration = Duration::from_secs_f64(duration_seconds * io_load);
        let mut phases: Vec<(String, f64, f64)> = Vec::new();

        // Memory pressure
        let mem_start = start.elapsed().as_secs_f64();
        let mut _memory: Vec<u8> = vec![0u8; memory_mb * 1024 * 1024];
        // Touch pages to ensure real allocation
        for i in (0.._memory.len()).step_by(4096) {
            _memory[i] = 1;
        }
        phases.push(("memory".into(), mem_start, start.elapsed().as_secs_f64() - mem_start));

        // CPU load using rayon
        let cpu_start = start.elapsed().as_secs_f64();
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()
            .unwrap();
        pool.install(|| {
            let deadline = Instant::now() + cpu_duration;
            (0..concurrency).into_par_iter().for_each(|_| {
                let mut hasher = Sha256::new();
                let mut counter: u64 = 0;
                while Instant::now() < deadline {
                    hasher.update(counter.to_le_bytes());
                    counter += 1;
                    if counter % 10000 == 0 {
                        let _ = hasher.finalize_reset();
                    }
                }
            });
        });
        phases.push(("cpu".into(), cpu_start, start.elapsed().as_secs_f64() - cpu_start));

        // IO load
        let io_start_t = start.elapsed().as_secs_f64();
        let io_deadline = Instant::now() + io_duration;
        while Instant::now() < io_deadline {
            if let Ok(mut tmpfile) = tempfile::tempfile() {
                let data = vec![0u8; 1024 * 64]; // 64KB writes
                let _ = tmpfile.write_all(&data);
                let _ = tmpfile.flush();
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        phases.push(("io".into(), io_start_t, start.elapsed().as_secs_f64() - io_start_t));

        let total = start.elapsed().as_secs_f64();
        (phases, total, memory_mb)
    });

    // Build Python dict result
    let dict = pyo3::types::PyDict::new(py);
    let phases_list = pyo3::types::PyList::empty(py);
    for (phase_type, start_time, duration) in &result.0 {
        let phase_dict = pyo3::types::PyDict::new(py);
        phase_dict.set_item("type", phase_type)?;
        phase_dict.set_item("start_seconds", start_time)?;
        phase_dict.set_item("duration_seconds", duration)?;
        phases_list.append(phase_dict)?;
    }
    dict.set_item("phases", phases_list)?;
    dict.set_item("total_duration_seconds", result.1)?;
    dict.set_item("peak_memory_mb", result.2)?;

    Ok(dict.into())
}

#[pymodule]
fn load_task(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    Ok(())
}
```

**Step 4: Build the extension**

Run: `cd /Users/sergeyk/w/DIMQ/load_task && maturin develop`
Expected: Build succeeds, module available

**Step 5: Write the failing test**

```python
# tests/test_load_task.py
import pytest

load_task = pytest.importorskip("load_task")


def test_load_task_default():
    result = load_task.run(duration_seconds=0.2, memory_mb=1)
    assert "phases" in result
    assert "total_duration_seconds" in result
    assert "peak_memory_mb" in result
    assert result["peak_memory_mb"] == 1
    assert len(result["phases"]) == 3


def test_load_task_phases_have_correct_keys():
    result = load_task.run(duration_seconds=0.1, memory_mb=1, concurrency=1)
    for phase in result["phases"]:
        assert "type" in phase
        assert "start_seconds" in phase
        assert "duration_seconds" in phase


def test_load_task_phase_types():
    result = load_task.run(duration_seconds=0.1, memory_mb=1)
    types = {p["type"] for p in result["phases"]}
    assert types == {"memory", "cpu", "io"}
```

**Step 6: Run test**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest tests/test_load_task.py -v`
Expected: All 3 tests PASS

**Step 7: Commit**

```bash
git add load_task/ tests/test_load_task.py
git commit -m "feat: add LoadTask Rust extension with CPU, IO, and memory pressure simulation"
```

---

### Task 11: Final — Run Full Test Suite & Verify

**Step 1: Run all tests**

Run: `cd /Users/sergeyk/w/DIMQ && uv run pytest -v`
Expected: All tests PASS (models, config, task, adaptive, orchestrator, worker, integration, load_task)

**Step 2: Verify CLI works end-to-end**

Create a test config:

```yaml
# /tmp/dimq-test.yaml
endpoint: "tcp://127.0.0.1:18555"
client_endpoint: "tcp://127.0.0.1:18556"
heartbeat_interval_seconds: 2
heartbeat_timeout_missed: 3
tasks:
  - name: "tests.sample_tasks:echo"
    max_retries: 2
    timeout_seconds: 10
```

Run orchestrator: `cd /Users/sergeyk/w/DIMQ && uv run dimq orchestrator --config /tmp/dimq-test.yaml &`
Run worker: `cd /Users/sergeyk/w/DIMQ && uv run dimq worker --config /tmp/dimq-test.yaml &`
Expected: Both start without errors, worker registers with orchestrator

Kill both processes after verifying.

**Step 3: Commit any remaining changes**

```bash
git add -A
git commit -m "chore: final verification pass"
```
