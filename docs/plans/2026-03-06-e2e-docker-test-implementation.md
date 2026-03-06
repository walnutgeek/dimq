# E2E Docker Test Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Docker-based end-to-end test that runs 3 containerized workers against an embedded orchestrator, submits LoadTask batches, drains results, and verifies adaptive parallelization factor adjustment.

**Architecture:** Rust extension renamed to `dimq_load_task`, new Python wrapper at `src/dimq/tasks/load.py`, multi-stage Dockerfile, docker-compose with 3 workers, pytest e2e test using embedded orchestrator with fast adaptive windows (2s eval, 10s reprobe).

**Tech Stack:** Docker, docker-compose, maturin, pyo3, pytest, ZeroMQ, Python 3.9

---

### Task 1: Rename Rust Extension from `load_task` to `dimq_load_task`

**Files:**
- Modify: `load_task/Cargo.toml`
- Modify: `load_task/pyproject.toml`
- Modify: `load_task/src/lib.rs:100-104`
- Modify: `tests/test_load_task.py:3`
- Modify: `.gitignore:18-19`
- Rename: `load_task/` -> `dimq_load_task/`

**Step 1: Update `load_task/Cargo.toml`**

Change both `name` and `lib.name` from `load_task` to `dimq_load_task`:

```toml
[package]
name = "dimq_load_task"
version = "0.1.0"
edition = "2021"

[lib]
name = "dimq_load_task"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.22", features = ["extension-module"] }
rayon = "1.10"
sha2 = "0.10"
tempfile = "3"
```

**Step 2: Update `load_task/pyproject.toml`**

```toml
[build-system]
requires = ["maturin>=1.0,<2.0"]
build-backend = "maturin"

[project]
name = "dimq-load-task"
version = "0.1.0"
requires-python = ">=3.9"

[tool.maturin]
features = ["pyo3/extension-module"]
```

**Step 3: Update the `#[pymodule]` name in `load_task/src/lib.rs`**

Change line 101 from `fn load_task(...)` to `fn dimq_load_task(...)`:

```rust
#[pymodule]
fn dimq_load_task(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    Ok(())
}
```

**Step 4: Rename directory**

```bash
git mv load_task dimq_load_task
```

**Step 5: Update `.gitignore`**

Change `load_task/target/` and `load_task/Cargo.lock` to:

```
dimq_load_task/target/
dimq_load_task/Cargo.lock
```

**Step 6: Update `tests/test_load_task.py`**

Change line 3 from `load_task = pytest.importorskip("load_task")` to:

```python
dimq_load_task = pytest.importorskip("dimq_load_task")
```

And update all references from `load_task.run(...)` to `dimq_load_task.run(...)`:

```python
import pytest

dimq_load_task = pytest.importorskip("dimq_load_task")


def test_load_task_default():
    result = dimq_load_task.run(duration_seconds=0.2, memory_mb=1)
    assert "phases" in result
    assert "total_duration_seconds" in result
    assert "peak_memory_mb" in result
    assert result["peak_memory_mb"] == 1
    assert len(result["phases"]) == 3


def test_load_task_phases_have_correct_keys():
    result = dimq_load_task.run(duration_seconds=0.1, memory_mb=1, concurrency=1)
    for phase in result["phases"]:
        assert "type" in phase
        assert "start_seconds" in phase
        assert "duration_seconds" in phase


def test_load_task_phase_types():
    result = dimq_load_task.run(duration_seconds=0.1, memory_mb=1)
    types = {p["type"] for p in result["phases"]}
    assert types == {"memory", "cpu", "io"}
```

**Step 7: Update `README.md`**

Find-and-replace all occurrences of `load_task` with `dimq_load_task` in README.md. The directory reference, import statement, function calls, etc. Also update the maturin develop command:

```bash
cd dimq_load_task
uv tool run maturin develop --uv
cd ..
```

**Step 8: Rebuild the Rust extension**

```bash
uv tool run maturin develop --manifest-path dimq_load_task/Cargo.toml --uv
```

**Step 9: Run existing tests to verify nothing broke**

Run: `uv run pytest -v`
Expected: All 29 tests PASS

**Step 10: Commit**

```bash
git add -A
git commit -m "refactor: rename load_task to dimq_load_task to avoid module collisions"
```

---

### Task 2: Create LoadTask Python Wrapper

**Files:**
- Create: `src/dimq/tasks/__init__.py`
- Create: `src/dimq/tasks/load.py`
- Create: `tests/test_tasks_load.py`

**Step 1: Write the failing test**

Create `tests/test_tasks_load.py`:

```python
import pytest

dimq_load_task = pytest.importorskip("dimq_load_task")

from dimq.tasks.load import LoadInput, LoadOutput, run_load


def test_run_load_returns_output():
    inp = LoadInput(duration_seconds=0.2, concurrency=1, cpu_load=0.5, io_load=0.1, memory_mb=1)
    result = run_load(inp)
    assert isinstance(result, LoadOutput)
    assert result.total_duration_seconds > 0
    assert result.peak_memory_mb == 1


def test_run_load_default_input():
    inp = LoadInput()
    result = run_load(inp)
    assert isinstance(result, LoadOutput)
    assert result.total_duration_seconds > 0
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tasks_load.py -v`
Expected: FAIL (ModuleNotFoundError: No module named 'dimq.tasks')

**Step 3: Write minimal implementation**

Create `src/dimq/tasks/__init__.py` (empty file):

```python
```

Create `src/dimq/tasks/load.py`:

```python
from __future__ import annotations

from pydantic import BaseModel

import dimq_load_task


class LoadInput(BaseModel):
    duration_seconds: float = 1.0
    concurrency: int = 1
    cpu_load: float = 0.7
    io_load: float = 0.2
    memory_mb: int = 50


class LoadOutput(BaseModel):
    total_duration_seconds: float
    peak_memory_mb: int


def run_load(input: LoadInput) -> LoadOutput:
    result = dimq_load_task.run(
        duration_seconds=input.duration_seconds,
        concurrency=input.concurrency,
        cpu_load=input.cpu_load,
        io_load=input.io_load,
        memory_mb=input.memory_mb,
    )
    return LoadOutput(
        total_duration_seconds=result["total_duration_seconds"],
        peak_memory_mb=result["peak_memory_mb"],
    )
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_tasks_load.py -v`
Expected: 2 PASS

Also run full suite: `uv run pytest -v`
Expected: All 31 tests PASS (29 existing + 2 new)

**Step 5: Commit**

```bash
git add src/dimq/tasks/__init__.py src/dimq/tasks/load.py tests/test_tasks_load.py
git commit -m "feat: add Pydantic-typed LoadTask wrapper for dimq_load_task"
```

---

### Task 3: Create Dockerfile (Multi-Stage Build)

**Files:**
- Create: `Dockerfile`
- Create: `e2e/config.yaml`

**Step 1: Create `e2e/config.yaml`**

This is the worker config baked into the Docker image:

```yaml
endpoint: "tcp://placeholder:5555"
heartbeat_interval_seconds: 1
heartbeat_timeout_missed: 3
tasks:
  - name: "dimq.tasks.load:run_load"
    max_retries: 1
    timeout_seconds: 30
```

**Step 2: Create `Dockerfile`**

```dockerfile
# Stage 1: Build Rust extension
FROM python:3.9-slim AS builder

# Install Rust toolchain and build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.cargo/bin:${PATH}"

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /build

# Copy project files
COPY pyproject.toml .python-version ./
COPY src/ src/
COPY dimq_load_task/ dimq_load_task/

# Create venv and install project
RUN uv sync --no-dev

# Build Rust extension into the venv
RUN uv tool run maturin develop --manifest-path dimq_load_task/Cargo.toml --uv


# Stage 2: Runtime image
FROM python:3.9-slim

WORKDIR /app

# Copy the entire venv from builder
COPY --from=builder /build/.venv /app/.venv

# Copy source and config
COPY src/ /app/src/
COPY e2e/config.yaml /app/config.yaml

# Put venv on PATH
ENV PATH="/app/.venv/bin:${PATH}"
ENV VIRTUAL_ENV="/app/.venv"

ENTRYPOINT ["dimq"]
CMD ["worker", "--config", "/app/config.yaml"]
```

**Step 3: Verify Docker build works**

```bash
docker build -t dimq-worker .
```

Expected: Image builds successfully (may take a few minutes for Rust compilation).

**Step 4: Quick smoke test**

```bash
docker run --rm dimq-worker worker --config /app/config.yaml --endpoint tcp://host.docker.internal:9999 &
sleep 2
docker ps
# Should see the container running (it will fail to connect but should start)
docker stop $(docker ps -q --filter ancestor=dimq-worker)
```

**Step 5: Update `.gitignore` — no changes needed** (Docker doesn't produce local artifacts)

**Step 6: Commit**

```bash
git add Dockerfile e2e/config.yaml
git commit -m "feat: add multi-stage Dockerfile for worker containers"
```

---

### Task 4: Create docker-compose.yml

**Files:**
- Create: `docker-compose.yml`

**Step 1: Create `docker-compose.yml`**

```yaml
services:
  worker-1:
    build: .
    command: ["worker", "--config", "/app/config.yaml", "--endpoint", "tcp://host.docker.internal:5555"]
    extra_hosts:
      - "host.docker.internal:host-gateway"
  worker-2:
    build: .
    command: ["worker", "--config", "/app/config.yaml", "--endpoint", "tcp://host.docker.internal:5555"]
    extra_hosts:
      - "host.docker.internal:host-gateway"
  worker-3:
    build: .
    command: ["worker", "--config", "/app/config.yaml", "--endpoint", "tcp://host.docker.internal:5555"]
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

Note: `extra_hosts` ensures `host.docker.internal` resolves on Linux. On Docker Desktop (Mac/Windows) it resolves automatically, but the extra_hosts directive is harmless there.

**Step 2: Verify compose file**

```bash
docker compose config
```

Expected: Prints resolved config without errors.

**Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat: add docker-compose with 3 worker services"
```

---

### Task 5: Register e2e pytest marker

**Files:**
- Modify: `pyproject.toml:30-32`

**Step 1: Add marker registration to pyproject.toml**

In the `[tool.pytest.ini_options]` section, add:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = [
    "e2e: end-to-end Docker tests (require Docker daemon)",
]
```

**Step 2: Verify existing tests still work**

Run: `uv run pytest -v`
Expected: All tests PASS (no marker warnings)

**Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "feat: register e2e pytest marker"
```

---

### Task 6: Write the E2E Test

**Files:**
- Create: `tests/test_e2e_docker.py`

**Step 1: Write the test file**

```python
from __future__ import annotations

import asyncio
import json
import subprocess
import time
import uuid

import pytest

from dimq.models import DimqConfig, TaskConfig
from dimq.orchestrator import Orchestrator


@pytest.fixture
def e2e_config():
    return DimqConfig(
        endpoint="tcp://0.0.0.0:5555",
        client_endpoint=None,
        heartbeat_interval_seconds=1,
        heartbeat_timeout_missed=3,
        adaptive_window_seconds=2.0,
        adaptive_reprobe_seconds=10.0,
        tasks=[
            TaskConfig(
                name="dimq.tasks.load:run_load",
                max_retries=1,
                timeout_seconds=30,
            ),
        ],
    )


@pytest.fixture
async def orchestrator(e2e_config):
    orch = Orchestrator(e2e_config)
    task = asyncio.create_task(orch.run())
    await asyncio.sleep(0.2)
    yield orch
    orch.shutdown()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.fixture
def docker_workers():
    """Start 3 Docker worker containers via docker-compose."""
    subprocess.run(
        ["docker", "compose", "up", "-d", "--build"],
        check=True,
        capture_output=True,
    )
    yield
    subprocess.run(
        ["docker", "compose", "down"],
        check=True,
        capture_output=True,
    )


async def wait_for_workers(orch: Orchestrator, count: int, timeout: float = 30.0):
    """Poll orchestrator until `count` workers have registered."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if len(orch.workers) >= count:
            return
        await asyncio.sleep(0.5)
    raise TimeoutError(
        f"Only {len(orch.workers)}/{count} workers registered within {timeout}s"
    )


@pytest.mark.e2e
@pytest.mark.timeout(120)
async def test_adaptive_parallelization(orchestrator, docker_workers):
    orch = orchestrator

    # Wait for all 3 workers to register
    await wait_for_workers(orch, 3)
    worker_ids = list(orch.workers.keys())
    assert len(worker_ids) == 3

    # Record initial factors
    initial_factors = {
        wid: orch.workers[wid].parallelization_factor for wid in worker_ids
    }

    total_submitted = 0
    total_drained = 0
    factor_history = {wid: [initial_factors[wid]] for wid in worker_ids}
    workers_that_got_tasks = set()

    # Run load loop for ~30 seconds
    test_duration = 30.0
    batch_size = 6  # 2 per worker
    drain_interval = 3.0
    task_counter = 0
    start = time.monotonic()
    last_drain = start

    while time.monotonic() - start < test_duration:
        # Submit a batch of load tasks
        for _ in range(batch_size):
            task_id = f"e2e-{task_counter:04d}"
            task_counter += 1
            orch.submit_task(
                "dimq.tasks.load:run_load",
                task_id,
                json.dumps({
                    "duration_seconds": 1.0,
                    "concurrency": 1,
                    "cpu_load": 0.5,
                    "io_load": 0.1,
                    "memory_mb": 5,
                }),
            )
            total_submitted += 1

        await asyncio.sleep(drain_interval)

        # Drain finished tasks
        finished = orch.drain_finished()
        total_drained += len(finished)

        # Track which workers got tasks
        for wid, ws in orch.workers.items():
            if ws.active_tasks:
                workers_that_got_tasks.add(wid)
            factor_history[wid].append(ws.parallelization_factor)

        # Also check attempt records on drained tasks
        for record in finished:
            for attempt in record.attempts:
                workers_that_got_tasks.add(attempt.worker_id)

        now = time.monotonic() - start
        print(
            f"  [{now:.1f}s] submitted={total_submitted} drained={total_drained} "
            f"pending={len(orch.tasks)} "
            f"factors={[orch.workers[w].parallelization_factor for w in worker_ids]}"
        )

    # Final drain: wait for remaining tasks to complete
    for _ in range(10):
        finished = orch.drain_finished()
        total_drained += len(finished)
        for record in finished:
            for attempt in record.attempts:
                workers_that_got_tasks.add(attempt.worker_id)
        if len(orch.tasks) == 0:
            break
        await asyncio.sleep(2.0)

    # ---- Assertions ----

    # 1. All tasks completed (no stuck tasks)
    assert len(orch.tasks) == 0, (
        f"{len(orch.tasks)} tasks still in orchestrator after test"
    )

    # 2. All submitted tasks were drained
    assert total_drained == total_submitted, (
        f"Submitted {total_submitted} but only drained {total_drained}"
    )

    # 3. All 3 workers received at least one task
    assert len(workers_that_got_tasks) == 3, (
        f"Only {len(workers_that_got_tasks)}/3 workers got tasks: {workers_that_got_tasks}"
    )

    # 4. At least one worker's factor changed (adaptive tuning worked)
    factor_changed = False
    for wid in worker_ids:
        history = factor_history[wid]
        if len(set(history)) > 1:
            factor_changed = True
            break
    assert factor_changed, (
        f"No worker's parallelization factor changed. Histories: {factor_history}"
    )

    # 5. drain_finished cleared tasks dict
    assert orch.drain_finished() == []

    print(f"\nFinal factor histories: {factor_history}")
    print(f"Total: submitted={total_submitted}, drained={total_drained}")
```

**Step 2: Verify the test is discovered but skipped without `-m e2e`**

Run: `uv run pytest -v`
Expected: All existing tests pass. `test_e2e_docker.py` is collected but NOT run (no `e2e` marker selected).

Note: pytest with the default config will still collect e2e tests. To exclude them by default, we can optionally add `addopts = "-m 'not e2e'"` to pyproject.toml. But the simplest approach: the test requires Docker, so if docker-compose isn't running, the fixture will fail. Let's add a skip condition.

**Step 3: Add Docker availability check**

Add this to the top of `tests/test_e2e_docker.py`, right after the imports:

```python
# Skip all tests in this module if Docker is not available
def _docker_available():
    try:
        result = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=5
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(not _docker_available(), reason="Docker not available"),
]
```

Remove the `@pytest.mark.e2e` from the test function (since `pytestmark` applies it module-wide).

**Step 4: Run the e2e test**

Run: `uv run pytest tests/test_e2e_docker.py -v -s`
Expected: PASS (takes ~60-90 seconds including Docker build)

The `-s` flag shows the print output so you can watch factor adjustments in real time.

**Step 5: Verify regular tests skip e2e**

Run: `uv run pytest -v`
Expected: `test_e2e_docker.py` tests are either skipped (Docker not available) or collected but not selected. All other tests PASS.

**Step 6: Commit**

```bash
git add tests/test_e2e_docker.py
git commit -m "feat: add Docker e2e test for adaptive parallelization"
```

---

### Task 7: Final Verification and README Update

**Files:**
- Modify: `README.md`

**Step 1: Run full test suite**

```bash
uv run pytest -v
```

Expected: All non-e2e tests PASS.

**Step 2: Run e2e test**

```bash
uv run pytest tests/test_e2e_docker.py -v -s
```

Expected: Test passes. Output shows factor adjustments over ~30 seconds.

**Step 3: Update README.md**

Add to the Testing section, after existing test commands:

```markdown
# Run end-to-end Docker test (requires Docker daemon)
uv run pytest tests/test_e2e_docker.py -v -s
```

Also update the Project Structure section to reflect new files:

```
DIMQ/
├── Dockerfile
├── docker-compose.yml
├── e2e/
│   └── config.yaml          # Worker config for Docker containers
├── src/dimq/
│   ├── tasks/
│   │   ├── __init__.py
│   │   └── load.py           # Pydantic wrapper for dimq_load_task
│   ...
├── dimq_load_task/            # Rust extension (renamed from load_task)
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/lib.rs
└── tests/
    ├── test_e2e_docker.py     # Docker-based e2e test
    ├── test_tasks_load.py     # LoadTask wrapper unit tests
    ...
```

**Step 4: Commit**

```bash
git add README.md
git commit -m "docs: update README with e2e test instructions and new project structure"
```
