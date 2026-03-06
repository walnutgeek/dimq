# E2E Docker Test Design

**Goal:** Verify end-to-end task processing with Docker-containerized workers, including adaptive parallelization factor adjustment under real load.

**Architecture:** Embedded orchestrator in the pytest process, 3 Docker worker containers connecting back to it, continuous LoadTask submission with periodic drain, assertions on adaptive tuning.

## Components

### 1. Rust Extension Rename

Rename `load_task` to `dimq_load_task` throughout:
- Directory: `load_task/` -> `dimq_load_task/`
- `Cargo.toml`: `name = "dimq_load_task"`, `lib.name = "dimq_load_task"`
- `pyproject.toml`: `name = "dimq-load-task"` (or `dimq_load_task`)
- Python imports: `import dimq_load_task`
- Update `tests/test_load_task.py` imports

### 2. LoadTask Python Wrapper

New file: `src/dimq/tasks/load.py`

Pydantic-typed wrapper around `dimq_load_task.run()`:
- `LoadInput`: duration_seconds, concurrency, cpu_load, io_load, memory_mb
- `LoadOutput`: total_duration_seconds, peak_memory_mb
- `run_load(input: LoadInput) -> LoadOutput`

Registered in config as `"dimq.tasks.load:run_load"`.

### 3. Docker Image (Dockerfile)

Multi-stage build:
1. **Builder stage:** Rust toolchain + Python 3.9 + uv. Builds `dimq_load_task` wheel via maturin.
2. **Runtime stage:** Python 3.9 + uv. Installs `dimq` package + pre-built `dimq_load_task` wheel. No Rust in final image.

Worker entrypoint: `dimq worker --config /app/config.yaml --endpoint <orchestrator-address>`

### 4. docker-compose.yml

Three worker services using the same image:

```yaml
services:
  worker-1:
    build: .
    command: dimq worker --config /app/config.yaml --endpoint tcp://host.docker.internal:5555
  worker-2:
    build: .
    command: dimq worker --config /app/config.yaml --endpoint tcp://host.docker.internal:5555
  worker-3:
    build: .
    command: dimq worker --config /app/config.yaml --endpoint tcp://host.docker.internal:5555
```

Workers connect to host orchestrator via `host.docker.internal`.

### 5. Configs

**Worker config** (baked into Docker image at `/app/config.yaml`):
```yaml
endpoint: "tcp://placeholder:5555"
tasks:
  - name: "dimq.tasks.load:run_load"
    max_retries: 1
    timeout_seconds: 30
```

The `endpoint` is overridden by the `--endpoint` CLI flag at runtime.

**Orchestrator config** (constructed in-process by the test):
```python
DimqConfig(
    endpoint="tcp://0.0.0.0:5555",
    client_endpoint=None,
    heartbeat_interval_seconds=1,
    heartbeat_timeout_missed=3,
    adaptive_window_seconds=2.0,
    adaptive_reprobe_seconds=10.0,
    tasks=[
        TaskConfig(name="dimq.tasks.load:run_load", max_retries=1, timeout_seconds=30),
    ],
)
```

### 6. E2E Test (`tests/test_e2e_docker.py`)

Marked with `@pytest.mark.e2e` (excluded from default test runs).

**Setup fixture:**
1. Start embedded orchestrator with fast adaptive config
2. Run `docker compose up -d` to start 3 worker containers
3. Wait for 3 workers to register (poll `orchestrator.workers`, timeout after ~10s)

**Test body (~30-40 seconds):**
1. Submit batches of LoadTask tasks (5-10 per batch) via `orchestrator.submit_task()`
2. Each task: short duration (~1-2s), moderate CPU/IO
3. Every 2-3 seconds, call `orchestrator.drain_finished()` to clear completed tasks
4. Record each worker's `parallelization_factor` at each drain interval

**Assertions:**
- All submitted tasks eventually complete (no stuck tasks)
- At least one worker's parallelization factor changed from its initial value
- All 3 workers received tasks (dispatch distributed)
- `drain_finished()` cleared tasks from `orchestrator.tasks`

**Teardown:**
- `docker compose down`
- Orchestrator shutdown

### 7. Test Isolation

- pytest marker: `@pytest.mark.e2e`
- Run with: `uv run pytest -m e2e -v`
- Default `pytest` skips e2e tests
- pyproject.toml marker registration: `markers = ["e2e: end-to-end Docker tests"]`
