# DIMQ - Distributed In-Memory Queue Design

## Overview

General-purpose distributed task processing framework. Workers connect to a central orchestrator via ZeroMQ, receive tasks, and return results. The orchestrator adaptively tunes per-worker parallelization to maximize throughput.

## Architecture

**Communication pattern:** ZMQ ROUTER with worker-pull model. Workers connect to the orchestrator's ROUTER socket, announce readiness, and receive tasks. Natural backpressure — workers only get work when they have free slots.

**Two ROUTER endpoints on orchestrator:**
1. Worker endpoint — handles worker registration, heartbeats, task dispatch, results
2. Client endpoint — handles task submission, status queries, result retrieval

## Communication Protocol

### Message Types

**Worker → Orchestrator:**
- `[READY, worker_id, cpu_count]` — registration, cpu_count used as initial parallelization factor
- `[HEARTBEAT, worker_id, active_task_count]` — periodic liveness signal
- `[RESULT, task_id, status, json_payload]` — task completion (status: COMPLETED | TIMEOUT | ERROR)

**Orchestrator → Worker:**
- `[TASK, task_id, task_type, json_payload, parallelization_factor]` — dispatch work + updated parallelization limit
- `[IDLE, parallelization_factor]` — no work available, but here's your updated limit

**Client → Orchestrator:**
- `[SUBMIT, task_type, task_id, json_payload]` — submit a task
- `[STATUS, task_id]` — query task status
- `[RESULT, task_id]` — retrieve task result

**Orchestrator → Client:**
- `[ACK, task_id]` — task accepted
- `[STATUS_REPLY, task_id, status, attempt_history]` — task status with attempt details
- `[RESULT_REPLY, task_id, status, json_payload]` — task result

### Flow

1. Worker starts, connects, sends READY with cpu_count
2. Client submits task via SUBMIT
3. Orchestrator queues task (FIFO), sends ACK
4. Orchestrator dispatches to a worker with free slots, includes current parallelization_factor
5. Worker runs task, sends RESULT back
6. Orchestrator stores result, dispatches more work or sends IDLE
7. Client polls for status/result

### Heartbeat & Failure Detection

- Workers send HEARTBEAT every N seconds (configurable, default 5s)
- Orchestrator marks worker dead after M missed heartbeats (configurable, default 3)
- Dead worker's in-flight tasks are re-queued (subject to retry policy)

## Adaptive Parallelization

Per-worker, independent tuning:

1. Initial value = worker's reported `cpu_count`
2. Every evaluation window (default 30s), measure throughput: `completed_tasks / window_duration`
3. Throughput increased → increment factor by 1, keep probing
4. Throughput plateaued or decreased → decrement by 1, enter steady mode
5. In steady mode, re-probe every 5 minutes by bumping +1
6. Minimum factor is always 1
7. Updated factor is communicated to worker via TASK or IDLE messages

## Task System

A task is a plain Python function with Pydantic-typed input and output:

```python
# in_out_task.py
class In(BaseModel):
    data: str

class Out(BaseModel):
    result: str

def build(input: In) -> Out:
    return Out(result=input.data.upper())
```

Referenced as `"in_out_task:build"` in config. Can be sync or async.

The framework uses `inspect` to extract:
- Input type from the first parameter's type annotation
- Output type from the return type annotation
- Whether the function is async (`inspect.iscoroutinefunction`)

### Timeout & Retry

- **Worker responsibility:** Cancel task if it exceeds `timeout_seconds`, report TIMEOUT to orchestrator. Worker does NOT restart tasks.
- **Orchestrator responsibility:** Track retry count, re-queue on any available worker if under `max_retries`. After exhausting retries, mark FAILED with full attempt history.
- **Worker disappearance:** Orchestrator detects via missed heartbeats, re-queues all in-flight tasks from that worker.

### Task States

`PENDING → RUNNING → COMPLETED | FAILED | RETRYING`

Orchestrator stores per-attempt records: attempt number, worker_id, start time, end time, status, error info.

## Configuration

Single shared config file used by both orchestrator and workers:

```yaml
endpoint: "tcp://0.0.0.0:5555"
heartbeat_interval_seconds: 5
heartbeat_timeout_missed: 3

tasks:
  - name: "in_out_task:build"
    max_retries: 3
    timeout_seconds: 30
  - name: "load_task:run"
    max_retries: 1
    timeout_seconds: 300
```

Workers connecting to remote orchestrator override the endpoint via CLI arg or env var.

## Result Storage

In-memory dict: `task_id → TaskResult` (status, output, error info, attempt history, timestamps). No persistence in v1 — results lost on orchestrator restart.

## LoadTask (Rust / pyo3)

Stress-testing task implemented in Rust using pyo3, built with maturin.

**Input:**
```python
class LoadTaskInput(BaseModel):
    duration_seconds: float = 1.0
    concurrency: int = 1          # CPU threads to use
    cpu_load: float = 0.7         # fraction of duration for CPU work
    io_load: float = 0.2          # fraction for simulated I/O
    memory_mb: int = 50           # memory to allocate
```

**Implementation:**
- Releases GIL via `py.allow_threads`
- CPU work: rayon-parallelized hashing/matrix ops across `concurrency` threads
- I/O simulation: thread sleep + temp file writes
- Memory pressure: allocate and touch `memory_mb` of memory
- Logs each phase with timestamps

**Output:**
```python
class LoadTaskOutput(BaseModel):
    phases: list[LoadPhase]       # type, start, duration per phase
    total_duration_seconds: float
    peak_memory_mb: int
```

**Build:** Separate crate under `load_task/`, uses `maturin develop` for dev builds.

## Project Structure

```
DIMQ/
├── pyproject.toml
├── src/
│   └── dimq/
│       ├── __init__.py
│       ├── orchestrator.py
│       ├── worker.py
│       ├── task.py             # task loading, introspection, registry
│       ├── models.py           # shared Pydantic models
│       ├── config.py           # config loading
│       └── adaptive.py         # adaptive parallelization logic
├── load_task/
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/
│       └── lib.rs
├── tests/
│   ├── test_orchestrator.py
│   ├── test_worker.py
│   ├── test_adaptive.py
│   └── test_integration.py
└── docs/
    └── plans/

Dependencies: pyzmq, pydantic, pyyaml, structlog
Rust: pyo3, maturin, rayon, tokio
```
