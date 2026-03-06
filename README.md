# DIMQ - Distributed In-Memory Queue

A general-purpose distributed task processing framework built on ZeroMQ. Workers connect to a central orchestrator, receive tasks, and return results. The orchestrator adaptively tunes per-worker parallelization to maximize throughput.

## Architecture

```
                    +-------------------+
  Clients -------->| Orchestrator      |<-------- Workers
  (DEALER)         | (2x ROUTER)       |          (DEALER)
                   |                   |
  SUBMIT/STATUS/   | - FIFO task queue |  READY/HEARTBEAT/
  RESULT queries   | - Result storage  |  RESULT messages
                   | - Retry logic     |
                   | - Adaptive tuning |
                   +-------------------+
```

**Orchestrator** runs two ZMQ ROUTER sockets: one for workers (registration, heartbeats, task dispatch/results) and one for clients (task submission, status queries, result retrieval).

**Workers** connect via ZMQ DEALER sockets, register with their CPU count, and pull tasks. They run tasks concurrently (sync tasks in a thread pool, async tasks natively) and report results back. Workers handle timeout cancellation locally; the orchestrator owns all retry decisions.

**Adaptive parallelization** starts each worker's parallel task limit at its CPU count, then probes upward. If throughput plateaus or drops, it scales back by one and enters steady mode, re-probing periodically.

**Tasks** are plain Python functions with Pydantic-typed input and output. No base class or decorator needed -- the framework introspects types via `inspect`.

## Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)
- Rust toolchain (for the LoadTask extension only)

## Setup

```bash
# Install Python dependencies
uv sync

# (Optional) Build the Rust LoadTask extension
cd load_task
uv tool run maturin develop --uv
cd ..
```

## Running

Create a config file:

```yaml
# config.yaml
endpoint: "tcp://0.0.0.0:5555"
client_endpoint: "tcp://0.0.0.0:5556"
heartbeat_interval_seconds: 5
heartbeat_timeout_missed: 3

tasks:
  - name: "my_app.tasks:process"
    max_retries: 3
    timeout_seconds: 30
```

Start the orchestrator and one or more workers:

```bash
# Terminal 1: start orchestrator
uv run dimq orchestrator --config config.yaml

# Terminal 2: start a worker (same machine)
uv run dimq worker --config config.yaml

# Terminal 3: start a worker on another machine (override endpoint)
uv run dimq worker --config config.yaml --endpoint tcp://orchestrator-host:5555
```

### Submitting tasks programmatically

```python
import zmq

ctx = zmq.Context()
sock = ctx.socket(zmq.DEALER)
sock.connect("tcp://localhost:5556")

# Submit a task
sock.send_multipart([
    b"SUBMIT",
    b"my_app.tasks:process",    # task type
    b"task-001",                 # task ID
    b'{"input_field": "value"}', # JSON payload matching the Pydantic input model
])

# Receive ACK
ack = sock.recv_multipart()  # [b"ACK", b"task-001"]

# Later, query the result
sock.send_multipart([b"RESULT", b"task-001"])
reply = sock.recv_multipart()
# [b"RESULT_REPLY", b"task-001", b"COMPLETED", b'{"output_field": "result"}']
```

## Testing

```bash
# Run all tests
uv run pytest -v

# Run specific test modules
uv run pytest tests/test_orchestrator.py -v
uv run pytest tests/test_integration.py -v

# Run LoadTask tests (requires Rust extension to be built)
uv run pytest tests/test_load_task.py -v
```

## Writing Custom Tasks

A task is a plain function with Pydantic-typed input and output. It can be sync or async.

```python
# my_app/tasks.py
from pydantic import BaseModel

class ImageInput(BaseModel):
    url: str
    width: int
    height: int

class ImageOutput(BaseModel):
    thumbnail_path: str
    original_size_bytes: int

def resize(input: ImageInput) -> ImageOutput:
    # Your logic here
    return ImageOutput(
        thumbnail_path=f"/tmp/{input.width}x{input.height}.jpg",
        original_size_bytes=1024,
    )

# Async tasks work the same way
async def fetch_and_resize(input: ImageInput) -> ImageOutput:
    ...
```

Register it in your config:

```yaml
tasks:
  - name: "my_app.tasks:resize"
    max_retries: 2
    timeout_seconds: 60
  - name: "my_app.tasks:fetch_and_resize"
    max_retries: 3
    timeout_seconds: 120
```

The framework uses `inspect` to automatically extract:
- Input type from the first parameter's annotation
- Output type from the return annotation
- Whether the function is sync or async

Sync tasks run in a thread pool. Async tasks run natively in the event loop. If a task exceeds `timeout_seconds`, the worker cancels it and reports a timeout to the orchestrator, which handles retries.

## LoadTask (Rust Extension)

A built-in stress-testing task implemented in Rust (pyo3). It creates configurable CPU, I/O, and memory pressure while releasing the GIL so Python threading works efficiently.

```python
import load_task

result = load_task.run(
    duration_seconds=5.0,
    concurrency=4,       # number of CPU threads
    cpu_load=0.7,        # fraction of duration for CPU work (SHA-256 hashing)
    io_load=0.2,         # fraction for I/O (temp file writes)
    memory_mb=100,       # memory to allocate and touch
)
# result = {
#     "phases": [
#         {"type": "memory", "start_seconds": 0.0, "duration_seconds": 0.001},
#         {"type": "cpu", "start_seconds": 0.001, "duration_seconds": 3.5},
#         {"type": "io", "start_seconds": 3.501, "duration_seconds": 1.0},
#     ],
#     "total_duration_seconds": 4.501,
#     "peak_memory_mb": 100,
# }
```

## Project Structure

```
DIMQ/
├── pyproject.toml
├── src/dimq/
│   ├── orchestrator.py    # ZMQ ROUTER, dispatch, heartbeat, retry
│   ├── worker.py          # ZMQ DEALER, task execution, timeout
│   ├── adaptive.py        # Throughput-based parallelization tuning
│   ├── task.py            # Task loading and introspection via inspect
│   ├── models.py          # Pydantic models (TaskRecord, DimqConfig, etc.)
│   ├── config.py          # YAML config loading
│   └── cli.py             # CLI entry points
├── load_task/             # Rust extension (pyo3 + maturin)
│   ├── Cargo.toml
│   ├── pyproject.toml
│   └── src/lib.rs
└── tests/
    ├── test_orchestrator.py
    ├── test_worker.py
    ├── test_adaptive.py
    ├── test_integration.py
    ├── test_load_task.py
    ├── test_models.py
    ├── test_config.py
    ├── test_task.py
    └── sample_tasks.py    # Test fixtures
```
