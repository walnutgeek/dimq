
## LoadTask (Rust Extension)

A built-in stress-testing task implemented in Rust (pyo3). It creates configurable CPU, I/O, and memory pressure while releasing the GIL so Python threading works efficiently.

```python
import dimq_load_task

result = dimq_load_task.run(
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

