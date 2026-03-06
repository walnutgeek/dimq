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
