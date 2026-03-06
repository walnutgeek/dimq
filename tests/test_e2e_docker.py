from __future__ import annotations

import asyncio
import json
import subprocess
import time

import pytest

from dimq.models import DimqConfig, TaskConfig
from dimq.orchestrator import Orchestrator


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
        timeout=300,
    )
    yield
    subprocess.run(
        ["docker", "compose", "down", "--remove-orphans"],
        timeout=60,
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

    # Run load loop for ~30 seconds.
    # Workers start with parallelization_factor == cpu_count (12 in Docker).
    # We need batch_size > single-worker capacity so tasks overflow to all workers.
    test_duration = 30.0
    batch_size = 40  # must exceed a single worker's slot count
    drain_interval = 3.0
    task_counter = 0
    start = time.monotonic()

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
