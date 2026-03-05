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
