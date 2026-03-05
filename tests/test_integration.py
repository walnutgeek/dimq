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
