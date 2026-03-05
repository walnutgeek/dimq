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
