import asyncio
import json
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


@pytest.mark.timeout(5)
async def test_drain_finished(orchestrator, worker_socket, client_socket):
    w_sock, worker_id = worker_socket
    c_sock, _ = client_socket

    await w_sock.send_multipart([b"READY", worker_id.encode(), b"2"])
    await asyncio.sleep(0.1)

    # Submit two tasks
    for tid in ["drain-001", "drain-002"]:
        await c_sock.send_multipart([
            b"SUBMIT", b"tests.sample_tasks:echo",
            tid.encode(), b'{"message": "x"}',
        ])
        await c_sock.recv_multipart()  # ACK

    # Complete first task
    msg1 = await w_sock.recv_multipart()
    await w_sock.send_multipart([
        b"RESULT", msg1[1], b"COMPLETED", b'{"echoed": "x"}',
    ])

    # Fail second task (exhaust retries: max_retries=2, so 3 attempts total)
    for _ in range(3):
        msg2 = await w_sock.recv_multipart()
        await w_sock.send_multipart([
            b"RESULT", msg2[1], b"ERROR", b'{"error": "boom"}',
        ])
        await asyncio.sleep(0.1)

    await asyncio.sleep(0.1)

    # Both should be finished now
    assert len(orchestrator.tasks) == 2
    finished = orchestrator.drain_finished()
    assert len(finished) == 2
    statuses = {r.status for r in finished}
    assert statuses == {TaskStatus.COMPLETED, TaskStatus.FAILED}

    # Tasks dict is now empty
    assert len(orchestrator.tasks) == 0

    # Draining again returns nothing
    assert orchestrator.drain_finished() == []


@pytest.mark.timeout(5)
async def test_direct_api_submit_and_result(orchestrator, worker_socket):
    """Test using the orchestrator's public methods directly (embedded mode)."""
    w_sock, worker_id = worker_socket

    await w_sock.send_multipart([b"READY", worker_id.encode(), b"1"])
    await asyncio.sleep(0.1)

    # Submit via direct API
    record = orchestrator.submit_task(
        "tests.sample_tasks:echo", "direct-001", '{"message": "embedded"}'
    )
    assert record.task_id == "direct-001"
    assert record.status == TaskStatus.PENDING

    # Status via direct API
    status, attempts = orchestrator.get_status("direct-001")
    assert status == TaskStatus.PENDING
    assert attempts == []

    # Let dispatch happen
    await asyncio.sleep(0.2)
    msg = await w_sock.recv_multipart()
    assert msg[0] == b"TASK"

    # Status should be RUNNING now
    status, attempts = orchestrator.get_status("direct-001")
    assert status == TaskStatus.RUNNING

    # Worker completes
    await w_sock.send_multipart([
        b"RESULT", b"direct-001", b"COMPLETED", b'{"echoed": "embedded"}',
    ])
    await asyncio.sleep(0.1)

    # Result via direct API
    status, payload = orchestrator.get_result("direct-001")
    assert status == TaskStatus.COMPLETED
    assert payload == '{"echoed": "embedded"}'

    # Not found returns None
    assert orchestrator.get_status("nonexistent") is None
    assert orchestrator.get_result("nonexistent") is None


@pytest.mark.timeout(5)
async def test_no_client_endpoint(config):
    """Orchestrator works without a client endpoint (embedded mode)."""
    config.client_endpoint = None
    orch = Orchestrator(config)
    task = asyncio.create_task(orch.run())
    await asyncio.sleep(0.1)

    # Direct API works
    record = orch.submit_task("tests.sample_tasks:echo", "embed-001", '{"message": "hi"}')
    assert record.status == TaskStatus.PENDING
    assert orch.get_status("embed-001") is not None

    orch.shutdown()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.timeout(5)
async def test_drain_via_client(orchestrator, worker_socket, client_socket):
    """Test DRAIN command over ZMQ client socket."""
    w_sock, worker_id = worker_socket
    c_sock, _ = client_socket

    await w_sock.send_multipart([b"READY", worker_id.encode(), b"1"])
    await asyncio.sleep(0.1)

    # Submit and complete a task
    orchestrator.submit_task("tests.sample_tasks:echo", "cdrain-001", '{"message": "z"}')
    await asyncio.sleep(0.2)

    msg = await w_sock.recv_multipart()
    await w_sock.send_multipart([
        b"RESULT", msg[1], b"COMPLETED", b'{"echoed": "z"}',
    ])
    await asyncio.sleep(0.1)

    # Drain via client
    await c_sock.send_multipart([b"DRAIN"])
    reply = await c_sock.recv_multipart()
    assert reply[0] == b"DRAIN_REPLY"
    records = json.loads(reply[1])
    assert len(records) == 1
    assert records[0]["task_id"] == "cdrain-001"
    assert records[0]["status"] == "COMPLETED"

    # Task is gone from orchestrator
    assert len(orchestrator.tasks) == 0
