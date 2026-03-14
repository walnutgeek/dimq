from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import structlog
import zmq
import zmq.asyncio

from dimq.adaptive import AdaptiveController
from dimq.models import (
    DimqConfig,
    TaskAttempt,
    TaskRecord,
    TaskStatus,
)

logger = structlog.get_logger()


@dataclass
class WorkerState:
    worker_id: str
    cpu_count: int
    parallelization_factor: int
    active_tasks: Set[str] = field(default_factory=set)
    last_heartbeat: float = field(default_factory=time.monotonic)
    adaptive: Optional[AdaptiveController] = None

    @property
    def free_slots(self) -> int:
        return max(0, self.parallelization_factor - len(self.active_tasks))


class Orchestrator:
    def __init__(self, config: DimqConfig):
        self.config = config
        self.workers: dict[str, WorkerState] = {}
        self.tasks: dict[str, TaskRecord] = {}
        self.task_queue: deque[str] = deque()  # task_ids in FIFO order
        self._task_configs = {tc.name: tc for tc in config.tasks}
        self._running = False
        self._ctx: zmq.asyncio.Context | None = None

    def shutdown(self) -> None:
        self._running = False

    # ------------------------------------------------------------------
    # Public API — usable directly when embedding the orchestrator
    # ------------------------------------------------------------------

    def submit_task(self, task_type: str, task_id: str, payload: str) -> TaskRecord:
        """Submit a task for processing. Returns the created TaskRecord."""
        record = TaskRecord(
            task_id=task_id,
            task_type=task_type,
            payload=payload,
        )
        self.tasks[task_id] = record
        self.task_queue.append(task_id)
        logger.info("task.submitted", task_id=task_id, task_type=task_type)
        return record

    def get_status(self, task_id: str) -> Optional[Tuple[TaskStatus, list]]:
        """Get task status and attempt history. Returns None if not found."""
        record = self.tasks.get(task_id)
        if not record:
            return None
        return record.status, [a.model_dump(mode="json") for a in record.attempts]

    def get_result(self, task_id: str) -> Optional[Tuple[TaskStatus, str]]:
        """Get task result. Returns (status, payload) or None if not found."""
        record = self.tasks.get(task_id)
        if not record:
            return None
        return record.status, record.result_payload or ""

    def drain_finished(self) -> List[TaskRecord]:
        """Remove and return all finished tasks (COMPLETED or FAILED)."""
        finished = []
        for task_id in list(self.tasks):
            record = self.tasks[task_id]
            if record.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                finished.append(self.tasks.pop(task_id))
        return finished

    # ------------------------------------------------------------------
    # ZMQ event loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        self._running = True
        self._ctx = zmq.asyncio.Context()

        worker_sock = self._ctx.socket(zmq.ROUTER)
        worker_sock.bind(self.config.endpoint)

        client_sock = None
        if self.config.client_endpoint:
            client_sock = self._ctx.socket(zmq.ROUTER)
            client_sock.bind(self.config.client_endpoint)

        logger.info(
            "orchestrator.started",
            worker_endpoint=self.config.endpoint,
            client_endpoint=self.config.client_endpoint,
        )

        poller = zmq.asyncio.Poller()
        poller.register(worker_sock, zmq.POLLIN)
        if client_sock:
            poller.register(client_sock, zmq.POLLIN)

        heartbeat_check_task = asyncio.create_task(self._heartbeat_checker())
        adaptive_task = asyncio.create_task(self._adaptive_evaluator())

        try:
            while self._running:
                events = dict(await poller.poll(timeout=100))

                if worker_sock in events:
                    frames = await worker_sock.recv_multipart()
                    identity = frames[0]
                    msg = frames[1:]
                    await self._handle_worker_msg(worker_sock, identity, msg)

                if client_sock and client_sock in events:
                    frames = await client_sock.recv_multipart()
                    identity = frames[0]
                    msg = frames[1:]
                    await self._handle_client_msg(client_sock, identity, msg)

                await self._dispatch_tasks(worker_sock)
                await self._send_idle_updates(worker_sock)
        finally:
            heartbeat_check_task.cancel()
            adaptive_task.cancel()
            worker_sock.close()
            if client_sock:
                client_sock.close()
            self._ctx.term()

    # ------------------------------------------------------------------
    # ZMQ message handlers (delegate to public API)
    # ------------------------------------------------------------------

    async def _handle_worker_msg(
        self, sock: zmq.asyncio.Socket, identity: bytes, msg: list[bytes]
    ) -> None:
        cmd = msg[0].decode()

        if cmd == "READY":
            worker_id = msg[1].decode()
            cpu_count = int(msg[2].decode())
            self.workers[worker_id] = WorkerState(
                worker_id=worker_id,
                cpu_count=cpu_count,
                parallelization_factor=2,
                adaptive=AdaptiveController(
                    cpu_count=cpu_count,
                    window_seconds=self.config.adaptive_window_seconds,
                    reprobe_seconds=self.config.adaptive_reprobe_seconds,
                ),
            )
            logger.info("worker.registered", worker_id=worker_id, cpu_count=cpu_count)

        elif cmd == "HEARTBEAT":
            worker_id = msg[1].decode()
            if worker_id in self.workers:
                self.workers[worker_id].last_heartbeat = time.monotonic()

        elif cmd == "RESULT":
            task_id = msg[1].decode()
            status_str = msg[2].decode()
            payload = msg[3].decode()
            self._record_task_result(task_id, status_str, payload)

    async def _handle_client_msg(
        self, sock: zmq.asyncio.Socket, identity: bytes, msg: list[bytes]
    ) -> None:
        cmd = msg[0].decode()

        if cmd == "SUBMIT":
            task_type = msg[1].decode()
            task_id = msg[2].decode()
            payload = msg[3].decode()
            self.submit_task(task_type, task_id, payload)
            await sock.send_multipart([identity, b"ACK", task_id.encode()])

        elif cmd == "STATUS":
            task_id = msg[1].decode()
            result = self.get_status(task_id)
            if result:
                status, attempts = result
                await sock.send_multipart([
                    identity,
                    b"STATUS_REPLY",
                    task_id.encode(),
                    status.value.encode(),
                    json.dumps(attempts).encode(),
                ])
            else:
                await sock.send_multipart([
                    identity, b"STATUS_REPLY", task_id.encode(),
                    b"NOT_FOUND", b"[]",
                ])

        elif cmd == "RESULT":
            task_id = msg[1].decode()
            result = self.get_result(task_id)
            if result:
                status, payload = result
                await sock.send_multipart([
                    identity, b"RESULT_REPLY", task_id.encode(),
                    status.value.encode(), payload.encode(),
                ])
            else:
                await sock.send_multipart([
                    identity, b"RESULT_REPLY", task_id.encode(),
                    b"NOT_FOUND", b"",
                ])

        elif cmd == "DRAIN":
            finished = self.drain_finished()
            records_json = json.dumps(
                [r.model_dump(mode="json") for r in finished]
            )
            await sock.send_multipart([
                identity, b"DRAIN_REPLY", records_json.encode(),
            ])

    # ------------------------------------------------------------------
    # Internal task result processing
    # ------------------------------------------------------------------

    def _record_task_result(
        self, task_id: str, status_str: str, payload: str,
        dead_worker: WorkerState | None = None,
    ) -> None:
        record = self.tasks.get(task_id)
        if not record:
            return

        # Find which worker had this task and update
        search_workers = [dead_worker] if dead_worker else list(self.workers.values())
        for w in search_workers:
            if task_id in w.active_tasks:
                w.active_tasks.discard(task_id)
                if w.adaptive:
                    if status_str == "TIMEOUT":
                        w.adaptive.record_timeout()
                        w.parallelization_factor = w.adaptive.factor
                        w.factor_changed = True
                    else:
                        w.adaptive.record_completion()
                # Update attempt
                for attempt in reversed(record.attempts):
                    if attempt.worker_id == w.worker_id and attempt.ended_at is None:
                        attempt.ended_at = datetime.now()
                        attempt.status = TaskStatus(status_str)
                        if status_str != "COMPLETED":
                            attempt.error = payload or f"Task {status_str}"
                        break
                break

        if status_str == "COMPLETED":
            record.status = TaskStatus.COMPLETED
            record.result_payload = payload
            logger.info("task.completed", task_id=task_id)
        else:
            # TIMEOUT or ERROR -- retry if possible
            task_cfg = self._task_configs.get(record.task_type)
            max_retries = task_cfg.max_retries if task_cfg else 3

            record.retry_count += 1
            if record.retry_count <= max_retries:
                record.status = TaskStatus.RETRYING
                self.task_queue.append(task_id)
                logger.info(
                    "task.retrying", task_id=task_id, retry=record.retry_count
                )
            else:
                record.status = TaskStatus.FAILED
                logger.info("task.failed", task_id=task_id, retries_exhausted=True)

    # ------------------------------------------------------------------
    # Task dispatch and background tasks
    # ------------------------------------------------------------------

    async def _dispatch_tasks(self, worker_sock: zmq.asyncio.Socket) -> None:
        if not self.task_queue:
            return

        for worker in self.workers.values():
            if not self.task_queue:
                break
            if worker.free_slots <= 0:
                continue
            while worker.free_slots > 0 and self.task_queue:
                task_id = self.task_queue.popleft()
                record = self.tasks[task_id]
                record.status = TaskStatus.RUNNING
                record.attempts.append(
                    TaskAttempt(
                        attempt_number=len(record.attempts) + 1,
                        worker_id=worker.worker_id,
                        started_at=datetime.now(),
                    )
                )
                worker.active_tasks.add(task_id)

                await worker_sock.send_multipart([
                    worker.worker_id.encode(),
                    b"TASK",
                    task_id.encode(),
                    record.task_type.encode(),
                    record.payload.encode(),
                    str(worker.parallelization_factor).encode(),
                ])
                logger.info(
                    "task.dispatched", task_id=task_id, worker=worker.worker_id
                )

    async def _heartbeat_checker(self) -> None:
        timeout = (
            self.config.heartbeat_interval_seconds
            * self.config.heartbeat_timeout_missed
        )
        while self._running:
            await asyncio.sleep(self.config.heartbeat_interval_seconds)
            now = time.monotonic()
            dead_workers = []
            for wid, ws in self.workers.items():
                if now - ws.last_heartbeat > timeout:
                    dead_workers.append(wid)

            for wid in dead_workers:
                ws = self.workers.pop(wid)
                # Re-queue all in-flight tasks (pass dead_worker so attempts are closed)
                for task_id in list(ws.active_tasks):
                    record = self.tasks.get(task_id)
                    if record and record.status == TaskStatus.RUNNING:
                        self._record_task_result(
                            task_id, "TIMEOUT", "", dead_worker=ws
                        )
                logger.warning(
                    "worker.dead", worker_id=wid, requeued=len(ws.active_tasks)
                )

    async def _adaptive_evaluator(self) -> None:
        while self._running:
            await asyncio.sleep(self.config.adaptive_window_seconds)
            for ws in self.workers.values():
                if ws.adaptive:
                    old_factor = ws.parallelization_factor
                    ws.adaptive.evaluate()
                    ws.parallelization_factor = ws.adaptive.factor
                    if ws.parallelization_factor != old_factor:
                        ws.factor_changed = True

    async def _send_idle_updates(self, worker_sock: zmq.asyncio.Socket) -> None:
        """Send IDLE messages to workers whose parallelization factor changed."""
        for worker in self.workers.values():
            if getattr(worker, "factor_changed", False):
                await worker_sock.send_multipart([
                    worker.worker_id.encode(),
                    b"IDLE",
                    str(worker.parallelization_factor).encode(),
                ])
                worker.factor_changed = False
