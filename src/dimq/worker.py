from __future__ import annotations

import asyncio
import os
import uuid
import json
from concurrent.futures import ThreadPoolExecutor

import structlog
import zmq
import zmq.asyncio

from dimq.models import DimqConfig
from dimq.task import load_task_func, TaskFunc

logger = structlog.get_logger()


class Worker:
    def __init__(self, config: DimqConfig):
        self.config = config
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self.cpu_count = os.cpu_count()
        self._running = False
        self._parallelization_factor = 2
        self._active_tasks: set[str] = set()
        self._task_funcs: dict[str, TaskFunc] = {}
        self._executor = ThreadPoolExecutor(max_workers=self.cpu_count * 2)

        # Load task functions
        for tc in config.tasks:
            self._task_funcs[tc.name] = load_task_func(tc.name)
            logger.info("worker.task_loaded", task=tc.name)

        # Build timeout lookup
        self._timeouts = {tc.name: tc.timeout_seconds for tc in config.tasks}

    def shutdown(self) -> None:
        self._running = False

    async def run(self) -> None:
        self._running = True
        ctx = zmq.asyncio.Context()
        sock = ctx.socket(zmq.DEALER)
        sock.setsockopt_string(zmq.IDENTITY, self.worker_id)
        sock.connect(self.config.endpoint)

        # Register
        await sock.send_multipart([
            b"READY",
            self.worker_id.encode(),
            str(self.cpu_count).encode(),
        ])
        logger.info("worker.registered", worker_id=self.worker_id, cpu_count=self.cpu_count)

        heartbeat_task = asyncio.create_task(self._heartbeat_loop(sock))

        try:
            while self._running:
                try:
                    msg = await asyncio.wait_for(sock.recv_multipart(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue

                cmd = msg[0].decode()

                if cmd == "TASK":
                    task_id = msg[1].decode()
                    task_type = msg[2].decode()
                    payload = msg[3].decode()
                    new_factor = int(msg[4].decode())
                    self._parallelization_factor = new_factor

                    self._active_tasks.add(task_id)
                    asyncio.create_task(
                        self._execute_task(sock, task_id, task_type, payload)
                    )

                elif cmd == "IDLE":
                    new_factor = int(msg[1].decode())
                    self._parallelization_factor = new_factor

        finally:
            heartbeat_task.cancel()
            sock.close()
            ctx.term()

    async def _execute_task(
        self,
        sock: zmq.asyncio.Socket,
        task_id: str,
        task_type: str,
        payload: str,
    ) -> None:
        tf = self._task_funcs.get(task_type)
        if not tf:
            await sock.send_multipart([
                b"RESULT", task_id.encode(), b"ERROR",
                json.dumps({"error": f"Unknown task type: {task_type}"}).encode(),
            ])
            self._active_tasks.discard(task_id)
            return

        timeout = self._timeouts.get(task_type, 60.0)
        input_obj = tf.input_model.model_validate_json(payload)

        try:
            if tf.is_async:
                result = await asyncio.wait_for(
                    tf.func(input_obj), timeout=timeout
                )
            else:
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(self._executor, tf.func, input_obj),
                    timeout=timeout,
                )

            await sock.send_multipart([
                b"RESULT",
                task_id.encode(),
                b"COMPLETED",
                result.model_dump_json().encode(),
            ])
            logger.info("worker.task_completed", task_id=task_id)

        except asyncio.TimeoutError:
            await sock.send_multipart([
                b"RESULT", task_id.encode(), b"TIMEOUT",
                json.dumps({"error": "Task timed out"}).encode(),
            ])
            logger.warning("worker.task_timeout", task_id=task_id)

        except Exception as e:
            await sock.send_multipart([
                b"RESULT", task_id.encode(), b"ERROR",
                json.dumps({"error": str(e)}).encode(),
            ])
            logger.error("worker.task_error", task_id=task_id, error=str(e))

        finally:
            self._active_tasks.discard(task_id)

    async def _heartbeat_loop(self, sock: zmq.asyncio.Socket) -> None:
        while self._running:
            await asyncio.sleep(self.config.heartbeat_interval_seconds)
            await sock.send_multipart([
                b"HEARTBEAT",
                self.worker_id.encode(),
                str(len(self._active_tasks)).encode(),
            ])
