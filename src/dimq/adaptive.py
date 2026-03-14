from __future__ import annotations

import time


class AdaptiveController:
    def __init__(
        self,
        cpu_count: int,
        window_seconds: float = 30.0,
        reprobe_seconds: float = 300.0,
    ):
        self.cpu_count: int = cpu_count  # informational only
        self.factor: int = 2
        self.window_seconds = window_seconds
        self.reprobe_seconds = reprobe_seconds

        self._completions: int = 0
        self._window_start: float = time.monotonic()
        self._prev_throughput: float = 0.0
        self._doubling: bool = True
        self._steady: bool = False
        self._steady_since: float = 0.0

    def record_completion(self) -> None:
        self._completions += 1

    def record_timeout(self) -> None:
        self.factor = max(1, self.factor // 2)
        self._doubling = False
        self._steady = False

    def evaluate(self) -> None:
        now = time.monotonic()
        elapsed = now - self._window_start
        if elapsed <= 0:
            return

        throughput = self._completions / elapsed

        if self._steady:
            # In steady mode, check if it's time to re-probe
            if now - self._steady_since >= self.reprobe_seconds:
                self.factor += 1
                self._steady = False
        elif self._doubling:
            if throughput > self._prev_throughput:
                self.factor *= 2
            else:
                self.factor = max(1, self.factor // 2)
                self._doubling = False
                self._steady = True
                self._steady_since = now
        else:
            if throughput > self._prev_throughput:
                self.factor += 1
            else:
                self.factor = max(1, self.factor - 1)
                self._steady = True
                self._steady_since = now

        self._prev_throughput = throughput
        self._completions = 0
        self._window_start = now
