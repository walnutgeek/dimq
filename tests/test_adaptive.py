import time

from dimq.adaptive import AdaptiveController


def test_initial_factor_is_two():
    ctrl = AdaptiveController(cpu_count=16, window_seconds=0.1)
    assert ctrl.factor == 2


def test_cpu_count_stored():
    ctrl = AdaptiveController(cpu_count=8)
    assert ctrl.cpu_count == 8


def test_doubling_phase():
    ctrl = AdaptiveController(cpu_count=4, window_seconds=0.05)

    # Window 1: 5 completions → throughput > 0, double: 2 → 4
    for _ in range(5):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == 4

    # Window 2: 15 completions → higher throughput, double: 4 → 8
    for _ in range(15):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == 8

    # Window 3: throughput drops → revert to 8 // 2 = 4, exit doubling
    for _ in range(5):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == 4
    assert not ctrl._doubling


def test_linear_after_doubling():
    ctrl = AdaptiveController(cpu_count=4, window_seconds=0.05)

    # Doubling: 2 → 4
    for _ in range(5):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == 4

    # Throughput drops → revert to 2, enter steady
    for _ in range(2):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == 2

    # Wait for reprobe
    ctrl._steady_since = time.monotonic() - ctrl.reprobe_seconds - 1
    time.sleep(0.06)
    ctrl.evaluate()
    # Reprobe: 2 → 3 (linear now)
    assert ctrl.factor == 3

    # More throughput → linear increment: 3 → 4
    for _ in range(20):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == 4


def test_decrease_in_linear_phase():
    ctrl = AdaptiveController(cpu_count=2, window_seconds=0.05)
    ctrl._doubling = False
    ctrl.factor = 5

    # Window 1: establish baseline
    for _ in range(10):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    prev_factor = ctrl.factor

    # Window 2: lower throughput → decrease by 1
    for _ in range(5):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == prev_factor - 1


def test_minimum_factor_is_one():
    ctrl = AdaptiveController(cpu_count=1, window_seconds=0.05)
    ctrl._doubling = False
    ctrl.factor = 1
    # No completions — throughput drops
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor >= 1


def test_halve_on_timeout():
    ctrl = AdaptiveController(cpu_count=8)
    assert ctrl.factor == 2

    ctrl.record_timeout()
    assert ctrl.factor == 1

    ctrl.record_timeout()
    assert ctrl.factor == 1


def test_timeout_exits_doubling():
    ctrl = AdaptiveController(cpu_count=8)
    assert ctrl._doubling is True

    ctrl.record_timeout()
    assert ctrl._doubling is False
