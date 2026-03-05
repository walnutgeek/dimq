import time

from dimq.adaptive import AdaptiveController


def test_initial_factor_from_cpu_count():
    ctrl = AdaptiveController(cpu_count=4, window_seconds=0.1)
    assert ctrl.factor == 4


def test_increase_on_throughput_gain():
    ctrl = AdaptiveController(cpu_count=1, window_seconds=0.05)
    # Window 1: 5 completions
    for _ in range(5):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    # Throughput > 0, should increase
    assert ctrl.factor == 2

    # Window 2: 15 completions (higher throughput)
    for _ in range(15):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == 3


def test_decrease_on_throughput_plateau():
    ctrl = AdaptiveController(cpu_count=2, window_seconds=0.05)

    # Window 1: establish baseline
    for _ in range(10):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    prev_factor = ctrl.factor

    # Window 2: same or lower throughput
    for _ in range(5):
        ctrl.record_completion()
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor == prev_factor - 1


def test_minimum_factor_is_one():
    ctrl = AdaptiveController(cpu_count=1, window_seconds=0.05)
    # No completions — throughput drops
    time.sleep(0.06)
    ctrl.evaluate()
    assert ctrl.factor >= 1
