import pytest

dimq_load_task = pytest.importorskip("dimq_load_task")

from dimq.tasks.load import LoadInput, LoadOutput, run_load


def test_run_load_returns_output():
    inp = LoadInput(duration_seconds=0.2, concurrency=1, cpu_load=0.5, io_load=0.1, memory_mb=1)
    result = run_load(inp)
    assert isinstance(result, LoadOutput)
    assert result.total_duration_seconds > 0
    assert result.peak_memory_mb == 1


def test_run_load_default_input():
    inp = LoadInput()
    result = run_load(inp)
    assert isinstance(result, LoadOutput)
    assert result.total_duration_seconds > 0
