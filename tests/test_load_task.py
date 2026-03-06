import pytest

dimq_load_task = pytest.importorskip("dimq_load_task")


def test_load_task_default():
    result = dimq_load_task.run(duration_seconds=0.2, memory_mb=1)
    assert "phases" in result
    assert "total_duration_seconds" in result
    assert "peak_memory_mb" in result
    assert result["peak_memory_mb"] == 1
    assert len(result["phases"]) == 3


def test_load_task_phases_have_correct_keys():
    result = dimq_load_task.run(duration_seconds=0.1, memory_mb=1, concurrency=1)
    for phase in result["phases"]:
        assert "type" in phase
        assert "start_seconds" in phase
        assert "duration_seconds" in phase


def test_load_task_phase_types():
    result = dimq_load_task.run(duration_seconds=0.1, memory_mb=1)
    types = {p["type"] for p in result["phases"]}
    assert types == {"memory", "cpu", "io"}
