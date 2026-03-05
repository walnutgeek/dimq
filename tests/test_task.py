import pytest
from pydantic import BaseModel

from dimq.task import load_task_func, TaskFunc
from tests.sample_tasks import EchoInput, EchoOutput


def test_load_sync_task():
    tf = load_task_func("tests.sample_tasks:echo")
    assert tf.input_model is EchoInput
    assert tf.output_model is EchoOutput
    assert tf.is_async is False

    result = tf.func(EchoInput(message="hello"))
    assert result == EchoOutput(echoed="hello")


def test_load_async_task():
    tf = load_task_func("tests.sample_tasks:async_echo")
    assert tf.is_async is True


def test_load_task_bad_annotations():
    with pytest.raises(ValueError, match="type annotation"):
        load_task_func("tests.sample_tasks:bad_no_annotations")


def test_load_task_module_not_found():
    with pytest.raises(ImportError):
        load_task_func("nonexistent_module:func")


def test_load_task_func_not_found():
    with pytest.raises(AttributeError):
        load_task_func("tests.sample_tasks:nonexistent")
