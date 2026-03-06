from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Type, get_type_hints

from pydantic import BaseModel


@dataclass
class TaskFunc:
    func: Callable
    input_model: Type[BaseModel]
    output_model: Type[BaseModel]
    is_async: bool


def load_task_func(task_name: str) -> TaskFunc:
    """Load a task function from 'module.path:function_name' string.

    Uses inspect to extract Pydantic input/output types and async status.
    """
    module_path, func_name = task_name.rsplit(":", 1)
    module = importlib.import_module(module_path)
    func = getattr(module, func_name)

    hints = get_type_hints(func)
    params = inspect.signature(func).parameters

    # Get input type from first parameter
    param_names = list(params.keys())
    if not param_names or param_names[0] not in hints:
        raise ValueError(
            f"Task '{task_name}': first parameter must have a Pydantic type annotation"
        )
    input_model = hints[param_names[0]]

    # Get output type from return annotation
    if "return" not in hints:
        raise ValueError(
            f"Task '{task_name}': must have a return type annotation"
        )
    output_model = hints["return"]

    return TaskFunc(
        func=func,
        input_model=input_model,
        output_model=output_model,
        is_async=inspect.iscoroutinefunction(func),
    )
