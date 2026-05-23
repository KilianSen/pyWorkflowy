"""pyWorkflowy — a full workflow engine for async/parallelized Python tasks."""

from __future__ import annotations

from pyworkflowy._backends import Pool, PoolKind
from pyworkflowy._classbased import TaskBase
from pyworkflowy._core import (
    Backoff,
    DepFailurePolicy,
    Task,
    TaskContext,
    TaskHandle,
    TaskResult,
    TaskStatus,
    current_task,
    task,
)
from pyworkflowy._events import EventHandler, EventSource
from pyworkflowy._persistence import Checkpointer, JSONCheckpointer, PickleCheckpointer
from pyworkflowy._runner import TaskRunner, get_current_runner
from pyworkflowy.exceptions import (
    CheckpointError,
    CycleError,
    DependencyFailedError,
    RetryExhaustedError,
    TaskCancelledError,
    TaskError,
    TaskTimeoutError,
)

__all__ = [
    "Backoff",
    "CheckpointError",
    "Checkpointer",
    "CycleError",
    "DepFailurePolicy",
    "DependencyFailedError",
    "EventHandler",
    "EventSource",
    "JSONCheckpointer",
    "PickleCheckpointer",
    "Pool",
    "PoolKind",
    "RetryExhaustedError",
    "Task",
    "TaskBase",
    "TaskCancelledError",
    "TaskContext",
    "TaskError",
    "TaskHandle",
    "TaskResult",
    "TaskRunner",
    "TaskStatus",
    "TaskTimeoutError",
    "current_task",
    "get_current_runner",
    "task",
]

__version__ = "0.5.0"
