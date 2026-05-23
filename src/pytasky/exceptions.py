"""Exceptions raised by pyTasky's runner, backends, scheduler, and persistence layer."""

from __future__ import annotations

__all__ = [
    "CheckpointError",
    "CycleError",
    "DependencyFailedError",
    "RetryExhaustedError",
    "TaskCancelledError",
    "TaskError",
    "TaskTimeoutError",
]


class TaskError(Exception):
    """Base class for every pyTasky-raised exception.

    Catch this if you want a single ``except`` clause for everything the
    runner, scheduler, or persistence layer can throw — narrower subclasses
    inherit from this and are preferred when you care about the specific
    failure mode.
    """


class TaskTimeoutError(TaskError):
    """Raised when a task exceeds its configured ``timeout=`` budget.

    Carries the elapsed seconds so callers can log / report the actual
    wall-clock spent before cancellation kicked in.
    """

    def __init__(self, message: str, *, elapsed: float | None = None) -> None:
        super().__init__(message)
        self.elapsed = elapsed


class TaskCancelledError(TaskError):
    """Raised inside ``handle.result()`` / ``await handle`` when the task was cancelled.

    Cooperative for asyncio (delivered via ``asyncio.CancelledError``),
    flag-based for the thread backend, and ``terminate()``-based for the
    process backend.
    """


class CycleError(TaskError):
    """Raised when a dependency cycle is detected in the submitted DAG.

    Detected eagerly at submit time (when a back-edge would close a cycle)
    *and* defensively before scheduling at ``runner.run()``. The error
    message names the cycle members so the offending edge is obvious.
    """


class DependencyFailedError(TaskError):
    """Raised inside a task whose dependency failed.

    Whether this fires depends on the task's ``on_dep_failure`` policy —
    only the ``"fail"`` policy raises this; ``"skip"`` and ``"run-anyway"``
    have their own (non-raising) behavior. The ``failed`` attribute lists
    the dependency names that failed.
    """

    def __init__(self, message: str, *, failed: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.failed = failed


class RetryExhaustedError(TaskError):
    """Raised when retries are exhausted *and* the last attempt failed.

    Wraps the last exception via ``__cause__`` so the original traceback
    remains accessible. Carries ``attempts`` (the number of attempts made,
    1-indexed including the original) for diagnostics.
    """

    def __init__(self, message: str, *, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts


class CheckpointError(TaskError):
    """Raised by a :class:`pytasky.Checkpointer` when state I/O fails.

    Wrapping read/write failures lets resume code distinguish a corrupt or
    missing checkpoint from genuine task failures.
    """
