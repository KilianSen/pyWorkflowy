"""Core task primitives: ``Task``, ``TaskHandle``, ``TaskResult``, ``TaskStatus``.

These pieces are deliberately decoupled from execution — they describe *what*
a task is and *what a submitted task looks like*, leaving the actual scheduling
to :mod:`pytasky._runner` and the backend code in :mod:`pytasky._backends`.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Iterable
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import StrEnum
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from pytasky.exceptions import TaskCancelledError, TaskError

if TYPE_CHECKING:
    from pytasky._runner import TaskRunner

__all__ = [
    "Backend",
    "Backoff",
    "DepFailurePolicy",
    "Task",
    "TaskContext",
    "TaskHandle",
    "TaskResult",
    "TaskStatus",
    "current_task",
    "task",
]

R = TypeVar("R")

Backend = Literal["asyncio", "thread", "process"]
Backoff = Literal["none", "linear", "exponential"]
DepFailurePolicy = Literal["skip", "fail", "run-anyway"]

_DEFAULT_RETRY_ON: tuple[type[BaseException], ...] = (Exception,)


class TaskStatus(StrEnum):
    """Lifecycle state of a submitted task.

    Transitions: ``PENDING`` → ``READY`` → ``RUNNING`` → (``RETRYING`` →
    ``RUNNING``)* → (``COMPLETED`` | ``FAILED`` | ``CANCELLED`` |
    ``SKIPPED``). ``SKIPPED`` is only reached when a dependency failed under
    the ``on_dep_failure="skip"`` policy.
    """

    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    RETRYING = "retrying"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


_TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.SKIPPED}
)


@dataclass(frozen=True, slots=True)
class TaskResult(Generic[R]):
    """Immutable record of how a single task call finished.

    Exactly one of ``value`` / ``error`` is meaningful — ``status`` is the
    authoritative truth and the other field is ``None`` when irrelevant.
    Duration is wall-clock seconds spent inside the runner (queued time not
    included); use ``finished_at - started_at`` if you need a different
    bracket.
    """

    name: str
    status: TaskStatus
    value: R | None = None
    error: BaseException | None = None
    attempts: int = 1
    started_at: float | None = None
    finished_at: float | None = None

    @property
    def duration(self) -> float | None:
        """Wall-clock seconds between ``started_at`` and ``finished_at``.

        Returns ``None`` while the task has not started, or for tasks that
        were skipped before ever running.
        """
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at

    @property
    def ok(self) -> bool:
        """True iff the task reached ``COMPLETED``."""
        return self.status == TaskStatus.COMPLETED


@dataclass(slots=True)
class TaskContext:
    """Ambient per-task state, exposed via :func:`current_task`.

    Read-only from user code — fields are owned by the runner. Useful for
    logging the current task name and attempt number from inside the task
    body itself.
    """

    name: str
    attempt: int
    cancel_event: threading.Event


_current_task: ContextVar[TaskContext | None] = ContextVar("pytasky_current_task", default=None)


def current_task() -> TaskContext | None:
    """Return the currently running task's :class:`TaskContext`, or ``None``.

    Useful for shared utilities that want to log the task name without
    threading it through every call. Implemented via
    :class:`contextvars.ContextVar` so it works across asyncio tasks; the
    thread and process backends populate it manually at entry.
    """
    return _current_task.get()


@dataclass(frozen=True, slots=True)
class Task(Generic[R]):
    """A reusable, configured callable wrapped for the runner.

    Built by the :func:`task` decorator or by subclassing :class:`Task`-like
    classes (see :class:`ClassTask` in this module). All execution-shaping
    knobs live here; ``.submit(...)`` returns a :class:`TaskHandle` bound to
    the currently active runner.
    """

    fn: Callable[..., Any]
    name: str
    backend: Backend = "asyncio"
    retries: int = 0
    timeout: float | None = None
    backoff: Backoff = "exponential"
    backoff_base: float = 1.0
    backoff_max: float = 30.0
    retry_on: tuple[type[BaseException], ...] = _DEFAULT_RETRY_ON
    on_dep_failure: DepFailurePolicy = "fail"

    @property
    def is_async(self) -> bool:
        """True iff ``fn`` is a coroutine function."""
        return iscoroutinefunction(self.fn)

    @property
    def max_attempts(self) -> int:
        """Total attempts including the original — i.e. ``retries + 1``."""
        return self.retries + 1

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Direct call: bypass the runner and invoke ``fn`` synchronously.

        Useful for tests that want to exercise the wrapped function as-is.
        Async tasks return their coroutine — ``await task(...)`` to run it.
        """
        return self.fn(*args, **kwargs)

    def submit(
        self,
        *args: Any,
        depends_on: Iterable[TaskHandle[Any]] = (),
        runner: TaskRunner | None = None,
        **kwargs: Any,
    ) -> TaskHandle[R]:
        """Submit this task to ``runner`` (or the ambient one) and return its handle.

        ``depends_on`` lists the handles that must finish before this task
        becomes ``READY``. The ``on_dep_failure`` policy on this :class:`Task`
        decides what happens if any of them ended in a non-``COMPLETED``
        state.
        """
        from pytasky._runner import get_current_runner  # local import to break cycle

        active = runner or get_current_runner()
        if active is None:
            raise RuntimeError(
                f"Task {self.name!r} was submitted with no runner active. Wrap your "
                "submission code in `with TaskRunner() as runner:` (or pass "
                "`runner=` explicitly)."
            )
        return active.submit(self, *args, depends_on=tuple(depends_on), **kwargs)


class TaskHandle(Generic[R]):
    """Future-like reference to a submitted task instance.

    Awaitable from async code (``await handle`` returns the value or raises),
    blocking-pollable from sync code (``handle.result(timeout=...)``).
    Created exclusively by the runner — you don't instantiate this yourself.
    """

    __slots__ = (
        "_cancel_event",
        "_done_event",
        "_result",
        "_runner",
        "_status",
        "args",
        "depends_on",
        "id",
        "kwargs",
        "name",
        "task",
    )

    def __init__(
        self,
        *,
        runner: TaskRunner,
        task: Task[R],
        handle_id: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        depends_on: tuple[TaskHandle[Any], ...],
    ) -> None:
        self._runner = runner
        self.task = task
        self.id = handle_id
        self.name = task.name
        self.args = args
        self.kwargs = kwargs
        self.depends_on = depends_on
        self._status: TaskStatus = TaskStatus.PENDING
        self._result: TaskResult[R] | None = None
        self._done_event = threading.Event()
        self._cancel_event = threading.Event()

    def __repr__(self) -> str:
        return f"TaskHandle(id={self.id!r}, name={self.name!r}, status={self._status.value!r})"

    @property
    def status(self) -> TaskStatus:
        return self._status

    def done(self) -> bool:
        """True iff the task has reached a terminal status."""
        return self._status in _TERMINAL_STATUSES

    def cancel(self) -> bool:
        """Request cancellation.

        Cooperative for asyncio (raises ``CancelledError`` at the next
        suspension point); flag-based for threads (the body must check
        ``current_task().cancel_event``); best-effort terminate() for
        processes. Returns ``True`` if cancellation was newly requested,
        ``False`` if the task was already terminal or cancelling.
        """
        if self.done():
            return False
        if self._cancel_event.is_set():
            return False
        self._cancel_event.set()
        self._runner._notify_cancel(self)
        return True

    def wait(self, timeout: float | None = None) -> bool:
        """Block until the task is done. Returns ``True`` on completion, ``False`` on timeout."""
        return self._done_event.wait(timeout)

    def result(self, timeout: float | None = None) -> R:
        """Block until done, then return the value (or raise the failure)."""
        if not self._done_event.wait(timeout):
            raise TimeoutError(f"Timed out waiting for task {self.name!r} to finish")
        return self._unwrap_result()

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Block until done, then return the captured exception (or ``None`` on success)."""
        if not self._done_event.wait(timeout):
            raise TimeoutError(f"Timed out waiting for task {self.name!r} to finish")
        assert self._result is not None
        return self._result.error

    def get_result(self, timeout: float | None = None) -> TaskResult[R]:
        """Block until done, then return the full result record (does not raise on failure)."""
        if not self._done_event.wait(timeout):
            raise TimeoutError(f"Timed out waiting for task {self.name!r} to finish")
        assert self._result is not None
        return self._result

    def __await__(self) -> Any:
        return self._await_impl().__await__()

    async def _await_impl(self) -> R:
        loop = asyncio.get_running_loop()
        # Avoid blocking the loop — wait on the threading event in an executor.
        if not self._done_event.is_set():
            await loop.run_in_executor(None, self._done_event.wait)
        return self._unwrap_result()

    def _unwrap_result(self) -> R:
        assert self._result is not None
        res = self._result
        if res.status == TaskStatus.COMPLETED:
            return res.value  # type: ignore[return-value]
        if res.status == TaskStatus.CANCELLED:
            raise TaskCancelledError(f"Task {self.name!r} was cancelled")
        if res.error is not None:
            raise res.error
        raise TaskError(f"Task {self.name!r} ended in status {res.status.value!r} with no error")

    # ---------- runner-internal setters (called under the runner's lock) ----------

    def _set_status(self, status: TaskStatus) -> None:
        self._status = status

    def _complete(self, result: TaskResult[R]) -> None:
        self._result = result
        self._status = result.status
        self._done_event.set()


# ---------- decorator surface ----------


def _resolve_task_name(fn: Callable[..., Any], explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    module = getattr(fn, "__module__", "") or ""
    qualname = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or repr(fn)
    if qualname.endswith("<lambda>"):
        return f"{module}.{qualname}#{id(fn):x}" if module else f"{qualname}#{id(fn):x}"
    return f"{module}.{qualname}" if module else qualname


def _validate_retry_on(
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None,
) -> tuple[type[BaseException], ...]:
    if retry_on is None:
        return _DEFAULT_RETRY_ON
    if isinstance(retry_on, type) and issubclass(retry_on, BaseException):
        return (retry_on,)
    if isinstance(retry_on, tuple) and all(
        isinstance(c, type) and issubclass(c, BaseException) for c in retry_on
    ):
        return retry_on
    raise TypeError(
        "retry_on must be an exception class or tuple of exception classes; "
        f"got {retry_on!r}"
    )


def _build_task(
    fn: Callable[..., Any],
    *,
    name: str | None,
    backend: Backend,
    retries: int,
    timeout: float | None,
    backoff: Backoff,
    backoff_base: float,
    backoff_max: float,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None,
    on_dep_failure: DepFailurePolicy,
) -> Task[Any]:
    if backend not in ("asyncio", "thread", "process"):
        raise ValueError(
            f"backend must be 'asyncio', 'thread', or 'process'; got {backend!r}"
        )
    if iscoroutinefunction(fn) and backend != "asyncio":
        raise ValueError(
            f"Task {fn!r} is async but backend={backend!r} was requested. Async "
            "tasks can only run on the 'asyncio' backend — use a sync `def` for "
            "thread/process execution."
        )
    if retries < 0:
        raise ValueError(f"retries must be >= 0, got {retries}")
    if backoff not in ("none", "linear", "exponential"):
        raise ValueError(
            f"backoff must be 'none', 'linear', or 'exponential'; got {backoff!r}"
        )
    if backoff_base < 0:
        raise ValueError(f"backoff_base must be >= 0, got {backoff_base}")
    if backoff_max < 0:
        raise ValueError(f"backoff_max must be >= 0, got {backoff_max}")
    if timeout is not None and timeout <= 0:
        raise ValueError(f"timeout must be > 0 or None, got {timeout}")
    if on_dep_failure not in ("skip", "fail", "run-anyway"):
        raise ValueError(
            f"on_dep_failure must be 'skip', 'fail', or 'run-anyway'; got {on_dep_failure!r}"
        )
    return Task(
        fn=fn,
        name=_resolve_task_name(fn, name),
        backend=backend,
        retries=retries,
        timeout=timeout,
        backoff=backoff,
        backoff_base=backoff_base,
        backoff_max=backoff_max,
        retry_on=_validate_retry_on(retry_on),
        on_dep_failure=on_dep_failure,
    )


def task(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    backend: Backend = "asyncio",
    retries: int = 0,
    timeout: float | None = None,
    backoff: Backoff = "exponential",
    backoff_base: float = 1.0,
    backoff_max: float = 30.0,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None = None,
    on_dep_failure: DepFailurePolicy = "fail",
    max_attempts: int | None = None,
) -> Any:
    """Wrap a callable as a :class:`Task`.

    Two call styles, mirroring :func:`pyhooky.hook`::

        @task                       # bare — auto-named from module.qualname
        @task(name="checkout", retries=3, timeout=10.0, backend="thread")

    ``max_attempts`` is sugar for ``retries = max_attempts - 1`` — pick whichever
    framing reads better. They are mutually exclusive.
    """
    if max_attempts is not None:
        if retries:
            raise ValueError("Pass either retries= or max_attempts=, not both")
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        retries = max_attempts - 1

    if callable(fn) and not isinstance(fn, type):
        return _build_task(
            fn,
            name=name,
            backend=backend,
            retries=retries,
            timeout=timeout,
            backoff=backoff,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            retry_on=retry_on,
            on_dep_failure=on_dep_failure,
        )

    def decorator(f: Callable[..., Any]) -> Task[Any]:
        return _build_task(
            f,
            name=name,
            backend=backend,
            retries=retries,
            timeout=timeout,
            backoff=backoff,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            retry_on=retry_on,
            on_dep_failure=on_dep_failure,
        )

    return decorator


# ---------- internal helpers used by the runner ----------


def _set_current_task(ctx: TaskContext | None) -> Any:
    """Set the contextvar; returns a reset-token for the caller to restore later."""
    return _current_task.set(ctx)


def _reset_current_task(token: Any) -> None:
    _current_task.reset(token)


def _now() -> float:
    return time.monotonic()


def _wallclock() -> float:
    return time.time()


# Avoid "unused" complaints from field — used for forward-compat slot layout if needed.
_FIELD = field
