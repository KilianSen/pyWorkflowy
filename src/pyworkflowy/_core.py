"""Core task primitives: ``Task``, ``TaskHandle``, ``TaskResult``, ``TaskStatus``.

These pieces are deliberately decoupled from execution — they describe *what*
a task is and *what a submitted task looks like*, leaving the actual scheduling
to :mod:`pyworkflowy._runner` and the pool executors in
:mod:`pyworkflowy._backends`.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import StrEnum
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, cast

from pyworkflowy._backends import OffloadPool
from pyworkflowy.exceptions import TaskCancelledError, TaskError

if TYPE_CHECKING:
    from pyworkflowy._runner import TaskRunner

__all__ = [
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

Backoff = Literal["none", "linear", "exponential"]
DepFailurePolicy = Literal["skip", "fail", "run-anyway"]

_DEFAULT_RETRY_ON: tuple[type[BaseException], ...] = (Exception,)
DEFAULT_POOL_NAME = "default"

# Poll interval (seconds) for _wait_for_cancel — see its docstring for the
# tradeoff (responsiveness vs. zombie threads on the happy path).
_OFFLOAD_CANCEL_POLL_INTERVAL = 0.05


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

    Most fields are read-only from user code — they're owned by the runner.
    The exception is progress: task bodies call :meth:`update_progress` to
    report how far they've gotten, which the runner exposes via
    :attr:`TaskHandle.progress` (and persists, throttled, through the
    configured checkpointer).
    """

    name: str
    attempt: int
    cancel_event: threading.Event
    _handle: TaskHandle[Any] | None = None
    _runner: TaskRunner | None = None

    @property
    def id(self) -> str:
        """Unique identifier of the submitted handle backing this context.

        Raises ``RuntimeError`` if called outside a runner (i.e. ``_handle``
        was never set).
        """
        if self._handle is None:
            raise RuntimeError("TaskContext.id is only available inside a running task body.")
        return self._handle.id

    def update_progress(self, fraction: float, message: str | None = None) -> None:
        """Report progress on the currently running task.

        ``fraction`` is clamped to ``[0.0, 1.0]``. The runner persists
        progress at most ~1 Hz (skipping deltas < 2 % outside terminal
        transitions) — call this as often as you like; the throttle lives
        in the runner.
        """
        if self._handle is None:
            return
        clamped = max(0.0, min(1.0, float(fraction)))
        self._handle._update_progress(clamped, message)

    async def offload(
        self,
        fn: Callable[..., R],
        *args: Any,
        pool: str = "offload",
        **kwargs: Any,
    ) -> R:
        """Run ``fn(*args, **kwargs)`` on the runner's offload thread pool.

        The await wakes with :class:`TaskCancelledError` if this task's cancel
        event fires before the call returns. The thread itself keeps running
        until ``fn`` completes naturally (Python threads cannot be killed);
        its result is discarded.
        """
        if self._runner is None:
            raise RuntimeError("TaskContext.offload is only valid inside a running task body.")
        pool_obj = self._runner._pools.get(pool)
        if pool_obj is None or pool_obj.kind != "offload":
            raise ValueError(
                f"No offload-kind pool named {pool!r} on this runner. "
                f"Configure one via TaskRunner(pools={{'offload': Pool(name='offload', "
                f"kind='offload', max_workers=N), ...}})."
            )
        executor = cast(OffloadPool, self._runner._get_pool_executor(pool_obj))
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(
            executor.thread_executor(),
            functools.partial(fn, *args, **kwargs),
        )
        cancel_wait = asyncio.create_task(_wait_for_cancel(self.cancel_event))
        try:
            done, _ = await asyncio.wait((future, cancel_wait), return_when=asyncio.FIRST_COMPLETED)
            if future in done:
                return future.result()
            raise TaskCancelledError(f"Task {self.name!r} was cancelled during offload")
        finally:
            cancel_wait.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancel_wait


async def _wait_for_cancel(event: threading.Event) -> None:
    """Wait for a :class:`threading.Event` to fire from inside an asyncio coroutine.

    Used by :meth:`TaskContext.offload` to race a cancellation flag against a
    futures-backed offload call. We poll instead of dispatching ``event.wait``
    onto an executor: the executor approach leaks a blocked thread per
    successful offload (the asyncio.Task can be cancelled, but the worker
    thread stuck in :meth:`threading.Event.wait` cannot). Polling at ~50 ms is
    quick enough to react to cancellation promptly without leaving zombie
    threads behind on the happy path.
    """
    while not event.is_set():
        await asyncio.sleep(_OFFLOAD_CANCEL_POLL_INTERVAL)


_current_task: ContextVar[TaskContext | None] = ContextVar("pyworkflowy_current_task", default=None)


def current_task() -> TaskContext | None:
    """Return the currently running task's :class:`TaskContext`, or ``None``.

    Useful for shared utilities that want to log the task name without
    threading it through every call. Implemented via
    :class:`contextvars.ContextVar` so it works across asyncio tasks; the
    thread and process pools populate it manually at entry.
    """
    return _current_task.get()


@dataclass(frozen=True, slots=True)
class Task(Generic[R]):
    """A reusable, configured callable wrapped for the runner.

    Built by the :func:`task` decorator or by subclassing :class:`TaskBase`.
    All execution-shaping knobs live here; ``.submit(...)`` returns a
    :class:`TaskHandle` bound to the currently active runner.

    The ``pool`` field names the runner pool the task wants to run on. The
    runner validates the pool exists (and that its kind matches the task — for
    instance, async functions need a pool of kind ``asyncio``) at submit time,
    not at decoration time.
    """

    fn: Callable[..., Any]
    name: str
    pool: str = DEFAULT_POOL_NAME
    retries: int = 0
    timeout: float | None = None
    backoff: Backoff = "exponential"
    backoff_base: float = 1.0
    backoff_max: float = 30.0
    retry_on: tuple[type[BaseException], ...] = _DEFAULT_RETRY_ON
    on_dep_failure: DepFailurePolicy = "fail"
    dedup_by: tuple[str, ...] = ()
    triggers: tuple[str, ...] = ()
    payload_map: tuple[tuple[str, str], ...] = ()

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
        *,
        payload: Mapping[str, Any] | None = None,
        args: tuple[Any, ...] = (),
        depends_on: Iterable[TaskHandle[Any]] = (),
        runner: TaskRunner | None = None,
        source: str = "manual",
        dedup_key: str | None = None,
    ) -> TaskHandle[R]:
        """Submit this task to ``runner`` (or the ambient one) and return its handle.

        ``payload`` is a mapping of keyword arguments to pass to the task body;
        ``args`` is a tuple of positional arguments. (Where you would once have
        written ``task.submit(1, 2, foo="bar")``, write
        ``task.submit(args=(1, 2), payload={"foo": "bar"})`` — this avoids
        kwarg-name collisions with ``source``/``depends_on``/``dedup_key``.)

        ``depends_on`` lists the handles that must finish before this task
        becomes ``READY``. The ``on_dep_failure`` policy on this :class:`Task`
        decides what happens if any of them ended in a non-``COMPLETED`` state.

        ``source`` is a free-form tag describing where the submission came from
        (``"manual"``, ``"cron"``, ``"event"``, ``"background"``, ...). Pools
        with a ``reserve_for`` rule use it to decide whether this task may
        claim a reserved slot.

        ``dedup_key`` (or this task's ``dedup_by`` config, which auto-computes
        a key from the payload) suppresses duplicate pending submissions: if a
        non-terminal handle already exists with the same ``(task.name, key)``
        pair, that handle is returned instead of creating a new one.
        """
        from pyworkflowy._runner import get_current_runner  # local import to break cycle

        active = runner or get_current_runner()
        if active is None:
            raise RuntimeError(
                f"Task {self.name!r} was submitted with no runner active. Wrap your "
                "submission code in `with TaskRunner() as runner:` (or pass "
                "`runner=` explicitly)."
            )
        return active.submit(
            self,
            payload=payload,
            args=args,
            depends_on=tuple(depends_on),
            source=source,
            dedup_key=dedup_key,
        )


class TaskHandle(Generic[R]):
    """Future-like reference to a submitted task instance.

    Awaitable from async code (``await handle`` returns the value or raises),
    blocking-pollable from sync code (``handle.result(timeout=...)``).
    Created exclusively by the runner — you don't instantiate this yourself.
    """

    __slots__ = (
        "_aiotask",
        "_cancel_event",
        "_dedup_key",
        "_done_event",
        "_last_progress_write",
        "_progress",
        "_progress_message",
        "_result",
        "_retry_at",
        "_runner",
        "_status",
        "args",
        "depends_on",
        "id",
        "kwargs",
        "name",
        "source",
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
        source: str = "manual",
        dedup_key: str | None = None,
    ) -> None:
        self._runner = runner
        self.task = task
        self.id = handle_id
        self.name = task.name
        self.args = args
        self.kwargs = kwargs
        self.depends_on = depends_on
        self.source = source
        self._dedup_key = dedup_key
        self._status: TaskStatus = TaskStatus.PENDING
        self._result: TaskResult[R] | None = None
        self._done_event = threading.Event()
        self._cancel_event = threading.Event()
        # The asyncio.Task wrapping the runner's per-handle coroutine. Set when
        # the runner schedules the handle; consumed by cross-thread cancel.
        self._aiotask: asyncio.Task[None] | None = None
        # Progress reporting state — written by TaskContext.update_progress.
        self._progress: float = 0.0
        self._progress_message: str | None = None
        self._last_progress_write: float = 0.0
        # When non-None, the task is sleeping until this wall-clock timestamp
        # before its next retry attempt. Set by the runner's retry loop;
        # cleared when the next attempt begins or the task reaches terminal.
        self._retry_at: float | None = None

    def __repr__(self) -> str:
        return f"TaskHandle(id={self.id!r}, name={self.name!r}, status={self._status.value!r})"

    @property
    def status(self) -> TaskStatus:
        return self._status

    @property
    def progress(self) -> float:
        """Fraction of work completed in ``[0.0, 1.0]``.

        Updated by the task body via :meth:`TaskContext.update_progress`; the
        runner reads this when checkpointing and surfaces it to any UI built
        on top of the handle.
        """
        return self._progress

    @property
    def progress_message(self) -> str | None:
        """Optional free-form status message accompanying :attr:`progress`."""
        return self._progress_message

    @property
    def retry_at(self) -> float | None:
        """Wall-clock timestamp at which the next retry attempt will begin.

        ``None`` outside the backoff window between attempts. Useful for
        rendering "retrying in 2m" in a UI without poking runner internals.
        """
        return self._retry_at

    @property
    def dedup_key(self) -> str | None:
        """The deduplication key this handle was submitted under, if any."""
        return self._dedup_key

    def done(self) -> bool:
        """True iff the task has reached a terminal status."""
        return self._status in _TERMINAL_STATUSES

    def cancel(self) -> bool:
        """Request cancellation.

        For asyncio tasks this is a *hard* cancel: the runner schedules
        ``asyncio.Task.cancel()`` via ``loop.call_soon_threadsafe`` so the
        await wakes with ``CancelledError`` immediately, regardless of which
        thread invoked :meth:`cancel`. For thread / process pools cancellation
        is cooperative — the body must check ``current_task().cancel_event``.
        Returns ``True`` if cancellation was newly requested, ``False`` if the
        task was already terminal or cancelling.

        Safe to call from any thread. See
        ``tests/test_cancel.py::test_cancel_from_non_loop_thread_interrupts_async_task``
        for the cross-thread guarantee.
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
        self._retry_at = None
        # Snap progress to 1.0 on successful completion; leave as-is otherwise.
        if result.status == TaskStatus.COMPLETED:
            self._progress = 1.0
        self._done_event.set()
        self._runner._on_handle_terminal(self)

    def _update_progress(self, fraction: float, message: str | None) -> None:
        self._progress = fraction
        if message is not None:
            self._progress_message = message
        self._runner._maybe_persist_progress(self)


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
        f"retry_on must be an exception class or tuple of exception classes; got {retry_on!r}"
    )


def _build_task(
    fn: Callable[..., Any],
    *,
    name: str | None,
    pool: str,
    retries: int,
    timeout: float | None,
    backoff: Backoff,
    backoff_base: float,
    backoff_max: float,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None,
    on_dep_failure: DepFailurePolicy,
    dedup_by: tuple[str, ...] = (),
    triggers: tuple[str, ...] = (),
    payload_map: dict[str, str] | tuple[tuple[str, str], ...] | None = None,
) -> Task[Any]:
    if not isinstance(pool, str) or not pool:
        raise ValueError(f"pool must be a non-empty string; got {pool!r}")
    if retries < 0:
        raise ValueError(f"retries must be >= 0, got {retries}")
    if backoff not in ("none", "linear", "exponential"):
        raise ValueError(f"backoff must be 'none', 'linear', or 'exponential'; got {backoff!r}")
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
    if not isinstance(dedup_by, tuple) or not all(isinstance(k, str) for k in dedup_by):
        raise TypeError(f"dedup_by must be a tuple of kwarg-name strings; got {dedup_by!r}")
    triggers_t: tuple[str, ...] = tuple(triggers)
    if not all(isinstance(t, str) for t in triggers_t):
        raise TypeError(f"triggers must be strings; got {triggers!r}")
    if payload_map is None:
        payload_map_t: tuple[tuple[str, str], ...] = ()
    elif isinstance(payload_map, dict):
        payload_map_t = tuple(sorted(payload_map.items()))
    else:
        payload_map_t = tuple(payload_map)
    return Task(
        fn=fn,
        name=_resolve_task_name(fn, name),
        pool=pool,
        retries=retries,
        timeout=timeout,
        backoff=backoff,
        backoff_base=backoff_base,
        backoff_max=backoff_max,
        retry_on=_validate_retry_on(retry_on),
        on_dep_failure=on_dep_failure,
        dedup_by=dedup_by,
        triggers=triggers_t,
        payload_map=payload_map_t,
    )


def task(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    pool: str = DEFAULT_POOL_NAME,
    retries: int = 0,
    timeout: float | None = None,
    backoff: Backoff = "exponential",
    backoff_base: float = 1.0,
    backoff_max: float = 30.0,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None = None,
    on_dep_failure: DepFailurePolicy = "fail",
    max_attempts: int | None = None,
    dedup_by: tuple[str, ...] = (),
    triggers: tuple[str, ...] = (),
    payload_map: dict[str, str] | None = None,
) -> Any:
    """Wrap a callable as a :class:`Task`.

    Two call styles::

        @task                       # bare — auto-named from module.qualname
        @task(name="checkout", retries=3, timeout=10.0, pool="io")

    ``pool`` names a runner pool. Default runners expose four named pools
    out of the box — ``"default"`` (asyncio), ``"thread"``, ``"process"``, and
    ``"offload"`` (call-only thread pool for ``ctx.offload()``) — sized via
    ``TaskRunner(max_workers=...)``. Configure your own with
    ``TaskRunner(pools={"io": Pool(...), ...})``.

    ``max_attempts`` is sugar for ``retries = max_attempts - 1`` — pick
    whichever framing reads better. They are mutually exclusive.
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
            pool=pool,
            retries=retries,
            timeout=timeout,
            backoff=backoff,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            retry_on=retry_on,
            on_dep_failure=on_dep_failure,
            dedup_by=dedup_by,
            triggers=triggers,
            payload_map=payload_map,
        )

    def decorator(f: Callable[..., Any]) -> Task[Any]:
        return _build_task(
            f,
            name=name,
            pool=pool,
            retries=retries,
            timeout=timeout,
            backoff=backoff,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            retry_on=retry_on,
            on_dep_failure=on_dep_failure,
            dedup_by=dedup_by,
            triggers=triggers,
            payload_map=payload_map,
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
