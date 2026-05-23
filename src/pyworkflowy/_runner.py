"""The :class:`TaskRunner` — orchestrates submission, scheduling, retries, and checkpointing.

Design choices:

- One runner owns one DAG. ``run()`` is idempotent within the runner's lifetime
  but tasks already executed are skipped on a re-run (lets checkpointing
  compose cleanly).
- The asyncio backend always runs the orchestration loop — even when the user
  picks ``thread``/``process`` as a default. Sync ``run()`` just wraps
  ``asyncio.run()`` around ``arun()``.
- Per-task backend overrides win over the runner default.
- Retries / timeouts / cancellation are managed at the runner level so the
  three backends only have to worry about a single attempt.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Literal

from pyworkflowy._backends import BackendExecutor, asyncio_execute, build_backend
from pyworkflowy._core import (
    Backend,
    Task,
    TaskContext,
    TaskHandle,
    TaskResult,
    TaskStatus,
    _reset_current_task,
    _set_current_task,
    _wallclock,
)
from pyworkflowy._dag import check_no_cycle
from pyworkflowy._persistence import (
    Checkpointer,
    JSONCheckpointer,
    PickleCheckpointer,
    ensure_jsonable,
)
from pyworkflowy.exceptions import (
    CycleError,
    DependencyFailedError,
    RetryExhaustedError,
    TaskCancelledError,
    TaskError,
    TaskTimeoutError,
)

__all__ = ["TaskRunner", "get_current_runner"]

OnTaskError = Literal["raise", "log", "continue"]

_logger = logging.getLogger("pyworkflowy")

_current_runner: ContextVar[TaskRunner | None] = ContextVar(
    "pyworkflowy_current_runner", default=None
)


def get_current_runner() -> TaskRunner | None:
    """Return the runner currently bound to this async task / thread, or ``None``."""
    return _current_runner.get()


@contextmanager
def _bind_runner(runner: TaskRunner) -> Iterator[None]:
    token = _current_runner.set(runner)
    try:
        yield
    finally:
        _current_runner.reset(token)


class TaskRunner:
    """Orchestrator for a DAG of submitted tasks.

    Construct one, ``.submit()`` tasks (each call returns a
    :class:`TaskHandle`), then ``.run()`` (sync) or ``await .arun()``
    (async) to execute them in DAG order. Use as a context manager to get
    automatic shutdown::

        with TaskRunner(max_workers=4) as runner:
            handle = my_task.submit(42)
            results = runner.run()

    Per-task backend choices override the runner default. Retries, timeouts,
    and cancellation are handled here so backends stay simple.
    """

    __slots__ = (
        "_backend_default",
        "_backends",
        "_backends_lock",
        "_bind_token",
        "_checkpoint_interval",
        "_checkpointer",
        "_handles",
        "_last_checkpoint",
        "_lock",
        "_loop",
        "_max_workers",
        "_on_task_error",
        "_resumed_results",
        "_shutdown",
        "_submission_order",
    )

    def __init__(
        self,
        *,
        max_workers: int = 8,
        backend: Backend = "asyncio",
        on_task_error: OnTaskError = "raise",
        checkpoint_path: str | None = None,
        checkpoint_interval: float = 5.0,
        checkpointer: Checkpointer | None = None,
    ) -> None:
        if max_workers < 1:
            raise ValueError(f"max_workers must be >= 1, got {max_workers}")
        if backend not in ("asyncio", "thread", "process"):
            raise ValueError(
                f"backend must be 'asyncio', 'thread', or 'process'; got {backend!r}"
            )
        if on_task_error not in ("raise", "log", "continue"):
            raise ValueError(
                f"on_task_error must be 'raise', 'log', or 'continue'; got {on_task_error!r}"
            )
        if checkpoint_interval < 0:
            raise ValueError(f"checkpoint_interval must be >= 0, got {checkpoint_interval}")
        self._max_workers = max_workers
        self._backend_default: Backend = backend
        self._on_task_error: OnTaskError = on_task_error
        self._lock = threading.RLock()
        self._backends_lock = threading.RLock()
        self._handles: dict[str, TaskHandle[Any]] = {}
        self._submission_order: list[str] = []
        self._backends: dict[str, BackendExecutor] = {}
        self._shutdown = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._resumed_results: dict[str, dict[str, Any]] = {}
        self._last_checkpoint = 0.0
        self._checkpoint_interval = checkpoint_interval
        if checkpointer is not None and checkpoint_path is not None:
            raise ValueError(
                "Pass either checkpointer= or checkpoint_path=, not both. The "
                "path is sugar for JSONCheckpointer(path)."
            )
        if checkpointer is not None:
            self._checkpointer: Checkpointer | None = checkpointer
        elif checkpoint_path is not None:
            self._checkpointer = JSONCheckpointer(checkpoint_path)
        else:
            self._checkpointer = None
        self._bind_token: Any = None

    # ---------- context manager ----------

    def __enter__(self) -> TaskRunner:
        # Bind ourselves as the ambient runner so `@task.submit()` calls inside
        # the `with` block find us. Reset on __exit__.
        self._bind_token = _current_runner.set(self)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._bind_token is not None:
            try:
                _current_runner.reset(self._bind_token)
            except ValueError:
                # Already reset by nested runner — fall back to clear.
                _current_runner.set(None)
            self._bind_token = None
        self.shutdown()

    # ---------- submission ----------

    def submit(
        self,
        target: Task[Any] | Callable[..., Any],
        *args: Any,
        depends_on: Iterable[TaskHandle[Any]] = (),
        **kwargs: Any,
    ) -> TaskHandle[Any]:
        """Submit a :class:`Task` (or a plain callable) for execution.

        Plain callables are wrapped with default config — equivalent to
        ``@task(fn)``. ``depends_on`` lists already-submitted handles whose
        completion gates this task's readiness. Raises
        :class:`pyworkflowy.CycleError` immediately if the new edge would close
        a cycle.
        """
        from pyworkflowy._core import _build_task  # local to avoid circular import

        if isinstance(target, Task):
            task_obj: Task[Any] = target
        elif callable(target):
            task_obj = _build_task(
                target,
                name=None,
                backend=self._backend_default,
                retries=0,
                timeout=None,
                backoff="exponential",
                backoff_base=1.0,
                backoff_max=30.0,
                retry_on=None,
                on_dep_failure="fail",
            )
        else:
            raise TypeError(
                f"submit() expected a Task or callable, got {type(target).__name__}"
            )

        depends_on_t = tuple(depends_on)
        # Validate dep handles belong to this runner.
        for dep in depends_on_t:
            if dep._runner is not self:
                raise ValueError(
                    f"Dependency {dep!r} belongs to a different runner. Cross-runner "
                    "dependencies are not supported — re-submit dependent tasks on "
                    "the same runner."
                )

        # If JSON checkpointing is on, eagerly verify args are serialisable so we
        # don't blow up mid-run. Skip for pickle, which is more permissive.
        if isinstance(self._checkpointer, JSONCheckpointer):
            ensure_jsonable(list(args), where=f"args for task {task_obj.name!r}")
            ensure_jsonable(dict(kwargs), where=f"kwargs for task {task_obj.name!r}")

        handle_id = self._next_handle_id(task_obj.name)
        handle: TaskHandle[Any] = TaskHandle(
            runner=self,
            task=task_obj,
            handle_id=handle_id,
            args=args,
            kwargs=kwargs,
            depends_on=depends_on_t,
        )

        with self._lock:
            if self._shutdown:
                raise RuntimeError("Cannot submit to a runner that has been shut down.")
            existing_deps = {
                hid: tuple(d.id for d in h.depends_on) for hid, h in self._handles.items()
            }
            try:
                check_no_cycle(
                    handle_id,
                    tuple(d.id for d in depends_on_t),
                    existing_deps,
                    name_lookup={hid: h.name for hid, h in self._handles.items()},
                )
            except CycleError:
                raise
            self._handles[handle_id] = handle
            self._submission_order.append(handle_id)
            # If we have a resumed result for this id, prime the handle with it.
            resumed = self._resumed_results.pop(handle_id, None)
            if resumed is not None:
                self._prime_from_resume(handle, resumed)
        return handle

    def _next_handle_id(self, name: str) -> str:
        return f"{name}#{uuid.uuid4().hex[:12]}"

    # ---------- running ----------

    def run(self) -> dict[str, TaskResult[Any]]:
        """Run all submitted tasks to completion, blocking. Returns a name→result map."""
        return asyncio.run(self.arun())

    async def arun(self) -> dict[str, TaskResult[Any]]:
        """Async variant of :meth:`run`. Use this when you already have an event loop."""
        with self._lock:
            if self._shutdown:
                raise RuntimeError("Cannot run a runner that has been shut down.")
            # Defensive cycle check across the whole graph (per-submit checks already
            # covered each insertion, but re-check in case state was prepped via resume).
            deps_map = {hid: tuple(d.id for d in h.depends_on) for hid, h in self._handles.items()}
            from pyworkflowy._dag import topo_order

            topo_order(deps_map)
            pending_ids = [
                hid
                for hid, h in self._handles.items()
                if not h.done()
            ]
        self._loop = asyncio.get_running_loop()
        with _bind_runner(self):
            await self._run_pending(pending_ids)
        return self.results()

    async def _run_pending(self, pending_ids: list[str]) -> None:
        # Map of id -> asyncio.Task once started, so cancel can interrupt them.
        running: dict[str, asyncio.Task[None]] = {}
        completed_event = asyncio.Event()
        completed_event.set()  # initial wakeup so the loop checks readiness once

        async def _runner_for(handle: TaskHandle[Any]) -> None:
            try:
                await self._execute_handle(handle)
            finally:
                completed_event.set()

        remaining = set(pending_ids)
        try:
            while remaining or running:
                # Start every newly-ready handle.
                progress = False
                for hid in list(remaining):
                    handle = self._handles[hid]
                    if hid in running:
                        continue
                    readiness = self._readiness(handle)
                    if readiness == "wait":
                        continue
                    if readiness == "skip":
                        self._mark_skipped(handle)
                        remaining.discard(hid)
                        progress = True
                        continue
                    if readiness == "fail":
                        self._mark_dep_failed(handle)
                        remaining.discard(hid)
                        progress = True
                        continue
                    # readiness == "go"
                    handle._set_status(TaskStatus.READY)
                    aiotask = asyncio.create_task(_runner_for(handle), name=f"pyworkflowy:{hid}")
                    running[hid] = aiotask
                    progress = True

                # Wait for something to complete.
                if not running and not progress:
                    raise TaskError(
                        "Runner deadlocked — no tasks are running and none can progress. "
                        "This usually means a dependency points to a handle that was never "
                        "submitted to this runner."
                    )
                if running:
                    completed_event.clear()
                    done, _ = await asyncio.wait(
                        running.values(),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    raise_later: BaseException | None = None
                    for d in done:
                        for hid, t in list(running.items()):
                            if t is d:
                                remaining.discard(hid)
                                del running[hid]
                                break
                        exc = d.exception()
                        if exc is not None and raise_later is None:
                            raise_later = exc
                    if raise_later is not None:
                        raise raise_later
        except BaseException:
            # Cancel anything still running before propagating.
            for t in running.values():
                t.cancel()
            if running:
                await asyncio.gather(*running.values(), return_exceptions=True)
            raise

    def _readiness(self, handle: TaskHandle[Any]) -> Literal["go", "wait", "skip", "fail"]:
        """Decide whether ``handle`` can start now."""
        if handle.done():
            return "skip"  # already resolved (e.g. resumed)
        failed: list[str] = []
        for dep in handle.depends_on:
            if not dep.done():
                return "wait"
            if dep.status != TaskStatus.COMPLETED:
                failed.append(dep.name)
        if not failed:
            return "go"
        policy = handle.task.on_dep_failure
        if policy == "run-anyway":
            return "go"
        if policy == "skip":
            return "skip"
        return "fail"

    def _mark_skipped(self, handle: TaskHandle[Any]) -> None:
        result: TaskResult[Any] = TaskResult(
            name=handle.name,
            status=TaskStatus.SKIPPED,
            attempts=0,
        )
        handle._complete(result)
        self._maybe_checkpoint()

    def _mark_dep_failed(self, handle: TaskHandle[Any]) -> None:
        failed_names = tuple(
            d.name for d in handle.depends_on if d.status != TaskStatus.COMPLETED
        )
        err = DependencyFailedError(
            f"Task {handle.name!r} has failed dependencies: {', '.join(failed_names)}",
            failed=failed_names,
        )
        result: TaskResult[Any] = TaskResult(
            name=handle.name,
            status=TaskStatus.FAILED,
            error=err,
            attempts=0,
        )
        handle._complete(result)
        if self._on_task_error == "raise":
            # Defer raising to the orchestrator gather; record on handle.
            pass
        elif self._on_task_error == "log":
            _logger.error("pyworkflowy: %s", err)
        self._maybe_checkpoint()

    async def _execute_handle(self, handle: TaskHandle[Any]) -> None:
        task_obj = handle.task
        attempts = 0
        last_exc: BaseException | None = None
        started_at = _wallclock()
        backend_name = task_obj.backend
        ctx = TaskContext(name=task_obj.name, attempt=1, cancel_event=handle._cancel_event)
        if handle._cancel_event.is_set():
            # Cancelled before we ever started — mark and return.
            result: TaskResult[Any] = TaskResult(
                name=task_obj.name,
                status=TaskStatus.CANCELLED,
                error=TaskCancelledError(f"Task {task_obj.name!r} was cancelled"),
                attempts=0,
                started_at=started_at,
                finished_at=_wallclock(),
            )
            handle._complete(result)
            self._maybe_checkpoint()
            return
        while attempts < task_obj.max_attempts:
            attempts += 1
            ctx.attempt = attempts
            if attempts == 1:
                handle._set_status(TaskStatus.RUNNING)
            else:
                handle._set_status(TaskStatus.RETRYING)
            try:
                value = await self._dispatch_one(handle, ctx, backend_name)
                finished = _wallclock()
                result: TaskResult[Any] = TaskResult(
                    name=task_obj.name,
                    status=TaskStatus.COMPLETED,
                    value=value,
                    attempts=attempts,
                    started_at=started_at,
                    finished_at=finished,
                )
                handle._complete(result)
                self._maybe_checkpoint()
                return
            except TaskCancelledError as exc:
                last_exc = exc
                result = TaskResult(
                    name=task_obj.name,
                    status=TaskStatus.CANCELLED,
                    error=exc,
                    attempts=attempts,
                    started_at=started_at,
                    finished_at=_wallclock(),
                )
                handle._complete(result)
                self._maybe_checkpoint()
                return
            except TaskTimeoutError as exc:
                # Timeouts terminate the task — no retry, surface the failure.
                last_exc = exc
                break
            except BaseException as exc:
                last_exc = exc
                if handle._cancel_event.is_set() and not isinstance(exc, TaskTimeoutError):
                    # Cancellation requested mid-attempt — surface as CANCELLED.
                    result = TaskResult(
                        name=task_obj.name,
                        status=TaskStatus.CANCELLED,
                        error=TaskCancelledError(f"Task {task_obj.name!r} was cancelled"),
                        attempts=attempts,
                        started_at=started_at,
                        finished_at=_wallclock(),
                    )
                    handle._complete(result)
                    self._maybe_checkpoint()
                    return
                if not _is_retryable(exc, task_obj.retry_on):
                    break
                if attempts >= task_obj.max_attempts:
                    break
                delay = _compute_backoff(task_obj, attempts)
                if delay > 0:
                    try:
                        await asyncio.sleep(delay)
                    except asyncio.CancelledError:
                        handle._cancel_event.set()
                        result = TaskResult(
                            name=task_obj.name,
                            status=TaskStatus.CANCELLED,
                            error=TaskCancelledError(
                                f"Task {task_obj.name!r} cancelled during backoff"
                            ),
                            attempts=attempts,
                            started_at=started_at,
                            finished_at=_wallclock(),
                        )
                        handle._complete(result)
                        self._maybe_checkpoint()
                        return

        # Retries exhausted (or non-retryable exception on first attempt).
        finished = _wallclock()
        if isinstance(last_exc, TaskTimeoutError):
            wrapped_err: BaseException = last_exc
        elif task_obj.retries > 0 and attempts > 1:
            wrapped_err = RetryExhaustedError(
                f"Task {task_obj.name!r} failed after {attempts} attempts: {last_exc!r}",
                attempts=attempts,
            )
            wrapped_err.__cause__ = last_exc
        else:
            wrapped_err = last_exc or TaskError(
                f"Task {task_obj.name!r} failed without an exception"
            )
        result = TaskResult(
            name=task_obj.name,
            status=TaskStatus.FAILED,
            error=wrapped_err,
            attempts=attempts,
            started_at=started_at,
            finished_at=finished,
        )
        handle._complete(result)
        self._maybe_checkpoint()
        if self._on_task_error == "raise":
            raise wrapped_err
        if self._on_task_error == "log":
            _logger.error(
                "pyworkflowy: task %r failed after %d attempt(s): %r",
                task_obj.name,
                attempts,
                wrapped_err,
            )

    async def _dispatch_one(
        self,
        handle: TaskHandle[Any],
        ctx: TaskContext,
        backend_name: Backend,
    ) -> Any:
        task_obj = handle.task
        args = handle.args
        kwargs = handle.kwargs
        timeout = task_obj.timeout
        if backend_name == "asyncio":
            return await asyncio_execute(
                task_obj.fn,
                args,
                kwargs,
                timeout=timeout,
                cancel_event=handle._cancel_event,
                task_name=task_obj.name,
                ctx=ctx,
                setup=lambda: _set_current_task(ctx),
                teardown=_reset_current_task,
            )
        # Thread / process — run on executor and await its future.
        if task_obj.is_async:
            raise TaskError(
                f"Task {task_obj.name!r} is async but backend={backend_name!r} was "
                "configured. Async tasks require the 'asyncio' backend."
            )
        executor = self._get_backend(backend_name)
        loop = asyncio.get_running_loop()
        # Process backend: the function ships across a pickle boundary, so we
        # cannot wrap it in a closure that sets contextvars (closures aren't
        # picklable). Documented: current_task() returns None inside process
        # workers. For threads, set the contextvar so introspection works.
        if backend_name == "thread":
            runnable: Callable[..., Any] = _thread_wrapped(task_obj.fn, ctx)
        else:
            runnable = task_obj.fn

        future: asyncio.Future[Any] = loop.run_in_executor(
            None,
            lambda: executor.execute(
                runnable,
                args,
                kwargs,
                timeout=timeout,
                cancel_event=handle._cancel_event,
                task_name=task_obj.name,
            ),
        )
        return await future

    def _get_backend(self, name: Backend) -> BackendExecutor:
        with self._backends_lock:
            existing = self._backends.get(name)
            if existing is not None:
                return existing
            backend = build_backend(name, self._max_workers)
            self._backends[name] = backend
            return backend

    # ---------- introspection ----------

    def handles(self) -> list[TaskHandle[Any]]:
        with self._lock:
            return list(self._handles.values())

    def results(self) -> dict[str, TaskResult[Any]]:
        with self._lock:
            return {h.name: h._result for h in self._handles.values() if h._result is not None}

    # ---------- cancellation ----------

    def cancel_all(self) -> int:
        cancelled = 0
        with self._lock:
            for h in self._handles.values():
                if not h.done() and not h._cancel_event.is_set():
                    h._cancel_event.set()
                    cancelled += 1
        return cancelled

    def _notify_cancel(self, handle: TaskHandle[Any]) -> None:
        # Currently a no-op — cancellation propagates via the cancel_event flag
        # checked by backends. Hook here in case future backends need a poke.
        return

    # ---------- shutdown ----------

    def shutdown(self, *, wait: bool = True) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
        with self._backends_lock:
            for backend in self._backends.values():
                backend.shutdown(wait=wait)
            self._backends.clear()

    # ---------- checkpointing ----------

    def _maybe_checkpoint(self) -> None:
        if self._checkpointer is None:
            return
        now = time.monotonic()
        if self._checkpoint_interval and (now - self._last_checkpoint) < self._checkpoint_interval:
            return
        self._last_checkpoint = now
        try:
            self._checkpointer.save(self._dump_state())
        except Exception as exc:
            _logger.error("pyworkflowy: checkpoint failed: %r", exc)

    def _dump_state(self) -> dict[str, Any]:
        with self._lock:
            handles_state: list[dict[str, Any]] = []
            for hid in self._submission_order:
                h = self._handles[hid]
                entry: dict[str, Any] = {
                    "id": h.id,
                    "name": h.name,
                    "args": list(h.args),
                    "kwargs": dict(h.kwargs),
                    "depends_on": [d.id for d in h.depends_on],
                    "status": h.status.value,
                }
                if h._result is not None:
                    entry["value"] = h._result.value if h._result.ok else None
                    entry["error"] = repr(h._result.error) if h._result.error is not None else None
                    entry["attempts"] = h._result.attempts
                    entry["started_at"] = h._result.started_at
                    entry["finished_at"] = h._result.finished_at
                handles_state.append(entry)
            return {"version": 1, "handles": handles_state}

    @classmethod
    def resume(
        cls,
        path: str,
        *,
        max_workers: int = 8,
        backend: Backend = "asyncio",
        on_task_error: OnTaskError = "raise",
        checkpoint_interval: float = 5.0,
        checkpointer: Checkpointer | None = None,
    ) -> TaskRunner:
        """Build a new runner pre-loaded with state from ``path``.

        Completed tasks from the previous run won't re-execute when their
        handles are re-submitted (the runner primes them with the persisted
        result on submit). Non-terminal tasks are erased — caller re-submits
        them fresh.
        """
        loader: Checkpointer
        if checkpointer is not None:
            loader = checkpointer
        elif path.endswith(".pkl") or path.endswith(".pickle"):
            loader = PickleCheckpointer(path)
        else:
            loader = JSONCheckpointer(path)
        state = loader.load()
        runner = cls(
            max_workers=max_workers,
            backend=backend,
            on_task_error=on_task_error,
            checkpoint_interval=checkpoint_interval,
            checkpointer=loader,
        )
        if state is None:
            return runner
        for entry in state.get("handles", []):
            if entry.get("status") in (TaskStatus.COMPLETED.value, TaskStatus.SKIPPED.value):
                runner._resumed_results[entry["id"]] = entry
        return runner

    def _prime_from_resume(self, handle: TaskHandle[Any], entry: dict[str, Any]) -> None:
        status_str = entry.get("status", TaskStatus.COMPLETED.value)
        status = TaskStatus(status_str)
        result: TaskResult[Any] = TaskResult(
            name=handle.name,
            status=status,
            value=entry.get("value"),
            attempts=entry.get("attempts", 1),
            started_at=entry.get("started_at"),
            finished_at=entry.get("finished_at"),
        )
        handle._complete(result)


# ---------- helpers ----------


def _is_retryable(exc: BaseException, retry_on: tuple[type[BaseException], ...]) -> bool:
    if isinstance(exc, TaskTimeoutError):
        return False
    if isinstance(exc, TaskCancelledError):
        return False
    return isinstance(exc, retry_on)


def _compute_backoff(task: Task[Any], attempt: int) -> float:
    """Return the sleep delay after ``attempt``-th failed attempt (1-indexed)."""
    if task.backoff == "none":
        return 0.0
    if task.backoff == "linear":
        return min(task.backoff_base * attempt, task.backoff_max)
    # exponential
    return min(task.backoff_base * (2 ** (attempt - 1)), task.backoff_max)


def _thread_wrapped(fn: Callable[..., Any], ctx: TaskContext) -> Callable[..., Any]:
    """Wrap ``fn`` so it sets the ``current_task`` contextvar on entry.

    For the *thread* backend; the process backend can't share contextvars
    across the boundary, so ``current_task()`` returns ``None`` inside
    process workers (documented behaviour).
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        token = _set_current_task(ctx)
        try:
            return fn(*args, **kwargs)
        finally:
            _reset_current_task(token)

    return wrapper


# Silence unused-import linters for re-exported names below.
_ = Awaitable
