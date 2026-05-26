"""The :class:`TaskRunner` — orchestrates submission, scheduling, retries, and checkpointing.

Design choices:

- One runner owns one DAG. ``run()`` is idempotent within the runner's lifetime
  but tasks already executed are skipped on a re-run (lets checkpointing
  compose cleanly).
- The runner's orchestration loop always runs on asyncio — even when tasks
  themselves picked a thread or process pool. Sync ``run()`` just wraps
  ``asyncio.run()`` around ``arun()``.
- Tasks declare a named ``pool``; each pool has its own kind, max_workers, and
  optional reservation rule. Pools are owned by the runner.
- Retries / timeouts / cancellation are managed at the runner level so the
  pool executors only have to worry about a single attempt.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from typing import Any, Literal

from pyworkflowy._backends import (
    Pool,
    PoolExecutor,
    asyncio_execute,
    build_pool_executor,
)
from pyworkflowy._core import (
    DEFAULT_POOL_NAME,
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
    CHECKPOINT_VERSION,
    Checkpointer,
    JSONCheckpointer,
    PickleCheckpointer,
    SnapshotCheckpointer,
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


def _default_pools(max_workers: int) -> dict[str, Pool]:
    """Standard pool set: one asyncio pool plus thread and process pools.

    Every kind gets a named pool so existing ``@task(pool="thread")`` and
    ``pool="process"`` calls work without explicit ``pools=`` configuration on
    the runner.
    """
    return {
        DEFAULT_POOL_NAME: Pool(name=DEFAULT_POOL_NAME, kind="asyncio", max_workers=max_workers),
        "thread": Pool(name="thread", kind="thread", max_workers=max_workers),
        "process": Pool(name="process", kind="process", max_workers=max_workers),
    }


class TaskRunner:
    """Orchestrator for a DAG of submitted tasks.

    Construct one, ``.submit()`` tasks (each call returns a
    :class:`TaskHandle`), then ``.run()`` (sync) or ``await .arun()``
    (async) to execute them in DAG order. Use as a context manager to get
    automatic shutdown::

        with TaskRunner(max_workers=4) as runner:
            handle = my_task.submit(42)
            results = runner.run()

    Tasks pick a named pool via ``@task(pool="...")``. By default the runner
    provides three named pools — ``"default"`` (asyncio), ``"thread"``, and
    ``"process"`` — each sized to ``max_workers``. Pass ``pools={...}`` to
    define your own (typical: ``"io"`` for thread-bound work, ``"cpu"`` for
    process-bound work, with reservation rules to keep manual work
    responsive). Retries, timeouts, and cancellation are handled here so pool
    executors stay simple.
    """

    __slots__ = (
        "_aiotasks",
        "_backends_lock",
        "_bind_token",
        "_checkpoint_interval",
        "_checkpointer",
        "_dedup_index",
        "_handles",
        "_last_checkpoint",
        "_lock",
        "_loop",
        "_max_workers",
        "_new_work_event",
        "_on_task_error",
        "_orphaned",
        "_pool_executors",
        "_pool_running",
        "_pool_running_unprivileged",
        "_pools",
        "_progress_throttle_delta",
        "_progress_throttle_seconds",
        "_resumed_results",
        "_shutdown",
        "_stop_event",
        "_submission_order",
    )

    def __init__(
        self,
        *,
        max_workers: int = 8,
        pools: Mapping[str, Pool] | None = None,
        on_task_error: OnTaskError = "raise",
        checkpoint_path: str | None = None,
        checkpoint_interval: float = 5.0,
        checkpointer: Checkpointer | None = None,
    ) -> None:
        if max_workers < 1:
            raise ValueError(f"max_workers must be >= 1, got {max_workers}")
        if on_task_error not in ("raise", "log", "continue"):
            raise ValueError(
                f"on_task_error must be 'raise', 'log', or 'continue'; got {on_task_error!r}"
            )
        if checkpoint_interval < 0:
            raise ValueError(f"checkpoint_interval must be >= 0, got {checkpoint_interval}")
        self._max_workers = max_workers
        if pools is None:
            self._pools: dict[str, Pool] = _default_pools(max_workers)
        else:
            if not pools:
                raise ValueError("pools= must contain at least one Pool entry")
            self._pools = dict(pools)
            for name, pool in self._pools.items():
                if pool.name != name:
                    raise ValueError(
                        f"Pool dict key {name!r} does not match Pool.name {pool.name!r}"
                    )
        self._on_task_error: OnTaskError = on_task_error
        self._lock = threading.RLock()
        self._backends_lock = threading.RLock()
        self._handles: dict[str, TaskHandle[Any]] = {}
        self._submission_order: list[str] = []
        self._pool_executors: dict[str, PoolExecutor] = {}
        self._pool_running: dict[str, int] = {name: 0 for name in self._pools}
        self._pool_running_unprivileged: dict[str, int] = {name: 0 for name in self._pools}
        self._aiotasks: dict[str, asyncio.Task[None]] = {}
        # (task_name, dedup_key) -> handle. Only holds non-terminal handles.
        self._dedup_index: dict[tuple[str, str], TaskHandle[Any]] = {}
        # Progress persistence throttle: skip a write if delta < threshold and
        # last write was within the cooldown window. Mirrors the consumer's
        # task_queue.py rule (delta < 2%, last write < 1s ago).
        self._progress_throttle_seconds = 1.0
        self._progress_throttle_delta = 0.02
        self._shutdown = False
        self._loop: asyncio.AbstractEventLoop | None = None
        # Lazily created on the runner's loop at arun/aserve startup. They
        # cannot be eagerly constructed here because asyncio.Event binds to
        # the running loop at instantiation time.
        self._new_work_event: asyncio.Event | None = None
        self._stop_event: asyncio.Event | None = None
        self._resumed_results: dict[str, dict[str, Any]] = {}
        # Entries from a prior checkpoint that were not terminal (status RUNNING,
        # RETRYING, PENDING, READY) — orphans the caller may want to re-submit.
        self._orphaned: list[dict[str, Any]] = []
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

    # ---------- pools ----------

    @property
    def pools(self) -> Mapping[str, Pool]:
        """The configured pools, keyed by name."""
        return self._pools

    @property
    def orphaned(self) -> list[dict[str, Any]]:
        """Handle entries from a prior checkpoint that did not reach a terminal status.

        Populated by :meth:`resume`. Each entry contains the persisted args,
        kwargs, pool, source, and last-seen status; iterate this list and
        re-submit the relevant tasks if you want continuity across restarts.
        """
        return list(self._orphaned)

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
        source: str = "manual",
        dedup_key: str | None = None,
        **kwargs: Any,
    ) -> TaskHandle[Any]:
        """Submit a :class:`Task` (or a plain callable) for execution.

        Plain callables are wrapped with default config — equivalent to
        ``@task(fn)``. ``depends_on`` lists already-submitted handles whose
        completion gates this task's readiness. ``source`` tags the
        submission for pool reservation purposes (see :class:`Pool`). Raises
        :class:`pyworkflowy.CycleError` immediately if the new edge would
        close a cycle.
        """
        from pyworkflowy._core import _build_task  # local to avoid circular import

        if isinstance(target, Task):
            task_obj: Task[Any] = target
        elif callable(target):
            task_obj = _build_task(
                target,
                name=None,
                pool=DEFAULT_POOL_NAME,
                retries=0,
                timeout=None,
                backoff="exponential",
                backoff_base=1.0,
                backoff_max=30.0,
                retry_on=None,
                on_dep_failure="fail",
            )
        else:
            raise TypeError(f"submit() expected a Task or callable, got {type(target).__name__}")

        # Validate the pool exists and is kind-compatible with the task.
        pool = self._pools.get(task_obj.pool)
        if pool is None:
            raise ValueError(
                f"Task {task_obj.name!r} declared pool={task_obj.pool!r}, but the runner "
                f"has no pool by that name. Configured pools: {sorted(self._pools)!r}."
            )
        if task_obj.is_async and pool.kind != "asyncio":
            raise ValueError(
                f"Task {task_obj.name!r} is async but pool {pool.name!r} is of kind "
                f"{pool.kind!r}. Async tasks require a pool of kind 'asyncio'."
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

        # Deduplication: compute an effective key from the explicit `dedup_key`
        # or from the task's `dedup_by` kwargs. If a non-terminal handle with
        # the same (name, key) is already registered, return it unchanged.
        effective_key = dedup_key
        if effective_key is None and task_obj.dedup_by:
            try:
                key_parts = [repr(kwargs[k]) for k in task_obj.dedup_by]
            except KeyError as exc:
                raise ValueError(
                    f"Task {task_obj.name!r} has dedup_by={task_obj.dedup_by!r} but "
                    f"kwarg {exc.args[0]!r} was not provided at submit time."
                ) from exc
            effective_key = "|".join(key_parts)
        if effective_key is not None:
            with self._lock:
                existing = self._dedup_index.get((task_obj.name, effective_key))
                if existing is not None and not existing.done():
                    return existing

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
            source=source,
            dedup_key=effective_key,
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
            if effective_key is not None:
                self._dedup_index[(task_obj.name, effective_key)] = handle
            # If we have a resumed result for this id, prime the handle with it.
            resumed = self._resumed_results.pop(handle_id, None)
            if resumed is not None:
                self._prime_from_resume(handle, resumed)
        # Persist the initial PENDING row immediately so any inspection of the
        # checkpointer right after submit() sees the handle. Skip when resumed
        # is not None — the handle was primed from a prior checkpoint and is
        # already terminal; the existing checkpoint row is authoritative.
        if self._checkpointer is not None and resumed is None:
            try:
                self._checkpointer.save_initial(self._handle_to_entry(handle))
            except Exception as exc:
                _logger.error("pyworkflowy: save_initial failed: %r", exc)
            self._last_checkpoint = time.monotonic()
        # Wake an idle serve loop so it picks up the new handle. Safe to call
        # cross-thread because we schedule the set onto the runner's own loop.
        self._wake_loop()
        return handle

    def _wake_loop(self) -> None:
        """Signal the dispatch loop that new work was submitted.

        No-op if no loop is bound (e.g. before :meth:`arun`/:meth:`aserve`
        starts) or the loop is already shutting down. Thread-safe — uses
        ``call_soon_threadsafe`` so this can fire from a FastAPI handler.
        """
        loop = self._loop
        event = self._new_work_event
        if loop is None or event is None:
            return
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(event.set)

    def _next_handle_id(self, name: str) -> str:
        return f"{name}#{uuid.uuid4().hex[:12]}"

    # ---------- running ----------

    def run(self) -> dict[str, TaskResult[Any]]:
        """Run all submitted tasks to completion, blocking. Returns a name→result map."""
        return asyncio.run(self.arun())

    async def arun(self) -> dict[str, TaskResult[Any]]:
        """Async variant of :meth:`run`. One-shot — returns when all currently
        submitted tasks reach a terminal status. Use :meth:`aserve` for a
        long-running daemon that picks up new submissions while running.
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError("Cannot run a runner that has been shut down.")
            # Defensive cycle check across the whole graph (per-submit checks already
            # covered each insertion, but re-check in case state was prepped via resume).
            deps_map = {hid: tuple(d.id for d in h.depends_on) for hid, h in self._handles.items()}
            from pyworkflowy._dag import topo_order

            topo_order(deps_map)
            pending_ids = [hid for hid, h in self._handles.items() if not h.done()]
        self._bind_loop_events()
        with _bind_runner(self):
            await self._run_pending(pending_ids, serve_mode=False)
        return self.results()

    async def aserve(self) -> None:
        """Run the dispatch loop continuously until :meth:`stop` is called.

        Unlike :meth:`arun`, this does *not* exit when the in-flight queue
        drains — it awaits new submissions (from any thread, via
        :meth:`submit` or :class:`Scheduler` / :class:`EventSource`) and
        keeps dispatching. Individual task failures are logged and consumed
        by the loop (use :attr:`on_task_error` to control per-task behaviour
        and ``runner.handles()`` / ``runner.results()`` to inspect outcomes).

        Graceful shutdown: :meth:`stop` lets in-flight tasks finish before
        :meth:`aserve` returns. Hard shutdown: cancel the awaiting task /
        coroutine — pending work is cancelled mid-flight via the existing
        cooperative cancellation machinery.
        """
        with self._lock:
            if self._shutdown:
                raise RuntimeError("Cannot serve a runner that has been shut down.")
            deps_map = {hid: tuple(d.id for d in h.depends_on) for hid, h in self._handles.items()}
            from pyworkflowy._dag import topo_order

            topo_order(deps_map)
            pending_ids = [hid for hid, h in self._handles.items() if not h.done()]
        self._bind_loop_events()
        with _bind_runner(self):
            await self._run_pending(pending_ids, serve_mode=True)

    def serve(self) -> None:
        """Sync wrapper around :meth:`aserve`. Blocks until :meth:`stop` is called."""
        asyncio.run(self.aserve())

    def stop(self) -> None:
        """Request a graceful stop of the serve loop.

        Safe to call from any thread (a FastAPI request handler, a signal
        handler, etc.). In-flight tasks finish before :meth:`aserve` returns;
        no new submissions are dispatched after :meth:`stop` fires. No-op if
        no serve loop is active.
        """
        loop = self._loop
        event = self._stop_event
        if loop is None or event is None:
            return
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(event.set)

    def _bind_loop_events(self) -> None:
        """Bind the runner to the currently running loop and refresh signal events."""
        self._loop = asyncio.get_running_loop()
        # Fresh events each entry — a re-run after the previous loop closed
        # leaves dangling Events that are tied to the dead loop.
        self._new_work_event = asyncio.Event()
        self._stop_event = asyncio.Event()

    async def _wait_for_new_work_or_stop(self) -> str:
        """Block until new work is submitted or stop is requested.

        Returns ``"stop"`` if :meth:`stop` fired, ``"new_work"`` otherwise.
        Caller must clear ``_new_work_event`` before processing new work.
        """
        assert self._new_work_event is not None and self._stop_event is not None
        new_work_task = asyncio.create_task(
            self._new_work_event.wait(), name="pyworkflowy:wait-new"
        )
        stop_task = asyncio.create_task(self._stop_event.wait(), name="pyworkflowy:wait-stop")
        try:
            _, pending = await asyncio.wait(
                (new_work_task, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
            return "stop" if stop_task.done() and not stop_task.cancelled() else "new_work"
        finally:
            for t in (new_work_task, stop_task):
                if not t.done():
                    t.cancel()

    async def _run_pending(self, pending_ids: list[str], serve_mode: bool = False) -> None:
        # Map of id -> asyncio.Task once started, so cancel can interrupt them.
        running: dict[str, asyncio.Task[None]] = {}

        async def _runner_for(handle: TaskHandle[Any]) -> None:
            await self._execute_handle(handle)

        remaining = set(pending_ids)
        # Track every id we've already pulled into `remaining` so the
        # newly-submitted rescan in serve mode only adds genuinely new ones.
        seen_ids: set[str] = set(remaining)
        raise_later: BaseException | None = None
        try:
            while True:
                # 1. In serve mode, exit if stop was signalled.
                if serve_mode and self._stop_event is not None and self._stop_event.is_set():
                    break

                # 2. In serve mode, pull in any newly-submitted handles.
                if (
                    serve_mode
                    and self._new_work_event is not None
                    and self._new_work_event.is_set()
                ):
                    self._new_work_event.clear()
                    with self._lock:
                        for hid, h in self._handles.items():
                            if hid not in seen_ids and not h.done():
                                remaining.add(hid)
                                seen_ids.add(hid)

                # 3. Start every newly-ready handle whose pool has spare capacity.
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
                    self._transition(handle, TaskStatus.READY)
                    pool_name = handle.task.pool
                    self._pool_running[pool_name] += 1
                    if not self._pools[pool_name].is_privileged(handle.source):
                        self._pool_running_unprivileged[pool_name] += 1
                    aiotask = asyncio.create_task(_runner_for(handle), name=f"pyworkflowy:{hid}")
                    handle._aiotask = aiotask
                    running[hid] = aiotask
                    self._aiotasks[hid] = aiotask
                    progress = True

                # 4. Decide what to wait on next.
                if running:
                    done, _ = await asyncio.wait(
                        running.values(),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for d in done:
                        for hid, t in list(running.items()):
                            if t is d:
                                remaining.discard(hid)
                                del running[hid]
                                self._aiotasks.pop(hid, None)
                                break
                        exc = d.exception()
                        if exc is None:
                            continue
                        if serve_mode:
                            # Individual task failures must not bring down the
                            # serve loop. The terminal status is already on the
                            # handle; surface the exception via logging only.
                            _logger.error("pyworkflowy: task raised in serve loop: %r", exc)
                        elif raise_later is None:
                            raise_later = exc
                    if raise_later is not None:
                        raise raise_later
                elif remaining:
                    # We have work but couldn't make progress this iteration.
                    if not progress:
                        if serve_mode:
                            # In serve mode, wait for new work that might unblock
                            # this — typically a dep handle being submitted.
                            reason = await self._wait_for_new_work_or_stop()
                            if reason == "stop":
                                break
                        else:
                            raise TaskError(
                                "Runner deadlocked — no tasks are running and none can "
                                "progress. This usually means a dependency points to a "
                                "handle that was never submitted to this runner, or every "
                                "remaining task's pool is full while reservation rules "
                                "block the rest."
                            )
                    # If we did make progress this iteration but nothing is
                    # running yet (only marked skipped/failed), loop again.
                else:
                    # Idle: no remaining, no running.
                    if not serve_mode:
                        break
                    reason = await self._wait_for_new_work_or_stop()
                    if reason == "stop":
                        break
            # Graceful shutdown: let any in-flight tasks finish naturally.
            if running:
                await asyncio.gather(*running.values(), return_exceptions=True)
        except BaseException:
            # Error path: cancel anything still in-flight before propagating.
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
        if failed:
            policy = handle.task.on_dep_failure
            if policy == "skip":
                return "skip"
            if policy == "fail":
                return "fail"
            # "run-anyway" falls through to capacity check below.
        # Capacity / reservation check. Two-part:
        # 1. The pool as a whole is full → nobody can start.
        # 2. The task is unprivileged and the unprivileged sub-cap is filled →
        #    only privileged sources (those in pool.reserve_for) may proceed.
        pool = self._pools[handle.task.pool]
        if self._pool_running.get(pool.name, 0) >= pool.max_workers:
            return "wait"
        if not pool.is_privileged(handle.source):
            unp = self._pool_running_unprivileged.get(pool.name, 0)
            if unp >= pool.unprivileged_cap():
                return "wait"
        return "go"

    def _mark_skipped(self, handle: TaskHandle[Any]) -> None:
        result: TaskResult[Any] = TaskResult(
            name=handle.name,
            status=TaskStatus.SKIPPED,
            attempts=0,
        )
        # Terminal — _complete routes through _on_handle_terminal → save_handle.
        handle._complete(result)

    def _mark_dep_failed(self, handle: TaskHandle[Any]) -> None:
        failed_names = tuple(d.name for d in handle.depends_on if d.status != TaskStatus.COMPLETED)
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
        # Terminal — _complete routes through _on_handle_terminal → save_handle.
        handle._complete(result)
        if self._on_task_error == "raise":
            # Defer raising to the orchestrator gather; record on handle.
            pass
        elif self._on_task_error == "log":
            _logger.error("pyworkflowy: %s", err)

    async def _execute_handle(self, handle: TaskHandle[Any]) -> None:
        task_obj = handle.task
        attempts = 0
        last_exc: BaseException | None = None
        started_at = _wallclock()
        pool = self._pools[task_obj.pool]
        ctx = TaskContext(
            name=task_obj.name,
            attempt=1,
            cancel_event=handle._cancel_event,
            _handle=handle,
        )
        try:
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
                return
            while attempts < task_obj.max_attempts:
                attempts += 1
                ctx.attempt = attempts
                handle._retry_at = None
                if attempts == 1:
                    self._transition(handle, TaskStatus.RUNNING)
                else:
                    self._transition(handle, TaskStatus.RETRYING)
                try:
                    value = await self._dispatch_one(handle, ctx, pool)
                    finished = _wallclock()
                    result = TaskResult(
                        name=task_obj.name,
                        status=TaskStatus.COMPLETED,
                        value=value,
                        attempts=attempts,
                        started_at=started_at,
                        finished_at=finished,
                    )
                    handle._complete(result)
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
                        return
                    if not _is_retryable(exc, task_obj.retry_on):
                        break
                    if attempts >= task_obj.max_attempts:
                        break
                    delay = _compute_backoff(task_obj, attempts)
                    if delay > 0:
                        handle._retry_at = _wallclock() + delay
                        # retry_at just changed — persist the row so a UI can
                        # render "retrying in Ns" without poking internals.
                        self._persist_handle(handle)
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
            if self._on_task_error == "raise":
                raise wrapped_err
            if self._on_task_error == "log":
                _logger.error(
                    "pyworkflowy: task %r failed after %d attempt(s): %r",
                    task_obj.name,
                    attempts,
                    wrapped_err,
                )
        finally:
            # Release the pool slot regardless of how we exit.
            with self._lock:
                if self._pool_running.get(pool.name, 0) > 0:
                    self._pool_running[pool.name] -= 1
                if (
                    not pool.is_privileged(handle.source)
                    and self._pool_running_unprivileged.get(pool.name, 0) > 0
                ):
                    self._pool_running_unprivileged[pool.name] -= 1

    async def _dispatch_one(
        self,
        handle: TaskHandle[Any],
        ctx: TaskContext,
        pool: Pool,
    ) -> Any:
        task_obj = handle.task
        args = handle.args
        kwargs = handle.kwargs
        timeout = task_obj.timeout
        if pool.kind == "asyncio":
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
        # Thread / process pool: run on executor and await its future.
        if task_obj.is_async:
            raise TaskError(
                f"Task {task_obj.name!r} is async but pool {pool.name!r} is of kind "
                f"{pool.kind!r}. Async tasks require a pool of kind 'asyncio'."
            )
        executor = self._get_pool_executor(pool)
        loop = asyncio.get_running_loop()
        # Process pool: the function ships across a pickle boundary, so we
        # cannot wrap it in a closure that sets contextvars (closures aren't
        # picklable). Documented: current_task() returns None inside process
        # workers. For threads, set the contextvar so introspection works.
        if pool.kind == "thread":
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

    def _get_pool_executor(self, pool: Pool) -> PoolExecutor:
        with self._backends_lock:
            existing = self._pool_executors.get(pool.name)
            if existing is not None:
                return existing
            executor = build_pool_executor(pool)
            self._pool_executors[pool.name] = executor
            return executor

    # ---------- introspection ----------

    def handles(self) -> list[TaskHandle[Any]]:
        with self._lock:
            return list(self._handles.values())

    def results(self) -> dict[str, TaskResult[Any]]:
        with self._lock:
            return {h.name: h._result for h in self._handles.values() if h._result is not None}

    def find_active(self, name: str, dedup_key: str) -> TaskHandle[Any] | None:
        """Return the non-terminal handle for (name, dedup_key), if any."""
        with self._lock:
            return self._dedup_index.get((name, dedup_key))

    def has_active(self, name: str, dedup_key: str) -> bool:
        """True iff a non-terminal handle exists for (name, dedup_key)."""
        return self.find_active(name, dedup_key) is not None

    # ---------- cancellation ----------

    def cancel_all(self) -> int:
        cancelled = 0
        with self._lock:
            for h in self._handles.values():
                if not h.done() and not h._cancel_event.is_set():
                    h._cancel_event.set()
                    self._notify_cancel(h)
                    cancelled += 1
        return cancelled

    def _notify_cancel(self, handle: TaskHandle[Any]) -> None:
        """Poke the running asyncio task so cancellation observes a suspension point.

        Without this, cancellation from a non-loop thread only sets the flag —
        a fully-async task awaiting ``asyncio.sleep`` would only see the flag
        on the next natural completion. Scheduling ``aiotask.cancel()`` via
        ``call_soon_threadsafe`` raises ``CancelledError`` at the next
        suspension point regardless of which thread called ``handle.cancel()``.
        """
        aiotask = handle._aiotask
        loop = self._loop
        if aiotask is None or loop is None:
            return
        if aiotask.done():
            return
        # If the loop is closed, fall back silently to the flag-based cancel.
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(aiotask.cancel)

    # ---------- shutdown ----------

    def shutdown(self, *, wait: bool = True) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
        with self._backends_lock:
            for backend in self._pool_executors.values():
                backend.shutdown(wait=wait)
            self._pool_executors.clear()

    # ---------- per-transition persistence ----------

    def _persist_handle(self, handle: TaskHandle[Any]) -> None:
        """Persist a single handle's current state via :meth:`Checkpointer.save_handle`.

        Called on every status transition (and from the retry loop when only
        ``retry_at`` changed). Never raises — checkpoint backends should be
        able to fail without bringing down the runner. Updates
        ``_last_checkpoint`` so the progress throttle treats this as a recent
        write.
        """
        if self._checkpointer is None:
            return
        try:
            self._checkpointer.save_handle(self._handle_to_entry(handle))
        except Exception as exc:
            _logger.error("pyworkflowy: save_handle failed: %r", exc)
        self._last_checkpoint = time.monotonic()

    def _transition(self, handle: TaskHandle[Any], status: TaskStatus) -> None:
        """Set ``handle`` to ``status`` and persist the transition.

        Use this for non-terminal transitions (READY, RUNNING, RETRYING).
        Terminal transitions go through :meth:`TaskHandle._complete`, which
        routes via :meth:`_on_handle_terminal` to ``save_handle`` already.
        """
        handle._set_status(status)
        self._persist_handle(handle)

    # ---------- progress & terminal hooks ----------

    def _on_handle_terminal(self, handle: TaskHandle[Any]) -> None:
        """Called from :meth:`TaskHandle._complete` when a handle terminates.

        Drops the handle from the dedup index so a future submit with the
        same key starts fresh, and persists the terminal row.
        """
        if handle._dedup_key is not None:
            with self._lock:
                key = (handle.name, handle._dedup_key)
                if self._dedup_index.get(key) is handle:
                    del self._dedup_index[key]
        self._persist_handle(handle)

    def _maybe_persist_progress(self, handle: TaskHandle[Any]) -> None:
        """Persist a progress update through the checkpointer, throttled.

        Skip if the delta since the last persisted progress is small *and*
        we wrote one recently. Terminal transitions go through
        :meth:`_on_handle_terminal` instead.
        """
        if self._checkpointer is None:
            return
        now = time.monotonic()
        last = handle._last_progress_write
        delta = abs(handle._progress - last)
        if (
            handle._progress < 1.0
            and delta < self._progress_throttle_delta
            and (now - self._last_checkpoint) < self._progress_throttle_seconds
        ):
            return
        handle._last_progress_write = handle._progress
        self._last_checkpoint = now
        try:
            self._checkpointer.save_handle(self._handle_to_entry(handle))
        except Exception as exc:
            _logger.error("pyworkflowy: progress checkpoint failed: %r", exc)

    # ---------- checkpointing ----------

    def _handle_to_entry(self, h: TaskHandle[Any]) -> dict[str, Any]:
        """Serialise a single handle into the v2 state schema."""
        entry: dict[str, Any] = {
            "id": h.id,
            "name": h.name,
            "args": list(h.args),
            "kwargs": dict(h.kwargs),
            "depends_on": [d.id for d in h.depends_on],
            "status": h.status.value,
            "pool": h.task.pool,
            "source": h.source,
            "dedup_key": h._dedup_key,
            "progress": h._progress,
            "progress_message": h._progress_message,
            "retry_at": h._retry_at,
        }
        if h._result is not None:
            entry["value"] = h._result.value if h._result.ok else None
            entry["error"] = repr(h._result.error) if h._result.error is not None else None
            entry["attempts"] = h._result.attempts
            entry["started_at"] = h._result.started_at
            entry["finished_at"] = h._result.finished_at
        return entry

    def _dump_state(self) -> dict[str, Any]:
        with self._lock:
            handles_state = [
                self._handle_to_entry(self._handles[hid]) for hid in self._submission_order
            ]
            return {"version": CHECKPOINT_VERSION, "handles": handles_state}

    @classmethod
    def resume(
        cls,
        path: str,
        *,
        max_workers: int = 8,
        pools: Mapping[str, Pool] | None = None,
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
        loader: SnapshotCheckpointer
        if checkpointer is not None:
            if not isinstance(checkpointer, SnapshotCheckpointer):
                raise TypeError(
                    "TaskRunner.resume() requires a SnapshotCheckpointer "
                    "(needs whole-state load). Per-row checkpointers should "
                    "reconstruct state from query() and submit() handles back "
                    "into a fresh runner."
                )
            loader = checkpointer
        elif path.endswith(".pkl") or path.endswith(".pickle"):
            loader = PickleCheckpointer(path)
        else:
            loader = JSONCheckpointer(path)
        state = loader.load()
        runner = cls(
            max_workers=max_workers,
            pools=pools,
            on_task_error=on_task_error,
            checkpoint_interval=checkpoint_interval,
            checkpointer=loader,
        )
        if state is None:
            return runner
        terminal = {
            TaskStatus.COMPLETED.value,
            TaskStatus.SKIPPED.value,
            TaskStatus.FAILED.value,
            TaskStatus.CANCELLED.value,
        }
        for entry in state.get("handles", []):
            status = entry.get("status")
            if status in terminal:
                runner._resumed_results[entry["id"]] = entry
            else:
                # RUNNING / RETRYING / READY / PENDING: orphan from a prior process.
                runner._orphaned.append(entry)
        return runner

    def _prime_from_resume(self, handle: TaskHandle[Any], entry: dict[str, Any]) -> None:
        status_str = entry.get("status", TaskStatus.COMPLETED.value)
        status = TaskStatus(status_str)
        if "progress" in entry:
            handle._progress = float(entry["progress"])
        if "progress_message" in entry:
            handle._progress_message = entry["progress_message"]
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

    For thread pools; process pools can't share contextvars across the
    boundary, so ``current_task()`` returns ``None`` inside process workers
    (documented behaviour).
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
