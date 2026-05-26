"""Execution pools: named pools backed by asyncio, threads, or processes.

Each :class:`Pool` declares a ``kind`` (asyncio / thread / process), a
``max_workers`` size, and an optional reservation rule. Tasks declare which
pool they want with ``@task(pool="<name>")``; the runner maps the pool name to
an executor (or to inline asyncio execution for the ``asyncio`` kind).

Retries and timeouts are layered *on top* of these pools by
:mod:`pyworkflowy._runner` — pools only run a single attempt.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any, Literal

from pyworkflowy.exceptions import TaskCancelledError, TaskTimeoutError

if TYPE_CHECKING:
    from pyworkflowy._core import TaskContext

__all__ = [
    "OffloadPool",
    "Pool",
    "PoolExecutor",
    "PoolKind",
    "ProcessPool",
    "ThreadPool",
    "asyncio_execute",
    "build_pool_executor",
]


PoolKind = Literal["asyncio", "thread", "process", "offload"]


# ---------- pool config ----------


@dataclass(frozen=True, slots=True)
class Pool:
    """Configuration for one named concurrency pool.

    ``kind`` picks the runtime: ``"asyncio"`` runs inline on the runner's
    event loop, ``"thread"`` uses a :class:`ThreadPoolExecutor`, and
    ``"process"`` uses a :class:`ProcessPoolExecutor`. ``max_workers`` caps
    concurrent attempts on this pool.

    ``reserve_for`` lets you protect headroom for high-priority task sources:
    tasks whose ``source`` is in ``reserve_for`` may use every slot, while
    other sources are capped at ``max_workers - reserved_slots``. When
    ``reserve_for`` is non-empty, ``reserved_slots`` defaults to 1 — at least
    one slot is always available for the privileged sources. Mirrors the
    background-task-reservation pattern from the consumer's in-house queue.
    """

    name: str
    kind: PoolKind = "asyncio"
    max_workers: int = 8
    reserve_for: tuple[str, ...] = ()
    reserved_slots: int = -1  # -1 = "auto: 1 if reserve_for else 0"

    def __post_init__(self) -> None:
        if self.max_workers < 1:
            raise ValueError(
                f"Pool {self.name!r}: max_workers must be >= 1, got {self.max_workers}"
            )
        if self.kind not in ("asyncio", "thread", "process", "offload"):
            raise ValueError(
                f"Pool {self.name!r}: kind must be 'asyncio', 'thread', 'process', "
                f"or 'offload'; got {self.kind!r}"
            )
        # Resolve auto reserved_slots in-place via object.__setattr__ (frozen dataclass).
        if self.reserved_slots == -1:
            object.__setattr__(self, "reserved_slots", 1 if self.reserve_for else 0)
        if self.reserved_slots < 0:
            raise ValueError(
                f"Pool {self.name!r}: reserved_slots must be >= 0, got {self.reserved_slots}"
            )
        if self.reserved_slots >= self.max_workers:
            raise ValueError(
                f"Pool {self.name!r}: reserved_slots ({self.reserved_slots}) must be "
                f"strictly less than max_workers ({self.max_workers})"
            )

    def is_privileged(self, source: str) -> bool:
        """True iff a task with this source may consume reserved slots.

        Sources in :attr:`reserve_for` are privileged; everything else
        competes for the unprivileged portion of the pool (max_workers minus
        reserved_slots).
        """
        return not self.reserve_for or source in self.reserve_for

    def unprivileged_cap(self) -> int:
        """Maximum concurrent unprivileged tasks on this pool.

        Equal to ``max_workers - reserved_slots`` — the rest of the pool is
        always reserved for sources in :attr:`reserve_for`.
        """
        return self.max_workers - self.reserved_slots


# ---------- pool executors ----------


class PoolExecutor:
    """Base for thread/process pool runtimes.

    Both real executors own a :mod:`concurrent.futures` executor and translate
    submit/cancel into futures calls. The asyncio kind has no
    :class:`PoolExecutor` — it runs cooperatively inside the runner's loop and
    needs no pool.
    """

    pool: Pool
    _executor: ThreadPoolExecutor | ProcessPoolExecutor

    def execute(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        timeout: float | None,
        cancel_event: threading.Event,
        task_name: str,
    ) -> Any:
        raise NotImplementedError

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)


class ThreadPool(PoolExecutor):
    """Runs each attempt on a :class:`concurrent.futures.ThreadPoolExecutor`.

    Cancellation is cooperative — the task body must check
    ``current_task().cancel_event`` (or accept the timeout) to actually
    stop. Threads cannot be force-killed, so a runaway task will hang
    shutdown if it ignores cooperative signals.
    """

    def __init__(self, pool: Pool) -> None:
        self.pool = pool
        self._executor = ThreadPoolExecutor(
            max_workers=pool.max_workers, thread_name_prefix=f"pyworkflowy-{pool.name}"
        )

    def execute(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        timeout: float | None,
        cancel_event: threading.Event,
        task_name: str,
    ) -> Any:
        future = self._executor.submit(fn, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except TimeoutError as exc:
            cancel_event.set()
            raise TaskTimeoutError(
                f"Task {task_name!r} exceeded its timeout of {timeout}s on "
                f"pool {self.pool.name!r} (thread)"
            ) from exc


class ProcessPool(PoolExecutor):
    """Runs each attempt on a :class:`concurrent.futures.ProcessPoolExecutor`.

    Task functions must be importable (top-level or class-level — not nested
    closures or lambdas), because the multiprocessing pickler serializes the
    function reference to ship it to the worker. On timeout, the runner
    cancels the future and the worker is left to finish or be reaped; a
    *force* terminate is intentionally avoided to keep pool stability.
    """

    def __init__(self, pool: Pool) -> None:
        self.pool = pool
        self._executor = ProcessPoolExecutor(max_workers=pool.max_workers)

    def execute(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        timeout: float | None,
        cancel_event: threading.Event,
        task_name: str,
    ) -> Any:
        future = self._executor.submit(fn, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except TimeoutError as exc:
            future.cancel()
            raise TaskTimeoutError(
                f"Task {task_name!r} exceeded its timeout of {timeout}s on "
                f"pool {self.pool.name!r} (process)"
            ) from exc


class OffloadPool(PoolExecutor):
    """Call-only thread pool for :meth:`TaskContext.offload` invocations.

    Structurally identical to :class:`ThreadPool`, but tasks may *not* target
    an offload-kind pool via ``@task(pool=...)``. The runner rejects such
    submissions at :meth:`TaskRunner.submit` time. Instead, offload pools
    are invoked from inside an async task body via ``ctx.offload(fn, ...)``
    to run sync C-extension chunks (Pillow, ONNX, numpy) off the event loop
    without blocking it.

    The :meth:`execute` method is implemented for symmetry with the other
    :class:`PoolExecutor` subclasses, but the runner will never call it for
    offload kinds — ``ctx.offload`` schedules work directly through the
    underlying ``_executor`` via ``loop.run_in_executor``.
    """

    def __init__(self, pool: Pool) -> None:
        self.pool = pool
        self._executor = ThreadPoolExecutor(
            max_workers=pool.max_workers,
            thread_name_prefix=f"pyworkflowy-offload-{pool.name}",
        )

    def execute(
        self,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        timeout: float | None,
        cancel_event: threading.Event,
        task_name: str,
    ) -> Any:
        future = self._executor.submit(fn, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except TimeoutError as exc:
            cancel_event.set()
            raise TaskTimeoutError(
                f"Task {task_name!r} exceeded its timeout of {timeout}s on "
                f"pool {self.pool.name!r} (offload)"
            ) from exc


def build_pool_executor(pool: Pool) -> PoolExecutor:
    """Build a :class:`PoolExecutor` for ``pool``.

    The asyncio kind is special-cased upstream and does not produce a
    :class:`PoolExecutor` — only ``thread``, ``process``, and ``offload``
    return one.
    """
    if pool.kind == "thread":
        return ThreadPool(pool)
    if pool.kind == "process":
        return ProcessPool(pool)
    if pool.kind == "offload":
        return OffloadPool(pool)
    raise ValueError(
        f"Pool {pool.name!r}: kind={pool.kind!r} does not need an executor "
        "(asyncio kind runs inline on the runner's loop)."
    )


# ---------- asyncio backend ----------


async def asyncio_execute(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    timeout: float | None,
    cancel_event: threading.Event,
    task_name: str,
    ctx: TaskContext,
    setup: Callable[[], Any],
    teardown: Callable[[Any], None],
) -> Any:
    """Run one attempt on the asyncio event loop.

    Supports both sync and async ``fn``. Sync ``fn`` runs inline on the loop
    (we deliberately don't shove it onto a thread — if the user wanted that,
    they'd pick a ``thread`` pool). Cancellation works cooperatively via
    asyncio's normal task cancellation machinery; the runner also sets
    ``cancel_event`` for parity with the other pool kinds.
    """
    token = setup()
    try:
        if iscoroutinefunction(fn):
            coro: Awaitable[Any] = fn(*args, **kwargs)
            try:
                if timeout is not None:
                    return await asyncio.wait_for(coro, timeout=timeout)
                return await coro
            except TimeoutError as exc:
                cancel_event.set()
                raise TaskTimeoutError(
                    f"Task {task_name!r} exceeded its timeout of {timeout}s on the asyncio pool"
                ) from exc
            except asyncio.CancelledError as exc:
                cancel_event.set()
                raise TaskCancelledError(f"Task {task_name!r} was cancelled") from exc
        # Sync fn on asyncio: run inline. Timeout is checked *after*; for
        # CPU-bound sync code, prefer a thread pool.
        if timeout is not None:
            loop = asyncio.get_running_loop()
            start = loop.time()
            result = fn(*args, **kwargs)
            elapsed = loop.time() - start
            if elapsed > timeout:
                raise TaskTimeoutError(
                    f"Task {task_name!r} took {elapsed:.3f}s, exceeding timeout of {timeout}s "
                    "(sync function on asyncio pool; consider a thread pool for "
                    "true timeout cancellation)",
                    elapsed=elapsed,
                )
            return result
        return fn(*args, **kwargs)
    finally:
        teardown(token)


# Silence unused-import linters for re-exports below.
_ = field
