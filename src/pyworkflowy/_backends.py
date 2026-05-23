"""Execution backends: asyncio, thread pool, process pool.

Each backend implements :meth:`Backend.execute` which takes a task body and
returns its value, honoring the runner-level concerns the caller has already
arranged (cancellation flag, contextvars). Retries and timeouts are layered
*on top* of these backends by :mod:`pyworkflowy._runner` — backends only run a
single attempt.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Any

from pyworkflowy.exceptions import TaskCancelledError, TaskTimeoutError

if TYPE_CHECKING:
    from pyworkflowy._core import TaskContext

__all__ = [
    "BackendExecutor",
    "ProcessBackend",
    "ThreadBackend",
    "asyncio_execute",
    "build_backend",
]


# ---------- thread / process backend ----------


class BackendExecutor:
    """Protocol-shaped base for the thread/process backends.

    Both real backends own a ``concurrent.futures`` executor and translate
    submit/cancel into futures calls. The asyncio backend is *not* a
    BackendExecutor — it runs cooperatively inside the runner's loop and
    needs no pool.
    """

    name: str
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


class ThreadBackend(BackendExecutor):
    """Runs each attempt on a :class:`concurrent.futures.ThreadPoolExecutor`.

    Cancellation is cooperative — the task body must check
    ``current_task().cancel_event`` (or accept the timeout) to actually
    stop. Threads cannot be force-killed, so a runaway task will hang
    shutdown if it ignores cooperative signals.
    """

    name = "thread"

    def __init__(self, max_workers: int) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="pyworkflowy"
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
                f"Task {task_name!r} exceeded its timeout of {timeout}s on the thread backend"
            ) from exc


class ProcessBackend(BackendExecutor):
    """Runs each attempt on a :class:`concurrent.futures.ProcessPoolExecutor`.

    Task functions must be importable (top-level or class-level — not nested
    closures or lambdas), because the multiprocessing pickler serializes the
    function reference to ship it to the worker. On timeout, the runner
    cancels the future and the worker is left to finish or be reaped; a
    *force* terminate is intentionally avoided to keep pool stability.
    """

    name = "process"

    def __init__(self, max_workers: int) -> None:
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

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
                f"Task {task_name!r} exceeded its timeout of {timeout}s on the process backend"
            ) from exc


def build_backend(name: str, max_workers: int) -> BackendExecutor:
    """Build a :class:`BackendExecutor` for ``name``.

    The asyncio backend is special-cased upstream and does not produce a
    :class:`BackendExecutor` — only ``thread`` and ``process`` return one.
    """
    if name == "thread":
        return ThreadBackend(max_workers)
    if name == "process":
        return ProcessBackend(max_workers)
    raise ValueError(f"Unknown backend {name!r}; expected 'thread' or 'process'.")


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
    they'd pick the ``thread`` backend). Cancellation works cooperatively
    via asyncio's normal task cancellation machinery; the runner also sets
    ``cancel_event`` for parity with the other backends.
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
                    f"Task {task_name!r} exceeded its timeout of {timeout}s on the asyncio backend"
                ) from exc
            except asyncio.CancelledError as exc:
                cancel_event.set()
                raise TaskCancelledError(f"Task {task_name!r} was cancelled") from exc
        # Sync fn on asyncio: run inline. Timeout is checked *after*; for
        # CPU-bound sync code, prefer the thread backend.
        if timeout is not None:
            loop = asyncio.get_running_loop()
            start = loop.time()
            result = fn(*args, **kwargs)
            elapsed = loop.time() - start
            if elapsed > timeout:
                raise TaskTimeoutError(
                    f"Task {task_name!r} took {elapsed:.3f}s, exceeding timeout of {timeout}s "
                    "(sync function on asyncio backend; consider backend='thread' for "
                    "true timeout cancellation)",
                    elapsed=elapsed,
                )
            return result
        return fn(*args, **kwargs)
    finally:
        teardown(token)
