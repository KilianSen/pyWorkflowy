from __future__ import annotations

import asyncio
import time

import pytest

from pyworkflowy import TaskRunner, TaskStatus, task
from pyworkflowy.exceptions import TaskTimeoutError


async def test_async_timeout_fires() -> None:
    @task(timeout=0.1)
    async def slow() -> int:
        await asyncio.sleep(1.0)
        return 1

    runner = TaskRunner(on_task_error="continue")
    h = runner.submit(slow)
    await runner.arun()
    runner.shutdown()

    assert h.status == TaskStatus.FAILED
    assert isinstance(h.get_result().error, TaskTimeoutError)


async def test_async_no_timeout_when_fast() -> None:
    @task(timeout=1.0)
    async def fast() -> int:
        await asyncio.sleep(0)
        return 42

    runner = TaskRunner()
    h = runner.submit(fast)
    await runner.arun()
    runner.shutdown()

    assert h.result() == 42


def test_thread_timeout_fires() -> None:
    @task(backend="thread", timeout=0.1)
    def slow() -> int:
        time.sleep(1.0)
        return 1

    with TaskRunner(on_task_error="continue") as runner:
        h = runner.submit(slow)
        runner.run()

    assert h.status == TaskStatus.FAILED
    assert isinstance(h.get_result().error, TaskTimeoutError)


def test_timeout_not_retried() -> None:
    """Timeouts should not trigger retries (they're treated as final)."""
    calls: list[int] = []

    @task(backend="thread", timeout=0.05, retries=3, backoff="none")
    def slow() -> int:
        calls.append(1)
        time.sleep(0.5)
        return 1

    with TaskRunner(on_task_error="continue") as runner:
        h = runner.submit(slow)
        runner.run()

    assert h.status == TaskStatus.FAILED
    # Only one attempt — timeouts are non-retryable.
    assert len(calls) == 1


def test_invalid_timeout() -> None:
    with pytest.raises(ValueError, match="timeout"):

        @task(timeout=0)
        def f() -> None:
            return None

    with pytest.raises(ValueError, match="timeout"):

        @task(timeout=-1.0)
        def f2() -> None:
            return None
