from __future__ import annotations

import asyncio
import time

import pytest

from pyworkflowy import TaskRunner, TaskStatus, task


async def test_retry_at_set_during_backoff() -> None:
    """While a retrying task is in its backoff window, handle.retry_at
    exposes a future wall-clock timestamp.
    """

    sleeps_started: asyncio.Event = asyncio.Event()
    real_sleep = asyncio.sleep

    async def slow_sleep(delay: float) -> None:
        sleeps_started.set()
        await real_sleep(0.2)  # short for the test but long enough to observe

    @task(retries=2, backoff="exponential", backoff_base=0.5)
    def f() -> int:
        raise RuntimeError("retry me")

    # Monkeypatch asyncio.sleep at the runner level via the existing path.
    # Easier: just observe handle.retry_at while the runner is sleeping.
    runner = TaskRunner(on_task_error="continue")
    h = runner.submit(f)

    async def observe() -> None:
        # Wait until first failure schedules a retry.
        deadline = time.monotonic() + 5.0
        observed_future = False
        while time.monotonic() < deadline:
            ra = h.retry_at
            if ra is not None and ra > time.time():
                observed_future = True
                break
            await asyncio.sleep(0.02)
        assert observed_future, "handle.retry_at was never set to a future timestamp"

    await asyncio.gather(runner.arun(), observe())
    runner.shutdown()
    # After the final failure, retry_at is cleared.
    assert h.retry_at is None
    assert h.status == TaskStatus.FAILED


def test_retry_at_cleared_on_success() -> None:
    """A successful task never has a lingering retry_at."""

    @task(retries=2, backoff="none")
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        h = runner.submit(f)
        runner.run()
    assert h.retry_at is None
    assert h.result() == 1
