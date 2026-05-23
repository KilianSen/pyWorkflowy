from __future__ import annotations

import pytest

from pyworkflowy import TaskRunner, TaskStatus, task
from pyworkflowy.exceptions import RetryExhaustedError


def test_retry_eventually_succeeds() -> None:
    calls: list[int] = []

    @task(retries=3, backoff="none")
    def flaky() -> int:
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("not yet")
        return 42

    with TaskRunner() as runner:
        h = runner.submit(flaky)
        runner.run()

    assert h.result() == 42
    assert len(calls) == 3
    assert h.get_result().attempts == 3


def test_retry_exhausted_raises() -> None:
    @task(retries=2, backoff="none")
    def always_bad() -> int:
        raise RuntimeError("nope")

    with TaskRunner(on_task_error="continue") as runner:
        h = runner.submit(always_bad)
        runner.run()

    assert h.status == TaskStatus.FAILED
    err = h.get_result().error
    assert isinstance(err, RetryExhaustedError)
    assert err.attempts == 3
    assert isinstance(err.__cause__, RuntimeError)


def test_no_retry_when_retries_zero() -> None:
    calls: list[int] = []

    @task(retries=0)
    def bad() -> int:
        calls.append(1)
        raise RuntimeError("x")

    with TaskRunner(on_task_error="continue") as runner:
        runner.submit(bad)
        runner.run()

    assert calls == [1]


def test_retry_on_filters_exception() -> None:
    calls: list[int] = []

    @task(retries=3, backoff="none", retry_on=ValueError)
    def f() -> int:
        calls.append(1)
        raise RuntimeError("not in retry_on")

    with TaskRunner(on_task_error="continue") as runner:
        runner.submit(f)
        runner.run()

    # Non-retryable error — single attempt only.
    assert len(calls) == 1


def test_retry_on_tuple() -> None:
    calls: list[int] = []

    @task(retries=2, backoff="none", retry_on=(ValueError, KeyError))
    def f() -> int:
        calls.append(1)
        if len(calls) < 2:
            raise ValueError("retry me")
        return 7

    with TaskRunner() as runner:
        h = runner.submit(f)
        runner.run()

    assert h.result() == 7
    assert len(calls) == 2


def test_linear_backoff_delays(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    import asyncio

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    @task(retries=2, backoff="linear", backoff_base=0.5, backoff_max=10.0)
    def f() -> int:
        raise RuntimeError("retry")

    with TaskRunner(on_task_error="continue") as runner:
        runner.submit(f)
        runner.run()

    # linear: 0.5 * 1, 0.5 * 2 = 0.5, 1.0
    assert sleeps == pytest.approx([0.5, 1.0])


def test_exponential_backoff_delays(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    import asyncio

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    @task(retries=3, backoff="exponential", backoff_base=1.0, backoff_max=100.0)
    def f() -> int:
        raise RuntimeError("retry")

    with TaskRunner(on_task_error="continue") as runner:
        runner.submit(f)
        runner.run()

    # exponential: 1, 2, 4
    assert sleeps == pytest.approx([1.0, 2.0, 4.0])


def test_backoff_max_caps() -> None:
    from pyworkflowy._core import _build_task
    from pyworkflowy._runner import _compute_backoff

    t = _build_task(
        lambda: 1,
        name="x",
        pool="default",
        retries=10,
        timeout=None,
        backoff="exponential",
        backoff_base=1.0,
        backoff_max=5.0,
        retry_on=None,
        on_dep_failure="fail",
    )
    # After enough attempts, the value should cap at 5.0
    assert _compute_backoff(t, 100) == 5.0


def test_backoff_none() -> None:
    from pyworkflowy._core import _build_task
    from pyworkflowy._runner import _compute_backoff

    t = _build_task(
        lambda: 1,
        name="x",
        pool="default",
        retries=2,
        timeout=None,
        backoff="none",
        backoff_base=1.0,
        backoff_max=5.0,
        retry_on=None,
        on_dep_failure="fail",
    )
    assert _compute_backoff(t, 1) == 0.0
    assert _compute_backoff(t, 5) == 0.0
