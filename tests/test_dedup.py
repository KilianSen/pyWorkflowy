from __future__ import annotations

import asyncio

import pytest

from pyworkflowy import TaskRunner, TaskStatus, task


def test_dedup_key_returns_same_handle_when_pending() -> None:
    @task
    def f(x: int) -> int:
        return x

    runner = TaskRunner()
    h1 = runner.submit(f, 1, dedup_key="k1")
    h2 = runner.submit(f, 1, dedup_key="k1")
    assert h1 is h2  # second submit returns the existing pending handle
    runner.run()
    runner.shutdown()
    assert h1.dedup_key == "k1"


def test_dedup_releases_after_terminal() -> None:
    """After the first handle terminates, a fresh submit with the same key
    produces a new handle.
    """

    @task
    def f() -> int:
        return 42

    with TaskRunner() as runner:
        h1 = runner.submit(f, dedup_key="k1")
        runner.run()
        # h1 is now terminal — same key should produce a new handle.
        h2 = runner.submit(f, dedup_key="k1")
        runner.run()
    assert h1 is not h2
    assert h1.result() == 42
    assert h2.result() == 42


def test_different_keys_dont_collide() -> None:
    @task
    def f(x: int) -> int:
        return x

    with TaskRunner() as runner:
        h1 = runner.submit(f, 1, dedup_key="a")
        h2 = runner.submit(f, 1, dedup_key="b")
        runner.run()
    assert h1 is not h2


def test_dedup_by_auto_computes_key() -> None:
    """@task(dedup_by=("book_id",)) auto-suppresses duplicate enqueues by kwarg."""
    calls: list[int] = []

    @task(dedup_by=("book_id",))
    def refresh(book_id: int) -> int:
        calls.append(book_id)
        return book_id

    runner = TaskRunner()
    h1 = runner.submit(refresh, book_id=7)
    h2 = runner.submit(refresh, book_id=7)
    h3 = runner.submit(refresh, book_id=8)
    assert h1 is h2
    assert h1 is not h3
    runner.run()
    runner.shutdown()
    # book_id=7 ran once, book_id=8 ran once
    assert sorted(calls) == [7, 8]


def test_dedup_by_missing_kwarg_raises() -> None:
    @task(dedup_by=("book_id",))
    def refresh(book_id: int) -> int:
        return book_id

    with TaskRunner() as runner, pytest.raises(ValueError, match="dedup_by"):
        runner.submit(refresh, 7)  # positional, not kwarg


async def test_dedup_while_running_still_returns_running_handle() -> None:
    """Submitting a duplicate while the original is RUNNING returns the
    original (not yet terminal).
    """
    started = asyncio.Event()
    release = asyncio.Event()

    @task
    async def slow() -> int:
        started.set()
        await release.wait()
        return 1

    runner = TaskRunner()
    h1 = runner.submit(slow, dedup_key="k")

    async def run_then_release() -> None:
        await started.wait()
        # Now h1 is RUNNING. Submit again — should return h1.
        h2 = runner.submit(slow, dedup_key="k")
        assert h2 is h1
        release.set()

    await asyncio.gather(runner.arun(), run_then_release())
    runner.shutdown()
    assert h1.status == TaskStatus.COMPLETED


def test_find_active_and_has_active() -> None:
    """find_active returns the live handle before run; both methods report
    False/None after the task completes.
    """

    @task
    def g(x: int) -> int:
        return x

    runner = TaskRunner()
    h = runner.submit(g, 42, dedup_key="k")
    task_name = h.name  # fully-qualified name used as the index key

    # Before run: handle is non-terminal, should be visible.
    assert runner.has_active(task_name, "k") is True
    found = runner.find_active(task_name, "k")
    assert found is h

    runner.run()
    runner.shutdown()

    # After run: handle is terminal, index should be empty.
    assert runner.has_active(task_name, "k") is False
    assert runner.find_active(task_name, "k") is None


def test_find_active_missing_key() -> None:
    """A key that was never submitted returns None / False."""

    runner = TaskRunner()

    assert runner.find_active("nonexistent", "no-such-key") is None
    assert runner.has_active("nonexistent", "no-such-key") is False

    runner.shutdown()
