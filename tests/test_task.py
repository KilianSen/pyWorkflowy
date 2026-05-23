from __future__ import annotations

import asyncio

import pytest

from pytasky import Task, TaskRunner, TaskStatus, current_task, task


def test_bare_decorator() -> None:
    @task
    def square(x: int) -> int:
        return x * x

    assert isinstance(square, Task)
    assert square.name.endswith("test_bare_decorator.<locals>.square")
    assert square(5) == 25


def test_decorator_with_args() -> None:
    @task(name="custom", retries=2, timeout=5.0, backend="thread")
    def f(x: int) -> int:
        return x + 1

    assert f.name == "custom"
    assert f.retries == 2
    assert f.timeout == 5.0
    assert f.backend == "thread"
    assert f.max_attempts == 3


def test_async_detected() -> None:
    @task
    async def af(x: int) -> int:
        await asyncio.sleep(0)
        return x

    assert af.is_async


def test_async_with_thread_backend_rejected() -> None:
    with pytest.raises(ValueError, match="async"):
        @task(backend="thread")
        async def af() -> None:
            return None


def test_invalid_backend() -> None:
    with pytest.raises(ValueError, match="backend"):
        @task(backend="nope")  # type: ignore[arg-type]
        def f() -> None:
            return None


def test_retries_negative() -> None:
    with pytest.raises(ValueError):
        @task(retries=-1)
        def f() -> None:
            return None


def test_max_attempts_alias() -> None:
    @task(max_attempts=3)
    def f() -> None:
        return None

    assert f.retries == 2
    assert f.max_attempts == 3


def test_max_attempts_conflict() -> None:
    with pytest.raises(ValueError):
        @task(retries=1, max_attempts=2)
        def f() -> None:
            return None


def test_submit_requires_runner() -> None:
    @task
    def f() -> int:
        return 1

    with pytest.raises(RuntimeError, match="no runner"):
        f.submit()


def test_submit_inside_runner() -> None:
    @task
    def f(x: int) -> int:
        return x * 2

    with TaskRunner() as runner:
        handle = f.submit(21)
        results = runner.run()
        assert handle.status == TaskStatus.COMPLETED
        assert handle.result() == 42
        assert results[f.name].value == 42


def test_current_task_inside_body() -> None:
    seen: list[str] = []

    @task
    def f() -> None:
        ctx = current_task()
        assert ctx is not None
        seen.append(ctx.name)

    with TaskRunner() as runner:
        f.submit()
        runner.run()

    assert len(seen) == 1
    assert seen[0] == f.name
