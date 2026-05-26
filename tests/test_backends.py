from __future__ import annotations

import asyncio

from pyworkflowy import TaskRunner, TaskStatus, current_task, task
from tests._helpers import add, boom, slow_square, square  # type: ignore[import-not-found]


def test_thread_backend_runs() -> None:
    @task(pool="thread")
    def f(x: int) -> int:
        return x * 2

    with TaskRunner() as runner:
        h = runner.submit(f, args=(21,))
        runner.run()
    assert h.result() == 42


def test_thread_backend_current_task_set() -> None:
    seen: list[str | None] = []

    @task(pool="thread")
    def f() -> None:
        ctx = current_task()
        seen.append(ctx.name if ctx is not None else None)

    with TaskRunner() as runner:
        runner.submit(f)
        runner.run()

    assert len(seen) == 1
    assert seen[0] is not None
    assert seen[0].endswith("f")


def test_process_backend_runs() -> None:
    sq = task(pool="process")(square)

    with TaskRunner() as runner:
        h = runner.submit(sq, args=(6,))
        runner.run()

    assert h.result() == 36


def test_process_backend_propagates_error() -> None:
    bad = task(pool="process", retries=0)(boom)

    with TaskRunner(on_task_error="continue") as runner:
        h = runner.submit(bad)
        runner.run()

    assert h.status == TaskStatus.FAILED
    err = h.get_result().error
    assert isinstance(err, RuntimeError)


def test_process_backend_multiple_args() -> None:
    a = task(pool="process")(add)
    with TaskRunner() as runner:
        h = runner.submit(a, args=(10, 11))
        runner.run()
    assert h.result() == 21


def test_thread_backend_concurrent() -> None:
    """Concurrent thread tasks all run."""
    f = task(pool="thread")(slow_square)

    with TaskRunner(max_workers=4) as runner:
        handles = [runner.submit(f, args=(x, 0.05)) for x in range(4)]
        runner.run()

    assert [h.result() for h in handles] == [0, 1, 4, 9]


async def test_async_backend_runs() -> None:
    @task
    async def f(x: int) -> int:
        await asyncio.sleep(0)
        return x + 100

    runner = TaskRunner()
    h = runner.submit(f, args=(1,))
    await runner.arun()
    runner.shutdown()
    assert h.result() == 101


async def test_sync_function_on_asyncio_backend() -> None:
    @task(pool="default")
    def f(x: int) -> int:
        return x + 5

    runner = TaskRunner()
    h = runner.submit(f, args=(3,))
    await runner.arun()
    runner.shutdown()
    assert h.result() == 8
