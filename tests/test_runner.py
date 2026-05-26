from __future__ import annotations

import asyncio

import pytest

from pyworkflowy import (
    TaskRunner,
    TaskStatus,
    get_current_runner,
    task,
)


def test_basic_run() -> None:
    @task
    def f(x: int) -> int:
        return x + 1

    with TaskRunner() as runner:
        h = f.submit(args=(2,))
        results = runner.run()
    assert h.result() == 3
    assert results[f.name].ok


def test_results_map() -> None:
    @task
    def f(x: int) -> int:
        return x * 10

    with TaskRunner() as runner:
        f.submit(args=(1,))
        f.submit(args=(2,))
        # two submissions of same task — results dict keyed by name; collisions overwrite
        results = runner.run()
    assert f.name in results


def test_submit_plain_callable() -> None:
    def plain(x: int) -> int:
        return x + 5

    with TaskRunner() as runner:
        h = runner.submit(plain, args=(10,))
        runner.run()
    assert h.result() == 15


async def test_arun_in_existing_loop() -> None:
    @task
    async def f(x: int) -> int:
        await asyncio.sleep(0)
        return x * 3

    runner = TaskRunner()
    h = runner.submit(f, args=(5,))
    h2 = runner.submit(f, args=(4,))
    await runner.arun()
    runner.shutdown()
    assert h.result() == 15
    assert h2.result() == 12


def test_get_current_runner_inside_run() -> None:
    seen: list[TaskRunner | None] = []

    @task
    def f() -> None:
        seen.append(get_current_runner())

    with TaskRunner() as runner:
        f.submit()
        runner.run()
    assert seen == [runner]


def test_context_manager_shuts_down() -> None:
    runner = TaskRunner()
    with runner as r:
        assert r is runner
    # second shutdown is fine
    runner.shutdown()


def test_submit_after_shutdown_rejected() -> None:
    runner = TaskRunner()
    runner.shutdown()

    @task
    def f() -> None:
        return None

    with pytest.raises(RuntimeError, match="shut down"):
        runner.submit(f)


def test_on_task_error_log(caplog: pytest.LogCaptureFixture) -> None:
    @task
    def boom() -> int:
        raise RuntimeError("x")

    with TaskRunner(on_task_error="log") as runner:
        h = runner.submit(boom)
        with caplog.at_level("ERROR", logger="pyworkflowy"):
            runner.run()
    assert h.status == TaskStatus.FAILED
    assert "failed" in caplog.text.lower()


def test_on_task_error_raise() -> None:
    @task
    def boom() -> int:
        raise RuntimeError("nope")

    with TaskRunner(on_task_error="raise") as runner:
        runner.submit(boom)
        with pytest.raises(RuntimeError, match="nope"):
            runner.run()


def test_on_task_error_continue() -> None:
    @task
    def boom() -> int:
        raise RuntimeError("yep")

    @task
    def good() -> int:
        return 1

    with TaskRunner(on_task_error="continue") as runner:
        bad_h = runner.submit(boom)
        good_h = runner.submit(good)
        runner.run()
    assert bad_h.status == TaskStatus.FAILED
    assert good_h.status == TaskStatus.COMPLETED


def test_invalid_max_workers() -> None:
    with pytest.raises(ValueError):
        TaskRunner(max_workers=0)


def test_invalid_on_task_error() -> None:
    with pytest.raises(ValueError):
        TaskRunner(on_task_error="weird")  # type: ignore[arg-type]


def test_handles_introspection() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        runner.submit(f)
        runner.submit(f)
        handles = runner.handles()
    assert len(handles) == 2


def test_handle_repr() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        h = runner.submit(f)
        assert "TaskHandle" in repr(h)
        assert f.name in repr(h)
        runner.run()


def test_get_result_does_not_raise() -> None:
    @task
    def f() -> int:
        raise ValueError("expected")

    with TaskRunner(on_task_error="continue") as runner:
        h = runner.submit(f)
        runner.run()

    res = h.get_result()
    assert res.status == TaskStatus.FAILED
    assert isinstance(res.error, ValueError)


async def test_handle_await() -> None:
    @task
    async def f(x: int) -> int:
        await asyncio.sleep(0)
        return x + 1

    runner = TaskRunner()
    h = runner.submit(f, args=(9,))
    # arun returns when all are done
    await runner.arun()
    runner.shutdown()
    assert await h == 10


def test_result_timeout_raises() -> None:
    @task
    def f() -> int:
        return 1

    runner = TaskRunner()
    h = runner.submit(f)
    with pytest.raises(TimeoutError):
        h.result(timeout=0.05)
    # Now run
    runner.run()
    runner.shutdown()
    assert h.result() == 1
