from __future__ import annotations

import asyncio
import threading
import time

import pytest

from pyworkflowy import Pool, Task, TaskRunner, TaskStatus, task
from pyworkflowy._backends import build_pool_executor


def test_pool_requires_positive_max_workers() -> None:
    with pytest.raises(ValueError, match="max_workers"):
        Pool(name="bad", kind="thread", max_workers=0)


def test_pool_rejects_bad_kind() -> None:
    with pytest.raises(ValueError, match="kind"):
        Pool(name="x", kind="weird", max_workers=2)  # type: ignore[arg-type]


def test_pool_auto_reserved_slots() -> None:
    p = Pool(name="cpu", kind="thread", max_workers=4, reserve_for=("manual", "cron"))
    # reserve_for non-empty → reserved_slots defaults to 1
    assert p.reserved_slots == 1
    # Unprivileged sources are capped at max_workers - reserved
    assert p.unprivileged_cap() == 3
    assert not p.is_privileged("background")
    # Privileged sources can use all slots
    assert p.is_privileged("manual")
    assert p.is_privileged("cron")


def test_pool_no_reservation_when_reserve_for_empty() -> None:
    p = Pool(name="x", kind="thread", max_workers=4)
    assert p.reserved_slots == 0
    assert p.is_privileged("manual")
    assert p.is_privileged("background")
    assert p.unprivileged_cap() == 4


def test_pool_reserved_slots_must_be_less_than_max_workers() -> None:
    with pytest.raises(ValueError, match="reserved_slots"):
        Pool(name="x", kind="thread", max_workers=2, reserve_for=("m",), reserved_slots=2)


def test_default_pools_include_thread_and_process() -> None:
    runner = TaskRunner()
    try:
        assert set(runner.pools) >= {"default", "thread", "process"}
        assert runner.pools["default"].kind == "asyncio"
        assert runner.pools["thread"].kind == "thread"
        assert runner.pools["process"].kind == "process"
    finally:
        runner.shutdown()


def test_unknown_pool_raises_at_submit() -> None:
    @task(pool="nonexistent")
    def f() -> int:
        return 1

    with TaskRunner() as runner, pytest.raises(ValueError, match="no pool by that name"):
        runner.submit(f)


def test_async_on_non_asyncio_pool_raises_at_submit() -> None:
    @task(pool="thread")
    async def af() -> int:
        return 1

    with TaskRunner() as runner, pytest.raises(ValueError, match="async"):
        runner.submit(af)


def test_custom_pools_replace_defaults() -> None:
    custom = {
        "io": Pool(name="io", kind="thread", max_workers=2),
        "default": Pool(name="default", kind="asyncio", max_workers=4),
    }
    with TaskRunner(pools=custom) as runner:
        assert "thread" not in runner.pools  # not auto-added when pools= is passed
        assert "io" in runner.pools


def test_pools_dict_key_must_match_name() -> None:
    bad = {"io": Pool(name="actually-cpu", kind="thread", max_workers=2)}
    with pytest.raises(ValueError, match="does not match"):
        TaskRunner(pools=bad)


def test_two_pools_run_concurrently() -> None:
    """Two tasks on different pools execute in parallel."""
    barrier = threading.Barrier(2, timeout=2.0)

    @task(pool="io")
    def io_task() -> str:
        barrier.wait()
        return "io"

    @task(pool="cpu")
    def cpu_task() -> str:
        barrier.wait()
        return "cpu"

    pools = {
        "default": Pool(name="default", kind="asyncio", max_workers=4),
        "io": Pool(name="io", kind="thread", max_workers=2),
        "cpu": Pool(name="cpu", kind="thread", max_workers=2),
    }
    with TaskRunner(pools=pools) as runner:
        h1 = runner.submit(io_task)
        h2 = runner.submit(cpu_task)
        runner.run()
    assert h1.result() == "io"
    assert h2.result() == "cpu"


def test_reservation_blocks_background_when_pool_full() -> None:
    """A pool with reserve_for=("manual",) and a single reserved slot should
    refuse to start a background task once non-background work fills the cap.
    """
    pool = Pool(name="work", kind="thread", max_workers=2, reserve_for=("manual",))
    # reserved_slots=1 (auto), so background cap = 1.

    started = threading.Event()
    release = threading.Event()
    bg_started = threading.Event()

    @task(pool="work")
    def manual() -> None:
        started.set()
        release.wait(timeout=2.0)

    @task(pool="work")
    def background_task() -> None:
        bg_started.set()
        release.wait(timeout=2.0)

    pools = {
        "default": Pool(name="default", kind="asyncio", max_workers=4),
        "work": pool,
    }

    async def drive() -> None:
        runner = TaskRunner(pools=pools)
        # Two background tasks — only one should run at a time (cap=1 for background).
        h_bg1 = runner.submit(background_task, source="background")
        h_bg2 = runner.submit(background_task, source="background")
        # Schedule the runner. Give the first background task a moment to start.
        run_task = asyncio.create_task(runner.arun())
        await asyncio.sleep(0.1)
        # Exactly one background task should be running. The other waits.
        assert bg_started.is_set()
        assert runner._pool_running_unprivileged["work"] == 1
        assert runner._pool_running["work"] == 1
        # Release and let things drain.
        release.set()
        await run_task
        runner.shutdown()
        assert h_bg1.status == TaskStatus.COMPLETED
        assert h_bg2.status == TaskStatus.COMPLETED

    asyncio.run(drive())


def test_reservation_lets_manual_through_when_background_running() -> None:
    """A manual-source submit may consume the reserved slot even while a
    background task already holds the other slot.

    Drive deterministically: the bg and manual tasks both gate on a barrier
    that requires both to reach it before either can proceed. If reservation
    were misconfigured (e.g. manual couldn't claim the reserved slot when
    background was running), the barrier would deadlock.
    """
    pool = Pool(name="work", kind="thread", max_workers=2, reserve_for=("manual",))

    barrier = threading.Barrier(2, timeout=5.0)

    @task(pool="work")
    def bg() -> str:
        barrier.wait()
        return "bg"

    @task(pool="work")
    def manual() -> str:
        barrier.wait()
        return "manual"

    pools = {
        "default": Pool(name="default", kind="asyncio", max_workers=4),
        "work": pool,
    }

    with TaskRunner(pools=pools) as runner:
        h_bg = runner.submit(bg, source="background")
        h_manual = runner.submit(manual, source="manual")
        runner.run()
    assert h_bg.result() == "bg"
    assert h_manual.result() == "manual"


def test_build_pool_executor_rejects_asyncio_kind() -> None:
    p = Pool(name="x", kind="asyncio", max_workers=2)
    with pytest.raises(ValueError, match="does not need an executor"):
        build_pool_executor(p)


def test_source_default_is_manual() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        h = runner.submit(f)
        runner.run()
    assert h.source == "manual"


def test_task_submit_source_kwarg() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        h = f.submit(source="cron")
        runner.run()
    assert h.source == "cron"
