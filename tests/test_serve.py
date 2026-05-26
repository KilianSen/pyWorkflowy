from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import pytest

from pyworkflowy import TaskRunner, TaskStatus, task
from pyworkflowy.schedule import Scheduler


async def test_aserve_blocks_until_stop() -> None:
    """aserve() does not return on its own; only stop() ends it."""
    runner = TaskRunner()
    try:
        # 0.2s timeout → expect TimeoutError because aserve must keep running.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(runner.aserve(), timeout=0.2)
    finally:
        runner.shutdown()


async def test_aserve_returns_after_stop() -> None:
    """A subsequent stop() releases aserve() promptly."""
    runner = TaskRunner()

    serve_task = asyncio.create_task(runner.aserve())
    await asyncio.sleep(0.05)
    runner.stop()
    # Should return within a reasonable window — stop wakes the idle loop.
    await asyncio.wait_for(serve_task, timeout=1.0)
    runner.shutdown()


async def test_aserve_picks_up_submission_after_start() -> None:
    """A task submitted while aserve() is running gets dispatched and completes."""

    @task
    async def f(x: int) -> int:
        return x * 2

    runner = TaskRunner()
    serve_task = asyncio.create_task(runner.aserve())
    try:
        # Give aserve a tick to enter the idle wait.
        await asyncio.sleep(0.05)
        h = runner.submit(f, args=(21,))
        # Wait for completion, then verify.
        await asyncio.wait_for(asyncio.to_thread(h.wait, 2.0), timeout=2.5)
        assert h.result() == 42
    finally:
        runner.stop()
        await asyncio.wait_for(serve_task, timeout=1.0)
        runner.shutdown()


async def test_aserve_cross_thread_submit_wakes_loop() -> None:
    """A submit() from a different OS thread wakes the loop via call_soon_threadsafe."""

    @task
    async def f(x: int) -> int:
        return x + 1

    runner = TaskRunner()
    serve_task = asyncio.create_task(runner.aserve())
    handle_box: list[Any] = []

    def submit_from_thread() -> None:
        h = runner.submit(f, args=(10,))
        handle_box.append(h)

    try:
        await asyncio.sleep(0.05)
        thread = threading.Thread(target=submit_from_thread, daemon=True)
        thread.start()
        thread.join(timeout=1.0)
        assert handle_box, "submit_from_thread did not register a handle"
        h = handle_box[0]
        # The loop must pick it up and run it.
        await asyncio.wait_for(asyncio.to_thread(h.wait, 2.0), timeout=2.5)
        assert h.result() == 11
    finally:
        runner.stop()
        await asyncio.wait_for(serve_task, timeout=1.0)
        runner.shutdown()


async def test_aserve_runs_multiple_submissions() -> None:
    """Many sequential submits all complete within one aserve() session."""

    @task
    async def f(x: int) -> int:
        return x

    runner = TaskRunner()
    serve_task = asyncio.create_task(runner.aserve())
    try:
        await asyncio.sleep(0.05)
        handles = [runner.submit(f, args=(i,)) for i in range(20)]
        for h in handles:
            await asyncio.wait_for(asyncio.to_thread(h.wait, 2.0), timeout=2.5)
        assert [h.result() for h in handles] == list(range(20))
    finally:
        runner.stop()
        await asyncio.wait_for(serve_task, timeout=1.0)
        runner.shutdown()


async def test_aserve_stop_lets_in_flight_finish() -> None:
    """Tasks running when stop() fires complete before aserve() returns."""
    finished = asyncio.Event()

    @task
    async def slow() -> int:
        await asyncio.sleep(0.2)
        finished.set()
        return 1

    runner = TaskRunner()
    serve_task = asyncio.create_task(runner.aserve())
    try:
        await asyncio.sleep(0.05)
        h = runner.submit(slow)
        # Give it a moment to actually start running.
        await asyncio.sleep(0.05)
        runner.stop()
        await asyncio.wait_for(serve_task, timeout=2.0)
    finally:
        runner.shutdown()
    assert finished.is_set(), "in-flight task did not get to finish before aserve returned"
    assert h.result() == 1
    assert h.status == TaskStatus.COMPLETED


async def test_aserve_swallows_individual_task_failures() -> None:
    """A failing task does not bring down the serve loop."""

    @task
    async def good() -> int:
        return 1

    @task
    async def bad() -> int:
        raise RuntimeError("nope")

    runner = TaskRunner()  # default on_task_error="raise" — should still not kill serve
    serve_task = asyncio.create_task(runner.aserve())
    try:
        await asyncio.sleep(0.05)
        h_bad = runner.submit(bad)
        await asyncio.wait_for(asyncio.to_thread(h_bad.wait, 2.0), timeout=2.5)
        assert h_bad.status == TaskStatus.FAILED
        # serve_task is still alive
        assert not serve_task.done()
        # And the loop can keep dispatching.
        h_good = runner.submit(good)
        await asyncio.wait_for(asyncio.to_thread(h_good.wait, 2.0), timeout=2.5)
        assert h_good.result() == 1
    finally:
        runner.stop()
        await asyncio.wait_for(serve_task, timeout=1.0)
        runner.shutdown()


async def test_aserve_with_scheduler_every() -> None:
    """Scheduler.every() ticks during aserve() — fires multiple times."""
    fired: list[int] = []

    @task
    async def tick() -> None:
        fired.append(1)

    runner = TaskRunner()
    sched = Scheduler(runner=runner, tick_seconds=0.05)
    sched.every(0.1).do(tick)
    sched.start()
    serve_task = asyncio.create_task(runner.aserve())
    try:
        # Let several ticks happen.
        await asyncio.sleep(0.5)
        assert len(fired) >= 2, f"expected at least 2 fires, got {len(fired)}"
    finally:
        sched.stop()
        runner.stop()
        await asyncio.wait_for(serve_task, timeout=1.0)
        runner.shutdown()


async def test_aserve_with_event_source() -> None:
    """Publishing an event during aserve() submits and runs the task."""

    class Bus:
        def __init__(self) -> None:
            self._subs: dict[str, list[Callable[[Mapping[str, Any]], None]]] = {}

        def subscribe(
            self,
            event_name: str,
            handler: Callable[[Mapping[str, Any]], None],
        ) -> Callable[[], None]:
            self._subs.setdefault(event_name, []).append(handler)
            return lambda: self._subs[event_name].remove(handler)

        def publish(self, event_name: str, payload: Mapping[str, Any]) -> None:
            for h in self._subs.get(event_name, ()):
                h(payload)

    seen: list[int] = []

    @task
    async def consume(book_id: int) -> int:
        seen.append(book_id)
        return book_id

    bus = Bus()
    runner = TaskRunner()
    sched = Scheduler(runner=runner, event_source=bus)
    job = sched.on("book.added").do(consume, payload_map={"book_id": "book_id"})

    serve_task = asyncio.create_task(runner.aserve())
    try:
        await asyncio.sleep(0.05)
        bus.publish("book.added", {"book_id": 7})
        # Drive a little to let the submitted handle complete.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and not seen:
            await asyncio.sleep(0.02)
        assert seen == [7]
        assert job.last_handle is not None
        assert job.last_handle.result(timeout=1.0) == 7
    finally:
        sched.stop()
        runner.stop()
        await asyncio.wait_for(serve_task, timeout=1.0)
        runner.shutdown()


def test_serve_sync_wrapper_returns_after_stop() -> None:
    """The sync serve() wrapper exits when stop() is called from another thread."""
    runner = TaskRunner()

    def stop_after_delay() -> None:
        time.sleep(0.1)
        runner.stop()

    threading.Thread(target=stop_after_delay, daemon=True).start()
    runner.serve()
    runner.shutdown()
