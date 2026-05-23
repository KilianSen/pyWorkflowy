from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import pytest

from pyworkflowy import TaskRunner, TaskStatus, task
from pyworkflowy.schedule import Scheduler


class FakeEventBus:
    """Minimal pub/sub bus matching the EventSource protocol."""

    def __init__(self) -> None:
        self._subs: dict[str, list[Callable[[Mapping[str, Any]], None]]] = {}
        self._lock = threading.RLock()

    def subscribe(
        self,
        event_name: str,
        handler: Callable[[Mapping[str, Any]], None],
    ) -> Callable[[], None]:
        with self._lock:
            self._subs.setdefault(event_name, []).append(handler)

        def unsubscribe() -> None:
            with self._lock:
                subs = self._subs.get(event_name, [])
                if handler in subs:
                    subs.remove(handler)

        return unsubscribe

    def publish(self, event_name: str, payload: Mapping[str, Any]) -> None:
        with self._lock:
            subs = list(self._subs.get(event_name, ()))
        for h in subs:
            h(payload)

    def subscriber_count(self, event_name: str) -> int:
        with self._lock:
            return len(self._subs.get(event_name, ()))


def test_event_publish_submits_task() -> None:
    seen: list[int] = []

    @task
    def refresh(book_id: int) -> int:
        seen.append(book_id)
        return book_id

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus)
        sched.on("book.added").do(refresh, payload_map={"book_id": "book_id"})
        bus.publish("book.added", {"book_id": 7})
        # The submit has happened; run the runner to execute.
        runner.run()
    assert seen == [7]


def test_event_handle_source_is_event() -> None:
    @task
    def f(book_id: int) -> int:
        return book_id

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus)
        job = sched.on("x").do(f, payload_map={"book_id": "book_id"})
        bus.publish("x", {"book_id": 1})
        runner.run()
    assert job.last_handle is not None
    assert job.last_handle.source == "event"


def test_event_payload_map_extra_keys_ignored() -> None:
    @task
    def f(book_id: int) -> int:
        return book_id

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus)
        job = sched.on("x").do(f, payload_map={"book_id": "book_id"})
        bus.publish("x", {"book_id": 5, "noise": "ignored"})
        runner.run()
    assert job.last_handle is not None
    assert job.last_handle.result() == 5


def test_event_unsubscribe_on_cancel() -> None:
    @task
    def f() -> int:
        return 1

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus)
        job = sched.on("x").do(f)
        assert bus.subscriber_count("x") == 1
        assert sched.cancel(job)
        assert bus.subscriber_count("x") == 0


def test_event_unsubscribe_on_stop() -> None:
    @task
    def f() -> int:
        return 1

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus)
        sched.on("x").do(f)
        sched.on("y").do(f)
        assert bus.subscriber_count("x") == 1
        assert bus.subscriber_count("y") == 1
        sched.stop()
    assert bus.subscriber_count("x") == 0
    assert bus.subscriber_count("y") == 0


def test_event_requires_bound_source() -> None:
    @task
    def f() -> int:
        return 1

    sched = Scheduler()
    with pytest.raises(RuntimeError, match="event source"):
        sched.on("x").do(f)


def test_bind_event_source_after_construction() -> None:
    @task
    def f() -> int:
        return 1

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner)
        sched.bind_event_source(bus)
        sched.on("x").do(f)
        assert bus.subscriber_count("x") == 1
        sched.stop()


def test_bind_event_source_rejects_rebind_with_live_jobs() -> None:
    @task
    def f() -> int:
        return 1

    bus1 = FakeEventBus()
    bus2 = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus1)
        sched.on("x").do(f)
        with pytest.raises(RuntimeError, match="rebind"):
            sched.bind_event_source(bus2)


def test_bind_tasks_autowires_triggers() -> None:
    seen: list[int] = []

    @task(triggers=("book.added",), payload_map={"book_id": "book_id"})
    def refresh(book_id: int) -> int:
        seen.append(book_id)
        return book_id

    @task  # no triggers — should be silently skipped
    def standalone() -> int:
        return 1

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus)
        jobs = sched.bind_tasks(refresh, standalone)
        assert len(jobs) == 1
        assert jobs[0].event_name == "book.added"
        bus.publish("book.added", {"book_id": 42})
        runner.run()
    assert seen == [42]


def test_event_job_listed_uniformly_with_time_based() -> None:
    @task
    def f() -> int:
        return 1

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus)
        sched.every(60).do(f)
        sched.on("x").do(f)
        all_jobs = sched.jobs()
        assert len(all_jobs) == 2
        assert any(j.is_event_driven for j in all_jobs)
        assert any(not j.is_event_driven for j in all_jobs)
        sched.stop()


def test_event_tick_does_not_fire_event_jobs() -> None:
    """The tick loop only fires time-based jobs; event jobs fire from the subscriber."""
    fired: list[int] = []

    @task
    def f() -> int:
        fired.append(1)
        return 1

    bus = FakeEventBus()
    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, event_source=bus)
        sched.on("x").do(f)
        # tick() should NOT fire the event-driven job.
        handles = sched.tick()
        runner.run()
        assert handles == []
        assert fired == []
        sched.stop()


def test_event_drops_when_no_runner_active() -> None:
    """Publishing with no active runner is a no-op (silent drop)."""

    @task
    def f() -> int:
        return 1

    bus = FakeEventBus()
    sched = Scheduler(event_source=bus)
    job = sched.on("x").do(f)
    # No runner anywhere — publishing should not raise.
    bus.publish("x", {})
    assert job.last_handle is None
    sched.stop()
