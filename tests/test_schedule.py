from __future__ import annotations

import time
from datetime import datetime, timedelta

import pytest

from pyworkflowy import TaskRunner, task
from pyworkflowy.schedule import Scheduler, parse_cron


def test_parse_cron_basic() -> None:
    expr = parse_cron("* * * * *")
    assert expr.minute == frozenset(range(60))
    assert expr.hour == frozenset(range(24))


def test_parse_cron_specific() -> None:
    expr = parse_cron("0 12 * * *")
    assert expr.minute == frozenset({0})
    assert expr.hour == frozenset({12})


def test_parse_cron_step() -> None:
    expr = parse_cron("*/15 * * * *")
    assert expr.minute == frozenset({0, 15, 30, 45})


def test_parse_cron_range() -> None:
    expr = parse_cron("0 9-17 * * *")
    assert expr.hour == frozenset(range(9, 18))


def test_parse_cron_list() -> None:
    expr = parse_cron("0,15,30 * * * *")
    assert expr.minute == frozenset({0, 15, 30})


def test_parse_cron_invalid_field_count() -> None:
    with pytest.raises(ValueError, match="5 fields"):
        parse_cron("* * *")


def test_parse_cron_out_of_range() -> None:
    with pytest.raises(ValueError):
        parse_cron("60 * * * *")


def test_parse_cron_bad_step() -> None:
    with pytest.raises(ValueError):
        parse_cron("*/0 * * * *")


def test_cron_matches() -> None:
    expr = parse_cron("0 12 * * *")
    assert expr.matches(datetime(2026, 1, 1, 12, 0))
    assert not expr.matches(datetime(2026, 1, 1, 12, 1))


def test_cron_next_after() -> None:
    expr = parse_cron("0 12 * * *")
    nxt = expr.next_after(datetime(2026, 1, 1, 10, 0))
    assert nxt == datetime(2026, 1, 1, 12, 0)


def test_scheduler_every_tick_fires() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, tick_seconds=0.05)
        job = sched.every(0.01).do(f)
        # Wait long enough then tick.
        time.sleep(0.05)
        fired = sched.tick()
        assert len(fired) == 1
        runner.run()
        assert fired[0].result() == 1
        # job auto-rescheduled
        assert not job.cancelled


def test_scheduler_one_shot_at() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        sched = Scheduler(runner=runner)
        sched.at(time.time() - 1).do(f)
        fired = sched.tick()
        assert len(fired) == 1
        # one-shot now cancelled, removed from jobs
        assert all(not j.one_shot or j.cancelled for j in sched.jobs())


def test_scheduler_at_datetime() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        sched = Scheduler(runner=runner)
        sched.at(datetime.fromtimestamp(time.time() - 1)).do(f)
        fired = sched.tick()
        assert len(fired) == 1


def test_scheduler_every_timedelta() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        sched = Scheduler(runner=runner)
        sched.every(timedelta(seconds=0.01)).do(f)
        time.sleep(0.05)
        fired = sched.tick()
        assert len(fired) == 1


def test_scheduler_do_rejects_callable() -> None:
    def plain() -> int:
        return 1

    with TaskRunner() as runner:
        sched = Scheduler(runner=runner)
        with pytest.raises(TypeError, match="Task"):
            sched.every(1).do(plain)  # type: ignore[arg-type]


def test_scheduler_requires_runner() -> None:
    @task
    def f() -> int:
        return 1

    sched = Scheduler()
    sched.every(0.01).do(f)
    time.sleep(0.05)
    with pytest.raises(RuntimeError, match="runner"):
        sched.tick()


def test_scheduler_with_runner_context() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        sched = Scheduler()
        sched.every(0.01).do(f)
        time.sleep(0.05)
        # Use ambient runner since runner context is active
        from pyworkflowy._runner import _bind_runner

        with _bind_runner(runner):
            fired = sched.tick()
        assert len(fired) == 1


def test_scheduler_start_stop_lifecycle() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        sched = Scheduler(runner=runner, tick_seconds=0.01)
        sched.every(0.005).do(f)
        sched.start()
        time.sleep(0.1)
        sched.stop(timeout=1.0)
        assert len(runner.handles()) >= 1


def test_scheduler_invalid_tick() -> None:
    with pytest.raises(ValueError):
        Scheduler(tick_seconds=0)


def test_scheduler_invalid_interval() -> None:
    sched = Scheduler()
    with pytest.raises(ValueError):
        sched.every(0)


def test_cancel_job() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        sched = Scheduler(runner=runner)
        job = sched.every(0.01).do(f)
        assert sched.cancel(job)
