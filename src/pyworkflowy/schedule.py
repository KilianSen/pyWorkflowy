"""Cron-like scheduling and reactive event-triggered tasks for pyWorkflowy.

Four scheduling forms — all produced via :class:`JobBuilder.do`:

* :meth:`Scheduler.every(seconds).do(task, ...)` — fixed-interval, anchored to
  ``start()`` time. The first fire is one interval after start.
* :meth:`Scheduler.cron("m h dom mon dow").do(task, ...)` — 5-field cron with
  ``*``, ``*/N``, ranges (``a-b``), and lists (``a,b,c``). No seconds field, no
  ``@hourly`` aliases — keep it minimal.
* :meth:`Scheduler.at(datetime).do(task, ...)` — one-shot at an absolute time.
* :meth:`Scheduler.on(event_name).do(task, payload_map=...)` — fire whenever
  the bound :class:`~pyworkflowy._events.EventSource` publishes the named
  event. Requires :meth:`Scheduler.bind_event_source` first.

Missed time-based fires while the scheduler was stopped are *not* backfilled.
Event-driven jobs only fire while the scheduler is running *and* an event
source is bound. Each call to ``do(task, *args, **kwargs)`` registers a new
job; the same task can be scheduled multiple times with different args.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from pyworkflowy._core import Task, TaskHandle
from pyworkflowy._events import EventHandler, EventSource
from pyworkflowy._runner import TaskRunner, get_current_runner

__all__ = [
    "CronExpression",
    "JobBuilder",
    "ScheduledJob",
    "Scheduler",
    "parse_cron",
]


# ---------- cron parsing ----------


@dataclass(frozen=True, slots=True)
class CronExpression:
    """Parsed 5-field cron expression.

    Each field is a frozenset of valid integer values. ``next_after(dt)``
    returns the next :class:`datetime` strictly greater than ``dt`` that
    matches the expression — used by the scheduler to compute fire times.
    """

    minute: frozenset[int]
    hour: frozenset[int]
    day_of_month: frozenset[int]
    month: frozenset[int]
    day_of_week: frozenset[int]
    source: str

    def matches(self, dt: datetime) -> bool:
        # day_of_week: cron-style 0-6 (Sunday=0). datetime.weekday is Monday=0,
        # so add 1 mod 7.
        dow = (dt.weekday() + 1) % 7
        return (
            dt.minute in self.minute
            and dt.hour in self.hour
            and dt.day in self.day_of_month
            and dt.month in self.month
            and dow in self.day_of_week
        )

    def next_after(self, dt: datetime) -> datetime:
        # Step minute-by-minute. The search space is bounded (a year of minutes
        # at most), so the linear scan stays trivially correct and small.
        candidate = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(366 * 24 * 60):
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        raise ValueError(  # pragma: no cover - implies a malformed expression
            f"Could not find next fire time within a year for cron {self.source!r}"
        )


_FIELD_RANGES: dict[str, tuple[int, int]] = {
    "minute": (0, 59),
    "hour": (0, 23),
    "day_of_month": (1, 31),
    "month": (1, 12),
    "day_of_week": (0, 6),
}


def _parse_field(spec: str, field_name: str) -> frozenset[int]:
    lo, hi = _FIELD_RANGES[field_name]
    full = set(range(lo, hi + 1))
    if spec == "*":
        return frozenset(full)

    result: set[int] = set()
    for part in spec.split(","):
        if "/" in part:
            base, step_str = part.split("/", 1)
            try:
                step = int(step_str)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid step value in cron field {field_name}={spec!r}: {part!r}"
                ) from exc
            if step <= 0:
                raise ValueError(f"Step in cron field {field_name}={spec!r} must be > 0")
            if base == "*":
                values: Iterable[int] = sorted(full)
            elif "-" in base:
                values = _parse_range(base, lo, hi, field_name, spec)
            else:
                try:
                    start = int(base)
                except ValueError as exc:
                    raise ValueError(
                        f"Invalid base in cron field {field_name}={spec!r}: {base!r}"
                    ) from exc
                values = range(start, hi + 1)
            for i, v in enumerate(values):
                if i % step == 0:
                    result.add(v)
        elif "-" in part:
            result.update(_parse_range(part, lo, hi, field_name, spec))
        else:
            try:
                v = int(part)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid value in cron field {field_name}={spec!r}: {part!r}"
                ) from exc
            if not (lo <= v <= hi):
                raise ValueError(
                    f"Value {v} out of range [{lo}, {hi}] in cron field {field_name}={spec!r}"
                )
            result.add(v)
    if not result:
        raise ValueError(f"Cron field {field_name}={spec!r} yielded no values")
    return frozenset(result)


def _parse_range(part: str, lo: int, hi: int, field_name: str, spec: str) -> list[int]:
    a_str, b_str = part.split("-", 1)
    try:
        a, b = int(a_str), int(b_str)
    except ValueError as exc:
        raise ValueError(f"Invalid range in cron field {field_name}={spec!r}: {part!r}") from exc
    if a > b or not (lo <= a <= hi) or not (lo <= b <= hi):
        raise ValueError(f"Range {part!r} out of bounds in cron field {field_name}={spec!r}")
    return list(range(a, b + 1))


def parse_cron(expression: str) -> CronExpression:
    """Parse a 5-field cron string.

    Supported syntax per field: ``*``, ``N``, ``a-b``, ``*/N``, ``a-b/N``,
    and comma-joined lists of any of the above. Fields are
    ``minute hour day-of-month month day-of-week`` — no seconds field, no
    ``@hourly``/``@daily`` aliases. Day-of-week uses cron's ``0=Sunday`` (..6=Saturday).
    """
    parts = expression.strip().split()
    if len(parts) != 5:
        raise ValueError(
            "Cron expression must have 5 fields (m h dom mon dow); "
            f"got {len(parts)}: {expression!r}"
        )
    minute, hour, dom, mon, dow = parts
    return CronExpression(
        minute=_parse_field(minute, "minute"),
        hour=_parse_field(hour, "hour"),
        day_of_month=_parse_field(dom, "day_of_month"),
        month=_parse_field(mon, "month"),
        day_of_week=_parse_field(dow, "day_of_week"),
        source=expression,
    )


# ---------- jobs ----------


@dataclass(slots=True)
class ScheduledJob:
    """One registered scheduling rule. Created internally by :class:`JobBuilder`.

    Holds enough state for cron/interval/one-shot/event triggers; only one
    of (``interval``, ``cron``, ``one_shot``, ``event_name``) is meaningful
    per job. Event-driven jobs hold an ``_unsubscribe`` callable returned by
    the bound :class:`EventSource` so :meth:`Scheduler.cancel` and
    :meth:`Scheduler.stop` can tear the subscription down cleanly.
    """

    task: Task[Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    next_fire: float
    interval: float | None = None  # seconds; for every()
    cron: CronExpression | None = None
    one_shot: bool = False
    event_name: str | None = None
    payload_map: tuple[tuple[str, str], ...] | None = None
    last_handle: TaskHandle[Any] | None = None
    cancelled: bool = field(default=False)
    _unsubscribe: Callable[[], None] | None = field(default=None, repr=False)

    @property
    def is_event_driven(self) -> bool:
        """True iff this job fires from an :class:`EventSource`, not on a clock."""
        return self.event_name is not None

    def schedule_next(self, now: float) -> None:
        if self.one_shot:
            self.cancelled = True
            return
        if self.is_event_driven:
            # Event-driven jobs never reschedule themselves; they fire from the
            # subscriber callback in :meth:`Scheduler._register_event_job`.
            return
        if self.interval is not None:
            # Advance from the prior fire time, not from now, so jitter doesn't
            # compound. If we're way behind (paused process), skip ahead.
            self.next_fire += self.interval
            if self.next_fire < now:
                self.next_fire = now + self.interval
            return
        if self.cron is not None:
            current_dt = datetime.fromtimestamp(now)
            nxt = self.cron.next_after(current_dt)
            self.next_fire = nxt.timestamp()


class JobBuilder:
    """Returned by :meth:`Scheduler.every`/``.cron``/``.at``/``.on`` to receive ``.do(task)``.

    Splitting builder from registration lets the trigger keyword (``every``,
    ``cron``, ``at``, ``on``) read naturally without forcing positional args.
    """

    __slots__ = ("_cron", "_event_name", "_interval", "_one_shot", "_scheduler", "_when")

    def __init__(
        self,
        scheduler: Scheduler,
        *,
        interval: float | None = None,
        cron: CronExpression | None = None,
        when: float | None = None,
        event_name: str | None = None,
    ) -> None:
        self._scheduler = scheduler
        self._interval = interval
        self._cron = cron
        self._when = when
        self._one_shot = when is not None
        self._event_name = event_name

    def do(
        self,
        task: Task[Any],
        *args: Any,
        payload_map: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> ScheduledJob:
        if not isinstance(task, Task):
            raise TypeError(
                f"Scheduler.do() expects a Task; got {type(task).__name__}. Wrap your "
                "callable with @task first."
            )
        if self._event_name is not None:
            pm = tuple(sorted(payload_map.items())) if payload_map else ()
            job = ScheduledJob(
                task=task,
                args=args,
                kwargs=kwargs,
                next_fire=float("inf"),
                event_name=self._event_name,
                payload_map=pm,
            )
            self._scheduler._register_event_job(job)
            return job
        if payload_map is not None:
            raise ValueError(
                "payload_map= is only valid for event-driven jobs created via "
                "Scheduler.on(event_name).do(...)."
            )
        now = time.time()
        if self._when is not None:
            next_fire = self._when
        elif self._interval is not None:
            next_fire = now + self._interval
        else:
            assert self._cron is not None
            next_fire = self._cron.next_after(datetime.fromtimestamp(now)).timestamp()
        job = ScheduledJob(
            task=task,
            args=args,
            kwargs=kwargs,
            next_fire=next_fire,
            interval=self._interval,
            cron=self._cron,
            one_shot=self._one_shot,
        )
        self._scheduler._add_job(job)
        return job


# ---------- scheduler ----------


class Scheduler:
    """Background job scheduler that submits tasks to a runner on a schedule.

    The scheduler owns its loop — once :meth:`start` is called (or
    ``await arun()`` for async use) it polls for due jobs every
    ``tick_seconds`` (default 0.5s) and submits them to the active runner.

    Missed jobs while the scheduler was stopped are *not* backfilled. If you
    need at-least-once semantics, layer your own deduplicating logic on top.
    """

    __slots__ = (
        "_clock",
        "_event_source",
        "_jobs",
        "_lock",
        "_runner",
        "_started",
        "_stop_event",
        "_thread",
        "_tick_seconds",
    )

    def __init__(
        self,
        *,
        runner: TaskRunner | None = None,
        tick_seconds: float = 0.5,
        clock: Callable[[], float] | None = None,
        event_source: EventSource | None = None,
    ) -> None:
        if tick_seconds <= 0:
            raise ValueError(f"tick_seconds must be > 0, got {tick_seconds}")
        self._runner = runner
        self._tick_seconds = tick_seconds
        self._lock = threading.RLock()
        self._jobs: list[ScheduledJob] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._clock = clock or time.time
        self._event_source: EventSource | None = event_source

    # ---------- registration API ----------

    def every(self, interval: float | timedelta) -> JobBuilder:
        seconds = interval.total_seconds() if isinstance(interval, timedelta) else float(interval)
        if seconds <= 0:
            raise ValueError(f"every() interval must be > 0, got {seconds}")
        return JobBuilder(self, interval=seconds)

    def cron(self, expression: str) -> JobBuilder:
        return JobBuilder(self, cron=parse_cron(expression))

    def at(self, when: datetime | float) -> JobBuilder:
        ts = when.timestamp() if isinstance(when, datetime) else float(when)
        return JobBuilder(self, when=ts)

    def on(self, event_name: str) -> JobBuilder:
        """Register a task to fire when ``event_name`` is published on the bound source.

        Requires :meth:`bind_event_source` first (or ``event_source=`` to the
        constructor). The follow-up ``.do(task, payload_map={...})`` accepts a
        mapping from event-payload keys to task kwarg names.
        """
        return JobBuilder(self, event_name=event_name)

    def bind_event_source(self, source: EventSource) -> None:
        """Plug an :class:`EventSource` into the scheduler.

        Must be called before :meth:`on` is used for any job. Rebinding when
        event-driven jobs already exist is rejected — tear them down first
        with :meth:`cancel`.
        """
        with self._lock:
            if any(j.is_event_driven and not j.cancelled for j in self._jobs):
                raise RuntimeError(
                    "Cannot rebind event source while event-driven jobs exist. "
                    "Cancel them first."
                )
            self._event_source = source

    def bind_tasks(self, *tasks: Task[Any]) -> list[ScheduledJob]:
        """Auto-register any task whose ``triggers`` metadata is non-empty.

        Reads each task's ``triggers`` and ``payload_map`` and wires it via
        :meth:`on`. Returns the list of jobs that were created. Tasks without
        triggers are silently ignored so this can be called on a heterogeneous
        list.
        """
        registered: list[ScheduledJob] = []
        for t in tasks:
            for event_name in t.triggers:
                payload_map = dict(t.payload_map) if t.payload_map else None
                registered.append(self.on(event_name).do(t, payload_map=payload_map))
        return registered

    def jobs(self) -> list[ScheduledJob]:
        with self._lock:
            return list(self._jobs)

    def cancel(self, job: ScheduledJob) -> bool:
        with self._lock:
            if job not in self._jobs:
                return False
            job.cancelled = True
        # Unsubscribe outside the lock — the source may take its own lock.
        unsub = job._unsubscribe
        if unsub is not None:
            try:
                unsub()
            except Exception:
                import logging

                logging.getLogger("pyworkflowy").exception(
                    "pyworkflowy: event unsubscribe raised; continuing"
                )
            job._unsubscribe = None
        return True

    def _add_job(self, job: ScheduledJob) -> None:
        with self._lock:
            self._jobs.append(job)

    def _register_event_job(self, job: ScheduledJob) -> None:
        """Subscribe ``job`` to the bound event source and append it to the registry."""
        if self._event_source is None:
            raise RuntimeError(
                f"Scheduler.on({job.event_name!r}) requires an event source. "
                "Call bind_event_source(source) before registering event jobs."
            )
        event_name = job.event_name
        assert event_name is not None
        payload_map = job.payload_map or ()

        def handler(payload: Mapping[str, Any]) -> None:
            if job.cancelled:
                return
            mapped = dict(job.kwargs)
            for event_key, kwarg_name in payload_map:
                if event_key in payload:
                    mapped[kwarg_name] = payload[event_key]
            runner = self._runner or get_current_runner()
            if runner is None:
                # No active runner — drop the event silently (mirrors how the
                # reference queue handles publish-before-startup).
                return
            try:
                handle = runner.submit(job.task, *job.args, source="event", **mapped)
            except Exception:
                import logging

                logging.getLogger("pyworkflowy").exception(
                    "pyworkflowy: event-triggered submit failed; dropping event"
                )
                return
            job.last_handle = handle

        unsub = self._event_source.subscribe(event_name, handler)
        job._unsubscribe = unsub
        with self._lock:
            self._jobs.append(job)

    # ---------- run / stop ----------

    def start(self) -> None:
        """Spin up the scheduler on a background thread. Returns immediately.

        Use :meth:`stop` to halt. Re-calling :meth:`start` is a no-op while
        already running.
        """
        with self._lock:
            if self._started:
                return
            self._started = True
            self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="pyworkflowy-scheduler", daemon=True
        )
        self._thread.start()

    def stop(self, *, wait: bool = True, timeout: float | None = None) -> None:
        with self._lock:
            self._started = False
            jobs = list(self._jobs)
        # Unsubscribe all event-driven jobs so a stopped scheduler doesn't keep
        # processing events. Time-based jobs need no teardown beyond the loop.
        for j in jobs:
            if j._unsubscribe is not None:
                try:
                    j._unsubscribe()
                except Exception:
                    import logging

                    logging.getLogger("pyworkflowy").exception(
                        "pyworkflowy: event unsubscribe raised on stop; continuing"
                    )
                j._unsubscribe = None
        self._stop_event.set()
        if wait and self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def tick(self) -> list[TaskHandle[Any]]:
        """Manually fire all due time-based jobs once. Returns the handles created.

        Event-driven jobs fire from their subscriber callback, not on the
        tick — they are skipped here. Useful for tests that drive the
        scheduler synchronously by advancing a fake clock and calling
        ``tick()`` instead of running the loop.
        """
        now = self._clock()
        runner = self._runner or get_current_runner()
        if runner is None:
            raise RuntimeError(
                "Scheduler has no runner — pass `runner=` to the constructor or "
                "call tick()/start() from inside `with TaskRunner() as runner:`."
            )
        fired: list[TaskHandle[Any]] = []
        with self._lock:
            jobs = [
                j
                for j in self._jobs
                if not j.cancelled and not j.is_event_driven and j.next_fire <= now
            ]
        for job in jobs:
            # Distinguish cron-fired jobs from manual submits via source="cron".
            source = "cron" if job.cron is not None else "manual"
            handle = runner.submit(job.task, *job.args, source=source, **job.kwargs)
            job.last_handle = handle
            job.schedule_next(now)
            fired.append(handle)
        with self._lock:
            # Drop cancelled jobs; unsubscribe any with a live subscription.
            keep: list[ScheduledJob] = []
            for j in self._jobs:
                if j.cancelled:
                    if j._unsubscribe is not None:
                        try:
                            j._unsubscribe()
                        except Exception:
                            import logging

                            logging.getLogger("pyworkflowy").exception(
                                "pyworkflowy: event unsubscribe raised; continuing"
                            )
                        j._unsubscribe = None
                    continue
                keep.append(j)
            self._jobs = keep
        return fired

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception:
                # The scheduler thread must not die on a transient error —
                # log and continue. Tasks that *fired* surface failures via
                # their handles; the scheduler itself must keep ticking.
                import logging

                logging.getLogger("pyworkflowy").exception(
                    "pyworkflowy: scheduler tick raised; continuing"
                )
            self._stop_event.wait(self._tick_seconds)

    async def arun(self) -> None:
        """Run the scheduler on the current event loop until stopped.

        Returns when :meth:`stop` is called from another task. Compared to
        :meth:`start`, this version is cooperative — no background thread.
        """
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception:
                import logging

                logging.getLogger("pyworkflowy").exception(
                    "pyworkflowy: scheduler tick raised; continuing"
                )
            try:
                await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(
                        None, self._stop_event.wait, self._tick_seconds
                    ),
                    timeout=self._tick_seconds + 1.0,
                )
            except TimeoutError:
                continue
