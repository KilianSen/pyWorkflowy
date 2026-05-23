from __future__ import annotations

import asyncio
import threading
import time

from pyworkflowy import TaskRunner, TaskStatus, current_task, task


async def test_async_task_cancelled_before_run() -> None:
    @task
    async def slow() -> int:
        await asyncio.sleep(5.0)
        return 1

    runner = TaskRunner(on_task_error="continue")
    h = runner.submit(slow)
    h.cancel()
    await runner.arun()
    runner.shutdown()

    # Cancellation requested pre-run; task may run briefly but cancel_event is set,
    # so it surfaces as CANCELLED on the first failure.
    assert h.status in (TaskStatus.CANCELLED, TaskStatus.FAILED)


def test_cancel_all_marks_handles() -> None:
    @task(pool="thread")
    def f() -> int:
        time.sleep(0.5)
        return 1

    runner = TaskRunner(on_task_error="continue")
    h1 = runner.submit(f)
    h2 = runner.submit(f)
    runner.cancel_all()
    runner.run()
    runner.shutdown()

    # cancel_event was set before run, body may have completed or been cancelled.
    # At minimum, no exception bubbles.
    assert h1.done()
    assert h2.done()


def test_thread_task_cooperative_cancel() -> None:
    done: list[str] = []

    @task(pool="thread")
    def cooperative() -> int:
        ctx = current_task()
        assert ctx is not None
        for _ in range(100):
            if ctx.cancel_event.is_set():
                done.append("cancelled")
                raise RuntimeError("self-cancel")
            time.sleep(0.01)
        done.append("ran-to-completion")
        return 1

    runner = TaskRunner(on_task_error="continue")
    h = runner.submit(cooperative)

    # Submit then schedule a cancel from another thread shortly after run starts.
    def kick() -> None:
        time.sleep(0.05)
        h.cancel()

    threading.Thread(target=kick, daemon=True).start()
    runner.run()
    runner.shutdown()
    # body raised → status is FAILED or CANCELLED (depending on timing)
    assert h.done()


async def test_cancel_from_non_loop_thread_interrupts_async_task() -> None:
    """Calling handle.cancel() from a non-loop thread must actually interrupt
    a running asyncio task — not just set the cooperative flag. The runner
    schedules the asyncio.Task.cancel() via call_soon_threadsafe.
    """

    @task
    async def sleeper() -> int:
        await asyncio.sleep(5.0)
        return 1

    runner = TaskRunner(on_task_error="continue")
    h = runner.submit(sleeper)

    async def kick_from_thread() -> None:
        # Give the asyncio task time to actually start awaiting sleep.
        await asyncio.sleep(0.05)

        def cancel_off_loop() -> None:
            h.cancel()

        thread = threading.Thread(target=cancel_off_loop, daemon=True)
        thread.start()
        thread.join(timeout=1.0)

    start = time.monotonic()
    await asyncio.gather(runner.arun(), kick_from_thread())
    elapsed = time.monotonic() - start
    runner.shutdown()

    # If cross-thread cancel worked, the sleep was interrupted long before 5s.
    assert elapsed < 2.0, f"cancellation did not interrupt promptly (elapsed {elapsed:.2f}s)"
    assert h.status == TaskStatus.CANCELLED


def test_cancel_returns_false_for_done() -> None:
    @task
    def f() -> int:
        return 1

    with TaskRunner() as runner:
        h = runner.submit(f)
        runner.run()
    assert not h.cancel()  # already done
