from __future__ import annotations

import asyncio
import time

from pyworkflowy import TaskRunner, TaskStatus, current_task, task


async def test_progress_update_visible_on_handle() -> None:
    """A running task can publish progress; the handle reflects the latest value."""

    @task
    async def worker() -> int:
        ctx = current_task()
        assert ctx is not None
        for i in range(5):
            ctx.update_progress((i + 1) / 5, f"step {i + 1}/5")
            await asyncio.sleep(0)
        return 5

    runner = TaskRunner()
    h = runner.submit(worker)
    await runner.arun()
    runner.shutdown()

    assert h.result() == 5
    assert h.progress == 1.0  # snapped to 1.0 on completion
    assert h.progress_message == "step 5/5"


async def test_progress_clamped() -> None:
    """fraction is clamped to [0, 1]."""
    captured: list[float] = []

    @task
    async def worker() -> None:
        ctx = current_task()
        assert ctx is not None
        ctx.update_progress(-1.0)
        captured.append(ctx._handle.progress)  # type: ignore[union-attr]
        ctx.update_progress(2.0)
        captured.append(ctx._handle.progress)  # type: ignore[union-attr]

    runner = TaskRunner()
    h = runner.submit(worker)
    await runner.arun()
    runner.shutdown()
    assert captured == [0.0, 1.0]
    assert h.progress == 1.0


def test_progress_thread_pool() -> None:
    """Progress also works from the thread pool (contextvar is set)."""

    @task(pool="thread")
    def worker() -> int:
        ctx = current_task()
        assert ctx is not None
        ctx.update_progress(0.5, "halfway")
        return 1

    with TaskRunner() as runner:
        h = runner.submit(worker)
        runner.run()
    assert h.result() == 1
    # Either 0.5 (if completed snapping is disabled for some reason) or 1.0.
    # The current implementation snaps to 1.0 on COMPLETED.
    assert h.progress == 1.0
    assert h.progress_message == "halfway"


async def test_progress_persist_throttled(tmp_path) -> None:
    """Rapid-fire progress updates do not write to the checkpointer on every tick."""
    import json

    cp_path = tmp_path / "state.json"

    @task
    async def worker() -> None:
        ctx = current_task()
        assert ctx is not None
        # 50 calls, no waits → most should be throttled out.
        for i in range(50):
            ctx.update_progress(i / 50)

    runner = TaskRunner(checkpoint_path=str(cp_path))
    runner.submit(worker)
    await runner.arun()
    runner.shutdown()

    # File must exist; we can't easily count writes without instrumentation,
    # but we can at least verify the terminal state is durable.
    assert cp_path.exists()
    state = json.loads(cp_path.read_text())
    entry = state["handles"][0]
    assert entry["status"] == TaskStatus.COMPLETED.value
    assert entry["progress"] == 1.0
