from __future__ import annotations

import json
from pathlib import Path

from pyworkflowy import Checkpointer, TaskRunner, TaskStatus, task


def _write_v2_state(path: Path, handles: list[dict]) -> None:
    state = {"version": 2, "handles": handles}
    path.write_text(json.dumps(state))


def test_resume_separates_orphans_from_terminal(tmp_path: Path) -> None:
    cp_path = tmp_path / "state.json"
    _write_v2_state(
        cp_path,
        [
            {
                "id": "done#1",
                "name": "step",
                "args": [1],
                "kwargs": {},
                "depends_on": [],
                "status": "completed",
                "pool": "default",
                "source": "manual",
                "dedup_key": None,
                "progress": 1.0,
                "progress_message": None,
                "retry_at": None,
                "value": 10,
                "error": None,
                "attempts": 1,
            },
            {
                "id": "orphan#1",
                "name": "step",
                "args": [2],
                "kwargs": {"x": "y"},
                "depends_on": [],
                "status": "running",
                "pool": "default",
                "source": "background",
                "dedup_key": None,
                "progress": 0.5,
                "progress_message": "halfway",
                "retry_at": None,
            },
            {
                "id": "orphan#2",
                "name": "step",
                "args": [],
                "kwargs": {},
                "depends_on": [],
                "status": "retrying",
                "pool": "default",
                "source": "manual",
                "dedup_key": None,
                "progress": 0.0,
                "progress_message": None,
                "retry_at": None,
            },
        ],
    )

    runner = TaskRunner.resume(str(cp_path))
    try:
        # The completed handle is primed; non-terminals go to orphaned.
        assert "done#1" in runner._resumed_results
        orphans = runner.orphaned
        assert len(orphans) == 2
        ids = {o["id"] for o in orphans}
        assert ids == {"orphan#1", "orphan#2"}
        # Persisted progress is preserved on the orphan entries.
        running_orphan = next(o for o in orphans if o["id"] == "orphan#1")
        assert running_orphan["progress"] == 0.5
        assert running_orphan["progress_message"] == "halfway"
    finally:
        runner.shutdown()


def test_resume_orphans_can_be_resubmitted(tmp_path: Path) -> None:
    """Caller-driven re-submit: iterate runner.orphaned and submit fresh handles."""
    cp_path = tmp_path / "state.json"
    _write_v2_state(
        cp_path,
        [
            {
                "id": "lost#1",
                "name": "f",
                "args": [42],
                "kwargs": {},
                "depends_on": [],
                "status": "running",
                "pool": "default",
                "source": "background",
                "dedup_key": None,
                "progress": 0.3,
                "progress_message": None,
                "retry_at": None,
            }
        ],
    )

    @task
    def f(x: int) -> int:
        return x * 2

    with TaskRunner.resume(str(cp_path)) as runner:
        orphans = runner.orphaned
        assert len(orphans) == 1
        # Re-submit using the caller's task and the persisted args.
        for o in orphans:
            runner.submit(f, *o["args"], **o["kwargs"], source=o["source"])
        runner.run()
        handles = runner.handles()
        completed = [h for h in handles if h.status == TaskStatus.COMPLETED]
        assert len(completed) == 1
        assert completed[0].result() == 84


def test_in_memory_row_checkpointer_save_handle_called() -> None:
    """A row-grained checkpointer sees one save_handle call per transition."""

    class RowCp(Checkpointer):
        def __init__(self) -> None:
            self.snapshot_writes = 0
            self.row_writes: list[dict] = []
            self.row_deletes: list[str] = []
            self._state: dict | None = None

        def save(self, state: dict) -> None:
            self.snapshot_writes += 1
            self._state = state

        def load(self) -> dict | None:
            return self._state

        def save_handle(self, entry: dict) -> None:
            self.row_writes.append(entry)
            # Maintain in-memory state too so resume works.
            self._state = self._state or {"version": 2, "handles": []}
            handles = self._state["handles"]
            for i, h in enumerate(handles):
                if h.get("id") == entry["id"]:
                    handles[i] = entry
                    return
            handles.append(entry)

        def delete_handle(self, handle_id: str) -> None:
            self.row_deletes.append(handle_id)

    cp = RowCp()

    @task
    def f(x: int) -> int:
        return x * 2

    with TaskRunner(checkpointer=cp, checkpoint_interval=0) as runner:
        runner.submit(f, 7)
        runner.run()

    # Terminal transition was persisted via save_handle, not via full save().
    assert len(cp.row_writes) >= 1
    final = cp.row_writes[-1]
    assert final["status"] == TaskStatus.COMPLETED.value
    assert final["value"] == 14


def test_query_filters_default_implementation(tmp_path: Path) -> None:
    """The default query() filters by equality against the loaded state."""

    @task
    def f(x: int) -> int:
        return x

    cp_path = tmp_path / "state.json"
    with TaskRunner(checkpoint_path=str(cp_path), checkpoint_interval=0) as runner:
        runner.submit(f, 1, source="manual")
        runner.submit(f, 2, source="cron")
        runner.submit(f, 3, source="background")
        runner.run()

    from pyworkflowy import JSONCheckpointer

    cp = JSONCheckpointer(str(cp_path))
    all_done = cp.query(status="completed")
    assert len(all_done) == 3
    cron_only = cp.query(source="cron")
    assert len(cron_only) == 1
    assert cron_only[0]["args"] == [2]


def test_query_returns_empty_for_missing_state(tmp_path: Path) -> None:
    from pyworkflowy import JSONCheckpointer

    cp = JSONCheckpointer(str(tmp_path / "absent.json"))
    assert cp.query(status="completed") == []
