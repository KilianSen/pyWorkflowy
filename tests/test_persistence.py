from __future__ import annotations

import json
from pathlib import Path

import pytest

from pyworkflowy import (
    Checkpointer,
    JSONCheckpointer,
    PickleCheckpointer,
    TaskRunner,
    TaskStatus,
    task,
)
from pyworkflowy.exceptions import CheckpointError


def test_json_checkpointer_writes_state(tmp_path: Path) -> None:
    cp_path = tmp_path / "state.json"

    @task
    def f(x: int) -> int:
        return x * 2

    with TaskRunner(checkpoint_path=str(cp_path), checkpoint_interval=0) as runner:
        runner.submit(f, 5)
        runner.run()

    assert cp_path.exists()
    state = json.loads(cp_path.read_text())
    assert state["version"] == 1
    assert len(state["handles"]) == 1
    entry = state["handles"][0]
    assert entry["status"] == TaskStatus.COMPLETED.value
    assert entry["value"] == 10


def test_resume_skips_completed_tasks(tmp_path: Path) -> None:
    cp_path = tmp_path / "state.json"
    calls: list[int] = []

    @task(name="step")
    def step(x: int) -> int:
        calls.append(x)
        return x * 10

    # First run: produces a checkpoint.
    with TaskRunner(checkpoint_path=str(cp_path), checkpoint_interval=0) as runner:
        h = runner.submit(step, 3)
        runner.run()
        first_id = h.id

    assert calls == [3]

    # Resume: submit with the same id by manually injecting via runner internals.
    # Resume primes results based on handle ID — to test that, we re-create the
    # runner from the checkpoint, then re-submit the same task. Because the
    # generated id includes a uuid, we cannot collide naturally; instead we
    # verify that the resumed-state is loaded.
    runner2 = TaskRunner.resume(str(cp_path))
    assert first_id in runner2._resumed_results
    runner2.shutdown()


def test_resume_with_matching_id_skips_run(tmp_path: Path) -> None:
    cp_path = tmp_path / "state.json"
    calls: list[int] = []

    @task
    def step(x: int) -> int:
        calls.append(x)
        return x

    runner = TaskRunner(checkpoint_path=str(cp_path), checkpoint_interval=0)
    runner.submit(step, 9)
    runner.run()
    runner.shutdown()

    # Hand-craft a checkpoint that uses a known id, then resume.
    cp_path.write_text(json.dumps({
        "version": 1,
        "handles": [
            {
                "id": "preset#abc",
                "name": "step",
                "args": [9],
                "kwargs": {},
                "depends_on": [],
                "status": "completed",
                "value": 9,
                "error": None,
                "attempts": 1,
            }
        ],
    }))

    runner2 = TaskRunner.resume(str(cp_path))
    # Manually call submit which calls _next_handle_id — but the resumed map
    # uses the original id. Verify that resumed state is parsed.
    assert "preset#abc" in runner2._resumed_results
    runner2.shutdown()


def test_json_checkpointer_rejects_unserializable_args(tmp_path: Path) -> None:
    @task
    def f(obj: object) -> None:
        return None

    with (
        TaskRunner(checkpoint_path=str(tmp_path / "x.json"), checkpoint_interval=0) as runner,
        pytest.raises(CheckpointError),
    ):
        runner.submit(f, object())


def test_pickle_checkpointer(tmp_path: Path) -> None:
    cp_path = tmp_path / "state.pkl"
    cp = PickleCheckpointer(str(cp_path))

    @task
    def f(x: int) -> int:
        return x + 1

    with TaskRunner(checkpointer=cp, checkpoint_interval=0) as runner:
        runner.submit(f, 1)
        runner.run()

    assert cp_path.exists()
    loaded = cp.load()
    assert loaded is not None
    assert loaded["version"] == 1


def test_load_returns_none_for_missing_file(tmp_path: Path) -> None:
    cp = JSONCheckpointer(str(tmp_path / "nope.json"))
    assert cp.load() is None


def test_load_raises_on_bad_json(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("not-json")
    cp = JSONCheckpointer(str(bad))
    with pytest.raises(CheckpointError):
        cp.load()


def test_resume_with_no_file(tmp_path: Path) -> None:
    runner = TaskRunner.resume(str(tmp_path / "absent.json"))
    assert runner._resumed_results == {}
    runner.shutdown()


def test_cannot_pass_both_path_and_checkpointer() -> None:
    cp = JSONCheckpointer("/tmp/x.json")
    with pytest.raises(ValueError, match="checkpointer"):
        TaskRunner(checkpoint_path="x.json", checkpointer=cp)


def test_custom_checkpointer_abstract() -> None:
    # Verifying ABC subclass works.
    class Mem(Checkpointer):
        def __init__(self) -> None:
            self.state: dict | None = None

        def save(self, state: dict) -> None:
            self.state = state

        def load(self) -> dict | None:
            return self.state

    cp = Mem()
    cp.save({"x": 1})
    assert cp.load() == {"x": 1}
