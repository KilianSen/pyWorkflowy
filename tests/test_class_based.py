from __future__ import annotations

import asyncio

import pytest

from pyworkflowy import Task, TaskBase, TaskRunner, TaskStatus


def test_subclass_produces_task() -> None:
    class Squared(TaskBase):
        name = "squared"

        def run(self, x: int) -> int:
            return x * x

    t = Squared()
    assert isinstance(t, Task)
    assert t.name == "squared"
    assert t(7) == 49


def test_class_attributes_carry_over() -> None:
    class Slow(TaskBase):
        name = "slow"
        pool = "thread"
        retries = 3
        timeout = 5.0

        def run(self) -> int:
            return 1

    t = Slow()
    assert t.pool == "thread"
    assert t.retries == 3
    assert t.timeout == 5.0


def test_async_run_detected() -> None:
    class AsyncOne(TaskBase):
        name = "async-one"

        async def run(self) -> int:
            await asyncio.sleep(0)
            return 1

    t = AsyncOne()
    assert t.is_async


def test_taskbase_cannot_be_instantiated_directly() -> None:
    with pytest.raises(TypeError):
        TaskBase()  # type: ignore[abstract]


def test_constructor_args_rejected() -> None:
    class Foo(TaskBase):
        def run(self) -> int:
            return 1

    with pytest.raises(TypeError, match="no constructor"):
        Foo(1, 2)


def test_class_based_runs_through_runner() -> None:
    class Adder(TaskBase):
        name = "adder"

        def run(self, a: int, b: int) -> int:
            return a + b

    add = Adder()
    with TaskRunner() as runner:
        h = add.submit(3, 4)
        runner.run()
    assert h.result() == 7
    assert h.status == TaskStatus.COMPLETED


def test_class_max_attempts() -> None:
    class Foo(TaskBase):
        max_attempts = 3

        def run(self) -> int:
            return 1

    t = Foo()
    assert t.retries == 2


def test_class_retries_max_attempts_conflict() -> None:
    class Foo(TaskBase):
        retries = 1
        max_attempts = 3

        def run(self) -> int:
            return 1

    with pytest.raises(ValueError):
        Foo()
