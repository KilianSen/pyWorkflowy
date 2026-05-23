"""Module-level helpers for tests.

Process-pool tests require their functions to be importable (pickle-able), so
they live here rather than inside test bodies.
"""

from __future__ import annotations


def square(x: int) -> int:
    return x * x


def add(a: int, b: int) -> int:
    return a + b


def boom() -> None:
    raise RuntimeError("boom from worker")


def slow_square(x: int, sleep_s: float = 0.1) -> int:
    import time

    time.sleep(sleep_s)
    return x * x
