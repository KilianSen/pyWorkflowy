"""Class-based task definition.

Subclass :class:`TaskBase`, override ``run``, set class-level config attributes,
and instantiate to get a :class:`Task`. The :class:`Task` instance forwards
``.submit()`` and ``.__call__()`` to the wrapped ``run`` method — so once
constructed, a class-based task is indistinguishable from one built by
:func:`@task`.

Class-based form is purely a convenience for code that prefers config-as-class
over config-as-decorator-kwargs; the underlying engine sees a normal
:class:`Task`.
"""

from __future__ import annotations

from typing import Any

from pyworkflowy._core import (
    DEFAULT_POOL_NAME,
    Backoff,
    DepFailurePolicy,
    Task,
    _build_task,
)

__all__ = ["TaskBase"]


_SENTINEL = object()


class TaskBase:
    """Base class for class-based task definitions.

    Subclass and override :meth:`run` (sync or async). Class-level attributes
    (``name``, ``pool``, ``retries``, ``timeout``, ``backoff``,
    ``backoff_base``, ``backoff_max``, ``retry_on``, ``on_dep_failure``)
    configure the task. Instantiating returns a fully configured
    :class:`pyworkflowy.Task` — the instance is *the* task, not a wrapper around
    one::

        class FetchUser(TaskBase):
            name = "fetch-user"
            pool = "thread"
            retries = 2

            def run(self, user_id: int) -> dict[str, Any]:
                ...

        fetch_user = FetchUser()
        handle = fetch_user.submit(42)
    """

    name: str | None = None
    pool: str = DEFAULT_POOL_NAME
    retries: int = 0
    timeout: float | None = None
    backoff: Backoff = "exponential"
    backoff_base: float = 1.0
    backoff_max: float = 30.0
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None = None
    on_dep_failure: DepFailurePolicy = "fail"
    max_attempts: int | None = None
    dedup_by: tuple[str, ...] = ()
    triggers: tuple[str, ...] = ()
    payload_map: dict[str, str] | None = None

    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Override me. Sync or async ``def`` both fine."""
        raise NotImplementedError(
            f"{type(self).__name__}.run is not implemented — override it in your subclass."
        )

    def __new__(cls, *args: Any, **kwargs: Any) -> Task[Any]:  # type: ignore[misc]
        if cls is TaskBase:
            raise TypeError(
                "TaskBase is abstract — subclass it and override `run` instead of "
                "instantiating it directly."
            )
        if args or kwargs:
            raise TypeError(
                f"{cls.__name__} takes no constructor arguments — pass values to "
                "`.submit(*args, **kwargs)` instead. Class-level attributes "
                "configure the task itself; runtime args go to `run`."
            )
        instance = object.__new__(cls)
        # Bind `run` so the resulting Task.fn behaves like a plain function.
        run_method = instance.run
        retries = cls.retries
        if cls.max_attempts is not None:
            if retries:
                raise ValueError(
                    f"{cls.__name__}: set either `retries` or `max_attempts`, not both"
                )
            if cls.max_attempts < 1:
                raise ValueError(
                    f"{cls.__name__}.max_attempts must be >= 1, got {cls.max_attempts}"
                )
            retries = cls.max_attempts - 1
        return _build_task(
            run_method,
            name=cls.name or f"{cls.__module__}.{cls.__qualname__}",
            pool=cls.pool,
            retries=retries,
            timeout=cls.timeout,
            backoff=cls.backoff,
            backoff_base=cls.backoff_base,
            backoff_max=cls.backoff_max,
            retry_on=cls.retry_on,
            on_dep_failure=cls.on_dep_failure,
            dedup_by=cls.dedup_by,
            triggers=cls.triggers,
            payload_map=cls.payload_map,
        )
