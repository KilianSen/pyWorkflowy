"""Pluggable checkpointing: serialise runner state to resume after a crash.

Default backend is :class:`JSONCheckpointer` — JSON-only, requires args to be
JSON-serialisable. :class:`PickleCheckpointer` is the more permissive option;
implement :class:`Checkpointer` to plug in your own (SQLite, Redis, Postgres,
etc.).

State schema (v2)::

    {
        "version": 2,
        "handles": [
            {
                "id": "...",
                "name": "...",
                "status": "running",
                "args": [...],
                "kwargs": {...},
                "depends_on": ["id1", "id2"],
                "pool": "io",
                "source": "manual",
                "dedup_key": None,
                "progress": 0.42,
                "progress_message": "shelf 3/7",
                "retry_at": None,
                "value": None,
                "error": None,
                "attempts": 1,
                "started_at": 1700000000.0,
                "finished_at": None,
            },
            ...
        ]
    }

The ABC provides two surfaces:

* **Snapshot mode** (``save``/``load``) writes the entire state dictionary in
  one shot. Default backends — :class:`JSONCheckpointer`, :class:`PickleCheckpointer`
  — are snapshot-based.
* **Row-grained mode** (``save_handle``/``delete_handle``/``query``) updates a
  single handle's row. Default implementations cascade to ``save`` (load,
  mutate, write) so existing subclasses keep working. SQL-backed implementations
  should override these for efficient UPSERT/DELETE/SELECT.

The runner prefers row-grained calls for progress updates and status
transitions; consumers that only override ``save``/``load`` still work, they
just pay a whole-state-rewrite per change.
"""

from __future__ import annotations

import contextlib
import json
import os
import pickle
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from pyworkflowy.exceptions import CheckpointError

__all__ = [
    "Checkpointer",
    "JSONCheckpointer",
    "PickleCheckpointer",
    "ensure_jsonable",
]

CHECKPOINT_VERSION = 2


class Checkpointer(ABC):
    """ABC for checkpoint storage backends.

    Snapshot methods (:meth:`save`, :meth:`load`) are required; row-grained
    methods (:meth:`save_handle`, :meth:`delete_handle`, :meth:`query`) have
    default implementations that cascade through :meth:`save`/:meth:`load`,
    so a minimal subclass only needs to implement the two abstract methods.

    Backends must be safe to call from a single writer (the runner) —
    concurrent writers are not supported by default. SQL-backed implementations
    can permit concurrent reads (e.g. for a UI listing tasks) by overriding
    :meth:`query`.
    """

    @abstractmethod
    def save(self, state: dict[str, Any]) -> None: ...

    @abstractmethod
    def load(self) -> dict[str, Any] | None: ...

    def save_handle(self, entry: dict[str, Any]) -> None:
        """Persist a single handle's row.

        Default implementation: read the entire state, replace the matching
        entry by ``id`` (or append if new), write whole state. Override in
        row-grained backends with an UPSERT.
        """
        state = self.load() or {"version": CHECKPOINT_VERSION, "handles": []}
        handles: list[dict[str, Any]] = state.get("handles", [])
        target_id = entry.get("id")
        for i, h in enumerate(handles):
            if h.get("id") == target_id:
                handles[i] = entry
                break
        else:
            handles.append(entry)
        state["handles"] = handles
        self.save(state)

    def delete_handle(self, handle_id: str) -> None:
        """Drop a single handle by id. No-op if the id is not present."""
        state = self.load()
        if state is None:
            return
        handles = state.get("handles", [])
        state["handles"] = [h for h in handles if h.get("id") != handle_id]
        self.save(state)

    def query(self, **filters: Any) -> list[dict[str, Any]]:
        """Return handle entries matching the given equality filters.

        Default implementation scans the loaded state. SQL backends should
        override with a real query for pagination, ordering, and aggregates.
        Filter keys are column names (``status``, ``name``, ``pool``,
        ``source``, ...); values are compared with ``==``.
        """
        state = self.load()
        if state is None:
            return []
        results: list[dict[str, Any]] = list(state.get("handles", []))
        for k, v in filters.items():
            results = [h for h in results if h.get(k) == v]
        return results


def ensure_jsonable(value: Any, *, where: str) -> None:
    """Raise :class:`pyworkflowy.CheckpointError` if ``value`` can't survive a JSON round-trip.

    Called at submit time when a JSON checkpointer is configured — fail fast
    so users see the error at the offending submission, not when the runner
    tries to persist state and dies mid-run.
    """
    try:
        json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise CheckpointError(
            f"{where} is not JSON-serialisable ({type(value).__name__}). The "
            "JSON checkpointer requires args, kwargs, and return values to be "
            "JSON-compatible. Switch to PickleCheckpointer or a custom backend "
            "for richer types."
        ) from exc


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` via a temp file + rename, for crash safety."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".pyworkflowy-", suffix=".tmp")
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp_path, path)
    except Exception:
        # Best-effort cleanup so we don't leave a tmp file behind on failure.
        with contextlib.suppress(OSError):
            tmp_path.unlink(missing_ok=True)
        raise


class JSONCheckpointer(Checkpointer):
    """JSON-backed checkpointer. Default — no extra deps.

    Args, kwargs, and return values must round-trip through ``json.dumps``.
    On read, a missing or unreadable file returns ``None`` (treated as
    "nothing to resume").
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def save(self, state: dict[str, Any]) -> None:
        try:
            payload = json.dumps(state, default=_json_default).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise CheckpointError(
                f"Failed to serialise checkpoint to JSON: {exc}. Switch to "
                "PickleCheckpointer if you need richer types."
            ) from exc
        try:
            _atomic_write_bytes(self.path, payload)
        except OSError as exc:
            raise CheckpointError(f"Failed to write checkpoint to {self.path!s}: {exc}") from exc

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            data = self.path.read_bytes()
        except OSError as exc:
            raise CheckpointError(f"Failed to read checkpoint at {self.path!s}: {exc}") from exc
        if not data:
            return None
        try:
            return json.loads(data)
        except (TypeError, ValueError) as exc:
            raise CheckpointError(f"Checkpoint at {self.path!s} is not valid JSON: {exc}") from exc


def _json_default(o: Any) -> Any:
    # Fallback for things JSON doesn't natively handle. We keep this conservative:
    # exception → repr, anything else → raise so the user sees the error.
    if isinstance(o, BaseException):
        return repr(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON-serialisable")


class PickleCheckpointer(Checkpointer):
    """Pickle-backed checkpointer for richer state.

    Accepts anything pickle accepts — at the cost of standard pickle caveats
    (insecure load from untrusted sources, version-fragile across class
    refactors). Use this when JSON is too restrictive.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def save(self, state: dict[str, Any]) -> None:
        try:
            payload = pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
        except (pickle.PicklingError, TypeError, AttributeError) as exc:
            raise CheckpointError(
                f"Failed to pickle checkpoint state: {exc}. Some objects (open files, "
                "lambdas, local closures) cannot be pickled — use top-level "
                "functions and pickle-compatible values."
            ) from exc
        try:
            _atomic_write_bytes(self.path, payload)
        except OSError as exc:
            raise CheckpointError(f"Failed to write checkpoint to {self.path!s}: {exc}") from exc

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            data = self.path.read_bytes()
        except OSError as exc:
            raise CheckpointError(f"Failed to read checkpoint at {self.path!s}: {exc}") from exc
        if not data:
            return None
        try:
            return pickle.loads(data)
        except (pickle.UnpicklingError, EOFError, AttributeError, ImportError) as exc:
            raise CheckpointError(
                f"Checkpoint at {self.path!s} could not be unpickled: {exc}"
            ) from exc
