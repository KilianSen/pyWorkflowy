"""Pluggable checkpointing: serialise runner state to resume after a crash.

Default backend is :class:`JSONCheckpointer` — JSON-only, requires args to be
JSON-serialisable. :class:`PickleCheckpointer` is the more permissive option;
implement :class:`Checkpointer` to plug in your own (SQLite, Redis, etc.).

State shape (JSON-flavoured)::

    {
        "version": 1,
        "handles": [
            {
                "id": "...",
                "name": "...",
                "status": "completed",
                "args": [...],
                "kwargs": {...},
                "depends_on": ["id1", "id2"],
                "result": <encoded value or null>,
                "error": "...",       # repr if present
                "attempts": 1
            },
            ...
        ]
    }
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

from pytasky.exceptions import CheckpointError

__all__ = [
    "Checkpointer",
    "JSONCheckpointer",
    "PickleCheckpointer",
    "ensure_jsonable",
]

CHECKPOINT_VERSION = 1


class Checkpointer(ABC):
    """ABC for checkpoint storage backends.

    Implementations must be safe to call from a single writer (the runner) —
    concurrent writers are not supported. Reads happen at resume time, before
    the runner is active.
    """

    @abstractmethod
    def save(self, state: dict[str, Any]) -> None: ...

    @abstractmethod
    def load(self) -> dict[str, Any] | None: ...


def ensure_jsonable(value: Any, *, where: str) -> None:
    """Raise :class:`pytasky.CheckpointError` if ``value`` can't survive a JSON round-trip.

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
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".pytasky-", suffix=".tmp")
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
            raise CheckpointError(
                f"Checkpoint at {self.path!s} is not valid JSON: {exc}"
            ) from exc


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
