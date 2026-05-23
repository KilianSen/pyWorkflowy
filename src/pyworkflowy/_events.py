"""Event source integration for reactive task triggers.

pyWorkflowy doesn't ship an event bus — it integrates with whatever your
application already uses. Any object that exposes
``subscribe(event_name, handler) -> unsubscribe_callable`` is a valid
:class:`EventSource`. The scheduler's :meth:`Scheduler.bind_event_source`
plugs one in; :meth:`Scheduler.on` + :meth:`JobBuilder.do` then register
tasks to fire when the named event is published.

Typical shape on the consumer side::

    class EventBus:
        def subscribe(self, event_name, handler):
            self._subs[event_name].append(handler)
            return lambda: self._subs[event_name].remove(handler)

        def publish(self, event_name, payload):
            for handler in self._subs.get(event_name, ()):
                handler(payload)
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Protocol, runtime_checkable

__all__ = ["EventHandler", "EventSource"]


EventHandler = Callable[[Mapping[str, Any]], None]
"""A subscriber callback. Receives the event payload as a mapping."""


@runtime_checkable
class EventSource(Protocol):
    """Anything that lets a subscriber listen for named events.

    Implementations must be safe to call ``subscribe`` from any thread; the
    returned unsubscribe callable must also be safe to call at any time and
    must be idempotent.
    """

    def subscribe(
        self,
        event_name: str,
        handler: EventHandler,
    ) -> Callable[[], None]: ...
