"""DAG bookkeeping: cycle detection and topological readiness for the runner.

The runner does not pre-compute an order — instead, each handle tracks its
``depends_on`` edges, and the runner asks the DAG "is this ready?" whenever
a dep completes. This module owns the cycle check and the readiness query.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from pyworkflowy.exceptions import CycleError

__all__ = ["check_no_cycle", "find_cycle", "topo_order"]


def find_cycle(
    new_id: str,
    new_deps: Iterable[str],
    existing_deps: Mapping[str, Iterable[str]],
) -> list[str] | None:
    """Return a cycle path including ``new_id`` if adding ``new_deps`` would create one.

    Returns the cycle as a list of node ids in the order they form the cycle,
    or ``None`` if the resulting graph stays acyclic. Used at ``submit()``
    time so the offending submission, not a later one, is the error site.
    """
    # Build the graph as it would be after the new node is inserted.
    graph: dict[str, list[str]] = {k: list(v) for k, v in existing_deps.items()}
    graph[new_id] = list(new_deps)

    # Iterative DFS with a path stack so we can recover the cycle members.
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {n: WHITE for n in graph}
    parent: dict[str, str | None] = {n: None for n in graph}

    for start in graph:
        if color[start] != WHITE:
            continue
        stack: list[tuple[str, int]] = [(start, 0)]
        color[start] = GRAY
        while stack:
            node, idx = stack[-1]
            neighbors = graph.get(node, ())
            if idx >= len(neighbors):
                color[node] = BLACK
                stack.pop()
                continue
            stack[-1] = (node, idx + 1)
            nxt = neighbors[idx]
            if nxt not in color:  # dep referencing an unknown id — ignore for cycle check
                continue
            if color[nxt] == GRAY:
                # Found cycle — walk parent chain from nxt back to nxt.
                cycle: list[str] = [nxt]
                cur: str | None = node
                while cur is not None and cur != nxt:
                    cycle.append(cur)
                    cur = parent[cur]
                cycle.append(nxt)
                cycle.reverse()
                return cycle
            if color[nxt] == WHITE:
                color[nxt] = GRAY
                parent[nxt] = node
                stack.append((nxt, 0))
    return None


def check_no_cycle(
    new_id: str,
    new_deps: Iterable[str],
    existing_deps: Mapping[str, Iterable[str]],
    *,
    name_lookup: Mapping[str, str] | None = None,
) -> None:
    """Raise :class:`pyworkflowy.CycleError` if inserting ``new_id`` would create a cycle.

    Names from ``name_lookup`` (id → task-name) are used in the error message
    when available so the cycle path is human-readable.
    """
    cycle = find_cycle(new_id, new_deps, existing_deps)
    if cycle is None:
        return
    if name_lookup is not None:
        labels = [f"{name_lookup.get(node, node)}({node})" for node in cycle]
    else:
        labels = list(cycle)
    raise CycleError("Adding this task would create a dependency cycle: " + " -> ".join(labels))


def topo_order(deps: Mapping[str, Iterable[str]]) -> list[str]:
    """Return a topological ordering of ``deps`` (Kahn's algorithm).

    Raises :class:`pyworkflowy.CycleError` if the graph is not a DAG. Used as a
    defensive check at run start, after the per-submit checks have already
    rejected cycles individually.
    """
    indegree: dict[str, int] = dict.fromkeys(deps, 0)
    children: dict[str, list[str]] = {n: [] for n in deps}
    for node, ds in deps.items():
        for d in ds:
            if d not in indegree:
                continue
            indegree[node] += 1
            children[d].append(node)

    ready = [n for n, deg in indegree.items() if deg == 0]
    order: list[str] = []
    while ready:
        n = ready.pop(0)
        order.append(n)
        for c in children[n]:
            indegree[c] -= 1
            if indegree[c] == 0:
                ready.append(c)
    if len(order) != len(deps):
        leftover = [n for n in deps if n not in order]
        raise CycleError("Dependency graph has a cycle among: " + ", ".join(sorted(leftover)))
    return order
