# pyWorkflowy

A full workflow engine for async/parallelized Python tasks. Tasks, DAGs, retries, timeouts, three execution backends, persistence/resume, and a cron-like scheduler — all in one library with zero runtime dependencies.

## Install

```bash
pip install pyworkflowy
```
or
```bash
uv add pyworkflowy
```

Core has no runtime dependencies. Process-pool support, threading, and asyncio are all stdlib.

## Tasks

### Decorator form

`@task` is the simplest entry point. Bare or parameterised, exactly like `@hook` in pyHooky:

```python
from pyworkflowy import task, TaskRunner

@task
def square(x: int) -> int:
    return x * x

@task(name="checkout", retries=3, timeout=10.0, backend="thread")
def checkout(cart_id: int) -> dict:
    ...
```

The wrapped object is a `Task` instance. Use `.submit(...)` to enqueue it on the active runner; calling it directly bypasses the runner (useful in unit tests):

```python
with TaskRunner() as runner:
    handle = square.submit(args=(5,))
    runner.run()
    assert handle.result() == 25

# direct call — no runner involved
assert square(5) == 25
```

The task name is auto-derived from `module.qualname` if you don't pass one. Lambdas and other anonymous functions get an `id()`-suffixed name so multiple ones in the same scope don't collide.

### Class form

When configuration-as-class reads better than configuration-as-kwargs, subclass `TaskBase`:

```python
from pyworkflowy import TaskBase

class FetchUser(TaskBase):
    name = "fetch-user"
    backend = "thread"
    retries = 2
    timeout = 5.0

    def run(self, user_id: int) -> dict[str, Any]:
        return http.get(f"/users/{user_id}").json()

fetch_user = FetchUser()
handle = fetch_user.submit(args=(42,))
```

Instantiating `FetchUser()` returns a `Task` — the same type the decorator produces. `run` may be `def` or `async def`; pyWorkflowy auto-detects.

> `TaskBase` constructors take no arguments — class-level attributes configure the *task*; runtime values are passed to `submit(args=..., payload=...)` and forwarded to `run` (positionally and as keyword arguments respectively).

### `max_attempts` vs `retries`

`max_attempts=3` means "up to 3 attempts including the first" — sugar for `retries=2`. Pass whichever framing reads better; passing both raises `ValueError`.

## Backends

| Backend     | Constraint                                  | Cancellation               |
|-------------|---------------------------------------------|----------------------------|
| `asyncio`   | sync or async tasks                         | cooperative (CancelledError) |
| `thread`    | sync tasks only                             | cooperative (cancel flag)  |
| `process`   | sync tasks; top-level/picklable functions   | best-effort (future.cancel) |

```python
@task(backend="thread")
def cpu_bound(x): ...

@task(backend="process")
def heavy(x): ...
```

The runner's `backend=` is the default for tasks that don't override it. The asyncio loop always orchestrates — `runner.run()` is just `asyncio.run(runner.arun())` — so picking `thread`/`process` as the runner default just changes the default for plain-callable submissions.

> **Async tasks on non-asyncio backends are rejected** at decoration time. Async needs the event loop; the thread/process pools can't run coroutines without one.

## DAG / dependencies

Pass `depends_on=[other_handle, ...]` when submitting. The runner topologically orders execution and gates each task on its deps reaching `COMPLETED`:

```python
with TaskRunner() as runner:
    h_load = load_csv.submit(args=("data.csv",))
    h_clean = clean_rows.submit(depends_on=[h_load])
    h_write = write_db.submit(depends_on=[h_clean])
    runner.run()
```

Cycles are detected eagerly: `runner.submit(t, depends_on=[h])` raises `CycleError` immediately if adding the edge would close a cycle.

### On-dependency-failure policies

Per-task — set via `@task(on_dep_failure=...)`:

| Policy        | Behaviour when a dep ends non-`COMPLETED`                          |
|---------------|--------------------------------------------------------------------|
| `"fail"`      | This task is marked `FAILED` with a `DependencyFailedError`. *(default)* |
| `"skip"`      | This task is marked `SKIPPED`; downstream sees the skip too.       |
| `"run-anyway"`| Task runs as if the dep had succeeded. Args you passed are used as-is. |

> Dependencies don't auto-thread their return values into the dependent task's args. If task B needs A's output, look it up after submit via `h_a.result()` *inside* B's body, or pass the value through your own closure.

## Retries / timeouts / cancellation

```python
@task(retries=3, backoff="exponential", backoff_base=1.0, backoff_max=30.0,
      retry_on=(TransientError,), timeout=15.0)
def fetch(url): ...
```

| Knob            | Effect                                                                |
|-----------------|-----------------------------------------------------------------------|
| `retries=N`     | Up to N additional attempts after the initial one (`max_attempts=N+1`). |
| `retry_on`      | Exception class or tuple. Only matches are retried; others fail immediately. Default: `Exception`. |
| `backoff`       | `"none"`, `"linear"`, or `"exponential"`. Delay is `base * attempt` (linear) or `base * 2^(attempt-1)` (exponential), capped at `backoff_max`. |
| `timeout`       | Seconds per attempt. Exceeding raises `TaskTimeoutError` — *not* retried (timeouts are terminal). |

Cancellation is per-handle: `handle.cancel()` requests stop. Cooperative for asyncio (CancelledError at the next await), cooperative for threads (your body must check `current_task().cancel_event`), best-effort for processes (the future is cancelled if not yet scheduled).

```python
from pyworkflowy import current_task

@task(backend="thread")
def long_loop(n):
    ctx = current_task()
    for i in range(n):
        if ctx.cancel_event.is_set():
            return "stopped early"
        do_chunk(i)
```

`runner.cancel_all()` sets the cancel flag on every non-terminal handle.

## Runner

```python
runner = TaskRunner(
    max_workers=8,
    backend="asyncio",           # default for tasks that don't specify
    on_task_error="raise",       # "raise" | "log" | "continue"
    checkpoint_path="state.json",
    checkpoint_interval=5.0,
)
```

`on_task_error` chooses how task failures propagate out of `run()`:

| Value      | Behaviour                                                         |
|------------|-------------------------------------------------------------------|
| `"raise"`  | The first failing task's exception aborts the runner. *(default)* |
| `"log"`    | Failures logged via `logging.getLogger("pyworkflowy")`; run continues. |
| `"continue"` | Failures stored on handles; no log, no raise.                   |

Use as a context manager so the executor pools are torn down cleanly:

```python
with TaskRunner() as runner:
    ...
    runner.run()
```

`with TaskRunner(...)` also binds the runner to a contextvar, so `task.submit(...)` finds it without explicit `runner=`. Outside the `with`, pass `runner=` or call `runner.submit(task, ...)` directly.

## Persistence / resume

Tell the runner where to write its state:

```python
with TaskRunner(checkpoint_path="state.json", checkpoint_interval=5.0) as runner:
    ...
    runner.run()
```

The default `JSONCheckpointer` writes after each task completion (rate-limited by `checkpoint_interval`). To resume after a crash, call `TaskRunner.resume`:

```python
runner = TaskRunner.resume("state.json")
# Re-submit the same tasks; previously-completed ones get their results
# primed and won't re-execute.
```

Resume uses each handle's persisted ID — so the runner needs to be re-built with the same submission order (or you have to inject IDs manually for now). Already-completed handles are primed with the persisted result on submit.

| Backend             | Trade-off                                                      |
|---------------------|----------------------------------------------------------------|
| `JSONCheckpointer`  | Default. Args/return values must JSON-serialise. Validated at submit. |
| `PickleCheckpointer`| Anything pickle accepts; standard pickle caveats apply.        |
| custom              | Subclass `Checkpointer` with `save()`/`load()`.                |

Unserialisable args raise `CheckpointError` at *submit* time so you see the error where it originated.

## Scheduling

```python
from pyworkflowy import TaskRunner, task
from pyworkflowy.schedule import Scheduler

@task
def cleanup():
    ...

with TaskRunner() as runner:
    sched = Scheduler(runner=runner)
    sched.every(60).do(cleanup)
    sched.cron("0 * * * *").do(cleanup)        # top of every hour
    sched.at(datetime(2026, 12, 31, 23, 59)).do(cleanup)  # one-shot

    sched.start()    # background thread
    # ... do other work ...
    sched.stop()
```

Or async, on your own loop:

```python
sched = Scheduler(runner=runner)
sched.every(0.5).do(cleanup)
await sched.arun()
```

For tests, `sched.tick()` fires every due job once and returns the resulting handles — useful with a fake clock.

### Cron subset

`m h dom mon dow` — five fields, space-separated:

| Syntax       | Example         | Meaning                          |
|--------------|-----------------|----------------------------------|
| `*`          | `*`             | every value in range             |
| `N`          | `5`             | exactly `5`                      |
| `a-b`        | `9-17`          | inclusive range                  |
| `a,b,c`      | `0,15,30`       | list                             |
| `*/N`        | `*/15`          | every N from the start of range  |
| `a-b/N`      | `9-17/2`        | every N within range             |

`day_of_week` uses cron-style `0=Sunday`. No seconds field, no `@hourly`/`@daily` aliases, no `L`/`W` modifiers. Missed fires while the scheduler is stopped are **not** backfilled.

## Async

Async tasks need the asyncio backend. The runner orchestrates on asyncio either way — `run()` just wraps `asyncio.run(arun())`.

```python
@task
async def fetch(url):
    async with httpx.AsyncClient() as c:
        return (await c.get(url)).json()

runner = TaskRunner()
h = runner.submit(fetch, args=("https://example.com",))
await runner.arun()
print(await h)   # handles are awaitable
runner.shutdown()
```

Handles are awaitable; awaiting yields the value or raises the failure. Sync handles also work — `handle.result(timeout=...)` blocks.

## Errors

| Exception                | Raised when                                                       |
|--------------------------|-------------------------------------------------------------------|
| `TaskError`              | Base class. Catch this to swallow any pyWorkflowy-raised error.       |
| `TaskTimeoutError`       | A task exceeds its `timeout=` budget. **Not retried**.            |
| `TaskCancelledError`     | A task was cancelled via `handle.cancel()` / `runner.cancel_all()`. |
| `CycleError`             | A submission would close a cycle in the dependency graph.         |
| `DependencyFailedError`  | A task with `on_dep_failure="fail"` had a failed dep.             |
| `RetryExhaustedError`    | All attempts failed; wraps the last exception via `__cause__`.    |
| `CheckpointError`        | Serialisation or I/O failed in a `Checkpointer`.                  |

Inside a task body, you can read `current_task()` for the current `TaskContext` (name, attempt number, cancel event).

## Threads / Multiprocessing notes

- The thread pool is created lazily on first use, shut down when the runner is shut down. `current_task()` *does* work in the thread backend because pyWorkflowy binds the contextvar on entry.
- The process backend's workers run in **separate interpreters** — module-level state is reimported, `current_task()` returns `None`, and the function reference is serialised via pickle. Top-level functions only; no lambdas, no nested defs.
- On Windows (and on Python 3.14+ generally), the default start method is `spawn` — the same caveat: every worker re-imports your code.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run pyrefly check
```
