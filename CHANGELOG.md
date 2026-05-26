# CHANGELOG


## v0.4.0 (2026-05-26)

### Bug Fixes

- **offload**: Tighten ctx.offload accessor, exception coverage, and OffloadPool.execute guard
  ([`db88cc0`](https://github.com/KilianSen/pyWorkflowy/commit/db88cc0d9b3c85f2b3b9e961664c180adc1e0e4f))

- OffloadPool.execute now raises NotImplementedError instead of silently running fn without
  _thread_wrapped if the submit-time guard were ever removed. - Add OffloadPool.thread_executor() so
  TaskContext.offload no longer reaches into the private _executor attribute. - Extract the 50ms
  cancel-poll interval into _OFFLOAD_CANCEL_POLL_INTERVAL. - Narrow the finally-block
  contextlib.suppress in TaskContext.offload to asyncio.CancelledError so KeyboardInterrupt /
  SystemExit aren't swallowed. - Document the offload pool's thread cost in _default_pools. - Add
  test for exception propagation through ctx.offload.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>

### Code Style

- Apply ruff format
  ([`60fcdce`](https://github.com/KilianSen/pyWorkflowy/commit/60fcdce11f2cfdcef8f56fc5d3ab00361df41649))

### Documentation

- Update pool listings and Checkpointer reference for 0.7.0
  ([`e6a761b`](https://github.com/KilianSen/pyWorkflowy/commit/e6a761bcf9e32241b76e25eb304697d8d6e84bc1))

- TaskRunner._default_pools now includes 'offload' pool; update docstrings - task() decorator
  docstring: four named pools (default, thread, process, offload) - README custom checkpointer row:
  reference SnapshotCheckpointer not Checkpointer

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>

- **handle**: Clarify TaskHandle.cancel hard-cancel semantics for asyncio
  ([`7f63972`](https://github.com/KilianSen/pyWorkflowy/commit/7f63972688fa2a3f8ce363002ccdc156b6d0552a))

- **persistence**: Clarify Checkpointer vs SnapshotCheckpointer guidance
  ([`34d0074`](https://github.com/KilianSen/pyWorkflowy/commit/34d00748a61f2584cb2f8b9ab81e59b3e10560ce))

Update module docstring to distinguish per-row (Checkpointer) from whole-state / resume-capable
  (SnapshotCheckpointer) subclassing guidance. Clarify tripwire comment in test_persistence.py to
  note that the counter is the authoritative signal and the raise may be swallowed.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **submit**: Clarify payload= idiom and schedule.do forwarding
  ([`b5f338d`](https://github.com/KilianSen/pyWorkflowy/commit/b5f338d610fb993ef5178e525601e6a87fc0dda3))

Fix 1: TaskRunner.submit() now uses explicit None-check for payload parameter so an explicit empty
  dict is properly copied instead of replaced with a fresh dict.

Fix 2: Added docstring note explaining how JobBuilder.do(task, *args, **kwargs) forwards args/kwargs
  to runner.submit() as args=tuple(...), payload=dict(...), clarifying the bridging between mutable
  kwargs and immutable payload idiom.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>

### Features

- **core**: Add TaskContext.id property
  ([`d6a8d59`](https://github.com/KilianSen/pyWorkflowy/commit/d6a8d5977e21c1a635bc078cb28b794ea2b15ae5))

Exposes the backing handle's unique id via a public property on TaskContext, with a RuntimeError
  guard for the no-handle case. Adds a test asserting ctx.id == handle.id from inside a task body.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **persistence**: Split Checkpointer and SnapshotCheckpointer
  ([`324895c`](https://github.com/KilianSen/pyWorkflowy/commit/324895cc2f96e6a809bbbeb854a5951568b057e7))

Per-row Checkpointer ABC now requires save_handle/delete_handle/query and adds a save_initial hook
  (defaulting to save_handle). save/load raise NotImplementedError on the base class.
  SnapshotCheckpointer carries the whole-state contract and cascades the row-grained methods through
  save/load. JSONCheckpointer and PickleCheckpointer rebase onto SnapshotCheckpointer.
  TaskRunner.resume now rejects per-row checkpointers with TypeError, since it needs whole-state
  load().

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>

- **pools**: Add offload PoolKind and TaskContext.offload
  ([`c359709`](https://github.com/KilianSen/pyWorkflowy/commit/c35970951554c2d2c04bd74eff1c175bcb96b4e7))

Add a new pool kind "offload" for runner-managed thread-pool offload of sync C-extension chunks
  (Pillow, ONNX, numpy) called from inside async task bodies. Offload pools are call-only: tasks may
  not target them via @task(pool=...). Instead, an async task body invokes ctx.offload(fn, ...) to
  run blocking work on the dedicated pool while still observing the task's cancel_event for prompt
  cancellation.

- _backends: widen PoolKind to include "offload"; add OffloadPool class; route build_pool_executor
  for kind="offload". - _runner: add "offload" entry to default pools; reject task submissions that
  target an offload-kind pool with a clear, actionable error; thread the runner reference into
  TaskContext so ctx.offload can locate the pool. - _core: add TaskContext._runner field; add async
  TaskContext.offload() method that races the offload future against a cancellation watcher;
  cancellation is observed via a short poll on the threading.Event to avoid leaking a blocked
  watcher thread on the happy path. - tests: cover the four behaviours — submit rejection, basic
  offload round-trip, prompt cancellation during a long blocking call, and per-pool bounded
  concurrency.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>

- **runner**: Add find_active/has_active dedup query API
  ([`f8e6994`](https://github.com/KilianSen/pyWorkflowy/commit/f8e699468304aa3c306745330090719b975252dd))

Expose two public methods on TaskRunner that let callers inspect the dedup index without replicating
  internal state management.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **runner**: Persist initial PENDING entry on submit() via save_initial
  ([`96ad52f`](https://github.com/KilianSen/pyWorkflowy/commit/96ad52ffc4659337e59a894ce155198a172f5a1b))

Call self._checkpointer.save_initial(self._handle_to_entry(handle)) in submit() before returning so
  any checkpointer inspection immediately after submit() sees the handle with status=pending. Skips
  the call when resumed is not None (handle primed from a prior checkpoint; existing row is
  authoritative). Also adds two tests in test_persistence.py: one verifying the PENDING row is
  written without run(), one verifying the default save_initial cascade to save_handle.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

### Testing

- **persistence**: Tighten save_initial cascade assertions
  ([`1af53f9`](https://github.com/KilianSen/pyWorkflowy/commit/1af53f91164213c92deea9a00be5a4cf4976f3bd))

Add save_initial_count to CountingCheckpointer, override save_initial to increment it and delegate
  via super(), and assert both save_initial_count == 1 AND save_handle_count == 1 after submit() —
  proving the cascade fired and cannot be bypassed by calling save_handle directly. Also convert the
  manual try/finally shutdown in test_submit_persists_pending_row_immediately to the with-statement
  context-manager pattern used by the rest of the file.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>


## v0.3.0 (2026-05-24)

### Features

- Update version to 0.6.0 and enhance task runner with serve functionality
  ([`061d144`](https://github.com/KilianSen/pyWorkflowy/commit/061d1446e1cf50f0ceb13379945501da17c01728))


## v0.2.0 (2026-05-23)

### Features

- Update version to 0.5.0 and refactor task backend to pool
  ([`fc30586`](https://github.com/KilianSen/pyWorkflowy/commit/fc305866c588b2978620f6110374485080a7df9c))

- Update version to 0.5.0 and refactor task backend to pool
  ([`27a0230`](https://github.com/KilianSen/pyWorkflowy/commit/27a0230356e323bf3f9697fde376f32ff2511553))

- Update version to 0.5.0 and refactor task backend to pool
  ([`1ce9121`](https://github.com/KilianSen/pyWorkflowy/commit/1ce912150ec42b9bf2737b5a1b30b123ac92505d))


## v0.1.0 (2026-05-23)

### Chores

- Initial project scaffolding
  ([`92945b5`](https://github.com/KilianSen/pyWorkflowy/commit/92945b5d92fd63ac5a79037de5a69ea0b3156993))

### Continuous Integration

- Add CI + Release workflows mirroring pyHooky
  ([`bbffc80`](https://github.com/KilianSen/pyWorkflowy/commit/bbffc803378f1fe7f68b3173ef299d060ed61964))

Adds .github/workflows/ci.yml (matrix test on 3.11-3.14 with ruff check + format check + pyrefly +
  pytest --cov) and release.yml (python-semantic-release with PyPI publish on master). Applies ruff
  format across the codebase so the format-check step is green.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>

### Features

- Add cron-like Scheduler
  ([`6e0a7b9`](https://github.com/KilianSen/pyWorkflowy/commit/6e0a7b9034b8ed8a895468ab19a9d6db84dc89b1))

- Add DAG checks and execution backends
  ([`5cc6d03`](https://github.com/KilianSen/pyWorkflowy/commit/5cc6d03f34770758d60fca4455f0b86f72e116ad))

- Add task primitives and class-based form
  ([`d4da68c`](https://github.com/KilianSen/pyWorkflowy/commit/d4da68cb36f1eeac87e7d9d507322d6a3f65f6c9))

- Add TaskRunner with retries, timeouts, and checkpointing
  ([`b83209a`](https://github.com/KilianSen/pyWorkflowy/commit/b83209af9d9543c469fe0e1cabd8525052e5c25d))

### Refactoring

- Rename pyTasky to pyWorkflowy
  ([`6dde5c8`](https://github.com/KilianSen/pyWorkflowy/commit/6dde5c8754c704f5baf647bdcd58f7b2892a9504))

Renames the package, distribution name, and all internal references from pytasky / pyTasky to
  pyworkflowy / pyWorkflowy. Internal class names (Task, TaskRunner, TaskHandle, ...) are unchanged
  — the library frames itself as a workflow engine, but the entities it orchestrates are still
  tasks.

BREAKING CHANGE: import path is now `from pyworkflowy import ...` and the distribution name is
  `pyworkflowy`.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>

### Testing

- Cover tasks, runner, DAG, retries, timeouts, persistence, scheduler
  ([`55ab258`](https://github.com/KilianSen/pyWorkflowy/commit/55ab258513058f6139b9f26b8e53426df829b390))

### Breaking Changes

- Import path is now `from pyworkflowy import ...` and the distribution name is `pyworkflowy`.
