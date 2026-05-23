# CHANGELOG


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
