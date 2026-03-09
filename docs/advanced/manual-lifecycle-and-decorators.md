# Manual Lifecycle and Decorators

These APIs are supported for integrations and framework glue, but are not part
of the quickstart recommended path.

## Manual lifecycle APIs

Use when start/end boundaries are controlled externally (for example framework
hooks in separate call sites):

- `Tracker.start_run(...)`
- `Tracker.begin_run(...)` / `Tracker.end_run()`

Prefer `run(...)` and `trace(...)` when you can keep each step in one call site.

## Step decorators

Use when you want reusable callable metadata defaults:

- `@define_step(...)`
- `@tracker.define_step(...)`

Decorator metadata can be useful in large shared step libraries, but introduces
extra concepts compared to explicit `run(..., outputs=[...], config=...)`.