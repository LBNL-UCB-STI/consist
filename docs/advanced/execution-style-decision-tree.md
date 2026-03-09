# Which execution style should I use?

Use this decision tree for choosing between `run`, `trace`, and `scenario`
surfaces.

```text
Start
 |
 +-- Is this a multi-step workflow with dependencies between steps?
 |      |
 |      +-- Yes --> Use scenario(...)
 |      |            |
 |      |            +-- Should this step skip user code on cache hit?
 |      |            |      |
 |      |            |      +-- Yes --> sc.run(...)
 |      |            |      +-- No  --> sc.trace(...)
 |      |            |
 |      |            +-- Need step-to-step artifacts? Use consist.ref/refs(...)
 |      |
 |      +-- No --> Single-step execution
 |                 |
 |                 +-- Should user code skip execution on cache hit?
 |                 |      |
 |                 |      +-- Yes --> consist.run(...) / tracker.run(...)
 |                 |      +-- No  --> consist.trace(...) / tracker.trace(...)
 |                 |
 |                 +-- Need explicit object wiring (library/app code)?
 |                        |
 |                        +-- Yes --> tracker.run(...) / tracker.trace(...)
 |                        +-- No  --> consist.run(...) / consist.trace(...)
 |
 +-- Need manual lifecycle split across call sites?
        |
        +-- Use begin_run/end_run or start_run (Advanced only)
```

## Rule of thumb

- Prefer **`run`** for expensive deterministic steps.
- Prefer **`trace`** for diagnostics, side effects, and always-execute blocks.
- Prefer **`scenario`** whenever steps depend on each other.
- Use manual lifecycle APIs only when integration architecture requires it.
