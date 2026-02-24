# Run

## `consist.run` options contract

`consist.run(...)` rejects legacy direct policy kwargs (for example
`cache_mode`, `output_missing`, `inject_context`) with a `TypeError`.

Use options objects instead:

- `cache_options=CacheOptions(...)`
- `output_policy=OutputPolicyOptions(...)`
- `execution_options=ExecutionOptions(...)`

For migration details, see
[Options Objects Migration Guide](../migrations/options-objects-migration-guide.md).

## Identity kwargs for run/trace surfaces

Public identity kwargs for `consist.run(...)`, `consist.trace(...)`,
`Tracker.run(...)`, `Tracker.trace(...)`, `ScenarioContext.run(...)`, and
`ScenarioContext.trace(...)` are:

- `adapter=...`
- `identity_inputs=[...]`

`config_plan=` and `hash_inputs=` are hidden compatibility kwargs. They remain
accepted for migration compatibility, but they are not the recommended public
surface for new code.

Run/trace parity is implemented through a shared invocation-resolution path
(`resolve_run_invocation`), so identity and cache-option validation rules are
consistent across both execution styles.

::: consist.api.run
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false

## `Run` model

Use `run.identity_summary` to debug cache identity composition
(`config_hash`, `input_hash`, `code_hash`, adapter metadata, and
`identity_inputs` digests).

::: consist.models.run.Run
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - duration_seconds
        - identity_summary
