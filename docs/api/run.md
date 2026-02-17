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

::: consist.api.run
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false

## `Run` model

::: consist.models.run.Run
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - duration_seconds
