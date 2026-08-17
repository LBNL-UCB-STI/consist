# Step Contracts

Step contracts expose resolved `@define_step` metadata without executing the
step. Use them when an external orchestrator, schema builder, or validation
layer needs the same declarative metadata that `Tracker.run(...)` and
`ScenarioContext.run(...)` would use at execution time.

## API reference

::: consist.core.step_contracts.StepContract
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.step_contracts.resolve_step_contract
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.step_contracts.collect_step_contracts
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true
