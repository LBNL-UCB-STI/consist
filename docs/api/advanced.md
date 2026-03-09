# API Advanced

Advanced APIs are fully supported, but they are not part of the onboarding
recommended path. Reach for these when integrating Consist into larger frameworks,
adapters, or custom orchestration layers.

## Manual lifecycle and decorator patterns

- [`consist.start_run`](api_helpers.md#consist.api.start_run)
- [`Tracker.begin_run`](tracker.md#consist.core.tracker.Tracker.begin_run)
- [`Tracker.end_run`](tracker.md#consist.core.tracker.Tracker.end_run)
- [`consist.define_step`](api_helpers.md#consist.api.define_step)
- [`Tracker.define_step`](tracker.md#consist.core.tracker.Tracker.define_step)

Guidance: [Manual Lifecycle and Decorators](../advanced/manual-lifecycle-and-decorators.md)

## Advanced cache/execution controls

- [`CacheOptions`](run.md#consist.run-options-contract)
- [`OutputPolicyOptions`](run.md#consist.run-options-contract)
- [`ExecutionOptions`](run.md#consist.run-options-contract)
- [`run.identity_summary`](run.md#run-model) for cache identity debugging

## Data/system integrations

- Config adapters:
  [`Tracker.prepare_config`](tracker.md#consist.core.tracker.Tracker.prepare_config),
  [`Tracker.apply_config_plan`](tracker.md#consist.core.tracker.Tracker.apply_config_plan)
- View and matrix systems:
  [`consist.view`](api_helpers.md#consist.api.view),
  [`consist.register_views`](api_helpers.md#consist.api.register_views),
  [`Tracker.load_matrix`](tracker.md#consist.core.tracker.Tracker.load_matrix)
- Query/lower-level utilities:
  [`consist.run_query`](api_helpers.md#consist.api.run_query),
  [`consist.db_session`](api_helpers.md#consist.api.db_session),
  [`Tracker.resolve_uri`](tracker.md#consist.core.tracker.Tracker.resolve_uri)

## Choosing execution style

Use the decision tree:
[Which execution style should I use?](../advanced/execution-style-decision-tree.md)

For a full inventory of all currently public symbols and methods, see
[Public API](public_api.md).
