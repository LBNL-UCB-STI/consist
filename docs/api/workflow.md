# Workflow Contexts

## Scenario Context

::: consist.core.workflow.ScenarioContext
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - run_id
        - config
        - inputs
        - add_input
        - declare_outputs
        - require_outputs
        - collect_by_keys
        - run
        - trace

## Run Context

::: consist.core.workflow.RunContext
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - run_dir
        - inputs
        - load
        - log_artifact
        - log_artifacts
        - log_input
        - log_output
        - log_meta
        - capture_outputs

## Coupler

::: consist.core.coupler.Coupler
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - set
        - set_from_artifact
        - update
        - view
        - get
        - require
        - keys
        - items
        - values
        - path
        - declare_outputs
        - missing_declared_outputs
        - require_outputs
        - collect_by_keys
        - describe_outputs
