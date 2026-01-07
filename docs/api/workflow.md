# Workflow Contexts

## Scenario Context

::: consist.core.workflow.ScenarioContext
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - __init__
        - run_id
        - config
        - inputs
        - add_input
        - run
        - trace
      filters:
        - "!^_"

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
        - log_input
        - log_output
        - log_meta
        - capture_outputs
      filters:
        - "!^_"

## Coupler

::: consist.core.coupler.Coupler
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members: true
      filters:
        - "!^_"
