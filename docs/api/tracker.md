# Tracker

::: consist.core.tracker.Tracker
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - __init__
        - engine
        - start_run
        - begin_run
        - end_run
        - scenario
        - task
        - is_cached
        - last_run
        - log_artifact
        - log_dataframe
        - log_artifacts
        - log_input
        - log_output
        - log_meta
        - capture_outputs
        - ingest
        - load
        - get_run
        - get_artifact
        - get_artifacts_for_run
        - get_artifact_lineage
        - find_run
        - find_runs
        - history
        - resolve_uri
        - view
        - create_view
        - cached_output
        - cached_artifacts
        - on_run_start
        - on_run_complete
        - on_run_failed
        - get_config_facet
        - get_config_facets
        - get_run_config_kv
        - find_runs_by_facet_kv
      filters:
        - "!^_"
