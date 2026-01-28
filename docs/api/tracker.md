# Tracker

::: consist.core.tracker.Tracker
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        # Core lifecycle
        - begin_run
        - start_run
        - run
        - trace
        - scenario
        - end_run
        - define_step
        - last_run
        - is_cached
        - cached_artifacts
        - cached_output
        - suspend_cache_options
        - restore_cache_options
        - capture_outputs
        - log_meta
        # Logging and loading
        - log_artifact
        - log_artifacts
        - log_input
        - log_output
        - log_dataframe
        - load
        - materialize
        - ingest
        # Querying and history
        - find_runs
        - find_run
        - find_latest_run
        - get_latest_run_id
        - find_artifacts
        - get_artifact
        - get_artifacts_for_run
        - get_run
        - get_run_config
        - get_run_inputs
        - get_run_outputs
        - get_artifact_lineage
        - print_lineage
        - history
        - diff_runs
        - get_config_facet
        - get_config_facets
        - get_run_config_kv
        - get_config_values
        - get_config_value
        - find_runs_by_facet_kv
        # Views and matrices
        - view
        - create_view
        - load_matrix
        - export_schema_sqlmodel
        - netcdf_metadata
        - openmatrix_metadata
        - spatial_metadata
        # Config canonicalization
        - canonicalize_config
        - prepare_config
        - apply_config_plan
        - identity_from_config_plan
        # Format-specific logging
        - log_h5_container
        - log_h5_table
        - log_netcdf_file
        - log_openmatrix_file
        # Advanced / low-level
        - engine
        - set_run_subdir_fn
        - run_artifact_dir
        - resolve_uri
        - run_query
        - get_run_record
        - resolve_historical_path
        - load_input_bundle
        - get_artifact_by_uri
        - get_run_artifact
        - load_run_output
        - find_matching_run
        - on_run_start
        - on_run_complete
        - on_run_failed
