# API Helpers

::: consist.api
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - view
        - use_tracker
        - run
        - trace
        - start_run
        - define_step
        - scenario
        - single_step_scenario
        - current_tracker
        - current_run
        - current_consist
        - output_dir
        - output_path
        - cached_artifacts
        - cached_output
        - get_artifact
        - register_artifact_facet_parser
        - log_artifact
        - log_artifacts
        - log_input
        - log_output
        - log_dataframe
        - log_meta
        - ingest
        - register_views
        - find_run
        - find_runs
        - db_session
        - run_query
        - pivot_facets
        - capture_outputs
        - load
        - load_df
        - load_relation
        - to_df
        - active_relation_count
        - set_current_tracker
        - noop_scenario
        - RelationConnectionLeakWarning
        - is_dataframe_artifact
        - is_tabular_artifact
        - is_json_artifact
        - is_zarr_artifact
        - is_hdf_artifact
        - is_netcdf_artifact
        - is_openmatrix_artifact
        - is_spatial_artifact
      filters:
        - "!^_"
