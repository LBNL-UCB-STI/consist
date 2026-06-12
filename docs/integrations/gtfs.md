# GTFS Support

GTFS (General Transit Feed Specification) is the standard transit feed format
used by many agencies and transit modeling pipelines. In Consist, GTFS support
has two layers: a raw driver that exposes feed members as loadable tables, and
a canonicalizer that turns one or more feeds plus a service date into a
queryable selected-service slice with stable semantic identity.

Use GTFS support when you want to:

- keep transit feeds in provenance and cache identity
- query raw feed members or canonical selected-service tables across runs
- wire a transit bundle through a small model step without writing one-off
  parsing code
- work with BEAM transit bundles without writing one-off parsing code

## What Consist recognizes

Consist accepts GTFS feeds in either of these forms:

- a `.zip` archive containing the feed tables
- a directory containing the feed tables

It ignores obvious junk files such as `README` and `LICENSE` text files. Raw
member loads preserve the source table shape. Canonical selected-service tables
add Consist-managed fields such as `feed_key` and are emitted for the supported
standard GTFS schemas.

## How to use it

### With Tracker

If you want GTFS tables pre-registered for querying, opt into the built-in
GTFS schema pack when you construct a tracker:

```python
from consist import Tracker

tracker = Tracker(
    run_dir="./runs",
    db_path="./provenance.duckdb",
    builtin_schemas=["gtfs"],
)
```

This keeps GTFS support explicit. Non-transit projects do not see GTFS tables
by default, while transit users get the standard tables ready to query.

For standalone semantic identity, call `tracker.canonicalize_gtfs(...)` inside
an active run:

```python
with tracker.start_run("weekday_transit", "gtfs"):
    weekday = tracker.canonicalize_gtfs(
        ["./inputs/beartransit-ca-us.zip"],
        service_date="2021-09-07",
        feed_keys=["beartransit"],
    )

print(weekday.service_slice_hash)
print(weekday.manifest["feeds"][0]["selection"]["active_service_ids"])
weekday.selected_tables["trips"].head()
tracker.load(weekday.table_artifacts["trips"]).df().head()
tracker.get_child_artifacts(weekday.selected_service_artifact)
```

The returned result includes raw feed hashes, a bundle hash, the selected
service slice hash, a logical selected-service parent artifact, a JSON
manifest artifact for identity and diagnostics, source feed artifacts, selected
tables as DataFrames, selected table child artifacts that can be passed to
downstream runs, and ingest specs that the tracker can apply automatically.
Use `weekday.selected_service_artifact` with `get_child_artifacts(...)` when
you want the selected table membership relation; use `weekday.manifest_artifact`
when you want the JSON manifest file itself.
Typed GTFS view rows store the selected-service parent ID in
`consist_artifact_id`, so view queries filter by the semantic service slice.
The parent artifact is logical and manifest-backed; preview it for a service
summary, and load table artifacts such as `weekday.table_artifacts["trips"]`
for row data.

Use `tracker.canonicalize_gtfs(...)` or `consist.canonicalize_gtfs(...)` for
normal workflows. The lower-level `canonicalize_gtfs_bundle(...)` helper is
available when you need the pure identity calculation without tracker-managed
artifacts, run metadata, or ingestion.

### With BEAM

BEAM projects usually keep their transit bundle under
`beam.routing.r5.directory`. When that path points at a GTFS bundle, Consist
discovers the feed archive or directory, uses the BEAM service date to pin the
transit slice for that run, and records the GTFS bundle as part of the run
identity.

In practice, that means BEAM transit changes invalidate cache the same way
other meaningful configuration changes do, without requiring separate GTFS
parsing code in your workflow.

If you want a hands-on walkthrough, see the notebook example below. It uses a
single zip snapshot of UC Berkeley's Bear Transit feed, compares weekday and
weekend service slices, and runs a tiny model against the selected service.

## What stays flexible

- Raw GTFS members are loadable through the `gtfs` driver.
- Canonical selected-service tables are typed and queryable when they match the
  built-in GTFS schema pack.
- Non-standard or vendor-specific tables remain available through the raw
  table path.
- The support is read/query oriented; it does not rewrite or mutate the feed.
- The capability is opt-in for general Tracker usage, so non-transit projects
  are not burdened with GTFS tables.

## Where to go next

- [Config Adapters overview](config_adapters.md)
- [BEAM Config Adapter](config_adapters_beam.md)
- [Tracker API](../api/tracker.md)
- [Example gallery](../examples.md), especially **05 GTFS Support**
