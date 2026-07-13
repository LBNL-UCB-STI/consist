# Artifact Admission

Artifact admission verifies that a regular file about to enter a workflow has
the same bytes as one exact input from an explicitly selected completed run. It
is separate from cache lookup: admission always computes a full raw-file
SHA-256 identity, while `Artifact.hash` may represent a fast, directory, or
caller-supplied fingerprint.

Consist reports evidence without deciding workflow policy. Callers such as
PILATES can classify an `AdmissionReport` as fatal, warning-only, or acceptable
for their execution mode.

## Basic usage

```python
from consist import check_artifact_identity

report = check_artifact_identity(
    tracker,
    execution_path="inputs/gtfs.zip",
    expected_run_id="baseline-beam-run",
    artifact_key="config:seattle/r5/seattle_gtfs.zip",
)

if report.outcome != "verified":
    raise RuntimeError(report.canonical_json())
```

The expected run must be `completed` and must have exactly one input link with
the requested artifact key. Historical hashes without explicit full-file
semantics remain unverified unless a distinct immutable `expected_bytes_path`
corroborates the stored historical fingerprint.

## Runtime-resolved inputs

Applications that already prove the consumer's host/container mapping should
pass that evidence through `AdmissionReference`. Consist hashes only
`execution_path`; it records `consumer_path` but does not recreate mount or
configuration resolution. The report's `physical_target_path` always comes
from resolving `execution_path`; callers cannot replace that audit value.

```python
from consist import AdmissionReference, check_admission_reference

report = check_admission_reference(
    tracker,
    expected_run_id="baseline-beam-run",
    reference=AdmissionReference(
        artifact_key="linkstats_warmstart",
        execution_path="workspace/beam/input/seattle/warmstart.csv.gz",
        consumer_path="/app/input/seattle/warmstart.csv.gz",
    ),
)
```

## Report schema compatibility

New reports use schema version 2, which adds `consumer_path` to the serialized
payload. Sidecars retain their own `report_schema_version`; consumers that read
both historical and new sidecars should branch on that field, treating a
missing `consumer_path` in version 1 as unavailable rather than as a path
claim. Consist does not rewrite existing sidecars.

## Public API

::: consist.core.admission.AdmissionReport
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.check_artifact_identity
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.AdmissionReference
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.check_admission_reference
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

## Developer identity helpers

These lower-level helpers define how artifact fingerprints are described and
how the admission-specific full-file identity is computed. Most application
code should use `check_artifact_identity(...)` instead.

::: consist.core.admission.admission_file_identity
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.hash_semantics_for_new_artifact
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true
