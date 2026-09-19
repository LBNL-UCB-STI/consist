# Artifact Admission

Artifact admission verifies that a regular file about to enter a workflow has
the same bytes as either one exact input from an explicitly selected completed
run or a declared `sha256:file:<64 lowercase hex>` identity. It is separate
from cache lookup: admission always computes a full raw-file SHA-256 identity,
while `Artifact.hash` may represent a fast, directory, or caller-supplied
fingerprint.

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

## Declared digests

Use a declared digest when a workflow has a locally governed canonical file
identity but no prior Consist run. `FileIdentity` rejects bare, uppercase, and
non-file identities before Consist reads candidate bytes. A successful report
only proves equality with the declaration; it does not authenticate the source
label or URI, create a trusted `Artifact`, or change cache hashing defaults.

```python
from consist import (
    DeclaredDigestExpectation,
    FileIdentity,
    check_expected_identity,
)

report = check_expected_identity(
    execution_path="inputs/initial_datastore.h5",
    input_role="urbansim_initial_datastore",
    expectation=DeclaredDigestExpectation(
        identity=FileIdentity.parse("sha256:file:<64 lowercase hex>"),
        source_label="UrbanSim initial datastore",
        source_uri="s3://governed-inputs/seattle/initial_datastore.h5",
    ),
)
```

For a runtime-resolved path, construct an `AdmissionReference` without an
`artifact_key` and with a nonempty `input_role`, then call
`check_admission_reference_expected_identity(...)`. It retains the same
execution, configuration, and consumer-path evidence as the prior-run wrapper.

## Report schema compatibility

New reports use schema version 3. It retains `consumer_path` from v2 and adds
the declared-digest shape: nullable `artifact_key` and `expected_run_id`, plus
optional `expected_source_label` and `expected_source_uri`. Sidecars retain
their own `report_schema_version`; readers may still parse historical v1/v2
reports and should treat fields absent from those versions as unavailable.
Consist does not rewrite existing sidecars.

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

::: consist.core.admission.FileIdentity
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.DeclaredDigestExpectation
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.check_expected_identity
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.check_admission_reference_expected_identity
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

::: consist.core.admission.compare_path_to_identity
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.hash_semantics_for_new_artifact
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true
