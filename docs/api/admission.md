# Artifact Admission

Artifact admission verifies that a regular file, directory container, or logical
array container about to enter a workflow matches either one exact artifact from
an explicitly selected completed run or a declared canonical identity
(`sha256:file:<64 hex>`, `sha256:manifest-v1:<64 hex>`, or
`sha256:logical_matrices:<64 hex>`). It is separate from cache lookup: admission
computes explicit verification identities rather than relying unconditionally on
fast, directory, or caller-supplied `Artifact.hash` fingerprints.

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

The expected run must be `completed` and must have a unique link matching the
requested artifact key and `expected_direction` (`"any"` by default, or
explicitly constrained to `"input"` or `"output"`). Historical hashes without
explicit full-file semantics remain unverified unless a distinct immutable
`expected_bytes_path` corroborates the stored historical fingerprint.

## Cross-tracker & output expectations

When a downstream stage needs to admit an output produced in a separate
upstream provenance database, pass `source_tracker_db` (opened strictly
read-only without schema initialization) or construct a typed
`PriorRunExpectation`:

```python
from consist import PriorRunExpectation, check_prior_run_expectation

expectation = PriorRunExpectation(
    expected_run_id="upstream-beam-run",
    artifact_key="transit_skims",
    expected_direction="output",
    source_tracker_db="beam/provenance.duckdb",
)

report = check_prior_run_expectation(
    execution_path="inputs/transit_skims.omx",
    expectation=expectation,
)
```

For runtime-resolved paths, combine `AdmissionReference` with
`check_admission_reference_prior_run_expectation(...)`.

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

Use a declared digest when a workflow has a locally governed canonical identity
but no prior Consist run. `AdmissionIdentity` (or `FileIdentity` for raw files)
rejects bare, uppercase, and unsupported identity prefixes before Consist reads
candidate bytes. A successful report only proves equality with the declaration;
it does not authenticate the source label or URI, create a trusted `Artifact`,
or change cache hashing defaults.

```python
from consist import (
    AdmissionIdentity,
    DeclaredDigestExpectation,
    check_expected_identity,
)

report = check_expected_identity(
    execution_path="inputs/initial_datastore.h5",
    input_role="urbansim_initial_datastore",
    expectation=DeclaredDigestExpectation(
        identity=AdmissionIdentity.parse("sha256:file:<64 lowercase hex>"),
        source_label="UrbanSim initial datastore",
        source_uri="s3://governed-inputs/seattle/initial_datastore.h5",
    ),
)
```

For a runtime-resolved path, construct an `AdmissionReference` without an
`artifact_key` and with a nonempty `input_role`, then call
`check_admission_reference_expected_identity(...)`. It retains the same
execution, configuration, and consumer-path evidence as the prior-run wrapper.

## Directory & logical array admission

Admission supports directory artifacts (`sha256:manifest-v1:<64 hex>`) and
format-neutral logical array containers (`sha256:logical_matrices:<64 hex>`)
across `.omx`/`.h5` and `.zarr` containers.

### Identity schemes (`identity_scheme`)

Both `PriorRunExpectation` and `DeclaredDigestExpectation` accept
`identity_scheme`:

- `"auto"` (default): uses a stored or sidecar `sha256:logical_matrices:...`
  identity when present, or falls back to container identity (`sha256:file:...`
  for regular files, `sha256:manifest-v1:...` for directory artifacts such as
  `.zarr`).
- `"container"`: forces byte-for-byte file (`sha256:file:...`) or directory
  manifest (`sha256:manifest-v1:...`) verification.
- `"logical"`: forces logical array verification (`sha256:logical_matrices:...`),
  enabling cross-format verification when an upstream `.zarr` container is
  materialized or converted to `.omx` (or vice versa).

### Canonicalization profiles (`logical_profile` & `ArrayCanonicalizationPolicy`)

Logical array hashing is governed by `ArrayCanonicalizationPolicy` or its named
profiles via `logical_profile`:

- `"preserve_canonical"` (default): preserves native numeric dtypes (`float64`,
  `float32`, `float16`, signed/unsigned integers, booleans) in canonical
  little-endian form while normalizing non-signaling IEEE 754 `NaN` bit patterns
  and `-0.0` for floating-point arrays. Includes `/lookup` zone mappings in OMX
  containers alongside `/data` matrices.
- `"float32_matrices_v1"`: explicit opt-in profile that casts floating-point
  matrices under `/data` to canonical little-endian `float32` (`<f4`) with
  canonical `NaN` (`0x7FC00000`) and `+0.0` (`0x00000000`) normalization,
  matching external JVM/Scala writers that emit `float32` logical matrix
  digests.

```python
from consist import (
    PriorRunExpectation,
    check_prior_run_expectation,
    compute_omx_logical_matrices_sha256,
    write_artifact_provenance_sidecar,
)

# Compute a logical digest and write a .provenance.json sidecar
digest = compute_omx_logical_matrices_sha256(
    "outputs/skims.omx",
    policy="preserve_canonical",
)
write_artifact_provenance_sidecar(
    "outputs/skims.omx",
    content_hash=digest,
    logical_profile="preserve_canonical",
    producer="skim_builder",
)

# Admit a downstream OMX file against an upstream Zarr output by logical content
report = check_prior_run_expectation(
    execution_path="inputs/skims.omx",
    expectation=PriorRunExpectation(
        expected_run_id="upstream-skim-run",
        artifact_key="skims_zarr",
        expected_direction="output",
        identity_scheme="logical",
        logical_profile="preserve_canonical",
    ),
)
```

## Report schema compatibility

New reports use schema version 4. It retains `consumer_path` from v2, the
declared-digest shape and `expected_direction` / `source_tracker_db` from v3,
and adds `identity_scheme`, `logical_profile`, `expected_identity_source`, and
`actual_identity_source` for directory and logical array admission.
Sidecars retain their own `report_schema_version`; historical v1/v2 prior-run
reports default `expected_direction` to `"input"`, and readers should treat
other fields absent from older versions as unavailable. Consist does not
rewrite existing sidecars.

## Public API

::: consist.core.admission.AdmissionReport
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.PriorRunExpectation
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.check_artifact_identity
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.check_prior_run_expectation
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

::: consist.core.admission.check_admission_reference_prior_run_expectation
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.admission.AdmissionIdentity
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

## Logical array & container identity helpers

::: consist.core.logical_array_identity.ArrayCanonicalizationPolicy
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.MatrixContainerSummary
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.ArtifactProvenanceSidecar
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.compute_logical_matrices_sha256
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.compute_omx_logical_matrices_sha256
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.compute_zarr_logical_matrices_sha256
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.admission_directory_identity
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.inspect_matrix_container
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.write_artifact_provenance_sidecar
    options:
      show_source: false
      show_root_heading: true
      show_root_toc_entry: true

::: consist.core.logical_array_identity.read_artifact_provenance_sidecar
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
