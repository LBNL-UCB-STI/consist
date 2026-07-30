# Consist provenance schema

Consist publishes a small LinkML module for schemas that need portable
references to a Consist run or artifact. The source module is
[`src/consist/schemas/provenance.yaml`](../src/consist/schemas/provenance.yaml).
It is a portable reference and production-lineage contract, not a complete
lineage model, ORM export, LinkML runtime dependency, or artifact registry. Its
pre-1.0 release status is **experimental**.

## What the module defines

`ConsistRunReference` contains a required public `run_id`, with optional
`provenance_namespace` and audit-only `signature`. When a namespace is present,
the pair `(provenance_namespace, run_id)` identifies a run in a logical Consist
ledger. Without it, the reference is deliberately local to its surrounding
provenance context.

Consist does not yet assign or persist provenance namespaces automatically.
Deployments that exchange references must choose and preserve one stable
namespace per ledger; a PILATES deployment can own that convention until a
cross-ledger Consist identity is introduced.

`ConsistArtifactReference` contains a required public UUID `artifact_id` and
optional namespace, artifact key, fingerprint, fingerprint strategy, and
nested producing run. The `producing_run` is optional because an external input
need not have been produced by Consist.

The portable fingerprint is exactly `Artifact.hash`. Its meaning follows the
hashing strategy: it can be content- or metadata-based. `Artifact.content_id`
is a database-local deduplication key, so it is not a portable fingerprint and
does not occur as a schema field.

The module deliberately excludes artifact locations, recovery paths,
parent-container structure, mutable metadata, schema-contract declarations,
and validation evidence. Those are either mutable observations or separate
contracts. Domain roles such as a population snapshot or an ATLAS vehicle
output remain downstream-owned semantics.

## What it does not model yet

This release can say that a downstream object refers to artifact X and that
Consist run Y produced it. It intentionally does not represent consumed
artifacts, input-binding roles, requested versus execution/cache-source runs,
artifact-to-artifact derivation, or validation, admission, and materialization
observations. Those require a later, separately reviewed observation layer.

## Downstream composition

Import this module from an operational profile or provenance adapter, not from
a shared semantic kernel or a model-owned row schema:

```yaml
imports:
  - linkml:types
  # A checked, immutable release asset stored with the downstream build.
  - ./vendor/consist-provenance-0.1.0/provenance.merged

classes:
  PilatesAtlasVehicleArtifact:
    attributes:
      physical_artifact:
        range: ConsistArtifactReference
        required: true
```

The `AtlasVehicleOutputRecord` itself should remain Consist-independent. A
tested local-import example lives at
[`tests/fixtures/linkml/downstream_operational_profile.yaml`](../tests/fixtures/linkml/downstream_operational_profile.yaml).
`consist:provenance` is the module's intended logical identifier, not a remote
import resolver in this release; consumers should use the pinned local merged
asset above.

## Release assets and offline builds

Each schema release must publish all of the following immutable assets:

- `provenance.yaml`, the modular source;
- `provenance.merged.yaml`, the complete resolved import closure;
- generated LinkML reference documentation under `reference/`;
- `SHA256SUMS`, covering every shipped asset.

Create the release directory with the development-only LinkML toolchain:

```bash
uv run --group dev python scripts/build_provenance_schema_release.py \
  --output dist/provenance-schema-0.1.0
```

The output directory must be new or empty. This avoids carrying stale generated
pages forward after a schema rename or removal.

A downstream build should obtain a versioned release asset, verify its
checksum, and import the local merged schema. It must not resolve a mutable
default-branch URL or fetch a remote schema during normal runtime work. Python
consumers may locate the packaged modular source with
`importlib.resources.files("consist.schemas").joinpath("provenance.yaml")`;
non-Python consumers can use the downloaded merged asset.

The public LinkML Schema Registry is a later discovery mechanism, not a package
manager, import resolver, or artifact-custody service. Register only after a
downstream consumer has successfully imported a released asset.

## Compatibility policy

The schema version has an independent lifecycle from the Python package. Consist
`0.4.0` ships schema `0.1.0`; later package releases can ship the same schema
version unchanged. Additive classes or optional slots are normally backward
compatible. Renaming or removing a class or slot, changing a required field, or
changing identity/fingerprint meaning is a breaking schema change: update the
schema version deliberately, publish a release note, and keep a compatibility
path where feasible. Do not silently repurpose existing fields.

LinkML is a development dependency only. Normal Consist imports, tracking,
caching, querying, recovery, run identity, artifact identity, persistence, and
database migrations are unchanged by this module.
