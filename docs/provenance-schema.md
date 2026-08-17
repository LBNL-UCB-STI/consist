# Consist provenance schema

Consist publishes two small, experimental LinkML modules for portable provenance handoffs. They are a durable provenance graph and optional cache-aware invocation contract—not an ORM export, registry, complete execution-observation model, or LinkML runtime dependency.

- [`src/consist/schemas/provenance.yaml`](../src/consist/schemas/provenance.yaml) supplies ledger context, runs, artifacts, trusted identities, parent structure, and input/output lineage edges.
- [`src/consist/schemas/binding.yaml`](../src/consist/schemas/binding.yaml) imports the provenance graph and adds optional cache-aware binding invocation facts.

Both modules are schema version `0.1.0`, independently versioned from the Consist Python package. A package release can ship the same unchanged schema version; an additive optional field is normally compatible, while removing or renaming a class or slot, changing a required field, or changing identity meaning is a deliberate breaking schema change.

## Durable provenance graph

`ConsistProvenanceDocument` is a portable handoff envelope. It carries a required `ConsistProvenanceContext` and optional lists of runs, artifacts, and associations. The context has a stable `namespace` and an optional `locator`. The logical identity of a run or artifact is `(namespace, local ID)`. The locator is mutable retrieval metadata and is **never identity-bearing**: it must not affect equality, deduplication, namespace assignment, or the identity used to compare references.

Every run or artifact may carry its own optional `provenance_context`. When it is omitted inside a `ConsistProvenanceDocument`, the record **inherits the enclosing** document context. A record-level context overrides the enclosing one. A standalone reference with neither context is deliberately local to its containing handoff and cannot safely be merged with a different document. This lets identical local IDs remain distinct in separate Consist ledgers.

`ConsistRunReference` has a required public `run_id` and optional signature, model name, parent run, and structured config/input/git identity evidence. `ConsistArtifactReference` has a required public UUID `artifact_id` and optional artifact key, `Artifact.hash` fingerprint, fingerprint strategy, trusted immutable identity, parent artifact, and producing run.

`fingerprint` is the broader portable `Artifact.hash`, whose hashing strategy can be content- or metadata-based. `Artifact.content_id` is a database-local deduplication key and never appears in these modules. `immutable_identity` is stronger and narrower: Consist currently attests only lowercase SHA-256 values with `kind: file` or `kind: manifest-v1`; do not infer one from a broader fingerprint alone.

`ConsistRunArtifactAssociation` records an input or output edge and always requires its run, artifact, and direction. `role`, `binding_parameter`, and `is_implicit` are optional evidence. A role is specific to an invocation and is not an alias for `artifact_key`.

An input edge says the run consumed or used the artifact and does not imply production. An output association says the run produced the artifact. If its artifact also supplies `producing_run`, that reference must resolve to the same run after context inheritance. An output association without `producing_run` is still a production assertion; consumers must not add a contradictory producer. Matching forms are two convenient views of one production fact, not two events. `parent_artifact` only preserves container/member structure; it is not an artifact derivation edge.

## Optional cache-aware binding invocations

Import `binding.yaml` only when a consumer needs to distinguish the current request from a prior cached result. `ConsistBindingInvocationReference` contains the required requested run, effective execution run, and cache outcome; the cache-source run and strict binding identity digest are optional.

| Cache outcome | requested run | effective execution run | cache-source run |
| --- | --- | --- | --- |
| `miss` | newly executing run | same newly executing run | omitted |
| `hit` | current invocation's new run | historical producer | same historical producer |

For a cache miss, requested and effective execution runs must resolve to the same run and cache-source run must be absent. For a cache hit, effective execution and cache-source runs must resolve to the same historical producer; that producer must not be rewritten with the current request's input intent. The optional binding identity digest is a lowercase SHA-256 digest of a strict resolved-binding contract, not an artifact identity or the opaque binding JSON.

## Scope boundary

The modules deliberately exclude mutable artifact locations and recovery paths; run status, timestamps, descriptions, tags, arbitrary metadata, and scenario fields; schema profiles and observations; complete manifests; admission reports and registry aliases; full resolved-binding JSON; and PILATES-owned scenario semantics. Domain roles such as a population snapshot or ATLAS vehicle output remain downstream-owned semantics.

## Downstream composition

Downstreams should pin and checksum an immutable merged release asset. Import the durable graph whenever an operational profile needs Consist lineage:

```yaml
imports:
  - linkml:types
  - ./vendor/consist-provenance-0.1.1/provenance.merged

classes:
  PilatesAtlasVehicleArtifact:
    attributes:
      physical_artifact:
        range: ConsistArtifactReference
        required: true
```

Only a cache-aware adapter also imports `./vendor/consist-provenance-0.1.1/binding.merged.yaml` (as `binding.merged`). The model-owned row schema remains independent of Consist. The local composition example is [`tests/fixtures/linkml/downstream_operational_profile.yaml`](../tests/fixtures/linkml/downstream_operational_profile.yaml).

`consist:provenance` and `consist:binding` are logical identifiers, not remote import resolvers in this release. Consumers must use a checked local asset rather than a mutable branch URL or network fetch during normal work.

## Release assets and offline builds

Each schema release publishes:

- `provenance.yaml` and `binding.yaml`, the modular sources;
- `provenance.merged.yaml` and `binding.merged.yaml`, their resolved import closures;
- generated LinkML reference documentation under `reference/provenance/` and `reference/binding/`;
- `SHA256SUMS`, covering every shipped asset.

Create a new or empty release directory with the development-only LinkML toolchain:

```bash
uv run --group dev python scripts/build_provenance_schema_release.py \
  --output dist/provenance-schema-0.1.1
```

The corrected generated bundle is a prospective release-asset version `0.1.1`; the unchanged provenance and binding vocabulary remains schema version `0.1.0`. Do not replace the already checksummed `0.1.0` assets in place.

Merged assets record `source_file_date` and `generation_date` as timezone-aware UTC timestamps in `Z` notation. Before publishing, lint the modular sources and both merged assets, then verify the manifest:

```bash
linkml-lint dist/provenance-schema-0.1.1/provenance.yaml
linkml-lint dist/provenance-schema-0.1.1/binding.yaml
linkml-lint --ignore-warnings dist/provenance-schema-0.1.1/provenance.merged.yaml
linkml-lint --ignore-warnings dist/provenance-schema-0.1.1/binding.merged.yaml
(cd dist/provenance-schema-0.1.1 && sha256sum -c SHA256SUMS)
```

The merged lints ignore LinkML's `standard_naming` warnings for generator-created flattened slots such as `ConsistRunReference__run_id`; errors still fail the command. The builder rejects a nonempty output directory so stale generated pages cannot survive a schema rename or removal. A downstream build verifies `SHA256SUMS` before importing a merged asset. Python consumers may locate the packaged source with `importlib.resources.files("consist.schemas")`; normal `import consist`, tracking, caching, querying, recovery, persistence, and database migrations load neither `linkml` nor `linkml_runtime`.

The public LinkML Schema Registry is a later discovery mechanism, not a package manager, import resolver, or artifact-custody service. Register only after a downstream has successfully imported a released asset.
