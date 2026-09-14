from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from consist.models.artifact import Artifact
from consist.models.run import Run

UTC = timezone.utc

# Version of the OpenLineage core spec the emitted events conform to.
OPENLINEAGE_SPEC_VERSION = "2-0-2"
_SPEC_BASE = f"https://openlineage.io/spec/{OPENLINEAGE_SPEC_VERSION}/OpenLineage.json"

#: ``schemaURL`` for the emitted run events (required by the spec).
OPENLINEAGE_SCHEMA_URL = f"{_SPEC_BASE}#/$defs/RunEvent"

#: ``_schemaURL`` for Consist's own (non-standard) facets.
CONSIST_FACET_SCHEMA_URL = f"{_SPEC_BASE}#/$defs/BaseFacet"

#: ``producer``/``_producer`` URI identifying Consist as the metadata producer.
DEFAULT_PRODUCER = "https://github.com/LBNL-UCB-STI/consist"

_FACETS_BASE = "https://openlineage.io/spec/facets"
_PARENT_RUN_FACET_SCHEMA_URL = (
    f"{_FACETS_BASE}/1-0-0/ParentRunFacet.json#/$defs/ParentRunFacet"
)
_ERROR_MESSAGE_RUN_FACET_SCHEMA_URL = (
    f"{_FACETS_BASE}/1-0-0/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet"
)
_JOB_TYPE_JOB_FACET_SCHEMA_URL = (
    f"{_FACETS_BASE}/2-0-2/JobTypeJobFacet.json#/$defs/JobTypeJobFacet"
)
_DOCUMENTATION_JOB_FACET_SCHEMA_URL = (
    f"{_FACETS_BASE}/1-0-0/DocumentationJobFacet.json#/$defs/DocumentationJobFacet"
)
_DATASOURCE_DATASET_FACET_SCHEMA_URL = (
    f"{_FACETS_BASE}/1-0-0/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet"
)
_DATASET_VERSION_FACET_SCHEMA_URL = (
    f"{_FACETS_BASE}/1-0-0/DatasetVersionDatasetFacet.json"
    "#/$defs/DatasetVersionDatasetFacet"
)
_SCHEMA_DATASET_FACET_SCHEMA_URL = (
    f"{_FACETS_BASE}/1-0-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet"
)
_DOCUMENTATION_DATASET_FACET_SCHEMA_URL = (
    f"{_FACETS_BASE}/1-0-0/DocumentationDatasetFacet.json"
    "#/$defs/DocumentationDatasetFacet"
)


@dataclass(frozen=True)
class OpenLineageOptions:
    enabled: bool
    namespace: str
    path: Path


class OpenLineageEmitter:
    def __init__(
        self,
        options: OpenLineageOptions,
        *,
        producer: str = DEFAULT_PRODUCER,
        schema_resolver: Optional[
            Callable[[Artifact], Optional[tuple[Any, list[Any]]]]
        ] = None,
        run_lookup: Optional[Callable[[str], Optional[Run]]] = None,
        run_facet_resolver: Optional[Callable[[Run], dict[str, Any]]] = None,
    ) -> None:
        self._options = options
        self._producer = producer
        self._schema_resolver = schema_resolver
        self._run_lookup = run_lookup
        self._run_facet_resolver = run_facet_resolver

    def emit_start(
        self, run: Run, *, inputs: list[Artifact], outputs: list[Artifact]
    ) -> None:
        self._emit("START", run, inputs, outputs, error=None)

    def emit_complete(
        self, run: Run, *, inputs: list[Artifact], outputs: list[Artifact]
    ) -> None:
        self._emit("COMPLETE", run, inputs, outputs, error=None)

    def emit_fail(
        self,
        run: Run,
        error: Exception,
        *,
        inputs: list[Artifact],
        outputs: list[Artifact],
    ) -> None:
        self._emit("FAIL", run, inputs, outputs, error=error)

    def _emit(
        self,
        event_type: str,
        run: Run,
        inputs: list[Artifact],
        outputs: list[Artifact],
        error: Exception | None,
    ) -> None:
        if not self._options.enabled:
            return
        payload = self._build_event(event_type, run, inputs, outputs, error)
        self._options.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._options.path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")

    def _build_event(
        self,
        event_type: str,
        run: Run,
        inputs: list[Artifact],
        outputs: list[Artifact],
        error: Exception | None,
    ) -> dict[str, Any]:
        completed_at = run.ended_at
        started_at = run.started_at
        if event_type == "COMPLETE" and completed_at:
            event_time = completed_at
        elif event_type == "START" and started_at:
            event_time = started_at
        else:
            event_time = datetime.now(UTC)

        return {
            "eventType": event_type,
            "eventTime": event_time.isoformat(),
            "producer": self._producer,
            "schemaURL": OPENLINEAGE_SCHEMA_URL,
            "run": {
                "runId": _run_uuid(run.id, self._options.namespace),
                "facets": self._run_facets(run, error),
            },
            "job": {
                "name": _job_name_for_run(run),
                "namespace": self._options.namespace,
                "facets": self._job_facets(run),
            },
            "inputs": [
                self._dataset_from_artifact(artifact, run) for artifact in inputs
            ],
            "outputs": [
                self._dataset_from_artifact(artifact, run) for artifact in outputs
            ],
        }

    def _facet(self, schema_url: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Stamp a facet payload with the ``_producer``/``_schemaURL`` the spec requires."""
        return {
            "_producer": self._producer,
            "_schemaURL": schema_url,
            **payload,
        }

    def _run_facets(self, run: Run, error: Exception | None) -> dict[str, Any]:
        facets: dict[str, Any] = {}
        if self._run_lookup and run.parent_run_id:
            parent_run = self._run_lookup(run.parent_run_id)
            parent_facet = _parent_facet(run, parent_run, self._options.namespace)
            if parent_facet:
                facets["parent"] = self._facet(
                    _PARENT_RUN_FACET_SCHEMA_URL, parent_facet
                )

        consist_facet: dict[str, Any] = {
            "run_id": run.id,
            "model_name": run.model_name,
            "status": run.status,
            "config_hash": run.config_hash,
            "input_hash": run.input_hash,
            "git_hash": run.git_hash,
            "year": run.year,
            "iteration": run.iteration,
            "parent_run_id": run.parent_run_id,
        }
        if self._run_facet_resolver:
            extra = self._run_facet_resolver(run)
            if extra:
                consist_facet.update(extra)
        facets["consist"] = self._facet(
            CONSIST_FACET_SCHEMA_URL,
            {k: v for k, v in consist_facet.items() if v is not None},
        )

        if error is not None:
            facets["errorMessage"] = self._facet(
                _ERROR_MESSAGE_RUN_FACET_SCHEMA_URL,
                {"message": str(error), "programmingLanguage": "python"},
            )

        return facets

    def _job_facets(self, run: Run) -> dict[str, Any]:
        facets: dict[str, Any] = {}
        facets["jobType"] = self._facet(
            _JOB_TYPE_JOB_FACET_SCHEMA_URL,
            {
                "processingType": "BATCH",
                "integration": "CONSIST",
                "jobType": _job_type_for_run(run),
            },
        )
        if run.description:
            facets["documentation"] = self._facet(
                _DOCUMENTATION_JOB_FACET_SCHEMA_URL,
                {"description": run.description},
            )
        return facets

    def _dataset_from_artifact(self, artifact: Artifact, run: Run) -> dict[str, Any]:
        facets: dict[str, Any] = {}
        data_source = _data_source_facet_from_artifact(artifact)
        if data_source:
            facets["dataSource"] = self._facet(
                _DATASOURCE_DATASET_FACET_SCHEMA_URL, data_source
            )
        version = _dataset_version_facet_from_artifact(artifact)
        if version:
            facets["version"] = self._facet(_DATASET_VERSION_FACET_SCHEMA_URL, version)
        schema_facet = _schema_facet_from_artifact(artifact, self._schema_resolver)
        if schema_facet:
            facets["schema"] = self._facet(
                _SCHEMA_DATASET_FACET_SCHEMA_URL, schema_facet
            )
        documentation = _documentation_facet_from_artifact(artifact)
        if documentation:
            facets["documentation"] = self._facet(
                _DOCUMENTATION_DATASET_FACET_SCHEMA_URL, documentation
            )
        consist_facet = _consist_dataset_facet(artifact)
        if consist_facet:
            facets["consist"] = self._facet(CONSIST_FACET_SCHEMA_URL, consist_facet)

        return {
            "namespace": self._options.namespace,
            "name": _dataset_name_from_artifact(artifact, run),
            "facets": facets,
        }


def _run_uuid(run_id: str, namespace: str) -> str:
    """
    Return an OpenLineage-compatible ``runId`` for a Consist run id.

    OpenLineage requires ``run.runId`` to be a UUID, while Consist run ids are
    human-meaningful strings (``"baseline"``, ``"step_a"``). Ids that already are
    UUIDs are passed through; anything else is mapped to a deterministic UUIDv5 of
    ``consist://<namespace>/<run id>``, so repeated emissions of the same run keep
    the same OpenLineage identity. The original id stays available on the
    ``consist`` run facet as ``run_id``.
    """
    try:
        return str(uuid.UUID(str(run_id)))
    except (AttributeError, TypeError, ValueError):
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"consist://{namespace}/{run_id}"))


def _job_name_for_run(run: Run) -> str:
    if run.model_name == "scenario":
        return run.id
    return run.model_name


def _job_type_for_run(run: Run) -> str:
    if run.model_name == "scenario":
        return "consist_scenario"
    return "consist_step"


def _parent_facet(
    run: Run, parent_run: Optional[Run], namespace: str
) -> Optional[dict[str, Any]]:
    if not run.parent_run_id or not parent_run:
        return None
    return {
        "job": {"name": _job_name_for_run(parent_run), "namespace": namespace},
        "run": {"runId": _run_uuid(run.parent_run_id, namespace)},
    }


def _dataset_name_from_artifact(artifact: Artifact, run: Run) -> str:
    year = None
    iteration = None
    if isinstance(artifact.meta, dict):
        year = artifact.meta.get("year")
        iteration = artifact.meta.get("iteration")
    if year is None:
        year = run.year
    if iteration is None:
        iteration = run.iteration
    suffix = ""
    if year is not None:
        suffix = f"_{year}"
    if iteration is not None:
        suffix = f"{suffix}_iteration_{iteration}"
    return f"{artifact.key}{suffix}"


def _schema_facet_from_artifact(
    artifact: Artifact,
    schema_resolver: Optional[Callable[[Artifact], Optional[tuple[Any, list[Any]]]]],
) -> Optional[dict[str, Any]]:
    profile = None
    if isinstance(artifact.meta, dict):
        profile = artifact.meta.get("schema_profile")
    if profile and isinstance(profile, dict) and "fields" in profile:
        return {
            "fields": [
                {"name": field["name"], "type": field.get("logical_type", "")}
                for field in profile["fields"]
            ]
        }
    if schema_resolver:
        result = schema_resolver(artifact)
        if result:
            _, fields = result
            return {
                "fields": [
                    {"name": field.name, "type": field.logical_type} for field in fields
                ]
            }
    return None


def _data_source_facet_from_artifact(artifact: Artifact) -> Optional[dict[str, Any]]:
    if not artifact.driver and not artifact.container_uri:
        return None
    data_source: dict[str, Any] = {}
    if artifact.driver:
        data_source["name"] = artifact.driver
    if artifact.container_uri:
        data_source["uri"] = artifact.container_uri
    return data_source or None


def _dataset_version_facet_from_artifact(
    artifact: Artifact,
) -> Optional[dict[str, Any]]:
    version = None
    # Intentional hash usage: OpenLineage datasetVersion should be a portable
    # content fingerprint, not a Consist-internal ``content_id``.
    if artifact.hash:
        version = artifact.hash
    elif isinstance(artifact.meta, dict):
        version = artifact.meta.get("schema_id")
    if not version:
        return None
    return {"datasetVersion": str(version)}


def _documentation_facet_from_artifact(artifact: Artifact) -> Optional[dict[str, Any]]:
    if not isinstance(artifact.meta, dict):
        return None
    description = artifact.meta.get("description")
    if not description:
        return None
    return {"description": str(description)}


def _consist_dataset_facet(artifact: Artifact) -> Optional[dict[str, Any]]:
    facet: dict[str, Any] = {
        "key": artifact.key,
        "uri": artifact.container_uri,
        "driver": artifact.driver,
        # Intentional exposure of the raw checksum for external/debug consumers.
        "hash": artifact.hash,
    }
    if isinstance(artifact.meta, dict):
        meta = artifact.meta
        facet["schema_id"] = meta.get("schema_id")
        facet["schema_summary"] = meta.get("schema_summary")
        facet["schema_profile"] = meta.get("schema_profile")
        facet["dlt_table_name"] = meta.get("dlt_table_name")
        facet["is_ingested"] = meta.get("is_ingested")
    facet = {k: v for k, v in facet.items() if v is not None}
    return facet or None
