import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from consist.core.openlineage import (
    DEFAULT_PRODUCER,
    OPENLINEAGE_SCHEMA_URL,
    OpenLineageEmitter,
    OpenLineageOptions,
    _dataset_name_from_artifact,
    _run_uuid,
)
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run


def test_openlineage_dataset_name_suffixes():
    run = Run(
        id="r1",
        model_name="step",
        config_hash=None,
        git_hash=None,
        meta={},
        tags=[],
    )
    artifact = Artifact(key="trips", container_uri="inputs://trips.csv", driver="csv")
    artifact.meta = {"year": 2026, "iteration": 1}

    assert _dataset_name_from_artifact(artifact, run) == "trips_2026_iteration_1"


def test_openlineage_jsonl_written(tmp_path: Path):
    run = Run(
        id="r1",
        model_name="step_a",
        config_hash=None,
        git_hash=None,
        meta={},
        tags=[],
        started_at=datetime.now(timezone.utc),
    )
    artifact = Artifact(key="trips", container_uri="inputs://trips.csv", driver="csv")
    artifact.meta = {"year": 2025}

    emitter = OpenLineageEmitter(
        OpenLineageOptions(
            enabled=True, namespace="consist", path=tmp_path / "openlineage.jsonl"
        )
    )
    emitter.emit_start(run, inputs=[artifact], outputs=[])
    emitter.emit_complete(run, inputs=[artifact], outputs=[artifact])

    lines = (tmp_path / "openlineage.jsonl").read_text().strip().splitlines()
    assert len(lines) == 2
    payload = json.loads(lines[0])
    assert payload["eventType"] == "START"
    assert payload["job"]["name"] == "step_a"
    assert payload["job"]["namespace"] == "consist"
    assert payload["run"]["runId"] == _run_uuid("r1", "consist")
    assert payload["run"]["facets"]["consist"]["run_id"] == "r1"
    assert payload["producer"] == DEFAULT_PRODUCER
    assert "eventTime" in payload
    assert payload["job"]["facets"]["jobType"]["jobType"] == "consist_step"


def test_openlineage_schema_facet_from_meta(tmp_path: Path):
    run = Run(
        id="r1",
        model_name="step_a",
        config_hash=None,
        git_hash=None,
        meta={},
        tags=[],
        started_at=datetime.now(timezone.utc),
    )
    artifact = Artifact(key="trips", container_uri="inputs://trips.csv", driver="csv")
    artifact.hash = "hash123"
    artifact.meta = {
        "schema_profile": {"fields": [{"name": "id", "logical_type": "bigint"}]}
    }

    emitter = OpenLineageEmitter(
        OpenLineageOptions(
            enabled=True, namespace="consist", path=tmp_path / "openlineage.jsonl"
        )
    )
    emitter.emit_complete(run, inputs=[], outputs=[artifact])

    payload = json.loads((tmp_path / "openlineage.jsonl").read_text().strip())
    schema = payload["outputs"][0]["facets"]["schema"]
    assert schema["fields"][0]["name"] == "id"
    assert schema["fields"][0]["type"] == "bigint"
    assert payload["outputs"][0]["facets"]["version"]["datasetVersion"] == "hash123"


def test_openlineage_parent_facet_for_scenario(tmp_path: Path):
    tracker = Tracker(run_dir=tmp_path, openlineage_enabled=True)
    with tracker.scenario("baseline") as sc:
        sc.run(lambda: None, name="step_a")

    lines = (tmp_path / "openlineage.jsonl").read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    child_events = [
        evt
        for evt in events
        if evt["job"]["name"] == "step_a" and evt["eventType"] == "START"
    ]
    assert child_events
    parent_facet = child_events[0]["run"]["facets"]["parent"]
    assert parent_facet["job"]["name"] == "baseline"
    assert child_events[0]["run"]["facets"]["consist"]["parent_run_id"] == "baseline"
    assert child_events[0]["job"]["facets"]["jobType"]["jobType"] == "consist_step"

    scenario_events = [
        evt
        for evt in events
        if evt["job"]["name"] == "baseline" and evt["eventType"] == "START"
    ]
    assert scenario_events
    # The parent facet must point at the same runId the scenario itself reports.
    assert parent_facet["run"]["runId"] == scenario_events[0]["run"]["runId"]
    assert (
        scenario_events[0]["job"]["facets"]["jobType"]["jobType"] == "consist_scenario"
    )


def test_openlineage_includes_config_facet(tmp_path: Path):
    tracker = Tracker(run_dir=tmp_path, openlineage_enabled=True)
    tracker.run(
        fn=lambda: None,
        name="step_a",
        config={"region": "north", "year": 2025},
        facet={"region": "north"},
    )

    lines = (tmp_path / "openlineage.jsonl").read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    start_event = next(evt for evt in events if evt["eventType"] == "START")
    consist_facet = start_event["run"]["facets"]["consist"]
    assert consist_facet["config_facet"]["region"] == "north"
    assert "region" in consist_facet["config_keys"]


def _facet_groups(event: dict) -> dict[str, dict]:
    """Collect every facet in an event, keyed by a readable location."""
    groups = {
        f"run.facets.{name}": facet for name, facet in event["run"]["facets"].items()
    }
    groups.update(
        {f"job.facets.{name}": facet for name, facet in event["job"]["facets"].items()}
    )
    for side in ("inputs", "outputs"):
        for dataset in event[side]:
            for name, facet in dataset["facets"].items():
                groups[f"{side}[{dataset['name']}].facets.{name}"] = facet
    return groups


def _is_uri(value: object) -> bool:
    parsed = urlparse(str(value))
    return bool(parsed.scheme and parsed.netloc)


def test_openlineage_events_satisfy_spec_required_fields(tmp_path: Path):
    """
    Emitted events must carry the fields the OpenLineage spec marks as required.

    A ``RunEvent`` requires ``eventTime``/``producer``/``schemaURL`` (the last two as
    URIs) and a UUID ``run.runId``; every facet requires ``_producer`` and
    ``_schemaURL``. Without these, standards-based consumers reject the payload.
    """
    tracker = Tracker(run_dir=tmp_path, openlineage_enabled=True)
    with tracker.scenario("baseline") as sc:
        sc.run(lambda: None, name="step_a")

    lines = (tmp_path / "openlineage.jsonl").read_text().strip().splitlines()
    events = [json.loads(line) for line in lines]
    assert events

    for event in events:
        assert event["schemaURL"] == OPENLINEAGE_SCHEMA_URL
        assert _is_uri(event["schemaURL"])
        assert _is_uri(event["producer"])
        assert datetime.fromisoformat(event["eventTime"]).tzinfo is not None
        # Raises ValueError if the runId is not a UUID.
        uuid.UUID(event["run"]["runId"])

        for location, facet in _facet_groups(event).items():
            assert _is_uri(facet.get("_producer")), location
            assert _is_uri(facet.get("_schemaURL")), location


def test_openlineage_job_type_facet_declares_processing_and_integration(tmp_path: Path):
    """``JobTypeJobFacet`` requires both ``processingType`` and ``integration``."""
    tracker = Tracker(run_dir=tmp_path, openlineage_enabled=True)
    tracker.run(fn=lambda: None, name="step_a")

    lines = (tmp_path / "openlineage.jsonl").read_text().strip().splitlines()
    start_event = next(
        json.loads(line) for line in lines if json.loads(line)["eventType"] == "START"
    )
    job_type = start_event["job"]["facets"]["jobType"]
    assert job_type["processingType"] == "BATCH"
    assert job_type["integration"] == "CONSIST"
    assert job_type["jobType"] == "consist_step"


def test_openlineage_dataset_facets_are_stamped(tmp_path: Path):
    """Dataset facets are facets too, so they carry the required base fields."""
    run = Run(
        id="r1",
        model_name="step_a",
        config_hash=None,
        git_hash=None,
        meta={},
        tags=[],
        started_at=datetime.now(timezone.utc),
    )
    artifact = Artifact(key="trips", container_uri="inputs://trips.csv", driver="csv")
    artifact.hash = "hash123"
    artifact.meta = {
        "description": "trip records",
        "schema_profile": {"fields": [{"name": "id", "logical_type": "bigint"}]},
    }

    emitter = OpenLineageEmitter(
        OpenLineageOptions(
            enabled=True, namespace="consist", path=tmp_path / "openlineage.jsonl"
        )
    )
    emitter.emit_complete(run, inputs=[artifact], outputs=[artifact])

    payload = json.loads((tmp_path / "openlineage.jsonl").read_text().strip())
    facets = payload["outputs"][0]["facets"]
    assert set(facets) == {
        "dataSource",
        "version",
        "schema",
        "documentation",
        "consist",
    }
    for name, facet in facets.items():
        assert facet["_producer"] == DEFAULT_PRODUCER, name
        assert _is_uri(facet["_schemaURL"]), name
    # Stamping must not disturb the facet payloads themselves.
    assert facets["version"]["datasetVersion"] == "hash123"
    assert facets["schema"]["fields"][0]["name"] == "id"


def test_run_uuid_is_deterministic_and_passes_through_uuids():
    first = _run_uuid("baseline", "consist")
    assert first == _run_uuid("baseline", "consist")
    # Namespacing keeps identically-named runs in different projects distinct.
    assert first != _run_uuid("baseline", "other-project")
    # An id that is already a UUID is preserved as-is.
    existing = "8f0d0f16-2f6d-4a1e-9a4a-0f6f4b9a5f1e"
    assert _run_uuid(existing, "consist") == existing
