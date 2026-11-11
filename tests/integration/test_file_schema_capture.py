import os

import pandas as pd
from typer.testing import CliRunner

from consist.cli import app
from consist.core.tracker import Tracker
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation
from tests.helpers.artifact_factory import make_artifact


def test_profile_file_artifact_persists_schema(tracker, tmp_path):
    path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1], "b": ["x"]}).to_csv(path, index=False)

    with tracker.start_run("run_profile", model="test_model"):
        art = tracker.log_artifact(path, key="data", direction="output")
        run = tracker.current_consist.run

        tracker.artifact_schemas.profile_file_artifact(
            artifact=art,
            run=run,
            resolved_path=str(path),
            source="file",
            driver="csv",
            sample_rows=1,
        )

        fetched = tracker.db.get_artifact_schema_for_artifact(artifact_id=art.id)
        assert fetched is not None


def test_log_artifact_profiles_schema_when_enabled(tracker, tmp_path):
    path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1], "b": ["x"]}).to_csv(path, index=False)

    with tracker.start_run("run_profile_log", model="test_model"):
        art = tracker.log_artifact(
            path,
            key="data",
            direction="output",
            profile_file_schema=True,
        )

        fetched = tracker.db.get_artifact_schema_for_artifact(artifact_id=art.id)
        assert fetched is not None


def test_cli_schema_uses_db_when_file_missing(tracker, tmp_path):
    path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1]}).to_csv(path, index=False)

    with tracker.start_run("run_profile_missing", model="test_model"):
        art = tracker.log_artifact(
            path,
            key="data",
            direction="output",
            profile_file_schema=True,
        )

    os.remove(path)

    fetched = tracker.db.get_artifact_schema_for_artifact(artifact_id=art.id)
    assert fetched is not None


def test_cli_schema_capture_file_profiles_existing_artifact(tracker, run_dir):
    path = run_dir / "data_capture.csv"
    pd.DataFrame({"a": [1], "b": ["x"]}).to_csv(path, index=False)

    with tracker.start_run("run_capture_cli", model="test_model"):
        art = tracker.log_artifact(path, key="capture_data", direction="output")

    assert tracker.db.get_artifact_schema_for_artifact(artifact_id=art.id) is None

    db_path = str(tracker.db_path)
    tracker.close()

    result = CliRunner().invoke(
        app,
        [
            "schema",
            "capture-file",
            "--artifact-key",
            "capture_data",
            "--db-path",
            db_path,
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert "Captured file schema for artifact 'capture_data'" in result.stdout

    reopened = Tracker(run_dir=run_dir, db_path=db_path)
    try:
        fetched = reopened.db.get_artifact_schema_for_artifact(artifact_id=art.id)
        assert fetched is not None
        schema, _ = fetched
        observations = reopened.query_rows(
            "SELECT * FROM main.artifact_schema_observation WHERE schema_id = ?",
            [schema.id],
            model=ArtifactSchemaObservation,
        )
        assert any(
            obs.artifact_id == art.id and obs.source == "file" for obs in observations
        )
    finally:
        reopened.close()


def test_profile_file_artifact_skips_when_schema_id_present(tracker, tmp_path):
    path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1], "b": ["x"]}).to_csv(path, index=False)

    with tracker.start_run("run_profile_skip", model="test_model"):
        art = tracker.log_artifact(path, key="data", direction="output")
        art = art.with_schema("skip-id")

        tracker.artifact_schemas.profile_file_artifact(
            artifact=art,
            run=tracker.current_consist.run,
            resolved_path=str(path),
            source="file",
            driver="csv",
            sample_rows=1,
        )

    schemas = tracker.query_rows(
        "SELECT * FROM main.artifact_schema", model=ArtifactSchema
    )
    observations = tracker.query_rows(
        "SELECT * FROM main.artifact_schema_observation",
        model=ArtifactSchemaObservation,
    )

    assert schemas == []
    assert observations == []


def test_log_artifact_profiles_schema_if_changed(tracker, tmp_path):
    path_a = tmp_path / "data_a.csv"
    path_b = tmp_path / "data_b.csv"
    df = pd.DataFrame({"a": [1], "b": ["x"]})
    df.to_csv(path_a, index=False)
    df.to_csv(path_b, index=False)

    with tracker.start_run("run_profile_if_changed_a", model="test_model"):
        art_a = tracker.log_artifact(
            path_a,
            key="data",
            direction="output",
            profile_file_schema=True,
        )

    with tracker.start_run("run_profile_if_changed_b", model="test_model"):
        art_b = tracker.log_artifact(
            path_b,
            key="data",
            direction="output",
            profile_file_schema="if_changed",
        )

    schemas = tracker.query_rows(
        "SELECT * FROM main.artifact_schema", model=ArtifactSchema
    )
    observations = tracker.query_rows(
        "SELECT * FROM main.artifact_schema_observation",
        model=ArtifactSchemaObservation,
    )

    schema_a = tracker.db.get_artifact_schema_for_artifact(artifact_id=art_a.id)
    schema_b = tracker.db.get_artifact_schema_for_artifact(artifact_id=art_b.id)
    assert schema_a is not None
    assert schema_b is not None
    assert art_a.identity.anchor == art_b.identity.anchor
    assert schema_a[0].id == schema_b[0].id
    assert len(schemas) == 1
    assert len(observations) == 2


def test_profile_file_artifact_if_changed_reuses_by_anchor(tracker, tmp_path):
    path_a = tmp_path / "data_content_a.csv"
    path_b = tmp_path / "data_content_b.csv"
    df = pd.DataFrame({"a": [1], "b": ["x"]})
    df.to_csv(path_a, index=False)
    df.to_csv(path_b, index=False)

    with tracker.start_run("run_profile_content_id_a", model="test_model"):
        art_a = tracker.log_artifact(path_a, key="data", direction="output")
        run_a = tracker.current_consist.run
        tracker.artifact_schemas.profile_file_artifact(
            artifact=art_a,
            run=run_a,
            resolved_path=str(path_a),
            source="file",
            driver="csv",
            sample_rows=1,
        )

    with tracker.start_run("run_profile_content_id_b", model="test_model"):
        art_b = tracker.log_artifact(path_b, key="data", direction="output")
        run_b = tracker.current_consist.run

        assert art_b.identity.anchor == art_a.identity.anchor

        tracker.artifact_schemas.profile_file_artifact(
            artifact=art_b,
            run=run_b,
            resolved_path=str(path_b),
            source="file",
            driver="csv",
            sample_rows=1,
            reuse_if_unchanged=True,
        )

    schema_a = tracker.db.get_artifact_schema_for_artifact(artifact_id=art_a.id)
    schema_b = tracker.db.get_artifact_schema_for_artifact(artifact_id=art_b.id)

    assert schema_a is not None
    assert schema_b is not None
    assert schema_a[0].id == schema_b[0].id


def test_profile_file_artifact_if_changed_reuses_preexisting_anchor_observation(
    tracker, tmp_path
):
    path_a = tmp_path / "data_legacy_a.csv"
    path_b = tmp_path / "data_legacy_b.csv"
    df = pd.DataFrame({"a": [1], "b": ["x"]})
    df.to_csv(path_a, index=False)
    df.to_csv(path_b, index=False)

    with tracker.start_run("run_profile_legacy_a", model="test_model"):
        art_a = tracker.log_artifact(path_a, key="data", direction="output")
        run_a = tracker.current_consist.run

    legacy_artifact = make_artifact(
        key="data_legacy",
        container_uri="outputs://legacy_data.csv",
        driver="csv",
        identity=art_a.identity,
        run_id=run_a.id,
    )
    legacy_schema = ArtifactSchema(
        id="schema_legacy_hash_only",
        summary_json={"table_name": "legacy_table"},
        profile_version=1,
    )
    legacy_obs = ArtifactSchemaObservation(
        artifact_id=legacy_artifact.id,
        schema_id=legacy_schema.id,
        run_id=run_a.id,
        source="file",
    )
    tracker.db.upsert_artifact(legacy_artifact)
    tracker.db.persist_artifact_schema(legacy_schema, observation=legacy_obs)

    with tracker.start_run("run_profile_legacy_b", model="test_model"):
        art_b = tracker.log_artifact(path_b, key="data", direction="output")
        run_b = tracker.current_consist.run

        assert art_b.identity.anchor == art_a.identity.anchor

        tracker.artifact_schemas.profile_file_artifact(
            artifact=art_b,
            run=run_b,
            resolved_path=str(path_b),
            source="file",
            driver="csv",
            sample_rows=1,
            reuse_if_unchanged=True,
        )

    schema_b = tracker.db.get_artifact_schema_for_artifact(artifact_id=art_b.id)

    assert schema_b is not None
    assert schema_b[0].id == "schema_legacy_hash_only"
