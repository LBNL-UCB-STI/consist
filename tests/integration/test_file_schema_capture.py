import os

import pandas as pd
from sqlmodel import Session, select

from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation


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


def test_profile_file_artifact_skips_when_schema_id_present(tracker, tmp_path):
    path = tmp_path / "data.csv"
    pd.DataFrame({"a": [1], "b": ["x"]}).to_csv(path, index=False)

    with tracker.start_run("run_profile_skip", model="test_model"):
        art = tracker.log_artifact(path, key="data", direction="output")
        art.meta["schema_id"] = "skip-id"

        tracker.artifact_schemas.profile_file_artifact(
            artifact=art,
            run=tracker.current_consist.run,
            resolved_path=str(path),
            source="file",
            driver="csv",
            sample_rows=1,
        )

    with Session(tracker.engine) as session:
        schemas = session.exec(select(ArtifactSchema)).all()
        observations = session.exec(select(ArtifactSchemaObservation)).all()

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

    with Session(tracker.engine) as session:
        schemas = session.exec(select(ArtifactSchema)).all()
        observations = session.exec(select(ArtifactSchemaObservation)).all()

    schema_a = tracker.db.get_artifact_schema_for_artifact(artifact_id=art_a.id)
    schema_b = tracker.db.get_artifact_schema_for_artifact(artifact_id=art_b.id)
    assert schema_a is not None
    assert schema_b is not None
    assert schema_a[0].id == schema_b[0].id
    assert len(schemas) == 1
    assert len(observations) == 2
