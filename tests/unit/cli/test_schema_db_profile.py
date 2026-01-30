from __future__ import annotations

import uuid
from pathlib import Path

from sqlmodel import Session

from consist import Tracker
from consist.cli import ConsistShell
from consist.models.artifact import Artifact
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
)


def test_schema_command_uses_db_profile_when_available(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "schema.db"
    tracker = Tracker(run_dir=tmp_path / "run", db_path=str(db_path))

    schema_id = "schema123"
    artifact_id = uuid.uuid4()

    schema = ArtifactSchema(
        id=schema_id, summary_json={"column_count": 1}, profile_version=1
    )
    field = ArtifactSchemaField(
        schema_id=schema_id,
        ordinal_position=1,
        name="col",
        logical_type="integer",
        nullable=False,
    )
    artifact = Artifact(
        id=artifact_id,
        key="test_artifact",
        container_uri="inputs://data.csv",
        driver="csv",
        hash="abc",
        meta={"schema_id": schema_id},
    )

    observation = ArtifactSchemaObservation(
        artifact_id=artifact_id,
        schema_id=schema_id,
        source="duckdb",
    )

    with Session(tracker.engine) as session:
        session.add(schema)
        session.add(field)
        session.add(artifact)
        session.add(observation)
        session.commit()

    shell = ConsistShell(tracker)
    shell.do_schema_profile("test_artifact")

    captured = capsys.readouterr().out
    assert "Schema: test_artifact" in captured
    assert "col" in captured
    assert "integer" in captured
