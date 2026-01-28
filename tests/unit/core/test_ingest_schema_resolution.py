from __future__ import annotations

import uuid
from unittest.mock import patch

from sqlmodel import Field, SQLModel

from consist import Tracker
from consist.models.artifact import Artifact


def test_ingest_resolves_schema_from_registered_schemas(tmp_path):
    class MySchema(SQLModel):
        id: int = Field(primary_key=True)
        name: str

    class OtherSchema(SQLModel):
        id: int = Field(primary_key=True)
        name: str

    run_dir = tmp_path / "runs"
    run_dir.mkdir(parents=True, exist_ok=True)
    tracker = Tracker(
        run_dir=run_dir,
        db_path=str(tmp_path / "provenance.db"),
        schemas=[MySchema],
    )

    artifact = Artifact(
        id=uuid.uuid4(),
        key="artifact",
        container_uri="inputs://data.csv",
        driver="csv",
        hash="abc",
        meta={"schema_name": "MySchema"},
    )

    with patch(
        "consist.core.tracker.ingest_artifact", return_value="ok"
    ) as mock_ingest:
        tracker.ingest(artifact, data=[{"id": 1, "name": "alpha"}])
        assert mock_ingest.call_args.kwargs["schema"] is MySchema

    with patch(
        "consist.core.tracker.ingest_artifact", return_value="ok"
    ) as mock_ingest:
        tracker.ingest(
            artifact,
            data=[{"id": 2, "name": "beta"}],
            schema=OtherSchema,
        )
        assert mock_ingest.call_args.kwargs["schema"] is OtherSchema
