from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest
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


def test_registered_schema_lookup_api(tmp_path):
    class LookupSchema(SQLModel):
        id: int = Field(primary_key=True)
        name: str

    class FallbackSchema(SQLModel):
        id: int = Field(primary_key=True)

    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.db"),
        schemas=[LookupSchema],
    )

    assert tracker.get_registered_schema("LookupSchema") is LookupSchema
    assert tracker.get_registered_schema("UnknownSchema") is None
    assert tracker.get_registered_schema("UnknownSchema", default=FallbackSchema) is (
        FallbackSchema
    )

    registry = tracker.registered_schemas
    assert registry["LookupSchema"] is LookupSchema
    assert list(registry.keys()) == ["LookupSchema"]

    with pytest.raises(TypeError):
        registry["Other"] = FallbackSchema

    with pytest.raises(TypeError, match="must be a string"):
        tracker.get_registered_schema(123)
    with pytest.raises(ValueError, match="non-empty string"):
        tracker.get_registered_schema("   ")


def test_ingest_ignores_whitespace_schema_name(tmp_path):
    class MySchema(SQLModel):
        id: int = Field(primary_key=True)
        name: str

    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.db"),
        schemas=[MySchema],
    )

    artifact = Artifact(
        id=uuid.uuid4(),
        key="artifact",
        container_uri="inputs://data.csv",
        driver="csv",
        hash="abc",
        meta={"schema_name": "   "},
    )

    with patch(
        "consist.core.tracker.ingest_artifact", return_value="ok"
    ) as mock_ingest:
        tracker.ingest(artifact, data=[{"id": 1, "name": "alpha"}])
        assert mock_ingest.call_args.kwargs["schema"] is None
