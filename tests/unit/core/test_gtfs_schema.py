from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd
from sqlmodel import Session, select

from consist.core.tracker import Tracker
from consist.models.artifact_schema import ArtifactSchemaRelation
from consist.models.gtfs import GtfsTrips


def _write_gtfs_zip(path: Path) -> Path:
    rows = pd.DataFrame(
        [
            {"trip_id": "T1", "route_id": "R1", "service_id": "S1"},
        ]
    )
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("trips.txt", rows.to_csv(index=False))
    return path


def test_gtfs_schemas_are_not_registered_by_default(tracker: Tracker) -> None:
    assert "GtfsTrips" not in tracker.registered_schemas


def test_gtfs_schemas_are_registered_and_typed(gtfs_tracker: Tracker) -> None:
    assert "GtfsTrips" in gtfs_tracker.registered_schemas
    view_model = gtfs_tracker.view(GtfsTrips)

    assert view_model.__tablename__ == "v_trips"
    assert {"feed_key", "route_id", "service_id", "trip_id"}.issubset(
        view_model.model_fields
    )
    assert view_model.model_fields["route_id"].is_required() is True
    assert view_model.model_fields["shape_id"].is_required() is False


def test_gtfs_schema_profile_records_fk_relations(
    gtfs_tracker: Tracker, tmp_path: Path
) -> None:
    feed_zip = _write_gtfs_zip(tmp_path / "gtfs.zip")

    with gtfs_tracker.start_run("gtfs_schema_profile", "gtfs"):
        artifact = gtfs_tracker.log_artifact(
            str(feed_zip),
            key="trips_feed",
            driver="gtfs",
            table_path="trips.txt",
            schema=GtfsTrips,
            profile_file_schema=False,
        )

    schema_bundle = gtfs_tracker.db.get_artifact_schema_for_artifact(
        artifact_id=artifact.id
    )
    assert schema_bundle is not None
    schema_id = schema_bundle[0].id
    with Session(gtfs_tracker.engine) as session:
        relations = session.exec(
            select(ArtifactSchemaRelation).where(
                ArtifactSchemaRelation.schema_id == schema_id
            )
        ).all()

    assert {
        (relation.from_field, relation.to_table, relation.to_field)
        for relation in relations
    } >= {
        ("route_id", "routes", "route_id"),
        ("service_id", "calendar", "service_id"),
    }
