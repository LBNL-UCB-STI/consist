from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from consist.core.tracker import Tracker
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


def test_gtfs_schemas_are_registered_and_typed(
    gtfs_tracker: Tracker, tmp_path: Path
) -> None:
    assert gtfs_tracker.registered_schemas.get("GtfsTrips") is GtfsTrips
    assert GtfsTrips.__tablename__ == "trips"
    assert {"feed_key", "route_id", "service_id", "trip_id"}.issubset(
        GtfsTrips.model_fields
    )
    assert GtfsTrips.model_fields["route_id"].is_required() is True
    assert GtfsTrips.model_fields["shape_id"].is_required() is False

    trips_parquet = tmp_path / "trips.parquet"
    pd.DataFrame(
        [{"feed_key": "feed", "trip_id": "T1", "route_id": "R1", "service_id": "S1"}]
    ).to_parquet(trips_parquet, index=False)
    with gtfs_tracker.start_run("gtfs_typed_view", "gtfs"):
        gtfs_tracker.log_artifact(
            str(trips_parquet), key="trips", driver="parquet", schema=GtfsTrips
        )

    gtfs_tracker.create_view("v_trips", "trips")
    rows = gtfs_tracker.query_rows(
        "SELECT feed_key, trip_id, route_id, service_id FROM v_trips",
        model=GtfsTrips,
    )
    assert len(rows) == 1
    assert rows[0].trip_id == "T1"
    assert rows[0].shape_id is None


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

    assert gtfs_tracker.db is not None
    schema_bundle = gtfs_tracker.db.get_artifact_schema_for_artifact(
        artifact_id=artifact.id
    )
    assert schema_bundle is not None
    schema_id = schema_bundle[0].id
    relations = gtfs_tracker.db.get_artifact_schema_relations(schema_id=schema_id)

    assert {
        (relation.from_field, relation.to_table, relation.to_field)
        for relation in relations
    } >= {
        ("route_id", "routes", "route_id"),
        ("service_id", "calendar", "service_id"),
    }
