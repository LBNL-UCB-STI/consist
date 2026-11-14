from __future__ import annotations

from pathlib import Path

import pandas as pd

import pytest

import consist
from consist.core.tracker import Tracker
from consist.errors import DeprecatedViewsError
from consist.models.gtfs import GtfsTrips


def test_legacy_view_helpers_removed_from_tracker_and_public_api(
    tracker: Tracker,
) -> None:
    assert "view" not in dir(consist)
    assert "register_views" not in dir(consist)
    with pytest.warns(DeprecationWarning):
        with pytest.raises(DeprecatedViewsError):
            tracker.view(GtfsTrips)
    with pytest.warns(DeprecationWarning):
        with pytest.raises(DeprecatedViewsError):
            tracker.register_views(GtfsTrips)
    with pytest.warns(DeprecationWarning):
        with pytest.raises(DeprecatedViewsError):
            consist.view(GtfsTrips)
    with pytest.warns(DeprecationWarning):
        with pytest.raises(DeprecatedViewsError):
            consist.register_views(GtfsTrips)


def test_create_view_and_query_rows_with_typed_model(
    gtfs_tracker: Tracker, tmp_path: Path
) -> None:
    trips_parquet = tmp_path / "trips.parquet"
    pd.DataFrame(
        [
            {
                "feed_key": "feed_a",
                "trip_id": "T1",
                "route_id": "R1",
                "service_id": "S1",
            },
            {
                "feed_key": "feed_a",
                "trip_id": "T2",
                "route_id": "R2",
                "service_id": "S1",
            },
        ]
    ).to_parquet(trips_parquet, index=False)

    with gtfs_tracker.start_run("gtfs_view_run", "gtfs"):
        gtfs_tracker.log_artifact(
            str(trips_parquet),
            key="trips",
            driver="parquet",
            schema=GtfsTrips,
        )

    gtfs_tracker.create_view("v_gtfs_trips_a", "trips")
    gtfs_tracker.create_view("v_gtfs_trips_b", "trips")

    rows_a = gtfs_tracker.query_rows(
        "SELECT feed_key, trip_id, route_id, service_id FROM v_gtfs_trips_a ORDER BY trip_id",
        model=GtfsTrips,
    )
    rows_b = gtfs_tracker.query_rows(
        "SELECT feed_key, trip_id, route_id, service_id FROM v_gtfs_trips_b ORDER BY trip_id",
        model=GtfsTrips,
    )

    assert len(rows_a) == 2
    assert all(isinstance(row, GtfsTrips) for row in rows_a)
    assert [row.trip_id for row in rows_a] == ["T1", "T2"]
    assert [row.route_id for row in rows_b] == ["R1", "R2"]
