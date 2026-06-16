from __future__ import annotations

import warnings

from consist.core.views import create_view_model
from consist.models.gtfs import GtfsTrips


def test_create_view_model_is_idempotent() -> None:
    first = create_view_model(GtfsTrips, name="v_gtfs_trips")

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        second = create_view_model(GtfsTrips, name="v_gtfs_trips")

    assert first is second
    assert captured == []


def test_create_view_model_distinct_names_do_not_collide() -> None:
    first = create_view_model(GtfsTrips, name="v_gtfs_trips_a")

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        second = create_view_model(GtfsTrips, name="v_gtfs_trips_b")

    assert first is not second
    assert first.__name__ != second.__name__
    assert captured == []
