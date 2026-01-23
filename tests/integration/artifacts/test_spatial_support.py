"""Integration tests for spatial artifact support."""

from pathlib import Path

import pytest

from consist.core.tracker import Tracker

gpd = pytest.importorskip("geopandas")


def test_spatial_load_and_metadata_ingestion(tracker: Tracker) -> None:
    """
    Tests loading and metadata ingestion for spatial artifacts.

    Verifies:
    1. load() returns GeoDataFrame for geojson driver
    2. Spatial metadata is ingested and queryable
    """
    geojson_path = (
        Path(__file__).resolve().parents[2] / "resources/spatial/tiny.geojson"
    )

    with tracker.start_run("run_spatial", model="test_model"):
        artifact = tracker.log_artifact(
            str(geojson_path),
            key="tiny_spatial",
            driver="geojson",
            direction="output",
        )

        loaded = tracker.load(artifact)
        assert isinstance(loaded, gpd.GeoDataFrame)
        assert len(loaded) == 2

        tracker.ingest(artifact)

    view = tracker.spatial_metadata("tiny_spatial")
    metadata = view.get_metadata("tiny_spatial")

    assert not metadata.empty
    row = metadata.iloc[0]
    assert row.feature_count == 2
    assert row.crs == "EPSG:4326"
    assert "Point" in row.geometry_types
    assert "Polygon" in row.geometry_types
