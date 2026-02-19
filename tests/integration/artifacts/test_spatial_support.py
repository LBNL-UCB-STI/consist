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


def test_spatial_metadata_filters_by_run_ids_and_year(tracker: Tracker) -> None:
    """
    Spatial metadata queries should honor `run_ids` and `year` filters together.

    This serves as living documentation for how `SpatialMetadataView.get_metadata`
    narrows results in end-to-end usage after artifact ingestion.
    """
    geojson_path = (
        Path(__file__).resolve().parents[2] / "resources/spatial/tiny.geojson"
    )

    with tracker.start_run("spatial_run_2020", model="test_model", year=2020):
        artifact_2020 = tracker.log_artifact(
            str(geojson_path),
            key="tiny_spatial_filters",
            driver="geojson",
            direction="output",
        )
        tracker.ingest(artifact_2020)

    with tracker.start_run("spatial_run_2021", model="test_model", year=2021):
        artifact_2021 = tracker.log_artifact(
            str(geojson_path),
            key="tiny_spatial_filters",
            driver="geojson",
            direction="output",
        )
        tracker.ingest(artifact_2021)

    view = tracker.spatial_metadata("tiny_spatial_filters")

    only_2020 = view.get_metadata("tiny_spatial_filters", year=2020)
    assert set(only_2020["run_id"]) == {"spatial_run_2020"}
    assert set(only_2020["year"]) == {2020}

    run_id_and_year = view.get_metadata(
        "tiny_spatial_filters",
        run_ids=["spatial_run_2021"],
        year=2021,
    )
    assert set(run_id_and_year["run_id"]) == {"spatial_run_2021"}
    assert set(run_id_and_year["year"]) == {2021}

    non_matching = view.get_metadata(
        "tiny_spatial_filters",
        run_ids=["spatial_run_2020"],
        year=2021,
    )
    assert non_matching.empty
