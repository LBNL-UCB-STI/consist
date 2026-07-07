"""Integration tests for spatial artifact support."""

import importlib
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from consist import is_spatial_artifact
from consist.core.tracker import Tracker


def _write_geoparquet_fixture(tmp_path: Path, gpd):
    shapely_geometry: Any = importlib.import_module("shapely.geometry")
    Point = shapely_geometry.Point
    Polygon = shapely_geometry.Polygon

    gdf = gpd.GeoDataFrame(
        {
            "name": ["point", "polygon"],
            "value": [1, 2],
            "geometry": [
                Point(-122.3321, 47.6062),
                Polygon(
                    [
                        (-122.34, 47.60),
                        (-122.30, 47.60),
                        (-122.30, 47.63),
                        (-122.34, 47.63),
                    ]
                ),
            ],
        },
        crs="EPSG:4326",
    )
    parquet_path = tmp_path / "tiny.parquet"
    gdf.to_parquet(parquet_path)
    return gdf, parquet_path


def test_spatial_load_and_metadata_ingestion_for_geoparquet(
    tracker: Tracker, tmp_path: Path
) -> None:
    """GeoParquet should load through geopandas and ingest spatial metadata."""
    gpd = pytest.importorskip("geopandas")
    pytest.importorskip("pyarrow")
    expected_gdf, parquet_path = _write_geoparquet_fixture(tmp_path, gpd)

    with tracker.start_run("run_spatial", model="test_model"):
        artifact = tracker.log_artifact(
            str(parquet_path),
            key="tiny_spatial",
            driver="geoparquet",
            direction="output",
        )

        loaded = tracker.load(artifact)
        assert isinstance(loaded, gpd.GeoDataFrame)
        assert list(loaded["name"]) == list(expected_gdf["name"])
        assert len(loaded) == len(expected_gdf)

        tracker.ingest(artifact)

    view = tracker.spatial_metadata("tiny_spatial")
    metadata = view.get_metadata("tiny_spatial")

    assert not metadata.empty
    row = metadata.iloc[0]
    assert row.feature_count == 2
    assert row.crs == "EPSG:4326"
    assert "Point" in row.geometry_types
    assert "Polygon" in row.geometry_types
    assert row.geometry_column == "geometry"
    assert "geometry" in row.column_names


def test_parquet_inference_remains_non_spatial(
    tracker: Tracker, tmp_path: Path
) -> None:
    """A plain .parquet artifact should still infer the generic parquet driver."""
    pytest.importorskip("pyarrow")
    parquet_path = tmp_path / "tiny.parquet"
    pd.DataFrame({"name": ["point", "polygon"], "value": [1, 2]}).to_parquet(
        parquet_path
    )

    with tracker.start_run("run_parquet", model="test_model"):
        artifact = tracker.log_artifact(
            str(parquet_path),
            key="tiny_parquet",
            direction="output",
        )

    assert artifact.driver == "parquet"
    assert is_spatial_artifact(artifact) is False


def test_spatial_metadata_filters_by_run_ids_and_year(tracker: Tracker) -> None:
    """
    Spatial metadata queries should honor `run_ids` and `year` filters together.

    This serves as living documentation for how `SpatialMetadataView.get_metadata`
    narrows results in end-to-end usage after artifact ingestion.
    """
    pytest.importorskip("geopandas")
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
