"""Tests for artifact type guards and driver type checking."""

from __future__ import annotations

import pytest
from consist import (
    is_dataframe_artifact,
    is_tabular_artifact,
    is_json_artifact,
    is_zarr_artifact,
    is_hdf_artifact,
    DriverType,
)
from consist.models.artifact import Artifact
from consist.api import (
    ArtifactLike,
    DataFrameArtifact,
    TabularArtifact,
    JsonArtifact,
    ZarrArtifact,
    HdfStoreArtifact,
)


def _artifact(driver: str) -> Artifact:
    """Create a test artifact with the given driver."""
    return Artifact(
        key="test",
        uri="workspace://test.txt",
        driver=driver,
    )


class TestDriverType:
    """Test the DriverType enum."""

    def test_driver_type_has_all_known_drivers(self) -> None:
        """Verify DriverType enum contains all known drivers."""
        expected = {"parquet", "csv", "zarr", "json", "h5_table", "h5", "hdf5", "other"}
        actual = {dt.value for dt in DriverType}
        assert actual == expected

    def test_driver_type_enum_members_are_strings(self) -> None:
        """Verify all DriverType members are string values."""
        for dt in DriverType:
            assert isinstance(dt.value, str)
            # Verify member name matches value in expected format
            assert dt.value in ("parquet", "csv", "zarr", "json", "h5_table", "h5", "hdf5", "other")

    def test_driver_type_comparison(self) -> None:
        """Verify DriverType enums can be compared by value."""
        assert DriverType.PARQUET.value == "parquet"
        assert DriverType.ZARR.value == "zarr"
        # Verify string comparison works because DriverType(str, Enum)
        assert DriverType.CSV == "csv"


class TestTypeGuards:
    """Test artifact type guard functions."""

    def test_is_dataframe_artifact_parquet(self) -> None:
        """Verify is_dataframe_artifact recognizes parquet."""
        art = _artifact("parquet")
        assert is_dataframe_artifact(art) is True

    def test_is_dataframe_artifact_csv(self) -> None:
        """Verify is_dataframe_artifact recognizes csv."""
        art = _artifact("csv")
        assert is_dataframe_artifact(art) is True

    def test_is_dataframe_artifact_h5_table(self) -> None:
        """Verify is_dataframe_artifact recognizes h5_table."""
        art = _artifact("h5_table")
        assert is_dataframe_artifact(art) is True

    def test_is_dataframe_artifact_rejects_json(self) -> None:
        """Verify is_dataframe_artifact rejects JSON (use is_tabular_artifact instead)."""
        art = _artifact("json")
        assert is_dataframe_artifact(art) is False

    def test_is_dataframe_artifact_rejects_zarr(self) -> None:
        """Verify is_dataframe_artifact rejects zarr."""
        art = _artifact("zarr")
        assert is_dataframe_artifact(art) is False

    def test_is_tabular_artifact_includes_json(self) -> None:
        """Verify is_tabular_artifact recognizes all tabular formats including json."""
        for driver in ("parquet", "csv", "h5_table", "json"):
            art = _artifact(driver)
            assert is_tabular_artifact(art) is True, f"Expected {driver} to be tabular"

    def test_is_tabular_artifact_rejects_zarr(self) -> None:
        """Verify is_tabular_artifact rejects zarr."""
        art = _artifact("zarr")
        assert is_tabular_artifact(art) is False

    def test_is_json_artifact(self) -> None:
        """Verify is_json_artifact recognizes json."""
        art = _artifact("json")
        assert is_json_artifact(art) is True

    def test_is_json_artifact_rejects_csv(self) -> None:
        """Verify is_json_artifact rejects non-json."""
        art = _artifact("csv")
        assert is_json_artifact(art) is False

    def test_is_zarr_artifact(self) -> None:
        """Verify is_zarr_artifact recognizes zarr."""
        art = _artifact("zarr")
        assert is_zarr_artifact(art) is True

    def test_is_zarr_artifact_rejects_other(self) -> None:
        """Verify is_zarr_artifact rejects non-zarr."""
        art = _artifact("parquet")
        assert is_zarr_artifact(art) is False

    def test_is_hdf_artifact_h5(self) -> None:
        """Verify is_hdf_artifact recognizes h5."""
        art = _artifact("h5")
        assert is_hdf_artifact(art) is True

    def test_is_hdf_artifact_hdf5(self) -> None:
        """Verify is_hdf_artifact recognizes hdf5."""
        art = _artifact("hdf5")
        assert is_hdf_artifact(art) is True

    def test_is_hdf_artifact_rejects_h5_table(self) -> None:
        """Verify is_hdf_artifact rejects h5_table (different from h5/hdf5)."""
        art = _artifact("h5_table")
        assert is_hdf_artifact(art) is False


class TestRuntimeCheckable:
    """Test @runtime_checkable protocol support."""

    def test_artifact_isinstance_artifactlike(self) -> None:
        """Verify Artifact instances pass isinstance checks for ArtifactLike."""
        art = _artifact("parquet")
        assert isinstance(art, ArtifactLike)

    def test_artifact_isinstance_dataframe_artifact(self) -> None:
        """Verify parquet Artifact passes isinstance check for DataFrameArtifact."""
        art = _artifact("parquet")
        assert isinstance(art, DataFrameArtifact)

    def test_artifact_isinstance_zarr_artifact(self) -> None:
        """Verify zarr Artifact passes isinstance check for ZarrArtifact."""
        art = _artifact("zarr")
        assert isinstance(art, ZarrArtifact)

    def test_artifact_isinstance_json_artifact(self) -> None:
        """Verify json Artifact passes isinstance check for JsonArtifact."""
        art = _artifact("json")
        assert isinstance(art, JsonArtifact)

    def test_artifact_isinstance_hdf_artifact(self) -> None:
        """Verify h5 Artifact passes isinstance check for HdfStoreArtifact."""
        art = _artifact("h5")
        assert isinstance(art, HdfStoreArtifact)

    def test_artifact_isinstance_tabular_artifact(self) -> None:
        """Verify tabular Artifacts pass isinstance check for TabularArtifact."""
        for driver in ("parquet", "csv", "h5_table", "json"):
            art = _artifact(driver)
            assert isinstance(art, TabularArtifact), f"Expected {driver} to be TabularArtifact"

    def test_isinstance_checks_structural_typing_only(self) -> None:
        """Verify protocol checks use structural typing (attributes only, not Literal values).

        Note: @runtime_checkable protocols only validate that attributes exist,
        not their specific values. Use type guards (is_dataframe_artifact, etc.)
        for runtime validation of Literal type constraints.
        """
        # All Artifact instances pass isinstance for all protocols
        # because they all have the required attributes (driver, uri, meta, path)
        zarr_art = _artifact("zarr")
        parquet_art = _artifact("parquet")

        # Both pass structural checks (have driver, uri, meta, path)
        assert isinstance(zarr_art, ArtifactLike)
        assert isinstance(parquet_art, ArtifactLike)

        # Both also pass protocol checks (because protocols only check structure)
        assert isinstance(zarr_art, DataFrameArtifact)
        assert isinstance(parquet_art, DataFrameArtifact)

        # Use type guards for actual Literal type validation
        assert is_zarr_artifact(zarr_art)
        assert not is_zarr_artifact(parquet_art)
        assert is_dataframe_artifact(parquet_art)
        assert not is_dataframe_artifact(zarr_art)
