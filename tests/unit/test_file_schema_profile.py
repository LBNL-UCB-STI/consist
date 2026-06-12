import pandas as pd
import pytest

from consist.core.identity import IdentityManager
from consist.tools.schema_profile import MAX_FIELDS, profile_file_schema


def test_profile_file_schema_csv(tmp_path):
    path = tmp_path / "sample.csv"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(path, index=False)

    result = profile_file_schema(
        identity=IdentityManager(),
        path=str(path),
        driver="csv",
        sample_rows=2,
        source="file",
    )
    assert result.schema_id
    assert result.summary["n_columns"] == 2
    assert [field.name for field in result.fields] == ["a", "b"]


def test_profile_file_schema_truncates_large_field_count(tmp_path):
    df = pd.DataFrame({f"c{i}": [i] for i in range(MAX_FIELDS + 1)})
    path = tmp_path / "wide.csv"
    df.to_csv(path, index=False)

    result = profile_file_schema(
        identity=IdentityManager(),
        path=str(path),
        driver="csv",
        sample_rows=1,
        source="file",
    )
    assert result.summary["truncated"]["fields"] is True


def test_profile_file_schema_parquet(tmp_path):
    pytest = __import__("pytest")
    pytest.importorskip("pyarrow")

    path = tmp_path / "sample.parquet"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_parquet(path, index=False)

    result = profile_file_schema(
        identity=IdentityManager(),
        path=str(path),
        driver="parquet",
        sample_rows=2,
        source="file",
    )
    assert result.schema_id
    assert result.summary["n_columns"] == 2
    assert [field.name for field in result.fields] == ["a", "b"]


def test_profile_file_schema_parquet_index_columns(tmp_path):
    pytest = __import__("pytest")
    pytest.importorskip("pyarrow")

    path = tmp_path / "sample_index.parquet"
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).set_index("a")
    df.to_parquet(path)

    result = profile_file_schema(
        identity=IdentityManager(),
        path=str(path),
        driver="parquet",
        sample_rows=2,
        source="file",
    )
    assert set(field.name for field in result.fields) == {"a", "b"}


def test_profile_file_schema_h5_index_columns(tmp_path):
    pytest = __import__("pytest")
    pytest.importorskip("tables")

    path = tmp_path / "sample.h5"
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).set_index("a")
    df.to_hdf(path, key="data", format="table", mode="w")

    result = profile_file_schema(
        identity=IdentityManager(),
        path=str(path),
        driver="h5_table",
        table_path="data",
        sample_rows=2,
        source="file",
    )
    assert set(field.name for field in result.fields) == {"a", "b"}


def test_profile_file_schema_gtfs_member(tmp_path):
    feed_dir = tmp_path / "feed"
    feed_dir.mkdir()
    pd.DataFrame(
        [
            {"trip_id": "T1", "route_id": "R1", "service_id": "S1"},
            {"trip_id": "T2", "route_id": "R1", "service_id": "S2"},
        ]
    ).to_csv(feed_dir / "trips.txt", index=False)

    result = profile_file_schema(
        identity=IdentityManager(),
        path=str(feed_dir),
        driver="gtfs",
        table_path="trips.txt",
        sample_rows=2,
        source="file",
    )
    assert result.summary["n_columns"] == 3
    assert [field.name for field in result.fields] == [
        "trip_id",
        "route_id",
        "service_id",
    ]


def test_profile_file_schema_rejects_unsafe_view_name(tmp_path):
    path = tmp_path / "sample.csv"
    pd.DataFrame({"a": [1], "b": ["x"]}).to_csv(path, index=False)

    with pytest.raises(ValueError, match="Invalid view_name"):
        profile_file_schema(
            identity=IdentityManager(),
            path=str(path),
            driver="csv",
            sample_rows=1,
            source="file",
            view_name='tmp_view"; DROP VIEW run;--',
        )
