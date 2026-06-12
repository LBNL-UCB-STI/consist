import pandas as pd
import pytest
import zipfile

from consist.api import load, to_df
from consist.models.artifact import Artifact


def _csv_artifact(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    path = tmp_path / "data.csv"
    df.to_csv(path, index=False, sep=";")
    return Artifact(key="data", container_uri=str(path), driver="csv")


def _parquet_artifact(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    path = tmp_path / "data.parquet"
    try:
        df.to_parquet(path, index=False)
    except Exception:
        pytest.skip("Parquet support missing (pyarrow or fastparquet required).")
    return Artifact(key="data", container_uri=str(path), driver="parquet")


def _json_artifact(tmp_path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    path = tmp_path / "data.json"
    df.to_json(path, orient="records", lines=True)
    return Artifact(key="data", container_uri=str(path), driver="json")


def _h5_table_artifact(tmp_path):
    try:
        import tables  # noqa: F401
    except Exception:
        pytest.skip("HDF5 support missing (tables required).")
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    path = tmp_path / "data.h5"
    df.to_hdf(path, key="table", format="table", index=False)
    return Artifact(
        key="data",
        container_uri=str(path),
        driver="h5_table",
        table_path="table",
    )


def _gtfs_artifact(tmp_path):
    path = tmp_path / "data.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(
            "trips.txt",
            pd.DataFrame(
                [
                    {
                        "trip_id": "0001",
                        "route_id": "001",
                        "service_id": "20240101",
                    },
                    {
                        "trip_id": "0002",
                        "route_id": "002",
                        "service_id": "20240102",
                    },
                ]
            ).to_csv(index=False),
        )
    return Artifact(
        key="data",
        container_uri=str(path),
        driver="gtfs",
        table_path="trips.txt",
    )


def test_csv_load_kwargs_supported(tmp_path):
    artifact = _csv_artifact(tmp_path)
    relation = load(artifact, delimiter=";", columns=["a"])
    df = to_df(relation)
    assert list(df.columns) == ["a"]
    assert df["a"].tolist() == [1, 2]


def test_csv_load_kwargs_unsupported(tmp_path):
    artifact = _csv_artifact(tmp_path)
    with pytest.raises(ValueError, match="Unsupported load kwargs"):
        load(artifact, parse_dates=["a"])


def test_parquet_load_kwargs_supported(tmp_path):
    artifact = _parquet_artifact(tmp_path)
    relation = load(artifact, columns=["b"])
    df = to_df(relation)
    assert list(df.columns) == ["b"]
    assert df["b"].tolist() == [3, 4]


def test_parquet_load_kwargs_unsupported(tmp_path):
    artifact = _parquet_artifact(tmp_path)
    with pytest.raises(ValueError, match="Unsupported load kwargs"):
        load(artifact, delimiter=";")


def test_json_load_kwargs_supported(tmp_path):
    artifact = _json_artifact(tmp_path)
    relation = load(artifact, orient="records", lines=True)
    df = to_df(relation)
    assert list(df.columns) == ["a", "b"]
    assert df["a"].tolist() == [1, 2]


def test_json_load_kwargs_unsupported(tmp_path):
    artifact = _json_artifact(tmp_path)
    with pytest.raises(ValueError, match="Unsupported load kwargs"):
        load(artifact, columns=["a"])


def test_h5_table_load_kwargs_supported(tmp_path):
    artifact = _h5_table_artifact(tmp_path)
    relation = load(artifact, columns=["a"])
    df = to_df(relation)
    assert list(df.columns) == ["a"]
    assert df["a"].tolist() == [1, 2]


def test_h5_table_load_kwargs_unsupported(tmp_path):
    artifact = _h5_table_artifact(tmp_path)
    with pytest.raises(ValueError, match="Unsupported load kwargs"):
        load(artifact, delimiter=";")


def test_gtfs_load_uses_table_path(tmp_path):
    artifact = _gtfs_artifact(tmp_path)
    relation = load(artifact)
    df = to_df(relation)
    assert list(df.columns) == ["trip_id", "route_id", "service_id"]
    assert df["trip_id"].tolist() == ["0001", "0002"]
    assert df["route_id"].tolist() == ["001", "002"]
    assert df["service_id"].tolist() == ["20240101", "20240102"]


def test_gtfs_load_missing_columns_raise(tmp_path):
    artifact = _gtfs_artifact(tmp_path)
    with pytest.raises(KeyError, match="Missing GTFS columns"):
        load(artifact, columns=["trip_id", "missing"])
