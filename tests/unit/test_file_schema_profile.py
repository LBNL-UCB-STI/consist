import pandas as pd

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
