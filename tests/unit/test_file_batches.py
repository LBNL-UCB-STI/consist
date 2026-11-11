import pandas as pd

from consist.tools.file_batches import yield_file_batches


def test_yield_file_batches_csv(tmp_path):
    path = tmp_path / "example.csv"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(path, index=False)

    batches = list(yield_file_batches(str(path), driver="csv", max_rows=1))
    assert len(batches) == 1
    assert list(batches[0].columns) == ["a", "b"]
    assert len(batches[0]) == 1


def test_yield_file_batches_parquet_import_path(tmp_path):
    import consist.integrations.dlt_loader as dlt_loader

    assert dlt_loader.yield_file_batches is yield_file_batches
    path = tmp_path / "example.parquet"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_parquet(path, index=False)
    batches = list(yield_file_batches(str(path), driver="parquet", max_rows=1))
    assert len(batches) == 1
    assert list(batches[0].columns) == ["a", "b"]
    assert len(batches[0]) == 1
