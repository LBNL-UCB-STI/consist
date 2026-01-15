from __future__ import annotations

from typing import Iterable, Optional, Literal

import pandas as pd


def yield_file_batches(
    path: str, *, driver: Literal["parquet", "csv"], max_rows: Optional[int] = None
) -> Iterable[pd.DataFrame]:
    """
    Yield Pandas DataFrame batches from a file without loading it all into memory.

    Parameters
    ----------
    path : str
        Filesystem path to the source file.
    driver : {"parquet", "csv"}
        File format driver to use for batch iteration.
    max_rows : Optional[int], default None
        Optional limit on total rows yielded across batches.

    Yields
    ------
    pd.DataFrame
        A batch of rows from the source file.

    Raises
    ------
    ValueError
        If the driver is unsupported.
    """
    if driver == "parquet":
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(path)
        row_count = 0
        for batch in parquet_file.iter_batches():
            df = batch.to_pandas()
            if max_rows is not None:
                remaining = max_rows - row_count
                if remaining <= 0:
                    return
                df = df.head(remaining)
            row_count += len(df)
            yield df
            if max_rows is not None and row_count >= max_rows:
                return
    elif driver == "csv":
        row_count = 0
        for chunk in pd.read_csv(path, chunksize=50000):
            if max_rows is not None:
                remaining = max_rows - row_count
                if remaining <= 0:
                    return
                chunk = chunk.head(remaining)
            row_count += len(chunk)
            yield chunk
            if max_rows is not None and row_count >= max_rows:
                return
    else:
        raise ValueError(f"Unsupported driver for file batches: {driver}")
