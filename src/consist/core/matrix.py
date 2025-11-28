import pandas as pd
from typing import List

try:
    import xarray as xr
except ImportError:
    xr = None


class MatrixViewFactory:
    """
    Creates virtual xarray Datasets by querying the Consist metadata catalog
    and lazily loading the corresponding Zarr stores from disk.
    """

    def __init__(self, tracker):
        self.tracker = tracker

    def load_matrix_view(self, concept_key: str, variables: List[str] = None) -> "xr.Dataset":
        """
        Returns a lazy xarray Dataset containing all runs that match the concept_key.
        The dataset is concatenated along the 'run_id' dimension.
        """
        if xr is None: raise ImportError("xarray is required.")
        if not self.tracker.engine: raise RuntimeError("Database connection required.")

        # 1. Query Metadata
        query = f"""
            SELECT a.uri, r.id as run_id, r.year, r.iteration 
            FROM artifact a
            JOIN run r ON a.run_id = r.id
            WHERE a.key = '{concept_key}'
            ORDER BY r.year, r.iteration
        """
        df = pd.read_sql(query, self.tracker.engine)
        if df.empty: return xr.Dataset()

        # 2. Lazy Open & Stack
        datasets = []
        for _, row in df.iterrows():
            try:
                uri = row['uri']
                path = self.tracker.resolve_uri(uri)

                ds = xr.open_zarr(path, consolidated=False)
                if variables: ds = ds[variables]

                # FIX: Stack along 'run_id' dimension.
                # Treat 'year' and 'iteration' as metadata (coords) for that run.
                ds = ds.assign_coords({
                    "year": row['year'] or 0,
                    "iteration": row['iteration'] or 0
                })
                # Expand run_id to be the primary concatenation dimension
                ds = ds.expand_dims(run_id=[row['run_id']])

                datasets.append(ds)
            except Exception as e:
                print(f"[Consist Warning] Failed to load matrix {uri}: {e}")
                continue

        if not datasets: return xr.Dataset()

        # 3. Combine
        # Use concat to keep dimensions dense (avoiding sparse NaNs)
        return xr.concat(datasets, dim="run_id")