"""
Consist Core Matrix Module

This module provides the `MatrixViewFactory`, which is responsible for creating
virtual xarray Datasets across runs.
"""

from __future__ import annotations

from importlib import import_module
import logging
from types import ModuleType
from typing import TYPE_CHECKING, Any, List, Optional

import pandas as pd

_UNSET = object()
xr: Any = _UNSET


def _get_xr() -> Optional[ModuleType]:
    global xr
    if xr is _UNSET:
        try:
            xr = import_module("xarray")
        except ImportError:
            xr = None
    return xr


if TYPE_CHECKING:
    XrDataset = Any
    from consist.core.tracker import Tracker


class MatrixViewFactory:
    """Factory class creating virtual xarray Datasets from Consist's metadata."""

    def __init__(self, tracker: Tracker) -> None:
        self.tracker = tracker

    def load_matrix_view(
        self,
        concept_key: str,
        variables: Optional[List[str]] = None,
        *,
        run_ids: Optional[List[str]] = None,
        parent_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
    ) -> "XrDataset":
        """Returns a lazy xarray Dataset containing all runs matching ``concept_key``."""
        xr_module = _get_xr()
        if xr_module is None:
            raise ImportError("xarray is required.")
        db = self.tracker.db
        if db is None:
            raise RuntimeError("Database connection required.")

        clauses = ["a.key = ?"]
        params: list[Any] = [concept_key]
        if run_ids:
            placeholders = ", ".join("?" for _ in run_ids)
            clauses.append(f"r.id IN ({placeholders})")
            params.extend(run_ids)
        if parent_id:
            clauses.append("r.parent_run_id = ?")
            params.append(parent_id)
        if model:
            clauses.append("r.model_name = ?")
            params.append(model)
        if status:
            clauses.append("r.status = ?")
            params.append(status)

        sql = (
            "SELECT a.uri AS container_uri, r.id AS run_id, r.year, r.iteration "
            "FROM artifact a "
            "JOIN run r ON a.run_id = r.id "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY r.year, r.iteration"
        )
        df = db.fetchdf(sql, params)
        if df.empty:
            return xr_module.Dataset()

        datasets = []
        for _, row in df.iterrows():
            container_uri = str(row["container_uri"])
            try:
                path = self.tracker.resolve_uri(container_uri)
                ds = xr_module.open_zarr(path, consolidated=False)
                if variables:
                    ds = ds[variables]
                ds = ds.assign_coords(
                    {
                        "year": int(row["year"]) if pd.notna(row["year"]) else 0,
                        "iteration": int(row["iteration"])
                        if pd.notna(row["iteration"])
                        else 0,
                    }
                )
                ds = ds.expand_dims(run_id=[row["run_id"]])
                datasets.append(ds)
            except Exception as e:
                logging.warning(
                    f"[Consist Warning] Failed to load matrix {container_uri}: {e}"
                )
                continue

        if not datasets:
            return xr_module.Dataset()

        return xr_module.concat(datasets, dim="run_id")
