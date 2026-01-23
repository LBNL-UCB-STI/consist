from __future__ import annotations

from importlib.util import find_spec


def resolve_netcdf_engine() -> str | None:
    """Return the preferred NetCDF engine for xarray I/O."""
    if find_spec("h5netcdf") is not None:
        return "h5netcdf"
    return None
