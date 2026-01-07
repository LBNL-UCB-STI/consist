from __future__ import annotations

import gzip
import shutil
from pathlib import Path
from typing import Tuple


def build_activitysim_test_configs(
    tmp_path: Path, *, sample_rate: float = 0.25, households_sample_size: int = 2000
) -> Tuple[Path, Path]:
    """
    Build a small ActivitySim config case for tests.

    Parameters
    ----------
    tmp_path : Path
        Pytest temporary directory for staging the config.
    sample_rate : float, default 0.25
        Sample rate written to `settings_local.yaml`.
    households_sample_size : int, default 2000
        Households sample size written to overlay `settings.yaml`.

    Returns
    -------
    Tuple[Path, Path]
        (base_dir, overlay_dir) for the staged config.
    """
    resources = (
        Path(__file__).resolve().parent.parent / "resources" / "activitysim_small"
    )
    base_src = resources / "base"
    overlay_src = resources / "overlay"

    case_dir = tmp_path / f"activitysim_case_{str(sample_rate).replace('.', '_')}"
    base_dst = case_dir / "base"
    overlay_dst = case_dir / "overlay"

    shutil.copytree(base_src, base_dst)
    shutil.copytree(overlay_src, overlay_dst)

    _write_setting_override(
        base_dst / "settings_local.yaml", "sample_rate", sample_rate
    )
    _write_setting_override(
        overlay_dst / "settings.yaml",
        "households_sample_size",
        households_sample_size,
    )

    _ensure_gzip_coefficients(base_dst)

    return base_dst, overlay_dst


def _ensure_gzip_coefficients(base_dir: Path) -> None:
    source = base_dir / "trip_scheduling_coefficients.csv"
    target = base_dir / "trip_scheduling_coefficients.csv.gz"
    with source.open("rb") as handle:
        payload = handle.read()
    with gzip.open(target, "wb") as handle:
        handle.write(payload)


def _write_setting_override(path: Path, key: str, value: float | int) -> None:
    lines = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
    filtered = [line for line in lines if not line.startswith(f"{key}:")]
    filtered.append(f"{key}: {value}")
    path.write_text("\n".join(filtered) + "\n", encoding="utf-8")
