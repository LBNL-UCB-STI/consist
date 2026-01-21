from __future__ import annotations

import shutil
from pathlib import Path
from typing import Tuple


def build_beam_test_configs(tmp_path: Path) -> Tuple[Path, Path, Path]:
    resources = Path(__file__).resolve().parent.parent / "resources" / "beam_small"
    case_dir = tmp_path / "beam_case"
    case_dir.mkdir(parents=True, exist_ok=True)
    for name in ("base.conf", "overlay.conf", "common.conf", "inputs"):
        src = resources / name
        dst = case_dir / name
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)
    return case_dir, case_dir / "overlay.conf", case_dir / "base.conf"
