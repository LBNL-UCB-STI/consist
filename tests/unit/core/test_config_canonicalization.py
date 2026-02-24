from __future__ import annotations

import os
import time
from pathlib import Path

from consist.core.config_canonicalization import compute_config_pack_hash
from consist.core.identity import IdentityManager


def test_compute_config_pack_hash_is_stable_in_fast_mode(tmp_path: Path):
    identity = IdentityManager(project_root=tmp_path, hashing_strategy="fast")
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True)
    config_file = config_dir / "settings.yaml"
    config_file.write_text("sample_rate: 0.25\n", encoding="utf-8")

    hash_a = compute_config_pack_hash(root_dirs=[config_dir], identity=identity)

    # Simulate materialization/copy drift where mtime changes but content does not.
    future_mtime = time.time() + 60
    os.utime(config_file, (future_mtime, future_mtime))

    hash_b = compute_config_pack_hash(root_dirs=[config_dir], identity=identity)
    assert hash_a == hash_b
