from __future__ import annotations

import os
import time
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

import consist
from consist.core.config_canonicalization import (
    CanonicalizationReference,
    CanonicalizationSnapshot,
    ConfigReference,
    compute_config_pack_hash,
)
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


def test_canonicalization_snapshot_keeps_reference_facts_immutable(
    tmp_path: Path,
) -> None:
    source = tmp_path / "network.csv"
    reference = ConfigReference(
        config_key="beam.network.file",
        raw_value="network.csv",
        canonical_value="config:root/network.csv",
        status="resolved",
        required=True,
    )
    item = CanonicalizationReference(
        reference=reference,
        resolved_path=source,
        artifact_keys=("config:root/network.csv",),
    )
    snapshot = CanonicalizationSnapshot(
        adapter_name="beam",
        adapter_version="1",
        identity_hash="identity",
        references=(item,),
    )

    assert snapshot.references[0].reference is reference
    assert snapshot.references[0].resolved_path == source
    assert snapshot.references[0].artifact_keys == ("config:root/network.csv",)
    with pytest.raises(FrozenInstanceError):
        snapshot.identity_hash = "changed"  # type: ignore[misc]


def test_canonicalization_snapshot_types_are_top_level_public_api() -> None:
    assert consist.CanonicalizationReference is CanonicalizationReference
    assert consist.CanonicalizationSnapshot is CanonicalizationSnapshot
