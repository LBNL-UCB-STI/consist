from __future__ import annotations

import logging
from pathlib import Path
import pandas as pd
import pytest

from consist.core.directory_artifacts import build_directory_manifest
from consist.core.materialize import (
    count_directory_files_and_bytes,
    emit_directory_rollup_summary,
    format_byte_size,
    resolve_artifact_role,
    stage_inputs,
)
from consist.models.artifact import Artifact
from consist.digest import compute_digest


def test_format_byte_size():
    assert format_byte_size(0) == "0 B"
    assert format_byte_size(500) == "500 B"
    assert format_byte_size(1024) == "1.0 KB"
    assert format_byte_size(2048) == "2.0 KB"
    assert format_byte_size(1024 * 1024) == "1.0 MB"
    assert format_byte_size(1024 * 1024 * 1024) == "1.0 GB"


def test_count_directory_files_and_bytes_filesystem(tmp_path: Path):
    d = tmp_path / "sample_dir"
    d.mkdir()
    (d / "file1.txt").write_bytes(b"hello")  # 5 bytes
    sub = d / "subdir"
    sub.mkdir()
    (sub / "file2.txt").write_bytes(b"world!")  # 6 bytes

    files, nbytes = count_directory_files_and_bytes(d)
    assert files == 2
    assert nbytes == 11

    # Non-existent
    assert count_directory_files_and_bytes(tmp_path / "nonexistent") == (0, 0)

    # Single file
    assert count_directory_files_and_bytes(d / "file1.txt") == (1, 5)


def test_count_directory_files_and_bytes_manifest(tmp_path: Path):
    d = tmp_path / "manifest_dir"
    d.mkdir()
    (d / "a.bin").write_bytes(b"1234")
    (d / "b.bin").write_bytes(b"56789")

    manifest = build_directory_manifest(d)
    files, nbytes = count_directory_files_and_bytes(d, manifest=manifest)
    assert files == 2
    assert nbytes == 9


def test_resolve_artifact_role():
    # config_role priority
    art_cfg = Artifact(
        key="my_key",
        uri="./path",
        driver="directory",
        meta={"config_role": "r5_osm_source", "role": "secondary"},
    )
    assert resolve_artifact_role(art_cfg, fallback_key="fallback") == "r5_osm_source"

    # role priority over key
    art_role = Artifact(
        key="my_key",
        uri="./path",
        driver="directory",
        meta={"role": "skims"},
    )
    assert resolve_artifact_role(art_role, fallback_key="fallback") == "skims"

    # fallback_key when no meta role
    art_no_role = Artifact(
        key="my_key",
        uri="./path",
        driver="directory",
        meta={},
    )
    assert resolve_artifact_role(art_no_role, fallback_key="param_role") == "param_role"

    # artifact.key when no fallback
    assert resolve_artifact_role(art_no_role) == "my_key"


def test_emit_directory_rollup_summary(caplog):
    caplog.set_level(logging.INFO)
    rollups = {
        "r5_osm_source": (3, 45 * 1024 * 1024),
        "configs": (12, 120 * 1024),
    }

    emit_directory_rollup_summary(
        "Staged directory inputs", rollups, step_id="beam_step"
    )

    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(info_records) == 1
    msg = info_records[0].getMessage()
    assert "[Consist] Staged directory inputs for step 'beam_step':" in msg
    assert "configs (12 files, 120.0 KB)" in msg
    assert "r5_osm_source (3 files, 45.0 MB)" in msg


def test_stage_inputs_directory_rollup(tracker, tmp_path: Path, caplog):
    caplog.set_level(logging.INFO)

    # Prepare a directory artifact (e.g. config directory)
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "settings.yaml").write_bytes(b"model: test\n")
    (cfg_dir / "params.csv").write_bytes(b"a,b\n1,2\n")

    art_digest = compute_digest(cfg_dir, driver="directory")
    art = Artifact(
        key="asim_configs",
        uri=str(cfg_dir),
        driver="directory",
        digest=art_digest,
        meta={"config_role": "configs"},
    )

    dest_dir = Path(tracker.run_dir) / "staged_configs"
    res = stage_inputs(
        tracker,
        {"asim_configs": art},
        {"asim_configs": dest_dir},
    )

    assert res.complete is True
    assert dest_dir.exists()
    assert (dest_dir / "settings.yaml").exists()

    # Check the emitted rollup log
    rollup_logs = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and "Staged directory inputs" in r.getMessage()
    ]
    assert len(rollup_logs) == 1
    assert "configs (2 files," in rollup_logs[0]


def test_step_cache_hit_directory_hydration_rollup(tracker, tmp_path: Path, caplog):
    caplog.set_level(logging.INFO)

    zarr_dir = Path(tracker.run_dir) / "outputs" / "skims.zarr"
    zarr_dir.mkdir(parents=True)
    (zarr_dir / ".zarray").write_bytes(b'{"zarr_format": 2}')
    (zarr_dir / "0.0").write_bytes(b"chunk_data_0_0")
    (zarr_dir / "0.1").write_bytes(b"chunk_data_0_1")

    # Run step once to populate cache
    with tracker.start_run(
        "skims_step_initial",
        "skims_model",
        cache_mode="overwrite",
    ):
        tracker.log_artifact(
            str(zarr_dir),
            key="travel_skims",
            driver="zarr",
            direction="output",
        )

    # Hydrate outputs explicitly to check the hydration rollup
    caplog.clear()
    dest_root = Path(tracker.run_dir) / "hydrated_workspace"
    tracker.hydrate_run_outputs(
        "skims_step_initial",
        target_root=dest_root,
    )

    hydration_logs = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and "Hydrated directory outputs" in r.getMessage()
    ]
    assert len(hydration_logs) == 1
    assert "travel_skims (3 files," in hydration_logs[0]


def test_tracker_create_grouped_view_with_key(tracker, tmp_path: Path):
    f1 = tmp_path / "v1.parquet"
    pd.DataFrame({"id": [1, 2], "val": [10, 20]}).to_parquet(f1)
    f2 = tmp_path / "v2.parquet"
    pd.DataFrame({"id": [3, 4], "val": [30, 40]}).to_parquet(f2)

    with tracker.start_run("r1", "model_1", year=2020, cache_mode="overwrite"):
        tracker.log_artifact(
            str(f1),
            key="household_data",
            driver="parquet",
            profile_file_schema=True,
            facet={"year": 2020},
            facet_index=True,
        )

    with tracker.start_run("r2", "model_1", year=2021, cache_mode="overwrite"):
        tracker.log_artifact(
            str(f2),
            key="household_data",
            driver="parquet",
            profile_file_schema=True,
            facet={"year": 2021},
            facet_index=True,
        )

    # Use key directly in tracker.create_grouped_view!
    tracker.create_grouped_view(
        "v_households",
        key="household_data",
        mode="cold_only",
    )

    df = tracker.query_df("SELECT COUNT(*) AS total FROM v_households")
    assert int(df.iloc[0]["total"]) == 4

    # Test error when key has no schema
    with pytest.raises(
        ValueError, match="No schemas found for artifact key 'missing_key'"
    ):
        tracker.create_grouped_view("v_missing", key="missing_key")

    # Test error when providing both schema_id and key
    with pytest.raises(ValueError, match="Provide exactly one of schema_id or schema"):
        tracker.create_grouped_view("v_bad", schema_id="abc", key="household_data")


def test_materialize_requested_inputs_directory_rollup(tmp_path: Path, caplog) -> None:
    from datetime import datetime, timezone
    from consist.core.cache import ActiveRunCacheOptions, materialize_requested_inputs
    from consist.core.tracker import Tracker
    from consist.digest import Digest
    from consist.models.run import ConsistRecord, Run

    caplog.set_level(logging.INFO)

    source = tmp_path / "source.zarr"
    source.mkdir()
    (source / "0.0").write_bytes(b"skim_chunk_1")
    (source / "0.1").write_bytes(b"skim_chunk_2")
    destination = tmp_path / "staged" / "skims.zarr"
    manifest = build_directory_manifest(source)

    artifact = Artifact(
        key="skims",
        uri=str(source),
        driver="zarr",
        digest=Digest("dir_manifest_v1:sha256", manifest["tree_hash"]),
        meta={"directory_artifact": True, "directory_manifest": manifest},
    )

    now = datetime.now(timezone.utc)
    run = Run(
        id="run_step_001",
        model_name="skims_model",
        status="running",
        config_hash="config",
        git_hash="git",
        started_at=now,
        created_at=now,
        updated_at=now,
        meta={},
    )

    tracker = Tracker(run_dir=tmp_path, db_path=None, allow_external_paths=False)
    tracker.current_consist = ConsistRecord(run=run, inputs=[artifact])

    materialize_requested_inputs(
        tracker=tracker,
        options=ActiveRunCacheOptions(
            requested_input_paths={"skims": destination},
            requested_input_materialization="requested",
        ),
    )

    staged_logs = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO
        and "Staged directory inputs for step 'run_step_001'" in r.getMessage()
    ]
    assert len(staged_logs) == 1
    assert "skims (2 files," in staged_logs[0]


def test_create_grouped_view_isolates_identical_schemas_by_key(tracker, tmp_path: Path):
    f1 = tmp_path / "k1.parquet"
    pd.DataFrame({"id": [1, 2], "val": [10, 20]}).to_parquet(f1)
    f2 = tmp_path / "k2.parquet"
    pd.DataFrame({"id": [3, 4], "val": [30, 40]}).to_parquet(f2)

    with tracker.start_run("r1", "model_1", cache_mode="overwrite"):
        tracker.log_artifact(
            str(f1), key="key_alpha", driver="parquet", profile_file_schema=True
        )
        tracker.log_artifact(
            str(f2), key="key_beta", driver="parquet", profile_file_schema=True
        )

    tracker.create_grouped_view("v_alpha", key="key_alpha", mode="cold_only")
    tracker.create_grouped_view("v_beta", key="key_beta", mode="cold_only")

    df_a = tracker.query_df("SELECT * FROM v_alpha")
    assert len(df_a) == 2
    assert set(df_a["id"].tolist()) == {1, 2}

    df_b = tracker.query_df("SELECT * FROM v_beta")
    assert len(df_b) == 2
    assert set(df_b["id"].tolist()) == {3, 4}


def test_record_directory_rollup_tolerates_corrupted_or_missing_manifest(
    tmp_path: Path,
):
    from consist.core.materialize import record_directory_rollup

    d = tmp_path / "somedir"
    d.mkdir()
    (d / "file1.txt").write_bytes(b"12345")

    # Corrupted manifest (not a mapping)
    artifact = Artifact(
        key="my_dir",
        uri=str(d),
        driver="directory",
        meta={"directory_artifact": True, "directory_manifest": "not_a_mapping"},
    )
    rollups = {}
    record_directory_rollup(rollups, artifact, d)
    assert rollups == {"my_dir": (1, 5)}

    # Valid manifest with entries
    artifact2 = Artifact(
        key="my_dir2",
        uri=str(d),
        driver="directory",
        meta={
            "directory_artifact": True,
            "directory_manifest": {"entries": [{"kind": "file", "size": 42}]},
        },
    )
    rollups2 = {}
    record_directory_rollup(rollups2, artifact2, d)
    assert rollups2 == {"my_dir2": (1, 42)}
