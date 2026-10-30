"""Unit tests for coupler snapshot restoration, materialization, and ephemeral pruning (PR 2)."""

from collections.abc import Mapping
from pathlib import Path
from typing import Optional
from unittest.mock import patch
import uuid

import pandas as pd

from consist.core.directory_artifacts import (
    build_directory_manifest,
    build_shapefile_bundle_manifest,
)
from consist.core.noop import NoopScenarioContext
from consist.core.tracker import Tracker
from consist.core.workflow import RestoredCouplerSnapshotResult
from consist.models.artifact import Artifact


def _create_persisted_artifact(
    tracker: Tracker,
    *,
    key: str,
    uri: str,
    driver: str = "parquet",
    content_hash: Optional[str] = None,
    abs_path: Optional[str] = None,
    meta: Optional[dict] = None,
) -> Artifact:
    art = Artifact(
        id=uuid.uuid4(),
        key=key,
        container_uri=uri,
        driver=driver,
        hash=content_hash,
        abs_path=abs_path,
        meta=dict(meta or {}),
    )
    if tracker.db is not None:
        with tracker.db.session_scope() as session:
            session.add(art)
            session.commit()
            session.refresh(art)
            session.expunge(art)
    return art


def test_default_backward_compatibility_and_noop_parity(tmp_path: Path) -> None:
    """Verify default backward-compatibility and Noop parity with new kwargs."""
    db_path = tmp_path / "test.duckdb"
    tracker = Tracker(run_dir=tmp_path / "run", db_path=db_path)

    # 1. NoopScenarioContext parity
    noop_sc = NoopScenarioContext("noop_test")
    noop_sc.coupler.set("prior_key", "prior_value")

    res_noop = noop_sc.restore_coupler_snapshot(
        target_root=tmp_path / "target",
        source_root=tmp_path / "source",
        remappable_schemes={"beam_input": "beam/input"},
        prune_unrecoverable=True,
        validate_content_hash="never",
    )
    assert isinstance(res_noop, RestoredCouplerSnapshotResult)
    assert isinstance(res_noop, Mapping)
    assert res_noop == RestoredCouplerSnapshotResult()
    assert res_noop != RestoredCouplerSnapshotResult(pruned_keys=("x",))
    assert dict(res_noop) == {}
    assert len(res_noop) == 0
    assert not res_noop
    # Coupler should retain prior key since clear_existing was False
    assert "prior_key" in noop_sc.coupler

    res_noop_clear = noop_sc.restore_coupler_snapshot(
        clear_existing=True,
        prune_unrecoverable=True,
    )
    assert res_noop_clear == RestoredCouplerSnapshotResult()
    assert len(noop_sc.coupler) == 0

    # 2. Standard ScenarioContext without target_root or prune_unrecoverable
    data_file = tmp_path / "run" / "data.csv"
    data_file.parent.mkdir(parents=True, exist_ok=True)
    data_file.write_text("a,b\n1,2\n")

    with tracker.scenario("sc_compat") as sc:
        art = sc.add_input(data_file, key="in_csv")
        sc.coupler.set("in_csv", art)
        sc.capture_coupler_snapshot(phase="post", step_ordinal=1)

    with tracker.scenario("sc_compat_restore") as sc2:
        restored = sc2.restore_coupler_snapshot(
            scenario_run_id="sc_compat",
            phase="post",
        )
        assert isinstance(restored, RestoredCouplerSnapshotResult)
        assert isinstance(restored, Mapping)
        assert "in_csv" in restored
        assert restored["in_csv"].key == "in_csv"
        assert "in_csv" in sc2.coupler
        assert len(restored.pruned_keys) == 0
        assert len(restored.outcomes) == 0


def test_multi_node_restart_materialization_and_pruning(tmp_path: Path) -> None:
    """Verify multi-node restart materialization, re-anchoring, external mount safety, and pruning."""
    db_path = tmp_path / "test.duckdb"
    archive_dir = tmp_path / "archive"
    local_dir = tmp_path / "local"
    external_dir = tmp_path / "external"

    archive_dir.mkdir()
    local_dir.mkdir()
    external_dir.mkdir()

    # Setup archive files
    (archive_dir / "out").mkdir(parents=True)
    (archive_dir / "out" / "persisted.parquet").write_text("persisted-content")

    (archive_dir / "beam" / "input" / "transit").mkdir(parents=True)
    (archive_dir / "beam" / "input" / "transit" / "schedule.csv").write_text(
        "sched-content"
    )

    # Setup external file
    (external_dir / "ref.csv").write_text("external-ref-content")

    tracker_archive = Tracker(
        run_dir=archive_dir,
        db_path=db_path,
        mounts={
            "beam_input": archive_dir / "beam" / "input",
            "inputs": external_dir,
        },
    )

    persisted_art = _create_persisted_artifact(
        tracker_archive,
        key="persisted_out",
        uri="workspace://out/persisted.parquet",
        driver="parquet",
    )
    ephemeral_art = _create_persisted_artifact(
        tracker_archive,
        key="ephemeral_out",
        uri="workspace://beam/plans.parquet",
        driver="parquet",
        abs_path="/local/job27130000/plans.parquet",
    )
    submount_art = _create_persisted_artifact(
        tracker_archive,
        key="submount_sched",
        uri="beam_input://transit/schedule.csv",
        driver="csv",
    )
    ext_ref_art = _create_persisted_artifact(
        tracker_archive,
        key="ext_ref",
        uri="inputs://ref.csv",
        driver="csv",
    )

    with tracker_archive.scenario("prod_scenario") as sc:
        sc.coupler.set("persisted_out", persisted_art)
        sc.coupler.set("ephemeral_out", ephemeral_art)
        sc.coupler.set("submount_sched", submount_art)
        sc.coupler.set("ext_ref", ext_ref_art)
        sc.capture_coupler_snapshot(phase="post", step_ordinal=1)

    # 1. Test with mounts auto-detection on target node
    tracker_local = Tracker(
        run_dir=local_dir,
        db_path=db_path,
        mounts={
            "beam_input": local_dir / "beam" / "input",
            "inputs": external_dir,
        },
    )

    with tracker_local.scenario("resumed_scenario") as sc_resume:
        # Pre-seed coupler with ephemeral_out to test eviction when clear_existing=False
        sc_resume.coupler.set("ephemeral_out", ephemeral_art)

        result = sc_resume.restore_coupler_snapshot(
            scenario_run_id="prod_scenario",
            phase="post",
            target_root=local_dir,
            source_root=archive_dir,
            prune_unrecoverable=True,
            clear_existing=False,
        )

        assert isinstance(result, RestoredCouplerSnapshotResult)
        assert result.pruned_keys == ("ephemeral_out",)
        assert "ephemeral_out" not in result
        assert "ephemeral_out" not in sc_resume.coupler

        # Check persisted_out
        assert "persisted_out" in result
        assert "persisted_out" in sc_resume.coupler
        persisted_local_file = local_dir / "out" / "persisted.parquet"
        assert persisted_local_file.exists()
        assert persisted_local_file.read_text() == "persisted-content"
        hydrated_persisted = result["persisted_out"]
        assert hydrated_persisted.abs_path == str(persisted_local_file)
        assert hydrated_persisted.as_path(sc_resume.tracker) == persisted_local_file
        assert hydrated_persisted.path == persisted_local_file
        assert str(archive_dir.resolve()) in hydrated_persisted.recovery_roots

        # Check submount_sched
        assert "submount_sched" in result
        assert "submount_sched" in sc_resume.coupler
        sched_local_file = local_dir / "beam" / "input" / "transit" / "schedule.csv"
        assert sched_local_file.exists()
        assert sched_local_file.read_text() == "sched-content"
        hydrated_sched = result["submount_sched"]
        assert hydrated_sched.as_path(sc_resume.tracker) == sched_local_file
        assert hydrated_sched.path == sched_local_file

        # Check ext_ref
        assert "ext_ref" in result
        assert "ext_ref" in sc_resume.coupler
        # Should not have copied ext_ref into local_dir
        assert not (local_dir / "ref.csv").exists()
        assert result["ext_ref"].as_path(sc_resume.tracker) == external_dir / "ref.csv"

    # 2. Test explicit remappable_schemes override when mounts do not include the submount
    local_dir2 = tmp_path / "local2"
    local_dir2.mkdir()
    tracker_local2 = Tracker(
        run_dir=local_dir2,
        db_path=db_path,
        mounts={"inputs": external_dir},
    )

    with tracker_local2.scenario("resumed_scenario_explicit_schemes") as sc_resume2:
        result2 = sc_resume2.restore_coupler_snapshot(
            scenario_run_id="prod_scenario",
            phase="post",
            target_root=local_dir2,
            source_root=archive_dir,
            remappable_schemes={"beam_input": "beam/input"},
            prune_unrecoverable=True,
            clear_existing=True,
        )

        assert result2.pruned_keys == ("ephemeral_out",)
        assert "persisted_out" in result2
        assert "submount_sched" in result2
        sched_file2 = local_dir2 / "beam" / "input" / "transit" / "schedule.csv"
        assert sched_file2.exists()
        assert sched_file2.read_text() == "sched-content"
        assert result2["submount_sched"].path == sched_file2
        assert result2["submount_sched"].as_path(sc_resume2.tracker) == sched_file2


def test_directory_and_shapefile_bundle_materialization(tmp_path: Path) -> None:
    """Verify directory / Zarr and shapefile bundle artifacts end-to-end with PR 1."""
    db_path = tmp_path / "test.duckdb"
    archive_dir = tmp_path / "archive"
    local_dir = tmp_path / "local"

    archive_dir.mkdir()
    local_dir.mkdir()

    # Directory artifact (e.g. Zarr)
    zarr_dir = archive_dir / "matrices" / "zarr_data"
    zarr_dir.mkdir(parents=True)
    (zarr_dir / ".zgroup").write_text("{}")
    (zarr_dir / "0").write_text("chunk0")
    zarr_manifest = build_directory_manifest(zarr_dir)

    # Shapefile bundle
    shp_dir = archive_dir / "gis"
    shp_dir.mkdir(parents=True)
    shp_file = shp_dir / "zones.shp"
    shp_file.write_text("shp")
    (shp_dir / "zones.shx").write_text("shx")
    (shp_dir / "zones.dbf").write_text("dbf")
    (shp_dir / "zones.prj").write_text("prj")
    shp_manifest = build_shapefile_bundle_manifest(shp_file)

    tracker_archive = Tracker(run_dir=archive_dir, db_path=db_path)

    zarr_art = _create_persisted_artifact(
        tracker_archive,
        key="zarr_data",
        uri="workspace://matrices/zarr_data",
        driver="zarr",
        content_hash=zarr_manifest["tree_hash"],
        meta={
            "directory_artifact": True,
            "directory_manifest": zarr_manifest,
        },
    )
    shp_art = _create_persisted_artifact(
        tracker_archive,
        key="zones_shp",
        uri="workspace://gis/zones.shp",
        driver="shapefile",
        content_hash=shp_manifest["tree_hash"],
        meta={
            "file_bundle_artifact": True,
            "file_bundle_entry": "zones.shp",
            "file_bundle_manifest": shp_manifest,
        },
    )

    with tracker_archive.scenario("gis_scenario") as sc:
        sc.coupler.set("zarr_data", zarr_art)
        sc.coupler.set("zones_shp", shp_art)
        sc.capture_coupler_snapshot(phase="post", step_ordinal=1)

    tracker_local = Tracker(run_dir=local_dir, db_path=db_path)

    with tracker_local.scenario("gis_resumed") as sc_resume:
        result = sc_resume.restore_coupler_snapshot(
            scenario_run_id="gis_scenario",
            phase="post",
            target_root=local_dir,
            source_root=archive_dir,
            prune_unrecoverable=True,
        )

        assert "zarr_data" in result
        assert "zones_shp" in result

        target_zarr = local_dir / "matrices" / "zarr_data"
        assert target_zarr.is_dir()
        assert (target_zarr / ".zgroup").read_text() == "{}"
        assert (target_zarr / "0").read_text() == "chunk0"
        assert result["zarr_data"].path == target_zarr

        target_shp = local_dir / "gis" / "zones.shp"
        assert target_shp.exists()
        assert (local_dir / "gis" / "zones.shx").exists()
        assert (local_dir / "gis" / "zones.dbf").exists()
        assert (local_dir / "gis" / "zones.prj").exists()
        assert result["zones_shp"].path == target_shp


def test_startup_performance_guard_no_full_sha256(tmp_path: Path) -> None:
    """Verify restore_coupler_snapshot defaults to validate_content_hash='never' and avoids hashing."""
    db_path = tmp_path / "test.duckdb"
    archive_dir = tmp_path / "archive"
    local_dir = tmp_path / "local"

    archive_dir.mkdir()
    local_dir.mkdir()

    (archive_dir / "file.csv").write_text("x,y\n1,2\n")
    zarr_dir = archive_dir / "matrix_store"
    zarr_dir.mkdir()
    (zarr_dir / "chunk").write_text("data")

    tracker_archive = Tracker(run_dir=archive_dir, db_path=db_path)

    file_art = _create_persisted_artifact(
        tracker_archive,
        key="file_data",
        uri="workspace://file.csv",
        driver="csv",
        content_hash="recorded_file_sha256",
    )
    dir_art = _create_persisted_artifact(
        tracker_archive,
        key="matrix_data",
        uri="workspace://matrix_store",
        driver="zarr",
        content_hash="recorded_dir_sha256",
        meta={"artifact_kind": "directory"},
    )

    with tracker_archive.scenario("perf_scenario") as sc:
        sc.coupler.set("file_data", file_art)
        sc.coupler.set("matrix_data", dir_art)
        sc.capture_coupler_snapshot(phase="post", step_ordinal=1)

    tracker_local = Tracker(run_dir=local_dir, db_path=db_path)

    with tracker_local.scenario("perf_resumed") as sc_resume:
        with (
            patch("consist.core.materialize.file_sha256") as mock_file_sha,
            patch(
                "consist.core.materialize.build_directory_manifest"
            ) as mock_dir_manifest,
        ):
            result = sc_resume.restore_coupler_snapshot(
                scenario_run_id="perf_scenario",
                phase="post",
                target_root=local_dir,
                source_root=archive_dir,
                # default validate_content_hash is "never"
            )

            assert "file_data" in result
            assert "matrix_data" in result
            assert mock_file_sha.call_count == 0
            assert mock_dir_manifest.call_count == 0

    # Next verify that passing validate_content_hash="always" does invoke validation
    local_dir2 = tmp_path / "local2"
    local_dir2.mkdir()
    tracker_local2 = Tracker(run_dir=local_dir2, db_path=db_path)

    with tracker_local2.scenario("perf_resumed_always") as sc_resume2:
        with (
            patch(
                "consist.core.materialize.file_sha256",
                return_value="recorded_file_sha256",
            ) as mock_file_sha,
            patch(
                "consist.core.materialize.build_directory_manifest"
            ) as mock_dir_manifest,
        ):
            mock_dir_manifest.return_value = {
                "tree_hash": "recorded_dir_sha256",
                "members": [],
            }
            result2 = sc_resume2.restore_coupler_snapshot(
                scenario_run_id="perf_scenario",
                phase="post",
                target_root=local_dir2,
                source_root=archive_dir,
                validate_content_hash="always",
            )
            assert "file_data" in result2
            assert "matrix_data" in result2
            assert mock_file_sha.call_count > 0
            assert mock_dir_manifest.call_count > 0


def test_restore_coupler_snapshot_distinct_target_root_takes_precedence_over_run_dir(
    tmp_path: Path,
) -> None:
    """When target_root != tracker.run_dir and both have files at the relative path, target_root wins."""
    db_path = tmp_path / "test.duckdb"
    archive_dir = tmp_path / "archive"
    tracker_run_dir = tmp_path / "tracker_run_dir"
    custom_target_root = tmp_path / "custom_target_root"

    archive_dir.mkdir()
    tracker_run_dir.mkdir()
    custom_target_root.mkdir()

    # Archive has [1, 2, 3]
    archive_parquet = archive_dir / "out" / "persisted.parquet"
    archive_parquet.parent.mkdir(parents=True)
    pd.DataFrame({"id": [1, 2, 3]}).to_parquet(archive_parquet)

    # Stale file at tracker_run_dir has [999]
    stale_parquet = tracker_run_dir / "out" / "persisted.parquet"
    stale_parquet.parent.mkdir(parents=True)
    pd.DataFrame({"id": [999]}).to_parquet(stale_parquet)

    tracker_archive = Tracker(run_dir=archive_dir, db_path=db_path)
    persisted_art = _create_persisted_artifact(
        tracker_archive,
        key="persisted_out",
        uri="workspace://out/persisted.parquet",
        driver="parquet",
    )

    with tracker_archive.scenario("orig_scenario") as sc:
        sc.coupler.set("persisted_out", persisted_art)
        sc.capture_coupler_snapshot(phase="post", step_ordinal=1)

    tracker_resume = Tracker(run_dir=tracker_run_dir, db_path=db_path)
    with tracker_resume.scenario("resumed_scenario") as sc_resume:
        result = sc_resume.restore_coupler_snapshot(
            scenario_run_id="orig_scenario",
            phase="post",
            target_root=custom_target_root,
            source_root=archive_dir,
            prune_unrecoverable=True,
        )

        hydrated_art = result["persisted_out"]
        expected_target = (custom_target_root / "out" / "persisted.parquet").resolve()
        assert hydrated_art.abs_path == str(expected_target)
        assert hydrated_art.path == expected_target
        assert hydrated_art.as_path() == expected_target
        assert hydrated_art.as_path(sc_resume.tracker) == expected_target

        loaded_df = hydrated_art.as_df()
        assert loaded_df["id"].tolist() == [1, 2, 3]


def test_restore_coupler_snapshot_prune_without_target_root(tmp_path: Path) -> None:
    """When target_root is None and prune_unrecoverable=True, local files are kept and missing files pruned."""
    db_path = tmp_path / "test.duckdb"
    run_dir = tmp_path / "run_dir"
    run_dir.mkdir()

    present_csv = run_dir / "present.csv"
    present_csv.write_text("a\n1\n")

    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    present_art = _create_persisted_artifact(
        tracker,
        key="present_key",
        uri="workspace://present.csv",
        driver="csv",
    )
    missing_art = _create_persisted_artifact(
        tracker,
        key="missing_key",
        uri="workspace://missing.csv",
        driver="csv",
    )

    with tracker.scenario("local_prune_scenario") as sc:
        sc.coupler.set("present_key", present_art)
        sc.coupler.set("missing_key", missing_art)
        sc.capture_coupler_snapshot(phase="post", step_ordinal=1)

    with tracker.scenario("local_prune_resumed") as sc_resume:
        result = sc_resume.restore_coupler_snapshot(
            scenario_run_id="local_prune_scenario",
            phase="post",
            target_root=None,
            prune_unrecoverable=True,
        )
        assert list(result.keys()) == ["present_key"]
        assert result.pruned_keys == ("missing_key",)
        assert "missing_key" not in sc_resume.coupler
        assert result["present_key"].path == present_csv.resolve()
        assert result.outcomes["present_key"].status == "preserved_existing"
        assert result.outcomes["missing_key"].resolvable is False
