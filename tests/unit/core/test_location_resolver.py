"""Comprehensive cross-subsystem parity, security, mount-precedence, and regression tests.

Covers:
1. Remounted named mount across cache, materialize, tracker_archive, and views.
2. Multiple historical roots (rejected root skipped; stale/corrupted root skipped for valid root).
3. Step-scoped recovery copy on unmounted scratch:// with wiped producer workspace
   for scalar, directory (zarr), shapefile bundle, OutputSet, and HDF5 container member.
4. Archived + unmounted historical mount views.
5. Symlink & macOS/HPC mount-root parity (valid artifact under symlinked mount root succeeds;
   contained in-tree/leaf symlink rejected; dangling symlink raises ValueError).
6. Caller-specific policy regressions:
   - validate_cached_run_outputs does not hash scalar files on cache hits.
   - materialize_requested_inputs returns hash_mismatch rather than missing_source on wrong hash.
   - materialize_run_outputs(..., db_fallback="never") strictly isolated to source_root.
   - DB-less stub trackers pass unchanged.
   - set_artifact_recovery_roots(..., append=True) retains unextractable existing rows.
7. Performance / hot-path check confirming MountPrecedenceResolver adds zero per-artifact DB queries.
8. PILATES launcher.py mount-configuration regression test (Lawrencium job28061865 replay).
9. Zarr Lustre performance check (DirTreeStat witness skips full-tree chunk hashing).
10. Fail-closed root containment (C1): escaping symlinks are never yielded as safe candidates.
11. Historical candidates after ledger rows (C3): owning-run and mount_root fallbacks
    are always consulted; workspace instance verification (C2).
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import shutil
import uuid
import pandas as pd
import pytest

from typing import cast

from consist.core.fs import FileSystemManager, build_verified_location_row
from consist.core.location_resolver import (
    MountPrecedenceResolver,
    iter_artifact_candidates,
)
from consist.core.tracker import Tracker
from consist.core.tracker_recovery import (
    _load_cached_active_locations,
    resolve_verified_local_path,
    resolve_verified_local_path_detailed,
    warm_artifact_locations_cache,
)
from consist.location import (
    ArtifactLocationRow,
    DirTreeStat,
    TranslatedCoordinate,
)
from consist.models.artifact import Artifact
from consist.models.run import Run


def _write_shapefile_bundle(shapefile: Path) -> None:
    shapefile.parent.mkdir(parents=True, exist_ok=True)
    shapefile.write_bytes(b"shape geometry")
    shapefile.with_suffix(".shx").write_bytes(b"shape index")
    shapefile.with_suffix(".dbf").write_bytes(b"attribute table")
    shapefile.with_suffix(".prj").write_text('GEOGCS["WGS 84"]')


# ---------------------------------------------------------------------------
# 1. Remounted named mount across cache, materialize, archive, and views
# ---------------------------------------------------------------------------


def test_remounted_named_mount_resolved_across_subsystems(tmp_path: Path) -> None:
    """When outputs:// is remounted to /new/outputs (file absent), recover from /old/outputs."""
    db_path = tmp_path / "prov.duckdb"
    old_outputs = tmp_path / "old_outputs"
    new_outputs = tmp_path / "new_outputs"
    old_outputs.mkdir(parents=True)
    new_outputs.mkdir(parents=True)

    file_a = old_outputs / "table.parquet"
    pd.DataFrame({"x": [1, 2, 3]}).to_parquet(file_a)

    producer = Tracker(
        run_dir=tmp_path / "producer_ws",
        db_path=str(db_path),
        mounts={"outputs": str(old_outputs)},
    )
    with producer.start_run("producer_run", model="producer"):
        art = producer.log_output(file_a, key="table")
    producer.close()

    # Consumer tracker has outputs:// pointing to new_outputs where file is absent!
    consumer = Tracker(
        run_dir=tmp_path / "consumer_ws",
        db_path=str(db_path),
        mounts={"outputs": str(new_outputs)},
        allow_external_paths=True,
    )

    # 1a. iter_artifact_candidates yields primary (new) then historical (old)
    cands = list(iter_artifact_candidates(consumer, art))
    assert len(cands) == 2
    assert cands[0].source == "ledger_primary"
    assert cands[0].canonical_path == (new_outputs / "table.parquet").resolve()
    assert not cands[0].canonical_path.exists()
    assert cands[1].source == "ledger_historical"
    assert cands[1].canonical_path == (old_outputs / "table.parquet").resolve()
    assert cands[1].canonical_path.exists()

    # 1b. views transparently load from old_outputs
    consumer.create_view("v_table", "table")
    df = consumer.query_df("SELECT * FROM v_table")
    assert df is not None
    assert len(df) == 3

    # 1c. materialize_run_outputs restores the file into consumer workspace
    target_dir = consumer.run_dir / "restored"
    mat_res = consumer.materialize_run_outputs(
        "producer_run",
        target_root=target_dir,
    )
    assert "table" in mat_res.materialized_from_filesystem
    assert (target_dir / "table.parquet").exists()

    # 1d. archive_artifact archives from historical old_outputs
    archive_dest = tmp_path / "archive_remount"
    consumer.archive_artifact(art, archive_dest, mode="copy")
    assert (archive_dest / "table.parquet").exists()

    consumer.close()


# ---------------------------------------------------------------------------
# 2. Multiple historical roots: rejected root skipped; stale root skipped
# ---------------------------------------------------------------------------


def test_multiple_historical_roots_rejected_first_root_skips_to_valid_second(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    outside = tmp_path / "outside"
    outside.mkdir()
    bad_scratch = tmp_path / "bad_scratch"
    good_scratch = tmp_path / "good_scratch"

    bad_file = bad_scratch / "data.parquet"
    bad_file.parent.mkdir(parents=True)
    bad_file.symlink_to(outside / "secret.parquet")

    good_file = good_scratch / "data.parquet"
    good_file.parent.mkdir(parents=True)
    pd.DataFrame({"v": [10, 20]}).to_parquet(good_file)

    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(db_path))
    tracker.db.upsert_run(
        Run(
            id="run_bad",
            model_name="bad",
            meta={"mounts": {"scratch": str(bad_scratch)}},
        )
    )
    tracker.db.upsert_run(
        Run(
            id="run_good",
            model_name="good",
            meta={"mounts": {"scratch": str(good_scratch)}},
        )
    )

    art = Artifact(
        key="data",
        uri="scratch://data.parquet",
        driver="parquet",
        digest=None,
    )
    row = ArtifactLocationRow.create(
        digest=None,
        artifact_id=uuid.uuid4(),
        site="archive",
        mount_name="scratch",
        mount_instance_id="",
        uri="scratch://data.parquet",
    )

    resolver = MountPrecedenceResolver.from_tracker(tracker)
    primary, historical = resolver.resolve_row_coordinates(row, art)
    assert len(historical) == 1
    assert historical[0].canonical_path == good_file.resolve()
    tracker.close()


def test_multiple_historical_roots_stale_first_root_skips_to_valid_second(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    corrupt_root = tmp_path / "corrupt_root"
    valid_root = tmp_path / "valid_root"
    corrupt_root.mkdir(parents=True)
    valid_root.mkdir(parents=True)

    corrupt_file = corrupt_root / "out.parquet"
    valid_file = valid_root / "out.parquet"
    pd.DataFrame({"v": [-999]}).to_parquet(corrupt_file)
    pd.DataFrame({"v": [100, 200]}).to_parquet(valid_file)

    producer = Tracker(
        run_dir=tmp_path / "producer_ws",
        db_path=str(db_path),
        mounts={"data": str(valid_root)},
    )
    with producer.start_run("producer", model="m"):
        art = producer.log_output(valid_file, key="out")
    producer.close()

    consumer = Tracker(
        run_dir=tmp_path / "consumer_ws",
        db_path=str(db_path),
    )
    consumer.db.upsert_run(
        Run(
            id="producer_corrupt",
            model_name="m",
            meta={"mounts": {"data": str(corrupt_root)}},
        )
    )

    resolved = resolve_verified_local_path(consumer, art, allow_recovery_roots=True)
    assert resolved is not None
    assert resolved == valid_file.resolve()
    consumer.close()


# ---------------------------------------------------------------------------
# 3. Step-scoped recovery copy on unmounted scratch:// with wiped producer ws
# ---------------------------------------------------------------------------


def test_step_scoped_recovery_unmounted_scratch_wiped_workspace(tmp_path: Path) -> None:
    db_path = tmp_path / "prov.duckdb"
    scratch = tmp_path / "shared_scratch"
    producer_ws = tmp_path / "producer_ws"

    producer = Tracker(
        run_dir=producer_ws,
        db_path=str(db_path),
        mounts={"scratch": str(scratch)},
    )

    p_file = producer_ws / "scalar.parquet"
    pd.DataFrame({"a": [1, 2]}).to_parquet(p_file)

    z_dir = producer_ws / "skims.zarr"
    z_dir.mkdir(parents=True)
    (z_dir / "0.0").write_bytes(b"zarr-data")

    shp_file = producer_ws / "roads" / "roads.shp"
    _write_shapefile_bundle(shp_file)

    with producer.start_run("step_producer", model="m"):
        producer.log_output(p_file, key="scalar")
        producer.log_output(z_dir, key="zarr", artifact_kind="directory")
        producer.log_output(shp_file, key="roads")

    recovery_root = scratch / "run-x" / "consist-recovery" / "step_a"
    producer.archive_run_outputs("step_producer", recovery_root, mode="copy")
    producer.close()

    shutil.rmtree(producer_ws)

    consumer = Tracker(
        run_dir=tmp_path / "consumer_ws",
        db_path=str(db_path),
    )

    outputs = consumer.get_run_outputs("step_producer")
    assert "scalar" in outputs
    assert "zarr" in outputs
    assert "roads" in outputs

    for key in ("scalar", "zarr", "roads"):
        art = outputs[key]
        resolved = resolve_verified_local_path(consumer, art, allow_recovery_roots=True)
        assert resolved is not None
        assert resolved.exists()

    consumer.close()


def test_hdf5_container_member_parent_recovery_fallback(tmp_path: Path) -> None:
    db_path = tmp_path / "prov.duckdb"
    scratch = tmp_path / "scratch"
    producer_ws = tmp_path / "producer_ws"

    producer = Tracker(
        run_dir=producer_ws,
        db_path=str(db_path),
        mounts={"scratch": str(scratch)},
    )

    h5_file = producer_ws / "container.h5"
    h5_file.parent.mkdir(parents=True, exist_ok=True)
    with pd.HDFStore(str(h5_file), mode="w") as store:
        store.put("group/table", pd.DataFrame({"x": [10, 20]}))

    with producer.start_run("h5_producer", model="m"):
        parent_art = producer.log_output(h5_file, key="h5_parent", driver="h5")
        child_art = producer.log_output(
            h5_file,
            key="h5_child",
            driver="h5",
            member="group/table",
            parent_artifact_id=parent_art.id,
        )

    recovery_root = scratch / "recovery"
    producer.archive_artifact(parent_art, recovery_root, mode="copy")
    producer.close()

    h5_file.unlink()

    consumer = Tracker(
        run_dir=tmp_path / "consumer_ws",
        db_path=str(db_path),
    )

    resolved, _, _ = resolve_verified_local_path_detailed(
        consumer, child_art, allow_recovery_roots=True
    )
    assert resolved is not None
    assert resolved.exists()
    consumer.close()


# ---------------------------------------------------------------------------
# 4. Archived + unmounted historical mount views
# ---------------------------------------------------------------------------


def test_archived_unmounted_historical_mount_view(tmp_path: Path) -> None:
    db_path = tmp_path / "prov.duckdb"
    shared_scratch = tmp_path / "shared_scratch"
    producer_ws = tmp_path / "producer_ws"

    producer = Tracker(
        run_dir=producer_ws,
        db_path=str(db_path),
        mounts={"scratch": str(shared_scratch)},
    )
    data_file = producer_ws / "table.parquet"
    pd.DataFrame({"x": [100, 200, 300]}).to_parquet(data_file)
    with producer.start_run("prod", model="m"):
        art = producer.log_output(data_file, key="tbl")
    producer.archive_artifact(art, shared_scratch / "archive", mode="move")
    producer.close()

    viewer = Tracker(
        run_dir=tmp_path / "viewer_ws",
        db_path=str(db_path),
        read_only=True,
    )
    viewer.create_view("v_tbl", "tbl")
    df = viewer.query_df("SELECT * FROM v_tbl")
    assert df is not None
    assert len(df) == 3
    assert list(df["x"]) == [100, 200, 300]
    viewer.close()


# ---------------------------------------------------------------------------
# 5. Symlink & macOS/HPC mount-root parity tests
# ---------------------------------------------------------------------------


def test_valid_directory_under_symlinked_mount_root_succeeds(tmp_path: Path) -> None:
    real_mount = tmp_path / "real_scratch"
    mount_link = tmp_path / "symlinked_scratch"
    mount_link.symlink_to(real_mount, target_is_directory=True)

    zarr_dir = real_mount / "outputs" / "skims.zarr"
    zarr_dir.mkdir(parents=True)
    (zarr_dir / "0.0").write_bytes(b"z")

    fs = FileSystemManager(
        run_dir=tmp_path / "ws",
        mounts={"scratch": str(mount_link)},
    )
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://outputs/skims.zarr"
    )
    assert coord.is_resolved
    assert coord.lexical_path == mount_link / "outputs" / "skims.zarr"
    assert coord.mount_canonical_lexical_path == real_mount / "outputs" / "skims.zarr"
    assert coord.canonical_path == real_mount / "outputs" / "skims.zarr"


def test_contained_in_tree_symlink_rejected_by_archive_policy(tmp_path: Path) -> None:
    mount = tmp_path / "scratch"
    sub = mount / "sub"
    sub.mkdir(parents=True)
    real_file = sub / "real.csv"
    real_file.write_text("x,y\n1,2\n")
    symlink_file = sub / "link.csv"
    symlink_file.symlink_to(real_file)

    tracker = Tracker(
        run_dir=tmp_path / "ws",
        db_path=str(tmp_path / "prov.duckdb"),
        mounts={"scratch": str(mount)},
    )

    def reject_symlink(path: Path) -> bool:
        if tracker._archive_service._has_symlink_component(path):
            raise ValueError(f"OutputSet source contains a symlink: {path}")
        return True

    art = Artifact(
        key="member",
        uri="scratch://sub/link.csv",
        driver="csv",
        meta={"output_set_member": True},
    )
    with pytest.raises(ValueError, match="OutputSet source contains a symlink"):
        tracker._archive_service._find_archive_source_candidate(
            art,
            source_validator=reject_symlink,
            fallback_validator=reject_symlink,
            allow_run_owned_uri_fallback=True,
        )
    tracker.close()


def test_dangling_symlink_in_archive_source_raises_value_error(tmp_path: Path) -> None:
    mount = tmp_path / "scratch"
    sub = mount / "sub"
    sub.mkdir(parents=True)
    dangling_file = sub / "dangling.csv"
    dangling_file.symlink_to(sub / "nonexistent.csv")

    tracker = Tracker(
        run_dir=tmp_path / "ws",
        db_path=str(tmp_path / "prov.duckdb"),
        mounts={"scratch": str(mount)},
    )

    def reject_symlink(path: Path) -> bool:
        if tracker._archive_service._has_symlink_component(path):
            raise ValueError(f"Symlink source is not supported: {path}")
        return True

    art = Artifact(
        key="dangling",
        uri="scratch://sub/dangling.csv",
        driver="csv",
    )
    with pytest.raises(ValueError, match="Symlink source is not supported"):
        tracker._archive_service._find_archive_source_candidate(
            art,
            source_validator=reject_symlink,
            fallback_validator=reject_symlink,
            allow_run_owned_uri_fallback=True,
        )
    tracker.close()


# ---------------------------------------------------------------------------
# 6. Caller-specific policy regressions
# ---------------------------------------------------------------------------


def test_validate_cached_run_outputs_does_not_hash_scalar_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "prov.duckdb"
    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(db_path))

    out_file = tracker.run_dir / "out.csv"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text("x,y\n1,2\n")

    with tracker.start_run("p", model="producer"):
        tracker.log_output(out_file, key="out")

    import consist.core.cache as consist_cache

    digest_called = False

    def _spy_verify(*args: object, **kwargs: object) -> bool:
        nonlocal digest_called
        digest_called = True
        return True

    monkeypatch.setattr(consist_cache, "_verify_artifact_path_digest", _spy_verify)

    with tracker.start_run("c", model="producer", cache_mode="reuse") as t:
        assert t.is_cached

    assert not digest_called, (
        "Scalar file must not be hashed during cache-hit validation"
    )
    tracker.close()


def test_materialize_run_outputs_db_fallback_never_strictly_isolated(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    outside_dir = tmp_path / "outside_source"
    outside_dir.mkdir(parents=True)
    out_dir = outside_dir / "skims.zarr"
    out_dir.mkdir(parents=True)
    (out_dir / "0.0").write_bytes(b"outside")

    tracker = Tracker(
        run_dir=outside_dir,
        db_path=str(db_path),
    )
    with tracker.start_run("p", model="m"):
        tracker.log_output(out_dir, key="out", artifact_kind="directory")
    tracker.close()

    empty_source = tmp_path / "empty_source"
    empty_source.mkdir()

    consumer = Tracker(
        run_dir=tmp_path / "consumer",
        db_path=str(db_path),
        allow_external_paths=True,
    )
    mat_res = consumer.materialize_run_outputs(
        "p",
        target_root=tmp_path / "target",
        source_root=empty_source,
        db_fallback="never",
    )
    assert "out" in mat_res.skipped_missing_source
    assert not (tmp_path / "target" / "skims.zarr").exists()
    consumer.close()


# ---------------------------------------------------------------------------
# 7. Performance / hot-path regression check
# ---------------------------------------------------------------------------


def test_mount_precedence_resolver_memoization_zero_per_artifact_db_churn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "prov.duckdb"
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    tracker = Tracker(
        run_dir=tmp_path / "ws",
        db_path=str(db_path),
        mounts={"scratch": str(scratch)},
    )

    artifacts: list[Artifact] = []
    with tracker.start_run("batch_producer", model="m"):
        for i in range(50):
            p = tracker.run_dir / f"item_{i}.parquet"
            pd.DataFrame({"idx": [i]}).to_parquet(p)
            art = tracker.log_output(p, key=f"k_{i}")
            artifacts.append(art)

    tracker.close()

    consumer = Tracker(
        run_dir=tmp_path / "consumer_ws",
        db_path=str(db_path),
    )

    lookup_count = 0
    orig_lookup = consumer.db.lookup_historical_mount_roots

    def _counted_lookup(mount_name: str) -> tuple[str, ...]:
        nonlocal lookup_count
        lookup_count += 1
        return orig_lookup(mount_name)

    monkeypatch.setattr(consumer.db, "lookup_historical_mount_roots", _counted_lookup)

    resolver = MountPrecedenceResolver.from_tracker(consumer)
    for art in artifacts:
        rows = consumer.db.get_artifact_recovery_locations(art.id)
        for r in rows:
            resolver.resolve_row_coordinates(r, art)

    assert lookup_count <= 1, (
        f"Expected <=1 DB lookup for scheme 'scratch', got {lookup_count}"
    )
    consumer.close()


# ---------------------------------------------------------------------------
# 8. PILATES launcher.py mount-configuration regression test
# ---------------------------------------------------------------------------


def test_pilates_launcher_mount_configuration_resolves_step_scoped_scratch(
    tmp_path: Path,
) -> None:
    """Replays Lawrencium job28061865 cold-scratch recovery with PILATES launcher mount config."""
    db_path = tmp_path / "provenance.duckdb"
    producer_local_run = tmp_path / "job28019302" / "run"
    consumer_local_run = tmp_path / "job28061865" / "run"
    global_scratch = tmp_path / "lrc_scratch"
    inputs_root = tmp_path / "inputs"

    producer_local_run.mkdir(parents=True)
    consumer_local_run.mkdir(parents=True)
    global_scratch.mkdir(parents=True)
    inputs_root.mkdir(parents=True)

    # Step-scoped archive location on scratch
    recovery_dir = (
        global_scratch
        / "pilates-recovery"
        / "step_sim"
        / "beam"
        / "beam_output"
        / "skims.zarr"
    )
    recovery_dir.mkdir(parents=True)
    (recovery_dir / "0.0").write_bytes(b"pilates-zarr-chunks")

    # Producer setup
    producer = Tracker(
        run_dir=producer_local_run,
        db_path=str(db_path),
        mounts={
            "inputs": str(inputs_root),
            "workspace": str(producer_local_run),
            "beam_input": str(producer_local_run / "beam" / "input"),
            "scratch": str(global_scratch),
        },
    )

    local_zarr = producer_local_run / "beam" / "beam_output" / "skims.zarr"
    local_zarr.mkdir(parents=True)
    (local_zarr / "0.0").write_bytes(b"pilates-zarr-chunks")

    with producer.start_run("step_sim", model="beam"):
        art = producer.log_output(
            local_zarr, key="zarr_skims", artifact_kind="directory"
        )

    # Producer records active rows on workspace and scratch
    row_scratch = ArtifactLocationRow.create(
        digest=str(art.digest),
        artifact_id=art.id,
        site="archive",
        mount_name="scratch",
        mount_instance_id="",
        uri="scratch://pilates-recovery/step_sim/beam/beam_output/skims.zarr",
    )
    producer.db.upsert_artifact_locations([row_scratch])
    producer.close()

    # Cold scratch: wipe the producer local run dir
    shutil.rmtree(producer_local_run)

    # PILATES launcher.py configuration on the consumer node
    consumer = Tracker(
        run_dir=consumer_local_run,
        db_path=str(db_path),
        mounts={
            "inputs": str(inputs_root),
            "workspace": str(consumer_local_run),
            "beam_input": str(consumer_local_run / "beam" / "input"),
            "scratch": str(global_scratch),
        },
        allow_external_paths=True,
    )

    # MountPrecedenceResolver and iter_artifact_candidates must resolve the scratch candidate
    cands = list(iter_artifact_candidates(consumer, art))
    assert any(c.canonical_path == recovery_dir.resolve() for c in cands)

    # Materialize the input cleanly into consumer workspace
    staged_dest = consumer_local_run / "beam" / "beam_output" / "skims.zarr"
    mat_res = consumer.stage_artifact(art, destination=staged_dest)
    assert mat_res.status == "staged"
    assert (staged_dest / "0.0").read_bytes() == b"pilates-zarr-chunks"

    consumer.close()


# ---------------------------------------------------------------------------
# 9. Zarr Lustre performance check: DirTreeStat witness skips full-tree hashing
# ---------------------------------------------------------------------------


def test_dirtreestat_witness_fast_path_skips_full_tree_chunk_hashing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A settled DirTreeStat witness skips recursive manifest/chunk hashing on Lustre.

    Only settled (content-verified) witnesses certify bytes without
    re-validation; the witness is therefore settled on the validating
    tracker's own DB instance before the consumer run.
    """
    db_path = tmp_path / "prov.duckdb"
    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(db_path))

    zarr_dir = tracker.run_dir / "skims.zarr"
    zarr_dir.mkdir(parents=True)
    for i in range(10):
        (zarr_dir / f"chunk_{i}").write_bytes(f"chunk-content-{i}".encode())

    with tracker.start_run("zarr_producer", model="m"):
        art = tracker.log_output(zarr_dir, key="skims", artifact_kind="directory")

    import time

    future_time = time.time_ns() + 10_000_000_000
    witness = DirTreeStat.capture_dir(zarr_dir, observed_at_ns=future_time)
    row = tracker.db.get_active_artifact_locations(artifact_id=art.id)[0]

    # Spy on is_valid_directory_artifact_tree to ensure it is NOT called when
    # the witness is settled (digest-verified)
    import consist.core.cache as consist_cache

    manifest_validation_called = False

    def _spy_manifest_validator(*args: object, **kwargs: object) -> bool:
        nonlocal manifest_validation_called
        manifest_validation_called = True
        return True

    monkeypatch.setattr(
        consist_cache, "is_valid_directory_artifact_tree", _spy_manifest_validator
    )
    monkeypatch.setattr(time, "time_ns", lambda: future_time)

    consumer = Tracker(run_dir=tmp_path / "consumer_ws", db_path=str(db_path))
    # Settle on the validating instance: settlement registry is per-DB-instance.
    consumer.db.settle_artifact_location_witness(row.location_id, witness)
    with consumer.start_run(
        "zarr_consumer",
        model="m",
        inputs=[art],
        cache_mode="overwrite",
        cache_hydration="inputs-missing",
        validate_materialized_inputs=True,
    ):
        pass

    assert not manifest_validation_called, (
        "Settled FRESH witness must skip recursive directory tree validation"
    )
    consumer.close()
    tracker.close()


def test_materialize_requested_inputs_returns_hash_mismatch_not_missing_source(
    tmp_path: Path,
) -> None:
    """When bytes exist with wrong hash, requested input staging detects mismatch rather than missing."""
    db_path = tmp_path / "prov.duckdb"
    producer_ws = tmp_path / "producer_ws"
    consumer_ws = tmp_path / "consumer_ws"
    producer_ws.mkdir(parents=True)
    consumer_ws.mkdir(parents=True)

    f = producer_ws / "input.csv"
    f.write_text("x,y\n1,2\n")

    producer = Tracker(run_dir=producer_ws, db_path=str(db_path))
    with producer.start_run("p", model="m"):
        art = producer.log_output(f, key="inp")
    producer.close()

    # Corrupt the file on disk
    f.write_text("x,y\n999,888\n")

    consumer = Tracker(
        run_dir=consumer_ws, db_path=str(db_path), allow_external_paths=True
    )
    from consist.core.materialize import stage_artifact

    staged_dest = consumer_ws / "staged_input.csv"
    stage_res = stage_artifact(
        consumer,
        art,
        staged_dest,
        validate_content_hash="if-present",
    )
    assert stage_res.status == "failed"
    assert not staged_dest.exists()

    mat_res = consumer.materialize_run_outputs(
        "p",
        target_root=consumer_ws / "mat_target",
    )
    assert any(
        k == "inp" and "hash mismatch" in msg.lower() for k, msg in mat_res.failed
    )
    assert "inp" not in mat_res.skipped_missing_source

    with consumer.start_run(
        "c",
        model="consumer",
        inputs=[art],
        cache_mode="reuse",
        cache_hydration="inputs-missing",
        validate_materialized_inputs=True,
    ):
        pass

    consumer.close()


# ---------------------------------------------------------------------------
# 10. Fail-closed root containment (C1)
# ---------------------------------------------------------------------------


def _containment_case_setup(tmp_path: Path, case: str) -> tuple[Path, Path, bool]:
    """Build the containment fixture root and expected leaf path.

    Returns ``(root, expected_leaf, should_yield)`` where ``expected_leaf`` is
    the lexical leaf path under the (possibly symlinked) root.
    """
    root = tmp_path / "case_root"
    (root / "outputs").mkdir(parents=True)
    leaf = root / "outputs" / "x.csv"
    outside = tmp_path / "outside"
    outside.mkdir(exist_ok=True)

    if case == "safe_existing":
        leaf.write_bytes(b"payload")
        return root, leaf, True
    if case == "safe_nonexistent":
        return root, leaf, True
    if case == "escaping_ancestor":
        (outside / "x.csv").write_bytes(b"outside")
        (root / "outputs").rmdir()
        (root / "outputs").symlink_to(outside, target_is_directory=True)
        return root, leaf, False
    if case == "escaping_leaf":
        (outside / "x.csv").write_bytes(b"outside")
        leaf.symlink_to(outside / "x.csv")
        return root, leaf, False
    if case == "dangling_leaf":
        leaf.symlink_to(root / "outputs" / "missing.csv")
        return root, leaf, False
    if case == "symlink_loop":
        leaf.symlink_to(leaf)
        return root, leaf, False
    if case == "contained_leaf":
        (root / "real").mkdir()
        (root / "real" / "x.csv").write_bytes(b"contained")
        leaf.symlink_to(root / "real" / "x.csv")
        return root, leaf, True
    if case == "symlinked_root":
        real_root = tmp_path / "real_case_root"
        (real_root / "outputs").mkdir(parents=True)
        (real_root / "outputs" / "x.csv").write_bytes(b"payload")
        link = tmp_path / "root_link"
        link.symlink_to(real_root, target_is_directory=True)
        return link, link / "outputs" / "x.csv", True
    raise AssertionError(f"unknown case {case}")


def _candidates_for_root_kind(
    kind: str,
    tmp_path: Path,
    root: Path,
    *,
    on_rejected=None,
):
    """Invoke iter_artifact_candidates with real Tracker/DB root-kind plumbing."""
    artifact = Artifact(
        key="x",
        uri="./outputs/x.csv",
        driver="csv",
        digest=None,
    )
    ws = tmp_path / "ws"
    if kind == "source_root":
        tracker = Tracker(run_dir=ws, allow_external_paths=True)
        return list(
            iter_artifact_candidates(
                tracker, artifact, source_root=root, on_rejected=on_rejected
            )
        )
    db_path = str(tmp_path / "prov.duckdb")
    if kind == "historical_root":
        tracker = Tracker(run_dir=ws, db_path=db_path, allow_external_paths=True)
        try:
            run = Run(
                id="producer",
                model_name="m",
                meta={"_physical_run_dir": str(root)},
            )
            tracker.db.upsert_run(run)
            artifact = Artifact(
                key="x",
                uri="./outputs/x.csv",
                driver="csv",
                digest=None,
                run_id="producer",
            )
            return list(
                iter_artifact_candidates(tracker, artifact, on_rejected=on_rejected)
            )
        finally:
            tracker.close()
    if kind == "abs_recovery_root":
        tracker = Tracker(run_dir=ws, db_path=db_path, allow_external_paths=True)
        try:
            tracker.db.sync_artifact(artifact)
            tracker.set_artifact_recovery_roots(artifact, [str(root)])
            rows = tracker.db.get_artifact_recovery_locations(artifact.id)
            assert rows and all(r.mount_name == "abs" for r in rows)
            return list(
                iter_artifact_candidates(tracker, artifact, on_rejected=on_rejected)
            )
        finally:
            tracker.close()
    if kind == "named_mount_recovery_root":
        tracker = Tracker(
            run_dir=ws,
            db_path=db_path,
            mounts={"arch": str(root)},
            allow_external_paths=True,
        )
        try:
            tracker.db.sync_artifact(artifact)
            tracker.set_artifact_recovery_roots(artifact, [str(root)])
            rows = tracker.db.get_artifact_recovery_locations(artifact.id)
            assert rows and all(r.mount_name == "arch" for r in rows)
            return list(
                iter_artifact_candidates(tracker, artifact, on_rejected=on_rejected)
            )
        finally:
            tracker.close()
    raise AssertionError(f"unknown kind {kind}")


@pytest.mark.parametrize(
    "case",
    [
        "safe_existing",
        "safe_nonexistent",
        "escaping_ancestor",
        "escaping_leaf",
        "dangling_leaf",
        "symlink_loop",
        "contained_leaf",
        "symlinked_root",
    ],
)
@pytest.mark.parametrize(
    "kind",
    [
        "source_root",
        "historical_root",
        "abs_recovery_root",
        "named_mount_recovery_root",
    ],
)
def test_root_containment_fail_closed_by_root_kind(
    tmp_path: Path, kind: str, case: str
) -> None:
    root, expected_leaf, should_yield = _containment_case_setup(tmp_path, case)
    cands = _candidates_for_root_kind(kind, tmp_path, root)
    # get_historical_root(resolve=True) canonicalizes a symlinked root and a
    # contained leaf symlink stays visible as the canonical leaf, so match on
    # either the lexical leaf or its canonical resolution. Python 3.11's
    # Path.resolve() raises RuntimeError on symlink loops; the loop case never
    # needs canonical matching, so fall back to None there.
    try:
        expected_leaf_canonical: Path | None = expected_leaf.resolve()
    except (OSError, RuntimeError):
        expected_leaf_canonical = None
    safe_matches = [
        c
        for c in cands
        if (
            c.lexical_path == expected_leaf
            or (
                expected_leaf_canonical is not None
                and c.canonical_path == expected_leaf_canonical
            )
        )
    ]
    if should_yield:
        assert len(safe_matches) == 1
        assert safe_matches[0].canonical_path.exists() == (
            case in ("safe_existing", "contained_leaf", "symlinked_root")
        )
    else:
        assert safe_matches == []


@pytest.mark.parametrize(
    "case",
    ["escaping_ancestor", "escaping_leaf", "dangling_leaf", "symlink_loop"],
)
@pytest.mark.parametrize(
    "kind",
    [
        "source_root",
        "historical_root",
        "abs_recovery_root",
        "named_mount_recovery_root",
    ],
)
def test_include_rejected_yields_escaping_candidate_flagged(
    tmp_path: Path, kind: str, case: str
) -> None:
    root, expected_leaf, _ = _containment_case_setup(tmp_path, case)
    rejected: list[TranslatedCoordinate] = []
    cands = _candidates_for_root_kind(kind, tmp_path, root, on_rejected=rejected.append)
    assert not any(c.lexical_path == expected_leaf for c in cands)
    assert any(coord.lexical_path == expected_leaf for coord in rejected)


def test_unsafe_first_explicit_root_does_not_shadow_safe_second(tmp_path: Path) -> None:
    bad_root = tmp_path / "bad_root"
    (bad_root / "outputs").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "x.csv").write_bytes(b"outside")
    (bad_root / "outputs").rmdir()
    (bad_root / "outputs").symlink_to(outside, target_is_directory=True)

    good_root = tmp_path / "good_root"
    good_leaf = good_root / "outputs" / "x.csv"
    good_leaf.parent.mkdir(parents=True)
    good_leaf.write_bytes(b"good")

    artifact = Artifact(
        key="x",
        uri="./outputs/x.csv",
        driver="csv",
        digest=None,
    )
    tracker = Tracker(run_dir=tmp_path / "ws", allow_external_paths=True)
    cands = list(
        iter_artifact_candidates(
            tracker, artifact, source_root=[str(bad_root), str(good_root)]
        )
    )
    safe = [c for c in cands if c.lexical_path == good_leaf]
    assert len(safe) == 1
    assert safe[0].canonical_path == good_leaf.resolve()


# ---------------------------------------------------------------------------
# 11. Workspace instance verification (C2) with a real DB
# ---------------------------------------------------------------------------


def test_foreign_workspace_instance_row_not_resolved_as_candidate(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    ws_a = tmp_path / "wsA"
    ws_b = tmp_path / "wsB"
    for ws, payload in ((ws_a, b"aaa"), (ws_b, b"bbb")):
        (ws / "outputs").mkdir(parents=True)
        (ws / "outputs" / "x.csv").write_bytes(payload)

    from consist.location import workspace_mount_instance_id

    producer = Tracker(run_dir=ws_b, db_path=str(db_path))
    with producer.start_run("produce", model="m"):
        art_b = producer.log_output(ws_b / "outputs" / "x.csv", key="x")
    producer.close()

    # Foreign row: different artifact id, same digest as art_b, claims wsA's
    # workspace instance. Digest-based lookup pulls it into candidate rows.
    foreign_row = ArtifactLocationRow.create(
        digest=str(art_b.digest),
        artifact_id=uuid.uuid4(),
        site="archive",
        mount_name="workspace",
        mount_instance_id=workspace_mount_instance_id(ws_a),
        uri="workspace://outputs/x.csv",
    )

    tracker_c = Tracker(run_dir=tmp_path / "wsC", db_path=str(db_path))
    loaded = tracker_c.get_artifact(art_b.id)
    assert loaded is not None
    tracker_c.db.upsert_artifact_locations([foreign_row])

    cands = list(iter_artifact_candidates(tracker_c, loaded))
    # The foreign wsA row must not be resolved (C2), and wsA's bytes must
    # never be reachable through it.
    assert not any(
        c.row is not None and c.row.location_id == foreign_row.location_id
        for c in cands
    )
    assert not any(
        c.canonical_path == (ws_a / "outputs" / "x.csv").resolve() for c in cands
    )
    # art_B's own recorded location under wsB stays reachable.
    assert any(
        c.canonical_path == (ws_b / "outputs" / "x.csv").resolve() for c in cands
    )
    tracker_c.close()


# ---------------------------------------------------------------------------
# 12. Historical candidates after ledger rows (C3), real DB
# ---------------------------------------------------------------------------


def _producer_with_output(
    tmp_path: Path, db_path: Path, *, payload: bytes = b"historical-bytes"
) -> tuple[Artifact, Path]:
    ws_p = tmp_path / "wsP"
    (ws_p / "outputs").mkdir(parents=True)
    out = ws_p / "outputs" / "x.csv"
    out.write_bytes(payload)
    producer = Tracker(run_dir=ws_p, db_path=str(db_path))
    with producer.start_run("produce", model="m"):
        art = producer.log_output(out, key="x")
    producer.close()
    return art, out


def test_unresolvable_row_still_yields_owning_run_historical(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    art, out = _producer_with_output(tmp_path, db_path)

    consumer = Tracker(run_dir=tmp_path / "wsC", db_path=str(db_path))
    loaded = consumer.get_artifact(art.id)
    assert loaded is not None
    cold_row = ArtifactLocationRow.create(
        digest=str(loaded.digest),
        artifact_id=loaded.id,
        site="archive",
        mount_name="cold",
        mount_instance_id="",
        uri="cold://other/x.csv",
    )
    consumer.db.upsert_artifact_locations([cold_row])

    cands = list(iter_artifact_candidates(consumer, loaded))
    # The unresolvable cold row yields nothing; the owning-run historical
    # location at wsP stays reachable (C3).
    assert not any(
        c.row is not None and c.row.location_id == cold_row.location_id for c in cands
    )
    ws_p_cand = [c for c in cands if c.canonical_path == out.resolve()]
    assert len(ws_p_cand) == 1

    dest = consumer.run_dir / "staged" / "x.csv"
    result = consumer.stage_artifact(
        loaded, destination=dest, validate_content_hash="never"
    )
    assert result.status == "staged"
    assert dest.read_bytes() == b"historical-bytes"
    consumer.close()


def test_rowless_historical_uri_when_only_row_is_unresolvable(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    art, out = _producer_with_output(tmp_path, db_path)

    consumer = Tracker(run_dir=tmp_path / "wsC", db_path=str(db_path))
    loaded = consumer.get_artifact(art.id)
    assert loaded is not None
    # Mark the artifact's own row stale so the only active row is the
    # unresolvable cold row; the owning-run fallback must still fire.
    for own_row in consumer.db.get_active_artifact_locations(
        artifact_id=loaded.id, digest=loaded.digest, member=loaded.member
    ):
        consumer.db.mark_artifact_location_status(own_row.location_id, "stale")
    cold_row = ArtifactLocationRow.create(
        digest=str(loaded.digest),
        artifact_id=loaded.id,
        site="archive",
        mount_name="cold",
        mount_instance_id="",
        uri="cold://other/x.csv",
    )
    consumer.db.upsert_artifact_locations([cold_row])

    cands = list(iter_artifact_candidates(consumer, loaded))
    historical = [
        c
        for c in cands
        if c.source == "historical_uri"
        and c.row is None
        and c.canonical_path == out.resolve()
    ]
    assert len(historical) == 1
    consumer.close()


def test_ledger_primary_precedes_historical_candidate(tmp_path: Path) -> None:
    db_path = tmp_path / "prov.duckdb"
    art, _ = _producer_with_output(tmp_path, db_path)

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    scratch_file = scratch / "x.csv"
    scratch_file.write_bytes(b"ledger-bytes")

    consumer = Tracker(
        run_dir=tmp_path / "wsC",
        db_path=str(db_path),
        mounts={"scratch": str(scratch)},
    )
    loaded = consumer.get_artifact(art.id)
    assert loaded is not None
    # Mark the artifact's own row stale so the scratch row is the only active
    # ledger row; ordering between ledger and fallback sources is then exact.
    for own_row in consumer.db.get_active_artifact_locations(
        artifact_id=loaded.id, digest=loaded.digest, member=loaded.member
    ):
        consumer.db.mark_artifact_location_status(own_row.location_id, "stale")
    consumer.db.upsert_artifact_locations(
        [
            ArtifactLocationRow.create(
                digest=str(loaded.digest),
                artifact_id=loaded.id,
                site="archive",
                mount_name="scratch",
                mount_instance_id="",
                uri="scratch://x.csv",
            )
        ]
    )

    cands = list(iter_artifact_candidates(consumer, loaded))
    sources = [c.source for c in cands]
    assert sources[0] == "ledger_primary"
    assert "historical_uri" in sources
    assert sources.index("ledger_primary") < sources.index("historical_uri")
    primary = next(c for c in cands if c.source == "ledger_primary")
    assert primary.canonical_path == scratch_file.resolve()
    consumer.close()


def test_staging_falls_through_missing_ledger_row_to_historical(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    art, out = _producer_with_output(tmp_path, db_path)

    scratch = tmp_path / "scratch"
    scratch.mkdir()

    consumer = Tracker(
        run_dir=tmp_path / "wsC",
        db_path=str(db_path),
        mounts={"scratch": str(scratch)},
    )
    loaded = consumer.get_artifact(art.id)
    assert loaded is not None
    # Ledger row points at a missing file on the configured scratch mount.
    consumer.db.upsert_artifact_locations(
        [
            ArtifactLocationRow.create(
                digest=str(loaded.digest),
                artifact_id=loaded.id,
                site="archive",
                mount_name="scratch",
                mount_instance_id="",
                uri="scratch://x.csv",
            )
        ]
    )

    dest = consumer.run_dir / "staged" / "x.csv"
    result = consumer.stage_artifact(
        loaded, destination=dest, validate_content_hash="never"
    )
    assert result.status == "staged"
    assert dest.read_bytes() == out.read_bytes()
    consumer.close()


def test_artifact_mount_root_candidate_uses_unprefixed_suffix(
    tmp_path: Path,
) -> None:
    root_x = tmp_path / "rootX"
    (root_x / "a").mkdir(parents=True)
    (root_x / "a" / "b.csv").write_bytes(b"data-mount")

    root_w = tmp_path / "rootW"
    (root_w / "a").mkdir(parents=True)
    (root_w / "a" / "b.csv").write_bytes(b"workspace-mount")

    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(tmp_path / "prov.duckdb"))

    art_data = Artifact(
        key="b",
        uri="data://a/b.csv",
        driver="csv",
        digest=None,
        meta={"mount_root": str(root_x)},
    )
    cands = list(iter_artifact_candidates(tracker, art_data))
    assert any(
        c.source == "historical_uri"
        and c.canonical_path == (root_x / "a" / "b.csv").resolve()
        for c in cands
    )

    art_ws = Artifact(
        key="b",
        uri="workspace://a/b.csv",
        driver="csv",
        digest=None,
        meta={"mount_root": str(root_w)},
    )
    cands = list(iter_artifact_candidates(tracker, art_ws))
    assert any(
        c.source == "historical_uri"
        and c.canonical_path == (root_w / "a" / "b.csv").resolve()
        for c in cands
    )
    tracker.close()


def test_cross_run_artifact_does_not_leak_current_managed_mount(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    art, _ = _producer_with_output(tmp_path, db_path)

    consumer_scratch = tmp_path / "consumer_scratch"
    consumer_scratch.mkdir()
    (consumer_scratch / "x.csv").write_bytes(b"consumer-local")

    consumer = Tracker(
        run_dir=tmp_path / "wsC",
        db_path=str(db_path),
        mounts={"scratch": str(consumer_scratch)},
    )
    loaded = consumer.get_artifact(art.id)
    assert loaded is not None
    consumer.db.upsert_artifact_locations(
        [
            ArtifactLocationRow.create(
                digest=str(loaded.digest),
                artifact_id=loaded.id,
                site="archive",
                mount_name="scratch",
                mount_instance_id="",
                uri="scratch://x.csv",
            )
        ]
    )

    cands = list(iter_artifact_candidates(consumer, loaded))
    assert not any(c.source == "managed_mount" for c in cands)
    consumer.close()


# ---------------------------------------------------------------------------
# 13. Archive source policy (C4/C5/C6/D6)
# ---------------------------------------------------------------------------


def _sha256_file_digest(payload: str):
    import hashlib

    from consist.digest import Digest

    return Digest("sha256:file", hashlib.sha256(payload.encode("utf-8")).hexdigest())


def _reject_symlink_factory(service):
    def reject_symlink(path: Path) -> bool:
        if service._has_symlink_component(path):
            raise ValueError(f"OutputSet source contains a symlink: {path}")
        return True

    return reject_symlink


def test_archive_replace_backup_symlink_component_raises(tmp_path: Path) -> None:
    """C4: the raw backup entry is validated before resolution."""
    payload = "backup-payload"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "a.csv").write_text(payload)
    backup_target = tmp_path / "backup_moved"
    (backup_target / "outputs").mkdir(parents=True)
    (backup_target / "outputs" / "a.csv").symlink_to(outside / "a.csv")

    archive_root = tmp_path / "archive"
    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(tmp_path / "prov.duckdb"))
    service = tracker._archive_service
    art = Artifact(
        key="member",
        uri="workspace://outputs/a.csv",
        driver="csv",
        digest=_sha256_file_digest(payload),
    )
    tracker.db.sync_artifact(art)
    service.set_artifact_recovery_roots(art, [str(archive_root)])
    service._active_archive_replace_backup = (
        archive_root.resolve(),
        backup_target.resolve(),
    )
    try:
        with pytest.raises(ValueError, match="OutputSet source contains a symlink"):
            service._find_output_set_archive_source(art)
    finally:
        service._active_archive_replace_backup = None
        tracker.close()


def test_cross_run_outputset_prefers_clean_ledger_over_symlinked_current_mount(
    tmp_path: Path,
) -> None:
    """C5: a symlinked current-mount twin must not block a clean ledger source."""
    db_path = tmp_path / "prov.duckdb"
    producer_scratch = tmp_path / "producer_scratch"
    producer_scratch.mkdir()
    payload = "ledger-payload"
    ledger_file = producer_scratch / "data.csv"
    ledger_file.write_text(payload)

    producer = Tracker(
        run_dir=tmp_path / "producer_ws",
        db_path=str(db_path),
        mounts={"scratch": str(producer_scratch)},
    )
    with producer.start_run("produce", model="m"):
        art = producer.log_output(ledger_file, key="data")
    producer.close()

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "data.csv").write_text(payload)
    consumer_scratch = tmp_path / "consumer_scratch"
    consumer_scratch.mkdir()
    (consumer_scratch / "data.csv").symlink_to(outside / "data.csv")

    consumer = Tracker(
        run_dir=tmp_path / "consumer_ws",
        db_path=str(db_path),
        mounts={"scratch": str(consumer_scratch)},
    )
    loaded = consumer.get_artifact(art.id)
    assert loaded is not None
    service = consumer._archive_service
    reject_symlink = _reject_symlink_factory(service)
    source = service._find_archive_source_candidate(
        loaded,
        source_validator=reject_symlink,
        fallback_validator=reject_symlink,
        allow_run_owned_uri_fallback=False,
        include_current_managed_uri=False,
    )
    assert source == ledger_file.resolve()
    consumer.close()


@pytest.mark.parametrize("dangling", [False, True])
def test_escaping_ledger_source_symlink_still_raises(
    tmp_path: Path, dangling: bool
) -> None:
    """D6 kept: the pre-scan still sees escaping symlinked historical sources."""
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "x.csv").write_text("payload")

    hist_root = tmp_path / "hist_root"
    (hist_root / "outputs").mkdir(parents=True)
    leaf = hist_root / "outputs" / "x.csv"
    if dangling:
        leaf.symlink_to(outside / "missing.csv")
    else:
        leaf.symlink_to(outside / "x.csv")

    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(tmp_path / "prov.duckdb"))
    tracker.db.upsert_run(
        Run(id="producer", model_name="m", meta={"_physical_run_dir": str(hist_root)})
    )
    service = tracker._archive_service
    reject_symlink = _reject_symlink_factory(service)
    art = Artifact(
        key="x",
        uri="workspace://outputs/x.csv",
        driver="csv",
        digest=_sha256_file_digest("payload"),
        run_id="producer",
    )
    with pytest.raises(ValueError, match="OutputSet source contains a symlink"):
        service._find_archive_source_candidate(
            art,
            source_validator=reject_symlink,
            fallback_validator=reject_symlink,
            allow_run_owned_uri_fallback=False,
            include_current_managed_uri=False,
        )
    tracker.close()


def test_cross_run_no_ledger_rows_uses_current_mount(tmp_path: Path) -> None:
    """C6: cross-run fallback to the current configured mount."""
    db_path = tmp_path / "prov.duckdb"
    payload = "current-mount-payload"

    producer = Tracker(run_dir=tmp_path / "producer_ws", db_path=str(db_path))
    with producer.start_run("produce", model="m"):
        pass
    producer.close()

    consumer_scratch = tmp_path / "consumer_scratch"
    consumer_scratch.mkdir()
    (consumer_scratch / "data.csv").write_text(payload)

    consumer = Tracker(
        run_dir=tmp_path / "consumer_ws",
        db_path=str(db_path),
        mounts={"scratch": str(consumer_scratch)},
    )
    art = Artifact(
        key="data",
        uri="scratch://data.csv",
        driver="csv",
        digest=_sha256_file_digest(payload),
        run_id="produce",
    )
    service = consumer._archive_service
    reject_symlink = _reject_symlink_factory(service)

    # Clean file at the current mount: archive succeeds (C6).
    source = service._find_archive_source_candidate(
        art,
        source_validator=reject_symlink,
        fallback_validator=reject_symlink,
        allow_run_owned_uri_fallback=True,
        include_current_managed_uri=False,
    )
    assert source == (consumer_scratch / "data.csv").resolve()

    # Symlinked file at the current mount: symlink error raises.
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "data.csv").write_text(payload)
    (consumer_scratch / "data.csv").unlink()
    (consumer_scratch / "data.csv").symlink_to(outside / "data.csv")
    with pytest.raises(ValueError, match="OutputSet source contains a symlink"):
        service._find_archive_source_candidate(
            art,
            source_validator=reject_symlink,
            fallback_validator=reject_symlink,
            allow_run_owned_uri_fallback=True,
            include_current_managed_uri=False,
        )
    consumer.close()


def test_symlinked_mount_root_existing_policy_unchanged(tmp_path: Path) -> None:
    """macOS /tmp-style symlinked mount root: existing lexical policy is unchanged.

    A lexical path through a symlinked mount root trips the archive symlink
    policy exactly as it did before this branch; canonical resolution through
    ``test_valid_directory_under_symlinked_mount_root_succeeds`` covers the
    translation layer.
    """
    db_path = tmp_path / "prov.duckdb"
    real_mount = tmp_path / "real_mount"
    real_mount.mkdir()
    payload = "mountlink-payload"
    real_file = real_mount / "data.csv"
    real_file.write_text(payload)
    mount_link = tmp_path / "mountlink"
    mount_link.symlink_to(real_mount, target_is_directory=True)

    producer = Tracker(
        run_dir=tmp_path / "producer_ws",
        db_path=str(db_path),
        mounts={"scratch": str(mount_link)},
    )
    with producer.start_run("produce", model="m"):
        art = producer.log_output(real_file, key="data")
    producer.close()

    consumer = Tracker(
        run_dir=tmp_path / "consumer_ws",
        db_path=str(db_path),
        mounts={"scratch": str(mount_link)},
    )
    loaded = consumer.get_artifact(art.id)
    assert loaded is not None
    service = consumer._archive_service
    reject_symlink = _reject_symlink_factory(service)
    with pytest.raises(ValueError, match="OutputSet source contains a symlink"):
        service._find_archive_source_candidate(
            loaded,
            source_validator=reject_symlink,
            fallback_validator=reject_symlink,
        )
    # The canonical translation layer still resolves through the mount link.
    coord = consumer.fs.translate_location_coordinate(
        "scratch", "", "scratch://data.csv"
    )
    assert coord.is_resolved
    assert coord.canonical_path == real_file.resolve()
    consumer.close()


# ---------------------------------------------------------------------------
# 14. Shared-resolver batch bounds (Task 6)
# ---------------------------------------------------------------------------


def _batch_producer_with_mount_rows(
    tmp_path: Path,
    db_path: Path,
    *,
    runs: int,
    per_run: int,
) -> list[Artifact]:
    scratch_root = tmp_path / "scratch_root"
    team_root = tmp_path / "team_root"
    scratch_root.mkdir()
    team_root.mkdir()

    producer = Tracker(
        run_dir=tmp_path / "wsP",
        db_path=str(db_path),
        mounts={"scratch": str(scratch_root), "team": str(team_root)},
    )
    artifacts: list[Artifact] = []
    for r in range(runs):
        with producer.start_run(f"batch_{r}", model="m"):
            for i in range(per_run):
                p = producer.run_dir / f"r{r}_item_{i}.csv"
                p.write_text("v\n1\n", encoding="utf-8")
                art = producer.log_output(p, key=f"r{r}_k_{i}")
                artifacts.append(art)
                for mount in ("scratch", "team"):
                    mpath = (
                        Path(producer.fs.mounts[mount]) / f"run{r}" / f"item_{i}.csv"
                    )
                    mpath.parent.mkdir(parents=True, exist_ok=True)
                    mpath.write_text("v\n1\n", encoding="utf-8")
                    producer.db.upsert_artifact_locations(
                        [
                            ArtifactLocationRow.create(
                                digest=str(art.digest),
                                artifact_id=art.id,
                                site="archive",
                                mount_name=mount,
                                mount_instance_id="",
                                uri=f"{mount}://run{r}/item_{i}.csv",
                            )
                        ]
                    )
    producer.close()
    return artifacts


def test_warm_artifact_locations_cache_bounded_lookups(tmp_path: Path) -> None:
    """One warm pass for 50 artifacts from 2 runs makes bounded DB/get_run calls."""
    db_path = tmp_path / "prov.duckdb"
    artifacts = _batch_producer_with_mount_rows(tmp_path, db_path, runs=2, per_run=25)

    consumer = Tracker(run_dir=tmp_path / "wsC", db_path=str(db_path))
    loaded = [a for a in (consumer.get_artifact(x.id) for x in artifacts) if a]

    with (
        patch.object(consumer, "get_run", wraps=consumer.get_run) as get_run_spy,
        patch.object(
            consumer.db,
            "lookup_historical_mount_roots",
            wraps=consumer.db.lookup_historical_mount_roots,
        ) as lookup_spy,
        patch.object(
            consumer.db,
            "get_active_artifact_locations_bulk",
            wraps=consumer.db.get_active_artifact_locations_bulk,
        ) as bulk_spy,
    ):
        warm_artifact_locations_cache(consumer, loaded)

    assert get_run_spy.call_count <= 2
    assert lookup_spy.call_count <= 2  # one per distinct named mount
    assert bulk_spy.call_count == 1
    consumer.close()


def test_load_cached_active_locations_sibling_prefetch_bounded(tmp_path: Path) -> None:
    """Sibling prefetch through _load_cached_active_locations keeps lookups bounded."""
    db_path = tmp_path / "prov.duckdb"
    artifacts = _batch_producer_with_mount_rows(tmp_path, db_path, runs=1, per_run=25)

    consumer = Tracker(run_dir=tmp_path / "wsC", db_path=str(db_path))
    loaded = [a for a in (consumer.get_artifact(x.id) for x in artifacts) if a]
    run_id = str(loaded[0].run_id)
    consumer.get_artifacts_for_run(run_id)  # populate the sibling cache

    with (
        patch.object(consumer, "get_run", wraps=consumer.get_run) as get_run_spy,
        patch.object(
            consumer.db,
            "lookup_historical_mount_roots",
            wraps=consumer.db.lookup_historical_mount_roots,
        ) as lookup_spy,
        patch.object(
            consumer.db,
            "get_active_artifact_locations_bulk",
            wraps=consumer.db.get_active_artifact_locations_bulk,
        ) as bulk_spy,
    ):
        entries = _load_cached_active_locations(consumer, loaded[0])

    assert entries
    assert get_run_spy.call_count <= 2
    assert lookup_spy.call_count <= 2  # one per distinct named mount
    assert bulk_spy.call_count == 1
    consumer.close()


def test_stage_artifacts_to_destinations_bounded_get_run(tmp_path: Path) -> None:
    """Staging 20 artifacts from one run calls get_run at most once."""
    db_path = tmp_path / "prov.duckdb"
    artifacts = _batch_producer_with_mount_rows(tmp_path, db_path, runs=1, per_run=20)

    consumer = Tracker(run_dir=tmp_path / "wsC", db_path=str(db_path))
    loaded = [a for a in (consumer.get_artifact(x.id) for x in artifacts) if a]

    from consist.core.materialize import stage_artifacts_to_destinations

    items = [(a.key, a, consumer.run_dir / "staged" / f"{a.key}.csv") for a in loaded]
    with patch.object(consumer, "get_run", wraps=consumer.get_run) as get_run_spy:
        result = stage_artifacts_to_destinations(
            consumer, items, validate_content_hash="never"
        )

    assert get_run_spy.call_count <= 1
    assert all(
        s.status in ("staged", "preserved_existing") for s in result.outputs.values()
    )
    consumer.close()


def test_shared_resolver_site_correct_memoization(tmp_path: Path) -> None:
    """Row resolution order (workspace/archive site) does not change shared-resolver results."""
    db_path = tmp_path / "prov.duckdb"
    ws_p = tmp_path / "phys_ws"
    (ws_p / "outputs").mkdir(parents=True)
    (ws_p / "outputs" / "x.csv").write_text("v\n1\n", encoding="utf-8")

    consumer = Tracker(run_dir=tmp_path / "wsC", db_path=str(db_path))
    art = Artifact(
        key="x",
        uri="workspace://outputs/x.csv",
        driver="csv",
        run_id="runX",
        digest=None,
    )
    rows = [
        ArtifactLocationRow.create(
            digest=None,
            artifact_id=art.id,
            site=site,
            mount_name="workspace",
            mount_instance_id="",
            uri="workspace://outputs/x.csv",
        )
        for site in ("workspace", "archive")
    ]
    run_stub = SimpleNamespace(
        id="runX", meta={"_physical_run_dir": str(ws_p)}, run_dir=str(ws_p)
    )

    def _fresh(row: ArtifactLocationRow):
        resolver = MountPrecedenceResolver(
            consumer.fs, db=consumer.db, get_run=lambda rid: run_stub
        )
        return resolver.resolve_row_coordinates(row, art)

    expected_ws = _fresh(rows[0])
    expected_arch = _fresh(rows[1])

    shared_ws_first = MountPrecedenceResolver(
        consumer.fs, db=consumer.db, get_run=lambda rid: run_stub
    )
    assert shared_ws_first.resolve_row_coordinates(rows[0], art) == expected_ws
    assert shared_ws_first.resolve_row_coordinates(rows[1], art) == expected_arch

    shared_arch_first = MountPrecedenceResolver(
        consumer.fs, db=consumer.db, get_run=lambda rid: run_stub
    )
    assert shared_arch_first.resolve_row_coordinates(rows[1], art) == expected_arch
    assert shared_arch_first.resolve_row_coordinates(rows[0], art) == expected_ws

    for order in ((rows[0], rows[1]), (rows[1], rows[0])):
        seeded = MountPrecedenceResolver(
            consumer.fs, db=consumer.db, get_run=lambda rid: run_stub
        )
        seeded.seed_run_meta(run_stub)
        for row in order:
            assert seeded.resolve_row_coordinates(row, art) == _fresh(row)

    consumer.close()


def test_resolver_missing_run_meta_negative_lookup_memoized(tmp_path: Path) -> None:
    """A run without metadata is looked up once; later lookups reuse the negative memo."""
    db_path = tmp_path / "prov.duckdb"
    consumer = Tracker(run_dir=tmp_path / "wsC", db_path=str(db_path))
    get_run_calls: list[str] = []

    def _counting_get_run(run_id: str):
        get_run_calls.append(run_id)
        return None

    resolver = MountPrecedenceResolver(
        consumer.fs, db=consumer.db, get_run=_counting_get_run
    )

    assert resolver._lookup_owning_run_mount("ghost", "workspace", "workspace") is None
    assert get_run_calls == ["ghost"]

    assert resolver._lookup_owning_run_mount("ghost", "workspace", "workspace") is None
    assert resolver._lookup_owning_run_mount("ghost", "runs", "workspace") is None
    assert resolver._lookup_owning_run_mount("ghost", "workspace", "archive") is None
    assert get_run_calls == ["ghost"]

    consumer.close()


# ---------------------------------------------------------------------------
# 15. Witness settlement soundness (fresh witnesses are not digest certificates)
# ---------------------------------------------------------------------------


def test_unverified_fresh_witness_does_not_bless_tampered_recovery_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A witness captured without content verification must not certify tampered bytes.

    Regression for the FRESH-witness fast path: input rows register with
    lstat-only witnesses, so a tampered file can carry a FRESH witness that
    never matched the artifact digest. Full digest validation must run and
    reject the tampered source.
    """
    db_path = tmp_path / "prov.duckdb"
    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(db_path))

    f = tracker.run_dir / "io" / "input.csv"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("value\n1\n")

    with tracker.start_run("producer", model="m"):
        art = tracker.log_output(f, key="inp")

    # Tamper, then capture an unverified stat witness matching the tampered
    # bytes (simulating the unverified input-row capture path).
    f.write_text("value\n999\n")
    tampered_row = build_verified_location_row(tracker.fs, art, local_path=f)
    tracker.db.upsert_artifact_locations([tampered_row])

    # The tampered row must actually surface as a candidate.
    cands = list(iter_artifact_candidates(tracker, art))
    assert any(
        c.row is not None and c.row.location_id == tampered_row.location_id
        for c in cands
    )

    import consist.core.materialize as materialize_mod

    staged_items: list = []
    real_bulk = materialize_mod.materialize_artifacts_from_sources

    def _spy_bulk(items: object, **kwargs: object) -> object:
        staged_items.extend(items)  # type: ignore[attr-defined]
        return real_bulk(items, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        materialize_mod, "materialize_artifacts_from_sources", _spy_bulk
    )

    with tracker.start_run(
        "consumer",
        model="m",
        inputs=[art],
        cache_mode="reuse",
        cache_hydration="inputs-missing",
        validate_materialized_inputs=True,
    ):
        pass

    assert staged_items == [], (
        "Unverified FRESH witness must not bless a tampered recovery source"
    )
    tracker.close()


def test_rejected_primary_preserves_resolved_historical_coordinates(
    tmp_path: Path,
) -> None:
    """A rejected workspace primary must not discard resolved historical coords.

    Regression: relocated run dirs whose workspace primary is rejected used to
    lose their owning-run physical-dir recovery path.
    """
    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(tmp_path / "prov.duckdb"))
    resolver = MountPrecedenceResolver.from_tracker(tracker)

    phys_dir = tmp_path / "old_phys_run"
    phys_dir.mkdir(parents=True)

    def _fake_translate(
        mount_name: str,
        mount_instance_id: str,
        uri: str,
        historical_run_dir: str | None = None,
    ) -> TranslatedCoordinate:
        if historical_run_dir is None:
            return TranslatedCoordinate(
                status="rejected",
                mount_name=mount_name,
                uri=uri,
                rejection_reason="primary rejected",
            )
        hist_path = Path(historical_run_dir) / "outputs" / "a.csv"
        return TranslatedCoordinate(
            status="resolved",
            mount_name=mount_name,
            uri=uri,
            lexical_path=hist_path,
            mount_canonical_lexical_path=hist_path,
            canonical_path=hist_path,
        )

    resolver.fs = cast(
        FileSystemManager,
        SimpleNamespace(translate_location_coordinate=_fake_translate),
    )
    resolver._get_run_meta = lambda run_id: {"_physical_run_dir": str(phys_dir)}

    row = ArtifactLocationRow.create(
        digest=None,
        artifact_id=None,
        site="workspace",
        mount_name="workspace",
        mount_instance_id="",
        uri="workspace://outputs/a.csv",
    )
    artifact = Artifact(
        key="a",
        uri="workspace://outputs/a.csv",
        driver="csv",
        digest=None,
        run_id="r1",
    )

    primary, hist = resolver.resolve_row_coordinates(row, artifact)
    assert primary.status == "rejected"
    assert len(hist) == 1
    assert hist[0].is_resolved
    assert hist[0].canonical_path == (phys_dir / "outputs" / "a.csv")
    tracker.close()


# ---------------------------------------------------------------------------
# 16. Targeted regression tests (Critical #1-#3, Important #4-#6, Perf #1-#3)
# ---------------------------------------------------------------------------


def test_child_file_recovery_via_parent_artifact_id_hashed_and_unhashed(
    tmp_path: Path,
) -> None:
    """Critical #1: hashed and digest=None children recover via parent_artifact_id."""
    db_path = tmp_path / "prov.duckdb"
    ws = tmp_path / "ws"
    archive_root = tmp_path / "archive"
    bundle_dir = ws / "outputs" / "bundle"
    bundle_dir.mkdir(parents=True)
    hashed_file = bundle_dir / "hashed.csv"
    unhashed_file = bundle_dir / "unhashed.csv"
    hashed_file.write_text("a\n1\n", encoding="utf-8")
    unhashed_file.write_text("b\n2\n", encoding="utf-8")

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    with tracker.start_run("prod", model="m"):
        parent_art = tracker.log_output(
            bundle_dir, key="bundle", artifact_kind="directory"
        )
        hashed_child = tracker.log_output(
            hashed_file, key="hashed", parent_artifact_id=parent_art.id
        )
        unhashed_child = Artifact(
            key="unhashed",
            uri="workspace://outputs/bundle/unhashed.csv",
            driver="csv",
            digest=None,
            run_id="prod",
            parent_artifact_id=parent_art.id,
        )
        tracker.db.sync_artifact(unhashed_child)

    # Archive only the parent directory; children have no direct recovery rows.
    tracker.archive_artifact(parent_art, archive_root, mode="copy")
    assert not tracker.db.get_artifact_recovery_locations(hashed_child.id)
    assert not tracker.db.get_artifact_recovery_locations(unhashed_child.id)

    # Wipe the workspace copy so recovery must traverse parent_artifact_id.
    shutil.rmtree(bundle_dir)

    resolved_hashed = resolve_verified_local_path(
        tracker, hashed_child, allow_recovery_roots=True
    )
    resolved_unhashed = resolve_verified_local_path(
        tracker, unhashed_child, allow_recovery_roots=True
    )
    assert (
        resolved_hashed
        == (archive_root / "outputs" / "bundle" / "hashed.csv").resolve()
    )
    assert (
        resolved_unhashed
        == (archive_root / "outputs" / "bundle" / "unhashed.csv").resolve()
    )
    tracker.close()


def test_real_db_abs_mount_escaping_symlink_and_dotdot_rejected_in_stage_and_coupler(
    tmp_path: Path,
) -> None:
    """Critical #2: real-DB mount_name='abs' escaping symlink and '..' are rejected."""
    from consist.core.materialize import stage_artifact

    db_path = tmp_path / "prov.duckdb"
    ws = tmp_path / "ws"
    outside = tmp_path / "outside"
    arch = tmp_path / "arch"
    for p in (ws / "outputs", outside, arch / "outputs"):
        p.mkdir(parents=True, exist_ok=True)

    payload = "secret\n1\n"
    (outside / "x.csv").write_text(payload, encoding="utf-8")
    ws_file = ws / "outputs" / "x.csv"
    ws_file.write_text(payload, encoding="utf-8")

    tracker = Tracker(run_dir=ws, db_path=str(db_path), allow_external_paths=True)
    with tracker.scenario("s") as sc:
        sc.coupler.declare_outputs("x")
        res = sc.run(
            fn=lambda: None,
            name="step",
            run_id="r1",
            outputs=["x"],
            output_paths={"x": ws_file},
        )
        art = res.outputs["x"]

    # Delete workspace original and point real-DB abs recovery row to escaping symlink.
    ws_file.unlink()
    (arch / "outputs" / "x.csv").symlink_to(outside / "x.csv")
    tracker.set_artifact_recovery_roots(art, [str(arch)], append=False)
    rows = tracker.db.get_artifact_recovery_locations(art.id)
    assert rows and all(r.mount_name == "abs" for r in rows)

    dest = tmp_path / "dest" / "x.csv"
    stage_sym = stage_artifact(tracker, art, dest)
    assert stage_sym.status != "staged"
    assert not dest.exists()

    # Also test '..' traversal in a real-DB mount_name='abs' row.
    art_dotdot = Artifact(
        key="x_dotdot",
        uri="workspace://outputs/x.csv",
        driver="csv",
        digest=art.digest,
    )
    tracker.db.sync_artifact(art_dotdot)
    dotdot_row = ArtifactLocationRow.create(
        digest=str(art_dotdot.digest),
        artifact_id=art_dotdot.id,
        site="archive",
        mount_name="abs",
        mount_instance_id="",
        uri=f"file://{arch}/outputs/../../outside/x.csv",
    )
    tracker.db.upsert_artifact_locations([dotdot_row])

    stage_dotdot = stage_artifact(tracker, art_dotdot, dest)
    assert stage_dotdot.status != "staged"
    assert not dest.exists()

    # restore_coupler_snapshot must also reject/prune the escaping abs location.
    target_root = tmp_path / "restore_target"
    with tracker.scenario("s_restore") as sc2:
        restored = sc2.restore_coupler_snapshot(
            scenario_run_id="s",
            target_root=target_root,
            prune_unrecoverable=True,
        )
        assert "x" in restored.pruned_keys
        assert not (target_root / "outputs" / "x.csv").exists()
    tracker.close()


def test_select_materialize_artifact_source_scheme_prefixes_does_not_escape_above_mount(
    tmp_path: Path,
) -> None:
    """Critical #3: _select_materialize_artifact_source with scheme_prefixes stays inside mount."""
    from consist.core.materialize import _select_materialize_artifact_source

    parent_dir = tmp_path / "parent"
    mount_root = parent_dir / "scratch_mount"
    mount_root.mkdir(parents=True)
    # Same-named file exists ABOVE the mount root; primary mount file is missing.
    (parent_dir / "data.csv").write_text("above\n1\n", encoding="utf-8")

    tracker = Tracker(
        run_dir=tmp_path / "ws",
        db_path=str(tmp_path / "prov.duckdb"),
        mounts={"scratch": str(mount_root)},
    )
    art = Artifact(
        key="data",
        uri="scratch://data.csv",
        driver="csv",
        digest=None,
    )
    rel = tracker.fs.get_remappable_relative_path(
        art.uri, scheme_prefixes={"scratch": "sub"}
    )
    assert rel == Path("sub/data.csv")

    selected, _, _, _, _ = _select_materialize_artifact_source(
        tracker,
        artifact=art,
        relative_path=rel,
        source_root=None,
        validate_content_hash="never",
    )
    assert selected is None
    tracker.close()


def test_racy_witness_settles_and_fresh_tracker_skips_hashing_and_db_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Important #4 & Perf #3: racy->settled witness skips hashing and DB writes on new Tracker."""
    import consist.core.tracker_recovery as recovery_mod

    db_path = tmp_path / "prov.duckdb"
    ws = tmp_path / "ws"
    ws.mkdir(parents=True)
    f = ws / "io" / "data.csv"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("col\n1\n", encoding="utf-8")

    t1 = Tracker(run_dir=ws, db_path=str(db_path))
    with t1.start_run("prod", model="m"):
        art = t1.log_output(f, key="data")

    rows_initial = t1.db.get_active_artifact_locations(artifact_id=art.id)
    assert rows_initial
    assert rows_initial[0].witness is not None
    assert rows_initial[0].witness.digest_verified is True

    # Reset to unverified witness (digest_verified=False) to exercise verify-once settlement.
    t1.db.upsert_artifact_locations(
        [
            ArtifactLocationRow(
                location_id=rows_initial[0].location_id,
                digest=rows_initial[0].digest,
                artifact_id=rows_initial[0].artifact_id,
                site=rows_initial[0].site,
                mount_name=rows_initial[0].mount_name,
                mount_instance_id=rows_initial[0].mount_instance_id,
                uri=rows_initial[0].uri,
                member=rows_initial[0].member,
                status=rows_initial[0].status,
                witness=rows_initial[0].witness.with_digest_verified(False),
                observed_at_ns=rows_initial[0].observed_at_ns,
            )
        ]
    )

    with t1.start_run(
        "cons1",
        model="c",
        inputs=[art],
        cache_mode="reuse",
        cache_hydration="inputs-missing",
        validate_materialized_inputs=True,
    ):
        resolved_1, saw_mismatch_1, _ = resolve_verified_local_path_detailed(
            t1, art, verify_digest=False
        )
        assert resolved_1 == f.resolve()
        assert not saw_mismatch_1

    rows_settled = t1.db.get_active_artifact_locations(artifact_id=art.id)
    assert rows_settled
    assert rows_settled[0].witness is not None
    assert rows_settled[0].witness.digest_verified is True
    t1.close()

    # Fresh second Tracker on the same db_path: must skip hashing AND issue zero location updates.
    t2 = Tracker(run_dir=ws, db_path=str(db_path))
    hash_calls: list[Path] = []
    upsert_calls: list[object] = []

    def _spy_verify(path: Path, artifact: Artifact) -> bool:
        hash_calls.append(path)
        return True

    def _spy_upsert(rows: object) -> None:
        upsert_calls.append(rows)

    monkeypatch.setattr(recovery_mod, "verify_candidate_against_artifact", _spy_verify)
    monkeypatch.setattr(t2.db, "upsert_artifact_locations", _spy_upsert)

    resolved, saw_mismatch, _ = resolve_verified_local_path_detailed(
        t2, art, verify_digest=False
    )
    assert resolved == f.resolve()
    assert not saw_mismatch
    assert hash_calls == []
    assert upsert_calls == []
    t2.close()


def test_stage_artifact_default_if_present_fails_over_tampered_primary_to_archive_and_historical(
    tmp_path: Path,
) -> None:
    """Important #5 & #6: default validate_content_hash='if-present' fails over past tampered primary."""
    from consist.core.materialize import stage_artifact

    db_path = tmp_path / "prov.duckdb"
    ws = tmp_path / "ws"
    archive_root = tmp_path / "archive"
    t1 = Tracker(run_dir=ws, db_path=str(db_path))

    f = ws / "io" / "input.csv"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("value\n1\n", encoding="utf-8")

    with t1.start_run("prod", model="m"):
        art = t1.log_output(f, key="inp")

    # Archive clean copy, then tamper primary and record an unverified witness on primary.
    t1.archive_run_outputs("prod", archive_root, mode="copy")
    f.write_text("value\n999\n", encoding="utf-8")
    tampered_row = build_verified_location_row(t1.fs, art, local_path=f)
    t1.db.upsert_artifact_locations([tampered_row])

    dest_1 = ws / "stage_1" / "input.csv"
    res_1 = stage_artifact(t1, art, dest_1)
    assert res_1.status == "staged"
    assert dest_1.read_text(encoding="utf-8") == "value\n1\n"

    # Part 2: Step 5a/5b owning-run historical fallback when primary is missing/tampered
    # and no archive row exists.
    hist_ws = tmp_path / "hist_ws"
    hist_file = hist_ws / "io" / "hist.csv"
    hist_file.parent.mkdir(parents=True, exist_ok=True)
    hist_file.write_text("value\nhist\n", encoding="utf-8")

    t1.db.upsert_run(
        Run(id="hist_prod", model_name="m", meta={"_physical_run_dir": str(hist_ws)})
    )
    hist_art = Artifact(
        key="hist",
        uri="workspace://io/hist.csv",
        driver="csv",
        digest=_sha256_file_digest("value\nhist\n"),
        run_id="hist_prod",
    )
    t1.db.sync_artifact(hist_art)

    consumer_ws = tmp_path / "consumer_ws"
    t2 = Tracker(run_dir=consumer_ws, db_path=str(db_path), allow_external_paths=True)
    dest_2 = tmp_path / "stage_2" / "hist.csv"
    res_2 = stage_artifact(t2, hist_art, dest_2)
    assert res_2.status == "staged"
    assert dest_2.read_text(encoding="utf-8") == "value\nhist\n"
    t1.close()
    t2.close()


def test_unrelated_historical_mount_symlink_does_not_poison_archive_run_output_files(
    tmp_path: Path,
) -> None:
    """Important #6 (Archive): symlink in unrelated historical/current mount does not poison clean primary."""
    db_path = tmp_path / "prov.duckdb"
    clean_scratch = tmp_path / "clean_scratch"
    poisoned_scratch = tmp_path / "poisoned_scratch"
    outside = tmp_path / "outside"
    for p in (clean_scratch, poisoned_scratch, outside):
        p.mkdir()

    payload = "clean-archive-payload\n"
    clean_file = clean_scratch / "data.csv"
    clean_file.write_text(payload, encoding="utf-8")
    (outside / "data.csv").write_text(payload, encoding="utf-8")
    (poisoned_scratch / "data.csv").symlink_to(outside / "data.csv")

    t1 = Tracker(
        run_dir=tmp_path / "ws1",
        db_path=str(db_path),
        mounts={"scratch": str(clean_scratch)},
    )
    with t1.start_run("prod", model="m"):
        t1.log_output(clean_file, key="data")
    t1.close()

    # Second tracker has mounts['scratch'] pointing to poisoned_scratch (symlinked twin),
    # while producer's ledger primary points to clean_scratch.
    t2 = Tracker(
        run_dir=tmp_path / "ws2",
        db_path=str(db_path),
        mounts={"scratch": str(poisoned_scratch)},
    )
    archive_root = tmp_path / "archive"
    res = t2.archive_run_output_files("prod", archive_root)
    assert "data" in res.outputs
    assert (archive_root / "data.csv").read_text(encoding="utf-8") == payload
    t2.close()


def test_restore_coupler_snapshot_bulk_write_and_iter_candidates_zero_historical_probes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Perf #1 & #2: bulk recovery write (1 vs N) and zero historical probes on valid primary."""
    db_path = tmp_path / "prov.duckdb"
    ws = tmp_path / "ws"
    ws.mkdir(parents=True)

    tracker = Tracker(run_dir=ws, db_path=str(db_path), allow_external_paths=True)
    keys = [f"out_{i}" for i in range(4)]
    paths = {}
    for k in keys:
        p = ws / "outputs" / f"{k}.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(f"k\n{k}\n", encoding="utf-8")
        paths[k] = p

    with tracker.scenario("s_perf") as sc:
        sc.coupler.declare_outputs(*keys)
        sc.run(
            fn=lambda: None,
            name="multi_out",
            run_id="r_multi",
            outputs=keys,
            output_paths=paths,
        )

    # Perf #1: restore_coupler_snapshot executes 1 bulk recovery write (not N).
    bulk_calls: list[int] = []
    orig_bulk = tracker.db.replace_artifact_recovery_locations_bulk

    def _spy_bulk(locations_by_aid, **kwargs):
        bulk_calls.append(len(locations_by_aid))
        return orig_bulk(locations_by_aid, **kwargs)

    monkeypatch.setattr(
        tracker.db, "replace_artifact_recovery_locations_bulk", _spy_bulk
    )
    target_root = tmp_path / "restore_target"
    with tracker.scenario("s_restore_perf") as sc2:
        sc2.restore_coupler_snapshot(
            scenario_run_id="s_perf",
            target_root=target_root,
        )
    assert len(bulk_calls) == 1
    assert bulk_calls[0] == len(keys)

    # Perf #2: when ledger_primary is valid, consuming the first candidate from
    # iter_candidates executes zero lookup_historical_mount_roots calls.
    hist_calls: list[object] = []

    def _spy_hist(*args: object, **kwargs: object):
        hist_calls.append(args)
        return []

    monkeypatch.setattr(tracker.db, "lookup_historical_mount_roots", _spy_hist)
    loaded = tracker.get_run_outputs("r_multi")["out_0"]
    first_cand = next(tracker.location_resolver.iter_candidates(loaded))
    assert first_cand.source == "ledger_primary"
    assert first_cand.canonical_path.exists()
    assert hist_calls == []
    tracker.close()


def test_remounted_named_mount_tier4b_keeps_winning_row_active(
    tmp_path: Path,
) -> None:
    """Critical #1: Tier 4b historical mount win must keep the row active for subsequent calls."""
    db_path = tmp_path / "prov.duckdb"
    old_scratch = tmp_path / "old_scratch"
    new_scratch = tmp_path / "new_scratch"
    old_scratch.mkdir(parents=True)
    new_scratch.mkdir(parents=True)

    f = old_scratch / "data.csv"
    f.write_text("col\n1\n", encoding="utf-8")

    producer = Tracker(
        run_dir=tmp_path / "prod_ws",
        db_path=str(db_path),
        mounts={"scratch": str(old_scratch)},
    )
    with producer.start_run("prod", model="m"):
        art = producer.log_output(f, key="data")
    producer.close()

    consumer = Tracker(
        run_dir=tmp_path / "cons_ws",
        db_path=str(db_path),
        mounts={"scratch": str(new_scratch)},
        allow_external_paths=True,
    )
    # Tier 4a misses on new_scratch, Tier 4b succeeds on old_scratch.
    resolved_1 = resolve_verified_local_path(consumer, art)
    assert resolved_1 == f.resolve()

    # Winning row must remain active in DuckDB (not marked stale).
    active_rows = consumer.db.get_active_artifact_locations(artifact_id=art.id)
    assert len(active_rows) == 1
    assert active_rows[0].status == "active"

    # A second call (including on a fresh consumer Tracker) must still succeed.
    resolved_2 = resolve_verified_local_path(consumer, art)
    assert resolved_2 == f.resolve()
    consumer.close()

    consumer_2 = Tracker(
        run_dir=tmp_path / "cons_ws_2",
        db_path=str(db_path),
        mounts={"scratch": str(new_scratch)},
        allow_external_paths=True,
    )
    resolved_3 = resolve_verified_local_path(consumer_2, art)
    assert resolved_3 == f.resolve()
    consumer_2.close()


def test_racy_witness_not_laundered_on_upsert_and_in_window_validation_stays_racy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Critical #2: Path A avoids racy->settled laundering; Path B preserves racy=True in 2s window."""
    import consist.location as location_mod
    from consist.core.fs import capture_artifact_witness
    from consist.core.tracker_recovery import verify_and_settle_candidate
    from consist.location import FileStat, compute_location_id

    db_path = tmp_path / "prov.duckdb"
    ws = tmp_path / "ws"
    ws.mkdir(parents=True)
    f = ws / "io" / "data.csv"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text("aaaa\n", encoding="utf-8")

    # Path A: log_output -> digest_verified=True; stat mismatch resets digest_verified=False.
    t1 = Tracker(run_dir=ws, db_path=str(db_path))
    with t1.start_run("prod", model="m"):
        art = t1.log_output(f, key="data")

    rows_initial = t1.db.get_active_artifact_locations(artifact_id=art.id)
    assert len(rows_initial) == 1
    row_0 = rows_initial[0]
    assert row_0.witness is not None
    assert row_0.witness.digest_verified is True

    # Mutate file with different content/size and upsert an unverified witness -> resets digest_verified=False.
    f.write_text("bbbb_tampered\n", encoding="utf-8")
    settled_now_ns = int(f.stat().st_mtime_ns) + 5_000_000_000
    monkeypatch.setattr(location_mod.time, "time_ns", lambda: settled_now_ns)

    incoming_w = capture_artifact_witness(f, art, observed_at_ns=settled_now_ns)
    assert isinstance(incoming_w, FileStat)
    assert incoming_w.digest_verified is False

    digest_str = str(art.digest) if art.digest is not None else None
    incoming_row = ArtifactLocationRow(
        location_id=compute_location_id(
            digest=digest_str,
            artifact_id=art.id,
            site=row_0.site,
            mount_name=row_0.mount_name,
            mount_instance_id=row_0.mount_instance_id,
            uri=row_0.uri,
            member=row_0.member,
        ),
        digest=digest_str,
        artifact_id=art.id,
        site=row_0.site,
        mount_name=row_0.mount_name,
        mount_instance_id=row_0.mount_instance_id,
        uri=row_0.uri,
        member=row_0.member,
        status="active",
        witness=incoming_w,
        observed_at_ns=settled_now_ns,
    )
    t1.db.upsert_artifact_locations([incoming_row])

    rows_after = t1.db.get_active_artifact_locations(artifact_id=art.id)
    assert len(rows_after) == 1
    assert rows_after[0].witness is not None
    assert rows_after[0].witness.digest_verified is False

    # Because digest_verified was reset on stat mismatch, resolution re-verifies and detects tampering.
    resolved, saw_mismatch, _ = resolve_verified_local_path_detailed(
        t1, art, verify_digest=False
    )
    assert resolved is None
    assert saw_mismatch is True

    # Path B: witnessless advisory row validated once settles digest_verified=True.
    f_b = ws / "io" / "advisory.csv"
    f_b.write_text("col\n1\n", encoding="utf-8")
    with t1.start_run("prod_b", model="m2"):
        art_b = t1.log_output(f_b, key="adv")

    row_b_0 = t1.db.get_active_artifact_locations(artifact_id=art_b.id)[0]
    adv_digest_str = str(art_b.digest) if art_b.digest is not None else None
    adv_row = ArtifactLocationRow(
        location_id=compute_location_id(
            digest=adv_digest_str,
            artifact_id=art_b.id,
            site="advisory",
            mount_name="workspace",
            mount_instance_id=row_b_0.mount_instance_id,
            uri="workspace://io/advisory.csv",
            member="",
        ),
        digest=adv_digest_str,
        artifact_id=art_b.id,
        site="advisory",
        mount_name="workspace",
        mount_instance_id=row_b_0.mount_instance_id,
        uri="workspace://io/advisory.csv",
        member="",
        status="active",
        witness=None,
        observed_at_ns=int(f_b.stat().st_mtime_ns),
    )
    t1.db.upsert_artifact_locations([adv_row])

    in_window_ns = int(f_b.stat().st_mtime_ns) + 100_000_000
    is_valid, did_check = verify_and_settle_candidate(
        t1.db, f_b, art_b, row=adv_row, now_ns=in_window_ns
    )
    assert is_valid is True
    assert did_check is True

    settled_cached = t1.db.get_settled_witness(adv_row.location_id)
    assert settled_cached is not None
    assert settled_cached.digest_verified is True

    adv_rows_db = [
        r
        for r in t1.db.get_active_artifact_locations(artifact_id=art_b.id)
        if r.location_id == adv_row.location_id
    ]
    assert len(adv_rows_db) == 1
    assert adv_rows_db[0].witness is not None
    assert adv_rows_db[0].witness.digest_verified is True
    t1.close()


def test_directory_manifest_portable_identity_and_no_double_hash_on_cache_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Important #5: directory_manifest has portable identity and skips double-hashing."""
    from collections.abc import Mapping
    import consist.core.directory_artifacts as dir_mod
    from consist.core.cache import _candidate_matches_cache_policy
    from consist.core.tracker_recovery import _artifact_has_portable_content_identity

    db_path = tmp_path / "prov.duckdb"
    ws = tmp_path / "ws"
    zarr_dir = ws / "bundle.zarr"
    zarr_dir.mkdir(parents=True)
    (zarr_dir / ".zgroup").write_text('{"zarr_format":2}\n', encoding="utf-8")
    (zarr_dir / "chunk.bin").write_bytes(b"0123456789")

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    with tracker.start_run("prod", model="m"):
        art = tracker.log_output(
            zarr_dir, key="bundle", artifact_kind="directory", driver="zarr"
        )

    assert isinstance(art.meta.get("directory_manifest"), Mapping)
    assert _artifact_has_portable_content_identity(art) is True

    rows = tracker.db.get_active_artifact_locations(artifact_id=art.id)
    assert len(rows) == 1
    assert rows[0].witness is not None
    assert rows[0].witness.digest_verified is True

    # Spy on build_directory_manifest: _candidate_matches_cache_policy with
    # hashing_strategy != "full" and a settled/unverified row must NOT double-hash.
    build_calls: list[Path] = []
    orig_build = dir_mod.build_directory_manifest

    def _spy_build(path: Path, *args: object, **kwargs: object):
        build_calls.append(Path(path))
        return orig_build(path, *args, **kwargs)

    monkeypatch.setattr(dir_mod, "build_directory_manifest", _spy_build)

    unverified_row = ArtifactLocationRow(
        location_id=rows[0].location_id,
        digest=rows[0].digest,
        artifact_id=rows[0].artifact_id,
        site=rows[0].site,
        mount_name=rows[0].mount_name,
        mount_instance_id=rows[0].mount_instance_id,
        uri=rows[0].uri,
        member=rows[0].member,
        status="active",
        witness=None,
        observed_at_ns=rows[0].observed_at_ns,
    )
    cand = SimpleNamespace(row=unverified_row)
    tracker.db._settled_witnesses.pop(rows[0].location_id, None)

    ok = _candidate_matches_cache_policy(
        tracker.db,
        art,
        zarr_dir,
        cand=cast(object, cand),  # type: ignore[arg-type]
        validate_content=True,
        hashing_strategy="fast",
    )
    assert ok is True
    assert len(build_calls) <= 1
    tracker.close()


def test_materialize_skips_post_verify_and_prefetch_avoids_per_artifact_queries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Perf #1 & #2: skip post-selection verify_digest and issue 0 per-artifact location queries."""
    import consist.core.materialize as mat_mod

    db_path = tmp_path / "prov.duckdb"
    ws = tmp_path / "ws"
    ws.mkdir(parents=True)

    t1 = Tracker(run_dir=ws, db_path=str(db_path), allow_external_paths=True)
    arts: list[Artifact] = []
    with t1.start_run("prod", model="m"):
        for idx in range(3):
            p = ws / "outputs" / f"item_{idx}.csv"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(f"v\n{idx}\n", encoding="utf-8")
            arts.append(t1.log_output(p, key=f"item_{idx}"))
    t1.close()

    t2 = Tracker(
        run_dir=tmp_path / "cons_ws", db_path=str(db_path), allow_external_paths=True
    )
    # Perf #2: prefetch() populates resolver cache; resolve_verified_local_path issues 0 per-artifact queries.
    per_art_calls: list[object] = []
    orig_get = t2.db.get_active_artifact_locations

    def _spy_get(*args: object, **kwargs: object):
        per_art_calls.append((args, kwargs))
        return orig_get(*args, **kwargs)

    monkeypatch.setattr(t2.db, "get_active_artifact_locations", _spy_get)
    t2.location_resolver.prefetch(arts)
    for art in arts:
        resolved = resolve_verified_local_path(t2, art)
        assert resolved is not None and resolved.exists()
    assert per_art_calls == []

    # Perf #1: materialize_artifact skips post-selection verify_digest when source_already_verified=True.
    post_verify_calls: list[Path] = []

    def _spy_scalar_verify(*args: object, **kwargs: object) -> bool:
        post_verify_calls.append(Path(args[0]))  # type: ignore[arg-type]
        return True

    monkeypatch.setattr(mat_mod, "_verify_scalar_artifact_digest", _spy_scalar_verify)
    target_root = tmp_path / "cons_ws" / "staged"
    res = mat_mod.materialize_artifact(
        t2,
        arts[0],
        target_root=target_root,
        source_root=None,
        allowed_base=tmp_path / "cons_ws",
        preserve_existing=False,
        on_missing="raise",
        validate_content_hash="always",
    )
    assert res.status == "materialized_from_historical_root"
    assert (target_root / "outputs" / "item_0.csv").read_text(
        encoding="utf-8"
    ) == "v\n0\n"
    assert post_verify_calls == []
    t2.close()
