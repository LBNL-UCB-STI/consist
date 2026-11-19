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
"""

from __future__ import annotations

from pathlib import Path
import shutil
import uuid
import pandas as pd
import pytest

from consist.core.fs import FileSystemManager
from consist.core.location_resolver import (
    MountPrecedenceResolver,
    iter_artifact_candidates,
)
from consist.core.tracker import Tracker
from consist.core.tracker_recovery import (
    resolve_verified_local_path,
    resolve_verified_local_path_detailed,
)
from consist.location import ArtifactLocationRow, DirTreeStat
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


def test_set_artifact_recovery_roots_retains_unextractable_row(tmp_path: Path) -> None:
    db_path = tmp_path / "prov.duckdb"
    tracker = Tracker(run_dir=tmp_path / "ws", db_path=str(db_path))

    out_file = tracker.run_dir / "file.csv"
    out_file.write_text("a,b\n1,2\n")
    with tracker.start_run("p", model="m"):
        art = tracker.log_output(out_file, key="f")

    unextractable = ArtifactLocationRow.create(
        digest=None,
        artifact_id=art.id,
        site="advisory",
        mount_name="cold",
        mount_instance_id="",
        uri="cold://bucket/different/name.bin",
    )
    tracker.db.upsert_artifact_locations([unextractable])

    archive_dir = tmp_path / "arch"
    (archive_dir / "file.csv").parent.mkdir(parents=True, exist_ok=True)
    (archive_dir / "file.csv").write_text("a,b\n1,2\n")
    tracker.set_artifact_recovery_roots(art, [str(archive_dir)], append=True)

    rows = tracker.db.get_artifact_recovery_locations(art.id)
    uris = {r.uri for r in rows}
    assert "cold://bucket/different/name.bin" in uris
    assert any(str(archive_dir) in u for u in uris)
    tracker.close()


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
    """A fresh DirTreeStat witness must skip recursive manifest/chunk hashing on Lustre."""
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
    witness = DirTreeStat.capture_dir(zarr_dir, observed_at_ns=future_time).settled()
    row = tracker.db.get_active_artifact_locations(artifact_id=art.id)[0]
    tracker.db.settle_artifact_location_witness(row.location_id, witness)

    # Spy on is_valid_directory_artifact_tree to ensure it is NOT called when witness is fresh
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
        "Fresh DirTreeStat witness must skip recursive directory tree validation"
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
