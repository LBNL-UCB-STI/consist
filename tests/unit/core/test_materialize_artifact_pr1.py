from __future__ import annotations

from pathlib import Path
from typing import Any
import pytest

from consist.core.coupler import Coupler
from consist.core.directory_artifacts import (
    build_directory_manifest,
    build_shapefile_bundle_manifest,
)
from consist.core.fs import FileSystemManager
from consist.core.materialize import materialize_artifact
from consist.core.noop import NoopCoupler
from consist.models.artifact import Artifact


def _create_stub_tracker(
    run_dir: Path,
    mounts: dict[str, str | Path] | None = None,
    runs: dict[str, Any] | None = None,
):
    fs = FileSystemManager(run_dir=run_dir, mounts=mounts or {})

    class StubIdentity:
        hashing_strategy = "full"

    class StubRecoveryService:
        def __init__(self, tracker_inst):
            self._tracker = tracker_inst
            self.run_dir = run_dir
            self.mounts = mounts or {}
            self.allow_external_paths = True

        def materialize_artifact(
            self,
            artifact,
            *,
            target_root,
            source_root=None,
            preserve_existing=True,
            on_missing="warn",
            validate_content_hash="if-present",
            allow_external_paths=None,
            scheme_prefixes=None,
        ):
            return materialize_artifact(
                self._tracker,
                artifact,
                target_root=Path(target_root),
                source_root=Path(source_root) if source_root else None,
                allowed_base=None,
                preserve_existing=preserve_existing,
                on_missing=on_missing,
                validate_content_hash=validate_content_hash,
                scheme_prefixes=scheme_prefixes,
            )

    class StubTracker:
        def __init__(self):
            self.run_dir = run_dir
            self.fs = fs
            self.identity = StubIdentity()
            self._runs = runs or {}
            self._recovery_service = StubRecoveryService(self)

        def get_parent_artifact(self, _artifact):
            return None

        def get_run(self, run_id: str):
            return self._runs.get(run_id)

        def resolve_uri(self, uri: str) -> str:
            return self.fs.resolve_uri(uri)

        def materialize_artifact(
            self,
            artifact,
            *,
            target_root,
            source_root=None,
            preserve_existing=True,
            on_missing="warn",
            validate_content_hash="if-present",
            allow_external_paths=None,
            scheme_prefixes=None,
        ):
            return self._recovery_service.materialize_artifact(
                artifact,
                target_root=target_root,
                source_root=source_root,
                preserve_existing=preserve_existing,
                on_missing=on_missing,
                validate_content_hash=validate_content_hash,
                allow_external_paths=allow_external_paths,
                scheme_prefixes=scheme_prefixes,
            )

    return StubTracker()


# =========================================================================
# 1. Coupler.pop, CouplerView.pop, NoopCoupler.pop
# =========================================================================


def test_coupler_and_view_pop_behavior() -> None:
    coupler = Coupler()
    art_a = Artifact(key="a", container_uri="workspace://a.csv", driver="csv")
    coupler.set("a", art_a)

    # Pop existing returns artifact
    assert coupler.pop("a") == art_a
    assert "a" not in coupler
    assert coupler.get("a") is None

    # Pop missing returns default
    assert coupler.pop("a") is None
    sentinel = Artifact(key="s", container_uri="workspace://s.csv", driver="csv")
    assert coupler.pop("missing", sentinel) == sentinel

    # Key validation
    with pytest.raises(ValueError):
        coupler.pop("")

    # CouplerView pop
    view = coupler.view("sub")
    art_b = Artifact(key="sub/b", container_uri="workspace://b.csv", driver="csv")
    view.set("b", art_b)
    assert "sub/b" in coupler

    popped_b = view.pop("b")
    assert popped_b == art_b
    assert "b" not in view
    assert "sub/b" not in coupler

    assert view.pop("b", None) is None
    assert view.pop("nonexistent", sentinel) == sentinel

    with pytest.raises(ValueError):
        view.pop("")

    # NoopCoupler pop
    noop = NoopCoupler()
    assert noop.pop("anything") is None
    assert noop.pop("anything", sentinel) == sentinel


# =========================================================================
# 2. Directory and Zarr Materialization
# =========================================================================


def test_directory_artifact_materialization_with_manifest(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    dir_path = source_root / "models" / "zarr_data"
    dir_path.mkdir(parents=True)
    (dir_path / "data.bin").write_bytes(b"hello-zarr")
    (dir_path / "meta.json").write_text('{"chunks": 1}', encoding="utf-8")

    manifest = build_directory_manifest(dir_path)
    artifact = Artifact(
        key="zarr_output",
        container_uri="workspace://models/zarr_data",
        driver="zarr",
        hash=manifest["tree_hash"],
        meta={
            "directory_artifact": True,
            "directory_manifest": manifest,
        },
    )

    tracker = _create_stub_tracker(tmp_path / "workspace")

    # Materialize with validation
    result = materialize_artifact(
        tracker,
        artifact,
        target_root=target_root,
        source_root=source_root,
        allowed_base=None,
        preserve_existing=True,
        on_missing="warn",
        validate_content_hash="if-present",
    )

    assert result.status == "materialized_from_source_root"
    assert result.resolvable
    dest = target_root / "models" / "zarr_data"
    assert dest.is_dir()
    assert (dest / "data.bin").read_bytes() == b"hello-zarr"
    assert (dest / "meta.json").read_text(encoding="utf-8") == '{"chunks": 1}'
    assert result.artifact.abs_path == str(dest.resolve())

    # Second run with preserve_existing=True returns already_present
    result_preserved = materialize_artifact(
        tracker,
        artifact,
        target_root=target_root,
        source_root=source_root,
        allowed_base=None,
        preserve_existing=True,
        on_missing="warn",
        validate_content_hash="if-present",
    )
    assert result_preserved.status == "already_present"
    assert result_preserved.resolvable
    assert result_preserved.artifact.abs_path == str(dest.resolve())


def test_directory_artifact_materialization_never_skips_manifest_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    dir_path = source_root / "large_dir"
    dir_path.mkdir(parents=True)
    (dir_path / "large.dat").write_bytes(b"some-bytes")

    artifact = Artifact(
        key="large_dir",
        container_uri="workspace://large_dir",
        driver="directory",
        hash="arbitrary_hash",
        meta={"directory_artifact": True},
    )

    tracker = _create_stub_tracker(tmp_path / "workspace")

    # Spy on build_directory_manifest and file_sha256 to ensure they are NOT called
    called = []

    def fail_on_build(*args, **kwargs):
        called.append("build_directory_manifest")
        raise AssertionError("build_directory_manifest should not be called!")

    def fail_on_sha(*args, **kwargs):
        called.append("file_sha256")
        raise AssertionError("file_sha256 should not be called!")

    monkeypatch.setattr(
        "consist.core.materialize.build_directory_manifest", fail_on_build
    )
    monkeypatch.setattr("consist.core.materialize.file_sha256", fail_on_sha)

    result = materialize_artifact(
        tracker,
        artifact,
        target_root=target_root,
        source_root=source_root,
        allowed_base=None,
        preserve_existing=True,
        on_missing="warn",
        validate_content_hash="never",
    )

    assert result.status == "materialized_from_source_root"
    assert result.resolvable
    assert not called
    dest = target_root / "large_dir"
    assert dest.is_dir()
    assert (dest / "large.dat").read_bytes() == b"some-bytes"

    # With preserve_existing=True and destination existing, also does not call hashing
    result_preserved = materialize_artifact(
        tracker,
        artifact,
        target_root=target_root,
        source_root=source_root,
        allowed_base=None,
        preserve_existing=True,
        on_missing="warn",
        validate_content_hash="never",
    )
    assert result_preserved.status == "already_present"
    assert result_preserved.resolvable
    assert not called


def test_directory_artifact_manifest_mismatch_returns_hash_mismatch(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    dir_path = source_root / "dir_a"
    dir_path.mkdir(parents=True)
    (dir_path / "file.txt").write_text("hello", encoding="utf-8")

    manifest = build_directory_manifest(dir_path)
    # Corrupt the file after building manifest
    (dir_path / "file.txt").write_text("corrupted", encoding="utf-8")

    artifact = Artifact(
        key="dir_a",
        container_uri="workspace://dir_a",
        driver="directory",
        hash=manifest["tree_hash"],
        meta={"directory_artifact": True, "directory_manifest": manifest},
    )

    tracker = _create_stub_tracker(tmp_path / "workspace")

    result = materialize_artifact(
        tracker,
        artifact,
        target_root=target_root,
        source_root=source_root,
        allowed_base=None,
        preserve_existing=True,
        on_missing="warn",
        validate_content_hash="if-present",
    )

    assert result.status == "hash_mismatch"
    assert not result.resolvable


# =========================================================================
# 3. Shapefile Bundle Materialization & Sibling Safety
# =========================================================================


def test_shapefile_bundle_materialization_from_archive_dir(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    target_root = tmp_path / "target"

    # Archived bundle directory layout: outputs/roads.shp/ contains roads.shp, roads.dbf, roads.shx
    bundle_dir = archive_root / "outputs" / "roads.shp"
    bundle_dir.mkdir(parents=True)
    shp_file = bundle_dir / "roads.shp"
    shp_file.write_bytes(b"shp-bytes")
    (bundle_dir / "roads.dbf").write_bytes(b"dbf-bytes")
    (bundle_dir / "roads.shx").write_bytes(b"shx-bytes")
    (bundle_dir / "roads.prj").write_bytes(b"prj-bytes")

    manifest = build_shapefile_bundle_manifest(shp_file)
    artifact = Artifact(
        key="roads",
        container_uri="workspace://outputs/roads.shp",
        driver="shapefile",
        hash=manifest["tree_hash"],
        meta={
            "file_bundle": "shapefile",
            "file_bundle_entry": "roads.shp",
            "file_bundle_manifest": manifest,
            "recovery_roots": [str(archive_root)],
        },
    )

    # Pre-populate target directory with unrelated sibling files
    dest_dir = target_root / "outputs"
    dest_dir.mkdir(parents=True)
    unrelated_file = dest_dir / "unrelated_sibling.csv"
    unrelated_file.write_text("a,b,c\n1,2,3\n", encoding="utf-8")

    tracker = _create_stub_tracker(tmp_path / "workspace")

    result = materialize_artifact(
        tracker,
        artifact,
        target_root=target_root,
        source_root=None,
        allowed_base=None,
        preserve_existing=True,
        on_missing="warn",
        validate_content_hash="if-present",
    )

    assert result.status == "materialized_from_recovery_root"
    assert result.resolvable

    dest_shp = target_root / "outputs" / "roads.shp"
    assert dest_shp.is_file()
    assert dest_shp.read_bytes() == b"shp-bytes"
    assert (target_root / "outputs" / "roads.dbf").read_bytes() == b"dbf-bytes"
    assert (target_root / "outputs" / "roads.shx").read_bytes() == b"shx-bytes"
    assert (target_root / "outputs" / "roads.prj").read_bytes() == b"prj-bytes"

    # Crucial: destination artifact abs_path points to the .shp file
    assert result.artifact.abs_path == str(dest_shp.resolve())
    # Crucial: unrelated sibling file remains untouched
    assert unrelated_file.exists()
    assert unrelated_file.read_text(encoding="utf-8") == "a,b,c\n1,2,3\n"


def test_shapefile_bundle_materialization_from_workspace_file(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"

    # Workspace layout: outputs/roads.shp is a file, sidecars are siblings in outputs/
    source_outputs = source_root / "outputs"
    source_outputs.mkdir(parents=True)
    source_shp = source_outputs / "roads.shp"
    source_shp.write_bytes(b"shp-live-bytes")
    (source_outputs / "roads.dbf").write_bytes(b"dbf-live-bytes")
    (source_outputs / "roads.shx").write_bytes(b"shx-live-bytes")
    (source_outputs / "other_file.parquet").write_bytes(b"parquet-bytes")

    manifest = build_shapefile_bundle_manifest(source_shp)
    artifact = Artifact(
        key="roads",
        container_uri="workspace://outputs/roads.shp",
        driver="shapefile",
        hash=manifest["tree_hash"],
        meta={
            "file_bundle": "shapefile",
            "file_bundle_entry": "roads.shp",
            "file_bundle_manifest": manifest,
        },
    )

    dest_dir = target_root / "outputs"
    dest_dir.mkdir(parents=True)
    (dest_dir / "pre_existing.txt").write_text("hello", encoding="utf-8")

    tracker = _create_stub_tracker(tmp_path / "workspace")

    result = materialize_artifact(
        tracker,
        artifact,
        target_root=target_root,
        source_root=source_root,
        allowed_base=None,
        preserve_existing=True,
        on_missing="warn",
        validate_content_hash="if-present",
    )

    assert result.status == "materialized_from_source_root"
    assert result.resolvable

    dest_shp = target_root / "outputs" / "roads.shp"
    assert dest_shp.is_file()
    assert dest_shp.read_bytes() == b"shp-live-bytes"
    assert (target_root / "outputs" / "roads.dbf").read_bytes() == b"dbf-live-bytes"
    assert (target_root / "outputs" / "roads.shx").read_bytes() == b"shx-live-bytes"
    assert result.artifact.abs_path == str(dest_shp.resolve())
    assert (dest_dir / "pre_existing.txt").read_text(encoding="utf-8") == "hello"

    # Retain existing returns already_present
    result_preserved = materialize_artifact(
        tracker,
        artifact,
        target_root=target_root,
        source_root=source_root,
        allowed_base=None,
        preserve_existing=True,
        on_missing="warn",
        validate_content_hash="if-present",
    )
    assert result_preserved.status == "already_present"
    assert result_preserved.resolvable


# =========================================================================
# 4. Sub-Mount Relative Path Resolution
# =========================================================================


def test_submount_relative_path_with_explicit_scheme_prefixes(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"

    src_file = source_root / "beam" / "input" / "transit" / "schedule.csv"
    src_file.parent.mkdir(parents=True)
    src_file.write_text("trip_id,arrival_time\n1,08:00\n", encoding="utf-8")

    artifact = Artifact(
        key="transit_schedule",
        container_uri="beam_input://transit/schedule.csv",
        driver="csv",
        hash=None,
    )

    tracker = _create_stub_tracker(tmp_path / "workspace")

    # Materialize via tracker.materialize_artifact with scheme_prefixes
    result = tracker.materialize_artifact(
        artifact,
        target_root=target_root,
        source_root=source_root,
        scheme_prefixes={"beam_input": "beam/input"},
    )

    assert result.status == "materialized_from_source_root"
    assert result.resolvable
    dest_file = target_root / "beam" / "input" / "transit" / "schedule.csv"
    assert dest_file.exists()
    assert dest_file.read_text(encoding="utf-8") == "trip_id,arrival_time\n1,08:00\n"
    assert result.artifact.abs_path == str(dest_file.resolve())


def test_submount_relative_path_auto_derived_from_run_mount(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"

    # Mount configured under run_dir: workspace / "beam" / "input"
    mount_dir = workspace / "beam" / "input"
    tracker = _create_stub_tracker(
        run_dir=workspace,
        mounts={"beam_input": mount_dir},
    )

    src_file = source_root / "beam" / "input" / "transit" / "schedule.csv"
    src_file.parent.mkdir(parents=True)
    src_file.write_text("trip_id,arrival_time\n2,09:00\n", encoding="utf-8")

    artifact = Artifact(
        key="transit_schedule",
        container_uri="beam_input://transit/schedule.csv",
        driver="csv",
        hash=None,
    )

    # Materialize without explicit scheme_prefixes -> should auto-derive prefix "beam/input"
    result = tracker.materialize_artifact(
        artifact,
        target_root=target_root,
        source_root=source_root,
    )

    assert result.status == "materialized_from_source_root"
    assert result.resolvable
    dest_file = target_root / "beam" / "input" / "transit" / "schedule.csv"
    assert dest_file.exists()
    assert dest_file.read_text(encoding="utf-8") == "trip_id,arrival_time\n2,09:00\n"


def test_submount_relative_path_auto_derived_from_historical_run_mount(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"

    orig_run_dir = tmp_path / "historical_run"
    orig_mount = orig_run_dir / "beam" / "input"

    class StubRun:
        id = "run-123"
        meta = {
            "_physical_run_dir": str(orig_run_dir),
            "mounts": {"beam_input": str(orig_mount)},
        }

    tracker = _create_stub_tracker(
        run_dir=tmp_path / "fresh_workspace",
        runs={"run-123": StubRun()},
    )

    src_file = source_root / "beam" / "input" / "transit" / "schedule.csv"
    src_file.parent.mkdir(parents=True)
    src_file.write_text("trip_id,arrival_time\n3,10:00\n", encoding="utf-8")

    artifact = Artifact(
        key="transit_schedule",
        run_id="run-123",
        container_uri="beam_input://transit/schedule.csv",
        driver="csv",
        hash=None,
    )

    result = tracker.materialize_artifact(
        artifact,
        target_root=target_root,
        source_root=source_root,
    )

    assert result.status == "materialized_from_source_root"
    assert result.resolvable
    dest_file = target_root / "beam" / "input" / "transit" / "schedule.csv"
    assert dest_file.exists()
    assert dest_file.read_text(encoding="utf-8") == "trip_id,arrival_time\n3,10:00\n"


def test_submount_strict_filtering_when_scheme_prefixes_provided(
    tmp_path: Path,
) -> None:
    target_root = tmp_path / "target"
    artifact = Artifact(
        key="external",
        container_uri="unmapped://data.csv",
        driver="csv",
    )
    tracker = _create_stub_tracker(tmp_path / "workspace")

    # When scheme_prefixes is provided and does not contain "unmapped", it should skip unmapped
    result = tracker.materialize_artifact(
        artifact,
        target_root=target_root,
        scheme_prefixes={"other": "prefix"},
    )
    assert result.status == "skipped_unmapped"
    assert not result.resolvable
