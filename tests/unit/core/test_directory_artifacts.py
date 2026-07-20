from __future__ import annotations

from pathlib import Path
import shutil
from typing import Any, cast
from unittest.mock import ANY

import pytest

import consist
import consist.core.directory_artifacts as directory_artifacts
from consist.core.directory_artifacts import (
    build_directory_manifest,
    build_shapefile_bundle_manifest,
    materialize_directory_tree,
    materialize_shapefile_bundle,
    validate_directory_manifest,
    validate_directory_tree,
    validate_shapefile_bundle_root,
)
from consist.core.tracker import Tracker


def test_directory_manifest_is_stable_and_includes_empty_directories(
    tmp_path: Path,
) -> None:
    root = tmp_path / "raw_od_skims.zarr"
    (root / "skims").mkdir(parents=True)
    (root / "empty").mkdir()
    (root / ".zgroup").write_text('{"zarr_format": 2}\n')
    (root / "skims" / ".zarray").write_text('{"chunks": [1]}\n')
    (root / "skims" / "0.0").write_bytes(b"chunk-bytes")

    manifest = build_directory_manifest(root)

    assert manifest["version"] == 1
    assert manifest["tree_hash"]
    assert manifest["entries"] == [
        {"kind": "file", "path": ".zgroup", "sha256": ANY, "size": 19},
        {"kind": "directory", "path": "empty"},
        {"kind": "directory", "path": "skims"},
        {"kind": "file", "path": "skims/.zarray", "sha256": ANY, "size": 16},
        {"kind": "file", "path": "skims/0.0", "sha256": ANY, "size": 11},
    ]
    assert build_directory_manifest(root) == manifest
    validate_directory_tree(root, manifest)


def _write_shapefile_bundle(shapefile: Path) -> None:
    shapefile.parent.mkdir(parents=True, exist_ok=True)
    shapefile.write_bytes(b"shape geometry")
    shapefile.with_suffix(".shx").write_bytes(b"shape index")
    shapefile.with_suffix(".dbf").write_bytes(b"attribute table")
    shapefile.with_suffix(".prj").write_text('GEOGCS["WGS 84"]')


def test_shapefile_bundle_manifest_is_stable_and_exact(tmp_path: Path) -> None:
    shapefile = tmp_path / "roads.shp"
    _write_shapefile_bundle(shapefile)

    manifest = build_shapefile_bundle_manifest(shapefile)

    assert manifest["version"] == 1
    assert manifest["tree_hash"]
    assert [entry["path"] for entry in manifest["entries"]] == [
        "roads.dbf",
        "roads.prj",
        "roads.shp",
        "roads.shx",
    ]
    assert build_shapefile_bundle_manifest(shapefile) == manifest
    validate_shapefile_bundle_root(shapefile.parent, shapefile.name, manifest)

    (tmp_path / "roads.cpg").write_text("UTF-8")
    with pytest.raises(ValueError, match="unexpected"):
        validate_shapefile_bundle_root(shapefile.parent, shapefile.name, manifest)


def test_materializers_preserve_verified_destinations_without_source_bytes(
    tmp_path: Path,
) -> None:
    """Exact destinations are reusable without consulting unavailable sources."""
    directory_source = tmp_path / "directory_source"
    (directory_source / "nested").mkdir(parents=True)
    (directory_source / "nested" / "0.0").write_bytes(b"skim")
    directory_manifest = build_directory_manifest(directory_source)
    directory_destination = tmp_path / "directory_destination"
    shutil.copytree(directory_source, directory_destination)
    shutil.rmtree(directory_source)

    assert (
        materialize_directory_tree(
            directory_source,
            directory_destination,
            directory_manifest,
            preserve_existing=True,
        )
        is False
    )
    with pytest.raises(ValueError, match="preserve_existing=False"):
        materialize_directory_tree(
            directory_source,
            directory_destination,
            directory_manifest,
            preserve_existing=False,
        )

    bundle_source_root = tmp_path / "bundle_source"
    bundle_source = bundle_source_root / "roads.shp"
    _write_shapefile_bundle(bundle_source)
    bundle_manifest = build_shapefile_bundle_manifest(bundle_source)
    bundle_destination = tmp_path / "bundle_destination"
    shutil.copytree(bundle_source_root, bundle_destination)
    shutil.rmtree(bundle_source_root)

    assert (
        materialize_shapefile_bundle(
            bundle_source_root,
            bundle_destination,
            "roads.shp",
            bundle_manifest,
            preserve_existing=True,
        )
        is False
    )
    with pytest.raises(ValueError, match="preserve_existing=False"):
        materialize_shapefile_bundle(
            bundle_source_root,
            bundle_destination,
            "roads.shp",
            bundle_manifest,
            preserve_existing=False,
        )


@pytest.mark.parametrize("kind", ["directory", "shapefile"])
def test_materializers_clean_staging_when_destination_appears_during_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    """A destination-creation race must retain existing bytes and remove staging."""
    if kind == "directory":
        source = tmp_path / "directory_source"
        source.mkdir()
        (source / "0.0").write_bytes(b"skim")
        manifest = build_directory_manifest(source)
        destination = tmp_path / "directory_destination"

        def materialize() -> bool:
            return materialize_directory_tree(
                source, destination, manifest, preserve_existing=True
            )

        staging_prefix = ".consist-directory-directory_destination-"
    else:
        source = tmp_path / "bundle_source"
        shapefile = source / "roads.shp"
        _write_shapefile_bundle(shapefile)
        manifest = build_shapefile_bundle_manifest(shapefile)
        destination = tmp_path / "bundle_destination"

        def materialize() -> bool:
            return materialize_shapefile_bundle(
                source, destination, "roads.shp", manifest, preserve_existing=True
            )

        staging_prefix = ".consist-shapefile-bundle_destination-"

    def destination_wins_race(_staging: str | Path, target: str | Path) -> None:
        target_path = Path(target)
        target_path.mkdir()
        (target_path / "sentinel").write_text("existing", encoding="utf-8")
        raise FileExistsError("destination appeared during publish")

    monkeypatch.setattr(directory_artifacts.os, "rename", destination_wins_race)

    with pytest.raises(FileExistsError, match="destination appeared"):
        materialize()

    assert (destination / "sentinel").read_text(encoding="utf-8") == "existing"
    assert not list(destination.parent.glob(f"{staging_prefix}*"))


@pytest.mark.parametrize(
    "mutate_manifest",
    [
        lambda manifest: manifest["entries"].append(dict(manifest["entries"][0])),
        lambda manifest: manifest["entries"].__setitem__(
            0, {**manifest["entries"][0], "path": "../escape"}
        ),
        lambda manifest: manifest["entries"].__setitem__(
            0, {**manifest["entries"][0], "sha256": ""}
        ),
    ],
    ids=["duplicate-path", "traversal-path", "missing-file-hash"],
)
def test_directory_manifest_rejects_malformed_persisted_entries(
    tmp_path: Path,
    mutate_manifest,
) -> None:
    """Persisted manifests reject malformed members before filesystem work."""
    root = tmp_path / "raw_od_skims.zarr"
    root.mkdir()
    (root / "0.0").write_bytes(b"skim")
    manifest = build_directory_manifest(root)

    mutate_manifest(manifest)

    with pytest.raises(ValueError):
        validate_directory_manifest(manifest)


def test_log_archive_and_strictly_hydrate_shapefile_bundle(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)

    with tracker.start_run("spatial_completed", model="spatial"):
        logged = tracker.log_output(source, key="roads")

    assert logged.driver == "shapefile"
    assert logged.meta["file_bundle_artifact"] is True
    assert logged.meta["file_bundle_entry"] == "roads.shp"
    assert logged.hash == logged.meta["file_bundle_manifest"]["tree_hash"]

    archive_root = tmp_path / "archive"
    archived = tracker.archive_run_outputs("spatial_completed", archive_root)
    archive_bundle_root = archive_root / "outputs" / "roads.shp"
    assert archived["roads"] == archive_bundle_root
    validate_shapefile_bundle_root(
        archive_bundle_root,
        "roads.shp",
        logged.meta["file_bundle_manifest"],
    )

    for member in source.parent.glob("roads.*"):
        member.unlink()
    destination_root = tracker.run_dir / "restarted" / "roads"
    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "spatial_completed",
        destinations_by_key={"roads": destination_root},
        source_root=archive_root,
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    item = hydrated["roads"]
    assert item.status == "materialized_file_bundle_from_filesystem"
    assert item.artifact_kind == "file_bundle"
    assert item.resolvable
    assert item.path == destination_root.resolve()
    assert item.entry_path == (destination_root / "roads.shp").resolve()
    assert item.artifact.as_path() == item.entry_path


def test_hydrated_shapefile_bundle_loads_through_native_driver(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    geopandas = pytest.importorskip("geopandas")
    source = tracker.run_dir / "outputs" / "places.shp"
    source.parent.mkdir(parents=True)
    original = geopandas.GeoDataFrame(
        {"name": ["one", "two"]},
        geometry=geopandas.points_from_xy([1.0, 2.0], [3.0, 4.0]),
        crs="EPSG:4326",
    )
    original.to_file(source)

    with tracker.start_run("spatial_native", model="spatial"):
        tracker.log_output(source, key="places")
    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("spatial_native", archive_root)
    for member in source.parent.glob("places.*"):
        member.unlink()

    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "spatial_native",
        destinations_by_key={"places": tracker.run_dir / "restored" / "places"},
        source_root=archive_root,
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    item = hydrated["places"]
    assert consist.is_spatial_artifact(item.artifact)
    restored = consist.load(item.artifact, db_fallback="never")
    assert restored["name"].tolist() == ["one", "two"]


def test_shapefile_bundle_archive_rejects_changed_sidecar(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)
    with tracker.start_run("spatial_changed", model="spatial"):
        tracker.log_output(source, key="roads")

    source.with_suffix(".dbf").write_bytes(b"changed attributes")
    archive_root = tmp_path / "archive"

    with pytest.raises(ValueError, match="hash mismatch"):
        tracker.archive_run_outputs("spatial_changed", archive_root)
    assert not (archive_root / "outputs" / "roads.shp").exists()


def test_shapefile_bundle_archive_reuses_verified_bundle_when_live_bytes_are_gone(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)
    with tracker.start_run("spatial_archived", model="spatial"):
        tracker.log_output(source, key="roads")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("spatial_archived", archive_root)
    for member in source.parent.glob("roads.*"):
        member.unlink()

    archived_again = tracker.archive_run_outputs("spatial_archived", archive_root)

    assert archived_again["roads"] == archive_root / "outputs" / "roads.shp"


def test_shapefile_bundle_archive_rolls_back_on_metadata_failure(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed recovery-root update must not publish or move a bundle."""
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)
    with tracker.start_run("spatial_rollback", model="spatial"):
        artifact = tracker.log_output(source, key="roads")

    monkeypatch.setattr(
        tracker.db,
        "update_artifact_meta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    archive_root = tmp_path / "archive"

    with pytest.raises(RuntimeError, match="db down"):
        tracker.archive_artifact(artifact, archive_root, mode="move")

    assert {member.suffix for member in source.parent.glob("roads.*")} == {
        ".dbf",
        ".prj",
        ".shp",
        ".shx",
    }
    assert not (archive_root / "outputs" / "roads.shp").exists()


def test_shapefile_bundle_archive_move_removes_source_and_updates_runtime_path(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    """Moving a bundle removes every source sidecar only after publication."""
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)
    with tracker.start_run("spatial_move", model="spatial"):
        artifact = tracker.log_output(source, key="roads")

    archived = tracker.archive_artifact(artifact, tmp_path / "archive", mode="move")

    assert archived == tmp_path / "archive" / "outputs" / "roads.shp"
    assert not list(source.parent.glob("roads.*"))
    assert artifact.abs_path == str((archived / "roads.shp").resolve())


def test_shapefile_bundle_archive_rejects_unrelated_existing_member(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)
    with tracker.start_run("spatial_archive_extra", model="spatial"):
        tracker.log_output(source, key="roads")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("spatial_archive_extra", archive_root)
    archive_bundle_root = archive_root / "outputs" / "roads.shp"
    (archive_bundle_root / "unrelated.txt").write_text("unsafe extra")
    for member in source.parent.glob("roads.*"):
        member.unlink()

    with pytest.raises(ValueError, match="unexpected"):
        tracker.archive_run_outputs("spatial_archive_extra", archive_root)


def test_shapefile_bundle_hydration_rejects_unrelated_existing_member(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)
    with tracker.start_run("spatial_hydration_extra", model="spatial"):
        tracker.log_output(source, key="roads")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("spatial_hydration_extra", archive_root)
    destination_root = tracker.run_dir / "restored" / "roads"
    shutil.copytree(archive_root / "outputs" / "roads.shp", destination_root)
    (destination_root / "unrelated.txt").write_text("unsafe extra")
    for member in source.parent.glob("roads.*"):
        member.unlink()

    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "spatial_hydration_extra",
        destinations_by_key={"roads": destination_root},
        source_root=tmp_path / "missing_archive",
        preserve_existing=True,
        on_missing="warn",
        db_fallback="never",
    )

    assert hydrated["roads"].status == "failed"
    assert "unexpected" in (hydrated["roads"].message or "")


def test_shapefile_bundle_revalidates_source_before_publication(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)
    with tracker.start_run("spatial_race", model="spatial"):
        tracker.log_output(source, key="roads")

    original_copy2 = directory_artifacts.shutil.copy2

    def mutate_after_copy(
        source_path: Path,
        destination_path: Path,
    ) -> str | Path:
        copied = original_copy2(source_path, destination_path)
        if source_path.name == "roads.shx":
            source.with_suffix(".dbf").write_bytes(b"changed after copy")
        return copied

    monkeypatch.setattr(directory_artifacts.shutil, "copy2", mutate_after_copy)
    archive_root = tmp_path / "archive"

    with pytest.raises(ValueError, match="hash mismatch"):
        tracker.archive_run_outputs("spatial_race", archive_root)
    assert not (archive_root / "outputs" / "roads.shp").exists()


def test_explicit_tracker_load_ignores_artifact_runtime_path(tmp_path: Path) -> None:
    tracker_a = Tracker(run_dir=tmp_path / "workspace_a")
    tracker_b = Tracker(run_dir=tmp_path / "workspace_b")
    source_a = tracker_a.run_dir / "out.csv"
    source_b = tracker_b.run_dir / "out.csv"
    source_a.parent.mkdir(parents=True, exist_ok=True)
    source_b.parent.mkdir(parents=True, exist_ok=True)
    source_a.write_text("value\n1\n")
    source_b.write_text("value\n2\n")

    with tracker_a.start_run("producer", model="test"):
        artifact = tracker_a.log_output(source_a, key="out")

    loaded = consist.load(artifact, tracker=tracker_b, db_fallback="never")

    assert loaded.fetchone() == (2,)


def test_legacy_shapefile_artifact_cannot_be_archived_or_hydrated(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "roads.shp"
    _write_shapefile_bundle(source)
    with tracker.start_run("spatial_legacy", model="spatial"):
        legacy = tracker.log_output(source, key="roads")
        legacy.meta = {}
        tracker._sync_artifact_to_db(legacy, "output")

    with pytest.raises(ValueError, match="legacy Shapefile"):
        tracker.archive_artifact(legacy, tmp_path / "archive")

    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "spatial_legacy",
        destinations_by_key={"roads": tracker.run_dir / "restored" / "roads"},
        source_root=tmp_path / "archive",
        preserve_existing=False,
        on_missing="warn",
        db_fallback="never",
    )
    assert hydrated["roads"].status == "failed"
    assert "legacy Shapefile" in (hydrated["roads"].message or "")


def test_directory_manifest_rejects_symlink(tmp_path: Path) -> None:
    root = tmp_path / "raw_od_skims.zarr"
    root.mkdir()
    external = tmp_path / "outside"
    external.write_text("not part of the artifact\n")
    try:
        (root / "escape").symlink_to(external)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    with pytest.raises(ValueError, match="symlink"):
        build_directory_manifest(root)


def test_directory_manifest_rejects_symlinked_ancestor(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    root = real_parent / "raw_od_skims.zarr"
    root.mkdir(parents=True)
    linked_parent = tmp_path / "linked"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    with pytest.raises(ValueError, match="symlink"):
        build_directory_manifest(linked_parent / root.name)


def test_directory_validation_rejects_extra_or_changed_members(tmp_path: Path) -> None:
    root = tmp_path / "raw_od_skims.zarr"
    root.mkdir()
    member = root / "0.0"
    member.write_bytes(b"original")
    manifest = build_directory_manifest(root)

    member.write_bytes(b"changed")
    with pytest.raises(ValueError, match="hash mismatch"):
        validate_directory_tree(root, manifest)

    member.write_bytes(b"original")
    (root / "extra").write_bytes(b"unexpected")
    with pytest.raises(ValueError, match="unexpected"):
        validate_directory_tree(root, manifest)


def test_directory_manifest_rejects_file_descendant_conflict(tmp_path: Path) -> None:
    root = tmp_path / "raw_od_skims.zarr"
    root.mkdir()
    (root / "a").write_bytes(b"file")
    manifest = build_directory_manifest(root)
    manifest["entries"].append(
        {"kind": "file", "path": "a/child", "sha256": "0" * 64, "size": 0}
    )
    manifest["tree_hash"] = directory_artifacts._manifest_hash(
        {"version": manifest["version"], "entries": manifest["entries"]}
    )

    with pytest.raises(ValueError, match="conflicting"):
        validate_directory_manifest(manifest)


def test_log_archive_and_strictly_hydrate_directory_artifact(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    (source / "nested").mkdir(parents=True)
    (source / "empty").mkdir()
    (source / ".zgroup").write_text("{}\n")
    (source / "nested" / "0.0").write_bytes(b"skim")

    with tracker.start_run("beam_completed", model="beam"):
        logged = tracker.log_output(
            source,
            key="raw_od_skims_zarr_2018_0_sub0",
            artifact_kind="directory",
        )

    assert logged.driver == "zarr"
    assert logged.meta["directory_manifest"]["entries"]
    assert consist.is_zarr_artifact(logged)

    archive_root = tmp_path / "archive"
    archived = tracker.archive_run_outputs("beam_completed", archive_root)
    archive_path = archive_root / "outputs" / "raw_od_skims.zarr"
    assert archived["raw_od_skims_zarr_2018_0_sub0"] == archive_path
    assert build_directory_manifest(archive_path) == logged.meta["directory_manifest"]

    shutil.rmtree(source)
    destination = tracker.run_dir / "restarted" / "raw_od_skims.zarr"
    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "beam_completed",
        destinations_by_key={"raw_od_skims_zarr_2018_0_sub0": destination},
        source_root=archive_root,
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    item = hydrated["raw_od_skims_zarr_2018_0_sub0"]
    assert item.status == "materialized_directory_from_filesystem"
    assert item.artifact_kind == "directory"
    assert item.resolvable
    assert item.path == destination.resolve()
    assert item.artifact.driver == "zarr"
    assert consist.is_zarr_artifact(item.artifact)
    assert build_directory_manifest(destination) == logged.meta["directory_manifest"]


def test_directory_archive_rolls_back_on_metadata_failure(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed recovery-root update must not publish or move a directory."""
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_rollback", model="beam"):
        artifact = tracker.log_output(source, key="skims", artifact_kind="directory")

    monkeypatch.setattr(
        tracker.db,
        "update_artifact_meta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    archive_root = tmp_path / "archive"

    with pytest.raises(RuntimeError, match="db down"):
        tracker.archive_artifact(artifact, archive_root, mode="move")

    assert (source / "0.0").read_bytes() == b"skim"
    assert not (archive_root / "outputs" / "raw_od_skims.zarr").exists()


def test_directory_archive_move_removes_source_and_updates_runtime_path(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    """Moving a directory removes its source tree after successful registration."""
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_move", model="beam"):
        artifact = tracker.log_output(source, key="skims", artifact_kind="directory")

    archived = tracker.archive_artifact(artifact, tmp_path / "archive", mode="move")

    assert archived == tmp_path / "archive" / "outputs" / "raw_od_skims.zarr"
    assert not source.exists()
    assert artifact.abs_path == str(archived.resolve())


def test_top_level_log_output_declares_directory_artifact(
    tracker: Tracker,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")

    with tracker.start_run("beam_api", model="beam"):
        logged = consist.log_output(source, key="skims", artifact_kind="directory")

    assert logged.driver == "zarr"
    assert logged.meta["directory_artifact"] is True
    assert consist.is_zarr_artifact(logged)


def test_direct_zarr_output_logging_is_an_immutable_directory_artifact(
    tracker: Tracker,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")

    with tracker.start_run("beam_direct_zarr", model="beam"):
        logged = tracker.log_artifact(
            source,
            key="skims",
            direction="output",
            driver="zarr",
        )

    assert logged.driver == "zarr"
    assert logged.meta["directory_artifact"] is True
    assert logged.hash == logged.meta["directory_manifest"]["tree_hash"]
    assert consist.is_zarr_artifact(logged)


def test_hydrated_zarr_directory_loads_through_zarr_driver(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    xr = pytest.importorskip("xarray")
    pytest.importorskip("zarr")
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    xr.Dataset({"skim": ("origin", [1.0, 2.0])}).to_zarr(source)

    with tracker.start_run("beam_loadable_zarr", model="beam"):
        tracker.log_output(source, key="skims")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("beam_loadable_zarr", archive_root)
    shutil.rmtree(source)
    destination = tracker.run_dir / "restart" / "raw_od_skims.zarr"
    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "beam_loadable_zarr",
        destinations_by_key={"skims": destination},
        source_root=archive_root,
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )

    loaded = consist.load(hydrated["skims"].artifact)

    assert hydrated["skims"].artifact.driver == "zarr"
    assert loaded["skim"].values.tolist() == [1.0, 2.0]


def test_legacy_zarr_artifact_without_manifest_cannot_be_archived(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_legacy_zarr", model="beam"):
        legacy = tracker.log_artifact(
            source,
            key="skims",
            direction="output",
            driver="zarr",
        )

    legacy.meta = {}
    with pytest.raises(ValueError, match="legacy Zarr"):
        tracker.archive_artifact(legacy, tmp_path / "archive")


def test_legacy_zarr_artifact_without_manifest_cannot_be_hydrated(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_legacy_zarr_hydration", model="beam"):
        legacy = tracker.log_artifact(
            source,
            key="skims",
            direction="output",
            driver="zarr",
        )
        legacy.meta = {}
        tracker._sync_artifact_to_db(legacy, "output")

    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "beam_legacy_zarr_hydration",
        destinations_by_key={"skims": tracker.run_dir / "restart" / "skims.zarr"},
        source_root=tmp_path / "archive",
        preserve_existing=False,
        on_missing="warn",
        db_fallback="never",
    )

    assert hydrated["skims"].status == "failed"
    assert "legacy Zarr" in (hydrated["skims"].message or "")


def test_top_level_log_output_rejects_unknown_artifact_kind(tmp_path: Path) -> None:
    output = tmp_path / "output.csv"
    output.write_text("value\n1\n")

    with pytest.raises(ValueError, match="artifact_kind"):
        consist.log_output(
            output,
            key="output",
            artifact_kind=cast(Any, "unknown"),
        )


def test_file_log_output_keeps_legacy_positional_arguments(tracker: Tracker) -> None:
    output = tracker.run_dir / "outputs" / "legacy.csv"
    output.parent.mkdir(parents=True)
    output.write_text("value\n1\n")

    with tracker.start_run("legacy_output", model="model"):
        logged = tracker.log_output(output, "legacy", None)

    assert logged.key == "legacy"
    assert logged.driver == "csv"


def test_directory_archive_rejects_source_changed_after_logging(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    member = source / "0.0"
    member.write_bytes(b"original")

    with tracker.start_run("beam_changed", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    member.write_bytes(b"changed")
    archive_root = tmp_path / "archive"

    with pytest.raises(ValueError, match="hash mismatch"):
        tracker.archive_run_outputs("beam_changed", archive_root)

    assert not (archive_root / "outputs" / "raw_od_skims.zarr").exists()


def test_directory_archive_rejects_symlinked_source_after_logging(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_source_archive_link", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    moved_source = tmp_path / "moved.zarr"
    source.rename(moved_source)
    try:
        source.symlink_to(moved_source, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    with pytest.raises(ValueError, match="symlink"):
        tracker.archive_run_outputs("beam_source_archive_link", tmp_path / "archive")


def test_directory_archive_rejects_symlinked_archive_root(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_destination_archive_link", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    real_archive = tmp_path / "real_archive"
    real_archive.mkdir()
    archive_link = tmp_path / "archive_link"
    try:
        archive_link.symlink_to(real_archive, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    with pytest.raises(ValueError, match="symlink"):
        tracker.archive_run_outputs("beam_destination_archive_link", archive_link)

    assert not (real_archive / "outputs" / "raw_od_skims.zarr").exists()


def test_directory_hydration_uses_only_strict_source_root_and_clean_destination(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_strict", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("beam_strict", archive_root)
    shutil.rmtree(source)
    destination = tracker.run_dir / "restart" / "raw_od_skims.zarr"
    wrong_root = tmp_path / "wrong_archive"
    wrong_root.mkdir()

    missing = tracker.hydrate_run_outputs_to_destinations(
        "beam_strict",
        destinations_by_key={"skims": destination},
        source_root=wrong_root,
        preserve_existing=False,
        on_missing="warn",
        db_fallback="never",
    )

    assert missing["skims"].status == "missing_source"
    assert not destination.exists()

    destination.mkdir(parents=True)
    rejected = tracker.hydrate_run_outputs_to_destinations(
        "beam_strict",
        destinations_by_key={"skims": destination},
        source_root=archive_root,
        preserve_existing=False,
        on_missing="warn",
        db_fallback="never",
    )

    assert rejected["skims"].status == "failed"
    assert destination.is_dir()


def test_directory_hydration_rejects_symlink_source_root(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_source_link", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("beam_source_link", archive_root)
    source_link = tmp_path / "archive_link"
    try:
        source_link.symlink_to(archive_root, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    destination = tracker.run_dir / "restart" / "raw_od_skims.zarr"
    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "beam_source_link",
        destinations_by_key={"skims": destination},
        source_root=source_link,
        preserve_existing=False,
        on_missing="warn",
        db_fallback="never",
    )

    assert hydrated["skims"].status == "failed"
    assert not destination.exists()


def test_directory_hydration_rejects_dangling_destination_symlink(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_destination_link", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("beam_destination_link", archive_root)
    destination = tracker.run_dir / "restart" / "raw_od_skims.zarr"
    destination.parent.mkdir(parents=True)
    try:
        destination.symlink_to(tracker.run_dir / "elsewhere", target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this environment: {exc}")

    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "beam_destination_link",
        destinations_by_key={"skims": destination},
        source_root=archive_root,
        preserve_existing=False,
        on_missing="warn",
        db_fallback="never",
    )

    assert hydrated["skims"].status == "failed"
    assert destination.is_symlink()


def test_directory_hydration_rejects_incomplete_archive_tree(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_incomplete", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("beam_incomplete", archive_root)
    shutil.rmtree(source)
    (archive_root / "outputs" / "raw_od_skims.zarr" / "0.0").unlink()
    destination = tracker.run_dir / "restart" / "raw_od_skims.zarr"

    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "beam_incomplete",
        destinations_by_key={"skims": destination},
        source_root=archive_root,
        preserve_existing=False,
        on_missing="warn",
        db_fallback="never",
    )

    assert hydrated["skims"].status == "failed"
    assert not destination.exists()


def test_directory_hydration_validates_preserved_destination(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_preserve", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("beam_preserve", archive_root)
    destination = tracker.run_dir / "restart" / "raw_od_skims.zarr"
    initial = tracker.hydrate_run_outputs_to_destinations(
        "beam_preserve",
        destinations_by_key={"skims": destination},
        source_root=archive_root,
        preserve_existing=False,
        on_missing="raise",
        db_fallback="never",
    )
    assert initial["skims"].resolvable

    (destination / "unexpected").write_bytes(b"extra")
    preserved = tracker.hydrate_run_outputs_to_destinations(
        "beam_preserve",
        destinations_by_key={"skims": destination},
        source_root=archive_root,
        preserve_existing=True,
        on_missing="warn",
        db_fallback="never",
    )

    assert preserved["skims"].status == "failed"


@pytest.mark.parametrize(
    ("preserve_existing", "expected_status"),
    [(True, "preserved_existing"), (False, "failed")],
)
def test_directory_hydration_checks_existing_destination_before_source_bytes(
    tracker: Tracker,
    tmp_path: Path,
    preserve_existing: bool,
    expected_status: str,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_existing_before_source", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    destination = tracker.run_dir / "restart" / "raw_od_skims.zarr"
    shutil.copytree(source, destination)
    shutil.rmtree(source)
    unavailable_archive = tmp_path / "unavailable_archive"
    unavailable_archive.mkdir()

    hydrated = tracker.hydrate_run_outputs_to_destinations(
        "beam_existing_before_source",
        destinations_by_key={"skims": destination},
        source_root=unavailable_archive,
        preserve_existing=preserve_existing,
        on_missing="warn",
        db_fallback="never",
    )

    assert hydrated["skims"].status == expected_status


def test_directory_hydration_reopens_snapshot_with_original_archive_only(
    tracker: Tracker,
    tmp_path: Path,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_snapshot", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    archive_root = tmp_path / "archive"
    tracker.archive_run_outputs("beam_snapshot", archive_root)
    snapshot = tracker.snapshot_db(tmp_path / "snapshot" / "provenance.duckdb")
    shutil.rmtree(source)
    reopened = Tracker(run_dir=tracker.run_dir, db_path=snapshot)
    destination = tracker.run_dir / "restart" / "raw_od_skims.zarr"
    try:
        hydrated = reopened.hydrate_run_outputs_to_destinations(
            "beam_snapshot",
            destinations_by_key={"skims": destination},
            source_root=archive_root,
            preserve_existing=False,
            on_missing="raise",
            db_fallback="never",
        )
    finally:
        if reopened.engine is not None:
            reopened.engine.dispose()

    assert hydrated["skims"].status == "materialized_directory_from_filesystem"
    assert destination.is_dir()


def test_interrupted_directory_archive_never_publishes_partial_tree(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"first")
    (source / "0.1").write_bytes(b"second")
    with tracker.start_run("beam_interrupted", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    original_copy2 = directory_artifacts.shutil.copy2
    calls = 0

    def interrupt_after_first_copy(source_path, target_path, *args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated copy interruption")
        return original_copy2(source_path, target_path, *args, **kwargs)

    monkeypatch.setattr(directory_artifacts.shutil, "copy2", interrupt_after_first_copy)
    archive_root = tmp_path / "archive"

    with pytest.raises(OSError, match="simulated copy interruption"):
        tracker.archive_run_outputs("beam_interrupted", archive_root)

    assert not (archive_root / "outputs" / "raw_od_skims.zarr").exists()
    assert not list((archive_root / "outputs").glob(".consist-directory-*"))
    output = tracker.get_run_outputs("beam_interrupted")["skims"]
    assert str(archive_root.resolve()) not in output.recovery_roots


def test_directory_archive_rechecks_source_before_publication(
    tracker: Tracker,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tracker.run_dir / "outputs" / "raw_od_skims.zarr"
    source.mkdir(parents=True)
    (source / "0.0").write_bytes(b"skim")
    with tracker.start_run("beam_race", model="beam"):
        tracker.log_output(source, key="skims", artifact_kind="directory")

    original_copy2 = directory_artifacts.shutil.copy2

    def add_member_during_copy(source_path, target_path, *args, **kwargs):
        result = original_copy2(source_path, target_path, *args, **kwargs)
        (source / "late_member").write_bytes(b"late")
        return result

    monkeypatch.setattr(directory_artifacts.shutil, "copy2", add_member_during_copy)
    archive_root = tmp_path / "archive"

    with pytest.raises(ValueError, match="unexpected"):
        tracker.archive_run_outputs("beam_race", archive_root)

    assert not (archive_root / "outputs" / "raw_od_skims.zarr").exists()
