"""Checked raw-path translation for ledger location coordinates.

Covers the checked coordinate translation seam (traversal rejection,
containment, raw vs default return shapes), recovery-root extraction from
location rows (lexical suffix extraction, layout inversion, unextractable
rows), ``Tracker.artifact_recovery_roots`` historical mount translation
regardless of ``resolve``, the path-only filesystem-probe boundary in
``materialize._artifact_recovery_roots``, and ledger writeback retention of
unextractable rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast
import uuid

import pytest

from consist.core.fs import FileSystemManager
from consist.core.location_resolver import iter_artifact_candidates
from consist.core.tracker import Tracker
from consist.location import (
    ArtifactLocationRow,
    LocationSite,
    TranslatedCoordinate,
    workspace_mount_instance_id,
)
from consist.models.artifact import Artifact
from consist.models.run import Run

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fs(
    tmp_path: Path,
    *,
    mounts: dict[str, str] | None = None,
    run_dir: Path | None = None,
) -> FileSystemManager:
    return FileSystemManager(
        run_dir=run_dir if run_dir is not None else tmp_path / "ws",
        mounts=mounts or {},
    )


def _row(
    artifact_id: uuid.UUID | None,
    *,
    mount_name: str,
    uri: str,
    mount_instance_id: str = "",
    site: str = "archive",
) -> ArtifactLocationRow:
    return ArtifactLocationRow.create(
        digest=None,
        artifact_id=artifact_id,
        site=cast(LocationSite, site),
        mount_name=mount_name,
        mount_instance_id=mount_instance_id,
        uri=uri,
    )


def _produce_directory_artifact(
    tmp_path: Path,
    db_path: Path,
    *,
    rel: str = "beam/beam_output/skims.zarr",
    mounts: dict[str, str] | None = None,
) -> Artifact:
    """Create a real directory artifact via a producer tracker, then close it.

    The producer run records ``meta["mounts"]`` for any supplied mounts, which
    is how historical mount roots become visible to later sessions.
    """
    producer_ws = tmp_path / "producer_ws"
    artifact_dir = producer_ws.joinpath(*rel.split("/"))
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "0.0").write_bytes(b"z")
    producer = Tracker(
        run_dir=producer_ws,
        db_path=str(db_path),
        mounts=dict(mounts) if mounts else None,
    )
    try:
        with producer.start_run("produce", model="beam"):
            art = producer.log_output(
                artifact_dir, key="skims", artifact_kind="directory"
            )
    finally:
        producer.close()
    return art


def _consumer(tmp_path: Path, db_path: Path) -> Tracker:
    """Fresh writable tracker sharing the database but with no mounts."""
    return Tracker(run_dir=tmp_path / "consumer_ws", db_path=str(db_path))


def _upsert_mounted_run(
    tracker: Tracker,
    run_id: str,
    *,
    scratch_root: Path,
    created_at: datetime,
) -> None:
    tracker.db.upsert_run(
        Run(
            id=run_id,
            model_name="beam",
            meta={"mounts": {"scratch": str(scratch_root)}},
            created_at=created_at,
            updated_at=created_at,
        )
    )


def _seed_step_scoped_row(
    tracker: Tracker, artifact_id: uuid.UUID
) -> ArtifactLocationRow:
    row = _row(
        artifact_id,
        mount_name="scratch",
        uri="scratch://run-x/consist-recovery/step_a/beam/beam_output/skims.zarr",
    )
    tracker.db.upsert_artifact_locations([row])
    return row


# ---------------------------------------------------------------------------
# Checked translation: default vs raw shapes, containment
# ---------------------------------------------------------------------------


def test_default_mode_preserves_parent_canonicalized_leaf_shape(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    (mount / "renamed_dir" / "dir").mkdir(parents=True)
    (mount / "sub").symlink_to(mount / "renamed_dir", target_is_directory=True)
    (mount / "sub" / "dir" / "leaf.parquet").write_bytes(b"x")

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://sub/dir/leaf.parquet"
    )
    assert isinstance(coord, TranslatedCoordinate)
    assert coord.status == "resolved"
    assert coord.is_resolved
    assert coord.canonical_path is not None
    # Parent canonicalized through the symlink, leaf preserved.
    assert coord.canonical_path.name == "leaf.parquet"
    assert "renamed_dir" in coord.canonical_path.parts
    assert "sub" not in coord.canonical_path.parts
    assert coord.canonical_path.exists()
    assert (
        fs.resolve_location_coordinate("scratch", "", "scratch://sub/dir/leaf.parquet")
        == coord.canonical_path
    )


def test_raw_mode_retains_entire_lexical_path(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    (mount / "renamed_dir" / "dir").mkdir(parents=True)
    (mount / "sub").symlink_to(mount / "renamed_dir", target_is_directory=True)
    (mount / "sub" / "dir" / "leaf.parquet").write_bytes(b"x")

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://sub/dir/leaf.parquet"
    )
    assert coord.is_resolved
    assert coord.lexical_path is not None
    # Every component stays visible: mount root, symlinked prefix, leaf.
    assert coord.lexical_path == mount / "sub" / "dir" / "leaf.parquet"
    assert (mount / "sub").is_symlink()


def test_contained_leaf_symlink_visible_in_default_mode(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    (mount / "sub").mkdir(parents=True)
    (mount / "sub" / "target.parquet").write_bytes(b"x")
    (mount / "sub" / "leaf.parquet").symlink_to(mount / "sub" / "target.parquet")

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://sub/leaf.parquet"
    )
    assert coord.is_resolved
    assert coord.canonical_path is not None
    # Contained leaf symlink stays visible for caller-side symlink policies.
    assert coord.canonical_path.name == "leaf.parquet"
    assert coord.canonical_path.is_symlink()
    assert (
        fs.resolve_location_coordinate("scratch", "", "scratch://sub/leaf.parquet")
        == coord.canonical_path
    )


def test_escaping_leaf_symlink_rejected_in_both_modes(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.parquet").write_bytes(b"x")
    mount = tmp_path / "mount"
    (mount / "sub").mkdir(parents=True)
    (mount / "sub" / "leaf.parquet").symlink_to(outside / "secret.parquet")

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    assert (
        fs.resolve_location_coordinate("scratch", "", "scratch://sub/leaf.parquet")
        is None
    )
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://sub/leaf.parquet"
    )
    assert coord.status == "rejected"
    assert coord.is_rejected
    assert coord.rejection_reason is not None


def test_escaping_ancestor_symlink_rejected_in_both_modes(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    (outside / "dir").mkdir(parents=True)
    (outside / "dir" / "art.parquet").write_bytes(b"x")
    mount = tmp_path / "mount"
    mount.mkdir()
    (mount / "link").symlink_to(outside / "dir", target_is_directory=True)

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    assert (
        fs.resolve_location_coordinate("scratch", "", "scratch://link/art.parquet")
        is None
    )
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://link/art.parquet"
    )
    assert coord.status == "rejected"
    assert coord.is_rejected
    assert coord.rejection_reason is not None


def test_intermediate_and_terminal_traversal_rejected(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    (mount / "sub").mkdir(parents=True)
    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})

    # Terminal ".." previously passed the parent-only containment check.
    assert fs.resolve_location_coordinate("scratch", "", "scratch://..") is None
    assert (
        fs.resolve_location_coordinate("scratch", "", "scratch://sub/../escape") is None
    )
    for uri in ("scratch://..", "scratch://sub/../escape"):
        coord = fs.translate_location_coordinate("scratch", "", uri)
        assert coord.status == "rejected"
        assert coord.is_rejected
        assert coord.rejection_reason is not None


def test_traversal_rejected_through_historical_mount(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    mount.mkdir()
    fs = _make_fs(tmp_path, mounts={})
    assert (
        fs.resolve_location_coordinate(
            "scratch", "", "scratch://..", historical_run_dir=mount
        )
        is None
    )
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://..", historical_run_dir=mount
    )
    assert coord.status == "rejected"
    assert coord.is_rejected


def test_dangling_symlink_rejected(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    (mount / "sub").mkdir(parents=True)
    (mount / "sub" / "dangling.parquet").symlink_to(mount / "sub" / "missing.parquet")

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    assert (
        fs.resolve_location_coordinate("scratch", "", "scratch://sub/dangling.parquet")
        is None
    )
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://sub/dangling.parquet"
    )
    assert coord.status == "rejected"
    assert coord.is_rejected


def test_symlink_loop_rejected(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    mount.mkdir()
    (mount / "loop").symlink_to(mount / "loop")

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    assert fs.resolve_location_coordinate("scratch", "", "scratch://loop/x") is None
    coord = fs.translate_location_coordinate("scratch", "", "scratch://loop/x")
    assert coord.status == "rejected"
    assert coord.is_rejected


def test_missing_candidate_is_translated_for_probing(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    (mount / "sub").mkdir(parents=True)

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    # The artifact bytes do not exist yet; translation must not require them.
    coord = fs.translate_location_coordinate(
        "scratch", "", "scratch://sub/missing.parquet"
    )
    assert coord.is_resolved
    assert coord.canonical_path == (mount / "sub" / "missing.parquet").resolve()
    assert not coord.canonical_path.exists()
    assert coord.lexical_path == mount / "sub" / "missing.parquet"
    assert not coord.lexical_path.exists()
    assert (
        fs.resolve_location_coordinate("scratch", "", "scratch://sub/missing.parquet")
        == coord.canonical_path
    )


def test_mount_root_symlink_canonical_containment_lexical_visibility(
    tmp_path: Path,
) -> None:
    real = tmp_path / "real_mount"
    (real / "sub").mkdir(parents=True)
    (real / "sub" / "art.parquet").write_bytes(b"x")
    mount_link = tmp_path / "mount_link"
    mount_link.symlink_to(real, target_is_directory=True)

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount_link)})
    coord = fs.translate_location_coordinate("scratch", "", "scratch://sub/art.parquet")
    assert coord.is_resolved
    assert coord.canonical_path is not None and coord.canonical_path.exists()
    # Default mode canonicalizes through the mount-root symlink.
    assert "real_mount" in coord.canonical_path.parts
    # Lexical path keeps the mount-root symlink visible.
    assert coord.lexical_path == mount_link / "sub" / "art.parquet"
    # Mount canonical lexical path resolves mount root but keeps in-mount lexical.
    assert coord.mount_canonical_lexical_path == real / "sub" / "art.parquet"


def test_workspace_runs_and_abs_cases_preserved(tmp_path: Path) -> None:
    run_dir = tmp_path / "ws"
    (run_dir / "a").mkdir(parents=True)
    (run_dir / "a" / "b.parquet").write_bytes(b"x")

    fs = _make_fs(tmp_path, run_dir=run_dir)
    inst = fs.workspace_instance_id

    coord = fs.translate_location_coordinate(
        "workspace", inst, "workspace://a/b.parquet"
    )
    assert coord.is_resolved
    assert coord.canonical_path == (run_dir / "a" / "b.parquet").resolve()

    # Foreign workspace instance without a historical dir: unavailable.
    foreign_coord = fs.translate_location_coordinate(
        "workspace", "other-instance", "workspace://a/b.parquet"
    )
    assert foreign_coord.status == "unmounted"
    assert foreign_coord.is_unmounted
    assert foreign_coord.unmounted_uri == "workspace://a/b.parquet"
    assert (
        fs.resolve_location_coordinate(
            "workspace", "other-instance", "workspace://a/b.parquet"
        )
        is None
    )

    # Matching historical run dir: translated.
    hist = tmp_path / "old_ws"
    (hist / "a").mkdir(parents=True)
    (hist / "a" / "b.parquet").write_bytes(b"x")
    hist_inst = workspace_mount_instance_id(hist)
    coord_hist = fs.translate_location_coordinate(
        "workspace", hist_inst, "workspace://a/b.parquet", historical_run_dir=hist
    )
    assert coord_hist.is_resolved
    assert coord_hist.canonical_path == (hist / "a" / "b.parquet").resolve()

    # abs case returns the supplied path unchanged.
    coord_abs = fs.translate_location_coordinate(
        "abs", "", f"file://{tmp_path}/abs/x.parquet"
    )
    assert coord_abs.is_resolved
    assert coord_abs.lexical_path == Path(f"{tmp_path}/abs/x.parquet")
    assert coord_abs.canonical_path == Path(f"{tmp_path}/abs/x.parquet")
    assert coord_abs.mount_canonical_lexical_path == Path(f"{tmp_path}/abs/x.parquet")

    # Traversal is rejected in the workspace branch too.
    coord_ws_traversal = fs.translate_location_coordinate(
        "workspace", inst, "workspace://../escape"
    )
    assert coord_ws_traversal.status == "rejected"
    assert coord_ws_traversal.is_rejected


# ---------------------------------------------------------------------------
# Recovery-root extraction from location rows
# ---------------------------------------------------------------------------


def _step_scoped_row(
    artifact_id: uuid.UUID | None, *, step: str = "step_a"
) -> ArtifactLocationRow:
    return _row(
        artifact_id,
        mount_name="scratch",
        uri=(f"scratch://run-x/consist-recovery/{step}/beam/beam_output/skims.zarr"),
    )


def test_extraction_lexical_suffix_and_layout_inversion(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    artifact_dir = (
        mount
        / "run-x"
        / "consist-recovery"
        / "step_a"
        / "beam"
        / "beam_output"
        / "skims.zarr"
    )
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "0.0").write_bytes(b"z")

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    row = _step_scoped_row(None)
    rel = Path("beam/beam_output/skims.zarr")

    raw_root = fs.extract_recovery_root_from_location_row(
        "workspace://beam/beam_output/skims.zarr", row, resolve=False
    )
    assert raw_root == str(mount / "run-x" / "consist-recovery" / "step_a")
    # Layout inversion: lexical root / artifact rel == translated lexical coordinate.
    assert Path(raw_root) / rel == artifact_dir

    resolved_root = fs.extract_recovery_root_from_location_row(
        "workspace://beam/beam_output/skims.zarr", row, resolve=True
    )
    assert resolved_root == str(
        (mount / "run-x" / "consist-recovery" / "step_a").resolve()
    )


def test_extraction_survives_renamed_symlink_ancestor(tmp_path: Path) -> None:
    mount = tmp_path / "mount"
    step = mount / "run-x" / "consist-recovery" / "step_a"
    real_output = step / "beam_output_renamed"
    artifact_dir = real_output / "skims.zarr"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "0.0").write_bytes(b"z")
    # The archive tree exposes the output dir under its original name via a
    # symlink whose target has a different component name.
    (step / "beam").mkdir()
    (step / "beam" / "beam_output").symlink_to(real_output, target_is_directory=True)

    fs = _make_fs(tmp_path, mounts={"scratch": str(mount)})
    row = _step_scoped_row(None)

    raw_root = fs.extract_recovery_root_from_location_row(
        "workspace://beam/beam_output/skims.zarr", row, resolve=False
    )
    assert raw_root == str(mount / "run-x" / "consist-recovery" / "step_a")
    # Layout inversion holds on the lexical layout even through the symlink.
    assert (Path(raw_root) / "beam/beam_output/skims.zarr").exists()


def test_extraction_historical_mount_argument(tmp_path: Path) -> None:
    historical = tmp_path / "historical_scratch"
    artifact_dir = (
        historical
        / "run-x"
        / "consist-recovery"
        / "step_a"
        / "beam"
        / "beam_output"
        / "skims.zarr"
    )
    artifact_dir.mkdir(parents=True)

    # No current scratch mount: unavailable in this environment.
    fs = _make_fs(tmp_path, mounts={})
    row = _step_scoped_row(None)
    assert (
        fs.extract_recovery_root_from_location_row(
            "workspace://beam/beam_output/skims.zarr", row
        )
        == "scratch://run-x/consist-recovery/step_a"
    )

    # With the historical mount root: translated to a real path.
    extracted = fs.extract_recovery_root_from_location_row(
        "workspace://beam/beam_output/skims.zarr",
        row,
        historical_run_dir=historical,
    )
    assert extracted == str(
        (historical / "run-x" / "consist-recovery" / "step_a").resolve()
    )


def test_extraction_unextractable_rows_return_none(tmp_path: Path) -> None:
    fs = _make_fs(tmp_path, mounts={})

    # Row whose URI does not end with the artifact-relative layout.
    row = _row(None, mount_name="cold", uri="cold://entirely/different/file.parquet")
    assert (
        fs.extract_recovery_root_from_location_row(
            "workspace://beam/out/skims.zarr", row
        )
        is None
    )

    # Traversal in the row URI must not be reintroduced as an unresolved URI.
    traversal_row = _row(None, mount_name="cold", uri="cold://../escape/out/skims.zarr")
    assert (
        fs.extract_recovery_root_from_location_row(
            "workspace://out/skims.zarr", traversal_row
        )
        is None
    )

    # A rejected (unsafe) coordinate must not fall back to the unresolved URI
    # either: an escaping leaf symlink under a current mount is rejected.
    escaping_leaf_row = _row(
        None, mount_name="scratch", uri="scratch://sub/leaf.parquet"
    )
    escape_fs = _make_fs(tmp_path, mounts={"scratch": str(tmp_path / "mount2")})
    (tmp_path / "mount2" / "sub").mkdir(parents=True)
    (tmp_path / "mount2" / "sub" / "leaf.parquet").symlink_to(
        tmp_path / "outside" / "secret.parquet"
    )
    assert (
        escape_fs.extract_recovery_root_from_location_row(
            "workspace://sub/leaf.parquet", escaping_leaf_row
        )
        is None
    )

    # URI fallback still applies to unavailable, safe, suffix-matched rows.
    good_row = _row(None, mount_name="cold", uri="cold://bucket/prefix/out/skims.zarr")
    assert (
        fs.extract_recovery_root_from_location_row(
            "workspace://out/skims.zarr", good_row
        )
        == "cold://bucket/prefix"
    )


# ---------------------------------------------------------------------------
# Tracker.artifact_recovery_roots: historical translation regardless of resolve
# ---------------------------------------------------------------------------


def test_recovery_roots_translate_through_historical_mounts_regardless_of_resolve(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    scratch = tmp_path / "shared_scratch"
    step_root = scratch / "run-x" / "consist-recovery" / "step_a"
    artifact_dir = step_root / "beam" / "beam_output" / "skims.zarr"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "0.0").write_bytes(b"z")

    # The producer run records the scratch mount in its run meta.
    art = _produce_directory_artifact(
        tmp_path, db_path, mounts={"scratch": str(scratch)}
    )

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        # The workspace-relative layout survives the DB round-trip (with or
        # without the workspace scheme prefix).
        assert loaded.uri.endswith("beam/beam_output/skims.zarr")
        _seed_step_scoped_row(tracker, loaded.id)

        raw_roots = tracker.artifact_recovery_roots(loaded, resolve=False)
        assert raw_roots == [str(scratch / "run-x" / "consist-recovery" / "step_a")]
        assert "://" not in raw_roots[0]

        resolved_roots = tracker.artifact_recovery_roots(loaded, resolve=True)
        assert resolved_roots == [str(step_root.resolve())]
    finally:
        tracker.close()


def test_recovery_roots_first_existing_prefix_selection_with_symlink_mount(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    real_scratch = tmp_path / "real_scratch"
    scratch_link = tmp_path / "linked_scratch"
    scratch_link.symlink_to(real_scratch, target_is_directory=True)
    step_root = real_scratch / "run-x" / "consist-recovery" / "step_a"
    artifact_dir = step_root / "beam" / "beam_output" / "skims.zarr"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "0.0").write_bytes(b"z")

    art = _produce_directory_artifact(
        tmp_path, db_path, mounts={"scratch": str(scratch_link)}
    )

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        _seed_step_scoped_row(tracker, loaded.id)

        # resolve=False keeps the lexical (symlinked) mount path visible.
        raw_roots = tracker.artifact_recovery_roots(loaded, resolve=False)
        assert raw_roots == [
            str(scratch_link / "run-x" / "consist-recovery" / "step_a")
        ]
        # resolve=True canonicalizes the extracted root.
        resolved_roots = tracker.artifact_recovery_roots(loaded, resolve=True)
        assert resolved_roots == [str(step_root.resolve())]
    finally:
        tracker.close()


def test_recovery_roots_rejected_translation_does_not_hide_later_root(
    tmp_path: Path,
) -> None:
    """An escaping symlink under the first historical root must not hide the second."""
    db_path = tmp_path / "prov.duckdb"
    bad_scratch = tmp_path / "bad_scratch"
    good_scratch = tmp_path / "good_scratch"
    outside = tmp_path / "outside"
    outside.mkdir()
    # First historical root: the step prefix component is a symlink escaping
    # the mount, so translation is rejected there.
    bad_step = bad_scratch / "run-x" / "consist-recovery" / "step_a"
    bad_step.parent.mkdir(parents=True)
    bad_step.symlink_to(outside, target_is_directory=True)
    # Second historical root: real bytes.
    artifact_dir = (
        good_scratch
        / "run-x"
        / "consist-recovery"
        / "step_a"
        / "beam"
        / "beam_output"
        / "skims.zarr"
    )
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "0.0").write_bytes(b"z")

    art = _produce_directory_artifact(tmp_path, db_path)

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        now = datetime.now(timezone.utc)
        # The bad root's run is newer, so it is tried first.
        _upsert_mounted_run(
            tracker, "run-good", scratch_root=good_scratch, created_at=now
        )
        _upsert_mounted_run(
            tracker,
            "run-bad",
            scratch_root=bad_scratch,
            created_at=now + timedelta(seconds=1),
        )
        _seed_step_scoped_row(tracker, loaded.id)

        roots = tracker.artifact_recovery_roots(loaded, resolve=False)
        assert roots == [str(good_scratch / "run-x" / "consist-recovery" / "step_a")]
    finally:
        tracker.close()


def test_recovery_roots_all_rejected_historical_roots_do_not_leak_uri(
    tmp_path: Path,
) -> None:
    """When every historical mount rejects a coordinate, do not return the raw URI."""
    db_path = tmp_path / "prov.duckdb"
    bad_scratch = tmp_path / "bad_scratch"
    outside = tmp_path / "outside"
    outside.mkdir()
    bad_step = bad_scratch / "run-x" / "consist-recovery" / "step_a"
    bad_step.parent.mkdir(parents=True)
    bad_step.symlink_to(outside, target_is_directory=True)

    art = _produce_directory_artifact(tmp_path, db_path)

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        _upsert_mounted_run(
            tracker,
            "run-bad-only",
            scratch_root=bad_scratch,
            created_at=datetime.now(timezone.utc),
        )
        _seed_step_scoped_row(tracker, loaded.id)

        assert tracker.artifact_recovery_roots(loaded, resolve=False) == []
        assert tracker.artifact_recovery_roots(loaded, resolve=True) == []
    finally:
        tracker.close()


def test_recovery_roots_keep_untranslatable_uri_for_uri_aware_consumers(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "prov.duckdb"
    art = _produce_directory_artifact(tmp_path, db_path, rel="out/skims.zarr")

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        tracker.db.upsert_artifact_locations(
            [
                _row(
                    loaded.id,
                    mount_name="cold",
                    uri="cold://bucket/prefix/out/skims.zarr",
                )
            ]
        )
        # No cold mount and no historical roots: the URI stays available to
        # URI-aware consumers.
        assert tracker.artifact_recovery_roots(loaded) == ["cold://bucket/prefix"]
        # But the filesystem-probe boundary drops it.
        rec_rows = tracker.db.get_artifact_recovery_locations(loaded.id)
        assert list(iter_artifact_candidates(tracker, loaded, rows=rec_rows)) == []
        assert not any(
            "cold:" in str(c.canonical_path) or "bucket" in str(c.canonical_path)
            for c in iter_artifact_candidates(tracker, loaded)
        )
    finally:
        tracker.close()


# ---------------------------------------------------------------------------
# Path-only filesystem-probe boundary (materialize._artifact_recovery_roots)
# ---------------------------------------------------------------------------


def test_artifact_recovery_roots_never_probe_cwd_relative_pseudo_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unresolved scheme must not become a CWD-relative probe path."""
    db_path = tmp_path / "prov.duckdb"
    art = _produce_directory_artifact(tmp_path, db_path, rel="out/skims.zarr")

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        tracker.db.upsert_artifact_locations(
            [
                _row(
                    loaded.id,
                    mount_name="scratch",
                    uri="scratch://bucket/prefix/out/skims.zarr",
                )
            ]
        )

        # A directory matching the CWD-relative pseudo-path exists in the CWD.
        pseudo = tmp_path / "scratch:" / "bucket" / "prefix" / "out" / "skims.zarr"
        pseudo.mkdir(parents=True)
        (pseudo / "0.0").write_bytes(b"z")
        monkeypatch.chdir(tmp_path)

        rec_rows = tracker.db.get_artifact_recovery_locations(loaded.id)
        assert (
            list(iter_artifact_candidates(tracker, artifact=loaded, rows=rec_rows))
            == []
        )
        assert not any(
            c.canonical_path == pseudo
            for c in iter_artifact_candidates(tracker, artifact=loaded)
        )
    finally:
        tracker.close()


# ---------------------------------------------------------------------------
# Ledger writeback: unextractable rows are retained, never duplicated
# ---------------------------------------------------------------------------


def _unextractable_row(artifact_id: uuid.UUID) -> ArtifactLocationRow:
    return _row(
        artifact_id,
        mount_name="cold",
        uri="cold://entirely/different/layout.bin",
        site="advisory",
    )


def test_single_append_retains_unextractable_row(tmp_path: Path) -> None:
    db_path = tmp_path / "prov.duckdb"
    art = _produce_directory_artifact(tmp_path, db_path, rel="out/skims.zarr")

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        original = _unextractable_row(loaded.id)
        tracker.db.upsert_artifact_locations([original])

        archive_root = tmp_path / "archive"
        (archive_root / "out" / "skims.zarr").mkdir(parents=True)
        (archive_root / "out" / "skims.zarr" / "0.0").write_bytes(b"z")
        tracker.set_artifact_recovery_roots(loaded, [str(archive_root)], append=True)

        rows = tracker.db.get_artifact_recovery_locations(loaded.id)
        by_uri = {r.uri: r for r in rows}
        # The unextractable row survives with its coordinate, site, and status.
        assert original.uri in by_uri
        kept = by_uri[original.uri]
        assert kept.location_id == original.location_id
        assert kept.site == original.site
        assert kept.status == "active"
        assert kept.witness == original.witness
        # No duplicated artifact suffix was written.
        assert f"{original.uri}/out/skims.zarr" not in by_uri
        # The newly appended root registered correctly.
        assert any(r.uri.endswith(f"{archive_root}/out/skims.zarr") for r in rows)
    finally:
        tracker.close()


def test_bulk_append_retains_unextractable_row(tmp_path: Path) -> None:
    db_path = tmp_path / "prov.duckdb"
    art = _produce_directory_artifact(tmp_path, db_path, rel="out/skims.zarr")

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        original = _unextractable_row(loaded.id)
        tracker.db.upsert_artifact_locations([original])

        archive_root = tmp_path / "archive_bulk"
        (archive_root / "out" / "skims.zarr").mkdir(parents=True)
        (archive_root / "out" / "skims.zarr" / "0.0").write_bytes(b"z")
        tracker._set_artifact_recovery_roots_bulk(
            [loaded], [str(archive_root)], append=True
        )

        rows = tracker.db.get_artifact_recovery_locations(loaded.id)
        by_uri = {r.uri: r for r in rows}
        assert original.uri in by_uri
        kept = by_uri[original.uri]
        assert kept.location_id == original.location_id
        assert kept.site == original.site
        assert kept.status == "active"
        assert kept.witness == original.witness
        assert f"{original.uri}/out/skims.zarr" not in by_uri
    finally:
        tracker.close()


def test_append_with_renamed_symlink_ancestor_keeps_suffix_layout(
    tmp_path: Path,
) -> None:
    """Appending over an existing row with a renamed symlink ancestor keeps its layout."""
    db_path = tmp_path / "prov.duckdb"
    scratch = tmp_path / "scratch"
    step = scratch / "run-x" / "consist-recovery" / "step_a"
    real_output = step / "beam_output_renamed"
    artifact_dir = real_output / "skims.zarr"
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "0.0").write_bytes(b"z")
    (step / "beam").mkdir()
    (step / "beam" / "beam_output").symlink_to(real_output, target_is_directory=True)

    art = _produce_directory_artifact(tmp_path, db_path)

    archiver = Tracker(
        run_dir=tmp_path / "archiver_ws",
        db_path=str(db_path),
        mounts={"scratch": str(scratch)},
    )
    try:
        loaded = archiver.get_artifact(art.id)
        assert loaded is not None
        # Seed the step-scoped row whose lexical ancestor is a renamed symlink,
        # then append a second root to exercise writeback extraction of the
        # existing row.
        _seed_step_scoped_row(archiver, loaded.id)
        second_root = tmp_path / "archive_second"
        (second_root / "beam" / "beam_output" / "skims.zarr").mkdir(parents=True)
        (second_root / "beam" / "beam_output" / "skims.zarr" / "0.0").write_bytes(b"z")
        archiver.set_artifact_recovery_roots(loaded, [str(second_root)], append=True)

        rows = archiver.db.get_artifact_recovery_locations(loaded.id)
        uris = [r.uri for r in rows]
        # The existing step-scoped row survived extraction without a duplicated suffix.
        assert (
            "scratch://run-x/consist-recovery/step_a/beam/beam_output/skims.zarr"
            in uris
        )
        assert any(
            u.endswith(f"{second_root}/beam/beam_output/skims.zarr") for u in uris
        )
        duplicated = [
            u for u in uris if u.endswith("skims.zarr/beam/beam_output/skims.zarr")
        ]
        assert duplicated == []
    finally:
        archiver.close()


# ---------------------------------------------------------------------------
# Policy boundaries: translation/discovery never mutates ledger state
# ---------------------------------------------------------------------------


def test_translation_and_discovery_do_not_mutate_ledger(tmp_path: Path) -> None:
    db_path = tmp_path / "prov.duckdb"
    scratch = tmp_path / "scratch"
    artifact_dir = (
        scratch
        / "run-x"
        / "consist-recovery"
        / "step_a"
        / "beam"
        / "beam_output"
        / "skims.zarr"
    )
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "0.0").write_bytes(b"z")

    art = _produce_directory_artifact(tmp_path, db_path)

    tracker = _consumer(tmp_path, db_path)
    try:
        loaded = tracker.get_artifact(art.id)
        assert loaded is not None
        row = _seed_step_scoped_row(tracker, loaded.id)

        before = {
            r.location_id: (r.status, r.witness, r.observed_at_ns)
            for r in tracker.db.get_artifact_recovery_locations(loaded.id)
        }

        tracker.artifact_recovery_roots(loaded, resolve=False)
        tracker.artifact_recovery_roots(loaded, resolve=True)
        tracker.fs.extract_recovery_root_from_location_row(
            loaded.uri, row, historical_run_dir=scratch
        )
        list(iter_artifact_candidates(tracker, artifact=loaded))

        after = {
            r.location_id: (r.status, r.witness, r.observed_at_ns)
            for r in tracker.db.get_artifact_recovery_locations(loaded.id)
        }
        assert before == after
    finally:
        tracker.close()
