from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError, replace
import gc
import hashlib
import logging
from pathlib import Path
import pickle
from unittest.mock import MagicMock

import pytest

import consist
from consist.api import is_spatial_artifact
from consist.artifact import Artifact
from consist.core.artifacts import ArtifactManager
from consist.core.context import use_tracker
from consist.core.identity import IdentityManager
from consist.core.persistence import DatabaseManager
from consist.core.tracker import Tracker
from consist.digest import Digest, Identity
from consist.location import Location, Observation
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation
from tests.helpers.artifact_factory import make_artifact


@pytest.fixture
def mock_tracker(tmp_path: Path) -> MagicMock:
    tracker = MagicMock()
    tracker.run_dir = tmp_path / "workspace"
    tracker.run_dir.mkdir(parents=True, exist_ok=True)
    tracker.mounts = {}
    tracker.archive_mounts = {}
    tracker.db_path = None
    tracker.ledger = None
    tracker.db = None
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.fs.normalize_recovery_roots = lambda roots: (
        [str(roots)] if isinstance(roots, (str, Path)) else [str(r) for r in roots]
    )
    tracker.identity.hashing_strategy = "full"
    tracker.identity.compute_file_checksum.return_value = "a" * 64
    return tracker


def test_create_artifact_stateless(mock_tracker: MagicMock, tmp_path: Path) -> None:
    manager = ArtifactManager(mock_tracker)
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("col\n1\n", encoding="utf-8")

    art = manager.create_artifact(path=csv_path, key="test", run_id="run_ABC")

    assert art.run_id == "run_ABC"
    assert art.key == "test"
    assert art.driver == "csv"
    assert art.anchor == Digest("sha256:file", "a" * 64)
    assert art.hash == f"sha256:file:{'a' * 64}"
    assert art.resolve_bound_tracker() is mock_tracker


def test_create_artifact_keeps_parquet_inference_non_spatial(
    mock_tracker: MagicMock, tmp_path: Path
) -> None:
    path = tmp_path / "tiny.parquet"
    path.write_bytes(b"not a real parquet file")

    artifact = ArtifactManager(mock_tracker).create_artifact(path=path, key="tiny")

    assert artifact.driver == "parquet"
    assert is_spatial_artifact(artifact) is False


def test_create_artifact_uses_precomputed_hash(
    mock_tracker: MagicMock, tmp_path: Path
) -> None:
    manager = ArtifactManager(mock_tracker)
    precomputed = f"sha256:file:{'b' * 64}"

    art = manager.create_artifact(
        path=tmp_path / "test.csv",
        key="test",
        run_id="run_ABC",
        content_hash=precomputed,
    )

    assert art.hash == precomputed
    assert art.anchor == Digest.parse(precomputed)
    mock_tracker.identity.compute_file_checksum.assert_not_called()


def test_create_artifact_dir_uses_precomputed_hash(
    mock_tracker: MagicMock, tmp_path: Path
) -> None:
    manager = ArtifactManager(mock_tracker)
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()
    precomputed = f"sha256:manifest-v1:{'c' * 64}"

    art = manager.create_artifact(
        path=str(data_dir),
        key="dir_artifact",
        run_id="run_ABC",
        content_hash=precomputed,
    )

    assert art.hash == precomputed
    mock_tracker.identity.compute_file_checksum.assert_not_called()


def test_create_artifact_rejects_mismatched_validated_hash(
    mock_tracker: MagicMock, tmp_path: Path
) -> None:
    manager = ArtifactManager(mock_tracker)
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("col\n1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="content_hash does not match"):
        manager.create_artifact(
            path=csv_path,
            key="test",
            run_id="run_ABC",
            content_hash="d" * 64,
            validate_content_hash=True,
        )

    mock_tracker.identity.compute_file_checksum.assert_called_once()


def test_create_artifact_hashes_once_for_reuse_and_validation(tmp_path: Path) -> None:
    tracker = MagicMock()
    tracker.run_dir = tmp_path / "runs"
    tracker.run_dir.mkdir()
    tracker.mounts = {}
    tracker.archive_mounts = {}
    tracker.db_path = None
    tracker.ledger = None
    digest = "e" * 64
    tracker.identity.hashing_strategy = "full"
    tracker.identity.compute_file_checksum.return_value = digest
    tracker.db = MagicMock()
    tracker.db.find_latest_artifact_at_uri.return_value = None
    tracker.db.find_latest_artifact_by_hash.return_value = None

    manager = ArtifactManager(tracker)
    output_path = tmp_path / "output.csv"
    output_path.write_text("a,b\n1,2\n", encoding="utf-8")

    art = manager.create_artifact(
        path=output_path,
        key="output",
        run_id="run_ABC",
        direction="output",
        reuse_if_unchanged=True,
        validate_content_hash=True,
    )

    assert art.hash == f"sha256:file:{digest}"
    assert tracker.identity.compute_file_checksum.call_count == 1


def test_create_artifact_does_not_mark_fast_file_observations_verifiable(
    mock_tracker: MagicMock, tmp_path: Path
) -> None:
    mock_tracker.identity.hashing_strategy = "fast"
    mock_tracker.identity.compute_file_checksum.return_value = "f" * 64

    source = tmp_path / "fast.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    artifact = ArtifactManager(mock_tracker).create_artifact(
        path=source,
        key="fast_input",
        run_id="run_a",
    )

    assert artifact.identity.verifiable is False
    assert artifact.anchor == Digest("legacy-v1:unverified", "f" * 64)
    assert artifact.hints[0].basis == "computed"


def test_create_artifact_persists_typed_full_file_content_identity(
    mock_tracker: MagicMock, tmp_path: Path
) -> None:
    digest = "a" * 64
    mock_tracker.identity.hashing_strategy = "full"
    mock_tracker.identity.compute_file_checksum.return_value = digest

    source = tmp_path / "full.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    artifact = ArtifactManager(mock_tracker).create_artifact(
        path=source,
        key="full_input",
        run_id="run_a",
    )

    assert artifact.identity.verifiable is True
    assert artifact.anchor == Digest("sha256:file", digest)
    assert artifact.hints[0].basis == "computed"


def test_create_artifact_records_fast_directory_observation_as_historical(
    mock_tracker: MagicMock, tmp_path: Path
) -> None:
    mock_tracker.identity = IdentityManager(hashing_strategy="fast")

    directory = tmp_path / "fast_directory"
    (directory / "nested").mkdir(parents=True)
    (directory / "nested" / "payload.bin").write_bytes(b"payload")

    artifact = ArtifactManager(mock_tracker).create_artifact(
        path=directory,
        key="fast_directory",
        run_id="run_a",
        driver="other",
    )

    assert artifact.identity.verifiable is False
    assert artifact.anchor.scheme == "legacy-v1:unverified"
    assert artifact.hints[0].basis == "computed"


def test_create_artifacts_share_content_hub_when_hashes_match(tmp_path: Path) -> None:
    db_path = tmp_path / "shared_content.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        tracker = MagicMock()
        tracker.run_dir = tmp_path / "workspace"
        tracker.run_dir.mkdir()
        tracker.mounts = {"outputs": str(tmp_path)}
        tracker.archive_mounts = {}
        tracker.db_path = str(db_path)
        tracker.ledger = db.ledger
        tracker.db = db
        digest = "b" * 64
        tracker.identity.hashing_strategy = "full"
        tracker.identity.compute_file_checksum.return_value = digest

        manager = ArtifactManager(tracker)

        path_a = tmp_path / "a.parquet"
        path_a.write_text("a", encoding="utf-8")
        path_b = tmp_path / "b.parquet"
        path_b.write_text("b", encoding="utf-8")

        art_a = manager.create_artifact(
            path=path_a,
            key="shared",
            run_id="run_a",
            driver="parquet",
        )
        art_b = manager.create_artifact(
            path=path_b,
            key="shared",
            run_id="run_b",
            driver="parquet",
        )
        db.upsert_artifact(art_a)
        db.upsert_artifact(art_b)

        assert art_a.id != art_b.id
        assert art_a.run_id == "run_a"
        assert art_b.run_id == "run_b"
        assert art_a.anchor == art_b.anchor == Digest("sha256:file", digest)

        with db.ledger.exec_cursor() as cur:
            content_count = cur.execute(
                "SELECT COUNT(*) FROM main.content WHERE anchor = ?",
                [str(art_a.anchor)],
            ).fetchone()[0]
        assert content_count == 1
    finally:
        db.close()


def test_historical_artifact_replay_does_not_create_verifiable_digest(
    mock_tracker: MagicMock,
) -> None:
    manager = ArtifactManager(mock_tracker)
    existing = make_artifact(
        key="k",
        container_uri="inputs://a.parquet",
        driver="parquet",
        identity=Identity(anchor=Digest("legacy-v1:unverified", "1" * 64)),
        run_id="run_x",
    )

    out = manager.create_artifact(path=existing, run_id="run_x")

    assert out.identity.verifiable is False
    assert out.anchor == existing.anchor


def test_historical_persisted_artifact_replay_keeps_content_digest_empty(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "artifact_replay.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        original = make_artifact(
            key="geoid_to_zone",
            container_uri="outputs://zones.parquet",
            driver="parquet",
            identity=Identity(anchor=Digest("legacy-v1:unverified", "2" * 64)),
            run_id="run_a",
        )
        db.upsert_artifact(original)

        detached = db.get_artifact(original.id)
        assert detached is not None
        replayed = copy.deepcopy(detached)

        tracker = MagicMock()
        tracker.run_dir = tmp_path / "runs"
        tracker.run_dir.mkdir()
        tracker.mounts = {}
        tracker.archive_mounts = {}
        tracker.db_path = str(db_path)
        tracker.ledger = db.ledger
        tracker.db = db
        tracker.resolve_uri = lambda uri: f"/abs/{uri}"
        tracker.identity.hashing_strategy = "full"

        manager = ArtifactManager(tracker)
        with caplog.at_level(logging.WARNING):
            out = manager.create_artifact(path=replayed, run_id="run_b")

        with db.ledger.exec_cursor() as cur:
            digest_count = cur.execute(
                "SELECT COUNT(*) FROM main.content_digest WHERE anchor = ?",
                [str(original.anchor)],
            ).fetchone()[0]

        assert out is not replayed
        assert out.id == replayed.id
        assert out.hash == replayed.hash == str(original.anchor)
        assert digest_count == 0
        assert out.identity.verifiable is False
        assert "Failed to record artifact content identity" not in caplog.text
        assert out.parent_artifact_id is None
    finally:
        db.close()


def test_historical_artifact_driver_override_preserves_unverified_identity(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "artifact_driver_override.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        original = make_artifact(
            key="geoid_to_zone",
            container_uri="outputs://zones.parquet",
            driver="parquet",
            identity=Identity(anchor=Digest("legacy-v1:unverified", "3" * 64)),
            run_id="run_a",
        )
        db.upsert_artifact(original)
        db.ledger.record_run(run_id="run_b", model_name="demo")

        detached = db.get_artifact(original.id)
        assert detached is not None
        replayed = copy.deepcopy(detached)

        tracker = MagicMock()
        tracker.run_dir = tmp_path / "runs"
        tracker.run_dir.mkdir()
        tracker.mounts = {}
        tracker.archive_mounts = {}
        tracker.db_path = str(db_path)
        tracker.ledger = db.ledger
        tracker.db = db
        tracker.resolve_uri = lambda uri: f"/abs/{uri}"
        tracker.identity.hashing_strategy = "full"

        manager = ArtifactManager(tracker)
        with caplog.at_level(logging.WARNING):
            out = manager.create_artifact(
                path=replayed,
                run_id="run_b",
                driver="csv",
            )

        db.sync_artifact(out, run_id="run_b", direction="output")
        persisted = db.get_artifact(out.id)

        with db.ledger.exec_cursor() as cur:
            digest_count = cur.execute(
                "SELECT COUNT(*) FROM main.content_digest WHERE anchor = ?",
                [str(original.anchor)],
            ).fetchone()[0]

        assert out is not replayed
        assert out.id == replayed.id
        assert out.driver == "csv"
        assert replayed.driver == "parquet"
        assert persisted is not None
        assert persisted.identity.verifiable is False
        assert digest_count == 0
        assert "Failed to record artifact content identity" not in caplog.text
    finally:
        db.close()


def test_create_artifact_preserves_parent_artifact_id_on_detached_clone(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "artifact_parent_clone.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        parent = make_artifact(
            key="container",
            container_uri="outputs://container.h5",
            driver="h5",
            hash="c" * 64,
            run_id="run_parent",
        )
        db.upsert_artifact(parent)
        parent_id = parent.id

        original = make_artifact(
            key="geoid_to_zone",
            container_uri="outputs://zones.h5",
            table_path="/zones",
            driver="h5_table",
            hash="d" * 64,
            parent_artifact_id=parent_id,
            run_id="run_a",
            meta={"parent_id": str(parent_id)},
        )
        db.upsert_artifact(original)

        detached = db.get_artifact(original.id)
        assert detached is not None
        replayed = copy.deepcopy(detached)

        tracker = MagicMock()
        tracker.run_dir = tmp_path / "runs"
        tracker.run_dir.mkdir()
        tracker.mounts = {}
        tracker.archive_mounts = {}
        tracker.db_path = str(db_path)
        tracker.ledger = db.ledger
        tracker.db = db
        tracker.resolve_uri = lambda uri: f"/abs/{uri}"

        manager = ArtifactManager(tracker)
        out = manager.create_artifact(path=replayed, run_id="run_b")

        assert out.parent_artifact_id == parent_id
        assert out.meta["parent_id"] == str(parent_id)
        assert out.member == "/zones"
    finally:
        db.close()


def test_create_artifact_clones_reused_input_artifact_without_mutating_parent(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db_path = tmp_path / "artifact_input_reuse.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        input_path = tmp_path / "zones.parquet"
        input_path.write_text("zone,data\n1,a\n", encoding="utf-8")

        digest = "4" * 64
        original = make_artifact(
            key="geoid_to_zone",
            container_uri="inputs://zones.parquet",
            driver="parquet",
            hash=f"sha256:file:{digest}",
            run_id="run_a",
        )
        db.upsert_artifact(original)

        detached = db.get_artifact(original.id)
        assert detached is not None
        reused_parent = copy.deepcopy(detached)

        tracker = MagicMock()
        tracker.run_dir = tmp_path / "runs"
        tracker.run_dir.mkdir()
        tracker.mounts = {"inputs": str(tmp_path)}
        tracker.archive_mounts = {}
        tracker.db_path = str(db_path)
        tracker.ledger = db.ledger
        tracker.identity.hashing_strategy = "full"
        tracker.identity.compute_file_checksum.return_value = digest
        tracker.db = MagicMock(wraps=db)
        tracker.db.find_latest_artifact_at_uri.return_value = reused_parent

        manager = ArtifactManager(tracker)
        with caplog.at_level(logging.WARNING):
            out = manager.create_artifact(
                path=input_path,
                key="geoid_to_zone",
                direction="input",
                step_tag="consumer",
            )

        assert out is not reused_parent
        assert out.id == reused_parent.id
        assert out.anchor == reused_parent.anchor
        assert out.meta["step_tag"] == "consumer"
        assert "step_tag" not in reused_parent.meta
        assert "Failed to record artifact content identity" not in caplog.text
    finally:
        db.close()


def _seed_artifact_with_schema_dependent(
    db: DatabaseManager,
    *,
    key: str,
    schema_id: str,
    anchor: Digest,
) -> Artifact:
    db.ledger.record_run(run_id="run_a", model_name="demo")
    db.ledger.record_run(run_id="run_b", model_name="demo")
    original = make_artifact(
        key=key,
        container_uri=f"outputs://{key}.csv",
        driver="csv",
        identity=Identity(anchor=anchor),
        run_id="run_a",
        meta={"marker": "original"},
    )
    db.upsert_artifact(original)
    schema = ArtifactSchema(id=schema_id, summary_json={"fields": []})
    obs = ArtifactSchemaObservation(
        artifact_id=original.id,
        schema_id=schema.id,
        source="file",
        run_id="run_a",
    )
    db.ledger.upsert_artifact_schema(schema, [], observation=obs)
    return original


def test_sync_artifacts_and_schema_updates_preserve_existing_anchor_with_dependents(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "artifact_dependent_sync.duckdb"))
    try:
        anchor_a = Digest("sha256:file", "5" * 64)
        anchor_b = Digest("sha256:file", "6" * 64)
        original = _seed_artifact_with_schema_dependent(
            db,
            key="dependent_artifact",
            schema_id="schema_hash_dep",
            anchor=anchor_a,
        )

        # Even if an in-memory handle carries a different anchor for the same artifact UUID,
        # ON CONFLICT (id) DO UPDATE preserves the existing immutable anchor and updates meta.
        candidate = make_artifact(
            id=original.id,
            key="dependent_artifact",
            container_uri="outputs://dependent_artifact.csv",
            driver="csv",
            identity=Identity(anchor=anchor_b),
            run_id="run_a",
            meta={"marker": "updated"},
        )
        db.sync_artifacts(artifacts=[candidate], run_id="run_b", direction="output")
        persisted = db.get_artifact(original.id)
        assert persisted is not None
        assert persisted.anchor == anchor_a
        assert persisted.meta["marker"] == "updated"

        # persist_artifact_schema_observation_bundle preserves anchor and merges meta
        obs_b = ArtifactSchemaObservation(
            artifact_id=original.id,
            schema_id="schema_hash_dep",
            source="file",
            run_id="run_b",
        )
        db.persist_artifact_schema_observation_bundle(
            artifact=persisted,
            observation=obs_b,
            meta_updates={"obs_bundled": True},
        )
        after_obs = db.get_artifact(original.id)
        assert after_obs is not None
        assert after_obs.anchor == anchor_a
        assert after_obs.meta["marker"] == "updated"
        assert after_obs.meta["obs_bundled"] is True

        # persist_artifact_schema_profile preserves anchor and merges meta
        schema_prof = ArtifactSchema(id="schema_hash_prof", summary_json={"fields": []})
        obs_prof = ArtifactSchemaObservation(
            artifact_id=original.id,
            schema_id=schema_prof.id,
            source="file",
            run_id="run_b",
        )
        db.persist_artifact_schema_profile(
            artifact=after_obs,
            schema=schema_prof,
            fields=[],
            observation=obs_prof,
            meta_updates={"profiled": True},
            relations=[],
        )
        after_prof = db.get_artifact(original.id)
        assert after_prof is not None
        assert after_prof.anchor == anchor_a
        assert after_prof.schema_id == "schema_hash_prof"
        assert after_prof.meta["profiled"] is True

        # update_artifact_meta returns updated Artifact and preserves anchor
        updated = db.update_artifact_meta(after_prof, {"meta_patched": True})
        assert isinstance(updated, Artifact)
        after_meta = db.get_artifact(original.id)
        assert after_meta is not None
        assert after_meta.anchor == anchor_a
        assert after_meta.meta["meta_patched"] is True

        # sync_artifact preserves existing anchor while updating metadata/facets
        with_facet = after_meta.with_meta(
            {
                "facet": {"label": "demo"},
                "artifact_facet_namespace": "activitysim",
                "facet_applied": True,
            },
            merge=True,
        )
        db.sync_artifact(
            replace(with_facet, identity=Identity(anchor=anchor_b)),
            run_id="run_a",
            direction="output",
            facet_index=True,
        )
        after_sync = db.get_artifact(original.id)
        assert after_sync is not None
        assert after_sync.anchor == anchor_a
        assert after_sync.meta["facet_applied"] is True
        kv_rows = db.get_artifact_kv(original.id, namespace="activitysim")
        assert {r.key_path: r.value_str for r in kv_rows} == {"label": "demo"}
    finally:
        db.close()


def test_frozen_artifact_immutability_copy_pickle_and_meta_isolation(
    tmp_path: Path,
) -> None:
    local_bytes = b"x\n1\n"
    local_file = tmp_path / "transient.csv"
    local_file.write_bytes(local_bytes)
    local_digest = hashlib.sha256(local_bytes).hexdigest()

    class _DblessTracker:
        db_path = None
        mounts = {"outputs": str(tmp_path)}

    tracker = _DblessTracker()
    art = make_artifact(
        key="transient",
        container_uri="outputs://transient.csv",
        driver="csv",
        hash=f"sha256:file:{local_digest}",
        meta={"nested": {"inner_key": "inner_val"}, "list": [1, 2, 3]},
    ).bind_tracker(tracker)

    with pytest.raises(FrozenInstanceError):
        art.key = "mutated"  # type: ignore[misc]

    with pytest.raises(TypeError):
        art.meta["nested"] = {}  # type: ignore[index]

    # Deepcopy & copy preserve identity, metadata, hints, and weak tracker lookup by UUID
    deep_cloned = copy.deepcopy(art)
    assert deep_cloned is not art
    assert deep_cloned == art
    assert deep_cloned.id == art.id
    assert deep_cloned.key == art.key
    assert deep_cloned.primary_location == Location(uri="outputs://transient.csv")
    assert deep_cloned.hash == art.hash
    assert deep_cloned.resolve_bound_tracker() is tracker

    shallow_cloned = copy.copy(art)
    assert shallow_cloned == art
    assert shallow_cloned.resolve_bound_tracker() is tracker

    # Memo identity preservation inside compound structures
    container = [art, art]
    cloned_container = copy.deepcopy(container)
    assert cloned_container[0] is cloned_container[1]
    assert cloned_container[0].id == art.id

    # with_meta returns a new Artifact without mutating original
    updated = art.with_meta({"nested": {"inner_key": "modified"}}, merge=True)
    assert updated.id == art.id
    assert updated.meta["nested"]["inner_key"] == "modified"
    assert art.meta["nested"]["inner_key"] == "inner_val"

    # Pickle round-trip preserves Artifact fields
    unpickled = pickle.loads(pickle.dumps(art))
    assert unpickled == art
    assert unpickled.meta.to_dict() == art.meta.to_dict()

    # Deprecated v1 compatibility properties emit DeprecationWarning
    with pytest.deprecated_call(match="container_uri is deprecated"):
        assert art.container_uri == "outputs://transient.csv"
    with pytest.deprecated_call(match="abs_path is deprecated"):
        assert art.abs_path is not None


def test_persisted_artifact_reload_and_replace_survive_gc(tmp_path: Path) -> None:
    db = DatabaseManager(str(tmp_path / "gc_test.duckdb"))
    try:
        db.ledger.record_run(run_id="run_out", model_name="demo")
        db.ledger.record_run(run_id="run_in", model_name="demo")

        art = make_artifact(
            key="persisted_art",
            container_uri="outputs://persisted.csv",
            driver="csv",
            hash=f"sha256:file:{'8' * 64}",
            run_id="run_out",
            meta={"initial": "value", "nested": {"k": "v"}},
        )
        db.sync_artifacts(artifacts=[art], run_id="run_out", direction="output")
        art_id = art.id

        loaded = db.get_artifact(art_id)
        assert loaded is not None
        cloned = copy.deepcopy(loaded)
        replaced = replace(cloned, key="updated_key").with_meta({"step": 2}, merge=True)

        del art
        del loaded
        gc.collect()

        db.sync_artifacts(artifacts=[replaced], run_id="run_in", direction="input")
        db.update_artifact_meta(
            art_id, {"db_update": "from_deepcopy"}, merge=True, raise_on_error=True
        )

        persisted = db.get_artifact(art_id)
        assert persisted is not None
        assert persisted.meta["step"] == 2
        assert persisted.meta["db_update"] == "from_deepcopy"
    finally:
        db.close()


def test_artifact_with_hints_deduplicates_observations() -> None:
    anchor = Digest("sha256:file", "9" * 64)
    obs = Observation(
        location=Location(uri="outputs://dup.parquet"),
        identity=Identity(anchor=anchor),
        access_driver="parquet",
        basis="declared",
    )
    art = Artifact(
        id="00000000-0000-0000-0000-000000000099",
        key="dup",
        driver="parquet",
        identity=Identity(anchor=anchor),
    )
    with_hints = art.with_hints([obs, obs])
    assert len(with_hints.hints) == 1
    assert with_hints.primary_location == Location(uri="outputs://dup.parquet")


def test_artifact_tracker_property_and_ensure_local_path(
    tracker: Tracker, tmp_path: Path
) -> None:
    archive_dir = tmp_path / "archive"
    out_file = tracker.run_dir / "outputs" / "households.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_bytes(b"households-v1")

    with tracker.start_run("produce_households", "urbansim"):
        artifact = tracker.log_artifact(out_file, key="households", direction="output")

    # 1) Outside active context, artifact.tracker resolves via weakref
    assert artifact.tracker is tracker

    # Unbound artifact falls back to current_tracker() inside use_tracker
    unbound = make_artifact(
        key="unbound",
        container_uri="inputs://unbound.parquet",
        driver="parquet",
    )
    assert unbound.tracker is None
    with use_tracker(tracker):
        assert unbound.tracker is tracker

    # 2) When local path already exists on disk, ensure_local_path returns it directly
    resolved_existing = artifact.ensure_local_path()
    assert resolved_existing == out_file.resolve()
    assert consist.ensure_local_path(artifact) == out_file.resolve()

    # 3) Archive artifact, wipe scratch copy, and recover via ensure_local_path outside context
    tracker.archive_artifact(artifact, archive_dir, mode="copy")
    out_file.unlink()
    assert not out_file.exists()

    recovered_path = artifact.ensure_local_path()
    assert recovered_path == out_file.resolve()
    assert out_file.read_bytes() == b"households-v1"

    # 4) Explicit target_root materializes into the requested root
    custom_root = tmp_path / "custom_target"
    out_file.unlink()
    custom_path = consist.ensure_local_path(artifact, target_root=custom_root)
    assert custom_path == (custom_root / "outputs" / "households.parquet").resolve()
    assert custom_path.read_bytes() == b"households-v1"

    # 5) Missing source behavior: on_missing="warn" returns None, on_missing="raise" raises
    (archive_dir / "outputs" / "households.parquet").unlink()
    custom_path.unlink()

    assert artifact.ensure_local_path(on_missing="warn") is None
    with pytest.raises(FileNotFoundError):
        artifact.ensure_local_path(on_missing="raise")

    # 6) Detached artifact with target_root requires a tracker even if self.path exists elsewhere
    elsewhere_file = tmp_path / "elsewhere.parquet"
    elsewhere_file.write_bytes(b"elsewhere")
    detached = make_artifact(
        key="detached",
        container_uri=f"file://{elsewhere_file.resolve()}",
        driver="parquet",
        basis="declared",
    )
    assert detached.ensure_local_path() == elsewhere_file.resolve()
    assert (
        detached.ensure_local_path(
            target_root=tmp_path / "other_target", on_missing="warn"
        )
        is None
    )
