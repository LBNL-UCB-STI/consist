import copy
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import gc
import hashlib
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from consist import is_spatial_artifact
from consist.core.artifacts import ArtifactManager
from consist.core.identity import IdentityManager
from consist.core.persistence import DatabaseManager
from consist.digest import Digest, file_sha256
from consist.models.artifact import Artifact
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation


@pytest.fixture
def mock_tracker(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.hashing_strategy = "full"
    tracker.db = MagicMock()
    tracker.db.find_latest_artifact_at_uri.return_value = None
    return tracker


def test_create_artifact_stateless(mock_tracker, tmp_path):
    source = tmp_path / "test.csv"
    source.write_text("col\n1\n", encoding="utf-8")
    manager = ArtifactManager(mock_tracker)

    art = manager.create_artifact(path=source, key="test", run_id="run_ABC")

    assert art.run_id == "run_ABC"
    assert art.key == "test"
    assert art.digest == Digest("sha256:file", file_sha256(source))


def test_create_artifact_keeps_parquet_inference_non_spatial(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.hashing_strategy = "full"
    tracker.db = MagicMock()

    path = tmp_path / "tiny.parquet"
    path.write_bytes(b"not a real parquet file")

    artifact = ArtifactManager(tracker).create_artifact(path=path, key="tiny")

    assert artifact.driver == "parquet"
    assert is_spatial_artifact(artifact) is False


def test_create_artifact_uses_precomputed_hash(mock_tracker, tmp_path):
    manager = ArtifactManager(mock_tracker)
    precomputed = "a" * 64

    with patch("consist.core.artifacts.compute_digest") as mock_compute:
        art = manager.create_artifact(
            path=tmp_path / "test.csv",
            key="test",
            run_id="run_ABC",
            content_hash=precomputed,
        )

    assert art.digest == Digest("sha256:file", precomputed)
    mock_compute.assert_not_called()


def test_create_artifact_dir_uses_precomputed_hash(mock_tracker, tmp_path):
    manager = ArtifactManager(mock_tracker)
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()

    with patch("consist.core.artifacts.build_manifest_v1") as mock_manifest:
        art = manager.create_artifact(
            path=str(data_dir),
            key="dir_artifact",
            run_id="run_ABC",
            content_hash="precomputed_hash",
        )

    expected_opaque = hashlib.sha256(b"precomputed_hash").hexdigest()
    assert art.digest == Digest("caller_supplied_v1:sha256", expected_opaque)
    assert art.meta["caller_supplied_hash"] == "precomputed_hash"
    mock_manifest.assert_not_called()


def test_create_artifact_rejects_mismatched_validated_hash(mock_tracker, tmp_path):
    manager = ArtifactManager(mock_tracker)
    source = tmp_path / "test.csv"
    source.write_text("col\n1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="content_hash does not match"):
        manager.create_artifact(
            path=source,
            key="test",
            run_id="run_ABC",
            content_hash="different_hash",
            validate_content_hash=True,
        )


def test_create_artifact_hashes_once_for_reuse_and_validation(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.hashing_strategy = "full"
    tracker.db.find_latest_artifact_at_uri.return_value = None

    manager = ArtifactManager(tracker)
    output_path = tmp_path / "output.csv"
    output_path.write_text("a,b\n1,2\n", encoding="utf-8")
    expected_digest = Digest("sha256:file", file_sha256(output_path))

    with patch(
        "consist.core.artifacts.compute_digest",
        return_value=expected_digest,
    ) as mock_compute:
        art = manager.create_artifact(
            path=output_path,
            key="output",
            run_id="run_ABC",
            direction="output",
            reuse_if_unchanged=True,
            validate_content_hash=True,
        )

    assert art.digest == expected_digest
    assert mock_compute.call_count == 1


def test_create_artifact_does_not_dedupe_fast_file_observations(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.hashing_strategy = "fast"
    tracker.db = MagicMock()

    source = tmp_path / "fast.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    artifact = ArtifactManager(tracker).create_artifact(
        path=source,
        key="fast_input",
        run_id="run_a",
    )

    assert artifact.digest is not None
    assert artifact.digest.scheme == "stat_file_v1:sha256"


def test_create_artifact_persists_typed_full_file_content_identity(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.hashing_strategy = "full"
    tracker.db = MagicMock()

    source = tmp_path / "full.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    artifact = ArtifactManager(tracker).create_artifact(
        path=source,
        key="full_input",
        run_id="run_a",
    )

    assert artifact.digest == Digest("sha256:file", file_sha256(source))


def test_create_artifact_records_fast_directory_observation_without_content_identity(
    tmp_path,
):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity = IdentityManager(hashing_strategy="fast")
    tracker.db = MagicMock()

    directory = tmp_path / "fast_directory"
    (directory / "nested").mkdir(parents=True)
    (directory / "nested" / "payload.bin").write_bytes(b"payload")

    artifact = ArtifactManager(tracker).create_artifact(
        path=directory,
        key="fast_directory",
        run_id="run_a",
        driver="other",
    )

    assert artifact.digest is not None
    assert artifact.digest.scheme == "stat_dir_v1:sha256"


def test_create_artifacts_share_content_identity_when_hashes_match(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {"outputs": str(tmp_path)}
    tracker.identity.hashing_strategy = "full"
    tracker.db = MagicMock()

    manager = ArtifactManager(tracker)

    path_a = tmp_path / "a.parquet"
    path_a.write_text("identical", encoding="utf-8")
    path_b = tmp_path / "b.parquet"
    path_b.write_text("identical", encoding="utf-8")

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

    assert art_a.id != art_b.id
    assert art_a.run_id == "run_a"
    assert art_b.run_id == "run_b"
    assert art_a.digest == art_b.digest == Digest("sha256:file", file_sha256(path_a))


def test_legacy_artifact_without_digest_preserves_none_digest():
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.db = MagicMock()

    manager = ArtifactManager(tracker)

    existing = Artifact(
        key="k",
        uri="inputs://a.parquet",
        driver="parquet",
        digest=None,
        run_id="run_x",
    )

    out = manager.create_artifact(path=existing, run_id="run_x")

    assert out.digest is None


def test_detached_artifact_replay_preserves_digest_and_id(tmp_path, caplog):
    db = DatabaseManager(str(tmp_path / "artifact_replay.duckdb"))
    try:
        shared_digest = Digest("sha256:file", "b" * 64)
        original = Artifact(
            key="geoid_to_zone",
            uri="outputs://zones.parquet",
            driver="parquet",
            digest=shared_digest,
            run_id="run_a",
        )
        db.sync_artifact(original)
        artifact_id = original.id

        detached = db.get_artifact(artifact_id)
        assert detached is not None
        replayed = deepcopy(detached)

        tracker = MagicMock()
        tracker.resolve_uri = lambda uri: f"/abs/{uri}"
        tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
        tracker.db = db

        manager = ArtifactManager(tracker)
        with caplog.at_level(logging.WARNING):
            out = manager.create_artifact(path=replayed, run_id="run_b")

        assert out.id == replayed.id
        assert out.digest == replayed.digest == shared_digest
        assert out.parent_artifact_id is None
    finally:
        db.close()


def test_detached_artifact_driver_override_preserves_digest(tmp_path, caplog):
    db = DatabaseManager(str(tmp_path / "artifact_driver_override.duckdb"))
    try:
        shared_digest = Digest("sha256:file", "c" * 64)
        original = Artifact(
            key="geoid_to_zone",
            uri="outputs://zones.parquet",
            driver="parquet",
            digest=shared_digest,
            run_id="run_a",
        )
        db.sync_artifact(original)
        artifact_id = original.id

        detached = db.get_artifact(artifact_id)
        assert detached is not None
        replayed = deepcopy(detached)

        tracker = MagicMock()
        tracker.resolve_uri = lambda uri: f"/abs/{uri}"
        tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
        tracker.db = db

        manager = ArtifactManager(tracker)
        with caplog.at_level(logging.WARNING):
            out = manager.create_artifact(
                path=replayed,
                run_id="run_b",
                driver="csv",
            )

        db.sync_artifact(out, run_id="run_b", direction="output")
        persisted = db.get_artifact(out.id)

        assert out is not replayed
        assert out.id == replayed.id
        assert replayed.driver == "parquet"
        assert persisted is not None
        assert persisted.driver == "csv"
        assert persisted.digest == shared_digest
    finally:
        db.close()


def test_create_artifact_preserves_parent_artifact_id_on_detached_clone(tmp_path):
    db = DatabaseManager(str(tmp_path / "artifact_parent_clone.duckdb"))
    try:
        parent_id = Artifact(
            key="container",
            uri="outputs://container.h5",
            driver="h5",
            run_id="run_parent",
        ).id

        original = Artifact(
            key="geoid_to_zone",
            uri="outputs://zones.h5",
            member="/zones",
            driver="h5_table",
            digest=Digest("h5_fixed_v1:sha256", "d" * 64),
            parent_artifact_id=parent_id,
            run_id="run_a",
            meta={"parent_id": str(parent_id)},
        )
        db.sync_artifact(original)
        artifact_id = original.id

        detached = db.get_artifact(artifact_id)
        assert detached is not None
        replayed = deepcopy(detached)

        tracker = MagicMock()
        tracker.resolve_uri = lambda uri: f"/abs/{uri}"
        tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
        tracker.db = db

        manager = ArtifactManager(tracker)
        out = manager.create_artifact(path=replayed, run_id="run_b")

        assert out.parent_artifact_id == parent_id
        assert out.meta["parent_id"] == str(parent_id)
    finally:
        db.close()


def test_create_artifact_reuses_input_artifact_immutably(tmp_path, caplog):
    db = DatabaseManager(str(tmp_path / "artifact_input_reuse.duckdb"))
    try:
        original = Artifact(
            key="geoid_to_zone",
            uri="inputs://zones.parquet",
            driver="parquet",
            digest=Digest("sha256:file", "e" * 64),
            run_id="run_a",
        )
        db.sync_artifact(original)
        artifact_id = original.id

        detached = db.get_artifact(artifact_id)
        assert detached is not None
        reused_parent = deepcopy(detached)

        tracker = MagicMock()
        tracker.resolve_uri = lambda uri: str(tmp_path / uri.split("://", 1)[1])
        tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
        tracker.mounts = {}
        tracker.db = MagicMock(wraps=db)
        tracker.db.find_latest_artifact_at_uri.return_value = reused_parent

        manager = ArtifactManager(tracker)
        input_path = tmp_path / "zones.parquet"
        input_path.write_text("zone,data\n1,a\n", encoding="utf-8")

        with caplog.at_level(logging.WARNING):
            out = manager.create_artifact(
                path=input_path,
                key="geoid_to_zone",
                direction="input",
            )

        assert out.id == reused_parent.id
        assert out.digest == reused_parent.digest
    finally:
        db.close()


def test_sync_artifacts_updates_meta_with_schema_dependents(tmp_path):
    db = DatabaseManager(str(tmp_path / "artifact_dependent_sync.duckdb"))
    try:
        shared_digest = Digest("sha256:file", "1" * 64)
        original = Artifact(
            key="dependent_artifact",
            uri="outputs://dependent.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "original"},
        )
        db.sync_artifact(original)
        schema = ArtifactSchema(
            id="schema_hash",
            summary_json={"fields": []},
        )
        db.persist_artifact_schema(
            schema,
            fields=[],
            relations=[],
            observation=ArtifactSchemaObservation(
                artifact_id=original.id,
                schema_id=schema.id,
                source="file",
            ),
        )
        artifact_id = original.id

        merged = Artifact(
            id=artifact_id,
            key="dependent_artifact",
            uri="outputs://dependent.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "updated"},
        )

        db.sync_artifacts(artifacts=[merged], run_id="run_b", direction="output")

        persisted = db.get_artifact(artifact_id)
        assert persisted is not None
        assert persisted.meta["marker"] == "updated"
    finally:
        db.close()


def test_persist_artifact_schema_observation_bundle_with_dependents(tmp_path):
    db = DatabaseManager(str(tmp_path / "artifact_schema_observation_bundle.duckdb"))
    try:
        shared_digest = Digest("sha256:file", "2" * 64)
        original = Artifact(
            key="bundle_artifact",
            uri="outputs://bundle.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "original"},
        )
        db.sync_artifact(original)
        schema = ArtifactSchema(
            id="schema_hash_bundle",
            summary_json={"fields": []},
        )
        db.persist_artifact_schema(
            schema,
            fields=[],
            relations=[],
            observation=ArtifactSchemaObservation(
                artifact_id=original.id,
                schema_id=schema.id,
                source="file",
            ),
        )
        artifact_id = original.id

        artifact = Artifact(
            id=artifact_id,
            key="bundle_artifact",
            uri="outputs://bundle.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "updated"},
        )
        observation = ArtifactSchemaObservation(
            artifact_id=artifact_id,
            schema_id="schema_hash_bundle",
            source="file",
            run_id="run_b",
        )

        db.persist_artifact_schema_observation_bundle(
            artifact=artifact,
            observation=observation,
            meta_updates={"profiled": True},
        )

        persisted = db.get_artifact(artifact_id)
        assert persisted is not None
        assert persisted.meta["marker"] == "updated"
        assert persisted.meta["profiled"] is True
    finally:
        db.close()


def test_persist_artifact_schema_profile_with_dependents(tmp_path):
    db = DatabaseManager(str(tmp_path / "artifact_schema_profile.duckdb"))
    try:
        shared_digest = Digest("sha256:file", "3" * 64)
        original = Artifact(
            key="profile_artifact",
            uri="outputs://profile.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "original"},
        )
        db.sync_artifact(original)
        schema = ArtifactSchema(
            id="schema_hash_profile",
            summary_json={"fields": []},
        )
        db.persist_artifact_schema(
            schema,
            fields=[],
            relations=[],
            observation=ArtifactSchemaObservation(
                artifact_id=original.id,
                schema_id=schema.id,
                source="file",
            ),
        )
        artifact_id = original.id

        artifact = Artifact(
            id=artifact_id,
            key="profile_artifact",
            uri="outputs://profile.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "updated"},
        )
        observation = ArtifactSchemaObservation(
            artifact_id=artifact_id,
            schema_id="schema_hash_profile",
            source="file",
            run_id="run_b",
        )

        db.persist_artifact_schema_profile(
            artifact=artifact,
            schema=schema,
            fields=[],
            observation=observation,
            meta_updates={"profiled": True},
            relations=[],
        )

        persisted = db.get_artifact(artifact_id)
        assert persisted is not None
        assert persisted.meta["marker"] == "updated"
        assert persisted.meta["profiled"] is True
    finally:
        db.close()


def test_update_artifact_meta_with_dependents(tmp_path):
    db = DatabaseManager(str(tmp_path / "artifact_update_meta.duckdb"))
    try:
        shared_digest = Digest("sha256:file", "4" * 64)
        original = Artifact(
            key="meta_artifact",
            uri="outputs://meta.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "original"},
        )
        db.sync_artifact(original)
        schema = ArtifactSchema(
            id="schema_hash_meta",
            summary_json={"fields": []},
        )
        db.persist_artifact_schema(
            schema,
            fields=[],
            relations=[],
            observation=ArtifactSchemaObservation(
                artifact_id=original.id,
                schema_id=schema.id,
                source="file",
            ),
        )
        artifact_id = original.id

        artifact = Artifact(
            id=artifact_id,
            key="meta_artifact",
            uri="outputs://meta.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "updated"},
        )

        updated = db.update_artifact_meta(artifact, {"profiled": True})

        persisted = db.get_artifact(artifact_id)
        assert updated is True
        assert persisted is not None
        assert persisted.meta["marker"] == "updated"
        assert persisted.meta["profiled"] is True
    finally:
        db.close()


def test_persist_artifact_facet_bundle_with_dependents(tmp_path):
    db = DatabaseManager(str(tmp_path / "artifact_facet_bundle.duckdb"))
    try:
        shared_digest = Digest("sha256:file", "5" * 64)
        original = Artifact(
            key="facet_artifact",
            uri="outputs://facet.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "original"},
        )
        db.sync_artifact(original)
        schema = ArtifactSchema(
            id="schema_hash_facet",
            summary_json={"fields": []},
        )
        db.persist_artifact_schema(
            schema,
            fields=[],
            relations=[],
            observation=ArtifactSchemaObservation(
                artifact_id=original.id,
                schema_id=schema.id,
                source="file",
            ),
        )
        artifact_id = original.id

        artifact = Artifact(
            id=artifact_id,
            key="facet_artifact",
            uri="outputs://facet.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "updated"},
        )
        facet = ArtifactFacet(
            id="facet_hash",
            namespace="activitysim",
            schema_name="FacetSchema",
            schema_version="1",
            facet_json={"label": "demo"},
        )

        db.persist_artifact_facet_bundle(
            artifact=artifact,
            facet=facet,
            meta_updates={"facet_applied": True},
            kv_rows=[
                ArtifactKV(
                    artifact_id=artifact_id,
                    facet_id=facet.id,
                    key_path="marker",
                    value_type="str",
                    value_str="updated",
                )
            ],
        )

        persisted = db.get_artifact(artifact_id)
        assert persisted is not None
        assert persisted.meta["marker"] == "updated"
        assert persisted.meta["facet_applied"] is True
    finally:
        db.close()


def test_sync_artifact_with_facet_bundle_updates_meta_with_dependents(tmp_path):
    db = DatabaseManager(str(tmp_path / "artifact_sync_facet_bundle.duckdb"))
    try:
        shared_digest = Digest("sha256:file", "6" * 64)
        original = Artifact(
            key="sync_facet_artifact",
            uri="outputs://sync_facet_artifact.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "original"},
        )
        db.sync_artifact(original)
        schema = ArtifactSchema(
            id="schema_hash_sync_facet_artifact",
            summary_json={"fields": []},
        )
        db.persist_artifact_schema(
            schema,
            fields=[],
            relations=[],
            observation=ArtifactSchemaObservation(
                artifact_id=original.id,
                schema_id=schema.id,
                source="file",
            ),
        )
        artifact_id = original.id

        artifact = Artifact(
            id=artifact_id,
            key="sync_facet_artifact",
            uri="outputs://sync_facet_artifact.csv",
            driver="csv",
            digest=shared_digest,
            run_id="run_a",
            meta={"marker": "updated"},
        )
        facet = ArtifactFacet(
            id="facet_hash_sync",
            namespace="activitysim",
            schema_name="FacetSchema",
            schema_version="1",
            facet_json={"label": "demo"},
        )

        db.sync_artifact_with_facet_bundle(
            artifact=artifact,
            run_id="run_a",
            direction="output",
            facet=facet,
            meta_updates={"facet_applied": True},
            kv_rows=[
                ArtifactKV(
                    artifact_id=artifact_id,
                    facet_id=facet.id,
                    key_path="marker",
                    value_type="str",
                    value_str="updated",
                )
            ],
        )

        persisted = db.get_artifact(artifact_id)
        assert persisted is not None
        assert persisted.meta["marker"] == "updated"
        assert persisted.meta["facet_applied"] is True
    finally:
        db.close()


def test_artifact_deepcopy_and_copy_on_transient_instance():
    tracker = MagicMock()
    art = (
        Artifact(
            key="transient",
            uri="outputs://transient.csv",
            driver="csv",
            digest=Digest("sha256:file", "7" * 64),
            meta={"nested": {"inner_key": "inner_val"}, "list": [1, 2, 3]},
        )
        .with_abs_path("/tmp/transient.csv")
        .with_resolver(tracker)
    )

    deep_cloned = copy.deepcopy(art)
    assert deep_cloned is not art
    assert deep_cloned == art
    assert deep_cloned.key == art.key
    assert deep_cloned.uri == art.uri
    assert deep_cloned.digest == art.digest
    assert deep_cloned.abs_path == "/tmp/transient.csv"
    assert deep_cloned.resolver is tracker

    with pytest.raises(TypeError):
        deep_cloned.meta["nested"]["inner_key"] = "modified"
    with pytest.raises(AttributeError):
        deep_cloned.meta["list"].append(4)
    with pytest.raises(TypeError):
        deep_cloned.meta["list"][0] = 99  # type: ignore[index]
    assert art.meta["nested"]["inner_key"] == "inner_val"
    assert art.meta["list"] == [1, 2, 3]

    shallow_cloned = copy.copy(art)
    assert shallow_cloned is not art
    assert shallow_cloned.key == art.key
    assert shallow_cloned.abs_path == "/tmp/transient.csv"
    assert shallow_cloned.resolver is tracker


def test_artifact_deepcopy_avoids_object_dereferenced_error_after_gc(tmp_path):
    db = DatabaseManager(str(tmp_path / "deepcopy_test.duckdb"))
    try:
        tracker = MagicMock()
        art = (
            Artifact(
                key="persisted_art",
                uri="outputs://persisted.csv",
                driver="csv",
                digest=Digest("sha256:file", "8" * 64),
                run_id="run_1",
                meta={"initial": "value", "nested": {"k": "v"}},
            )
            .with_resolver(tracker)
            .with_abs_path("/tmp/abs/persisted.csv")
        )
        db.sync_artifact(art)

        c_deepcopy = copy.deepcopy(art)
        c_copy = copy.copy(art)

        art_id = art.id
        del art
        gc.collect()

        for clone in [c_deepcopy, c_copy]:
            assert clone.abs_path == "/tmp/abs/persisted.csv"
            assert clone.resolver is tracker
            updated_clone = clone.with_meta({"updated": "via_with_meta"}, merge=False)
            assert dict(updated_clone.meta) == {"updated": "via_with_meta"}

        db.update_artifact_meta(
            c_deepcopy, {"db_update": "from_deepcopy"}, raise_on_error=True
        )
        persisted = db.get_artifact(art_id)
        assert persisted is not None
        assert persisted.meta["db_update"] == "from_deepcopy"
    finally:
        db.close()


def test_artifact_deepcopy_on_db_loaded_instance(tmp_path):
    db = DatabaseManager(str(tmp_path / "uninitialized_private_test.duckdb"))
    try:
        tracker = MagicMock()
        art = Artifact(
            key="db_loaded_art",
            uri="outputs://db_loaded.csv",
            driver="csv",
            digest=Digest("sha256:file", "9" * 64),
            run_id="run_loaded",
            meta={"step": 1},
        )
        db.sync_artifact(art)
        art_id = art.id

        loaded = db.get_artifact(art_id)
        assert loaded is not None

        loaded = loaded.with_resolver(tracker).with_abs_path("/path/to/loaded.csv")
        cloned = copy.deepcopy(loaded)

        del loaded
        gc.collect()

        assert cloned.abs_path == "/path/to/loaded.csv"
        assert cloned.resolver is tracker
        updated = cloned.with_meta({"step": 2}, merge=False)
        assert dict(updated.meta) == {"step": 2}
    finally:
        db.close()


def test_artifact_deepcopy_memo_identity_preservation():
    art = Artifact(
        key="memo_test",
        uri="outputs://memo.csv",
        driver="csv",
        digest=Digest("sha256:file", "a" * 64),
        meta={"key": "val"},
    )
    container = [art, art]
    cloned_container = copy.deepcopy(container)

    assert cloned_container[0] is not art
    assert cloned_container[0] is cloned_container[1]
    assert cloned_container[0].key == "memo_test"


def test_artifact_replace_updates_and_preserves_runtime_bindings(tmp_path):
    tracker = MagicMock()
    orig_digest = Digest("sha256:file", "b" * 64)
    art = (
        Artifact(
            key="original_key",
            uri="outputs://original.csv",
            driver="csv",
            digest=orig_digest,
            meta={"orig": 1},
        )
        .with_abs_path("/path/to/original.csv")
        .with_resolver(tracker)
    )

    cloned = replace(
        art,
        key="updated_key",
        driver="parquet",
        meta={"orig": 2},
    )
    gc.collect()

    assert cloned.key == "updated_key"
    assert cloned.driver == "parquet"
    assert cloned.digest == orig_digest
    assert dict(cloned.meta) == {"orig": 2}
    assert cloned.abs_path == "/path/to/original.csv"
    assert cloned.resolver is tracker

    db = DatabaseManager(str(tmp_path / "replace_update_merge.duckdb"))
    try:
        db.sync_artifacts(artifacts=[art], run_id="run_out", direction="output")
        db.sync_artifacts(artifacts=[cloned], run_id="run_in", direction="input")
        persisted = db.get_artifact(art.id)
        assert persisted is not None
        assert persisted.driver == "parquet"
    finally:
        db.close()


def test_artifact_abs_path_is_immutable_and_with_abs_path_accepts_none_and_path():
    art = Artifact(
        key="path_test",
        uri="outputs://path.csv",
        driver="csv",
    )
    with pytest.raises((AttributeError, FrozenInstanceError)):
        art.abs_path = "/test/path"  # type: ignore[misc]

    with_str = art.with_abs_path("/test/path")
    assert with_str.abs_path == "/test/path"

    with_none = with_str.with_abs_path(None)
    assert with_none.abs_path is None

    with_path = art.with_abs_path(Path("/data/runs/output.parquet"))
    assert with_path.abs_path == "/data/runs/output.parquet"
    assert copy.deepcopy(with_path).abs_path == "/data/runs/output.parquet"


def test_artifact_tracker_property_and_ensure_local_path(
    tracker, tmp_path: Path
) -> None:
    import consist
    from consist.core.context import use_tracker

    archive_dir = tmp_path / "archive"
    out_file = tracker.run_dir / "outputs" / "households.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_bytes(b"households-v1")

    with tracker.start_run("produce_households", "urbansim"):
        artifact = tracker.log_artifact(out_file, key="households", direction="output")

    assert artifact.tracker is tracker

    unbound = Artifact(
        key="unbound",
        uri="inputs://unbound.parquet",
        driver="parquet",
    )
    assert unbound.tracker is None
    with use_tracker(tracker):
        assert unbound.tracker is tracker

    resolved_existing = artifact.ensure_local_path()
    assert resolved_existing == out_file.resolve()
    assert consist.ensure_local_path(artifact) == out_file.resolve()

    tracker.archive_artifact(artifact, archive_dir, mode="copy")
    out_file.unlink()
    assert not out_file.exists()

    recovered_path = artifact.ensure_local_path()
    assert recovered_path == out_file.resolve()
    assert out_file.read_bytes() == b"households-v1"

    custom_root = tmp_path / "custom_target"
    out_file.unlink()
    custom_path = consist.ensure_local_path(artifact, target_root=custom_root)
    assert custom_path == (custom_root / "outputs" / "households.parquet").resolve()
    assert custom_path.read_bytes() == b"households-v1"

    (archive_dir / "outputs" / "households.parquet").unlink()
    custom_path.unlink()
    artifact = artifact.with_abs_path(None)
    tracker._record_artifact_abs_path_override(artifact, None)

    assert artifact.ensure_local_path(on_missing="warn") is None
    with pytest.raises(FileNotFoundError):
        artifact.ensure_local_path(on_missing="raise")

    elsewhere_file = tmp_path / "elsewhere.parquet"
    elsewhere_file.write_bytes(b"elsewhere")
    detached = Artifact(
        key="detached",
        uri=str(elsewhere_file),
        driver="parquet",
    )
    assert detached.ensure_local_path() == elsewhere_file
    assert (
        detached.ensure_local_path(
            target_root=tmp_path / "other_target", on_missing="warn"
        )
        is None
    )


def test_artifact_meta_dict_returns_deep_unfrozen_json_serializable_copy() -> None:
    import json

    art = Artifact(
        key="nested_meta",
        uri="workspace://nested.parquet",
        driver="parquet",
        meta={
            "nested": {"a": 1, "items": [{"b": 2}, (3, 4)]},
            "tags": ["x", "y"],
        },
    )

    unfrozen = art.meta_dict()
    assert isinstance(unfrozen, dict)
    assert isinstance(unfrozen["nested"], dict)
    assert isinstance(unfrozen["nested"]["items"], list)
    assert isinstance(unfrozen["nested"]["items"][0], dict)
    assert isinstance(unfrozen["nested"]["items"][1], list)
    assert isinstance(unfrozen["tags"], list)

    dumped = json.dumps(unfrozen)
    assert json.loads(dumped) == {
        "nested": {"a": 1, "items": [{"b": 2}, [3, 4]]},
        "tags": ["x", "y"],
    }

    unfrozen["nested"]["a"] = 999
    unfrozen["tags"].append("z")
    assert art.meta["nested"]["a"] == 1
    assert list(art.meta["tags"]) == ["x", "y"]
