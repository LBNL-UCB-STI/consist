import copy
import gc
import logging
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from consist import is_spatial_artifact
from consist.core.artifacts import ArtifactManager, _clone_artifact_for_meta_update
from consist.core.identity import IdentityManager
from consist.core.persistence import (
    DatabaseManager,
    _clone_artifact,
    _clone_artifact_content,
)
from consist.models.artifact import (
    Artifact,
    ArtifactContent,
    get_tracker_ref,
    set_tracker_ref,
)
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_schema import ArtifactSchema
from consist.models.artifact_schema import ArtifactSchemaObservation
from consist.models.artifact_kv import ArtifactKV


@pytest.fixture
def mock_tracker():
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity.compute_file_checksum.return_value = "mock_hash"
    tracker.db = MagicMock()
    tracker.db.get_or_create_artifact_content.return_value = ArtifactContent(
        content_hash="mock_hash", driver="csv"
    )
    return tracker


def test_create_artifact_stateless(mock_tracker):
    manager = ArtifactManager(mock_tracker)

    art = manager.create_artifact(path="/tmp/test.csv", key="test", run_id="run_ABC")

    assert art.run_id == "run_ABC"
    assert art.key == "test"
    assert art.hash == "mock_hash"


def test_create_artifact_keeps_parquet_inference_non_spatial(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.compute_file_checksum.return_value = "mock_hash"
    tracker.db = MagicMock()
    tracker.db.get_or_create_artifact_content.return_value = ArtifactContent(
        content_hash="mock_hash", driver="parquet"
    )

    path = tmp_path / "tiny.parquet"
    path.write_bytes(b"not a real parquet file")

    artifact = ArtifactManager(tracker).create_artifact(path=path, key="tiny")

    assert artifact.driver == "parquet"
    assert is_spatial_artifact(artifact) is False


def test_create_artifact_uses_precomputed_hash(mock_tracker):
    manager = ArtifactManager(mock_tracker)

    art = manager.create_artifact(
        path="/tmp/test.csv",
        key="test",
        run_id="run_ABC",
        content_hash="precomputed_hash",
    )

    assert art.hash == "precomputed_hash"
    mock_tracker.identity.compute_file_checksum.assert_not_called()


def test_create_artifact_dir_uses_precomputed_hash(mock_tracker, tmp_path):
    manager = ArtifactManager(mock_tracker)
    data_dir = tmp_path / "data_dir"
    data_dir.mkdir()

    art = manager.create_artifact(
        path=str(data_dir),
        key="dir_artifact",
        run_id="run_ABC",
        content_hash="precomputed_hash",
    )

    assert art.hash == "precomputed_hash"
    mock_tracker.identity.compute_file_checksum.assert_not_called()


def test_create_artifact_rejects_mismatched_validated_hash(mock_tracker):
    manager = ArtifactManager(mock_tracker)

    with pytest.raises(ValueError, match="content_hash does not match"):
        manager.create_artifact(
            path="/tmp/test.csv",
            key="test",
            run_id="run_ABC",
            content_hash="different_hash",
            validate_content_hash=True,
        )

    mock_tracker.identity.compute_file_checksum.assert_called()


def test_create_artifact_hashes_once_for_reuse_and_validation(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.compute_file_checksum.return_value = "mock_hash"
    tracker.db.find_latest_artifact_at_uri.return_value = None
    tracker.db.find_latest_artifact_by_hash.return_value = None
    tracker.db.get_or_create_artifact_content.return_value = ArtifactContent(
        content_hash="mock_hash", driver="csv"
    )

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

    assert art.hash == "mock_hash"
    assert tracker.identity.compute_file_checksum.call_count == 1


def test_create_artifact_does_not_dedupe_fast_file_observations(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.hashing_strategy = "fast"
    tracker.identity.compute_file_checksum.return_value = "f" * 64
    tracker.db = MagicMock()

    source = tmp_path / "fast.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    artifact = ArtifactManager(tracker).create_artifact(
        path=source,
        key="fast_input",
        run_id="run_a",
    )

    assert artifact.content_id is None
    assert artifact.meta["observation_identity"] == f"stat-v1:file:{'f' * 64}"
    tracker.db.get_or_create_artifact_content.assert_not_called()


def test_create_artifact_persists_typed_full_file_content_identity(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {}
    tracker.identity.hashing_strategy = "full"
    digest = "a" * 64
    tracker.identity.compute_file_checksum.return_value = digest
    content_identity = f"sha256:file:{digest}"
    content_row = ArtifactContent(content_hash=content_identity, driver="csv")
    tracker.db = MagicMock()
    tracker.db.get_or_create_artifact_content.return_value = content_row

    source = tmp_path / "full.csv"
    source.write_text("value\n1\n", encoding="utf-8")

    artifact = ArtifactManager(tracker).create_artifact(
        path=source,
        key="full_input",
        run_id="run_a",
    )

    assert artifact.meta["content_identity"] == content_identity
    assert artifact.content_id == content_row.id
    tracker.db.get_or_create_artifact_content.assert_called_once_with(
        content_hash=content_identity,
        driver="csv",
    )


def test_create_artifact_records_fast_directory_observation_without_content_id(
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

    assert artifact.content_id is None
    assert artifact.meta["observation_identity"].startswith("stat-v2:directory:")
    tracker.db.get_or_create_artifact_content.assert_not_called()


def test_create_artifacts_share_content_id_when_hashes_match(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {"outputs": str(tmp_path)}
    tracker.identity.hashing_strategy = "full"
    digest = "b" * 64
    tracker.identity.compute_file_checksum.return_value = digest
    content_identity = f"sha256:file:{digest}"
    content_row = ArtifactContent(content_hash=content_identity, driver="parquet")
    tracker.db = MagicMock()
    tracker.db.get_or_create_artifact_content.return_value = content_row

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

    assert art_a.id != art_b.id
    assert art_a.run_id == "run_a"
    assert art_b.run_id == "run_b"
    assert art_a.content_id == art_b.content_id == content_row.id


def test_legacy_artifact_hash_does_not_attach_content_id():
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.db = MagicMock()

    manager = ArtifactManager(tracker)

    existing = Artifact(
        key="k",
        container_uri="inputs://a.parquet",
        driver="parquet",
        hash="hash_from_obj",
        run_id="run_x",
    )

    out = manager.create_artifact(path=existing, run_id="run_x")

    assert out.content_id is None
    tracker.db.get_or_create_artifact_content.assert_not_called()


def test_legacy_detached_artifact_remains_without_content_id(tmp_path, caplog):
    db = DatabaseManager(str(tmp_path / "artifact_replay.db"))

    with db.session_scope() as session:
        original = Artifact(
            key="geoid_to_zone",
            container_uri="outputs://zones.parquet",
            driver="parquet",
            hash="shared_hash",
            run_id="run_a",
        )
        session.add(original)
        session.commit()
        artifact_id = original.id

    detached = db.get_artifact(artifact_id)
    assert detached is not None
    replayed = deepcopy(detached)

    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity.compute_file_checksum.return_value = "shared_hash"
    tracker.db = db

    manager = ArtifactManager(tracker)
    with caplog.at_level(logging.WARNING):
        out = manager.create_artifact(path=replayed, run_id="run_b")

    content = db.find_artifact_content(content_hash="shared_hash", driver="parquet")

    assert out is not replayed
    assert out.id == replayed.id
    assert out.hash == replayed.hash == "shared_hash"
    assert content is None
    assert out.content_id is None
    assert "Failed to record artifact content identity" not in caplog.text
    assert out.parent_artifact_id is None


def test_legacy_detached_artifact_driver_override_does_not_create_content_identity(
    tmp_path, caplog
):
    db = DatabaseManager(str(tmp_path / "artifact_driver_override.db"))

    with db.session_scope() as session:
        original = Artifact(
            key="geoid_to_zone",
            container_uri="outputs://zones.parquet",
            driver="parquet",
            hash="shared_hash",
            run_id="run_a",
        )
        session.add(original)
        session.commit()
        artifact_id = original.id

    detached = db.get_artifact(artifact_id)
    assert detached is not None
    replayed = deepcopy(detached)

    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity.compute_file_checksum.return_value = "shared_hash"
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
    content = db.find_artifact_content(content_hash="shared_hash", driver="csv")

    assert out is not replayed
    assert out.id == replayed.id
    assert replayed.driver == "parquet"
    assert persisted is not None
    assert persisted.driver == "csv"
    assert content is None
    assert persisted.content_id is None
    assert replayed.content_id is None
    assert "Failed to record artifact content identity" not in caplog.text


def test_create_artifact_preserves_parent_artifact_id_on_detached_clone(tmp_path):
    db = DatabaseManager(str(tmp_path / "artifact_parent_clone.db"))
    parent_id = Artifact(
        key="container",
        container_uri="outputs://container.h5",
        driver="h5",
        run_id="run_parent",
    ).id

    with db.session_scope() as session:
        original = Artifact(
            key="geoid_to_zone",
            container_uri="outputs://zones.h5",
            table_path="/zones",
            driver="h5_table",
            hash="shared_hash",
            parent_artifact_id=parent_id,
            run_id="run_a",
            meta={"parent_id": str(parent_id)},
        )
        session.add(original)
        session.commit()
        artifact_id = original.id

    detached = db.get_artifact(artifact_id)
    assert detached is not None
    replayed = deepcopy(detached)

    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity.compute_file_checksum.return_value = "shared_hash"
    tracker.db = db

    manager = ArtifactManager(tracker)
    out = manager.create_artifact(path=replayed, run_id="run_b")

    assert out.parent_artifact_id == parent_id
    assert out.meta["parent_id"] == str(parent_id)


def test_create_artifact_clones_reused_input_artifact_before_mutation(tmp_path, caplog):
    db = DatabaseManager(str(tmp_path / "artifact_input_reuse.db"))

    with db.session_scope() as session:
        original = Artifact(
            key="geoid_to_zone",
            container_uri="inputs://zones.parquet",
            driver="parquet",
            hash="shared_hash",
            run_id="run_a",
        )
        session.add(original)
        session.commit()
        artifact_id = original.id

    detached = db.get_artifact(artifact_id)
    assert detached is not None
    reused_parent = deepcopy(detached)

    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: str(tmp_path / uri.split("://", 1)[1])
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity.compute_file_checksum.return_value = "shared_hash"
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

    content = db.find_artifact_content(content_hash="shared_hash", driver="parquet")

    assert out is not reused_parent
    assert out.id == reused_parent.id
    assert reused_parent.content_id is None
    assert content is None
    assert out.content_id is None
    assert "Failed to record artifact content identity" not in caplog.text


def test_sync_artifacts_preserves_existing_content_id_with_schema_dependents(
    tmp_path,
):
    db = DatabaseManager(str(tmp_path / "artifact_dependent_sync.db"))

    with db.session_scope() as session:
        original = Artifact(
            key="dependent_artifact",
            container_uri="outputs://dependent.csv",
            driver="csv",
            hash="shared_hash",
            run_id="run_a",
            meta={"marker": "original"},
        )
        schema = ArtifactSchema(
            id="schema_hash",
            summary_json={"fields": []},
        )
        session.add_all(
            [
                original,
                schema,
                ArtifactSchemaObservation(
                    artifact_id=original.id,
                    schema_id=schema.id,
                    source="file",
                ),
            ]
        )
        session.commit()
        artifact_id = original.id

    with db.session_scope() as session:
        content = ArtifactContent(content_hash="shared_hash", driver="csv")
        session.add(content)
        session.commit()
        content_id = content.id

    merged = Artifact(
        id=artifact_id,
        key="dependent_artifact",
        container_uri="outputs://dependent.csv",
        driver="csv",
        hash="shared_hash",
        content_id=content_id,
        run_id="run_a",
        meta={"marker": "updated"},
    )

    db.sync_artifacts(artifacts=[merged], run_id="run_b", direction="output")

    persisted = db.get_artifact(artifact_id)
    assert persisted is not None
    assert persisted.content_id is None
    assert persisted.meta["marker"] == "updated"


def test_persist_artifact_schema_observation_bundle_preserves_existing_content_id(
    tmp_path,
):
    db = DatabaseManager(str(tmp_path / "artifact_schema_observation_bundle.db"))

    with db.session_scope() as session:
        original = Artifact(
            key="bundle_artifact",
            container_uri="outputs://bundle.csv",
            driver="csv",
            hash="shared_hash",
            run_id="run_a",
            meta={"marker": "original"},
        )
        schema = ArtifactSchema(
            id="schema_hash_bundle",
            summary_json={"fields": []},
        )
        session.add_all(
            [
                original,
                schema,
                ArtifactSchemaObservation(
                    artifact_id=original.id,
                    schema_id=schema.id,
                    source="file",
                ),
            ]
        )
        session.commit()
        artifact_id = original.id

    with db.session_scope() as session:
        content = ArtifactContent(content_hash="shared_hash", driver="csv")
        session.add(content)
        session.commit()
        content_id = content.id

    artifact = Artifact(
        id=artifact_id,
        key="bundle_artifact",
        container_uri="outputs://bundle.csv",
        driver="csv",
        hash="shared_hash",
        content_id=content_id,
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
    assert persisted.content_id is None
    assert persisted.meta["marker"] == "updated"
    assert persisted.meta["profiled"] is True


def test_persist_artifact_schema_profile_preserves_existing_content_id(
    tmp_path,
):
    db = DatabaseManager(str(tmp_path / "artifact_schema_profile.db"))

    with db.session_scope() as session:
        original = Artifact(
            key="profile_artifact",
            container_uri="outputs://profile.csv",
            driver="csv",
            hash="shared_hash",
            run_id="run_a",
            meta={"marker": "original"},
        )
        schema = ArtifactSchema(
            id="schema_hash_profile",
            summary_json={"fields": []},
        )
        session.add_all(
            [
                original,
                schema,
                ArtifactSchemaObservation(
                    artifact_id=original.id,
                    schema_id=schema.id,
                    source="file",
                ),
            ]
        )
        session.commit()
        artifact_id = original.id

    with db.session_scope() as session:
        content = ArtifactContent(content_hash="shared_hash", driver="csv")
        session.add(content)
        session.commit()
        content_id = content.id

    artifact = Artifact(
        id=artifact_id,
        key="profile_artifact",
        container_uri="outputs://profile.csv",
        driver="csv",
        hash="shared_hash",
        content_id=content_id,
        run_id="run_a",
        meta={"marker": "updated"},
    )
    schema = ArtifactSchema(
        id="schema_hash_profile",
        summary_json={"fields": []},
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
    assert persisted.content_id is None
    assert persisted.meta["marker"] == "updated"
    assert persisted.meta["profiled"] is True


def test_update_artifact_meta_preserves_existing_content_id_with_dependents(
    tmp_path,
):
    db = DatabaseManager(str(tmp_path / "artifact_update_meta.db"))

    with db.session_scope() as session:
        original = Artifact(
            key="meta_artifact",
            container_uri="outputs://meta.csv",
            driver="csv",
            hash="shared_hash",
            run_id="run_a",
            meta={"marker": "original"},
        )
        schema = ArtifactSchema(
            id="schema_hash_meta",
            summary_json={"fields": []},
        )
        session.add_all(
            [
                original,
                schema,
                ArtifactSchemaObservation(
                    artifact_id=original.id,
                    schema_id=schema.id,
                    source="file",
                ),
            ]
        )
        session.commit()
        artifact_id = original.id

    with db.session_scope() as session:
        content = ArtifactContent(content_hash="shared_hash", driver="csv")
        session.add(content)
        session.commit()
        content_id = content.id

    artifact = Artifact(
        id=artifact_id,
        key="meta_artifact",
        container_uri="outputs://meta.csv",
        driver="csv",
        hash="shared_hash",
        content_id=content_id,
        run_id="run_a",
        meta={"marker": "updated"},
    )

    updated = db.update_artifact_meta(artifact, {"profiled": True})

    persisted = db.get_artifact(artifact_id)
    assert updated is True
    assert persisted is not None
    assert persisted.content_id is None
    assert persisted.meta["marker"] == "updated"
    assert persisted.meta["profiled"] is True


def test_persist_artifact_facet_bundle_preserves_existing_content_id(
    tmp_path,
):
    db = DatabaseManager(str(tmp_path / "artifact_facet_bundle.db"))

    with db.session_scope() as session:
        original = Artifact(
            key="facet_artifact",
            container_uri="outputs://facet.csv",
            driver="csv",
            hash="shared_hash",
            run_id="run_a",
            meta={"marker": "original"},
        )
        schema = ArtifactSchema(
            id="schema_hash_facet",
            summary_json={"fields": []},
        )
        session.add_all(
            [
                original,
                schema,
                ArtifactSchemaObservation(
                    artifact_id=original.id,
                    schema_id=schema.id,
                    source="file",
                ),
            ]
        )
        session.commit()
        artifact_id = original.id

    with db.session_scope() as session:
        content = ArtifactContent(content_hash="shared_hash", driver="csv")
        session.add(content)
        session.commit()
        content_id = content.id

    artifact = Artifact(
        id=artifact_id,
        key="facet_artifact",
        container_uri="outputs://facet.csv",
        driver="csv",
        hash="shared_hash",
        content_id=content_id,
        run_id="run_a",
        meta={"marker": "updated"},
    )
    facet = ArtifactFacet(
        id="facet_hash",
        namespace="activitysim",
        schema_name="FacetSchema",
        schema_version=1,
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
    assert persisted.content_id is None
    assert persisted.meta["marker"] == "updated"
    assert persisted.meta["facet_applied"] is True


def test_sync_artifact_preserves_existing_nonnull_content_id_with_dependents(
    tmp_path,
):
    db = DatabaseManager(str(tmp_path / "artifact_sync_artifact.db"))

    with db.session_scope() as session:
        content_a = ArtifactContent(content_hash="content_a", driver="csv")
        content_b = ArtifactContent(content_hash="content_b", driver="csv")
        original = Artifact(
            key="sync_artifact",
            container_uri="outputs://sync_artifact.csv",
            driver="csv",
            hash="shared_hash",
            content_id=content_a.id,
            run_id="run_a",
            meta={"marker": "original"},
        )
        schema = ArtifactSchema(
            id="schema_hash_sync_artifact",
            summary_json={"fields": []},
        )
        session.add_all(
            [
                content_a,
                content_b,
                original,
                schema,
                ArtifactSchemaObservation(
                    artifact_id=original.id,
                    schema_id=schema.id,
                    source="file",
                ),
            ]
        )
        session.commit()
        artifact_id = original.id
        content_a_id = content_a.id
        content_b_id = content_b.id

    artifact = Artifact(
        id=artifact_id,
        key="sync_artifact",
        container_uri="outputs://sync_artifact.csv",
        driver="csv",
        hash="shared_hash",
        content_id=content_b_id,
        run_id="run_a",
        meta={"marker": "updated"},
    )

    db.sync_artifact(artifact, run_id="run_a", direction="output")

    persisted = db.get_artifact(artifact_id)
    assert persisted is not None
    assert persisted.content_id == content_a_id
    assert persisted.meta["marker"] == "updated"


def test_sync_artifact_with_facet_bundle_preserves_existing_nonnull_content_id_with_dependents(
    tmp_path,
):
    db = DatabaseManager(str(tmp_path / "artifact_sync_facet_bundle.db"))

    with db.session_scope() as session:
        content_a = ArtifactContent(content_hash="content_a", driver="csv")
        content_b = ArtifactContent(content_hash="content_b", driver="csv")
        original = Artifact(
            key="sync_facet_artifact",
            container_uri="outputs://sync_facet_artifact.csv",
            driver="csv",
            hash="shared_hash",
            content_id=content_a.id,
            run_id="run_a",
            meta={"marker": "original"},
        )
        schema = ArtifactSchema(
            id="schema_hash_sync_facet_artifact",
            summary_json={"fields": []},
        )
        session.add_all(
            [
                content_a,
                content_b,
                original,
                schema,
                ArtifactSchemaObservation(
                    artifact_id=original.id,
                    schema_id=schema.id,
                    source="file",
                ),
            ]
        )
        session.commit()
        artifact_id = original.id
        content_a_id = content_a.id
        content_b_id = content_b.id

    artifact = Artifact(
        id=artifact_id,
        key="sync_facet_artifact",
        container_uri="outputs://sync_facet_artifact.csv",
        driver="csv",
        hash="shared_hash",
        content_id=content_b_id,
        run_id="run_a",
        meta={"marker": "updated"},
    )
    facet = ArtifactFacet(
        id="facet_hash_sync",
        namespace="activitysim",
        schema_name="FacetSchema",
        schema_version=1,
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
    assert persisted.content_id == content_a_id
    assert persisted.meta["marker"] == "updated"
    assert persisted.meta["facet_applied"] is True


def _deref_tracker(artifact: Artifact):
    ref = get_tracker_ref(artifact)
    assert ref is not None
    return ref()


def test_artifact_deepcopy_and_copy_on_transient_instance():
    tracker = MagicMock()
    art = Artifact(
        key="transient",
        container_uri="outputs://transient.csv",
        driver="csv",
        hash="hash123",
        meta={"nested": {"inner_key": "inner_val"}, "list": [1, 2, 3]},
    )
    art.abs_path = "/tmp/transient.csv"
    set_tracker_ref(art, tracker)

    # Deepcopy via standard copy module
    deep_cloned = copy.deepcopy(art)
    assert deep_cloned is not art
    assert deep_cloned.key == art.key
    assert deep_cloned.container_uri == art.container_uri
    assert deep_cloned.hash == art.hash
    assert deep_cloned.abs_path == "/tmp/transient.csv"
    assert _deref_tracker(deep_cloned) is tracker
    assert deep_cloned.__pydantic_fields_set__ == art.__pydantic_fields_set__

    # Deepcopy isolation: mutating nested meta does not affect original
    deep_cloned.meta["nested"]["inner_key"] = "modified"
    deep_cloned.meta["list"].append(4)
    assert art.meta["nested"]["inner_key"] == "inner_val"
    assert art.meta["list"] == [1, 2, 3]

    # Model_copy(deep=True)
    model_deep = art.model_copy(deep=True)
    assert model_deep is not art
    assert model_deep.abs_path == "/tmp/transient.csv"
    assert _deref_tracker(model_deep) is tracker
    model_deep.meta["nested"]["inner_key"] = "model_modified"
    assert art.meta["nested"]["inner_key"] == "inner_val"

    # Shallow copy via copy.copy
    shallow_cloned = copy.copy(art)
    assert shallow_cloned is not art
    assert shallow_cloned.key == art.key
    assert shallow_cloned.abs_path == "/tmp/transient.csv"
    assert _deref_tracker(shallow_cloned) is tracker

    # Shallow copy via model_copy(deep=False)
    model_shallow = art.model_copy(deep=False)
    assert model_shallow is not art
    assert model_shallow.key == art.key
    assert model_shallow.abs_path == "/tmp/transient.csv"
    assert _deref_tracker(model_shallow) is tracker


def test_artifact_deepcopy_avoids_object_dereferenced_error_after_gc(tmp_path):
    db = DatabaseManager(str(tmp_path / "deepcopy_test.duckdb"))
    tracker = MagicMock()

    with db.session_scope() as session:
        art = Artifact(
            key="persisted_art",
            container_uri="outputs://persisted.csv",
            driver="csv",
            hash="persisted_hash",
            run_id="run_1",
            meta={"initial": "value", "nested": {"k": "v"}},
        )
        session.add(art)
        session.commit()
        session.refresh(art)

        set_tracker_ref(art, tracker)
        art.abs_path = "/tmp/abs/persisted.csv"

        # Clone using all four copy variants
        c_deepcopy = copy.deepcopy(art)
        c_model_deep = art.model_copy(deep=True)
        c_copy = copy.copy(art)
        c_model_shallow = art.model_copy(deep=False)

    # Simulate original entity and session going out of scope and getting garbage-collected
    art_id = art.id
    del art
    del session
    gc.collect()

    # Verify that mutating ORM-instrumented attributes does NOT raise ObjectDereferencedError
    for clone in [c_deepcopy, c_model_deep, c_copy, c_model_shallow]:
        assert clone.abs_path == "/tmp/abs/persisted.csv"
        assert _deref_tracker(clone) is tracker

        # Mutate meta directly on the clone
        clone.meta = {"updated": "directly"}
        assert clone.meta == {"updated": "directly"}

    # Verify that db.update_artifact_meta succeeds and commits cleanly
    db.update_artifact_meta(
        c_deepcopy, {"db_update": "from_deepcopy"}, raise_on_error=True
    )
    persisted = db.get_artifact(art_id)
    assert persisted is not None
    assert persisted.meta["db_update"] == "from_deepcopy"

    # Verify model_copy(deep=True) also works with db.update_artifact_meta
    db.update_artifact_meta(
        c_model_deep, {"db_update": "from_model_deep"}, raise_on_error=True
    )
    persisted = db.get_artifact(art_id)
    assert persisted is not None
    assert persisted.meta["db_update"] == "from_model_deep"


def test_artifact_deepcopy_on_db_loaded_instance_with_uninitialized_pydantic_private(
    tmp_path,
):
    db = DatabaseManager(str(tmp_path / "uninitialized_private_test.duckdb"))
    tracker = MagicMock()

    with db.session_scope() as session:
        art = Artifact(
            key="db_loaded_art",
            container_uri="outputs://db_loaded.csv",
            driver="csv",
            hash="loaded_hash",
            run_id="run_loaded",
            meta={"step": 1},
        )
        session.add(art)
        session.commit()
        art_id = art.id

    # Query fresh instance directly from DB (bypasses __init__, so __pydantic_private__ may be None)
    loaded = db.get_artifact(art_id)
    assert loaded is not None

    set_tracker_ref(loaded, tracker)
    loaded.abs_path = "/path/to/loaded.csv"

    cloned = copy.deepcopy(loaded)
    model_cloned = loaded.model_copy(deep=True)

    del loaded
    gc.collect()

    for c in [cloned, model_cloned]:
        assert c.abs_path == "/path/to/loaded.csv"
        assert _deref_tracker(c) is tracker
        c.meta = {"step": 2}
        assert c.meta == {"step": 2}


def test_artifact_deepcopy_memo_identity_preservation():
    art = Artifact(
        key="memo_test",
        container_uri="outputs://memo.csv",
        driver="csv",
        hash="memo_hash",
        meta={"key": "val"},
    )
    container = [art, art]
    cloned_container = copy.deepcopy(container)

    assert cloned_container[0] is not art
    assert cloned_container[0] is cloned_container[1]
    assert cloned_container[0].key == "memo_test"


def test_artifact_model_copy_with_updates(tmp_path):
    tracker = MagicMock()
    art = Artifact(
        key="original_key",
        container_uri="outputs://original.csv",
        driver="csv",
        hash="original_hash",
        meta={"orig": 1},
    )
    art.abs_path = "/path/to/original.csv"
    set_tracker_ref(art, tracker)

    cloned = art.model_copy(
        deep=True,
        update={"key": "updated_key", "driver": "parquet", "meta": {"orig": 2}},
    )
    gc.collect()

    assert cloned.key == "updated_key"
    assert cloned.driver == "parquet"
    assert cloned.hash == "original_hash"
    assert cloned.meta == {"orig": 2}
    assert cloned.abs_path == "/path/to/original.csv"
    assert _deref_tracker(cloned) is tracker
    assert cloned._sa_instance_state.obj() is cloned

    # Verify merging a model_copy(deep=True, update=...) clone into a session where
    # the original artifact row is already persistent does not lose its primary key
    # identity or raise a duplicate primary key constraint error on autoflush.
    db = DatabaseManager(str(tmp_path / "model_copy_update_merge.duckdb"))
    db.sync_artifacts(artifacts=[art], run_id="run_out", direction="output")
    db.sync_artifacts(artifacts=[cloned], run_id="run_in", direction="input")
    persisted = db.get_artifact(art.id)
    assert persisted is not None
    assert persisted.driver == "parquet"


def test_artifact_abs_path_setter_accepts_none():
    art = Artifact(
        key="path_test",
        container_uri="outputs://path.csv",
        driver="csv",
        hash="h",
    )
    art.abs_path = "/test/path"
    assert art.abs_path == "/test/path"

    art.abs_path = None
    assert art.abs_path is None


def test_artifact_content_copy_and_deepcopy(tmp_path):
    db = DatabaseManager(str(tmp_path / "content_test.duckdb"))

    with db.session_scope() as session:
        content = ArtifactContent(
            content_hash="content_hash_123",
            driver="csv",
            meta={"schema": {"columns": ["a", "b"]}},
        )
        session.add(content)
        session.commit()
        session.refresh(content)

        c_deep = copy.deepcopy(content)
        c_model_deep = content.model_copy(deep=True)
        c_shallow = copy.copy(content)
        c_model_shallow = content.model_copy(deep=False)

    del content
    del session
    gc.collect()

    for c in [c_deep, c_model_deep, c_shallow, c_model_shallow]:
        assert c.content_hash == "content_hash_123"
        assert c.driver == "csv"
        # Mutation should not raise ObjectDereferencedError
        c.meta = {"schema": {"columns": ["x"]}}
        assert c.meta == {"schema": {"columns": ["x"]}}

    # Deepcopy isolation
    c_deep.meta["schema"]["columns"].append("y")
    assert c_deep.meta["schema"]["columns"] == ["x", "y"]
    assert c_model_deep.meta["schema"]["columns"] == ["x"]

    # Memo identity preservation
    content_list = [c_deep, c_deep]
    cloned_list = copy.deepcopy(content_list)
    assert cloned_list[0] is not c_deep
    assert cloned_list[0] is cloned_list[1]


def test_cloning_helpers_delegate_safely():
    original = Artifact(
        key="helper_test",
        container_uri="outputs://helper.csv",
        driver="csv",
        hash="helper_hash",
        meta={"nested": {"count": 1}},
    )
    original.abs_path = "/tmp/helper.csv"

    # _clone_artifact_for_meta_update
    cloned_for_meta = _clone_artifact_for_meta_update(original)
    assert cloned_for_meta is not original
    assert cloned_for_meta.abs_path == "/tmp/helper.csv"
    cloned_for_meta.meta["nested"]["count"] = 2
    assert original.meta["nested"]["count"] == 1  # Deepcopy isolation

    # persistence._clone_artifact and _clone_artifact_content
    p_clone = _clone_artifact(original)
    assert p_clone is not original
    p_clone.meta["nested"]["count"] = 4
    assert original.meta["nested"]["count"] == 1

    content_row = ArtifactContent(
        content_hash="h1",
        driver="csv",
        meta={"schema": {"c": 1}},
    )
    pc_clone = _clone_artifact_content(content_row)
    assert pc_clone is not content_row
    pc_clone.meta["schema"]["c"] = 2
    assert content_row.meta["schema"]["c"] == 1


def test_artifact_and_content_copy_with_expired_attributes(tmp_path):
    """Confirm copying expired ORM instances reloads attributes and produces clean detached clones."""
    db = DatabaseManager(str(tmp_path / "expired_test.duckdb"))

    with db.session_scope() as session:
        art = Artifact(
            key="expired_art_key",
            container_uri="outputs://expired.csv",
            driver="csv",
            hash="expired_hash_123",
            meta={"tags": ["fast", "cold"]},
        )
        content = ArtifactContent(
            content_hash="content_expired_hash_456",
            driver="csv",
            meta={"schema": {"columns": ["col_a", "col_b"]}},
        )
        session.add(art)
        session.add(content)
        session.commit()

        # 1. Directly after commit() without refresh(), attributes are expired in session.
        # __dict__ contains only '_sa_instance_state'.
        assert "_sa_instance_state" in art.__dict__
        assert "key" not in art.__dict__
        assert "content_hash" not in content.__dict__

        cloned_art = copy.deepcopy(art)
        shallow_art = copy.copy(art)
        cloned_content = copy.deepcopy(content)
        shallow_content = copy.copy(content)

        for a in [cloned_art, shallow_art]:
            assert a.id == art.id
            assert a.key == "expired_art_key"
            assert a.container_uri == "outputs://expired.csv"
            assert a.driver == "csv"
            assert a.hash == "expired_hash_123"
            assert a.meta == {"tags": ["fast", "cold"]}
            # Mutation on detached clone must not raise ObjectDereferencedError
            a.meta = {"tags": ["updated"]}
            assert a.meta == {"tags": ["updated"]}

        for c in [cloned_content, shallow_content]:
            assert c.content_hash == "content_expired_hash_456"
            assert c.driver == "csv"
            assert c.meta == {"schema": {"columns": ["col_a", "col_b"]}}
            c.meta = {"schema": {"columns": ["col_x"]}}
            assert c.meta == {"schema": {"columns": ["col_x"]}}

        # 2. Test after explicit session.expire()
        session.refresh(art)
        session.refresh(content)
        session.expire(art)
        session.expire(content)
        assert "key" not in art.__dict__
        assert "content_hash" not in content.__dict__

        cloned_art_after_expire = copy.deepcopy(art)
        cloned_content_after_expire = copy.deepcopy(content)

        assert cloned_art_after_expire.key == "expired_art_key"
        assert cloned_art_after_expire.id == art.id
        assert cloned_content_after_expire.content_hash == "content_expired_hash_456"

        cloned_art_after_expire.meta = {"tags": ["expired_mutation"]}
        assert cloned_art_after_expire.meta == {"tags": ["expired_mutation"]}


def test_artifact_deepcopy_circular_reference():
    """Verify that deepcopy preserves instance identity on self-referential graph structures."""
    art = Artifact(
        key="circular_key",
        container_uri="outputs://circular.csv",
        driver="csv",
        hash="h_circ",
    )
    art.meta = {"self": art}

    cloned = copy.deepcopy(art)

    assert cloned is not art
    assert cloned.key == "circular_key"
    assert cloned.meta["self"] is cloned


def test_artifact_copy_preserves_custom_attributes():
    """Verify non-model custom attributes attached to __dict__ are cloned."""
    art = Artifact(
        key="custom_attr_key",
        container_uri="outputs://custom.csv",
        driver="csv",
        hash="h_custom",
    )
    object.__setattr__(art, "extra_metric", {"runtime_sec": 42.5})

    cloned = copy.deepcopy(art)
    assert hasattr(cloned, "extra_metric")
    assert cloned.extra_metric == {"runtime_sec": 42.5}
    assert cloned.extra_metric is not art.extra_metric  # Deep-copied

    shallow = copy.copy(art)
    assert hasattr(shallow, "extra_metric")
    assert shallow.extra_metric == {"runtime_sec": 42.5}
    assert shallow.extra_metric is art.extra_metric  # Shallow-copied


def test_artifact_abs_path_setter_accepts_path_object():
    """Verify abs_path setter accepts pathlib.Path objects and stores normalized string."""
    art = Artifact(
        key="path_test",
        container_uri="outputs://path.csv",
        driver="csv",
        hash="h_path",
    )
    art.abs_path = Path("/data/runs/output.parquet")
    assert art.abs_path == "/data/runs/output.parquet"

    cloned = copy.deepcopy(art)
    assert cloned.abs_path == "/data/runs/output.parquet"
