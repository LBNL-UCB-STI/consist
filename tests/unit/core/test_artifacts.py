import logging
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from consist import is_spatial_artifact
from consist.core.artifacts import ArtifactManager
from consist.core.persistence import DatabaseManager
from consist.models.artifact import Artifact
from consist.models.artifact import ArtifactContent
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


def test_create_artifacts_share_content_id_when_hashes_match(tmp_path):
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.mounts = {"outputs": str(tmp_path)}
    tracker.identity.compute_file_checksum.return_value = "shared_hash"
    content_row = ArtifactContent(content_hash="shared_hash", driver="parquet")
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
        content_hash="shared_hash",
        driver="parquet",
    )
    art_b = manager.create_artifact(
        path=path_b,
        key="shared",
        run_id="run_b",
        content_hash="shared_hash",
        driver="parquet",
    )

    assert art_a.id != art_b.id
    assert art_a.run_id == "run_a"
    assert art_b.run_id == "run_b"
    assert art_a.content_id == art_b.content_id == content_row.id


def test_attach_content_id_uses_artifact_driver_when_arg_missing():
    tracker = MagicMock()
    tracker.resolve_uri = lambda uri: f"/abs/{uri}"
    tracker.fs.virtualize_path = lambda path: f"inputs://{Path(path).name}"
    tracker.identity.compute_file_checksum.return_value = "hash_from_obj"
    content_row = ArtifactContent(content_hash="hash_from_obj", driver="parquet")
    tracker.db = MagicMock()
    tracker.db.get_or_create_artifact_content.return_value = content_row

    manager = ArtifactManager(tracker)

    existing = Artifact(
        key="k",
        container_uri="inputs://a.parquet",
        driver="parquet",
        hash="hash_from_obj",
        run_id="run_x",
    )

    out = manager.create_artifact(path=existing, run_id="run_x")

    assert out.content_id is not None
    tracker.db.get_or_create_artifact_content.assert_called_with(
        content_hash="hash_from_obj", driver="parquet"
    )


def test_attach_content_id_handles_deepcopied_detached_artifact(tmp_path, caplog):
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
    assert content is not None
    assert out.content_id == content.id
    assert "Failed to record artifact content identity" not in caplog.text
    assert out.parent_artifact_id is None


def test_create_artifact_handles_driver_override_on_deepcopied_detached_artifact(
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
    assert content is not None
    assert persisted.content_id == content.id
    assert replayed.content_id != content.id
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
    assert content is not None
    assert out.content_id == content.id
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
