from __future__ import annotations

from pathlib import Path
import uuid

import duckdb
import pytest

from consist.core.persistence import (
    DatabaseManager,
    DatabaseMigrationRequired,
    IncompatibleSchemaError,
)
from consist.models.artifact import Artifact
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation


def test_v2_schema_init_creates_indexes_and_schema_version_idempotently(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "schema_v2.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        version_row = db.fetchone("SELECT version FROM consist_schema")
        assert version_row == (2,)
        assert db.has_table("artifact_content") is False

        indexes = {
            str(row[0])
            for row in db.fetchall(
                "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'artifact'"
            )
        }
        assert {
            "ix_artifact_uri",
            "ix_artifact_digest",
            "ix_artifact_run_id",
            "ix_artifact_parent_artifact_id",
        }.issubset(indexes)

        db.execute("DROP INDEX IF EXISTS ix_artifact_parent_artifact_id")
        db._init_schema_locked(db_uid=None)

        recreated_indexes = {
            str(row[0])
            for row in db.fetchall(
                "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'artifact'"
            )
        }
        assert "ix_artifact_parent_artifact_id" in recreated_indexes
        assert db.fetchall("SELECT version FROM consist_schema") == [(2,)]
    finally:
        db.close()


def test_opening_v1_or_unversioned_db_raises_migration_required(
    tmp_path: Path,
) -> None:
    assert issubclass(DatabaseMigrationRequired, IncompatibleSchemaError)

    v1_path = tmp_path / "v1_store.duckdb"
    con = duckdb.connect(str(v1_path))
    con.execute("CREATE TABLE consist_schema (version INTEGER NOT NULL)")
    con.execute("INSERT INTO consist_schema VALUES (1)")
    con.close()

    with pytest.raises(
        DatabaseMigrationRequired,
        match="Database uses Consist v0.4 schema \\(version=1\\)\\. Run 'consist migrate ",
    ):
        DatabaseManager(str(v1_path))

    legacy_path = tmp_path / "legacy_unversioned.duckdb"
    con_legacy = duckdb.connect(str(legacy_path))
    con_legacy.execute("CREATE TABLE run (id VARCHAR PRIMARY KEY)")
    con_legacy.close()

    with pytest.raises(
        DatabaseMigrationRequired,
        match="Database uses Consist v0.4 schema \\(version=1\\)\\. Run 'consist migrate ",
    ):
        DatabaseManager(str(legacy_path))


def test_find_schema_observation_for_content_identity_and_hash(tmp_path: Path) -> None:
    db_path = tmp_path / "content_obs.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        digest = "a" * 64
        content_identity = f"sha256:file:{digest}"
        a = Artifact(
            key="a",
            container_uri="inputs://a.csv",
            driver="csv",
            hash=digest,
            meta={"content_identity": content_identity},
        )
        b = Artifact(
            key="b",
            container_uri="inputs://b.csv",
            driver="csv",
            hash=digest,
            meta={"content_identity": content_identity},
        )
        db.sync_artifact(a)
        db.sync_artifact(b)

        schema = ArtifactSchema(
            id="sid", summary_json={"table_name": "a"}, profile_version=1
        )
        obs_in = ArtifactSchemaObservation(
            artifact_id=a.id, schema_id=schema.id, source="file"
        )
        db.persist_artifact_schema(schema, fields=[], relations=[], observation=obs_in)

        obs_by_identity = db.find_schema_observation_for_content_identity(
            content_identity=content_identity,
            digest=digest,
            driver="csv",
        )
        assert obs_by_identity is not None
        assert obs_by_identity.schema_id == "sid"

        obs_by_hash = db.find_schema_observation_for_hash(digest)
        assert obs_by_hash is not None
        assert obs_by_hash.schema_id == "sid"
    finally:
        db.close()


def test_parent_artifact_queries_support_canonical_and_legacy_rows(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "artifact_parent_lookup.duckdb"))
    try:
        parent_id = uuid.uuid4()
        parent = Artifact(
            id=parent_id,
            key="container",
            container_uri="outputs://container.h5",
            driver="h5",
            run_id="run_a",
        )
        child = Artifact(
            key="child",
            container_uri="outputs://container.h5",
            driver="h5_table",
            table_path="/a",
            parent_artifact_id=parent_id,
            run_id="run_a",
            meta={"parent_id": str(parent_id)},
        )
        legacy_child = Artifact(
            key="legacy_child",
            container_uri="outputs://legacy.h5",
            driver="h5_table",
            table_path="/legacy",
            run_id="run_a",
            meta={"parent_id": str(parent_id)},
        )
        db.sync_artifact(parent)
        db.sync_artifact(child)
        db.sync_artifact(legacy_child)

        children = db.get_child_artifacts(parent_id)
        assert [artifact.key for artifact in children] == ["child", "legacy_child"]

        resolved_parent = db.get_parent_artifact(child.id)
        assert resolved_parent is not None
        assert resolved_parent.id == parent_id

        resolved_legacy_parent = db.get_parent_artifact(legacy_child.id)
        assert resolved_legacy_parent is not None
        assert resolved_legacy_parent.id == parent_id
    finally:
        db.close()
