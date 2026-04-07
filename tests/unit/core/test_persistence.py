from pathlib import Path

from consist.core.persistence import DatabaseManager
from consist.core.schema_compat import apply_content_identity_compatibility
from consist.models.artifact import Artifact
from consist.models.artifact_schema import ArtifactSchemaObservation, ArtifactSchema


def test_find_artifact_content_reuses_existing_row(tmp_path: Path) -> None:
    db_path = tmp_path / "content_identity.db"
    db = DatabaseManager(str(db_path))

    first = db.get_or_create_artifact_content(
        content_hash="shared_hash",
        driver="parquet",
        meta={"source": "test"},
    )

    existing = db.find_artifact_content(
        content_hash="shared_hash",
        driver="parquet",
    )
    assert existing is not None
    assert existing.id == first.id

    second = db.get_or_create_artifact_content(
        content_hash="shared_hash",
        driver="parquet",
    )
    assert second.id == first.id

    # lookup without driver still rounds up the same row
    driver_agnostic = db.find_artifact_content(content_hash="shared_hash")
    assert driver_agnostic is not None
    assert driver_agnostic.id == first.id

    # a different driver yields no match
    assert db.find_artifact_content(content_hash="shared_hash", driver="csv") is None


def test_get_or_create_artifact_content_uses_in_memory_cache(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "content_identity_cache.db"
    db = DatabaseManager(str(db_path))

    session_calls = 0
    original_session_scope = db.session_scope

    def counting_session_scope():
        nonlocal session_calls
        session_calls += 1
        return original_session_scope()

    monkeypatch.setattr(db, "session_scope", counting_session_scope)

    first = db.get_or_create_artifact_content(
        content_hash="shared_hash",
        driver="parquet",
        meta={"source": "test"},
    )
    second = db.get_or_create_artifact_content(
        content_hash="shared_hash",
        driver="parquet",
    )

    assert first.id == second.id
    assert session_calls == 1


def test_content_identity_compatibility_recreates_index_idempotently(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content_index.db"
    db = DatabaseManager(str(db_path))

    with db.engine.begin() as conn:
        initial_rows = conn.exec_driver_sql(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE table_name = 'artifact'
              AND expressions = '[content_id]'
            """
        ).fetchall()

    assert len(initial_rows) == 1

    with db.engine.begin() as conn:
        existing_indexes = conn.exec_driver_sql(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE table_name = 'artifact'
              AND expressions = '[content_id]'
            """
        ).fetchall()
        for (index_name,) in existing_indexes:
            conn.exec_driver_sql(f"DROP INDEX IF EXISTS {index_name}")
    assert db._table_has_column(table_name="artifact", column_name="content_id")

    apply_content_identity_compatibility(db)
    apply_content_identity_compatibility(db)

    with db.engine.begin() as conn:
        rows = conn.exec_driver_sql(
            """
            SELECT index_name
            FROM duckdb_indexes()
            WHERE table_name = 'artifact'
              AND expressions = '[content_id]'
            """
        ).fetchall()

    assert rows == [("idx_artifact_content_id",)]


def test_content_id_backfill_is_explicit_not_automatic(tmp_path: Path) -> None:
    db_path = tmp_path / "content_backfill.db"
    db = DatabaseManager(str(db_path))

    with db.session_scope() as session:
        artifact = Artifact(
            key="network",
            container_uri="outputs://network.csv",
            driver="csv",
            hash="shared_hash",
            run_id="run_a",
        )
        session.add(artifact)
        session.commit()
        artifact_id = artifact.id

    reopened = DatabaseManager(str(db_path))
    before = reopened.get_artifact(artifact_id)

    assert before is not None
    assert before.content_id is None
    assert (
        reopened.find_artifact_content(content_hash="shared_hash", driver="csv") is None
    )

    reopened.backfill_artifact_content_ids()

    after = reopened.get_artifact(artifact_id)

    assert after is not None
    assert after.content_id is not None
    content = reopened.find_artifact_content(content_hash="shared_hash", driver="csv")
    assert content is not None
    assert after.content_id == content.id


def test_find_schema_observation_for_content_id(tmp_path: Path) -> None:
    db_path = tmp_path / "content_obs.db"
    db = DatabaseManager(str(db_path))

    # Create two artifacts that share content identity
    content = db.get_or_create_artifact_content(content_hash="h123", driver="csv")
    with db.session_scope() as session:
        a = Artifact(
            key="a",
            container_uri="inputs://a.csv",
            driver="csv",
            hash="h123",
            content_id=content.id,
        )
        b = Artifact(
            key="b",
            container_uri="inputs://b.csv",
            driver="csv",
            hash="h123",
            content_id=content.id,
        )
        session.add(a)
        session.add(b)
        session.flush()

        schema = ArtifactSchema(id="sid", summary_json={}, profile_version=1)
        session.add(schema)
        session.add(
            ArtifactSchemaObservation(
                artifact_id=a.id, schema_id=schema.id, source="file"
            )
        )
        session.commit()

    obs = db.find_schema_observation_for_content_id(content.id)
    assert obs is not None
    assert obs.schema_id == "sid"
