from __future__ import annotations

import pytest

from consist.core.persistence import (
    DatabaseManager,
    ProvenanceWriter,
    load_json_safe,
)
from consist.core.stores import HotDataStore, MetadataStore
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaRelation,
)


def test_load_json_safe_raises_value_error_for_invalid_json() -> None:
    with pytest.raises(ValueError, match="Invalid JSON"):
        load_json_safe("{not-valid-json")


def test_load_json_safe_raises_value_error_for_excessive_nesting_depth() -> None:
    deeply_nested = "[" * 51 + "0" + "]" * 51
    with pytest.raises(ValueError, match="JSON nesting depth exceeds limit"):
        load_json_safe(deeply_nested)


def test_has_table_column_returns_false_for_unsafe_table_name() -> None:
    db = DatabaseManager(":memory:")
    try:
        assert (
            db.has_table_column(
                table_schema="main",
                table_name="run; DROP TABLE run;",
                column_name="id",
            )
            is False
        )
    finally:
        db.close()


def test_query_df_rejects_non_select_and_multi_statement_sql() -> None:
    db = DatabaseManager(":memory:")
    try:
        with pytest.raises(ValueError, match="non-empty SELECT"):
            db.query_df("   ")

        with pytest.raises(ValueError, match="single read-only SELECT"):
            db.query_df("DELETE FROM run")

        with pytest.raises(ValueError, match="single read-only SELECT"):
            db.query_df("SELECT 1; DROP TABLE run;")
    finally:
        db.close()


def test_hot_data_store_rejects_unsafe_table_name() -> None:
    db = DatabaseManager(":memory:")
    try:
        meta_store = MetadataStore(db=db)
        hot_store = HotDataStore(db_path=":memory:", metadata_store=meta_store)
        try:
            assert hot_store.ingest_cache_hit("run; DROP TABLE run;", "abc123") is False
        finally:
            hot_store.close()
    finally:
        db.close()


def test_apply_physical_fks_quotes_valid_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = DatabaseManager(":memory:")
    try:
        db.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        db.execute("CREATE TABLE global_tables.parents (id INTEGER PRIMARY KEY)")
        db.execute("CREATE TABLE global_tables.children (parent_id INTEGER)")

        db.persist_artifact_schema(
            ArtifactSchema(
                id="schema_children",
                profile_version=1,
                summary_json={
                    "table_schema": "global_tables",
                    "table_name": "children",
                },
            ),
            fields=[
                ArtifactSchemaField(
                    schema_id="schema_children",
                    ordinal_position=1,
                    name="parent_id",
                    logical_type="INTEGER",
                )
            ],
            relations=[
                ArtifactSchemaRelation(
                    schema_id="schema_children",
                    from_field="parent_id",
                    to_table="parents",
                    to_field="id",
                )
            ],
        )

        alter_statements: list[str] = []
        original_execute = db.execute

        def intercepting_execute(sql: str, parameters: object = None):
            if sql.lstrip().upper().startswith("ALTER TABLE"):
                alter_statements.append(sql)
                return None
            return original_execute(sql, parameters)

        monkeypatch.setattr(db, "execute", intercepting_execute)

        applied = db.apply_physical_fks()

        assert applied == 1
        assert len(alter_statements) == 1
        assert alter_statements[0].startswith('ALTER TABLE "global_tables"."children"')
        assert (
            'ADD CONSTRAINT "fk_global_tables_children_parent_id"'
            in alter_statements[0]
        )
        assert 'FOREIGN KEY ("parent_id")' in alter_statements[0]
        assert 'REFERENCES "global_tables"."parents"("id")' in alter_statements[0]
    finally:
        db.close()


def test_apply_physical_fks_skips_invalid_identifiers() -> None:
    db = DatabaseManager(":memory:")
    try:
        db.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        db.execute("CREATE TABLE global_tables.children (parent_id INTEGER)")

        db.persist_artifact_schema(
            ArtifactSchema(
                id="schema_unsafe_fk",
                profile_version=1,
                summary_json={"table_name": "children"},
            ),
            fields=[
                ArtifactSchemaField(
                    schema_id="schema_unsafe_fk",
                    ordinal_position=1,
                    name="parent_id",
                    logical_type="INTEGER",
                )
            ],
            relations=[
                ArtifactSchemaRelation(
                    schema_id="schema_unsafe_fk",
                    from_field="parent_id",
                    to_table="parents;DROP TABLE run;--",
                    to_field="id",
                )
            ],
        )

        applied = db.apply_physical_fks()

        assert applied == 0
        assert db.has_table("run") is True
    finally:
        db.close()


def test_write_record_json_does_not_use_predictable_tmp_targets(tracker) -> None:
    with tracker.start_run("run_secure_snapshot_tmp", "demo") as t:
        record = t.current_consist
        assert record is not None

        per_run_dir = t.fs.run_dir / "consist_runs"
        per_run_dir.mkdir(parents=True, exist_ok=True)
        per_run_target = per_run_dir / "run_secure_snapshot_tmp.json"
        per_run_predictable_tmp = per_run_target.with_suffix(".tmp")
        per_run_predictable_tmp.write_text("sentinel", encoding="utf-8")

        latest_target = t.fs.run_dir / "consist.json"
        latest_predictable_tmp = latest_target.with_suffix(".tmp")
        latest_predictable_tmp.write_text("sentinel", encoding="utf-8")

        writer = ProvenanceWriter(t)
        writer._write_record_json(record)

        assert per_run_target.exists()
        assert latest_target.exists()
        assert per_run_predictable_tmp.read_text(encoding="utf-8") == "sentinel"
        assert latest_predictable_tmp.read_text(encoding="utf-8") == "sentinel"
