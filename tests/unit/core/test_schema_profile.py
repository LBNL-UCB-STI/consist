"""
Unit tests for Consist's schema discovery + persistence layer.

These tests validate the new "deduped schema" feature:
- profiling a DuckDB table produces a stable schema_id,
- schema payloads are persisted in normalized tables,
- artifacts store a pointer + small summary in `Artifact.meta`,
- repeated profiling dedupes schema rows but records multiple observations.
"""

import json
import uuid
from typing import Optional

from sqlalchemy import column, table
from sqlmodel import Field, SQLModel, Session, select

from consist.models.artifact import Artifact
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
    ArtifactSchemaRelation,
)


def test_profile_and_persist_ingested_schema(tracker, sample_csv):
    """
    Regression test for the end-to-end persistence path used by `Tracker.ingest()`.

    Checks:
    - schema_id + schema_summary are added to `artifact.meta`
    - deduped schema blob row is written once
    - per-field rows are written once
    - observations are appended on each profiling call
    """
    with tracker.start_run("schema_profile_run", "demo") as t:
        artifact = t.log_output(sample_csv("fixture.csv"), key="fixture")
        assert t.engine is not None

        with t.engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
            conn.exec_driver_sql(
                "CREATE TABLE global_tables.fixture (a INTEGER, b VARCHAR)"
            )
            conn.exec_driver_sql("INSERT INTO global_tables.fixture VALUES (1, 'x')")

        run = t.current_consist.run
        t.artifact_schemas.profile_ingested_table(
            artifact=artifact,
            run=run,
            table_schema="global_tables",
            table_name="fixture",
        )

        assert "schema_id" in artifact.meta
        assert "schema_summary" in artifact.meta
        summary = artifact.meta["schema_summary"]
        assert summary["table_schema"] == "global_tables"
        assert summary["table_name"] == "fixture"
        assert summary["n_columns"] == 2

        with Session(t.engine) as session:
            schema_id = artifact.meta["schema_id"]
            assert session.get(ArtifactSchema, schema_id) is not None

            fields = session.exec(
                select(ArtifactSchemaField).where(
                    ArtifactSchemaField.schema_id == schema_id
                )
            ).all()
            assert {f.name for f in fields} == {"a", "b"}
            ordinal_by_name = {f.name: f.ordinal_position for f in fields}
            assert ordinal_by_name["a"] == 1
            assert ordinal_by_name["b"] == 2

            observations = session.exec(
                select(ArtifactSchemaObservation).where(
                    ArtifactSchemaObservation.schema_id == schema_id
                )
            ).all()
            assert len(observations) == 1

        t.artifact_schemas.profile_ingested_table(
            artifact=artifact,
            run=run,
            table_schema="global_tables",
            table_name="fixture",
        )

        with Session(t.engine) as session:
            schema_id = artifact.meta["schema_id"]
            schemas = session.exec(select(ArtifactSchema)).all()
            assert len(schemas) == 1
            observations = session.exec(
                select(ArtifactSchemaObservation).where(
                    ArtifactSchemaObservation.schema_id == schema_id
                )
            ).all()
            assert len(observations) == 2

        if "schema_profile" in artifact.meta:
            json.dumps(artifact.meta["schema_profile"])


def test_profile_duckdb_table_truncates_fields_for_wide_tables(tracker):
    """
    Wide tables can create large schema payloads; we truncate stored JSON blobs but
    still retain per-field rows for schema export.

    Checks:
    - the summary reports that fields were truncated
    - the returned `fields` list still contains all fields for persistence/export
    """
    from consist.tools import schema_profile

    with tracker.start_run("schema_profile_wide", "demo") as t:
        assert t.engine is not None
        table_schema = "global_tables"
        table_name = "wide_table"

        wide_n = schema_profile.MAX_FIELDS + 1
        cols_sql = ", ".join(f"c{i} INTEGER" for i in range(wide_n))
        with t.engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {table_schema}")
            conn.exec_driver_sql(
                f"CREATE TABLE {table_schema}.{table_name} ({cols_sql})"
            )

        result = schema_profile.profile_duckdb_table(
            engine=t.engine,
            identity=t.identity,
            table_schema=table_schema,
            table_name=table_name,
        )
        assert result.summary["truncated"]["fields"] is True
        assert len(list(result.fields)) == wide_n
        assert result.schema_json is not None
        assert result.schema_json["fields"] == []


def test_profile_duckdb_table_respects_size_limits(tracker, monkeypatch):
    """
    The profiler should omit large inline/full JSON payloads when configured.

    Checks:
    - when MAX_INLINE_PROFILE_BYTES is very small, inline payload is omitted
    - when MAX_SCHEMA_JSON_BYTES is very small, full payload is omitted
    """
    from consist.tools import schema_profile

    with tracker.start_run("schema_profile_limits", "demo") as t:
        assert t.engine is not None
        table_schema = "global_tables"
        table_name = "small_table"
        with t.engine.begin() as conn:
            conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {table_schema}")
            conn.exec_driver_sql(
                f"CREATE TABLE {table_schema}.{table_name} (a INTEGER, b VARCHAR)"
            )

        monkeypatch.setattr(schema_profile, "MAX_INLINE_PROFILE_BYTES", 1)
        monkeypatch.setattr(schema_profile, "MAX_SCHEMA_JSON_BYTES", 1)

        result = schema_profile.profile_duckdb_table(
            engine=t.engine,
            identity=t.identity,
            table_schema=table_schema,
            table_name=table_name,
        )
        assert result.summary["truncated"]["inline_profile"] is True
        assert result.inline_profile_json is None
        assert result.summary["truncated"]["schema_json"] is True
        assert result.schema_json is None


def test_profile_user_provided_schema_persists_fields_and_meta(tracker, sample_csv):
    class UserSchema(SQLModel):
        id: int
        name: str
        optional_count: Optional[int] = None

    with tracker.start_run("user_schema_run", "demo") as t:
        artifact = t.log_artifact(
            sample_csv("user_schema.csv"), key="user_schema", schema=UserSchema
        )

        schema_id = artifact.meta.get("schema_id")
        assert schema_id
        assert artifact.meta.get("schema_name") == "UserSchema"
        assert artifact.meta.get("schema_summary", {}).get("n_columns") == 3

        with Session(t.engine) as session:
            schema_row = session.get(ArtifactSchema, schema_id)
            assert schema_row is not None

            fields = session.exec(
                select(ArtifactSchemaField).where(
                    ArtifactSchemaField.schema_id == schema_id
                )
            ).all()
            fields_by_name = {field.name: field for field in fields}
            assert fields_by_name["id"].logical_type == "integer"
            assert fields_by_name["id"].nullable is False
            assert fields_by_name["name"].logical_type == "varchar"
            assert fields_by_name["name"].nullable is False
            assert fields_by_name["optional_count"].logical_type == "integer"
            assert fields_by_name["optional_count"].nullable is True

            observations = session.exec(
                select(ArtifactSchemaObservation).where(
                    ArtifactSchemaObservation.artifact_id == artifact.id
                )
            ).all()
            assert len(observations) == 1
            assert observations[0].source == "user_provided"
            assert observations[0].schema_id == schema_id


def test_profile_user_provided_schema_persists_relations_and_hash(tracker, sample_csv):
    class HouseholdSchema(SQLModel):
        household_id: int
        person_id: int = Field(foreign_key="people.id")

    class HouseholdSchemaAlt(SQLModel):
        household_id: int
        person_id: int = Field(foreign_key="persons.id")

    with tracker.start_run("user_schema_relations", "demo") as t:
        artifact_a = t.log_artifact(
            sample_csv("user_schema_rel_a.csv"),
            key="user_schema_rel_a",
            schema=HouseholdSchema,
        )
        artifact_b = t.log_artifact(
            sample_csv("user_schema_rel_b.csv"),
            key="user_schema_rel_b",
            schema=HouseholdSchemaAlt,
        )

        schema_id_a = artifact_a.meta.get("schema_id")
        schema_id_b = artifact_b.meta.get("schema_id")
        assert schema_id_a
        assert schema_id_b
        assert schema_id_a != schema_id_b

        with Session(t.engine) as session:
            relations = session.exec(
                select(ArtifactSchemaRelation).where(
                    ArtifactSchemaRelation.schema_id == schema_id_a
                )
            ).all()
            assert len(relations) == 1
            rel = relations[0]
            assert rel.from_field == "person_id"
            assert rel.to_table == "people"
            assert rel.to_field == "id"


def test_schema_links_view_returns_relations(tracker, sample_csv):
    class HouseSchema(SQLModel):
        house_id: int
        owner_id: int = Field(foreign_key="owners.id")

    with tracker.start_run("schema_links_view", "demo") as t:
        artifact = t.log_artifact(
            sample_csv("schema_links_view.csv"),
            key="schema_links_view",
            schema=HouseSchema,
        )
        schema_id = artifact.meta.get("schema_id")
        assert schema_id

        if t.db:
            t.db._ensure_schema_links_view()

        with Session(t.engine) as session:
            links = table(
                "consist_schema_links",
                column("from_table"),
                column("from_field"),
                column("to_table"),
                column("to_field"),
                column("schema_id"),
            )
            stmt = (
                select(
                    links.c.from_table,
                    links.c.from_field,
                    links.c.to_table,
                    links.c.to_field,
                )
                .where(links.c.schema_id == schema_id)
                .select_from(links)
            )
            rows = session.exec(stmt).all()
            assert len(rows) == 1
            from_table, from_field, to_table, to_field = rows[0]
            assert from_table == "houseschema"
            assert from_field == "owner_id"
            assert to_table == "owners"
            assert to_field == "id"


def test_apply_physical_fks_best_effort(tracker):
    assert tracker.engine is not None
    assert tracker.db is not None

    with tracker.engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
        conn.exec_driver_sql("CREATE TABLE children (id INTEGER, parent_id INTEGER)")

    schema_row = ArtifactSchema(
        id="schema_fk_apply",
        summary_json={"table_name": "children"},
        profile_version=1,
    )
    field_rows = [
        ArtifactSchemaField(
            schema_id="schema_fk_apply",
            ordinal_position=1,
            name="id",
            logical_type="integer",
            nullable=False,
        ),
        ArtifactSchemaField(
            schema_id="schema_fk_apply",
            ordinal_position=2,
            name="parent_id",
            logical_type="integer",
            nullable=True,
        ),
    ]
    relation_rows = [
        ArtifactSchemaRelation(
            schema_id="schema_fk_apply",
            from_field="parent_id",
            to_table="parents",
            to_field="id",
        )
    ]

    tracker.db.upsert_artifact_schema(schema_row, field_rows, relation_rows)
    applied = tracker.db.apply_physical_fks()
    assert applied >= 0


def test_get_artifact_schema_for_artifact_prefers_user_provided(tracker):
    artifact_id = uuid.uuid4()
    artifact = Artifact(
        id=artifact_id,
        key="artifact_pref",
        container_uri="inputs://artifact.csv",
        driver="csv",
        hash="abc",
    )

    schema_user = "schema_user"
    schema_file = "schema_file"
    schema_duckdb = "schema_duckdb"

    with Session(tracker.engine) as session:
        session.add(artifact)
        session.add(
            ArtifactSchema(
                id=schema_user,
                summary_json={"table_name": "user_table"},
                profile_version=1,
            )
        )
        session.add(
            ArtifactSchema(
                id=schema_file,
                summary_json={"table_name": "file_table"},
                profile_version=1,
            )
        )
        session.add(
            ArtifactSchema(
                id=schema_duckdb,
                summary_json={"table_name": "duckdb_table"},
                profile_version=1,
            )
        )
        session.add(
            ArtifactSchemaField(
                schema_id=schema_user,
                ordinal_position=1,
                name="user_col",
                logical_type="varchar",
                nullable=True,
            )
        )
        session.add(
            ArtifactSchemaField(
                schema_id=schema_file,
                ordinal_position=1,
                name="file_col",
                logical_type="varchar",
                nullable=True,
            )
        )
        session.add(
            ArtifactSchemaField(
                schema_id=schema_duckdb,
                ordinal_position=1,
                name="duckdb_col",
                logical_type="varchar",
                nullable=True,
            )
        )
        session.add(
            ArtifactSchemaObservation(
                artifact_id=artifact_id,
                schema_id=schema_file,
                source="file",
            )
        )
        session.add(
            ArtifactSchemaObservation(
                artifact_id=artifact_id,
                schema_id=schema_duckdb,
                source="duckdb",
            )
        )
        session.add(
            ArtifactSchemaObservation(
                artifact_id=artifact_id,
                schema_id=schema_user,
                source="user_provided",
            )
        )
        session.commit()

    fetched = tracker.db.get_artifact_schema_for_artifact(
        artifact_id=artifact_id, prefer_source="duckdb"
    )
    assert fetched is not None
    schema, _ = fetched
    assert schema.id == schema_user


def test_get_artifact_schema_for_artifact_respects_prefer_source(tracker):
    artifact_id = uuid.uuid4()
    artifact = Artifact(
        id=artifact_id,
        key="artifact_pref_no_user",
        container_uri="inputs://artifact_no_user.csv",
        driver="csv",
        hash="def",
    )

    schema_file = "schema_file_only"
    schema_duckdb = "schema_duckdb_only"

    with Session(tracker.engine) as session:
        session.add(artifact)
        session.add(
            ArtifactSchema(
                id=schema_file,
                summary_json={"table_name": "file_table"},
                profile_version=1,
            )
        )
        session.add(
            ArtifactSchema(
                id=schema_duckdb,
                summary_json={"table_name": "duckdb_table"},
                profile_version=1,
            )
        )
        session.add(
            ArtifactSchemaField(
                schema_id=schema_file,
                ordinal_position=1,
                name="file_col",
                logical_type="varchar",
                nullable=True,
            )
        )
        session.add(
            ArtifactSchemaField(
                schema_id=schema_duckdb,
                ordinal_position=1,
                name="duckdb_col",
                logical_type="varchar",
                nullable=True,
            )
        )
        session.add(
            ArtifactSchemaObservation(
                artifact_id=artifact_id,
                schema_id=schema_file,
                source="file",
            )
        )
        session.add(
            ArtifactSchemaObservation(
                artifact_id=artifact_id,
                schema_id=schema_duckdb,
                source="duckdb",
            )
        )
        session.commit()

    fetched_default = tracker.db.get_artifact_schema_for_artifact(
        artifact_id=artifact_id
    )
    assert fetched_default is not None
    schema_default, _ = fetched_default
    assert schema_default.id == schema_file

    fetched_duckdb = tracker.db.get_artifact_schema_for_artifact(
        artifact_id=artifact_id, prefer_source="duckdb"
    )
    assert fetched_duckdb is not None
    schema_duckdb_row, _ = fetched_duckdb
    assert schema_duckdb_row.id == schema_duckdb
