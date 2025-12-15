"""
Unit tests for Consist's schema discovery + persistence layer.

These tests validate the new "deduped schema" feature:
- profiling a DuckDB table produces a stable schema_id,
- schema payloads are persisted in normalized tables,
- artifacts store a pointer + small summary in `Artifact.meta`,
- repeated profiling dedupes schema rows but records multiple observations.
"""

import json

from sqlmodel import Session, select

from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
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
    Wide tables can create large schema payloads; we cap per-schema field persistence.

    Checks:
    - the summary reports that fields were truncated
    - the returned `fields` list is empty (so we don't create thousands of rows)
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
        assert list(result.fields) == []


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
