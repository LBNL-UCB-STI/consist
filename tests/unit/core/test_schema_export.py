from __future__ import annotations

from consist.core.schema_export import parse_duckdb_type, render_sqlmodel_stub
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaRelation,
)


def test_render_sqlmodel_stub_compiles_and_is_deterministic():
    schema = ArtifactSchema(
        id="schema_123",
        summary_json={"table_name": "My Table"},
        profile_json=None,
    )
    fields = [
        ArtifactSchemaField(
            schema_id=schema.id,
            ordinal_position=1,
            name="ID",
            logical_type="INTEGER",
            nullable=False,
        ),
        ArtifactSchemaField(
            schema_id=schema.id,
            ordinal_position=2,
            name="Mass(kg)",
            logical_type="decimal(10,2)",
            nullable=True,
            stats_json={"min": 0.1, "max": 999.9},
        ),
        ArtifactSchemaField(
            schema_id=schema.id,
            ordinal_position=3,
            name="payload",
            logical_type="json",
            nullable=True,
        ),
        ArtifactSchemaField(
            schema_id=schema.id,
            ordinal_position=4,
            name="_dlt_id",
            logical_type="varchar",
            nullable=False,
        ),
        ArtifactSchemaField(
            schema_id=schema.id,
            ordinal_position=5,
            name="consist_run_id",
            logical_type="varchar",
            nullable=False,
        ),
    ]

    code1 = render_sqlmodel_stub(schema=schema, fields=fields)
    code2 = render_sqlmodel_stub(schema=schema, fields=fields)
    assert code1 == code2
    compile(code1, "<schema_export>", "exec")

    # Default: system columns are excluded.
    assert "_dlt_id" not in code1
    assert "consist_run_id" not in code1

    # Aggressive normalization: original column name preserved through sa_column when renamed.
    assert "Column('ID'" in code1
    assert "Column('Mass(kg)'" in code1

    # Stats hints on by default (when present).
    assert "# Stats:" in code1

    # Stubs are abstract by default (importable without a primary key).
    assert "__abstract__ = True" in code1


def test_render_sqlmodel_stub_can_include_system_cols():
    schema = ArtifactSchema(
        id="schema_abc", summary_json={"table_name": "t"}, profile_json=None
    )
    fields = [
        ArtifactSchemaField(
            schema_id=schema.id,
            ordinal_position=1,
            name="consist_run_id",
            logical_type="varchar",
            nullable=False,
        )
    ]
    code = render_sqlmodel_stub(schema=schema, fields=fields, include_system_cols=True)
    assert "consist_run_id" in code


def test_render_sqlmodel_stub_renders_foreign_keys():
    schema = ArtifactSchema(
        id="schema_fk", summary_json={"table_name": "children"}, profile_json=None
    )
    fields = [
        ArtifactSchemaField(
            schema_id=schema.id,
            ordinal_position=1,
            name="child_id",
            logical_type="integer",
            nullable=False,
        ),
        ArtifactSchemaField(
            schema_id=schema.id,
            ordinal_position=2,
            name="parent_id",
            logical_type="integer",
            nullable=False,
        ),
    ]
    relations = [
        ArtifactSchemaRelation(
            schema_id=schema.id,
            from_field="parent_id",
            to_table="parents",
            to_field="id",
            relationship_type="foreign_key",
        )
    ]
    code = render_sqlmodel_stub(schema=schema, fields=fields, relations=relations)
    assert "foreign_key='parents.id'" in code


def test_parse_duckdb_type_pandas_fallbacks():
    assert parse_duckdb_type("int64").sqlalchemy_type_expr == "BigInteger"
    assert parse_duckdb_type("uint32").sqlalchemy_type_expr == "BigInteger"
    assert parse_duckdb_type("float64").sqlalchemy_type_expr == "Float"
    assert parse_duckdb_type("object").sqlalchemy_type_expr == "String"
    assert parse_duckdb_type("string[python]").sqlalchemy_type_expr == "String"
    assert parse_duckdb_type("bool").sqlalchemy_type_expr == "Boolean"
    assert parse_duckdb_type("datetime64[ns]").sqlalchemy_type_expr == "DateTime"
    assert parse_duckdb_type("date32").sqlalchemy_type_expr == "Date"
    assert parse_duckdb_type("category").sqlalchemy_type_expr == "String"
