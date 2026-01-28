from __future__ import annotations

import json
import keyword
import re
from dataclasses import dataclass, field
from typing import Iterable, Optional, TYPE_CHECKING

from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaRelation,
)

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager

DEFAULT_SYSTEM_COLUMN_PREFIXES: tuple[str, ...] = ("consist_", "_dlt_")

_DECIMAL_RE = re.compile(
    r"^(?P<base>decimal|numeric)\s*\(\s*(?P<p>\d+)\s*,\s*(?P<s>\d+)\s*\)$"
)

_FORBIDDEN_ATTRS = {
    "metadata",  # SQLAlchemy DeclarativeBase attribute
}


@dataclass
class _ImportTracker:
    stdlib: set[str] = field(default_factory=set)
    typing: set[str] = field(default_factory=set)
    sqlalchemy: set[str] = field(default_factory=set)
    sqlmodel: set[str] = field(default_factory=set)

    def render(self) -> str:
        lines: list[str] = ["from __future__ import annotations", ""]

        if self.stdlib:
            lines.extend(sorted(self.stdlib))
            lines.append("")

        if self.typing:
            lines.append(f"from typing import {', '.join(sorted(self.typing))}")
            lines.append("")

        if self.sqlalchemy:
            lines.append(f"from sqlalchemy import {', '.join(sorted(self.sqlalchemy))}")
            lines.append("")

        if self.sqlmodel:
            lines.append(f"from sqlmodel import {', '.join(sorted(self.sqlmodel))}")
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"


@dataclass(frozen=True)
class _TypeSpec:
    python_type: str
    sqlalchemy_type_expr: str
    stdlib_imports: set[str] = field(default_factory=set)
    typing_imports: set[str] = field(default_factory=set)
    sqlalchemy_imports: set[str] = field(default_factory=set)


def parse_duckdb_type(logical_type: str) -> _TypeSpec:
    """
    Map DuckDB `information_schema.columns.data_type` strings to:
    - a Python typing annotation (as a string)
    - a SQLAlchemy column type expression (as a string)
    """
    t = (logical_type or "").strip().lower()
    if not t:
        return _TypeSpec(
            python_type="Any",
            sqlalchemy_type_expr="JSON",
            typing_imports={"Any"},
            sqlalchemy_imports={"JSON"},
        )

    if m := _DECIMAL_RE.match(t):
        p = int(m.group("p"))
        s = int(m.group("s"))
        return _TypeSpec(
            python_type="Decimal",
            sqlalchemy_type_expr=f"Numeric({p}, {s})",
            stdlib_imports={"from decimal import Decimal"},
            sqlalchemy_imports={"Numeric"},
        )

    if t in {"integer", "int4", "int", "smallint", "int2", "utinyint", "usmallint"}:
        return _TypeSpec(
            python_type="int",
            sqlalchemy_type_expr="Integer",
            sqlalchemy_imports={"Integer"},
        )
    if t in {"bigint", "int8", "ubigint", "hugeint"}:
        return _TypeSpec(
            python_type="int",
            sqlalchemy_type_expr="BigInteger",
            sqlalchemy_imports={"BigInteger"},
        )
    if t in {"double", "float8", "real", "float4", "float"}:
        return _TypeSpec(
            python_type="float",
            sqlalchemy_type_expr="Float",
            sqlalchemy_imports={"Float"},
        )
    if t in {"boolean", "bool"}:
        return _TypeSpec(
            python_type="bool",
            sqlalchemy_type_expr="Boolean",
            sqlalchemy_imports={"Boolean"},
        )

    if t in {"varchar", "text", "string", "character varying", "char", "bpchar"}:
        return _TypeSpec(
            python_type="str",
            sqlalchemy_type_expr="String",
            sqlalchemy_imports={"String"},
        )

    if t == "date":
        return _TypeSpec(
            python_type="datetime.date",
            sqlalchemy_type_expr="Date",
            stdlib_imports={"import datetime"},
            sqlalchemy_imports={"Date"},
        )
    if t in {"timestamp", "datetime", "timestamp without time zone"}:
        return _TypeSpec(
            python_type="datetime.datetime",
            sqlalchemy_type_expr="DateTime",
            stdlib_imports={"import datetime"},
            sqlalchemy_imports={"DateTime"},
        )
    if t in {"timestamp_tz", "timestamptz", "timestamp with time zone"}:
        return _TypeSpec(
            python_type="datetime.datetime",
            sqlalchemy_type_expr="DateTime(timezone=True)",
            stdlib_imports={"import datetime"},
            sqlalchemy_imports={"DateTime"},
        )

    if t == "uuid":
        return _TypeSpec(
            python_type="uuid.UUID",
            sqlalchemy_type_expr="String",
            stdlib_imports={"import uuid"},
            sqlalchemy_imports={"String"},
        )

    if t == "json":
        return _TypeSpec(
            python_type="dict[str, Any]",
            sqlalchemy_type_expr="JSON",
            typing_imports={"Any"},
            sqlalchemy_imports={"JSON"},
        )

    if t in {"blob", "bytea"}:
        return _TypeSpec(
            python_type="bytes",
            sqlalchemy_type_expr="LargeBinary",
            sqlalchemy_imports={"LargeBinary"},
        )

    # Pandas/Arrow dtype fallbacks (CSV profiling uses df.dtypes)
    if t in {"int64", "int32", "int16", "int8", "uint64", "uint32", "uint16", "uint8"}:
        return _TypeSpec(
            python_type="int",
            sqlalchemy_type_expr="BigInteger",
            sqlalchemy_imports={"BigInteger"},
        )
    if t in {"float64", "float32", "float16"}:
        return _TypeSpec(
            python_type="float",
            sqlalchemy_type_expr="Float",
            sqlalchemy_imports={"Float"},
        )
    if t in {"bool", "boolean", "bool_"}:
        return _TypeSpec(
            python_type="bool",
            sqlalchemy_type_expr="Boolean",
            sqlalchemy_imports={"Boolean"},
        )
    if t in {"object", "string", "string[python]", "string[pyarrow]"}:
        return _TypeSpec(
            python_type="str",
            sqlalchemy_type_expr="String",
            sqlalchemy_imports={"String"},
        )
    if t == "category":
        return _TypeSpec(
            python_type="str",
            sqlalchemy_type_expr="String",
            sqlalchemy_imports={"String"},
        )
    if t in {"datetime64[ns]", "datetime64[ns, tz]"}:
        return _TypeSpec(
            python_type="datetime.datetime",
            sqlalchemy_type_expr="DateTime",
            stdlib_imports={"import datetime"},
            sqlalchemy_imports={"DateTime"},
        )
    if t in {"date32", "date64"}:
        return _TypeSpec(
            python_type="datetime.date",
            sqlalchemy_type_expr="Date",
            stdlib_imports={"import datetime"},
            sqlalchemy_imports={"Date"},
        )

    # Conservative fallback: keep typing flexible and represent as JSON in SQLAlchemy.
    return _TypeSpec(
        python_type="Any",
        sqlalchemy_type_expr="JSON",
        typing_imports={"Any"},
        sqlalchemy_imports={"JSON"},
    )


def _sanitize_identifier(original_name: str) -> str:
    s = (original_name or "").strip().lower()
    s = re.sub(r"[^a-zA-Z0-9_]", "_", s)
    s = re.sub(r"_{2,}", "_", s)
    s = s.strip("_")
    if not s:
        s = "col"
    if s[0].isdigit():
        s = f"_{s}"
    if keyword.iskeyword(s) or s in _FORBIDDEN_ATTRS:
        s = f"{s}_"
    return s


def _to_class_name(table_name: str) -> str:
    base = _sanitize_identifier(table_name)
    parts = [p for p in base.split("_") if p]
    if not parts:
        return "Table"
    return "".join(p[:1].upper() + p[1:] for p in parts)


def _iter_export_fields(
    fields: Iterable[ArtifactSchemaField],
    *,
    include_system_cols: bool,
    system_column_prefixes: tuple[str, ...],
) -> list[ArtifactSchemaField]:
    out: list[ArtifactSchemaField] = []
    for row in fields:
        if not include_system_cols and any(
            row.name.startswith(prefix) for prefix in system_column_prefixes
        ):
            continue
        out.append(row)
    return out


def _render_stats_comments(field: ArtifactSchemaField) -> list[str]:
    comments: list[str] = []
    if field.is_enum and field.enum_values_json:
        enum_preview = field.enum_values_json[:20]
        suffix = "..." if len(field.enum_values_json) > 20 else ""
        comments.append(f"# Enum values: {json.dumps(enum_preview)}{suffix}")
    if field.stats_json:
        comments.append(
            f"# Stats: {json.dumps(field.stats_json, sort_keys=True, default=str)}"
        )
    return [c if len(c) <= 160 else (c[:157] + "...") for c in comments]


def render_sqlmodel_stub(
    *,
    schema: ArtifactSchema,
    fields: list[ArtifactSchemaField],
    relations: Optional[Iterable[ArtifactSchemaRelation]] = None,
    db: Optional["DatabaseManager"] = None,
    table_name: Optional[str] = None,
    class_name: Optional[str] = None,
    abstract: bool = True,
    include_system_cols: bool = False,
    include_stats_comments: bool = True,
    system_column_prefixes: tuple[str, ...] = DEFAULT_SYSTEM_COLUMN_PREFIXES,
) -> str:
    resolved_table_name = table_name
    if resolved_table_name is None:
        summary = getattr(schema, "summary_json", None) or {}
        tname = summary.get("table_name")
        resolved_table_name = tname if isinstance(tname, str) and tname else "table"

    resolved_class_name = class_name or _to_class_name(resolved_table_name)

    export_fields = _iter_export_fields(
        fields,
        include_system_cols=include_system_cols,
        system_column_prefixes=system_column_prefixes,
    )

    if relations is None and db is not None:
        relations = db.get_artifact_schema_relations(schema_id=schema.id)
    relation_lookup: dict[str, str] = {}
    if relations:
        for rel in relations:
            if rel.from_field not in relation_lookup:
                relation_lookup[rel.from_field] = f"{rel.to_table}.{rel.to_field}"

    imports = _ImportTracker()
    imports.sqlmodel.add("SQLModel")

    used_attr_names: dict[str, int] = {}
    rendered_fields: list[str] = []

    for row in export_fields:
        base_attr = _sanitize_identifier(row.name)
        n = used_attr_names.get(base_attr, 0) + 1
        used_attr_names[base_attr] = n
        attr_name = base_attr if n == 1 else f"{base_attr}__{n}"

        type_spec = parse_duckdb_type(row.logical_type)
        imports.stdlib |= type_spec.stdlib_imports
        imports.typing |= type_spec.typing_imports
        imports.sqlalchemy |= type_spec.sqlalchemy_imports

        py_type = type_spec.python_type
        if row.nullable:
            imports.typing.add("Optional")
            annotation = f"Optional[{py_type}]"
        else:
            annotation = py_type

        # Default to explicit SQLAlchemy Columns for all fields.
        # This avoids SQLModel type-inference edge cases (e.g. postponed annotations)
        # and preserves the original DB column names even when we normalize attributes.
        needs_sa_column = True

        stats_comments: list[str] = []
        if include_stats_comments:
            stats_comments = _render_stats_comments(row)

        if needs_sa_column:
            imports.sqlmodel.add("Field")
            imports.sqlalchemy.add("Column")

            column_name_repr = repr(row.name)
            col_type_expr = type_spec.sqlalchemy_type_expr
            if col_type_expr.startswith("DateTime("):
                imports.sqlalchemy.add("DateTime")
            nullable_expr = "True" if row.nullable else "False"
            fk_target = relation_lookup.get(row.name)
            field_args: list[str] = []
            if row.nullable:
                field_args.append("default=None")
            if fk_target:
                field_args.append(f"foreign_key={fk_target!r}")
            field_args.append(
                f"sa_column=Column({column_name_repr}, {col_type_expr}, nullable={nullable_expr})"
            )
            field_rhs = f"Field({', '.join(field_args)})"
            rendered_line = f"{attr_name}: {annotation} = {field_rhs}"
        else:
            if row.nullable:
                rendered_line = f"{attr_name}: {annotation} = None"
            else:
                rendered_line = f"{attr_name}: {annotation}"

        for comment in stats_comments:
            rendered_fields.append(f"    {comment}")
        rendered_fields.append(f"    {rendered_line}")

    header_comment = (
        f"# Generated by Consist schema export (schema_id={schema.id}).\n"
        "# NOTE: This class is `__abstract__` by default so it can be imported without\n"
        "# requiring a primary key. To map it as a real table/view, set `abstract=False`\n"
        "# during export (or remove `__abstract__` manually) AND define a primary key.\n"
    )

    body_lines: list[str] = [
        header_comment.rstrip(),
        "",
        f"class {resolved_class_name}(SQLModel, table=True):",
        f"    __tablename__ = {repr(resolved_table_name)}",
        '    __table_args__ = {"extend_existing": True}',
    ]
    if abstract:
        body_lines.append("    __abstract__ = True")

    if rendered_fields:
        body_lines.append("")
        body_lines.extend(rendered_fields)
    else:
        body_lines.append("")
        body_lines.append("    pass")

    return imports.render() + "\n".join(body_lines).rstrip() + "\n"
