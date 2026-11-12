from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Annotated, Any, Union, get_args, get_origin
import uuid

from pydantic import BaseModel
from pydantic.fields import FieldInfo as PydanticFieldInfo
from sqlalchemy.sql.schema import (
    Column as SAColumn,
    ForeignKey as SAForeignKey,
    Table as SATable,
)
from sqlalchemy.sql.sqltypes import JSON as SAJSON
from sqlmodel import SQLModel
from sqlmodel.main import FieldInfo as SQLModelFieldInfo


def optional_model_table_name(model_cls: type[BaseModel]) -> str | None:
    """Return explicit/declared SQL table name for ``SQLModel`` classes, else ``None``."""
    if isinstance(model_cls, type) and issubclass(model_cls, SQLModel):
        raw = model_cls.__tablename__
        if isinstance(raw, str) and raw:
            return raw
    return None


def model_table_name(
    model_cls: type[BaseModel], *, preserve_case_fallback: bool = False
) -> str:
    """Return resolved table name for a Pydantic/SQLModel class."""
    explicit = optional_model_table_name(model_cls)
    if explicit is not None:
        return explicit
    return model_cls.__name__ if preserve_case_fallback else model_cls.__name__.lower()


def model_table_schema(model_cls: type[BaseModel], *, default: str = "main") -> str:
    """Return SQL schema from ``__table__`` or ``__table_args__``, else ``default``."""
    if isinstance(model_cls, type) and issubclass(model_cls, SQLModel):
        cls_any: Any = model_cls
        try:
            table_obj = cls_any.__table__
        except AttributeError:
            table_obj = None
        if (
            isinstance(table_obj, SATable)
            and isinstance(table_obj.schema, str)
            and table_obj.schema
        ):
            return table_obj.schema

        try:
            raw_ta = cls_any.__table_args__
        except AttributeError:
            raw_ta = None
        if isinstance(raw_ta, Mapping):
            raw_sc = raw_ta.get("schema")
            if isinstance(raw_sc, str) and raw_sc:
                return raw_sc
        elif isinstance(raw_ta, tuple):
            for item in raw_ta:
                if isinstance(item, Mapping):
                    raw_sc = item.get("schema")
                    if isinstance(raw_sc, str) and raw_sc:
                        return raw_sc
    return default


def is_additive_schema_model(model_cls: type[BaseModel] | None) -> bool:
    """Return True when ``model_cls`` sets ``__consist_additive_schema__ = True``."""
    if model_cls is None:
        return False
    cls_any: Any = model_cls
    try:
        return bool(cls_any.__consist_additive_schema__)
    except AttributeError:
        return False


def field_sa_column(field_info: PydanticFieldInfo | Any) -> SAColumn[Any] | None:
    """Return underlying SQLAlchemy ``Column`` when attached to a ``SQLModel`` field."""
    if isinstance(field_info, SQLModelFieldInfo) and isinstance(
        field_info.sa_column, SAColumn
    ):
        return field_info.sa_column
    return None


def field_sql_column_name(attr_name: str, field_info: PydanticFieldInfo | Any) -> str:
    """Resolve physical SQL column name for a Pydantic/SQLModel field."""
    sa_col = field_sa_column(field_info)
    if sa_col is not None and isinstance(sa_col.name, str) and sa_col.name:
        return sa_col.name
    return attr_name


def field_is_primary_key(field_info: PydanticFieldInfo | Any) -> bool:
    """Return True when a field is marked as primary key."""
    if isinstance(field_info, SQLModelFieldInfo) and field_info.primary_key is True:
        return True
    sa_col = field_sa_column(field_info)
    return bool(sa_col is not None and sa_col.primary_key is True)


def field_is_json_column(field_info: PydanticFieldInfo | Any) -> bool:
    """Return True when a field's SQLAlchemy column type is ``JSON``."""
    sa_col = field_sa_column(field_info)
    return bool(sa_col is not None and isinstance(sa_col.type, SAJSON))


def field_duckdb_type(field_info: PydanticFieldInfo) -> str:
    """Map a Pydantic/SQLModel ``FieldInfo`` annotation + ``sa_column`` to a DuckDB SQL type."""
    sa_col = field_sa_column(field_info)
    if sa_col is not None and sa_col.type is not None:
        col_type_name = type(sa_col.type).__name__.upper()
        if "JSON" in col_type_name:
            return "JSON"
        if "FLOAT" in col_type_name or "DOUBLE" in col_type_name:
            return "DOUBLE"
        if "INT" in col_type_name:
            return "BIGINT"
        if "BOOL" in col_type_name:
            return "BOOLEAN"
        if "DATE" in col_type_name or "TIME" in col_type_name:
            return "TIMESTAMPTZ"
        if "STR" in col_type_name or "CHAR" in col_type_name or "TEXT" in col_type_name:
            return "VARCHAR"

    target: Any = field_info.annotation
    origin = get_origin(target)
    if origin is not None:
        non_none = [arg for arg in get_args(target) if arg is not type(None)]
        if len(non_none) == 1:
            target = non_none[0]
            origin = get_origin(target)

    if target is str:
        return "VARCHAR"
    if target is bool:
        return "BOOLEAN"
    if target is int:
        return "BIGINT"
    if target is float:
        return "DOUBLE"
    if target is uuid.UUID:
        return "UUID"
    if target is datetime:
        return "TIMESTAMPTZ"
    if origin in (dict, list, Mapping, Sequence) or target in (dict, list, Any):
        return "JSON"
    return "VARCHAR"


def field_foreign_key_target(field_info: PydanticFieldInfo | Any) -> str | None:
    """Extract ``table.column`` foreign-key target from a ``SQLModel`` field if present."""
    if (
        isinstance(field_info, SQLModelFieldInfo)
        and isinstance(field_info.foreign_key, str)
        and field_info.foreign_key
    ):
        return field_info.foreign_key
    sa_col = field_sa_column(field_info)
    if sa_col is None or not sa_col.foreign_keys:
        return None
    for fk in sa_col.foreign_keys:
        if isinstance(fk, SAForeignKey):
            if fk.target_fullname:
                return str(fk.target_fullname)
            try:
                col = fk.column
            except Exception:
                col = None
            if col is not None:
                return str(col)
    return None


def schema_model_user_columns(model_cls: type[BaseModel]) -> list[str]:
    """Return ordered non-``consist_*`` SQL column names for ``model_cls``."""
    cols: list[str] = []
    for attr_name, field_info in model_cls.model_fields.items():
        sql_name = field_sql_column_name(attr_name, field_info)
        if not sql_name.startswith("consist_"):
            cols.append(sql_name)
    return cols


def _normalize_annotation_types(annotation: Any) -> set[Any]:
    origin = get_origin(annotation)
    if origin is None:
        return {annotation}
    if origin in {dict, list, tuple}:
        return {origin}
    if origin is Annotated:
        args = get_args(annotation)
        return _normalize_annotation_types(args[0]) if args else {annotation}
    if origin is Union:
        args = get_args(annotation)
    else:
        args = get_args(annotation)
    if not args:
        return {annotation}
    normalized: set[Any] = set()
    for arg in args:
        if arg is type(None):
            continue
        normalized |= _normalize_annotation_types(arg)
    return normalized or {annotation}


def schema_model_dlt_columns(model_cls: type[BaseModel]) -> dict[str, dict[str, Any]]:
    """Build dlt column-hint dictionary from a Pydantic/SQLModel class."""
    columns: dict[str, dict[str, Any]] = {}
    for name, field in model_cls.model_fields.items():
        normalized = _normalize_annotation_types(field.annotation)
        dlt_type = "text"
        if field_is_json_column(field) or dict in normalized or list in normalized:
            dlt_type = "json"
        elif float in normalized:
            dlt_type = "double"
        elif int in normalized:
            dlt_type = "bigint"
        elif bool in normalized:
            dlt_type = "bool"
        columns[name] = {"data_type": dlt_type, "nullable": not field.is_required()}
    return columns
