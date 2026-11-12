from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from typing import Annotated, Any, get_args, get_origin
import uuid

from pydantic import BaseModel
from pydantic.fields import FieldInfo as PydanticFieldInfo


def _field_extra_mapping(field_info: PydanticFieldInfo | Any) -> Mapping[str, Any]:
    extra = (
        field_info.json_schema_extra
        if isinstance(field_info, PydanticFieldInfo)
        else None
    )
    return extra if isinstance(extra, Mapping) else {}


def optional_model_table_name(model_cls: type[BaseModel]) -> str | None:
    """Return explicit/declared SQL table name for ``BaseModel`` / ``SQLModel`` classes, else ``None``."""
    if not isinstance(model_cls, type) or not issubclass(model_cls, BaseModel):
        return None
    try:
        raw = model_cls.__tablename__  # type: ignore[attr-defined]
    except AttributeError:
        raw = None
    if isinstance(raw, str) and raw:
        return raw
    cfg = model_cls.model_config
    if isinstance(cfg, Mapping):
        extra = cfg.get("json_schema_extra")
        if isinstance(extra, Mapping):
            tablename = extra.get("tablename") or extra.get("table_name")
            if isinstance(tablename, str) and tablename:
                return tablename
    return None


def model_table_name(
    model_cls: type[BaseModel], *, preserve_case_fallback: bool = False
) -> str:
    """Return resolved table name for a Pydantic ``BaseModel`` or ``SQLModel`` class."""
    explicit = optional_model_table_name(model_cls)
    if explicit is not None:
        return explicit
    return model_cls.__name__ if preserve_case_fallback else model_cls.__name__.lower()


def model_table_schema(model_cls: type[BaseModel], *, default: str = "main") -> str:
    """Return SQL schema from ``__table__``, ``__table_args__``, or ``model_config``, else ``default``."""
    if not isinstance(model_cls, type) or not issubclass(model_cls, BaseModel):
        return default

    cls_any: Any = model_cls
    try:
        table_obj = cls_any.__table__
    except AttributeError:
        table_obj = None
    if table_obj is not None:
        try:
            schema_val = table_obj.schema
        except AttributeError:
            schema_val = None
        if isinstance(schema_val, str) and schema_val:
            return schema_val

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

    cfg = model_cls.model_config
    if isinstance(cfg, Mapping):
        extra = cfg.get("json_schema_extra")
        if isinstance(extra, Mapping):
            schema_name = extra.get("schema")
            if isinstance(schema_name, str) and schema_name:
                return schema_name

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


def field_sa_column(field_info: PydanticFieldInfo | Any) -> Any:
    """Return underlying SQLAlchemy ``Column`` when attached to a ``SQLModel`` field."""
    try:
        sa_col = field_info.sa_column
    except AttributeError:
        return None
    if sa_col is None or sa_col.__class__.__name__ == " PydanticUndefinedType":
        return None
    if type(sa_col).__module__.startswith("sqlalchemy"):
        return sa_col
    return None


def field_sql_column_name(attr_name: str, field_info: PydanticFieldInfo | Any) -> str:
    """Resolve physical SQL column name for a Pydantic/SQLModel field."""
    sa_col = field_sa_column(field_info)
    if sa_col is not None:
        try:
            col_name = sa_col.name
        except AttributeError:
            col_name = None
        if isinstance(col_name, str) and col_name:
            return col_name
    extra = _field_extra_mapping(field_info)
    extra_col = extra.get("column_name")
    if isinstance(extra_col, str) and extra_col:
        return extra_col
    return attr_name


def field_is_primary_key(field_info: PydanticFieldInfo | Any) -> bool:
    """Return True when a field is marked as primary key."""
    try:
        if field_info.primary_key is True:
            return True
    except AttributeError:
        pass
    extra = _field_extra_mapping(field_info)
    if extra.get("primary_key") is True:
        return True
    sa_col = field_sa_column(field_info)
    if sa_col is not None:
        try:
            return bool(sa_col.primary_key is True)
        except AttributeError:
            return False
    return False


def _annotation_is_json_container(annotation: Any) -> bool:
    target = annotation
    origin = get_origin(target)
    if origin is Annotated:
        ann_args = get_args(target)
        if ann_args:
            target = ann_args[0]
            origin = get_origin(target)
    if origin is not None:
        non_none = [arg for arg in get_args(target) if arg is not type(None)]
        if len(non_none) == 1:
            target = non_none[0]
            origin = get_origin(target)
    return origin in (dict, list, Mapping, Sequence) or target in (dict, list, Any)


def field_is_json_column(field_info: PydanticFieldInfo | Any) -> bool:
    """Return True when a field maps to a JSON SQL column."""
    extra = _field_extra_mapping(field_info)
    if extra.get("json") is True:
        return True
    sa_col = field_sa_column(field_info)
    if sa_col is not None:
        try:
            col_type = sa_col.type
        except AttributeError:
            col_type = None
        if col_type is not None:
            return "JSON" in type(col_type).__name__.upper()
    if isinstance(field_info, PydanticFieldInfo):
        return _annotation_is_json_container(field_info.annotation)
    return False


def field_duckdb_type(field_info: PydanticFieldInfo) -> str:
    """Map a Pydantic/SQLModel ``FieldInfo`` annotation + column metadata to a DuckDB SQL type."""
    sa_col = field_sa_column(field_info)
    if sa_col is not None:
        try:
            col_type = sa_col.type
        except AttributeError:
            col_type = None
        if col_type is not None:
            col_type_name = type(col_type).__name__.upper()
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
            if (
                "STR" in col_type_name
                or "CHAR" in col_type_name
                or "TEXT" in col_type_name
            ):
                return "VARCHAR"

    if field_is_json_column(field_info):
        return "JSON"

    target: Any = field_info.annotation
    origin = get_origin(target)
    if origin is Annotated:
        ann_args = get_args(target)
        if ann_args:
            target = ann_args[0]
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
    if target is date:
        return "DATE"
    if target is datetime:
        return "TIMESTAMPTZ"
    if origin in (dict, list, Mapping, Sequence) or target in (dict, list, Any):
        return "JSON"
    return "VARCHAR"


def field_foreign_key_target(field_info: PydanticFieldInfo | Any) -> str | None:
    """Extract ``table.column`` foreign-key target from a Pydantic or SQLModel field if present."""
    try:
        fk_attr = field_info.foreign_key
    except AttributeError:
        fk_attr = None
    if isinstance(fk_attr, str) and fk_attr:
        return fk_attr

    extra = _field_extra_mapping(field_info)
    extra_fk = extra.get("foreign_key")
    if isinstance(extra_fk, str) and extra_fk:
        return extra_fk

    sa_col = field_sa_column(field_info)
    if sa_col is None:
        return None
    try:
        foreign_keys = sa_col.foreign_keys
    except AttributeError:
        return None
    if not foreign_keys:
        return None
    for fk in foreign_keys:
        try:
            target_fullname = fk.target_fullname
        except AttributeError:
            target_fullname = None
        if isinstance(target_fullname, str) and target_fullname:
            return target_fullname
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
    """Build ``dlt`` column-hint dictionary from a Pydantic ``BaseModel`` or ``SQLModel`` class."""
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
