from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence, Set as AbstractSet
from dataclasses import dataclass
import json
from typing import Any, Dict, Hashable, List, Optional, Type, Union, TYPE_CHECKING

from pydantic import BaseModel, TypeAdapter

from consist.core._schema_model_introspection import (
    field_is_json_column,
    field_sql_column_name,
    model_table_name,
    model_table_schema,
)
from consist.core.indexing import FacetIndex, IndexBySpec, RunFieldIndex
from consist.core.run_matching import find_matching_runs_for_tracker
from consist.models.run import Run
from consist.models.run_config_kv import RunConfigKV
from consist.types import ParameterizedSQL

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager
    from consist.core.tracker import Tracker


_VALID_PIVOT_VALUE_COLUMNS = frozenset(
    {"value_num", "value_str", "value_bool", "value_json", "auto"}
)


def _quote_ident(ident: str) -> str:
    return '"' + str(ident).replace('"', '""') + '"'


@dataclass(frozen=True)
class _TableSpec:
    schema_name: str
    table_name: str
    model_cls: Optional[Type[BaseModel]]
    columns: Optional[tuple[str, ...]]
    field_to_col: Optional[dict[str, str]]
    col_to_field: Optional[dict[str, str]]
    json_columns: frozenset[str] = frozenset()

    @property
    def qualified_sql(self) -> str:
        return f"{_quote_ident(self.schema_name)}.{_quote_ident(self.table_name)}"

    def has_column(self, name: str) -> bool:
        if self.columns is None:
            return True
        if self.col_to_field is not None and name in self.col_to_field:
            return True
        if self.field_to_col is not None and name in self.field_to_col:
            return True
        return name in self.columns

    def resolve_column_name(self, name: str) -> str:
        if self.field_to_col is not None and name in self.field_to_col:
            return self.field_to_col[name]
        if self.col_to_field is not None and name in self.col_to_field:
            return name
        if self.columns is not None and name in self.columns:
            return name
        if self.columns is not None:
            raise ValueError(
                f"Column {name!r} does not exist on table {self.table_name!r}."
            )
        return name


@dataclass(frozen=True)
class _ProjectionItem:
    kind: str  # "scalar" | "model" | "explicit_dict" | "wildcard_dict"
    start_idx: int
    width: int
    model_cls: Optional[Type[BaseModel]] = None
    col_names: tuple[str, ...] = ()
    col_to_field: Optional[dict[str, str]] = None
    json_columns: frozenset[str] = frozenset()


def _decode_json_if_str(val: Any) -> Any:
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return val
    return val


_KNOWN_CONFIG_MODELS_CACHE: Optional[dict[str, Type[BaseModel]]] = None


def _known_config_models_by_table() -> dict[str, Type[BaseModel]]:
    global _KNOWN_CONFIG_MODELS_CACHE
    if _KNOWN_CONFIG_MODELS_CACHE is not None:
        return _KNOWN_CONFIG_MODELS_CACHE

    from consist.models.activitysim import (
        ActivitySimCoefficientsCache,
        ActivitySimCoefficientTemplateRefsCache,
        ActivitySimConfigIngestRunLink,
        ActivitySimConstantsCache,
        ActivitySimProbabilitiesCache,
        ActivitySimProbabilitiesEntriesCache,
        ActivitySimProbabilitiesMetaEntriesCache,
    )
    from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink

    mapping: dict[str, Type[BaseModel]] = {}
    for cls in (
        ActivitySimConstantsCache,
        ActivitySimCoefficientsCache,
        ActivitySimCoefficientTemplateRefsCache,
        ActivitySimProbabilitiesCache,
        ActivitySimProbabilitiesEntriesCache,
        ActivitySimProbabilitiesMetaEntriesCache,
        ActivitySimConfigIngestRunLink,
        BeamConfigCache,
        BeamConfigIngestRunLink,
    ):
        mapping[model_table_name(cls)] = cls
    _KNOWN_CONFIG_MODELS_CACHE = mapping
    return mapping


def _inspect_model_spec(spec: Type[BaseModel]) -> _TableSpec:
    table_name = model_table_name(spec)
    schema_name = model_table_schema(spec, default="global_tables")
    field_to_col: dict[str, str] = {}
    col_to_field: dict[str, str] = {}
    json_cols: set[str] = set()
    for fname, finfo in spec.model_fields.items():
        cname = field_sql_column_name(fname, finfo)
        if field_is_json_column(finfo):
            json_cols.add(cname)
        field_to_col[fname] = cname
        col_to_field[cname] = fname

    return _TableSpec(
        schema_name=schema_name,
        table_name=table_name,
        model_cls=spec,
        columns=tuple(field_to_col.values()),
        field_to_col=field_to_col,
        col_to_field=col_to_field,
        json_columns=frozenset(json_cols),
    )


def _inspect_table_spec(
    spec: Type[BaseModel] | str,
    *,
    inferred_columns: Optional[tuple[str, ...]] = None,
) -> _TableSpec:
    if isinstance(spec, str):
        cleaned = spec.strip()
        if not cleaned:
            raise ValueError("Table identifier must be a non-empty string.")
        if "." in cleaned:
            raw_schema, raw_table = cleaned.split(".", 1)
            schema_name = raw_schema.strip().strip('"')
            table_name = raw_table.strip().strip('"')
        else:
            schema_name = "global_tables"
            table_name = cleaned.strip('"')
        if not schema_name or not table_name:
            raise ValueError(f"Invalid table identifier: {spec!r}")

        known_cls = (
            _known_config_models_by_table().get(table_name)
            if schema_name == "global_tables"
            else None
        )
        if known_cls is not None:
            known_spec = _inspect_model_spec(known_cls)
            return _TableSpec(
                schema_name=schema_name,
                table_name=table_name,
                model_cls=None,
                columns=known_spec.columns,
                field_to_col=known_spec.field_to_col,
                col_to_field=known_spec.col_to_field,
                json_columns=known_spec.json_columns,
            )

        if inferred_columns is not None:
            identity_map = {col: col for col in inferred_columns}
            return _TableSpec(
                schema_name=schema_name,
                table_name=table_name,
                model_cls=None,
                columns=inferred_columns,
                field_to_col=identity_map,
                col_to_field=identity_map,
            )

        return _TableSpec(
            schema_name=schema_name,
            table_name=table_name,
            model_cls=None,
            columns=None,
            field_to_col=None,
            col_to_field=None,
        )

    if isinstance(spec, type) and issubclass(spec, BaseModel):
        return _inspect_model_spec(spec)

    raise TypeError(
        f"Expected a SQLModel/Pydantic class or table name string, got {spec!r}"
    )


def _match_alias_for_target(
    target: Any,
    table_spec: _TableSpec,
    link_spec: _TableSpec,
) -> str:
    if isinstance(target, str):
        cleaned = target.strip().strip('"')
        if cleaned == "t" or cleaned == table_spec.table_name:
            return "t"
        if cleaned == "l" or cleaned == link_spec.table_name:
            return "l"
        if "." in cleaned:
            _, tail = cleaned.split(".", 1)
            tail = tail.strip().strip('"')
            if tail == table_spec.table_name:
                return "t"
            if tail == link_spec.table_name:
                return "l"
        raise ValueError(
            f"Unknown table alias or name {target!r}; expected 't', 'l', "
            f"{table_spec.table_name!r}, or {link_spec.table_name!r}."
        )
    if isinstance(target, type) and issubclass(target, BaseModel):
        if target is table_spec.model_cls:
            return "t"
        if target is link_spec.model_cls:
            return "l"
        other_spec = _inspect_model_spec(target)
        if other_spec.table_name == table_spec.table_name:
            return "t"
        if other_spec.table_name == link_spec.table_name:
            return "l"
    raise ValueError(
        f"Could not match column target {target!r} to table or link_table."
    )


def _resolve_qualified_column(
    spec: Any,
    table_spec: _TableSpec,
    link_spec: _TableSpec,
) -> tuple[str, str]:
    """Resolve a column reference into ``(alias, db_col_name)``."""
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql.elements import ClauseElement
    from sqlalchemy.sql.schema import Column as SAColumn

    if isinstance(spec, InstrumentedAttribute):
        alias = _match_alias_for_target(spec.class_, table_spec, link_spec)
        owner = table_spec if alias == "t" else link_spec
        return alias, owner.resolve_column_name(str(spec.key))

    if isinstance(spec, SAColumn):
        alias = _match_alias_for_target(spec.table.name, table_spec, link_spec)
        owner = table_spec if alias == "t" else link_spec
        return alias, owner.resolve_column_name(str(spec.name))

    if isinstance(spec, ClauseElement):
        raise TypeError(
            "SQLAlchemy expressions are not supported in Consist v0.5 config queries. "
            "Pass column names, (table, column) pairs, or SQLModel column attributes."
        )

    if isinstance(spec, tuple) and len(spec) == 2:
        target, col_raw = spec
        if not isinstance(col_raw, str) or not col_raw:
            raise TypeError(
                f"Expected (table_or_alias, column_name) with string column_name, got {spec!r}"
            )
        alias = _match_alias_for_target(target, table_spec, link_spec)
        owner = table_spec if alias == "t" else link_spec
        return alias, owner.resolve_column_name(col_raw)

    if isinstance(spec, str):
        cleaned = spec.strip()
        if not cleaned:
            raise ValueError("Column name must be non-empty.")
        if "." in cleaned:
            prefix, col_part = cleaned.rsplit(".", 1)
            alias = _match_alias_for_target(prefix, table_spec, link_spec)
            owner = table_spec if alias == "t" else link_spec
            return alias, owner.resolve_column_name(col_part.strip().strip('"'))

        col_clean = cleaned.strip('"')
        in_t = (
            table_spec.has_column(col_clean)
            if table_spec.columns is not None
            else False
        )
        in_l = (
            link_spec.has_column(col_clean) if link_spec.columns is not None else False
        )
        if in_t and not in_l:
            return "t", table_spec.resolve_column_name(col_clean)
        if in_l and not in_t:
            return "l", link_spec.resolve_column_name(col_clean)
        if in_t and in_l:
            return "t", table_spec.resolve_column_name(col_clean)
        if table_spec.columns is None and link_spec.columns is None:
            if col_clean in {"run_id", "table_name"}:
                return "l", col_clean
            return "t", col_clean
        if table_spec.columns is None and not in_l:
            return "t", col_clean
        if link_spec.columns is None and not in_t:
            return "l", col_clean
        raise ValueError(
            f"Column {col_clean!r} was not found on {table_spec.table_name!r} or {link_spec.table_name!r}."
        )

    raise TypeError(f"Unsupported column specification: {spec!r}")


def _compile_where_clause(
    clause: Any,
    table_spec: _TableSpec,
    link_spec: _TableSpec,
    sql_parts: list[str],
    params: list[Any],
) -> None:
    from sqlalchemy.sql.elements import ClauseElement

    if clause is None:
        return

    if isinstance(clause, ClauseElement):
        raise TypeError(
            "SQLAlchemy clause/expression objects are not supported in config_run_query(..., where=...) "
            "in Consist v0.5. Pass a dict mapping column names to values (e.g. {'key': 'x'}), "
            "ParameterizedSQL(sql, params), or a (sql_fragment, params) tuple."
        )

    if isinstance(clause, ParameterizedSQL):
        frag = clause.sql.strip()
        if frag:
            sql_parts.append(f"({frag})")
            params.extend(clause.params)
        return

    if isinstance(clause, Mapping):
        for raw_key, raw_val in clause.items():
            alias, col_name = _resolve_qualified_column(raw_key, table_spec, link_spec)
            q_col = f"{alias}.{_quote_ident(col_name)}"
            if raw_val is None:
                sql_parts.append(f"{q_col} IS NULL")
            elif isinstance(raw_val, (Sequence, AbstractSet)) and not isinstance(
                raw_val, (str, bytes, bytearray)
            ):
                items = list(raw_val)
                if not items:
                    sql_parts.append("1 = 0")
                else:
                    non_null = [v for v in items if v is not None]
                    has_null = any(v is None for v in items)
                    sub_parts: list[str] = []
                    if non_null:
                        placeholders = ", ".join("?" for _ in non_null)
                        sub_parts.append(f"{q_col} IN ({placeholders})")
                        params.extend(non_null)
                    if has_null:
                        sub_parts.append(f"{q_col} IS NULL")
                    if len(sub_parts) == 1:
                        sql_parts.append(sub_parts[0])
                    else:
                        sql_parts.append(f"({' OR '.join(sub_parts)})")
            else:
                sql_parts.append(f"{q_col} = ?")
                params.append(raw_val)
        return

    if (
        isinstance(clause, tuple)
        and len(clause) == 2
        and isinstance(clause[0], str)
        and isinstance(clause[1], Sequence)
        and not isinstance(clause[1], (str, bytes, bytearray))
    ):
        frag = clause[0].strip()
        if frag:
            sql_parts.append(f"({frag})")
            params.extend(clause[1])
        return

    if isinstance(clause, str):
        raise TypeError(
            "Raw SQL strings are not accepted directly in where=... without bound parameters; "
            "pass ParameterizedSQL(sql, params) or (sql_fragment, params)."
        )

    if isinstance(clause, Iterable):
        for item in clause:
            _compile_where_clause(item, table_spec, link_spec, sql_parts, params)
        return

    raise TypeError(
        f"Unsupported where predicate {clause!r}. Pass a Mapping, ParameterizedSQL, "
        "or (sql_fragment, params) tuple."
    )


def _normalize_columns_arg(
    columns: Iterable[Any], *, valid_qual_names: AbstractSet[str]
) -> list[Any]:
    from sqlalchemy.orm.attributes import InstrumentedAttribute
    from sqlalchemy.sql.elements import ClauseElement

    if isinstance(columns, (str, bytes)):
        return [columns]
    if isinstance(columns, (InstrumentedAttribute, ClauseElement)):
        return [columns]
    if isinstance(columns, type) and issubclass(columns, BaseModel):
        return [columns]
    if (
        isinstance(columns, tuple)
        and len(columns) == 2
        and (
            (isinstance(columns[0], type) and issubclass(columns[0], BaseModel))
            or (
                isinstance(columns[0], str)
                and (
                    columns[0].strip().strip('"') in valid_qual_names
                    or (
                        "." in columns[0]
                        and columns[0].split(".", 1)[1].strip().strip('"')
                        in valid_qual_names
                    )
                )
            )
        )
        and isinstance(columns[1], str)
    ):
        return [columns]
    return list(columns)


def _compile_config_run_plan(
    table: Type[BaseModel] | str,
    *,
    link_table: Type[BaseModel] | str,
    table_name: Optional[str] = None,
    columns: Optional[Iterable[Any]] = None,
    where: Optional[Any] = None,
    join_on: Optional[Iterable[str]] = None,
    inferred_table_columns: Optional[tuple[str, ...]] = None,
    inferred_link_columns: Optional[tuple[str, ...]] = None,
) -> tuple[ParameterizedSQL, list[_ProjectionItem], bool]:
    table_spec = _inspect_table_spec(table, inferred_columns=inferred_table_columns)
    link_spec = _inspect_table_spec(link_table, inferred_columns=inferred_link_columns)
    valid_qual_names = frozenset(
        {"t", "l", table_spec.table_name, link_spec.table_name}
    )

    projections: list[_ProjectionItem] = []
    select_exprs: list[str] = []
    unwrap_single = False

    if columns is None:
        if link_spec.columns is not None and not link_spec.has_column("run_id"):
            raise ValueError(
                "config_run_query requires link_table to have a run_id column "
                "when columns is not specified."
            )
        run_id_col = link_spec.resolve_column_name("run_id")
        select_exprs.append(f'l.{_quote_ident(run_id_col)} AS "run_id"')
        projections.append(
            _ProjectionItem(
                kind="scalar",
                start_idx=0,
                width=1,
                col_names=(run_id_col,),
                json_columns=(
                    frozenset({run_id_col})
                    if run_id_col in link_spec.json_columns
                    else frozenset()
                ),
            )
        )

        if table_spec.model_cls is not None and table_spec.columns is not None:
            for c in table_spec.columns:
                select_exprs.append(f"t.{_quote_ident(c)} AS {_quote_ident(c)}")
            projections.append(
                _ProjectionItem(
                    kind="model",
                    start_idx=1,
                    width=len(table_spec.columns),
                    model_cls=table_spec.model_cls,
                    col_names=table_spec.columns,
                    col_to_field=table_spec.col_to_field,
                    json_columns=table_spec.json_columns,
                )
            )
        elif table_spec.columns is not None and len(table_spec.columns) > 0:
            for c in table_spec.columns:
                select_exprs.append(f"t.{_quote_ident(c)} AS {_quote_ident(c)}")
            projections.append(
                _ProjectionItem(
                    kind="explicit_dict",
                    start_idx=1,
                    width=len(table_spec.columns),
                    col_names=table_spec.columns,
                    json_columns=table_spec.json_columns,
                )
            )
        else:
            select_exprs.append("t.*")
            projections.append(
                _ProjectionItem(kind="wildcard_dict", start_idx=1, width=-1)
            )
    else:
        col_items = _normalize_columns_arg(columns, valid_qual_names=valid_qual_names)
        if not col_items:
            raise ValueError("config_run_query requires at least one column.")
        unwrap_single = len(col_items) == 1
        cursor_idx = 0
        for item in col_items:
            if isinstance(item, type) and issubclass(item, BaseModel):
                alias = _match_alias_for_target(item, table_spec, link_spec)
                owner = table_spec if alias == "t" else link_spec
                model_spec = (
                    owner if owner.model_cls is item else _inspect_model_spec(item)
                )
                assert model_spec.columns is not None
                for c in model_spec.columns:
                    select_exprs.append(
                        f"{alias}.{_quote_ident(c)} AS {_quote_ident(c)}"
                    )
                projections.append(
                    _ProjectionItem(
                        kind="model",
                        start_idx=cursor_idx,
                        width=len(model_spec.columns),
                        model_cls=item,
                        col_names=model_spec.columns,
                        col_to_field=model_spec.col_to_field,
                        json_columns=model_spec.json_columns,
                    )
                )
                cursor_idx += len(model_spec.columns)
            else:
                alias, c = _resolve_qualified_column(item, table_spec, link_spec)
                owner = table_spec if alias == "t" else link_spec
                select_exprs.append(f"{alias}.{_quote_ident(c)} AS {_quote_ident(c)}")
                projections.append(
                    _ProjectionItem(
                        kind="scalar",
                        start_idx=cursor_idx,
                        width=1,
                        col_names=(c,),
                        json_columns=(
                            frozenset({c}) if c in owner.json_columns else frozenset()
                        ),
                    )
                )
                cursor_idx += 1

    if join_on is None:
        join_cols = ["content_hash"]
        t_has_fn = (
            table_spec.has_column("file_name")
            if table_spec.columns is not None
            else False
        )
        l_has_fn = (
            link_spec.has_column("file_name")
            if link_spec.columns is not None
            else False
        )
        if t_has_fn and l_has_fn:
            join_cols.append("file_name")
    else:
        join_cols = list(join_on)
        if not join_cols:
            raise ValueError("config_run_query join_on requires at least one column.")

    join_predicates: list[str] = []
    for jcol in join_cols:
        if not table_spec.has_column(jcol) or not link_spec.has_column(jcol):
            raise ValueError(
                f"config_run_query join_on column {jcol!r} must exist on table and link_table."
            )
        t_col = table_spec.resolve_column_name(jcol)
        l_col = link_spec.resolve_column_name(jcol)
        join_predicates.append(f"t.{_quote_ident(t_col)} = l.{_quote_ident(l_col)}")

    where_parts: list[str] = []
    params: list[Any] = []

    resolved_table_name = table_name
    l_has_table_name = (
        link_spec.has_column("table_name") if link_spec.columns is not None else False
    )
    if resolved_table_name is None and l_has_table_name:
        resolved_table_name = table_spec.table_name
    if resolved_table_name is not None:
        if link_spec.columns is not None and not link_spec.has_column("table_name"):
            raise ValueError(
                "config_run_query table_name provided but link_table has no table_name column."
            )
        tn_col = link_spec.resolve_column_name("table_name")
        where_parts.append(f"l.{_quote_ident(tn_col)} = ?")
        params.append(resolved_table_name)

    if where is not None:
        _compile_where_clause(where, table_spec, link_spec, where_parts, params)

    sql = (
        f"SELECT {', '.join(select_exprs)} "
        f"FROM {table_spec.qualified_sql} AS t "
        f"JOIN {link_spec.qualified_sql} AS l ON {' AND '.join(join_predicates)}"
    )
    if where_parts:
        sql += f" WHERE {' AND '.join(where_parts)}"

    return ParameterizedSQL(sql=sql, params=tuple(params)), projections, unwrap_single


def build_config_run_query(
    table: Type[BaseModel] | str,
    *,
    link_table: Type[BaseModel] | str,
    table_name: Optional[str] = None,
    columns: Optional[Iterable[Any]] = None,
    where: Optional[Any] = None,
    join_on: Optional[Iterable[str]] = None,
    db: Optional["DatabaseManager"] = None,
) -> ParameterizedSQL:
    """Build a parameterized DuckDB SQL query joining a config cache table to its run-link table."""
    from consist.core.persistence import _validate_select_only_sql

    inferred_table_columns: Optional[tuple[str, ...]] = None
    inferred_link_columns: Optional[tuple[str, ...]] = None
    if isinstance(link_table, str):
        l_pre = _inspect_table_spec(link_table)
        if l_pre.columns is None:
            if db is None:
                if table_name is None:
                    raise ValueError(
                        f"Cannot infer whether unknown string link_table {link_table!r} "
                        "has a 'table_name' column without a database connection. "
                        "Pass tracker=... (or use tracker.config_run_query(...)) or "
                        "specify table_name=... explicitly."
                    )
            else:
                inferred_link_columns = _introspect_db_table_columns(db, l_pre)
    if db is not None and isinstance(table, str):
        t_pre = _inspect_table_spec(table)
        if t_pre.columns is None:
            inferred_table_columns = _introspect_db_table_columns(db, t_pre)

    query, _, _ = _compile_config_run_plan(
        table,
        link_table=link_table,
        table_name=table_name,
        columns=columns,
        where=where,
        join_on=join_on,
        inferred_table_columns=inferred_table_columns,
        inferred_link_columns=inferred_link_columns,
    )
    _validate_select_only_sql(query.sql)
    return query


def _introspect_db_table_columns(
    db: "DatabaseManager", spec: _TableSpec
) -> Optional[tuple[str, ...]]:
    rows = db.fetchall(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = current_database() "
        "AND table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [spec.schema_name, spec.table_name],
    )
    if not rows:
        return None
    return tuple(str(r[0]) for r in rows)


def execute_config_run_rows(
    db_or_tracker: "DatabaseManager | Tracker",
    table: Type[BaseModel] | str,
    *,
    link_table: Type[BaseModel] | str,
    table_name: Optional[str] = None,
    columns: Optional[Iterable[Any]] = None,
    where: Optional[Any] = None,
    join_on: Optional[Iterable[str]] = None,
) -> list[Any]:
    """Execute a config-to-run join query on ``DatabaseManager`` and hydrate result rows."""
    from consist.core.persistence import DatabaseManager, _validate_select_only_sql
    from consist.core.tracker import Tracker

    if isinstance(db_or_tracker, DatabaseManager):
        db: Optional[DatabaseManager] = db_or_tracker
    elif isinstance(db_or_tracker, Tracker):
        db = db_or_tracker.db
    else:
        try:
            db = db_or_tracker.db  # type: ignore[attr-defined]
        except AttributeError:
            db = None
    if db is None:
        raise RuntimeError("Database connection required.")

    inferred_table_columns: Optional[tuple[str, ...]] = None
    if isinstance(table, str):
        t_pre = _inspect_table_spec(table)
        if t_pre.columns is None:
            inferred_table_columns = _introspect_db_table_columns(db, t_pre)

    inferred_link_columns: Optional[tuple[str, ...]] = None
    if isinstance(link_table, str):
        l_pre = _inspect_table_spec(link_table)
        if l_pre.columns is None:
            inferred_link_columns = _introspect_db_table_columns(db, l_pre)

    query, projections, unwrap_single = _compile_config_run_plan(
        table,
        link_table=link_table,
        table_name=table_name,
        columns=columns,
        where=where,
        join_on=join_on,
        inferred_table_columns=inferred_table_columns,
        inferred_link_columns=inferred_link_columns,
    )
    _validate_select_only_sql(query.sql)

    with db.exec_cursor(include_temp_views=True) as cur:
        res = cur.execute(query.sql, list(query.params))
        description = res.description or ()
        raw_rows = res.fetchall()

    col_labels = [str(desc[0]) for desc in description]
    col_is_db_json = [str(desc[1]) == "JSON" for desc in description]
    adapters: dict[Type[BaseModel], TypeAdapter[Any]] = {
        proj.model_cls: TypeAdapter(proj.model_cls)
        for proj in projections
        if proj.kind == "model" and proj.model_cls is not None
    }

    def _hydrate_cell(
        proj: _ProjectionItem, offset: int, cname: Optional[str], val: Any
    ) -> Any:
        col_idx = proj.start_idx + offset
        is_json = (
            (cname is not None and cname in proj.json_columns)
            or (cname is None and bool(proj.json_columns))
            or (col_idx < len(col_is_db_json) and col_is_db_json[col_idx])
        )
        return _decode_json_if_str(val) if is_json else val

    results: list[Any] = []
    for raw_row in raw_rows:
        built: list[Any] = []
        for proj in projections:
            if proj.kind == "scalar":
                built.append(_hydrate_cell(proj, 0, None, raw_row[proj.start_idx]))
            elif proj.kind == "model" and proj.model_cls is not None:
                slice_vals = raw_row[proj.start_idx : proj.start_idx + proj.width]
                col_map = proj.col_to_field or {}
                payload = {
                    col_map.get(cname, cname): _hydrate_cell(proj, offset, cname, val)
                    for offset, (cname, val) in enumerate(
                        zip(proj.col_names, slice_vals)
                    )
                }
                built.append(adapters[proj.model_cls].validate_python(payload))
            elif proj.kind == "explicit_dict":
                slice_vals = raw_row[proj.start_idx : proj.start_idx + proj.width]
                built.append(
                    {
                        cname: _hydrate_cell(proj, offset, cname, val)
                        for offset, (cname, val) in enumerate(
                            zip(proj.col_names, slice_vals)
                        )
                    }
                )
            elif proj.kind == "wildcard_dict":
                slice_vals = raw_row[proj.start_idx :]
                slice_names = col_labels[proj.start_idx :]
                built.append(
                    {
                        cname: _hydrate_cell(proj, offset, cname, val)
                        for offset, (cname, val) in enumerate(
                            zip(slice_names, slice_vals)
                        )
                    }
                )
        results.append(built[0] if unwrap_single else tuple(built))
    return results


def build_pivot_facets(
    *,
    namespace: Optional[str] = None,
    keys: Iterable[str],
    value_column: str = "value_num",
    value_columns: Optional[Mapping[str, str]] = None,
    label_prefix: str = "",
    label_map: Optional[Mapping[str, str]] = None,
    run_id_label: str = "run_id",
    table: Any = None,
) -> ParameterizedSQL:
    """Build a parameterized DuckDB SQL query pivoting ``main.run_config_kv`` keys into columns."""
    keys_list = list(dict.fromkeys(keys))
    if not keys_list:
        raise ValueError("pivot_facets requires at least one key")

    if value_column not in _VALID_PIVOT_VALUE_COLUMNS:
        raise ValueError(
            f"pivot_facets value_column must be one of {sorted(_VALID_PIVOT_VALUE_COLUMNS)}"
        )

    resolved_value_columns = dict(value_columns or {})
    resolved_label_map = dict(label_map or {})

    select_parts: list[str] = [f"kv.run_id AS {_quote_ident(run_id_label)}"]
    params: list[Any] = []

    for key in keys_list:
        col_choice = resolved_value_columns.get(key, value_column)
        if col_choice not in _VALID_PIVOT_VALUE_COLUMNS:
            raise ValueError(
                f"pivot_facets value_columns[{key!r}] must be one of {sorted(_VALID_PIVOT_VALUE_COLUMNS)}"
            )
        label = resolved_label_map.get(key, f"{label_prefix}{key}")
        q_label = _quote_ident(label)
        if col_choice == "value_num":
            expr = f"MAX(CASE WHEN kv.key = ? THEN kv.value_num END) AS {q_label}"
        elif col_choice == "value_str":
            expr = f"MAX(CASE WHEN kv.key = ? THEN kv.value_str END) AS {q_label}"
        elif col_choice == "value_bool":
            expr = f"BOOL_OR(CASE WHEN kv.key = ? THEN kv.value_bool END) AS {q_label}"
        elif col_choice == "value_json":
            expr = (
                "CAST(MAX(CASE WHEN kv.key = ? THEN CAST(kv.value_json AS VARCHAR) END) "
                f"AS JSON) AS {q_label}"
            )
        else:
            expr = (
                "MAX(CASE WHEN kv.key = ? THEN COALESCE("
                "kv.value_str, "
                "CAST(kv.value_num AS VARCHAR), "
                "CAST(kv.value_bool AS VARCHAR), "
                "CAST(kv.value_json AS VARCHAR)"
                f") END) AS {q_label}"
            )
        select_parts.append(expr)
        params.append(key)

    key_placeholders = ", ".join("?" for _ in keys_list)
    where_parts = [f"kv.key IN ({key_placeholders})"]
    params.extend(keys_list)

    if namespace is not None:
        where_parts.append("kv.namespace = ?")
        params.append(namespace)

    if table is None or table is RunConfigKV:
        from_target = "main.run_config_kv"
    elif isinstance(table, str):
        cleaned = table.strip()
        if "." in cleaned:
            raw_sc, raw_tb = cleaned.split(".", 1)
            sc_clean = raw_sc.strip().strip('"')
            tb_clean = raw_tb.strip().strip('"')
            from_target = f"{_quote_ident(sc_clean)}.{_quote_ident(tb_clean)}"
        else:
            tb_clean = cleaned.strip('"')
            from_target = f"main.{_quote_ident(tb_clean)}"
    else:
        resolved_spec = _inspect_table_spec(table)
        from_target = (
            f"{_quote_ident(resolved_spec.schema_name)}."
            f"{_quote_ident(resolved_spec.table_name)}"
        )

    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {from_target} AS kv "
        f"WHERE {' AND '.join(where_parts)} "
        "GROUP BY kv.run_id "
        "ORDER BY kv.run_id ASC"
    )
    return ParameterizedSQL(sql=sql, params=tuple(params))


def _get_run_field(run: Run, field_name: str) -> Any:
    if field_name not in Run.model_fields:
        raise AttributeError(f"'Run' object has no attribute {field_name!r}")
    return run.model_dump()[field_name]


class RunQueryService:
    """
    Query and config-diff helpers for Tracker-backed run metadata.
    """

    def __init__(self, tracker: "Tracker") -> None:
        self._tracker = tracker

    def config_run_query(
        self,
        table: Type[BaseModel] | str,
        *,
        link_table: Type[BaseModel] | str,
        table_name: Optional[str] = None,
        columns: Optional[Iterable[Any]] = None,
        where: Optional[Any] = None,
        join_on: Optional[Iterable[str]] = None,
    ) -> ParameterizedSQL:
        return build_config_run_query(
            table,
            link_table=link_table,
            table_name=table_name,
            columns=columns,
            where=where,
            join_on=join_on,
            db=self._tracker.db,
        )

    def config_run_rows(
        self,
        table: Type[BaseModel] | str,
        *,
        link_table: Type[BaseModel] | str,
        table_name: Optional[str] = None,
        columns: Optional[Iterable[Any]] = None,
        where: Optional[Any] = None,
        join_on: Optional[Iterable[str]] = None,
    ) -> list[Any]:
        return execute_config_run_rows(
            self._tracker,
            table,
            link_table=link_table,
            table_name=table_name,
            columns=columns,
            where=where,
            join_on=join_on,
        )

    def pivot_facets(
        self,
        *,
        namespace: Optional[str] = None,
        keys: Iterable[str],
        value_column: str = "value_num",
        value_columns: Optional[Mapping[str, str]] = None,
        label_prefix: str = "",
        label_map: Optional[Mapping[str, str]] = None,
        run_id_label: str = "run_id",
    ) -> ParameterizedSQL:
        return build_pivot_facets(
            namespace=namespace,
            keys=keys,
            value_column=value_column,
            value_columns=value_columns,
            label_prefix=label_prefix,
            label_map=label_map,
            run_id_label=run_id_label,
        )

    def find_runs(
        self,
        tags: Optional[List[str]] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        phase: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        parent_id: Optional[str] = None,
        facet: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = 100,
        index_by: Optional[Union[str, IndexBySpec]] = None,
        name: Optional[str] = None,
        run_scope: Optional[str] = None,
        raise_on_error: bool = True,
        *,
        offset: int = 0,
    ) -> Union[List[Run], Dict[Hashable, Run]]:
        runs: List[Run] = []
        if self._tracker.db:
            runs = self._tracker.db.find_runs(
                tags,
                year,
                iteration,
                stage,
                phase,
                model,
                status,
                parent_id,
                facet,
                metadata,
                limit,
                name,
                run_scope,
                raise_on_error,
                offset=offset,
            )

        if index_by:
            if isinstance(index_by, FacetIndex):
                facet_key = index_by.key
                if not self._tracker.db:
                    return {}
                values_by_run = self._tracker.db.get_facet_values_for_runs(
                    [r.id for r in runs],
                    key=facet_key,
                    namespace=model,
                )
                return {values_by_run[r.id]: r for r in runs if r.id in values_by_run}

            if isinstance(index_by, RunFieldIndex):
                return {_get_run_field(r, index_by.field): r for r in runs}

            if isinstance(index_by, str) and (
                index_by.startswith("facet.") or index_by.startswith("facet:")
            ):
                if not self._tracker.db:
                    return {}
                facet_key = (
                    index_by.split(".", 1)[1]
                    if index_by.startswith("facet.")
                    else index_by.split(":", 1)[1]
                )
                values_by_run = self._tracker.db.get_facet_values_for_runs(
                    [r.id for r in runs],
                    key=facet_key,
                    namespace=model,
                )
                return {values_by_run[r.id]: r for r in runs if r.id in values_by_run}

            if not isinstance(index_by, str):
                raise TypeError(f"Unsupported index_by type: {type(index_by)}")
            return {_get_run_field(r, index_by): r for r in runs}

        return runs

    def find_run(self, **kwargs: Any) -> Run:
        run_id = kwargs.pop("id", None) or kwargs.pop("run_id", None)

        if run_id:
            run = self._tracker.get_run(run_id)
            if not run:
                raise ValueError(f"No run found with ID: {run_id}")
            return run

        kwargs["limit"] = 2
        results = self.find_runs(**kwargs)

        if not results:
            raise ValueError(f"No run found matching criteria: {kwargs}")

        if len(results) > 1:
            raise ValueError(
                f"Multiple runs ({len(results)}+) found matching criteria: {kwargs}. Narrow your search."
            )

        assert isinstance(results, list)
        return results[0]

    def find_latest_run(
        self,
        *,
        parent_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        stage: Optional[str] = None,
        phase: Optional[str] = None,
        tags: Optional[List[str]] = None,
        facet: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 10_000,
        name: Optional[str] = None,
    ) -> Run:
        runs_result = self.find_runs(
            parent_id=parent_id,
            model=model,
            status=status,
            year=year,
            iteration=iteration,
            stage=stage,
            phase=phase,
            tags=tags,
            facet=facet,
            metadata=metadata,
            limit=limit,
            name=name,
        )
        runs = (
            list(runs_result.values()) if isinstance(runs_result, dict) else runs_result
        )
        if not runs:
            raise ValueError("No runs found matching criteria for find_latest_run().")

        def _latest_key(run: Run) -> tuple[int, int, float, str]:
            has_iteration = 1 if run.iteration is not None else 0
            iteration_value = run.iteration if run.iteration is not None else -1
            created_at_ts = run.created_at.timestamp()
            return (has_iteration, iteration_value, created_at_ts, run.id)

        return max(runs, key=_latest_key)

    def find_matching_runs(
        self,
        *,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[Union[int, str]] = None,
        iteration: Optional[Union[int, str]] = None,
        stage: Optional[str] = None,
        phase: Optional[str] = None,
        cache_epoch: Optional[Union[int, str]] = None,
        run_scope: Optional[str] = None,
        parent_id: Optional[str] = None,
        facet: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        allow_missing_cache_epoch: bool = False,
    ) -> List[Run]:
        return find_matching_runs_for_tracker(
            self.find_runs,
            model=model,
            status=status,
            year=year,
            iteration=iteration,
            stage=stage,
            phase=phase,
            cache_epoch=cache_epoch,
            run_scope=run_scope,
            parent_id=parent_id,
            facet=facet,
            limit=limit,
            allow_missing_cache_epoch=allow_missing_cache_epoch,
        )

    def find_matching_run(self, **filters: Any) -> Optional[Run]:
        matches = self.find_matching_runs(**filters)
        if not matches:
            return None
        return matches[0]

    def get_latest_run_id(self, **kwargs: Any) -> str:
        return self.find_latest_run(**kwargs).id

    def get_config_facet(self, facet_id: str):
        if not self._tracker.db:
            return None
        return self._tracker.db.get_config_facet(facet_id)

    def get_config_facets(
        self,
        *,
        namespace: Optional[str] = None,
        schema_name: Optional[str] = None,
        limit: int = 100,
    ):
        if not self._tracker.db:
            return []
        return self._tracker.db.get_config_facets(
            namespace=namespace, schema_name=schema_name, limit=limit
        )

    def get_run_config_kv(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ):
        if not self._tracker.db:
            return []
        return self._tracker.db.get_run_config_kv(
            run_id, namespace=namespace, prefix=prefix, limit=limit
        )

    def get_run_config_kv_for_runs(
        self,
        run_ids: Iterable[str],
        *,
        namespace: Optional[str] = None,
        keys: Optional[Iterable[str]] = None,
        limit: int = 100_000,
    ) -> List[RunConfigKV]:
        if not self._tracker.db:
            return []

        run_id_list = list(run_ids)
        if not run_id_list:
            return []

        key_list = list(dict.fromkeys(keys)) if keys is not None else None
        return self._tracker.db.get_run_config_kv_for_runs(
            run_id_list,
            namespace=namespace,
            keys=key_list,
            limit=limit,
        )

    def _resolve_config_namespace(
        self, run_id: str, namespace: Optional[str]
    ) -> Optional[str]:
        if namespace is not None:
            return namespace
        run = self._tracker.get_run(run_id)
        return run.model_name if run else None

    @staticmethod
    def _coerce_config_kv_value(row: RunConfigKV) -> Any:
        if row.value_type == "json":
            return row.value_json
        if row.value_type == "str":
            return row.value_str
        if row.value_type == "int":
            return int(row.value_num) if row.value_num is not None else None
        if row.value_type == "float":
            return float(row.value_num) if row.value_num is not None else None
        if row.value_type == "bool":
            return row.value_bool
        return None

    def get_config_values(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        keys: Optional[Iterable[str]] = None,
        limit: int = 10_000,
        resolve_canonical: bool = True,
    ) -> Dict[str, Any]:
        resolved_namespace = self._resolve_config_namespace(run_id, namespace)
        rows = self.get_run_config_kv(
            run_id, namespace=resolved_namespace, prefix=prefix, limit=limit
        )
        if not rows and resolve_canonical:
            canonical_id = self._tracker.resolve_canonical_run_id(run_id)
            if canonical_id != run_id:
                resolved_namespace = self._resolve_config_namespace(
                    canonical_id, namespace
                )
                rows = self.get_run_config_kv(
                    canonical_id,
                    namespace=resolved_namespace,
                    prefix=prefix,
                    limit=limit,
                )
        if not rows:
            return {}

        if keys is not None:
            key_set = set(keys)
            rows = [row for row in rows if row.key in key_set]

        return {row.key: self._coerce_config_kv_value(row) for row in rows}

    def diff_runs(
        self,
        run_id_a: str,
        run_id_b: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        keys: Optional[Iterable[str]] = None,
        limit: int = 10_000,
        include_equal: bool = False,
    ) -> Dict[str, Any]:
        namespace_a = self._resolve_config_namespace(run_id_a, namespace)
        namespace_b = self._resolve_config_namespace(run_id_b, namespace)

        left = self.get_config_values(
            run_id_a,
            namespace=namespace_a,
            prefix=prefix,
            keys=keys,
            limit=limit,
        )
        right = self.get_config_values(
            run_id_b,
            namespace=namespace_b,
            prefix=prefix,
            keys=keys,
            limit=limit,
        )

        changes: Dict[str, Dict[str, Any]] = {}
        all_keys = set(left.keys()) | set(right.keys())
        for key in sorted(all_keys):
            in_left = key in left
            in_right = key in right
            left_val = left.get(key)
            right_val = right.get(key)
            if in_left and not in_right:
                status = "removed"
            elif in_right and not in_left:
                status = "added"
            elif left_val == right_val:
                status = "same"
            else:
                status = "changed"
            if status == "same" and not include_equal:
                continue
            changes[key] = {
                "left": left_val,
                "right": right_val,
                "status": status,
            }

        return {
            "namespace": {"left": namespace_a, "right": namespace_b},
            "changes": changes,
        }

    def get_config_value(
        self,
        run_id: str,
        key: str,
        *,
        namespace: Optional[str] = None,
        default: Any = None,
        resolve_canonical: bool = True,
    ) -> Any:
        resolved_namespace = self._resolve_config_namespace(run_id, namespace)
        rows = self.get_run_config_kv(
            run_id, namespace=resolved_namespace, prefix=key, limit=10_000
        )
        if not rows and resolve_canonical:
            canonical_id = self._tracker.resolve_canonical_run_id(run_id)
            if canonical_id != run_id:
                resolved_namespace = self._resolve_config_namespace(
                    canonical_id, namespace
                )
                rows = self.get_run_config_kv(
                    canonical_id,
                    namespace=resolved_namespace,
                    prefix=key,
                    limit=10_000,
                )
        for row in rows:
            if row.key == key:
                return self._coerce_config_kv_value(row)
        return default

    def find_runs_by_facet_kv(
        self,
        *,
        namespace: str,
        key: str,
        value_type: Optional[str] = None,
        value_str: Optional[str] = None,
        value_num: Optional[float] = None,
        value_bool: Optional[bool] = None,
        limit: int = 100,
    ):
        if not self._tracker.db:
            return []
        return self._tracker.db.find_runs_by_facet_kv(
            namespace=namespace,
            key=key,
            value_type=value_type,
            value_str=value_str,
            value_num=value_num,
            value_bool=value_bool,
            limit=limit,
        )
