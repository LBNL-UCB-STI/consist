from __future__ import annotations

from dataclasses import dataclass
import logging
import math
from pathlib import Path
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
)

import duckdb
from pydantic import BaseModel
from sqlmodel import SQLModel

from consist.core._schema_model_introspection import (
    model_table_name,
    schema_model_user_columns,
)
from consist.core.stores import get_hot_data_db
from consist.models.artifact import Artifact
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager
    from consist.core.tracker import Tracker

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(identifier: str, *, label: str) -> str:
    if not _SAFE_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(
            f"Invalid {label}. Only letters, numbers, and underscores are allowed, "
            "and the identifier must not start with a number."
        )
    return identifier


def _quote_ident(identifier: str) -> str:
    """Quote an identifier for DuckDB SQL (double-quote, escaping embedded quotes)."""
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_table(schema: str, table: str) -> str:
    safe_schema = _validate_identifier(schema, label="schema")
    safe_table = _validate_identifier(table, label="concept_key")
    return f"{_quote_ident(safe_schema)}.{_quote_ident(safe_table)}"


def _model_tablename(model: Type[BaseModel]) -> str:
    return model_table_name(model)


def _dlt_child_list_cte(
    db: DatabaseManager,
    child_name: str,
    alias: str,
    empty_array_type: str,
) -> str:
    """Build a ``dlt`` child-list aggregation CTE or a typed empty fallback CTE."""
    if db.has_table(child_name, table_schema="global_tables"):
        child_table = _qualified_table("global_tables", child_name)
        return f"""
            SELECT
                _dlt_parent_id,
                list(value ORDER BY _dlt_list_idx) AS {_quote_ident(alias)}
            FROM {child_table}
            GROUP BY _dlt_parent_id
        """
    return (
        "SELECT CAST(NULL AS VARCHAR) AS _dlt_parent_id, "
        f"CAST([] AS {empty_array_type}) AS {_quote_ident(alias)} WHERE FALSE"
    )


def _build_metadata_artifact_join(
    db: DatabaseManager,
    concept_key: str,
    table_alias: str,
) -> Tuple[Optional[str], list[Any]]:
    """Build ``JOIN main.artifact a`` SQL and parameters for metadata view tables."""
    has_art_col = db.has_table_column(
        table_name=concept_key,
        column_name="consist_artifact_id",
        table_schema="global_tables",
    )
    has_run_col = db.has_table_column(
        table_name=concept_key,
        column_name="consist_run_id",
        table_schema="global_tables",
    )
    if has_art_col and has_run_col:
        return (
            f"JOIN main.artifact a ON ("
            f"TRY_CAST({table_alias}.consist_artifact_id AS UUID) = a.id "
            f"OR ({table_alias}.consist_artifact_id IS NULL AND a.key = ? "
            f"AND CAST({table_alias}.consist_run_id AS VARCHAR) = a.run_id)"
            f")",
            [concept_key],
        )
    if has_art_col:
        return (
            f"JOIN main.artifact a ON TRY_CAST({table_alias}.consist_artifact_id AS UUID) = a.id",
            [],
        )
    if has_run_col:
        return (
            f"JOIN main.artifact a ON (a.key = ? "
            f"AND CAST({table_alias}.consist_run_id AS VARCHAR) = a.run_id)",
            [concept_key],
        )
    return None, []


def _safe_duckdb_path_literal(path_str: str) -> str:
    """Return a SQL-safe, quoted literal for a DuckDB file path."""
    resolved = Path(path_str).resolve()
    if not resolved.exists():
        raise ValueError(f"Path does not exist: {resolved}")
    escaped = str(resolved).replace("'", "''")
    return f"'{escaped}'"


def _safe_duckdb_string_literal(value: Optional[str]) -> str:
    """Return a SQL-safe string literal for DuckDB, escaping single quotes."""
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _safe_duckdb_literal(value: Any) -> str:
    """Return a SQL-safe DuckDB literal for scalar values."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "NULL"
        return str(value)
    return _safe_duckdb_string_literal(str(value))


def _facet_type_to_duckdb(value_types: List[str]) -> str:
    """Resolve a deterministic DuckDB type from observed ArtifactKV value types."""
    observed = {t for t in value_types if t and t != "null"}
    if not observed:
        return "VARCHAR"
    if observed == {"bool"}:
        return "BOOLEAN"
    if observed == {"int"}:
        return "BIGINT"
    if observed == {"float"} or observed == {"int", "float"}:
        return "DOUBLE"
    return "VARCHAR"


def _python_value_to_facet_type(value: Any) -> str:
    """Map a Python facet value to the tracked facet type labels."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    return "json" if isinstance(value, (dict, list, tuple)) else "str"


@dataclass(frozen=True)
class GroupedViewRecord:
    artifact: Artifact
    run: Run
    facet_values: Dict[str, Any]


def _extract_schema_model_columns(schema_model: Type[BaseModel]) -> List[str]:
    return schema_model_user_columns(schema_model)


class ViewFactory:
    """Factory class generating Hybrid Views in DuckDB."""

    def __init__(self, tracker: Tracker) -> None:
        self.tracker = tracker

    def _require_db(self) -> DatabaseManager:
        db = get_hot_data_db(self.tracker) or self.tracker.db
        if db is None:
            raise RuntimeError("Cannot create views: No database configured.")
        return db

    def create_view(
        self,
        view_name: str,
        concept_key: str,
        driver_filter: Optional[List[str]] = None,
        schema_model: Optional[Type[SQLModel]] = None,
    ) -> bool:
        """Create or replace a hybrid view (TEMP VIEW when tracker is read-only)."""
        return self.create_hybrid_view(
            view_name=view_name,
            concept_key=concept_key,
            driver_filter=driver_filter,
            schema_model=schema_model,
        )

    def create_grouped_hybrid_view(
        self,
        *,
        view_name: str,
        schema_id: Optional[str] = None,
        schema_ids: Optional[List[str]] = None,
        schema_compatible: bool = False,
        predicates: Optional[List[Dict[str, Any]]] = None,
        namespace: Optional[str] = None,
        drivers: Optional[List[str]] = None,
        attach_facets: Optional[List[str]] = None,
        include_system_columns: bool = True,
        mode: Literal["hybrid", "hot_only", "cold_only"] = "hybrid",
        if_exists: Literal["replace", "error"] = "replace",
        missing_files: Literal["warn", "error", "skip_silent"] = "warn",
        run_id: Optional[str] = None,
        parent_run_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
    ) -> bool:
        """Create a selector-driven hybrid view across many artifacts."""
        self._require_db()
        if self.tracker.db is None:
            raise RuntimeError("Cannot create grouped views: No database configured.")
        if schema_id is None and not schema_ids:
            raise ValueError(
                "create_grouped_hybrid_view requires schema_id or schema_ids."
            )
        if mode not in {"hybrid", "hot_only", "cold_only"}:
            raise ValueError(f"Unsupported grouped-view mode: {mode}")
        if if_exists not in {"replace", "error"}:
            raise ValueError(f"Unsupported if_exists policy: {if_exists}")
        if missing_files not in {"warn", "error", "skip_silent"}:
            raise ValueError(f"Unsupported missing_files policy: {missing_files}")

        facets = list(dict.fromkeys(attach_facets or []))
        selected = self.tracker.db.find_artifacts_for_grouped_view(
            schema_id=schema_id,
            schema_ids=schema_ids,
            schema_compatible=schema_compatible,
            predicates=predicates or [],
            namespace=namespace,
            drivers=drivers,
            run_id=run_id,
            parent_run_id=parent_run_id,
            model=model,
            status=status,
            year=year,
            iteration=iteration,
        )

        facet_values_by_artifact: Dict[Any, Dict[str, Dict[str, Any]]] = {}
        facet_types: Dict[str, List[str]] = {facet: [] for facet in facets}
        if facets and selected:
            artifact_ids = [artifact.id for artifact, _ in selected]
            facet_values_by_artifact = self.tracker.db.get_artifact_kv_values_bulk(
                artifact_ids=artifact_ids,
                key_paths=facets,
                namespace=namespace,
            )

        run_namespace = model
        if run_namespace is None and selected:
            model_names = {
                run.model_name
                for _, run in selected
                if isinstance(run.model_name, str) and run.model_name
            }
            if len(model_names) == 1:
                run_namespace = next(iter(model_names))

        facet_values_by_run: Dict[str, Dict[str, Any]] = {}
        if facets and selected and run_namespace is not None:
            run_ids = [run.id for _, run in selected if isinstance(run.id, str)]
            if run_ids:
                for facet in facets:
                    facet_values_by_run[facet] = (
                        self.tracker.db.get_facet_values_for_runs(
                            run_ids,
                            key=facet,
                            namespace=run_namespace,
                        )
                    )

        records: List[GroupedViewRecord] = []
        for artifact, run in selected:
            facet_values: Dict[str, Any] = {}
            art_facet_map = facet_values_by_artifact.get(artifact.id, {})
            for facet in facets:
                info = art_facet_map.get(facet)
                if info is None:
                    run_facet_map = facet_values_by_run.get(facet, {})
                    if run.id in run_facet_map:
                        run_value = run_facet_map.get(run.id)
                        facet_values[facet] = run_value
                        facet_types[facet].append(
                            _python_value_to_facet_type(run_value)
                        )
                    else:
                        facet_values[facet] = None
                        facet_types[facet].append("null")
                else:
                    facet_values[facet] = info.get("value")
                    facet_types[facet].append(str(info.get("type") or "null"))
            records.append(
                GroupedViewRecord(
                    artifact=artifact,
                    run=run,
                    facet_values=facet_values,
                )
            )

        facet_casts = {
            facet: _facet_type_to_duckdb(types) for facet, types in facet_types.items()
        }

        parts: List[str] = []
        if mode in {"hybrid", "hot_only"}:
            hot_sql = self._generate_grouped_hot_query(
                records=records,
                facets=facets,
                facet_casts=facet_casts,
                include_system_columns=include_system_columns,
            )
            if hot_sql:
                parts.append(hot_sql)

        if mode in {"hybrid", "cold_only"}:
            cold_sql = self._generate_grouped_cold_query_optimized(
                records=records,
                facets=facets,
                facet_casts=facet_casts,
                include_system_columns=include_system_columns,
                missing_files=missing_files,
            )
            if cold_sql:
                parts.append(cold_sql)

        if not parts:
            schema_cols: List[str] = []
            fallback_schema_ids: List[str] = []
            if schema_id is not None:
                fallback_schema_ids.append(schema_id)
            if schema_ids:
                fallback_schema_ids.extend(schema_ids)
            for sid in sorted(set(fallback_schema_ids)):
                schema_bundle = self.tracker.db.get_artifact_schema(schema_id=sid)
                if schema_bundle is None:
                    continue
                _schema_row, fields = schema_bundle
                schema_cols = [row.name for row in fields]
                break
            query = self._typed_empty_query(
                schema_columns=schema_cols,
                facets=facets,
                facet_casts=facet_casts,
                include_system_columns=include_system_columns,
            )
        else:
            query = "\nUNION ALL BY NAME\n".join(parts)

        self._create_view_sql(view_name=view_name, query=query, if_exists=if_exists)
        return True

    def create_hybrid_view(
        self,
        view_name: str,
        concept_key: str,
        driver_filter: Optional[List[str]] = None,
        schema_model: Optional[Type[BaseModel]] = None,
    ) -> bool:
        """Creates or replaces a DuckDB SQL VIEW combining hot and cold data."""
        db = self._require_db()
        hot_table_exists = db.has_table(concept_key, table_schema="global_tables")
        concept_cols = (
            self._table_columns("global_tables", concept_key)
            if hot_table_exists
            else set()
        )
        cold_sql, extra_hot_queries = self._generate_cold_and_extra_hot_queries(
            concept_key=concept_key,
            driver_filter=driver_filter,
        )

        parts: List[str] = []
        if hot_table_exists:
            if "consist_run_id" in concept_cols:
                hot_query = f"""
                    SELECT t.*, r.parent_run_id AS consist_scenario_id
                    FROM global_tables.{_quote_ident(concept_key)} t
                    LEFT JOIN main.run r ON CAST(t.consist_run_id AS VARCHAR) = r.id
                """
            elif "consist_artifact_id" in concept_cols:
                hot_query = f"""
                    SELECT t.*, r.parent_run_id AS consist_scenario_id
                    FROM global_tables.{_quote_ident(concept_key)} t
                    LEFT JOIN main.artifact a ON TRY_CAST(t.consist_artifact_id AS UUID) = a.id
                    LEFT JOIN main.run r ON a.run_id = r.id
                """
            else:
                hot_query = f"""
                    SELECT t.*, CAST(NULL AS VARCHAR) AS consist_scenario_id
                    FROM global_tables.{_quote_ident(concept_key)} t
                """
            parts.append(hot_query)

        parts.extend(extra_hot_queries)

        if cold_sql:
            parts.append(cold_sql)

        if not parts:
            if schema_model is not None:
                schema_columns = _extract_schema_model_columns(schema_model)
                query = self._typed_empty_query(
                    schema_columns=schema_columns,
                    facets=[],
                    facet_casts={},
                    include_system_columns=True,
                )
            else:
                query = "SELECT 1 AS _empty_marker WHERE 1=0"
        else:
            query = "\nUNION ALL BY NAME\n".join(parts)

        self._create_view_sql(view_name=view_name, query=query, if_exists="replace")
        return True

    def _create_view_sql(
        self,
        *,
        view_name: str,
        query: str,
        if_exists: Literal["replace", "error"] = "replace",
    ) -> None:
        db = self._require_db()
        quoted_view_name = _quote_ident(view_name)
        if db.read_only:
            if if_exists == "error":
                if db.has_temp_view(view_name):
                    raise ValueError(f"View already exists: {view_name}")
                row = db.fetchone(
                    "SELECT COUNT(*) FROM information_schema.views "
                    "WHERE table_schema IN ('main', 'temp') AND table_name = ?",
                    [view_name],
                )
                if row is not None and int(row[0]) > 0:
                    raise ValueError(f"View already exists: {view_name}")
            temp_sql = f"CREATE OR REPLACE TEMP VIEW {quoted_view_name} AS \n{query}"
            db.register_temp_view(view_name, temp_sql)
            return

        if if_exists == "error":
            row = db.fetchone(
                "SELECT COUNT(*) FROM information_schema.views "
                "WHERE table_schema = 'main' AND table_name = ?",
                [view_name],
            )
            if row is not None and int(row[0]) > 0:
                raise ValueError(f"View already exists: {view_name}")
            db.execute(f"CREATE VIEW {quoted_view_name} AS \n{query}")
        else:
            db.execute(f"CREATE OR REPLACE VIEW {quoted_view_name} AS \n{query}")

    def _table_columns(self, schema: str, table: str) -> set[str]:
        db = self._require_db()
        try:
            rows = db.fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            )
            return {str(row[0]) for row in rows}
        except duckdb.Error:
            return set()

    def _typed_empty_query(
        self,
        *,
        schema_columns: List[str],
        facets: List[str],
        facet_casts: Dict[str, str],
        include_system_columns: bool,
    ) -> str:
        cols: List[str] = []
        aliases: set[str] = set()

        def _add(expr: str, alias: str) -> None:
            if alias in aliases:
                return
            aliases.add(alias)
            cols.append(expr)

        if include_system_columns:
            _add("CAST(NULL AS VARCHAR) AS consist_run_id", "consist_run_id")
            _add("CAST(NULL AS INTEGER) AS consist_year", "consist_year")
            _add("CAST(NULL AS INTEGER) AS consist_iteration", "consist_iteration")
            _add("CAST(NULL AS VARCHAR) AS consist_artifact_id", "consist_artifact_id")
            _add("CAST(NULL AS VARCHAR) AS consist_scenario_id", "consist_scenario_id")

        for col_name in schema_columns:
            if col_name.startswith("consist_"):
                continue
            _add(f"NULL AS {_quote_ident(col_name)}", col_name)

        for facet in facets:
            alias = f"facet_{facet}"
            cast_type = facet_casts.get(facet, "VARCHAR")
            _add(f"CAST(NULL AS {cast_type}) AS {_quote_ident(alias)}", alias)

        if not cols:
            return "SELECT 1 AS _empty_marker WHERE 1=0"
        return f"SELECT {', '.join(cols)} WHERE 1=0"

    def _generate_grouped_hot_query(
        self,
        *,
        records: List[GroupedViewRecord],
        facets: List[str],
        facet_casts: Dict[str, str],
        include_system_columns: bool,
    ) -> Optional[str]:
        grouped_by_table: Dict[str, List[GroupedViewRecord]] = {}
        for record in records:
            meta = record.artifact.meta or {}
            if not meta.get("is_ingested"):
                continue
            table_name = meta.get("dlt_table_name")
            if not isinstance(table_name, str) or not table_name:
                table_name = record.artifact.key
            grouped_by_table.setdefault(table_name, []).append(record)

        if not grouped_by_table:
            return None

        union_parts: List[str] = []
        facet_col_names = {facet: f"facet_{idx}" for idx, facet in enumerate(facets)}
        db = self._require_db()
        for table_name, table_records in grouped_by_table.items():
            if not db.has_table(table_name, table_schema="global_tables"):
                logging.warning(
                    "[Consist Warning] Skipping grouped hot table that does not exist: %s",
                    table_name,
                )
                continue

            table_cols = self._table_columns("global_tables", table_name)
            has_art_col = "consist_artifact_id" in table_cols
            has_run_col = "consist_run_id" in table_cols
            if not has_art_col and not has_run_col:
                logging.warning(
                    "[Consist Warning] Skipping grouped hot table missing consist_artifact_id: %s",
                    table_name,
                )
                continue

            exclude_cols: List[str] = []
            for col_name in (
                "consist_run_id",
                "consist_year",
                "consist_iteration",
                "consist_artifact_id",
                "consist_scenario_id",
            ):
                if col_name in table_cols:
                    exclude_cols.append(col_name)

            if exclude_cols:
                excluded = ", ".join(_quote_ident(name) for name in exclude_cols)
                table_expr = f"t.* EXCLUDE ({excluded})"
            else:
                table_expr = "t.*"

            meta_alias = _quote_ident(f"meta_hot_{len(union_parts)}")
            meta_columns = ["art_id", "run_id", "year", "iter", "scenario"] + list(
                facet_col_names.values()
            )
            meta_rows: List[str] = []
            for record in table_records:
                row_values = [
                    _safe_duckdb_string_literal(str(record.artifact.id)),
                    _safe_duckdb_string_literal(record.run.id),
                    _safe_duckdb_literal(record.run.year),
                    _safe_duckdb_literal(record.run.iteration),
                    _safe_duckdb_string_literal(record.run.parent_run_id),
                ]
                for facet in facets:
                    row_values.append(
                        _safe_duckdb_literal(record.facet_values.get(facet))
                    )
                meta_rows.append(f"({', '.join(row_values)})")
            if not meta_rows:
                continue

            select_fields = [table_expr]
            if include_system_columns:
                run_expr = (
                    f"COALESCE(CAST(t.consist_run_id AS VARCHAR), {meta_alias}.run_id)"
                    if has_run_col
                    else f"{meta_alias}.run_id"
                )
                year_expr = (
                    "COALESCE(CAST(t.consist_year AS INTEGER), "
                    f"CAST({meta_alias}.year AS INTEGER))"
                    if "consist_year" in table_cols
                    else f"CAST({meta_alias}.year AS INTEGER)"
                )
                iter_expr = (
                    "COALESCE(CAST(t.consist_iteration AS INTEGER), "
                    f"CAST({meta_alias}.iter AS INTEGER))"
                    if "consist_iteration" in table_cols
                    else f"CAST({meta_alias}.iter AS INTEGER)"
                )
                art_expr = (
                    "COALESCE(CAST(t.consist_artifact_id AS VARCHAR), "
                    f"{meta_alias}.art_id)"
                    if has_art_col
                    else f"{meta_alias}.art_id"
                )
                scenario_expr = (
                    "COALESCE(CAST(t.consist_scenario_id AS VARCHAR), "
                    f"{meta_alias}.scenario)"
                    if "consist_scenario_id" in table_cols
                    else f"{meta_alias}.scenario"
                )
                select_fields.extend(
                    [
                        f"{run_expr} AS consist_run_id",
                        f"{year_expr} AS consist_year",
                        f"{iter_expr} AS consist_iteration",
                        f"{art_expr} AS consist_artifact_id",
                        f"{scenario_expr} AS consist_scenario_id",
                    ]
                )

            for facet in facets:
                cast_type = facet_casts.get(facet, "VARCHAR")
                facet_col = facet_col_names[facet]
                select_fields.append(
                    f"CAST({meta_alias}.{_quote_ident(facet_col)} AS {cast_type}) "
                    f"AS {_quote_ident(f'facet_{facet}')}"
                )

            if has_art_col and has_run_col:
                join_on = (
                    f"(CAST(t.consist_artifact_id AS VARCHAR) = {meta_alias}.art_id "
                    f"OR (t.consist_artifact_id IS NULL AND CAST(t.consist_run_id AS VARCHAR) = {meta_alias}.run_id))"
                )
            elif has_art_col:
                join_on = (
                    f"CAST(t.consist_artifact_id AS VARCHAR) = {meta_alias}.art_id"
                )
            else:
                join_on = f"CAST(t.consist_run_id AS VARCHAR) = {meta_alias}.run_id"

            meta_cols_sql = ", ".join(_quote_ident(name) for name in meta_columns)
            query = f"""
            SELECT
                {", ".join(select_fields)}
            FROM global_tables.{_quote_ident(table_name)} t
            JOIN (
                VALUES {", ".join(meta_rows)}
            ) AS {meta_alias}({meta_cols_sql})
            ON {join_on}
            """
            union_parts.append(query)

        if not union_parts:
            return None
        return "\nUNION ALL BY NAME\n".join(union_parts)

    def _resolve_cold_record_path(
        self,
        record: GroupedViewRecord,
        *,
        hydrate_missing: bool = True,
    ) -> Optional[Path]:
        primary_path = Path(self.tracker.resolve_uri(record.artifact.container_uri))
        if primary_path.exists():
            return primary_path

        if record.artifact.abs_path:
            abs_candidate = Path(record.artifact.abs_path)
            if abs_candidate.exists():
                return abs_candidate

        if record.run is not None:
            try:
                historical_path = self.tracker.resolve_historical_path(
                    record.artifact, record.run
                )
                if historical_path.exists():
                    return historical_path
            except (OSError, ValueError):
                pass

        if record.artifact.recovery_roots:
            try:
                best_location = record.artifact.accessibility(
                    self.tracker
                ).best_location
                if best_location is not None and best_location.exists:
                    return best_location.path
            except (OSError, ValueError):
                pass

        if hydrate_missing and record.artifact.recovery_roots:
            ensured_path = record.artifact.ensure_local_path(
                tracker=self.tracker,
                on_missing="warn",
            )
            if ensured_path is not None and ensured_path.exists():
                return ensured_path

        return None

    def _generate_grouped_cold_query_optimized(
        self,
        *,
        records: List[GroupedViewRecord],
        facets: List[str],
        facet_casts: Dict[str, str],
        include_system_columns: bool,
        missing_files: Literal["warn", "error", "skip_silent"],
    ) -> Optional[str]:
        grouped: Dict[str, List[Tuple[GroupedViewRecord, Path]]] = {}
        for record in records:
            meta = record.artifact.meta or {}
            if meta.get("is_ingested"):
                continue

            driver = record.artifact.driver
            if driver not in {"parquet", "csv"}:
                continue

            resolved_path = self._resolve_cold_record_path(record)
            if resolved_path is None:
                abs_path = self.tracker.resolve_uri(record.artifact.container_uri)
                if missing_files == "error":
                    raise FileNotFoundError(abs_path)
                if missing_files == "warn":
                    logging.warning(
                        "[Consist Warning] Skipping missing artifact in grouped view: %s",
                        abs_path,
                    )
                continue

            grouped.setdefault(driver, []).append((record, resolved_path))

        if not grouped:
            return None

        union_parts: List[str] = []
        facet_col_names = {facet: f"facet_{idx}" for idx, facet in enumerate(facets)}
        for driver, items in grouped.items():
            if not items:
                continue

            meta_rows: List[str] = []
            path_list: List[str] = []
            for record, resolved_path in items:
                path_literal = _safe_duckdb_path_literal(str(resolved_path))
                path_list.append(path_literal)
                row_values = [
                    path_literal,
                    _safe_duckdb_string_literal(record.run.id),
                    _safe_duckdb_string_literal(str(record.artifact.id)),
                    _safe_duckdb_literal(record.run.year),
                    _safe_duckdb_literal(record.run.iteration),
                    _safe_duckdb_string_literal(record.run.parent_run_id),
                ]
                for facet in facets:
                    row_values.append(
                        _safe_duckdb_literal(record.facet_values.get(facet))
                    )
                meta_rows.append(f"({', '.join(row_values)})")

            if driver == "parquet":
                reader_func = (
                    f"read_parquet([{', '.join(path_list)}], "
                    "union_by_name=true, filename=true)"
                )
            elif driver == "csv":
                reader_func = (
                    f"read_csv_auto([{', '.join(path_list)}], "
                    "union_by_name=true, filename=true, normalize_names=true)"
                )
            else:
                continue

            cte_name = _quote_ident(f"meta_{driver}_{len(union_parts)}")
            meta_columns = [
                "fpath",
                "run_id",
                "art_id",
                "year",
                "iter",
                "scenario",
            ] + list(facet_col_names.values())
            meta_cols_sql = ", ".join(_quote_ident(name) for name in meta_columns)

            select_fields: List[str] = ["data.* EXCLUDE (filename)"]
            if include_system_columns:
                select_fields.extend(
                    [
                        f"{cte_name}.run_id AS consist_run_id",
                        f"{cte_name}.art_id AS consist_artifact_id",
                        f"CAST({cte_name}.year AS INTEGER) AS consist_year",
                        f"CAST({cte_name}.iter AS INTEGER) AS consist_iteration",
                        f"{cte_name}.scenario AS consist_scenario_id",
                    ]
                )
            for facet in facets:
                cast_type = facet_casts.get(facet, "VARCHAR")
                facet_col = facet_col_names[facet]
                select_fields.append(
                    f"CAST({cte_name}.{_quote_ident(facet_col)} AS {cast_type}) "
                    f"AS {_quote_ident(f'facet_{facet}')}"
                )

            query = f"""
            SELECT
                {", ".join(select_fields)}
            FROM {reader_func} data
            JOIN (
                VALUES {", ".join(meta_rows)}
            ) AS {cte_name}({meta_cols_sql})
            ON data.filename = {cte_name}.fpath
            """
            union_parts.append(query)

        if not union_parts:
            return None
        return "\nUNION ALL BY NAME\n".join(union_parts)

    def _generate_cold_and_extra_hot_queries(
        self,
        *,
        concept_key: str,
        driver_filter: Optional[List[str]] = None,
    ) -> Tuple[Optional[str], List[str]]:
        drivers = driver_filter or ["parquet", "csv"]
        metadata_db = self.tracker.db
        if metadata_db is None:
            return None, []

        all_results = metadata_db.find_artifacts_with_runs_for_key(key=concept_key)
        if not all_results:
            return None, []

        driver_set = set(drivers)
        cold_records: List[GroupedViewRecord] = []
        extra_hot_records: List[GroupedViewRecord] = []
        for artifact, run in all_results:
            meta = artifact.meta or {}
            if meta.get("is_ingested"):
                dlt_table = meta.get("dlt_table_name")
                if (
                    isinstance(dlt_table, str)
                    and dlt_table
                    and dlt_table != concept_key
                ):
                    extra_hot_records.append(
                        GroupedViewRecord(artifact=artifact, run=run, facet_values={})
                    )
            elif artifact.driver in driver_set:
                cold_records.append(
                    GroupedViewRecord(artifact=artifact, run=run, facet_values={})
                )

        extra_hot_queries: List[str] = []
        if extra_hot_records:
            extra_hot_sql = self._generate_grouped_hot_query(
                records=extra_hot_records,
                facets=[],
                facet_casts={},
                include_system_columns=True,
            )
            if extra_hot_sql:
                extra_hot_queries.append(extra_hot_sql)

        cold_sql = self._generate_grouped_cold_query_optimized(
            records=cold_records,
            facets=[],
            facet_casts={},
            include_system_columns=True,
            missing_files="warn",
        )
        return cold_sql, extra_hot_queries

    def _generate_cold_query_optimized(
        self, concept_key: str, driver_filter: Optional[List[str]] = None
    ) -> Optional[str]:
        cold_sql, _ = self._generate_cold_and_extra_hot_queries(
            concept_key=concept_key,
            driver_filter=driver_filter,
        )
        return cold_sql
