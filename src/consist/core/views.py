import logging
import math
import os
import types
from copy import copy
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    TYPE_CHECKING,
    Type,
    TypeVar,
    Tuple,
)
from sqlalchemy import Column
from sqlmodel import select, Session, text, SQLModel, Field, col

from consist.models.artifact import Artifact
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker

T = TypeVar("T", bound=SQLModel)


def _quote_ident(identifier: str) -> str:
    """
    Quote an identifier for DuckDB SQL (double-quote, escaping embedded quotes).
    """
    return '"' + identifier.replace('"', '""') + '"'


def _safe_duckdb_path_literal(path_str: str) -> str:
    """
    Return a SQL-safe, quoted literal for a DuckDB file path.

    Ensures the path resolves and exists, then escapes single quotes.
    """
    resolved = Path(path_str).resolve()
    if not resolved.exists():
        raise ValueError(f"Path does not exist: {resolved}")
    escaped = str(resolved).replace("'", "''")
    return f"'{escaped}'"


def _safe_duckdb_string_literal(value: Optional[str]) -> str:
    """
    Return a SQL-safe string literal for DuckDB, escaping single quotes.
    """
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _safe_duckdb_literal(value: Any) -> str:
    """
    Return a SQL-safe DuckDB literal for scalar values.
    """
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
    """
    Resolve a deterministic DuckDB type from observed ArtifactKV value types.
    """
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


@dataclass(frozen=True)
class GroupedViewRecord:
    artifact: Artifact
    run: Run
    facet_values: Dict[str, Any]


def create_view_model(model: Type[T], name: Optional[str] = None) -> Type[T]:
    """
    Creates a dynamic SQLModel class that maps to a Consist Hybrid View.
    Ensures table=True is passed to the metaclass.
    """
    # 1. Determine View Name
    if name:
        view_name = name
    elif hasattr(model, "__tablename__"):
        view_name = f"v_{model.__tablename__}"
    else:
        view_name = f"v_{model.__name__.lower()}"

    # 2. Clone Annotations
    # Prefer resolved Pydantic field annotations (works even when the model was defined
    # under `from __future__ import annotations`).
    annotations: Dict[str, Any] = {}
    for field_name, field_info in model.model_fields.items():
        ann = getattr(field_info, "annotation", None)
        annotations[field_name] = ann if ann is not None else Any
    annotations.update(
        {
            "consist_run_id": str,
            "consist_year": Optional[int],
            "consist_iteration": Optional[int],
            "consist_artifact_id": Optional[str],
            "consist_scenario_id": Optional[str],
        }
    )

    # 3. Construct Namespace
    namespace = {
        "__tablename__": view_name,
        "__table_args__": {"extend_existing": True},
        "__annotations__": annotations,
        "consist_run_id": Field(primary_key=True),
        "consist_year": Field(default=None, nullable=True),
        "consist_iteration": Field(default=None, nullable=True),
        "consist_artifact_id": Field(default=None, nullable=True),
        "consist_scenario_id": Field(default=None, nullable=True),
    }

    # 4. Clone Fields
    for field_name, field_info in model.model_fields.items():
        if field_name not in namespace:
            sa_column = getattr(field_info, "sa_column", None)
            if isinstance(sa_column, Column):
                # NOTE:
                # SQLAlchemy `Column` objects are bound to a specific `Table` instance.
                # If we reuse the same Column object from the schema model, SQLAlchemy
                # will raise:
                #   "Column object 'X' already assigned to Table 'Y'"
                #
                # This comes up in practice when a schema model uses a custom column
                # name (e.g., `Column("Mass(kg)", ...)`) and we create a dynamic view
                # model pointing at a different table (`v_*`).
                #
                # We therefore create a *new* Column with the same name and type so
                # the view model preserves the original column names for quoting and
                # for typed empty views, without sharing SQLAlchemy state.
                sa_column = Column(
                    sa_column.name,
                    copy(sa_column.type),
                    nullable=sa_column.nullable,
                    primary_key=sa_column.primary_key,
                    index=sa_column.index,
                    unique=sa_column.unique,
                )

            default_factory = getattr(field_info, "default_factory", None)
            if default_factory is not None:
                if sa_column is not None:
                    namespace[field_name] = Field(
                        default_factory=default_factory, sa_column=sa_column
                    )
                else:
                    namespace[field_name] = Field(default_factory=default_factory)
            else:
                if sa_column is not None:
                    namespace[field_name] = Field(
                        default=getattr(field_info, "default", None),
                        sa_column=sa_column,
                    )
                else:
                    namespace[field_name] = Field(
                        default=getattr(field_info, "default", None)
                    )

    # 5. Create Dynamic Class
    def exec_body(ns):
        ns.update(namespace)

    # Pass {"table": True} to the keyword args of class creation
    return types.new_class(
        f"Virtual{model.__name__}", (SQLModel,), {"table": True}, exec_body
    )  # ty: ignore[invalid-return-type]


class ViewRegistry:
    """
    Registry for dynamic view classes.
    Accessing a view (e.g. registry.Person) automatically refreshes
    the underlying DuckDB SQL definition to include new files.

    Use ``register(model, key=...)`` to add SQLModel schemas. Accessing the
    attribute returns a dynamic SQLModel view class that can be queried via
    ``select(...)``.
    """

    def __init__(self, tracker: "Tracker"):
        self._tracker = tracker
        # Stores (ModelClass, ConceptKey)
        self._registry: Dict[str, Tuple[Type[SQLModel], Optional[str]]] = {}
        # Caches the Python class object to ensure identity stability
        self._class_cache: Dict[str, Type[SQLModel]] = {}

    def register(self, model: Type[SQLModel], key: Optional[str] = None):
        name = model.__name__
        previous = self._registry.get(name)
        self._registry[name] = (model, key)
        # Invalidate the cached class only when the registration changes.
        if previous != (model, key):
            self._class_cache.pop(name, None)

    def __getattr__(self, name: str) -> Type[SQLModel]:
        # 1. Check if registered
        if name in self._registry:
            model, key = self._registry[name]

            # 2. Refresh the SQL View in DB
            # We assume accessing the property means the user wants to query it now.
            # Recreating the view ensures all files on disk are picked up.
            factory = ViewFactory(self._tracker)

            cached = self._class_cache.get(name)
            if cached is not None:
                concept_key = key or getattr(
                    model, "__tablename__", model.__name__.lower()
                )
                view_name = f"v_{concept_key}"
                factory.create_hybrid_view(view_name, concept_key, schema_model=model)
                return cached

            # This calls create_hybrid_view inside
            # We don't need the return value (the python class) necessarily
            # if we have it cached, but the factory method does both.
            view_cls = factory.create_view_from_model(model, key)

            # 3. Update Cache & Return
            self._class_cache[name] = view_cls
            return view_cls

        raise AttributeError(f"'ViewRegistry' object has no attribute '{name}'")

    def __repr__(self):
        return f"<ViewRegistry registered={list(self._registry.keys())}>"


class ViewFactory:
    """
    A factory class responsible for generating **"Hybrid Views"** in DuckDB, acting as
    Consist's **"The Virtualizer"** component.

    Hybrid Views combine data from materialized tables (often ingested via dlt)
    with data directly from file-based artifacts (e.g., Parquet, CSV),
    providing a unified SQL interface to query both "hot" and "cold" data
    transparently. This approach is central to Consist's flexible data access strategy.

    Attributes
    ----------
    tracker : Tracker
        An instance of the Consist `Tracker`, which provides access to the database
        engine, artifact resolution, and other run-time context necessary for
        view creation.
    """

    def __init__(self, tracker: "Tracker") -> None:
        """
        Initializes the ViewFactory with a reference to the main Tracker.

        Parameters
        ----------
        tracker : Tracker
            An instance of the Consist `Tracker`, which provides access to the
            database engine, artifact resolution, and other run-time context
            required for creating and managing views.
        """
        self.tracker = tracker

    def create_view_from_model(
        self, model: Type[SQLModel], key: Optional[str] = None
    ) -> Type[SQLModel]:
        """
        Creates both the SQL View and the Python SQLModel class for a given schema.
        """
        concept_key = key or getattr(model, "__tablename__", model.__name__.lower())
        view_name = f"v_{concept_key}"

        # Pass schema_model to handle empty states
        self.create_hybrid_view(view_name, concept_key, schema_model=model)

        return create_view_model(model, name=view_name)

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
        """
        Create a selector-driven hybrid view across many artifacts.

        This method powers schema-family analysis views where artifacts may have
        different keys but represent the same logical table. Selection is based
        on a required ``schema_id`` plus optional facet/run predicates.

        The resulting SQL view can combine:
        - hot rows from ingested ``global_tables.*`` relations,
        - cold rows from files (currently parquet/csv readers),
        - optional typed ``facet_*`` projection columns,
        - optional Consist system columns.

        Parameters
        ----------
        view_name : str
            Name of the SQL view to create.
        schema_id : Optional[str], optional
            Primary selector for a single schema id.
        schema_ids : Optional[List[str]], optional
            Alternative selector for multiple schema ids.
            This is mainly used by higher-level model-class resolution in
            ``Tracker.create_grouped_view``.
        schema_compatible : bool, default False
            If True, include artifacts observed with schema variants deemed
            compatible by field-name subset/superset matching.
        predicates : Optional[List[Dict[str, Any]]], optional
            Parsed ArtifactKV predicates (as produced by
            ``Tracker._parse_artifact_param_expression``).
        namespace : Optional[str], optional
            Default ArtifactKV namespace used when a predicate does not provide
            one explicitly.
        drivers : Optional[List[str]], optional
            Optional artifact-driver filter (e.g., ``["parquet"]``).
        attach_facets : Optional[List[str]], optional
            Facet key paths to expose as typed columns named ``facet_<key>``.
        include_system_columns : bool, default True
            If True, include ``consist_run_id``, ``consist_artifact_id``,
            ``consist_year``, ``consist_iteration``, and
            ``consist_scenario_id``.
        mode : {"hybrid", "hot_only", "cold_only"}, default "hybrid"
            Controls which storage tier(s) are included in the view.
        if_exists : {"replace", "error"}, default "replace"
            View creation behavior when ``view_name`` already exists.
        missing_files : {"warn", "error", "skip_silent"}, default "warn"
            Policy for selected cold artifacts whose files no longer exist.
        run_id : Optional[str], optional
            Optional exact run-id filter.
        parent_run_id : Optional[str], optional
            Optional parent/scenario run-id filter.
        model : Optional[str], optional
            Optional run model-name filter.
        status : Optional[str], optional
            Optional run status filter.
        year : Optional[int], optional
            Optional run year filter.
        iteration : Optional[int], optional
            Optional run iteration filter.

        Returns
        -------
        bool
            ``True`` when view creation is completed.

        Raises
        ------
        RuntimeError
            If the tracker is not configured with a database/engine.
        ValueError
            If policy arguments have unsupported values, or ``if_exists="error"``
            and the view already exists.
        FileNotFoundError
            If ``missing_files="error"`` and a selected cold file is missing.

        Notes
        -----
        - Empty selections still produce a valid typed empty view.
        - Facet column types are inferred deterministically from indexed KV types:
          bool -> BOOLEAN, int -> BIGINT, float/int mix -> DOUBLE, otherwise VARCHAR.
        """
        if not self.tracker.engine:
            raise RuntimeError("Cannot create views: No database engine configured.")
        if not self.tracker.db:
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

        records: List[GroupedViewRecord] = []
        for artifact, run in selected:
            facet_values: Dict[str, Any] = {}
            art_facet_map = facet_values_by_artifact.get(artifact.id, {})
            for facet in facets:
                info = art_facet_map.get(facet)
                if info is None:
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
        schema_model: Optional[Type[SQLModel]] = None,
    ) -> bool:
        """
        Creates or replaces a DuckDB SQL VIEW that combines "hot" and "cold" data for a given concept.

        This method generates a **"Hybrid View"** which allows transparent querying across
        different data storage types. It implements **"View Optimization"** by leveraging
        DuckDB's capabilities for vectorized reads from files. The resulting view uses
        `UNION ALL BY NAME` to gracefully handle **"Schema Evolution"** (different columns
        across runs or data sources) by nulling out missing columns.

        "Hot" data refers to records already materialized into a DuckDB table (e.g., via ingestion).
        "Cold" data refers to records still residing in file-based artifacts (e.g., Parquet, CSV).
        Identifiers are quoted for SQL safety; missing cold-file paths are skipped at view creation.

        Parameters
        ----------
        view_name : str
            The name to assign to the newly created or replaced SQL view. This is the name
            you will use in your SQL queries to access the combined data.
        concept_key : str
            The semantic key identifying the data concept (e.g., "households", "transactions").
            Artifacts and materialized tables matching this key will be included in the view.
        driver_filter : Optional[List[str]], optional
            An optional list of artifact drivers (e.g., "parquet", "csv") to include
            when querying "cold" data. If `None`, "parquet" and "csv" drivers are considered
            by default.
        schema_model : Type[SQLModel], optional
            SQL table definition for underlying data

        Returns
        -------
        bool
            True if the view creation was attempted (even if the view ends up empty), False otherwise.

        Raises
        ------
        RuntimeError
            If the `Tracker`'s database engine is not configured (i.e., `db_path` was not
            provided during `Tracker` initialization).
        """
        if not self.tracker.engine:
            raise RuntimeError("Cannot create views: No database engine configured.")

        # 1. Identify 'Hot' Data
        # Explicitly check for the table in global_tables schema
        hot_table_exists = self._check_table_exists("global_tables", concept_key)

        # 2. Identify 'Cold' Artifacts
        cold_sql = self._generate_cold_query_optimized(concept_key, driver_filter)

        parts = []

        if hot_table_exists:
            # Join 'run' table to get parent_run_id as consist_scenario_id
            # We use an alias 't' for the data table and 'r' for the run table
            hot_query = f"""
                SELECT t.*, r.parent_run_id AS consist_scenario_id
                FROM global_tables.{_quote_ident(concept_key)} t
                LEFT JOIN run r ON t.consist_run_id = r.id
            """
            parts.append(hot_query)

        if cold_sql:
            parts.append(cold_sql)

        if not parts:
            # --- NEW: Typed Empty View ---
            if schema_model:
                schema_columns: List[str] = []
                for attr_name, field_info in schema_model.model_fields.items():
                    sa_col = getattr(field_info, "sa_column", None)
                    col_name = (
                        getattr(sa_col, "name", None) if sa_col is not None else None
                    )
                    sql_name = (
                        str(col_name)
                        if isinstance(col_name, str) and col_name
                        else str(attr_name)
                    )
                    if sql_name.startswith("consist_"):
                        continue
                    schema_columns.append(sql_name)
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
        quoted_view_name = _quote_ident(view_name)
        with self.tracker.engine.begin() as conn:
            if if_exists == "error":
                existing = conn.execute(
                    text(
                        "SELECT count(*) FROM information_schema.views "
                        "WHERE table_name = :name"
                    ),
                    {"name": view_name},
                ).scalar()
                if existing and int(existing) > 0:
                    raise ValueError(f"View already exists: {view_name}")
            else:
                conn.execute(text(f"DROP VIEW IF EXISTS {quoted_view_name}"))

            sql = f"CREATE VIEW {quoted_view_name} AS \n{query}"
            conn.execute(text(sql))

    def _table_columns(self, schema: str, table: str) -> set[str]:
        sql = text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table"
        )
        try:
            with self.tracker.engine.connect() as conn:
                rows = conn.execute(sql, {"schema": schema, "table": table}).fetchall()
            return {str(row[0]) for row in rows}
        except Exception:
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
                continue
            grouped_by_table.setdefault(table_name, []).append(record)

        if not grouped_by_table:
            return None

        union_parts: List[str] = []
        facet_col_names = {facet: f"facet_{idx}" for idx, facet in enumerate(facets)}
        for table_name, table_records in grouped_by_table.items():
            if not self._check_table_exists("global_tables", table_name):
                logging.warning(
                    "[Consist Warning] Skipping grouped hot table that does not exist: %s",
                    table_name,
                )
                continue

            table_cols = self._table_columns("global_tables", table_name)
            if "consist_artifact_id" not in table_cols:
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
                    if "consist_run_id" in table_cols
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
                    if "consist_artifact_id" in table_cols
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

            meta_cols_sql = ", ".join(_quote_ident(name) for name in meta_columns)
            query = f"""
            SELECT
                {", ".join(select_fields)}
            FROM global_tables.{_quote_ident(table_name)} t
            JOIN (
                VALUES {", ".join(meta_rows)}
            ) AS {meta_alias}({meta_cols_sql})
            ON CAST(t.consist_artifact_id AS VARCHAR) = {meta_alias}.art_id
            """
            union_parts.append(query)

        if not union_parts:
            return None
        return "\nUNION ALL BY NAME\n".join(union_parts)

    def _generate_grouped_cold_query_optimized(
        self,
        *,
        records: List[GroupedViewRecord],
        facets: List[str],
        facet_casts: Dict[str, str],
        include_system_columns: bool,
        missing_files: Literal["warn", "error", "skip_silent"],
    ) -> Optional[str]:
        grouped: Dict[str, List[GroupedViewRecord]] = {}
        for record in records:
            meta = record.artifact.meta or {}
            if meta.get("is_ingested"):
                continue

            driver = record.artifact.driver
            if driver not in {"parquet", "csv"}:
                continue

            abs_path = self.tracker.resolve_uri(record.artifact.container_uri)
            if not os.path.exists(abs_path):
                if missing_files == "error":
                    raise FileNotFoundError(abs_path)
                if missing_files == "warn":
                    logging.warning(
                        "[Consist Warning] Skipping missing artifact in grouped view: %s",
                        abs_path,
                    )
                continue

            grouped.setdefault(driver, []).append(record)

        if not grouped:
            return None

        union_parts: List[str] = []
        facet_col_names = {facet: f"facet_{idx}" for idx, facet in enumerate(facets)}
        for driver, items in grouped.items():
            if not items:
                continue

            meta_rows: List[str] = []
            path_list: List[str] = []
            for record in items:
                abs_path = self.tracker.resolve_uri(record.artifact.container_uri)
                path_list.append(_safe_duckdb_path_literal(abs_path))
                row_values = [
                    _safe_duckdb_path_literal(abs_path),
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

    def _check_table_exists(self, schema: str, table: str) -> bool:
        """
        Robustly checks if a table exists in a specific schema.
        """
        sql = text(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name = :table"
        )
        try:
            with self.tracker.engine.connect() as conn:
                count = conn.execute(sql, {"schema": schema, "table": table}).scalar()
                return count > 0
        except Exception as e:
            logging.warning(f"Failed to check table existence: {e}")
            return False

    def _generate_cold_query_optimized(
        self, concept_key: str, driver_filter: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Generates a single optimized SQL query for all "cold" artifacts of a given `concept_key`.

        This method is central to **"View Optimization"** by employing **"Vectorization"**:
        it uses DuckDB's `read_parquet` or `read_csv_auto` functions with a list of file paths
        for efficient, single-pass reads. It also dynamically injects run-specific metadata
        (e.g., `consist_run_id`, `consist_year`) into the loaded data using a Common Table Expression (CTE)
        to allow unified querying with hot data. This approach significantly reduces query
        complexity and improves performance when dealing with numerous file-based artifacts.

        Parameters
        ----------
        concept_key : str
            The semantic key for which to generate the cold data query.
        driver_filter : Optional[List[str]], optional
            An optional list of artifact drivers (e.g., "parquet", "csv") to include
            when querying "cold" data. If `None`, "parquet" and "csv" drivers are considered
            by default.

        Returns
        -------
        Optional[str]
            A SQL query string that, when executed, will return the combined data
            from all matching "cold" artifacts with injected provenance metadata.
            Returns `None` if no cold data artifacts are found or eligible.
        """
        drivers = driver_filter or ["parquet", "csv"]

        with Session(self.tracker.engine) as session:
            # Query Artifacts + Join Run to get Year/Iteration info
            statement = (
                select(Artifact, Run)
                .join(Run, Artifact.run_id == Run.id)  # ty: ignore[invalid-argument-type]
                .where(Artifact.key == concept_key)
                .where(col(Artifact.driver).in_(drivers))
            )
            results = session.exec(statement).all()

        if not results:
            return None
        records = [
            GroupedViewRecord(artifact=artifact, run=run, facet_values={})
            for artifact, run in results
        ]
        return self._generate_grouped_cold_query_optimized(
            records=records,
            facets=[],
            facet_casts={},
            include_system_columns=True,
            missing_files="warn",
        )
