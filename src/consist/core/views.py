from __future__ import annotations

import logging
import math
import re
from collections.abc import Iterable, Mapping, Sequence
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
    Tuple,
)
import uuid

from pydantic import BaseModel

from consist.artifact import Artifact
from consist.digest import Digest, Kind
from consist.engine import _is_candidate_visible, resolve_uri_to_path
from consist.location import SiteContext, matches_hot_db_info, parse_hot_db_member
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.store.ledger import StoreLedger

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(identifier: str, *, label: str) -> str:
    if not _SAFE_IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(
            f"Invalid {label}. Only letters, numbers, and underscores are allowed, "
            "and the identifier must not start with a number."
        )
    return identifier


def _quote_ident(identifier: str) -> str:
    """
    Quote an identifier for DuckDB SQL (double-quote, escaping embedded quotes).
    """
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_table(
    schema: str, table: str, *, table_label: str = "concept_key"
) -> str:
    safe_schema = _validate_identifier(schema, label="schema")
    safe_table = _validate_identifier(table, label=table_label)
    return f"{_quote_ident(safe_schema)}.{_quote_ident(safe_table)}"


def _model_tablename(model: type[BaseModel]) -> str:
    """Resolve the DuckDB/SQLModel table name for a schema model class."""
    for base in model.__mro__:
        if "__tablename__" in base.__dict__:
            raw_name = base.__dict__["__tablename__"]
            if isinstance(raw_name, str) and raw_name.strip():
                return raw_name.strip()
    return model.__name__.lower()


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
    run: Optional[Run]
    facet_values: Dict[str, Any]
    hot_table: Optional[str] = None
    v1_null_artifact_id: bool = False


def build_v2_hot_artifact_join_sql(
    hot_alias: str = "h",
    artifact_alias: str = "a",
) -> str:
    """Return the explicit UUID cast join predicate between a dlt hot table (`VARCHAR`) and v2 `artifact` (`UUID`)."""
    return f"TRY_CAST({hot_alias}.consist_artifact_id AS UUID) = {artifact_alias}.id"


def _extract_schema_model_columns(schema_model: Type[BaseModel]) -> List[str]:
    from sqlalchemy.sql.schema import Column as SAColumn
    from sqlmodel.main import FieldInfo as SQLModelFieldInfo

    schema_columns: List[str] = []
    for attr_name, field_info in schema_model.model_fields.items():
        sa_col = (
            field_info.sa_column if isinstance(field_info, SQLModelFieldInfo) else None
        )
        col_name = sa_col.name if isinstance(sa_col, SAColumn) else None
        sql_name = (
            str(col_name) if isinstance(col_name, str) and col_name else str(attr_name)
        )
        if sql_name.startswith("consist_"):
            continue
        schema_columns.append(sql_name)
    return schema_columns


class ViewFactory:
    """
    A factory class responsible for generating **"Hybrid Views"** in DuckDB, acting as
    Consist's **"The Virtualizer"** component.

    Hybrid Views combine data from materialized tables (often ingested via dlt)
    with data directly from file-based artifacts (e.g., Parquet, CSV),
    providing a unified SQL interface to query both "hot" and "cold" data
    transparently. This approach is central to Consist's flexible data access strategy.
    """

    def __init__(self, tracker: "Tracker") -> None:
        self.tracker = tracker

    def _require_ledger(self) -> "StoreLedger":
        ledger = self.tracker.ledger
        if ledger is None:
            raise RuntimeError("Cannot create views: No database configured.")
        return ledger

    def _batch_global_tables_columns(
        self,
        ledger: "StoreLedger",
        table_names: Iterable[str],
    ) -> dict[str, set[str]]:
        names = sorted({str(t) for t in table_names if t})
        if not names:
            return {}
        try:
            with ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT table_name, column_name "
                    "FROM information_schema.columns "
                    "WHERE table_catalog = current_database() "
                    "AND table_schema = 'global_tables' "
                    "AND table_name IN (SELECT UNNEST(?))",
                    [names],
                ).fetchall()
            cols_by_table: dict[str, set[str]] = {}
            for tbl_raw, col_raw in rows:
                if tbl_raw is not None and col_raw is not None:
                    cols_by_table.setdefault(str(tbl_raw), set()).add(str(col_raw))
            return cols_by_table
        except Exception:
            return {}

    def _batch_locate_artifacts(
        self,
        ledger: "StoreLedger",
        artifacts: Sequence[Artifact],
    ) -> tuple[
        dict[uuid.UUID, str],
        dict[uuid.UUID, list[dict[str, Any]]],
        set[uuid.UUID],
    ]:
        """Batch-query `StoreLedger.locate_current` once for all `artifacts`.

        Returns `(hot_table_by_artifact_id, cold_locations_by_artifact_id, v1_null_art_ids)`.
        """
        if not artifacts:
            return {}, {}, set()

        hist_anchors = sorted(
            {
                str(art.identity.anchor)
                for art in artifacts
                if art.identity.anchor.kind is Kind.HISTORICAL
            }
            | {
                str(art.recorded_anchor)
                for art in artifacts
                if art.recorded_anchor is not None
                and art.recorded_anchor.kind is Kind.HISTORICAL
            }
        )
        promoted_by_hist: dict[str, Digest] = {}
        if hist_anchors:
            with ledger.exec_cursor() as cur:
                promo_rows = cur.execute(
                    "SELECT historical_anchor, verified_anchor "
                    "FROM main.anchor_promotion "
                    "WHERE historical_anchor IN (SELECT UNNEST(?))",
                    [hist_anchors],
                ).fetchall()
            for h_str, v_str in promo_rows:
                try:
                    promoted_by_hist[str(h_str)] = Digest.parse(str(v_str))
                except ValueError:
                    continue

        anchor_set: dict[str, Digest] = {}
        anchors_for_art: dict[uuid.UUID, list[str]] = {}
        for art in artifacts:
            art_keys: list[str] = []
            primary = art.identity.anchor
            p_str = str(primary)
            anchor_set[p_str] = primary
            art_keys.append(p_str)
            if p_str in promoted_by_hist:
                prom = promoted_by_hist[p_str]
                prom_str = str(prom)
                anchor_set[prom_str] = prom
                art_keys.append(prom_str)
            if art.recorded_anchor is not None:
                rec_str = str(art.recorded_anchor)
                anchor_set[rec_str] = art.recorded_anchor
                if rec_str not in art_keys:
                    art_keys.append(rec_str)
                if rec_str in promoted_by_hist:
                    prom = promoted_by_hist[rec_str]
                    prom_str = str(prom)
                    anchor_set[prom_str] = prom
                    if prom_str not in art_keys:
                        art_keys.append(prom_str)
            anchors_for_art[art.id] = art_keys

        all_loc_rows = ledger.locate_current(
            list(anchor_set.values()),
            include_evicted=False,
        )
        rows_by_anchor: dict[str, list[dict[str, Any]]] = {}
        for loc_row in all_loc_rows:
            a_key = str(loc_row.get("anchor") or "")
            if a_key:
                rows_by_anchor.setdefault(a_key, []).append(loc_row)

        candidate_hot_by_art: dict[uuid.UUID, str] = {}
        candidate_v1_run_scoped_ids: set[uuid.UUID] = set()
        cold_rows_by_art: dict[uuid.UUID, list[dict[str, Any]]] = {}
        candidate_hot_tables: set[str] = set()
        art_by_id: dict[uuid.UUID, Artifact] = {art.id: art for art in artifacts}

        site_ctx = self.tracker.site_context or SiteContext.detect()

        def _loc_sort_key(row: dict[str, Any]) -> tuple[int, int]:
            row_site = str(row.get("site") or "local")
            row_uri = str(row.get("uri") or "")
            matched_mount = None
            if row_uri and not row_uri.startswith("db://"):
                try:
                    _, matched_mount = resolve_uri_to_path(row_uri, ledger.mounts)
                except Exception:
                    matched_mount = None
            visible = _is_candidate_visible(
                row,
                site_context=site_ctx,
                matched_mount=matched_mount,
            )
            if visible and row_site == site_ctx.site:
                vis_rank = 0
            elif visible:
                vis_rank = 1
            else:
                vis_rank = 2
            obs_ns = int(row.get("observed_at_ns") or 0)
            return (vis_rank, -obs_ns)

        for art in artifacts:
            merged_rows: list[dict[str, Any]] = []
            for a_key in anchors_for_art.get(art.id, []):
                merged_rows.extend(rows_by_anchor.get(a_key, []))
            merged_rows.sort(key=_loc_sort_key)

            hot_tbl: Optional[str] = None
            hot_v1_run_scoped = False
            cold_list: list[dict[str, Any]] = []
            for r in merged_rows:
                uri = str(r.get("uri") or "")
                if not uri:
                    continue
                info = parse_hot_db_member(uri, r.get("member"))
                if info is not None and info.schema_name == "global_tables":
                    matched, is_v1_run = matches_hot_db_info(
                        info,
                        artifact_id=art.id,
                        run_id=art.run_id,
                    )
                    if matched and hot_tbl is None:
                        hot_tbl = info.table_name
                        hot_v1_run_scoped = is_v1_run
                    continue
                if uri.startswith("db://"):
                    continue
                cold_list.append(r)

            if hot_tbl is None:
                for obs in art.hints:
                    info = parse_hot_db_member(obs.location.uri, obs.location.member)
                    if info is not None and info.schema_name == "global_tables":
                        matched, is_v1_run = matches_hot_db_info(
                            info,
                            artifact_id=art.id,
                            run_id=art.run_id,
                        )
                        if matched:
                            hot_tbl = info.table_name
                            hot_v1_run_scoped = is_v1_run
                            break

            if hot_tbl is None and bool(art.meta.get("is_ingested")):
                raw_meta_tbl = (
                    art.meta.get("dlt_table_name")
                    or art.meta.get("_dlt_table_name")
                    or art.key
                )
                if isinstance(raw_meta_tbl, str) and raw_meta_tbl.strip():
                    hot_tbl = raw_meta_tbl.strip()

            if hot_tbl is not None:
                candidate_hot_by_art[art.id] = hot_tbl
                candidate_hot_tables.add(hot_tbl)
                if hot_v1_run_scoped:
                    candidate_v1_run_scoped_ids.add(art.id)
            cold_rows_by_art[art.id] = cold_list

        # Validate hot tables exist in DuckDB via a single batched information_schema query
        # and verify `consist_artifact_id` (plus v1-migrated `consist_artifact_id IS NULL`
        # rows keyed by `consist_run_id` when `art.meta.get("is_ingested")` is set).
        cols_by_table = self._batch_global_tables_columns(ledger, candidate_hot_tables)
        existing_tables: dict[str, Optional[set[str]]] = {}
        v1_null_art_ids: set[uuid.UUID] = set()

        for tbl_name in sorted(candidate_hot_tables):
            tbl_cols = cols_by_table.get(tbl_name)
            if not tbl_cols:
                existing_tables[tbl_name] = None
                continue
            if "consist_artifact_id" in tbl_cols:
                present_ids: set[str] = set()
                try:
                    with ledger.exec_cursor() as cur:
                        id_rows = cur.execute(
                            f"SELECT DISTINCT CAST(consist_artifact_id AS VARCHAR) "
                            f"FROM global_tables.{_quote_ident(tbl_name)} "
                            "WHERE consist_artifact_id IS NOT NULL"
                        ).fetchall()
                        present_ids = {str(r[0]) for r in id_rows if r[0]}

                        if "consist_run_id" in tbl_cols:
                            v1_candidates = [
                                art_by_id[aid]
                                for aid, tname in candidate_hot_by_art.items()
                                if tname == tbl_name
                                and str(aid) not in present_ids
                                and art_by_id[aid].run_id
                                and (
                                    aid in candidate_v1_run_scoped_ids
                                    or bool(art_by_id[aid].meta.get("is_ingested"))
                                    or art_by_id[aid].driver == "dlt"
                                )
                            ]
                            if v1_candidates:
                                cand_run_ids = sorted(
                                    {str(a.run_id) for a in v1_candidates if a.run_id}
                                )
                                run_rows = cur.execute(
                                    f"SELECT DISTINCT CAST(consist_run_id AS VARCHAR) "
                                    f"FROM global_tables.{_quote_ident(tbl_name)} "
                                    "WHERE consist_artifact_id IS NULL "
                                    "AND CAST(consist_run_id AS VARCHAR) IN (SELECT UNNEST(?))",
                                    [cand_run_ids],
                                ).fetchall()
                                matched_v1_runs = {str(r[0]) for r in run_rows if r[0]}
                                for v1_art in v1_candidates:
                                    if (
                                        v1_art.run_id is not None
                                        and str(v1_art.run_id) in matched_v1_runs
                                    ):
                                        present_ids.add(str(v1_art.id))
                                        v1_null_art_ids.add(v1_art.id)
                except Exception:
                    present_ids = set()
                existing_tables[tbl_name] = present_ids
            else:
                existing_tables[tbl_name] = {"*"}

        verified_hot_by_art: dict[uuid.UUID, str] = {}
        for art_id, tbl_name in candidate_hot_by_art.items():
            verified_ids = existing_tables.get(tbl_name)
            if verified_ids is None:
                continue
            if "*" in verified_ids or str(art_id) in verified_ids:
                verified_hot_by_art[art_id] = tbl_name

        return verified_hot_by_art, cold_rows_by_art, v1_null_art_ids

    def _resolve_cold_record_path(
        self,
        record: GroupedViewRecord,
        cold_locations: Sequence[dict[str, Any]],
    ) -> Optional[Path]:
        ledger = self._require_ledger()
        seen_paths: set[Path] = set()

        def _try_uri(uri: str) -> Optional[Path]:
            cleaned = uri.strip()
            if not cleaned or cleaned.startswith("db://"):
                return None
            for resolver in (
                lambda: resolve_uri_to_path(cleaned, ledger.mounts)[0],
                lambda: Path(self.tracker.resolve_uri(cleaned)),
            ):
                try:
                    candidate = resolver()
                except Exception:
                    continue
                if candidate is None:
                    continue
                try:
                    resolved = candidate.resolve()
                except OSError:
                    continue
                if resolved in seen_paths:
                    continue
                seen_paths.add(resolved)
                if resolved.exists():
                    return resolved
            return None

        for loc_row in cold_locations:
            uri = str(loc_row.get("uri") or "")
            found = _try_uri(uri)
            if found is not None:
                return found

        for obs in record.artifact.hints:
            found = _try_uri(obs.location.uri)
            if found is not None:
                return found

        if record.run is not None:
            try:
                historical_path = self.tracker.resolve_historical_path(
                    record.artifact, record.run
                )
                if historical_path.exists():
                    return historical_path.resolve()
            except Exception:
                pass

        return None

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
        ledger = self._require_ledger()
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

        from consist.integrations.ibis import suspend_tracker_ibis_connection

        with suspend_tracker_ibis_connection(self.tracker):
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
                    if run is not None
                    and isinstance(run.model_name, str)
                    and run.model_name
                }
                if len(model_names) == 1:
                    run_namespace = next(iter(model_names))

            facet_values_by_run: Dict[str, Dict[str, Any]] = {}
            if facets and selected and run_namespace is not None:
                run_ids = [
                    run.id
                    for _, run in selected
                    if run is not None and isinstance(run.id, str)
                ]
                if run_ids:
                    for facet in facets:
                        facet_values_by_run[facet] = (
                            self.tracker.db.get_facet_values_for_runs(
                                run_ids,
                                key=facet,
                                namespace=run_namespace,
                            )
                        )

            all_artifacts = [artifact for artifact, _ in selected]
            hot_by_art, cold_locs_by_art, v1_null_art_ids = (
                self._batch_locate_artifacts(ledger, all_artifacts)
            )

            records: List[GroupedViewRecord] = []
            for artifact, run in selected:
                facet_values: Dict[str, Any] = {}
                art_facet_map = facet_values_by_artifact.get(artifact.id, {})
                for facet in facets:
                    info = art_facet_map.get(facet)
                    if info is None:
                        run_facet_map = facet_values_by_run.get(facet, {})
                        if run is not None and run.id in run_facet_map:
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
                        hot_table=hot_by_art.get(artifact.id),
                        v1_null_artifact_id=(artifact.id in v1_null_art_ids),
                    )
                )

            facet_casts = {
                facet: _facet_type_to_duckdb(types)
                for facet, types in facet_types.items()
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
                    cold_locations_by_artifact=cold_locs_by_art,
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
        """
        Creates or replaces a DuckDB SQL VIEW that combines "hot" and "cold" data for a given concept.
        """
        ledger = self._require_ledger()
        from consist.integrations.ibis import suspend_tracker_ibis_connection

        with suspend_tracker_ibis_connection(self.tracker):
            concept_cols = self._batch_global_tables_columns(ledger, [concept_key]).get(
                concept_key
            )
            hot_table_exists = bool(concept_cols)
            hot_artifact_ids_in_concept: set[str] = set()
            hot_v1_null_run_ids_in_concept: set[str] = set()
            if concept_cols and "consist_artifact_id" in concept_cols:
                try:
                    with ledger.exec_cursor() as cur:
                        id_rows = cur.execute(
                            f"SELECT DISTINCT CAST(consist_artifact_id AS VARCHAR) "
                            f"FROM global_tables.{_quote_ident(concept_key)} "
                            "WHERE consist_artifact_id IS NOT NULL"
                        ).fetchall()
                        hot_artifact_ids_in_concept = {
                            str(r[0]) for r in id_rows if r[0]
                        }
                        if "consist_run_id" in concept_cols:
                            v1_rows = cur.execute(
                                f"SELECT DISTINCT CAST(consist_run_id AS VARCHAR) "
                                f"FROM global_tables.{_quote_ident(concept_key)} "
                                "WHERE consist_artifact_id IS NULL "
                                "AND consist_run_id IS NOT NULL"
                            ).fetchall()
                            hot_v1_null_run_ids_in_concept = {
                                str(r[0]) for r in v1_rows if r[0]
                            }
                except Exception:
                    hot_artifact_ids_in_concept = set()
                    hot_v1_null_run_ids_in_concept = set()

            drivers = driver_filter or ["parquet", "csv"]
            cold_sql, extra_hot_tables = self._generate_cold_and_extra_hot_queries(
                ledger=ledger,
                concept_key=concept_key,
                drivers=drivers,
                concept_hot_artifact_ids=hot_artifact_ids_in_concept,
                concept_hot_v1_run_ids=hot_v1_null_run_ids_in_concept,
            )

            parts: List[str] = []
            emitted_hot_tables: set[str] = set()

            if hot_table_exists and concept_cols is not None:
                emitted_hot_tables.add(concept_key)
                if "consist_run_id" in concept_cols:
                    hot_query = (
                        "SELECT t.*, r.parent_run_id AS consist_scenario_id "
                        f"FROM global_tables.{_quote_ident(concept_key)} t "
                        "LEFT JOIN main.run r ON t.consist_run_id = r.id"
                    )
                else:
                    hot_query = (
                        "SELECT t.*, CAST(NULL AS VARCHAR) AS consist_scenario_id "
                        f"FROM global_tables.{_quote_ident(concept_key)} t"
                    )
                parts.append(hot_query)

            if extra_hot_tables:
                extra_cols_by_table = self._batch_global_tables_columns(
                    ledger, extra_hot_tables.keys()
                )
            else:
                extra_cols_by_table = {}

            for extra_tbl, (
                art_id_set,
                v1_run_set,
                all_run_set,
            ) in extra_hot_tables.items():
                if extra_tbl in emitted_hot_tables:
                    continue
                extra_cols = extra_cols_by_table.get(extra_tbl)
                if not extra_cols:
                    continue
                if (
                    "consist_artifact_id" not in extra_cols
                    and "consist_run_id" not in extra_cols
                ):
                    continue

                where_clauses: list[str] = []
                if "consist_artifact_id" in extra_cols:
                    if art_id_set:
                        escaped_ids = ", ".join(
                            _safe_duckdb_string_literal(aid)
                            for aid in sorted(art_id_set)
                        )
                        where_clauses.append(
                            f"CAST(t.consist_artifact_id AS VARCHAR) IN ({escaped_ids})"
                        )
                    if "consist_run_id" in extra_cols and v1_run_set:
                        escaped_v1_runs = ", ".join(
                            _safe_duckdb_string_literal(rid)
                            for rid in sorted(v1_run_set)
                        )
                        where_clauses.append(
                            "(t.consist_artifact_id IS NULL AND "
                            f"CAST(t.consist_run_id AS VARCHAR) IN ({escaped_v1_runs}))"
                        )
                elif "consist_run_id" in extra_cols and all_run_set:
                    escaped_runs = ", ".join(
                        _safe_duckdb_string_literal(rid) for rid in sorted(all_run_set)
                    )
                    where_clauses.append(
                        f"CAST(t.consist_run_id AS VARCHAR) IN ({escaped_runs})"
                    )

                if not where_clauses:
                    continue

                emitted_hot_tables.add(extra_tbl)
                if "consist_run_id" in extra_cols:
                    select_from = (
                        "SELECT t.*, r.parent_run_id AS consist_scenario_id "
                        f"FROM global_tables.{_quote_ident(extra_tbl)} t "
                        "LEFT JOIN main.run r ON t.consist_run_id = r.id"
                    )
                else:
                    select_from = (
                        "SELECT t.*, CAST(NULL AS VARCHAR) AS consist_scenario_id "
                        f"FROM global_tables.{_quote_ident(extra_tbl)} t"
                    )
                parts.append(f"{select_from} WHERE {' OR '.join(where_clauses)}")

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
        ledger = self._require_ledger()
        quoted_view_name = _quote_ident(view_name)
        with ledger._lock:
            with ledger.exec_cursor() as cur:
                if if_exists == "error":
                    existing = cur.execute(
                        "SELECT COUNT(*) FROM information_schema.views "
                        "WHERE table_catalog = current_database() "
                        "AND table_schema = 'main' AND table_name = ?",
                        [view_name],
                    ).fetchone()
                    if existing is not None and int(existing[0]) > 0:
                        raise ValueError(f"View already exists: {view_name}")
                    cur.execute(f"CREATE VIEW {quoted_view_name} AS \n{query}")
                else:
                    cur.execute(
                        f"CREATE OR REPLACE VIEW {quoted_view_name} AS \n{query}"
                    )

    def _table_columns(self, schema: str, table: str) -> set[str]:
        ledger = self._require_ledger()
        try:
            with ledger.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_catalog = current_database() "
                    "AND table_schema = ? AND table_name = ?",
                    [schema, table],
                ).fetchall()
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
            if not record.hot_table:
                continue
            grouped_by_table.setdefault(record.hot_table, []).append(record)

        if not grouped_by_table:
            return None

        cols_by_table = self._batch_global_tables_columns(
            self._require_ledger(), grouped_by_table.keys()
        )
        union_parts: List[str] = []
        facet_col_names = {facet: f"facet_{idx}" for idx, facet in enumerate(facets)}
        for table_name, table_records in grouped_by_table.items():
            table_cols = cols_by_table.get(table_name)
            if not table_cols:
                logging.warning(
                    "[Consist Warning] Skipping grouped hot table that does not exist: %s",
                    table_name,
                )
                continue

            if "consist_artifact_id" not in table_cols:
                logging.warning(
                    "[Consist Warning] Skipping grouped hot table missing consist_artifact_id: %s",
                    table_name,
                )
                continue

            has_v1_null = "consist_run_id" in table_cols and any(
                r.v1_null_artifact_id for r in table_records
            )

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
            base_meta_cols = ["art_id", "run_id", "year", "iter", "scenario"]
            if has_v1_null:
                base_meta_cols.append("v1_null")
            meta_columns = base_meta_cols + list(facet_col_names.values())
            meta_rows: List[str] = []
            for record in table_records:
                run_id_val = (
                    record.run.id if record.run is not None else record.artifact.run_id
                )
                year_val = record.run.year if record.run is not None else None
                iter_val = record.run.iteration if record.run is not None else None
                scenario_val = (
                    record.run.parent_run_id if record.run is not None else None
                )
                row_values = [
                    _safe_duckdb_string_literal(str(record.artifact.id)),
                    _safe_duckdb_string_literal(run_id_val),
                    _safe_duckdb_literal(year_val),
                    _safe_duckdb_literal(iter_val),
                    _safe_duckdb_string_literal(scenario_val),
                ]
                if has_v1_null:
                    row_values.append(
                        _safe_duckdb_literal(bool(record.v1_null_artifact_id))
                    )
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
            if has_v1_null:
                join_cond = (
                    f"(CAST(t.consist_artifact_id AS VARCHAR) = {meta_alias}.art_id "
                    f"OR ({meta_alias}.v1_null AND t.consist_artifact_id IS NULL "
                    f"AND CAST(t.consist_run_id AS VARCHAR) = {meta_alias}.run_id))"
                )
            else:
                join_cond = (
                    f"CAST(t.consist_artifact_id AS VARCHAR) = {meta_alias}.art_id"
                )
            query = f"""
            SELECT
                {", ".join(select_fields)}
            FROM global_tables.{_quote_ident(table_name)} t
            JOIN (
                VALUES {", ".join(meta_rows)}
            ) AS {meta_alias}({meta_cols_sql})
            ON {join_cond}
            """
            union_parts.append(query)

        if not union_parts:
            return None
        return "\nUNION ALL BY NAME\n".join(union_parts)

    def _generate_grouped_cold_query_optimized(
        self,
        *,
        records: List[GroupedViewRecord],
        cold_locations_by_artifact: Mapping[uuid.UUID, Sequence[dict[str, Any]]],
        facets: List[str],
        facet_casts: Dict[str, str],
        include_system_columns: bool,
        missing_files: Literal["warn", "error", "skip_silent"],
    ) -> Optional[str]:
        grouped: Dict[str, List[Tuple[GroupedViewRecord, Path]]] = {}
        for record in records:
            if record.hot_table:
                continue

            driver = record.artifact.driver
            if driver not in {"parquet", "csv"}:
                continue

            cold_locs = cold_locations_by_artifact.get(record.artifact.id, ())
            resolved_path = self._resolve_cold_record_path(record, cold_locs)
            if resolved_path is None:
                fallback_uri = ""
                if cold_locs:
                    fallback_uri = str(cold_locs[0].get("uri") or "")
                elif record.artifact.hints:
                    fallback_uri = record.artifact.hints[0].location.uri
                else:
                    fallback_uri = record.artifact.key
                try:
                    abs_path = (
                        self.tracker.resolve_uri(fallback_uri)
                        if fallback_uri
                        else record.artifact.key
                    )
                except Exception:
                    abs_path = fallback_uri or record.artifact.key
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
                run_id_val = (
                    record.run.id if record.run is not None else record.artifact.run_id
                )
                year_val = record.run.year if record.run is not None else None
                iter_val = record.run.iteration if record.run is not None else None
                scenario_val = (
                    record.run.parent_run_id if record.run is not None else None
                )
                row_values = [
                    path_literal,
                    _safe_duckdb_string_literal(run_id_val),
                    _safe_duckdb_string_literal(str(record.artifact.id)),
                    _safe_duckdb_literal(year_val),
                    _safe_duckdb_literal(iter_val),
                    _safe_duckdb_string_literal(scenario_val),
                ]
                for facet in facets:
                    row_values.append(
                        _safe_duckdb_literal(record.facet_values.get(facet))
                    )
                meta_rows.append(f"({', '.join(row_values)})")

            unique_paths = list(dict.fromkeys(path_list))
            if driver == "parquet":
                reader_func = (
                    f"read_parquet([{', '.join(unique_paths)}], "
                    "union_by_name=true, filename=true)"
                )
            elif driver == "csv":
                reader_func = (
                    f"read_csv_auto([{', '.join(unique_paths)}], "
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
        ledger = self._require_ledger()
        try:
            with ledger.exec_cursor() as cur:
                row = cur.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_catalog = current_database() "
                    "AND table_schema = ? AND table_name = ?",
                    [schema, table],
                ).fetchone()
                return row is not None and int(row[0]) > 0
        except Exception as e:
            logging.warning(f"Failed to check table existence: {e}")
            return False

    def _generate_cold_and_extra_hot_queries(
        self,
        *,
        ledger: "StoreLedger",
        concept_key: str,
        drivers: List[str],
        concept_hot_artifact_ids: set[str],
        concept_hot_v1_run_ids: Optional[set[str]] = None,
    ) -> tuple[Optional[str], dict[str, tuple[set[str], set[str], set[str]]]]:
        """Query `main.artifact LEFT JOIN main.run` for `concept_key`, deduplicate hot artifacts, and build the cold SQL branch."""
        from consist.store.ledger import StoreLedger as _StoreLedgerCls

        v1_concept_runs = concept_hot_v1_run_ids or set()
        run_cols = ", ".join(
            f"r.{c.strip()}" for c in _StoreLedgerCls._RUN_SELECT_COLUMNS.split(",")
        )
        with ledger.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT a.id, a.key, a.driver, a.anchor, a.run_id, "
                "a.parent_artifact_id, a.schema_id, a.meta, a.created_at, "
                f"{run_cols} "
                "FROM main.artifact a "
                "LEFT JOIN main.run r ON a.run_id = r.id "
                "WHERE a.key = ? "
                "ORDER BY a.created_at ASC, a.id ASC",
                [concept_key],
            ).fetchall()

        if not rows:
            return None, {}

        art_rows = [r[:9] for r in rows]
        run_rows = [r[9:] for r in rows]
        hydrated_arts = ledger._hydrate_artifact_rows(art_rows)
        hydrated_runs = [
            _StoreLedgerCls._hydrate_run_row(r) if r[0] is not None else None
            for r in run_rows
        ]

        hot_by_art, cold_locs_by_art, v1_null_art_ids = self._batch_locate_artifacts(
            ledger, hydrated_arts
        )
        extra_hot_tables: dict[str, tuple[set[str], set[str], set[str]]] = {}
        cold_records: List[GroupedViewRecord] = []

        for art, run_obj in zip(hydrated_arts, hydrated_runs):
            hot_tbl = hot_by_art.get(art.id)
            is_v1_ingested_in_concept = (
                art.run_id is not None
                and str(art.run_id) in v1_concept_runs
                and (
                    art.id in v1_null_art_ids
                    or bool(art.meta.get("is_ingested"))
                    or art.driver == "dlt"
                )
            )
            if str(art.id) in concept_hot_artifact_ids or is_v1_ingested_in_concept:
                hot_tbl = concept_key
            elif hot_tbl is not None and hot_tbl != concept_key:
                art_ids_s, v1_runs_s, all_runs_s = extra_hot_tables.setdefault(
                    hot_tbl, (set(), set(), set())
                )
                art_ids_s.add(str(art.id))
                if art.run_id is not None:
                    all_runs_s.add(str(art.run_id))
                    if art.id in v1_null_art_ids:
                        v1_runs_s.add(str(art.run_id))

            if hot_tbl is not None:
                continue
            if art.driver not in drivers:
                continue
            cold_records.append(
                GroupedViewRecord(
                    artifact=art,
                    run=run_obj,
                    facet_values={},
                    hot_table=None,
                )
            )

        if not cold_records:
            return None, extra_hot_tables

        cold_sql = self._generate_grouped_cold_query_optimized(
            records=cold_records,
            cold_locations_by_artifact=cold_locs_by_art,
            facets=[],
            facet_casts={},
            include_system_columns=True,
            missing_files="warn",
        )
        return cold_sql, extra_hot_tables
