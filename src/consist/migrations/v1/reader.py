"""
Read-only, column-introspecting Consist v1 database reader (`V1DatabaseReader`).

Inspects `duckdb_tables()` and `duckdb_columns()` dynamically so historical
databases from `v0.1.x` through `v0.4.2` can be read without assuming later
columns (`hash_algorithm`, `hash_semantics`, `content_id`, `parent_artifact_id`,
`recovery_roots`, `facet`, `git_hash`, `signature`, `stage`, `phase`) or tables
(`artifact_content`, `artifact_kv`, `artifact_facet`, `run_artifact_dir_entry`,
`global_tables.*`) exist.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any
import uuid

import duckdb


def _parse_json_obj(val: Any) -> dict[str, Any]:
    if isinstance(val, dict):
        return dict(val)
    if isinstance(val, str) and (raw := val.strip()):
        try:
            loaded = json.loads(raw)
            return dict(loaded) if isinstance(loaded, dict) else {}
        except ValueError:
            pass
    return {}


def _parse_json_list_str(val: Any) -> list[str]:
    if isinstance(val, (list, tuple)):
        return [str(x) for x in val]
    if isinstance(val, str) and (raw := val.strip()):
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, list):
                return [str(x) for x in loaded]
        except ValueError:
            pass
    return []


def _parse_uuid(val: Any) -> uuid.UUID | None:
    if isinstance(val, uuid.UUID):
        return val
    if val is not None and (raw := str(val).strip()):
        return uuid.UUID(raw)
    return None


def _parse_datetime(val: Any) -> datetime | None:
    if isinstance(val, datetime):
        return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
    if val is not None and (raw := str(val).strip()):
        try:
            dt = datetime.fromisoformat(raw)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except ValueError:
            pass
    return None


def _parse_json_any(val: Any) -> Any:
    if isinstance(val, str) and (raw := val.strip()):
        try:
            return json.loads(raw)
        except ValueError:
            return val
    return val


def _parse_typed_kv_value(kv_row: Mapping[str, Any]) -> Any:
    vtype = str(kv_row.get("value_type") or "").strip().lower()
    if vtype == "null":
        return None
    if vtype == "int" and kv_row.get("value_num") is not None:
        return int(round(float(kv_row["value_num"])))
    if vtype == "float" and kv_row.get("value_num") is not None:
        return float(kv_row["value_num"])
    if vtype == "bool" and kv_row.get("value_bool") is not None:
        return bool(kv_row["value_bool"])
    if vtype in ("str", "string") and kv_row.get("value_str") is not None:
        return str(kv_row["value_str"])
    if vtype == "json" and kv_row.get("value_json") is not None:
        return _parse_json_any(kv_row["value_json"])

    for val_col in (
        "value_str",
        "value_num",
        "value_bool",
        "value_json",
        "value",
    ):
        if kv_row.get(val_col) is not None:
            raw_val = kv_row[val_col]
            return _parse_json_any(raw_val) if val_col == "value_json" else raw_val
    return None


def _split_escaped_key_path(key_path: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    escaped = False
    for ch in key_path:
        if escaped:
            buf.append(ch)
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == ".":
            parts.append("".join(buf))
            buf.clear()
        else:
            buf.append(ch)
    if escaped:
        buf.append("\\")
    parts.append("".join(buf))
    return [p for p in parts if p] or [key_path]


def unflatten_facet_key_path(target: dict[str, Any], key_path: str, val: Any) -> None:
    """Insert `val` into `target` along unescaped `.` segments in `key_path`."""
    segments = _split_escaped_key_path(key_path)
    cur = target
    for seg in segments[:-1]:
        existing = cur.get(seg)
        if not isinstance(existing, dict):
            nested: dict[str, Any] = {}
            cur[seg] = nested
            cur = nested
        else:
            cur = existing
    cur[segments[-1]] = val


_FK_REF_RE = re.compile(
    r'REFERENCES\s+(?:(?:"((?:[^"]|"")+)"|([A-Za-z_][A-Za-z0-9_]*))\s*\.\s*)?'
    r'(?:(?:"((?:[^"]|"")+)"|([A-Za-z_][A-Za-z0-9_]*))\s*\.\s*)?'
    r'(?:"((?:[^"]|"")+)"|([A-Za-z_][A-Za-z0-9_]*))',
    re.IGNORECASE,
)


def _unescape_ident_match(quoted: str | None, plain: str | None) -> str | None:
    if quoted is not None:
        return quoted.replace('""', '"')
    return plain


def _extract_fk_target(
    constraint_text: str,
    *,
    default_schema: str,
) -> tuple[str, str] | None:
    m = _FK_REF_RE.search(constraint_text)
    if m is None:
        return None
    p1 = _unescape_ident_match(m.group(1), m.group(2))
    p2 = _unescape_ident_match(m.group(3), m.group(4))
    tbl = _unescape_ident_match(m.group(5), m.group(6))
    if not tbl:
        return None
    if p2 is not None:
        return (p2, tbl)
    if p1 is not None:
        return (p1, tbl)
    return (default_schema, tbl)


def _opt_str(val: Any) -> str | None:
    return str(val) if val is not None else None


def _quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


@dataclass(frozen=True, slots=True)
class V1RunRecord:
    """Normalized record read from a v1 `run` table."""

    id: str
    model_name: str
    status: str
    description: str | None = None
    year: int | None = None
    iteration: int | None = None
    stage: str | None = None
    phase: str | None = None
    parent_run_id: str | None = None
    config_hash: str | None = None
    git_hash: str | None = None
    input_hash: str | None = None
    signature: str | None = None
    tags: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    facet: dict[str, Any] | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class V1ArtifactRecord:
    """Normalized record read from a v1 `artifact` (and optional `artifact_content`) table."""

    id: uuid.UUID
    key: str
    container_uri: str
    driver: str
    hash: str | None = None
    hash_algorithm: str | None = None
    hash_semantics: dict[str, Any] | str | None = None
    table_path: str | None = None
    array_path: str | None = None
    content_id: uuid.UUID | None = None
    parent_artifact_id: uuid.UUID | None = None
    run_id: str | None = None
    schema_id: str | None = None
    recovery_roots: list[str] | None = None
    facet: dict[str, Any] | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    content_hash: str | None = None
    content_meta: dict[str, Any] | None = None
    is_dir: bool = False
    facet_kv: dict[str, Any] = field(default_factory=dict)
    facet_id: str | None = None
    facet_namespace: str | None = None
    facet_schema_version: str | None = None


@dataclass(frozen=True, slots=True)
class V1LinkRecord:
    """Normalized record read from a v1 `run_artifact_link` table."""

    run_id: str
    artifact_id: uuid.UUID
    direction: str
    is_implicit: bool = False


@dataclass(frozen=True, slots=True)
class V1DirEntryRecord:
    """Normalized record read from a legacy `run_artifact_dir_entry` table."""

    artifact_id: uuid.UUID | None = None
    run_id: str | None = None
    rel_path: str = ""
    kind: str = "file"
    size: int | None = None
    sha256: str | None = None
    mtime_ns: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class V1DltTableInfo:
    """Metadata describing a non-`main` (`dlt` / `global_tables`) table in a v1 DB."""

    schema_name: str
    table_name: str
    row_count: int
    columns: dict[str, str]
    has_consist_artifact_id: bool
    consist_artifact_id_type: str | None
    has_consist_run_id: bool
    consist_run_id_type: str | None
    is_dlt_internal: bool


@dataclass(frozen=True, slots=True)
class V1ConfigFacetRecord:
    """Normalized record read from `main.config_facet`."""

    id: str
    namespace: str
    schema_name: str
    schema_version: str | None = None
    facet_json: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class V1RunConfigKvRecord:
    """Normalized record read from `main.run_config_kv`."""

    run_id: str
    facet_id: str | None
    namespace: str | None
    key: str
    value_type: str
    value: Any
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class V1CouplerSnapshotRecord:
    """Normalized record read from `main.run_coupler_snapshot`."""

    run_id: str
    phase: str
    role_key: str
    scenario_run_id: str
    step_ordinal: int
    artifact_id: uuid.UUID
    producer_run_id: str | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class V1BindingInvocationRecord:
    """Normalized record read from `main.run_binding_invocation`."""

    id: uuid.UUID
    requested_run_id: str
    execution_run_id: str
    cache_source_run_id: str | None
    cache_outcome: str
    binding_json: str
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class V1ArtifactSchemaRecord:
    """Normalized record read from `main.artifact_schema`."""

    id: str
    profile_version: int
    summary_json: dict[str, Any]
    profile_json: dict[str, Any] | None = None
    created_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class V1ArtifactSchemaFieldRecord:
    """Normalized record read from `main.artifact_schema_field`."""

    schema_id: str
    ordinal_position: int | None
    name: str
    logical_type: str
    nullable: bool
    stats_json: dict[str, Any] | None = None
    is_enum: bool = False
    enum_values_json: list[str] | None = None


@dataclass(frozen=True, slots=True)
class V1ArtifactSchemaObservationRecord:
    """Normalized record read from `main.artifact_schema_observation`."""

    id: uuid.UUID
    artifact_id: uuid.UUID
    schema_id: str
    run_id: str | None
    source: str
    sample_rows: int | None
    observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class V1ArtifactSchemaRelationRecord:
    """Normalized record read from `main.artifact_schema_relation`."""

    schema_id: str
    from_field: str
    to_table: str
    to_field: str
    relationship_type: str
    cardinality: str | None = None


@dataclass(frozen=True, slots=True)
class V1ArtifactSchemaBundle:
    """All `artifact_schema*` rows read from a v1 database."""

    schemas: tuple[V1ArtifactSchemaRecord, ...] = ()
    fields: tuple[V1ArtifactSchemaFieldRecord, ...] = ()
    observations: tuple[V1ArtifactSchemaObservationRecord, ...] = ()
    relations: tuple[V1ArtifactSchemaRelationRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class V1SequenceDDL:
    """Catalog sequence DDL read from `duckdb_sequences()`."""

    schema_name: str
    sequence_name: str
    sql: str


@dataclass(frozen=True, slots=True)
class V1TableDDL:
    """Catalog table DDL read from `duckdb_tables()` ordered by FK dependencies."""

    schema_name: str
    table_name: str
    sql: str
    fk_targets: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class V1IndexDDL:
    """Catalog secondary index DDL read from `duckdb_indexes()`."""

    schema_name: str
    table_name: str
    index_name: str
    sql: str


@dataclass(frozen=True, slots=True)
class V1UserSchemasDDL:
    """Introspected sequences, FK-ordered tables, and secondary indexes for user schemas."""

    schemas: tuple[str, ...]
    sequences: tuple[V1SequenceDDL, ...]
    tables: tuple[V1TableDDL, ...]
    indexes: tuple[V1IndexDDL, ...]
    catalog_name: str = ""


@dataclass(frozen=True, slots=True)
class V1SchemaInventory:
    """Summary of tables, columns, and row counts discovered in a v1 database."""

    db_path: Path
    consist_schema_version: int | None
    main_tables: frozenset[str]
    non_main_schemas: dict[str, tuple[str, ...]]
    table_columns: dict[str, frozenset[str]]
    run_count: int
    artifact_count: int
    link_count: int
    artifact_content_count: int | None
    dir_entry_count: int | None
    dlt_tables: tuple[V1DltTableInfo, ...]


class V1DatabaseReader:
    """Read-only, column-introspecting reader for historical Consist v1 DuckDB files."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        if not self.db_path.exists():
            raise FileNotFoundError(f"v1 database does not exist: {self.db_path}")
        self._con: duckdb.DuckDBPyConnection = duckdb.connect(
            str(self.db_path),
            read_only=True,
        )
        self._closed = False
        self._main_tables_cache: set[str] | None = None
        self._table_columns_cache: dict[tuple[str, str], dict[str, str]] = {}

    def __enter__(self) -> V1DatabaseReader:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._con.close()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return the underlying read-only DuckDB connection."""
        return self._con

    def _main_tables(self) -> set[str]:
        if self._main_tables_cache is None:
            self._main_tables_cache = {
                str(r[0])
                for r in self._con.execute(
                    "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
                ).fetchall()
            }
        return set(self._main_tables_cache)

    def _columns_for(self, schema_name: str, table_name: str) -> dict[str, str]:
        cache_key = (schema_name, table_name)
        cached = self._table_columns_cache.get(cache_key)
        if cached is not None:
            return dict(cached)
        rows = self._con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE schema_name = ? AND table_name = ? "
            "ORDER BY column_index",
            [schema_name, table_name],
        ).fetchall()
        cols = {str(col_name): str(dtype) for col_name, dtype in rows}
        self._table_columns_cache[cache_key] = cols
        return dict(cols)

    def _count_rows(self, schema_name: str, table_name: str) -> int:
        q_schema = _quote_ident(schema_name)
        q_table = _quote_ident(table_name)
        row = self._con.execute(f"SELECT COUNT(*) FROM {q_schema}.{q_table}").fetchone()
        return int(row[0]) if row is not None and row[0] is not None else 0

    def list_dlt_tables(self) -> list[V1DltTableInfo]:
        """Inspect all non-`main` tables (e.g. `global_tables.*`, `_dlt_*`)."""
        rows = self._con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() "
            "WHERE schema_name NOT IN ('main', 'information_schema', 'pg_catalog') "
            "ORDER BY schema_name, table_name"
        ).fetchall()
        out: list[V1DltTableInfo] = []
        for raw_schema, raw_table in rows:
            s_name = str(raw_schema)
            t_name = str(raw_table)
            cols = self._columns_for(s_name, t_name)
            art_id_type = cols.get("consist_artifact_id")
            run_id_type = cols.get("consist_run_id")
            out.append(
                V1DltTableInfo(
                    schema_name=s_name,
                    table_name=t_name,
                    row_count=self._count_rows(s_name, t_name),
                    columns=cols,
                    has_consist_artifact_id=art_id_type is not None,
                    consist_artifact_id_type=art_id_type,
                    has_consist_run_id=run_id_type is not None,
                    consist_run_id_type=run_id_type,
                    is_dlt_internal=t_name.startswith("_dlt_"),
                )
            )
        return out

    def inventory(self) -> V1SchemaInventory:
        """Build a full schema and row-count inventory of the v1 database."""
        main_tables = self._main_tables()
        table_cols: dict[str, frozenset[str]] = {
            t: frozenset(self._columns_for("main", t).keys())
            for t in sorted(main_tables)
        }

        version: int | None = None
        if "consist_schema" in main_tables:
            v_row = self._con.execute(
                'SELECT MAX(version) FROM main."consist_schema"'
            ).fetchone()
            if v_row is not None and v_row[0] is not None:
                version = int(v_row[0])

        dlt_tables = self.list_dlt_tables()
        non_main_by_schema: dict[str, list[str]] = {}
        for info in dlt_tables:
            non_main_by_schema.setdefault(info.schema_name, []).append(info.table_name)

        return V1SchemaInventory(
            db_path=self.db_path,
            consist_schema_version=version,
            main_tables=frozenset(main_tables),
            non_main_schemas={s: tuple(tbls) for s, tbls in non_main_by_schema.items()},
            table_columns=table_cols,
            run_count=self._count_rows("main", "run") if "run" in main_tables else 0,
            artifact_count=(
                self._count_rows("main", "artifact") if "artifact" in main_tables else 0
            ),
            link_count=(
                self._count_rows("main", "run_artifact_link")
                if "run_artifact_link" in main_tables
                else 0
            ),
            artifact_content_count=(
                self._count_rows("main", "artifact_content")
                if "artifact_content" in main_tables
                else None
            ),
            dir_entry_count=(
                self._count_rows("main", "run_artifact_dir_entry")
                if "run_artifact_dir_entry" in main_tables
                else None
            ),
            dlt_tables=tuple(dlt_tables),
        )

    def _iter_rows(
        self,
        table: str,
        *,
        order_by: tuple[str, ...] = (),
        required_cols: set[str] | None = None,
        batch_size: int = 4096,
    ) -> tuple[set[str], Iterator[dict[str, Any]]]:
        if table not in self._main_tables():
            return set(), iter(())
        cols = set(self._columns_for("main", table).keys())
        if required_cols is not None and not (required_cols <= cols):
            return cols, iter(())
        order_cols = [c for c in order_by if c in cols]
        order_clause = (
            f" ORDER BY {', '.join(_quote_ident(c) for c in order_cols)}"
            if order_cols
            else ""
        )
        sql = f"SELECT * FROM main.{_quote_ident(table)}{order_clause}"

        def _gen() -> Iterator[dict[str, Any]]:
            cur = self._con.cursor()
            try:
                cur.execute(sql)
                col_names = [str(d[0]) for d in (cur.description or ())]
                while batch := cur.fetchmany(batch_size):
                    for r in batch:
                        yield dict(zip(col_names, r))
            finally:
                cur.close()

        return cols, _gen()

    def _fetch_rows(
        self,
        table: str,
        *,
        order_by: tuple[str, ...] = (),
        required_cols: set[str] | None = None,
    ) -> tuple[set[str], list[dict[str, Any]]]:
        cols, row_iter = self._iter_rows(
            table,
            order_by=order_by,
            required_cols=required_cols,
        )
        return cols, list(row_iter)

    def iter_runs(self) -> Iterator[V1RunRecord]:
        """Yield `V1RunRecord` rows from `main.run`, adapting to available columns."""
        _, rows = self._iter_rows(
            "run",
            order_by=("created_at", "started_at", "id"),
            required_cols={"id"},
        )
        for row in rows:
            meta = _parse_json_obj(row.get("meta"))
            raw_facet = row.get("facet")
            facet = (
                _parse_json_obj(raw_facet)
                if raw_facet is not None
                else (
                    dict(meta["facet"]) if isinstance(meta.get("facet"), dict) else None
                )
            )
            stage_val = row.get("stage")
            if stage_val is None and isinstance(meta.get("stage"), str):
                stage_val = meta["stage"]
            phase_val = row.get("phase")
            if phase_val is None and isinstance(meta.get("phase"), str):
                phase_val = meta["phase"]

            started_at = _parse_datetime(row.get("started_at"))
            created_at = _parse_datetime(row.get("created_at")) or started_at

            yield V1RunRecord(
                id=str(row["id"]),
                model_name=str(row.get("model_name") or "unknown"),
                status=str(row.get("status") or "completed"),
                description=_opt_str(row.get("description")),
                year=int(row["year"]) if row.get("year") is not None else None,
                iteration=(
                    int(row["iteration"]) if row.get("iteration") is not None else None
                ),
                stage=_opt_str(stage_val),
                phase=_opt_str(phase_val),
                parent_run_id=_opt_str(row.get("parent_run_id")),
                config_hash=_opt_str(row.get("config_hash")),
                git_hash=_opt_str(row.get("git_hash")),
                input_hash=_opt_str(row.get("input_hash")),
                signature=_opt_str(row.get("signature")),
                tags=_parse_json_list_str(row.get("tags")),
                meta=meta,
                facet=facet,
                started_at=started_at,
                ended_at=_parse_datetime(row.get("ended_at")),
                created_at=created_at,
                updated_at=_parse_datetime(row.get("updated_at")),
            )

    def iter_artifacts(self) -> Iterator[V1ArtifactRecord]:
        """Yield `V1ArtifactRecord` rows from `main.artifact` (joining `artifact_content` when present)."""
        art_cols, rows = self._iter_rows(
            "artifact",
            order_by=("created_at", "id"),
            required_cols={"id"},
        )
        first_row = next(rows, None)
        if first_row is None:
            return

        content_by_id: dict[uuid.UUID, tuple[str | None, dict[str, Any] | None]] = {}
        _, ac_rows = self._iter_rows("artifact_content", required_cols={"id"})
        for ac_row in ac_rows:
            ac_id = _parse_uuid(ac_row.get("id"))
            if ac_id is not None:
                c_hash = _opt_str(ac_row.get("content_hash"))
                c_meta = (
                    _parse_json_obj(ac_row.get("meta")) if "meta" in ac_row else None
                )
                content_by_id[ac_id] = (c_hash, c_meta)

        artifact_facets_by_id: dict[str, dict[str, Any]] = {}
        _, af_rows = self._iter_rows("artifact_facet", required_cols={"id"})
        for af_row in af_rows:
            af_id = _opt_str(af_row.get("id"))
            if af_id:
                artifact_facets_by_id[af_id] = af_row

        kv_by_art_id: dict[uuid.UUID, dict[str, Any]] = {}
        kv_meta_by_art_id: dict[uuid.UUID, tuple[str | None, str | None]] = {}
        if "artifact_kv" in self._main_tables():
            akv_cols = set(self._columns_for("main", "artifact_kv").keys())
            if {"artifact_id", "key_path"} <= akv_cols:
                select_akv_cols = [
                    c
                    for c in (
                        "artifact_id",
                        "facet_id",
                        "namespace",
                        "key_path",
                        "value_type",
                        "value_str",
                        "value_num",
                        "value_bool",
                        "value_json",
                        "value",
                    )
                    if c in akv_cols
                ]
                cur_akv = self._con.cursor()
                try:
                    cur_akv.execute(
                        f"SELECT {', '.join('akv.' + _quote_ident(c) for c in select_akv_cols)} "
                        'FROM main."artifact_kv" akv'
                    )
                    akv_names = [str(d[0]) for d in (cur_akv.description or ())]
                    while akv_batch := cur_akv.fetchmany(4096):
                        for raw_akv in akv_batch:
                            kv_row = dict(zip(akv_names, raw_akv))
                            kv_art_id = _parse_uuid(kv_row.get("artifact_id"))
                            key_path = _opt_str(kv_row.get("key_path"))
                            if kv_art_id is None or not key_path:
                                continue
                            val = _parse_typed_kv_value(kv_row)
                            kv_by_art_id.setdefault(kv_art_id, {})[key_path] = val
                            if kv_art_id not in kv_meta_by_art_id:
                                kv_meta_by_art_id[kv_art_id] = (
                                    _opt_str(kv_row.get("facet_id")),
                                    _opt_str(kv_row.get("namespace")),
                                )
                finally:
                    cur_akv.close()

        def _all_art_rows() -> Iterator[dict[str, Any]]:
            yield first_row
            yield from rows

        for row in _all_art_rows():
            art_id = _parse_uuid(row["id"])
            if art_id is None:
                continue

            meta = _parse_json_obj(row.get("meta"))
            uri_val = (
                row.get("container_uri")
                if "container_uri" in art_cols
                else row.get("uri")
            )

            hash_alg = row.get("hash_algorithm")
            if hash_alg is None and isinstance(meta.get("hash_algorithm"), str):
                hash_alg = meta["hash_algorithm"]

            raw_sem = (
                row.get("hash_semantics")
                if row.get("hash_semantics") is not None
                else meta.get("hash_semantics")
            )
            if isinstance(raw_sem, str):
                parsed_sem = _parse_json_obj(raw_sem)
                hash_sem: dict[str, Any] | str | None = parsed_sem or raw_sem
            elif isinstance(raw_sem, dict):
                hash_sem = dict(raw_sem)
            else:
                hash_sem = None

            raw_roots = (
                row.get("recovery_roots")
                if row.get("recovery_roots") is not None
                else meta.get("recovery_roots")
            )
            recovery_roots = (
                _parse_json_list_str(raw_roots) if raw_roots is not None else None
            )

            schema_id = row.get("schema_id")
            if schema_id is None and isinstance(meta.get("schema_id"), str):
                schema_id = meta["schema_id"]

            raw_facet = (
                row.get("facet") if row.get("facet") is not None else meta.get("facet")
            )
            facet_dict: dict[str, Any] = (
                _parse_json_obj(raw_facet) if raw_facet is not None else {}
            )

            kv_facet_id, kv_ns = kv_meta_by_art_id.get(art_id, (None, None))
            facet_id = (
                _opt_str(meta.get("artifact_facet_id"))
                or _opt_str(meta.get("facet_id"))
                or kv_facet_id
            )
            facet_ns = (
                _opt_str(meta.get("facet_namespace"))
                or _opt_str(meta.get("artifact_facet_namespace"))
                or kv_ns
            )
            facet_ver = _opt_str(meta.get("facet_schema_version")) or _opt_str(
                meta.get("artifact_facet_schema_version")
            )
            if facet_id and facet_id in artifact_facets_by_id:
                af_rec = artifact_facets_by_id[facet_id]
                af_json = _parse_json_obj(
                    af_rec.get("json") if "json" in af_rec else af_rec.get("facet_json")
                )
                if af_json and not facet_dict:
                    facet_dict = dict(af_json)
                elif af_json:
                    merged_af = dict(af_json)
                    merged_af.update(facet_dict)
                    facet_dict = merged_af
                if facet_ns is None:
                    facet_ns = _opt_str(af_rec.get("namespace"))
                if facet_ver is None:
                    facet_ver = _opt_str(af_rec.get("schema_version"))

            facet_kv: dict[str, Any] = {}
            if facet_dict:
                facet_kv.update(facet_dict)
            if art_id in kv_by_art_id:
                for kp, kval in kv_by_art_id[art_id].items():
                    facet_kv[kp] = kval
                    unflatten_facet_key_path(facet_dict, kp, kval)

            facet: dict[str, Any] | None = facet_dict if facet_dict else None

            driver_str = str(row.get("driver") or "other")
            is_dir_val = bool(
                row.get("is_dir")
                or meta.get("is_dir")
                or meta.get("directory_artifact")
                or isinstance(meta.get("directory_manifest"), Mapping)
                or driver_str in ("directory", "zarr")
            )

            content_id = _parse_uuid(row.get("content_id"))
            content_hash: str | None = None
            content_meta: dict[str, Any] | None = None
            if content_id is not None and content_id in content_by_id:
                content_hash, content_meta = content_by_id[content_id]

            parent_art_id = _parse_uuid(row.get("parent_artifact_id"))
            if parent_art_id is None:
                parent_art_id = _parse_uuid(meta.get("parent_artifact_id"))

            yield V1ArtifactRecord(
                id=art_id,
                key=str(row.get("key") or ""),
                container_uri=str(uri_val or ""),
                driver=driver_str,
                hash=_opt_str(row.get("hash")),
                hash_algorithm=_opt_str(hash_alg),
                hash_semantics=hash_sem,
                table_path=_opt_str(row.get("table_path")),
                array_path=_opt_str(row.get("array_path")),
                content_id=content_id,
                parent_artifact_id=parent_art_id,
                run_id=_opt_str(row.get("run_id")),
                schema_id=_opt_str(schema_id),
                recovery_roots=recovery_roots,
                facet=facet,
                meta=meta,
                created_at=_parse_datetime(row.get("created_at")),
                content_hash=content_hash,
                content_meta=content_meta,
                is_dir=is_dir_val,
                facet_kv=facet_kv,
                facet_id=facet_id,
                facet_namespace=facet_ns,
                facet_schema_version=facet_ver,
            )

    def iter_links(self) -> Iterator[V1LinkRecord]:
        """Yield `V1LinkRecord` rows from `main.run_artifact_link`."""
        _, rows = self._iter_rows(
            "run_artifact_link",
            order_by=("run_id", "artifact_id"),
            required_cols={"run_id", "artifact_id"},
        )
        for row in rows:
            art_id = _parse_uuid(row.get("artifact_id"))
            if art_id is None:
                continue
            yield V1LinkRecord(
                run_id=str(row["run_id"]),
                artifact_id=art_id,
                direction=str(row.get("direction") or "output"),
                is_implicit=bool(row.get("is_implicit", False)),
            )

    def iter_dir_entries(self) -> Iterator[V1DirEntryRecord]:
        """Yield `V1DirEntryRecord` rows from `main.run_artifact_dir_entry` if the table exists."""
        _, rows = self._iter_rows("run_artifact_dir_entry")
        for row in rows:
            rel_path = (
                row.get("rel_path") or row.get("relative_path") or row.get("path") or ""
            )
            sha_val = (
                row.get("sha256")
                or row.get("hash")
                or row.get("file_hash")
                or row.get("entry_hash")
            )
            size_val = row.get("size") if "size" in row else row.get("size_bytes")
            mtime_val = row.get("mtime_ns") if "mtime_ns" in row else row.get("mtime")
            yield V1DirEntryRecord(
                artifact_id=_parse_uuid(row.get("artifact_id")),
                run_id=_opt_str(row.get("run_id")),
                rel_path=str(rel_path),
                kind=str(row.get("kind") or "file"),
                size=int(size_val) if size_val is not None else None,
                sha256=_opt_str(sha_val),
                mtime_ns=int(mtime_val) if mtime_val is not None else None,
                raw=row,
            )

    def read_config_facets(self) -> dict[str, V1ConfigFacetRecord]:
        """Read `main.config_facet` keyed by `id` if the table exists."""
        _, rows = self._fetch_rows(
            "config_facet",
            order_by=("created_at", "id"),
            required_cols={"id"},
        )
        out: dict[str, V1ConfigFacetRecord] = {}
        for row in rows:
            fid = _opt_str(row.get("id"))
            if not fid:
                continue
            raw_json = row.get("json") if "json" in row else row.get("facet_json")
            out[fid] = V1ConfigFacetRecord(
                id=fid,
                namespace=str(row.get("namespace") or ""),
                schema_name=str(row.get("schema_name") or ""),
                schema_version=_opt_str(row.get("schema_version")),
                facet_json=_parse_json_obj(raw_json),
                created_at=_parse_datetime(row.get("created_at")),
            )
        return out

    def read_run_config_kv(self) -> dict[str, list[V1RunConfigKvRecord]]:
        """Read `main.run_config_kv` grouped by `run_id`, preserving `int` vs `float`."""
        if "run_config_kv" not in self._main_tables():
            return {}
        cols = set(self._columns_for("main", "run_config_kv").keys())
        if not ({"run_id", "key"} <= cols):
            return {}
        select_cols = [
            c
            for c in (
                "run_id",
                "facet_id",
                "namespace",
                "key",
                "value_type",
                "value_str",
                "value_num",
                "value_bool",
                "value_json",
                "value",
                "created_at",
            )
            if c in cols
        ]
        order_cols = [c for c in ("run_id", "key") if c in cols]
        order_clause = (
            f" ORDER BY {', '.join('rkv.' + _quote_ident(c) for c in order_cols)}"
            if order_cols
            else ""
        )
        cur = self._con.execute(
            f"SELECT {', '.join('rkv.' + _quote_ident(c) for c in select_cols)} "
            f'FROM main."run_config_kv" rkv{order_clause}'
        )
        col_names = [str(d[0]) for d in (cur.description or ())]
        out: dict[str, list[V1RunConfigKvRecord]] = {}
        for raw in cur.fetchall():
            row = dict(zip(col_names, raw))
            run_id = _opt_str(row.get("run_id"))
            key = _opt_str(row.get("key"))
            if not run_id or not key:
                continue
            val = _parse_typed_kv_value(row)
            vtype = str(row.get("value_type") or "").strip().lower()
            if not vtype:
                if val is None:
                    vtype = "null"
                elif isinstance(val, bool):
                    vtype = "bool"
                elif isinstance(val, int):
                    vtype = "int"
                elif isinstance(val, float):
                    vtype = "float"
                elif isinstance(val, str):
                    vtype = "str"
                else:
                    vtype = "json"
            out.setdefault(run_id, []).append(
                V1RunConfigKvRecord(
                    run_id=run_id,
                    facet_id=_opt_str(row.get("facet_id")),
                    namespace=_opt_str(row.get("namespace")),
                    key=key,
                    value_type=vtype,
                    value=val,
                    created_at=_parse_datetime(row.get("created_at")),
                )
            )
        return out

    def iter_coupler_snapshots(self) -> Iterator[V1CouplerSnapshotRecord]:
        """Yield `V1CouplerSnapshotRecord` rows from `main.run_coupler_snapshot` if present."""
        cols, rows = self._fetch_rows(
            "run_coupler_snapshot",
            order_by=("run_id", "step_ordinal", "phase"),
            required_cols={"run_id", "phase", "artifact_id"},
        )
        if not rows:
            return
        run_parents: dict[str, str | None] = {}
        if "scenario_run_id" not in cols and "run" in self._main_tables():
            run_cols = set(self._columns_for("main", "run").keys())
            if {"id", "parent_run_id"} <= run_cols:
                for rid, pid in self._con.execute(
                    'SELECT id, parent_run_id FROM main."run"'
                ).fetchall():
                    if rid is not None:
                        run_parents[str(rid)] = _opt_str(pid)

        for row in rows:
            art_id = _parse_uuid(row.get("artifact_id"))
            run_id = _opt_str(row.get("run_id"))
            phase = _opt_str(row.get("phase"))
            role_key = _opt_str(
                row.get("role_key")
                if "role_key" in cols
                else (row.get("slot_key") if "slot_key" in cols else row.get("key"))
            )
            if art_id is None or not run_id or not phase or not role_key:
                continue
            scen_id = _opt_str(row.get("scenario_run_id"))
            if not scen_id:
                scen_id = run_parents.get(run_id) or run_id
            step_ord = (
                int(row["step_ordinal"]) if row.get("step_ordinal") is not None else 0
            )
            yield V1CouplerSnapshotRecord(
                run_id=run_id,
                phase=phase,
                role_key=role_key,
                scenario_run_id=scen_id,
                step_ordinal=step_ord,
                artifact_id=art_id,
                producer_run_id=_opt_str(row.get("producer_run_id")),
                updated_at=_parse_datetime(row.get("updated_at")),
            )

    def iter_binding_invocations(self) -> Iterator[V1BindingInvocationRecord]:
        """Yield `V1BindingInvocationRecord` rows from `main.run_binding_invocation` if present."""
        _, rows = self._fetch_rows(
            "run_binding_invocation",
            order_by=("created_at", "id"),
            required_cols={"id", "requested_run_id", "execution_run_id"},
        )
        for row in rows:
            inv_id = _parse_uuid(row.get("id"))
            req_id = _opt_str(row.get("requested_run_id"))
            exec_id = _opt_str(row.get("execution_run_id"))
            if inv_id is None or not req_id or not exec_id:
                continue
            raw_bjson = row.get("binding_json")
            bjson = (
                raw_bjson
                if isinstance(raw_bjson, str)
                else json.dumps(raw_bjson or {}, sort_keys=True, separators=(",", ":"))
            )
            yield V1BindingInvocationRecord(
                id=inv_id,
                requested_run_id=req_id,
                execution_run_id=exec_id,
                cache_source_run_id=_opt_str(row.get("cache_source_run_id")),
                cache_outcome=str(row.get("cache_outcome") or "unknown"),
                binding_json=bjson,
                created_at=_parse_datetime(row.get("created_at")),
            )

    def read_artifact_schemas(self) -> V1ArtifactSchemaBundle:
        """Read `main.artifact_schema*` (`schema`, `field`, `observation`, `relation`)."""
        schemas: list[V1ArtifactSchemaRecord] = []
        _, s_rows = self._fetch_rows(
            "artifact_schema",
            order_by=("created_at", "id"),
            required_cols={"id"},
        )
        for row in s_rows:
            sid = _opt_str(row.get("id"))
            if not sid:
                continue
            raw_prof = row.get("profile_json")
            prof_obj = _parse_json_obj(raw_prof) if raw_prof is not None else None
            schemas.append(
                V1ArtifactSchemaRecord(
                    id=sid,
                    profile_version=(
                        int(row["profile_version"])
                        if row.get("profile_version") is not None
                        else 1
                    ),
                    summary_json=_parse_json_obj(row.get("summary_json")),
                    profile_json=prof_obj if prof_obj else None,
                    created_at=_parse_datetime(row.get("created_at")),
                )
            )

        fields_out: list[V1ArtifactSchemaFieldRecord] = []
        _, f_rows = self._fetch_rows(
            "artifact_schema_field",
            order_by=("schema_id", "ordinal_position", "name"),
            required_cols={"schema_id", "name"},
        )
        for row in f_rows:
            sid = _opt_str(row.get("schema_id"))
            fname = _opt_str(row.get("name"))
            if not sid or not fname:
                continue
            raw_stats = row.get("stats_json")
            stats_obj = _parse_json_obj(raw_stats) if raw_stats is not None else None
            raw_enums = row.get("enum_values_json")
            enums_list = (
                _parse_json_list_str(raw_enums) if raw_enums is not None else None
            )
            fields_out.append(
                V1ArtifactSchemaFieldRecord(
                    schema_id=sid,
                    ordinal_position=(
                        int(row["ordinal_position"])
                        if row.get("ordinal_position") is not None
                        else None
                    ),
                    name=fname,
                    logical_type=str(row.get("logical_type") or "VARCHAR"),
                    nullable=(
                        bool(row["nullable"])
                        if row.get("nullable") is not None
                        else True
                    ),
                    stats_json=stats_obj if stats_obj else None,
                    is_enum=bool(row.get("is_enum", False)),
                    enum_values_json=enums_list if enums_list else None,
                )
            )

        observations: list[V1ArtifactSchemaObservationRecord] = []
        _, o_rows = self._fetch_rows(
            "artifact_schema_observation",
            order_by=("observed_at", "id"),
            required_cols={"id", "artifact_id", "schema_id"},
        )
        for row in o_rows:
            oid = _parse_uuid(row.get("id"))
            aid = _parse_uuid(row.get("artifact_id"))
            sid = _opt_str(row.get("schema_id"))
            if oid is None or aid is None or not sid:
                continue
            observations.append(
                V1ArtifactSchemaObservationRecord(
                    id=oid,
                    artifact_id=aid,
                    schema_id=sid,
                    run_id=_opt_str(row.get("run_id")),
                    source=str(row.get("source") or "duckdb"),
                    sample_rows=(
                        int(row["sample_rows"])
                        if row.get("sample_rows") is not None
                        else None
                    ),
                    observed_at=_parse_datetime(row.get("observed_at")),
                )
            )

        relations: list[V1ArtifactSchemaRelationRecord] = []
        _, r_rows = self._fetch_rows(
            "artifact_schema_relation",
            order_by=("schema_id", "from_field"),
            required_cols={"schema_id", "from_field", "to_table", "to_field"},
        )
        for row in r_rows:
            sid = _opt_str(row.get("schema_id"))
            ff = _opt_str(row.get("from_field"))
            tt = _opt_str(row.get("to_table"))
            tf = _opt_str(row.get("to_field"))
            if not sid or not ff or not tt or not tf:
                continue
            relations.append(
                V1ArtifactSchemaRelationRecord(
                    schema_id=sid,
                    from_field=ff,
                    to_table=tt,
                    to_field=tf,
                    relationship_type=str(
                        row.get("relationship_type") or "foreign_key"
                    ),
                    cardinality=_opt_str(row.get("cardinality")),
                )
            )

        return V1ArtifactSchemaBundle(
            schemas=tuple(schemas),
            fields=tuple(fields_out),
            observations=tuple(observations),
            relations=tuple(relations),
        )

    def read_user_schema_tables_ddl(self) -> V1UserSchemasDDL:
        """Introspect user schemas (`global_tables`, `_dlt_*`, etc.) for DDL replay.

        Queries `duckdb_sequences()`, `duckdb_tables()`, `duckdb_constraints()`, and
        `duckdb_indexes()`, ordering tables topologically by foreign-key constraints.
        """
        excluded = ("main", "information_schema", "pg_catalog")
        excl_sql = "('main', 'information_schema', 'pg_catalog')"
        cat_row = self._con.execute("SELECT current_database()").fetchone()
        catalog_name = (
            str(cat_row[0]) if cat_row is not None and cat_row[0] is not None else ""
        )

        seq_rows = self._con.execute(
            "SELECT schema_name, sequence_name, sql FROM duckdb_sequences() "
            f"WHERE schema_name NOT IN {excl_sql} AND NOT temporary "
            "ORDER BY schema_name, sequence_name"
        ).fetchall()
        sequences: list[V1SequenceDDL] = []
        schema_set: set[str] = set()
        for raw_s, raw_seq, raw_sql in seq_rows:
            s_name = str(raw_s)
            seq_name = str(raw_seq)
            sql_str = str(raw_sql or "").strip()
            if sql_str:
                q_seq_target = f"{_quote_ident(s_name)}.{_quote_ident(seq_name)}"
                sql_str = re.sub(
                    r"^(\s*CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?)"
                    r'(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*)(?![A-Za-z0-9_"]|\s*\.)',
                    lambda m: f"{m.group(1)}{q_seq_target}",
                    sql_str,
                    count=1,
                    flags=re.IGNORECASE,
                )
                schema_set.add(s_name)
                sequences.append(
                    V1SequenceDDL(
                        schema_name=s_name,
                        sequence_name=seq_name,
                        sql=sql_str,
                    )
                )

        tbl_rows = self._con.execute(
            "SELECT schema_name, table_name, sql FROM duckdb_tables() "
            f"WHERE schema_name NOT IN {excl_sql} AND NOT temporary "
            "ORDER BY schema_name, table_name"
        ).fetchall()

        fk_rows = self._con.execute(
            "SELECT schema_name, table_name, constraint_type, constraint_text "
            "FROM duckdb_constraints() "
            f"WHERE schema_name NOT IN {excl_sql} "
            "ORDER BY schema_name, table_name, constraint_index"
        ).fetchall()
        fk_map: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for raw_s, raw_t, raw_ctype, raw_ctext in fk_rows:
            if str(raw_ctype or "").upper() != "FOREIGN KEY":
                continue
            s_name = str(raw_s)
            t_name = str(raw_t)
            target = _extract_fk_target(str(raw_ctext or ""), default_schema=s_name)
            if (
                target is not None
                and target[0] not in excluded
                and target != (s_name, t_name)
            ):
                bucket = fk_map.setdefault((s_name, t_name), [])
                if target not in bucket:
                    bucket.append(target)

        raw_tables: dict[tuple[str, str], V1TableDDL] = {}
        for raw_s, raw_t, raw_sql in tbl_rows:
            s_name = str(raw_s)
            t_name = str(raw_t)
            schema_set.add(s_name)
            sql_str = str(raw_sql or "").strip()
            key = (s_name, t_name)
            raw_tables[key] = V1TableDDL(
                schema_name=s_name,
                table_name=t_name,
                sql=sql_str,
                fk_targets=tuple(fk_map.get(key, ())),
            )

        ordered_tables: list[V1TableDDL] = []
        visited: set[tuple[str, str]] = set()
        visiting: set[tuple[str, str]] = set()

        def _visit(node: tuple[str, str]) -> None:
            if node in visited or node not in raw_tables:
                return
            if node in visiting:
                return
            visiting.add(node)
            for dep in raw_tables[node].fk_targets:
                _visit(dep)
            visiting.remove(node)
            visited.add(node)
            ordered_tables.append(raw_tables[node])

        for key in sorted(raw_tables):
            _visit(key)

        idx_rows = self._con.execute(
            "SELECT schema_name, table_name, index_name, sql FROM duckdb_indexes() "
            f"WHERE schema_name NOT IN {excl_sql} "
            "ORDER BY schema_name, table_name, index_name"
        ).fetchall()
        indexes: list[V1IndexDDL] = []
        for raw_s, raw_t, raw_idx, raw_sql in idx_rows:
            sql_str = str(raw_sql or "").strip()
            if not sql_str:
                continue
            s_name = str(raw_s)
            schema_set.add(s_name)
            indexes.append(
                V1IndexDDL(
                    schema_name=s_name,
                    table_name=str(raw_t),
                    index_name=str(raw_idx),
                    sql=sql_str,
                )
            )

        return V1UserSchemasDDL(
            schemas=tuple(sorted(schema_set)),
            sequences=tuple(sequences),
            tables=tuple(ordered_tables),
            indexes=tuple(indexes),
            catalog_name=catalog_name,
        )

    def read_runs(self) -> list[V1RunRecord]:
        """Materialize all `V1RunRecord` rows from `main.run`."""
        return list(self.iter_runs())

    def read_artifacts(self) -> list[V1ArtifactRecord]:
        """Materialize all `V1ArtifactRecord` rows from `main.artifact`."""
        return list(self.iter_artifacts())

    def read_links(self) -> list[V1LinkRecord]:
        """Materialize all `V1LinkRecord` rows from `main.run_artifact_link`."""
        return list(self.iter_links())

    def read_dir_entries(self) -> list[V1DirEntryRecord]:
        """Materialize all `V1DirEntryRecord` rows from `main.run_artifact_dir_entry`."""
        return list(self.iter_dir_entries())

    def read_coupler_snapshots(self) -> list[V1CouplerSnapshotRecord]:
        """Materialize all `V1CouplerSnapshotRecord` rows from `main.run_coupler_snapshot`."""
        return list(self.iter_coupler_snapshots())

    def read_binding_invocations(self) -> list[V1BindingInvocationRecord]:
        """Materialize all `V1BindingInvocationRecord` rows from `main.run_binding_invocation`."""
        return list(self.iter_binding_invocations())


__all__ = [
    "V1ArtifactRecord",
    "V1ArtifactSchemaBundle",
    "V1ArtifactSchemaFieldRecord",
    "V1ArtifactSchemaObservationRecord",
    "V1ArtifactSchemaRecord",
    "V1ArtifactSchemaRelationRecord",
    "V1BindingInvocationRecord",
    "V1ConfigFacetRecord",
    "V1CouplerSnapshotRecord",
    "V1DatabaseReader",
    "V1DirEntryRecord",
    "V1DltTableInfo",
    "V1IndexDDL",
    "V1LinkRecord",
    "V1RunConfigKvRecord",
    "V1RunRecord",
    "V1SchemaInventory",
    "V1SequenceDDL",
    "V1TableDDL",
    "V1UserSchemasDDL",
    "unflatten_facet_key_path",
]
