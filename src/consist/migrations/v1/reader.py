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

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
import json
from pathlib import Path
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
        return val
    if val is not None and (raw := str(val).strip()):
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            pass
    return None


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

    def __enter__(self) -> V1DatabaseReader:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._con.close()

    def _main_tables(self) -> set[str]:
        return {
            str(r[0])
            for r in self._con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main'"
            ).fetchall()
        }

    def _columns_for(self, schema_name: str, table_name: str) -> dict[str, str]:
        rows = self._con.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE schema_name = ? AND table_name = ? "
            "ORDER BY column_index",
            [schema_name, table_name],
        ).fetchall()
        return {str(col_name): str(dtype) for col_name, dtype in rows}

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

    def _fetch_rows(
        self,
        table: str,
        *,
        order_by: tuple[str, ...] = (),
        required_cols: set[str] | None = None,
    ) -> tuple[set[str], list[dict[str, Any]]]:
        if table not in self._main_tables():
            return set(), []
        cols = set(self._columns_for("main", table).keys())
        if required_cols is not None and not (required_cols <= cols):
            return cols, []
        order_cols = [c for c in order_by if c in cols]
        order_clause = (
            f" ORDER BY {', '.join(_quote_ident(c) for c in order_cols)}"
            if order_cols
            else ""
        )
        cur = self._con.execute(
            f"SELECT * FROM main.{_quote_ident(table)}{order_clause}"
        )
        col_names = [str(d[0]) for d in (cur.description or ())]
        return cols, [dict(zip(col_names, r)) for r in cur.fetchall()]

    def iter_runs(self) -> Iterator[V1RunRecord]:
        """Yield `V1RunRecord` rows from `main.run`, adapting to available columns."""
        _, rows = self._fetch_rows(
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
        art_cols, rows = self._fetch_rows(
            "artifact",
            order_by=("created_at", "id"),
            required_cols={"id"},
        )
        if not rows:
            return

        content_by_id: dict[uuid.UUID, tuple[str | None, dict[str, Any] | None]] = {}
        _, ac_rows = self._fetch_rows("artifact_content", required_cols={"id"})
        for ac_row in ac_rows:
            ac_id = _parse_uuid(ac_row.get("id"))
            if ac_id is not None:
                c_hash = _opt_str(ac_row.get("content_hash"))
                c_meta = (
                    _parse_json_obj(ac_row.get("meta")) if "meta" in ac_row else None
                )
                content_by_id[ac_id] = (c_hash, c_meta)

        for row in rows:
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
                if "hash_semantics" in art_cols
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
                if "recovery_roots" in art_cols
                else meta.get("recovery_roots")
            )
            recovery_roots = (
                _parse_json_list_str(raw_roots) if raw_roots is not None else None
            )

            schema_id = row.get("schema_id")
            if schema_id is None and isinstance(meta.get("schema_id"), str):
                schema_id = meta["schema_id"]

            raw_facet = row.get("facet") if "facet" in art_cols else meta.get("facet")
            facet = _parse_json_obj(raw_facet) if raw_facet is not None else None

            content_id = _parse_uuid(row.get("content_id"))
            content_hash: str | None = None
            content_meta: dict[str, Any] | None = None
            if content_id is not None and content_id in content_by_id:
                content_hash, content_meta = content_by_id[content_id]

            yield V1ArtifactRecord(
                id=art_id,
                key=str(row.get("key") or ""),
                container_uri=str(uri_val or ""),
                driver=str(row.get("driver") or "other"),
                hash=_opt_str(row.get("hash")),
                hash_algorithm=_opt_str(hash_alg),
                hash_semantics=hash_sem,
                table_path=_opt_str(row.get("table_path")),
                array_path=_opt_str(row.get("array_path")),
                content_id=content_id,
                parent_artifact_id=_parse_uuid(row.get("parent_artifact_id")),
                run_id=_opt_str(row.get("run_id")),
                schema_id=_opt_str(schema_id),
                recovery_roots=recovery_roots,
                facet=facet,
                meta=meta,
                created_at=_parse_datetime(row.get("created_at")),
                content_hash=content_hash,
                content_meta=content_meta,
            )

    def iter_links(self) -> Iterator[V1LinkRecord]:
        """Yield `V1LinkRecord` rows from `main.run_artifact_link`."""
        _, rows = self._fetch_rows(
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
        _, rows = self._fetch_rows("run_artifact_dir_entry")
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


__all__ = [
    "V1ArtifactRecord",
    "V1DatabaseReader",
    "V1DirEntryRecord",
    "V1DltTableInfo",
    "V1LinkRecord",
    "V1RunRecord",
    "V1SchemaInventory",
]
