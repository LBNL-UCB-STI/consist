"""
`StoreLedger`: v2 DuckDB store manager with live-ledger registration,
re-entrant transaction-cursor affinity, in-memory session overlay for read-only
connections, place-superseded location queries, and domain CRUD helpers.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import threading
import time
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    overload,
    runtime_checkable,
)
import uuid

import duckdb
from pydantic import TypeAdapter

from consist.core.facet_common import flatten_facet_values
from consist.digest import Digest, Identity, Kind, SchemeRegistry
from consist.location import (
    DirTreeStat,
    FileStat,
    LedgerRef,
    Location,
    MemberOf,
    Mount,
    Observation,
    Witness,
    deserialize_witness_json,
    normalize_mounts,
    register_live_ledger,
    serialize_witness_json,
    unregister_live_ledger,
)
from consist.meta import FrozenMeta, merge_artifact_output_meta
from consist.store.detect import (
    ConsistError,
    IncompatibleSchemaError,
    SchemaState,
    detect_schema,
    inspect_v1_summary,
)
from consist.store.schema import init_v2_schema

if TYPE_CHECKING:
    import pandas as pd

    from consist.artifact import Artifact
    from consist.models.artifact_schema import (
        ArtifactSchema,
        ArtifactSchemaField,
        ArtifactSchemaRelation,
    )
    from consist.models.run import (
        Run,
        RunArtifacts,
        RunBindingInvocation,
        RunCouplerSnapshot,
    )

LocationStatus: TypeAlias = Literal[
    "pending_transfer", "active", "evicted", "failed_transfer"
]
DigestBasis: TypeAlias = Literal["computed", "implied", "declared", "trusted_foreign"]

VALID_DIGEST_BASES: frozenset[str] = frozenset(
    {"computed", "implied", "declared", "trusted_foreign"}
)
UNVERIFIED_DIGEST_BASES: frozenset[str] = frozenset({"declared", "trusted_foreign"})

LOCATION_STATUS_RANKS: dict[str, int] = {
    "pending_transfer": 0,
    "active": 1,
    "evicted": 2,
    "failed_transfer": 2,
}

_MAX_FACET_KV_KEYS: int = 500
_ModelT = TypeVar("_ModelT")


class _UnsetType:
    pass


_UNSET = _UnsetType()


@runtime_checkable
class _ArtifactIdProtocol(Protocol):
    id: uuid.UUID
    run_id: str | None


@runtime_checkable
class _RunRecordProtocol(Protocol):
    id: str
    model_name: str
    status: str
    description: str | None
    year: int | None
    iteration: int | None
    stage: str | None
    phase: str | None
    parent_run_id: str | None
    git_hash: str | None
    config_hash: str | None
    input_hash: str | None
    signature: str | None
    tags: list[str]
    meta: dict[str, Any]
    started_at: datetime | None
    ended_at: datetime | None
    created_at: datetime
    updated_at: datetime | None


@runtime_checkable
class _BindingInvocationProtocol(Protocol):
    id: uuid.UUID
    requested_run_id: str
    execution_run_id: str
    cache_source_run_id: str | None
    cache_outcome: str
    binding_json: str
    created_at: datetime


@runtime_checkable
class _CouplerSnapshotProtocol(Protocol):
    run_id: str
    phase: str
    role_key: str
    scenario_run_id: str
    step_ordinal: int
    artifact_id: uuid.UUID
    producer_run_id: str | None
    updated_at: datetime


@runtime_checkable
class _SchemaRecordProtocol(Protocol):
    id: str
    profile_version: int
    summary_json: dict[str, Any]
    profile_json: dict[str, Any] | None
    created_at: datetime


@runtime_checkable
class _SchemaFieldProtocol(Protocol):
    schema_id: str
    ordinal_position: int | None
    name: str
    logical_type: str
    nullable: bool
    stats_json: dict[str, Any] | None
    is_enum: bool
    enum_values_json: list[str] | None


@runtime_checkable
class _SchemaRelationProtocol(Protocol):
    schema_id: str
    from_field: str
    to_table: str
    to_field: str
    relationship_type: str
    cardinality: str | None


@runtime_checkable
class _SchemaObservationProtocol(Protocol):
    id: uuid.UUID
    artifact_id: uuid.UUID
    schema_id: str
    run_id: str | None
    source: str
    sample_rows: int | None
    observed_at: datetime


def resolve_status_rank(status: str, status_rank: int | None = None) -> int:
    """Resolve or validate monotonic status rank for a `location_event`."""
    if status not in LOCATION_STATUS_RANKS:
        raise ValueError(
            f"Unknown location status {status!r}; expected one of {sorted(LOCATION_STATUS_RANKS)}"
        )
    if status_rank is not None:
        return int(status_rank)
    return LOCATION_STATUS_RANKS[status]


def _coerce_digest(val: Digest | str) -> Digest:
    return val if isinstance(val, Digest) else Digest.parse(str(val))


def _coerce_uuid(val: uuid.UUID | str) -> uuid.UUID:
    return val if isinstance(val, uuid.UUID) else uuid.UUID(str(val))


def _try_parse_uuid(val: uuid.UUID | str) -> uuid.UUID | None:
    if isinstance(val, uuid.UUID):
        return val
    try:
        return uuid.UUID(str(val))
    except (ValueError, TypeError):
        return None


def _default_writer() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _load_json_if_str(raw: Any) -> Any:
    return json.loads(raw) if isinstance(raw, str) and raw else raw


def _parse_json_mapping(raw: Any) -> dict[str, Any]:
    parsed = _load_json_if_str(raw)
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _parse_json_list_str(raw: Any) -> list[str]:
    parsed = _load_json_if_str(raw)
    if isinstance(parsed, Sequence) and not isinstance(parsed, (str, bytes)):
        return [str(x) for x in parsed]
    return []


def _build_facet_kv_json(
    meta_dict: Mapping[str, Any],
    *,
    include_json_leaves: bool,
    facet_index: bool | None = None,
) -> str | None:
    if (
        facet_index is False
        or meta_dict.get("facet_index") is False
        or meta_dict.get("config_facet_index") is False
        or meta_dict.get("artifact_facet_index") is False
    ):
        return None
    raw_facet = meta_dict.get("facet")
    if not isinstance(raw_facet, Mapping) or not raw_facet:
        fallback_key = "config_facet" if include_json_leaves else "artifact_facet"
        raw_facet = meta_dict.get(fallback_key)
    if not isinstance(raw_facet, Mapping) or not raw_facet:
        return None
    flattened = flatten_facet_values(
        facet_dict=FrozenMeta(raw_facet).to_dict(),
        include_json_leaves=include_json_leaves,
    )
    if not flattened or len(flattened) > _MAX_FACET_KV_KEYS:
        return None

    kv_map: dict[str, dict[str, Any]] = {}
    for row in flattened:
        entry: dict[str, Any] = {"t": row.value_type}
        if row.value_str is not None:
            entry["s"] = row.value_str
        if row.value_num is not None:
            entry["n"] = row.value_num
        if row.value_bool is not None:
            entry["b"] = row.value_bool
        if row.value_type == "json":
            entry["j"] = row.value_json
        kv_map[row.key_path] = entry

    return json.dumps(kv_map, sort_keys=True, separators=(",", ":"))


def _validate_select_only_sql(sql: str) -> None:
    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL query must be a non-empty SELECT statement")
    stmts = duckdb.extract_statements(sql)
    if not (
        len(stmts) == 1 and stmts[0].type == duckdb.StatementType.SELECT  # ty: ignore[unresolved-attribute]
    ):
        raise ValueError(
            "query_df and query_rows only permit a single read-only SELECT statement"
        )


class StoreLedger:
    """Manages a Consist v2 `.duckdb` connection with re-entrant transaction-cursor
    affinity and an in-memory overlay when `read_only=True`.
    """

    def __init__(
        self,
        db_path: Path | str = ":memory:",
        *,
        read_only: bool = False,
        db_uid: str | None = None,
        instance_key: str | None = None,
        writer: str | None = None,
        mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    ) -> None:
        raw_path = str(db_path).strip()
        self.db_path: Path | None = (
            None
            if not raw_path or raw_path == ":memory:"
            else Path(raw_path).expanduser().resolve()
        )
        self.read_only: bool = bool(read_only)
        self.instance_key: str = instance_key or str(uuid.uuid4())
        self.writer: str = writer or _default_writer()
        self._closed: bool = False
        self._lock: threading.RLock = threading.RLock()
        self._tx_depth: int = 0
        self._tx_cursor: duckdb.DuckDBPyConnection | None = None

        # In-memory session overlay used when read_only=True
        self._overlay_observations: dict[uuid.UUID, dict[str, Any]] = {}
        self._overlay_events: list[dict[str, Any]] = []
        self._overlay_digests: dict[tuple[str, str], tuple[str, DigestBasis]] = {}
        self._overlay_digest_conflicts: list[dict[str, Any]] = []
        self._memoized_observations: dict[str, tuple[Observation, ...]] = {}
        self._memoized_locations: dict[tuple[str, str, str], Any] = {}
        self.mounts: dict[str, Mount] = normalize_mounts(
            mounts, strip_trailing_punct=True
        )

        target = ":memory:" if self.db_path is None else str(self.db_path)
        if self.db_path is not None:
            if self.read_only and not self.db_path.exists():
                raise FileNotFoundError(
                    f"Database file does not exist for read-only access: {self.db_path}"
                )
            if self.db_path.exists() and self.db_path.stat().st_size > 0:
                state = detect_schema(self.db_path)
                if state is SchemaState.V1:
                    raise inspect_v1_summary(self.db_path)
                if state in (
                    SchemaState.FUTURE,
                    SchemaState.UNKNOWN,
                    SchemaState.INTERRUPTED,
                    SchemaState.UNREADABLE,
                ):
                    raise IncompatibleSchemaError(
                        f"Cannot open database {self.db_path}: schema state is {state.value!r}"
                    )

        self._con: duckdb.DuckDBPyConnection = duckdb.connect(
            target,
            read_only=self.read_only,
        )

        if not self.read_only:
            self.db_uid: str = init_v2_schema(self._con, db_uid=db_uid)
        else:
            row = self._con.execute(
                "SELECT value FROM main.consist_meta WHERE key = 'db_uid'"
            ).fetchone()
            self.db_uid = (
                str(row[0]) if row is not None else (db_uid or str(uuid.uuid4()))
            )

        register_live_ledger(
            self,
            instance_key=self.instance_key,
            db_path=self.db_path,
        )

    @property
    def schema_state(self) -> SchemaState:
        return SchemaState.V2

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """Return a cloned `DuckDBPyConnection` cursor sharing this database handle."""
        with self._lock:
            if self._closed:
                raise ConsistError("StoreLedger is closed")
            if self._tx_depth > 0:
                raise ConsistError(
                    "Cannot allocate an independent cursor() inside an active "
                    "StoreLedger.transaction(); use ledger.exec_cursor() instead"
                )
            return self._con.cursor()

    @contextmanager
    def exec_cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield the active transaction cursor when in `transaction()`, or a temporary cursor."""
        with self._lock:
            if self._closed:
                raise ConsistError("StoreLedger is closed")
            if self._tx_depth > 0 and self._tx_cursor is not None:
                yield self._tx_cursor
                return
            cur = self._con.cursor()
            try:
                yield cur
            finally:
                cur.close()

    @contextmanager
    def transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Execute nested `StoreLedger` operations atomically on one shared cursor."""
        self._require_writable("transaction")
        with self._lock:
            if self._closed:
                raise ConsistError("StoreLedger is closed")
            if self._tx_depth > 0 and self._tx_cursor is not None:
                self._tx_depth += 1
                try:
                    yield self._tx_cursor
                finally:
                    self._tx_depth -= 1
                return

            tx_cur = self._con.cursor()
            try:
                tx_cur.execute("BEGIN TRANSACTION")
                self._tx_cursor = tx_cur
                self._tx_depth = 1
                try:
                    yield tx_cur
                    tx_cur.execute("COMMIT")
                except BaseException:
                    self._memoized_observations.clear()
                    self._memoized_locations.clear()
                    try:
                        tx_cur.execute("ROLLBACK")
                    except Exception:
                        pass
                    raise
                finally:
                    self._tx_depth = 0
                    self._tx_cursor = None
            finally:
                tx_cur.close()

    def _require_writable(self, op_name: str) -> None:
        if self.read_only:
            raise PermissionError(
                f"Cannot execute {op_name}() on a read-only StoreLedger"
            )

    @property
    def ref(self) -> LedgerRef:
        return LedgerRef(
            db_uid=self.db_uid,
            instance_key=self.instance_key,
            db_uri=str(self.db_path) if self.db_path is not None else None,
        )

    @property
    def overlay_digest_conflicts(self) -> tuple[dict[str, Any], ...]:
        """Return conflicts recorded in the read-only session overlay."""
        with self._lock:
            return tuple(self._overlay_digest_conflicts)

    def __enter__(self) -> StoreLedger:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def close(self) -> None:
        """Unregister from `_live_ledgers` and close the underlying DuckDB connection."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            unregister_live_ledger(
                self,
                instance_key=self.instance_key,
                db_path=self.db_path,
            )
            if self._tx_cursor is not None:
                try:
                    self._tx_cursor.close()
                except Exception:
                    pass
                self._tx_cursor = None
                self._tx_depth = 0
            self._con.close()

    def record_content(
        self,
        anchor: Digest | str,
        repr_kind: str,
        *,
        schema_id: str | None = None,
        basis: DigestBasis = "computed",
        created_at: datetime | None = None,
    ) -> Digest:
        """Insert a `content` hub row (and primary `content_digest` claim if verifiable)."""
        anchor_digest = _coerce_digest(anchor)
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        with self._lock:
            if not self.read_only:
                effective_repr = (
                    "historical" if anchor_digest.kind is Kind.HISTORICAL else repr_kind
                )
                with self.exec_cursor() as cur:
                    cur.execute(
                        "INSERT INTO main.content (anchor, repr_kind, schema_id, created_at) "
                        "VALUES (?, ?, ?, ?) ON CONFLICT (anchor) DO NOTHING",
                        [str(anchor_digest), effective_repr, schema_id, now],
                    )
            if anchor_digest.kind is not Kind.HISTORICAL:
                self.record_content_digest(
                    anchor_digest,
                    anchor_digest,
                    basis=basis,
                    source=self.writer,
                    computed_at=now,
                )
        return anchor_digest

    def record_content_digest(
        self,
        anchor: Digest | str,
        digest: Digest | str,
        *,
        basis: DigestBasis = "computed",
        repr_kind: str | None = None,
        source: str | None = None,
        computed_at: datetime | None = None,
    ) -> bool:
        """Record a `content_digest` claim or audit a conflict in `content_digest_conflict`.

        A verified `basis='computed'` digest supersedes any conflicting prior
        `'declared'` or `'trusted_foreign'` claim, logging the superseded claim in
        `content_digest_conflict`. When `read_only=True`, stores claims in the
        in-memory session overlay without mutating disk.
        """
        anchor_digest = _coerce_digest(anchor)
        claim_digest = _coerce_digest(digest)
        if (
            anchor_digest.kind is Kind.HISTORICAL
            or claim_digest.kind is Kind.HISTORICAL
        ):
            raise ValueError(
                "Kind.HISTORICAL digests cannot be stored in content_digest"
            )
        if basis not in VALID_DIGEST_BASES:
            raise ValueError(
                f"content_digest basis must be one of {sorted(VALID_DIGEST_BASES)}, got {basis!r}"
            )
        now = computed_at if computed_at is not None else datetime.now(timezone.utc)
        src = source or self.writer
        anchor_str = str(anchor_digest)
        scheme_str = SchemeRegistry.get(claim_digest.scheme).name
        val_str = claim_digest.hex

        with self._lock:
            if self.read_only:
                with self.exec_cursor() as cur:
                    db_row = cur.execute(
                        "SELECT value, basis FROM main.content_digest WHERE anchor = ? AND scheme = ?",
                        [anchor_str, scheme_str],
                    ).fetchone()
                ov_entry = self._overlay_digests.get((anchor_str, scheme_str))
                if ov_entry is not None:
                    kept_val, kept_basis = ov_entry
                elif db_row is not None:
                    kept_val, kept_basis = str(db_row[0]), str(db_row[1])
                else:
                    self._overlay_digests[(anchor_str, scheme_str)] = (val_str, basis)
                    return True

                if kept_val == val_str:
                    if basis == "computed" and kept_basis != "computed":
                        self._overlay_digests[(anchor_str, scheme_str)] = (
                            val_str,
                            "computed",
                        )
                    return True

                if basis == "computed" and kept_basis in UNVERIFIED_DIGEST_BASES:
                    self._overlay_digest_conflicts.append(
                        {
                            "anchor": anchor_str,
                            "scheme": scheme_str,
                            "kept_value": val_str,
                            "rejected_value": kept_val,
                            "source": src,
                            "seen_at": now,
                        }
                    )
                    self._overlay_digests[(anchor_str, scheme_str)] = (
                        val_str,
                        "computed",
                    )
                    return True

                self._overlay_digest_conflicts.append(
                    {
                        "anchor": anchor_str,
                        "scheme": scheme_str,
                        "kept_value": kept_val,
                        "rejected_value": val_str,
                        "source": src,
                        "seen_at": now,
                    }
                )
                return False

            if repr_kind is not None:
                self.record_content(
                    anchor_digest,
                    repr_kind,
                    basis=basis if anchor_digest == claim_digest else "computed",
                    created_at=now,
                )

            with self.exec_cursor() as cur:
                existing_row = cur.execute(
                    "SELECT value, basis FROM main.content_digest WHERE anchor = ? AND scheme = ?",
                    [anchor_str, scheme_str],
                ).fetchone()
                if existing_row is None:
                    cur.execute(
                        "INSERT INTO main.content_digest (anchor, scheme, value, basis, computed_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        [anchor_str, scheme_str, val_str, basis, now],
                    )
                    return True

                kept_value = str(existing_row[0])
                kept_basis_str = str(existing_row[1])
                if kept_value == val_str:
                    if basis == "computed" and kept_basis_str != "computed":
                        cur.execute(
                            "UPDATE main.content_digest SET basis = 'computed', computed_at = ? "
                            "WHERE anchor = ? AND scheme = ?",
                            [now, anchor_str, scheme_str],
                        )
                    return True

                if basis == "computed" and kept_basis_str in UNVERIFIED_DIGEST_BASES:
                    cur.execute(
                        "INSERT INTO main.content_digest_conflict "
                        "(anchor, scheme, kept_value, rejected_value, source, seen_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        [anchor_str, scheme_str, val_str, kept_value, src, now],
                    )
                    cur.execute(
                        "UPDATE main.content_digest "
                        "SET value = ?, basis = 'computed', computed_at = ? "
                        "WHERE anchor = ? AND scheme = ?",
                        [val_str, now, anchor_str, scheme_str],
                    )
                    return True

                cur.execute(
                    "INSERT INTO main.content_digest_conflict "
                    "(anchor, scheme, kept_value, rejected_value, source, seen_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    [anchor_str, scheme_str, kept_value, val_str, src, now],
                )
                return False

    def get_identity(self, anchor: Digest | str) -> Identity:
        """Reconstruct `Identity(anchor, claims)` from `content_digest` plus session overlay."""
        anchor_digest = _coerce_digest(anchor)
        if anchor_digest.kind is Kind.HISTORICAL:
            return Identity(anchor=anchor_digest)

        anchor_str = str(anchor_digest)
        with self._lock:
            with self.exec_cursor() as cur:
                rows = cur.execute(
                    "SELECT scheme, value FROM main.content_digest WHERE anchor = ?",
                    [anchor_str],
                ).fetchall()
            claims_by_scheme: dict[str, Digest] = {
                anchor_digest.scheme: anchor_digest,
            }
            for scheme, value in rows:
                d = Digest(scheme=str(scheme), hex=str(value))
                claims_by_scheme.setdefault(d.scheme, d)
            for (ov_anchor, ov_scheme), (
                ov_val,
                ov_basis,
            ) in self._overlay_digests.items():
                if ov_anchor == anchor_str:
                    d = Digest(scheme=ov_scheme, hex=ov_val)
                    if ov_basis == "computed" or d.scheme not in claims_by_scheme:
                        claims_by_scheme[d.scheme] = d
        return Identity(anchor=anchor_digest, claims=tuple(claims_by_scheme.values()))

    def record_identity(
        self,
        identity: Identity,
        repr_kind: str = "file",
        *,
        schema_id: str | None = None,
        basis: DigestBasis = "computed",
        anchor_basis: DigestBasis | None = None,
        source: str | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Record `identity.anchor` in `content` and all verifiable `identity.claims` in `content_digest`."""
        if basis not in VALID_DIGEST_BASES:
            raise ValueError(
                f"record_identity basis must be one of {sorted(VALID_DIGEST_BASES)}, got {basis!r}"
            )
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        effective_anchor_basis: DigestBasis = (
            anchor_basis
            if anchor_basis is not None
            else ("computed" if basis in ("computed", "implied") else basis)
        )
        with self._lock:
            self.record_content(
                identity.anchor,
                repr_kind,
                schema_id=schema_id,
                basis=effective_anchor_basis,
                created_at=now,
            )
            if identity.verifiable:
                src = source or self.writer
                for claim in identity.claims:
                    claim_basis: DigestBasis = (
                        effective_anchor_basis if claim == identity.anchor else basis
                    )
                    self.record_content_digest(
                        identity.anchor,
                        claim,
                        basis=claim_basis,
                        source=src,
                        computed_at=now,
                    )

    def record_content_manifest(
        self,
        anchor: Digest | str,
        *,
        format: str,
        entry_count: int,
        total_bytes: int,
        body: bytes,
        policy_sha256: str | None = None,
    ) -> None:
        """Insert a canonical structural manifest into `content_manifest`."""
        self._require_writable("record_content_manifest")
        anchor_digest = _coerce_digest(anchor)
        with self.exec_cursor() as cur:
            cur.execute(
                "INSERT INTO main.content_manifest "
                "(anchor, format, policy_sha256, entry_count, total_bytes, body) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (anchor) DO NOTHING",
                [
                    str(anchor_digest),
                    format,
                    policy_sha256,
                    int(entry_count),
                    int(total_bytes),
                    bytes(body),
                ],
            )

    def record_anchor_promotion(
        self,
        historical_anchor: Digest | str,
        verified_anchor: Digest | str,
        *,
        basis: Literal["v1_fast_recomputed", "caller_supplied_match", "asserted"],
        asserted_by: str | None = None,
        evidence: Mapping[str, Any] | None = None,
        promoted_at: datetime | None = None,
    ) -> bool:
        """Insert a promotion from a `Kind.HISTORICAL` tombstone to a verified anchor."""
        self._require_writable("record_anchor_promotion")
        hist = _coerce_digest(historical_anchor)
        ver = _coerce_digest(verified_anchor)
        if hist.kind is not Kind.HISTORICAL:
            raise ValueError(f"historical_anchor must be Kind.HISTORICAL, got {hist}")
        if ver.kind is Kind.HISTORICAL:
            raise ValueError(f"verified_anchor cannot be Kind.HISTORICAL, got {ver}")

        now = promoted_at if promoted_at is not None else datetime.now(timezone.utc)
        ev_json = (
            json.dumps(dict(evidence), sort_keys=True, separators=(",", ":"))
            if evidence is not None
            else None
        )
        with self.exec_cursor() as cur:
            existing = cur.execute(
                "SELECT verified_anchor FROM main.anchor_promotion WHERE historical_anchor = ?",
                [str(hist)],
            ).fetchone()
            if existing is not None:
                return str(existing[0]) == str(ver)

            cur.execute(
                "INSERT INTO main.anchor_promotion "
                "(historical_anchor, verified_anchor, basis, asserted_by, evidence, promoted_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [str(hist), str(ver), basis, asserted_by, ev_json, now],
            )
            return True

    record_promotion = record_anchor_promotion

    def get_promotion(
        self,
        historical_anchor: Digest | str,
    ) -> tuple[Digest, str, dict[str, Any] | None] | None:
        """Return `(verified_anchor, basis, evidence_dict)` for `historical_anchor` if promoted."""
        hist = _coerce_digest(historical_anchor)
        with self.exec_cursor() as cur:
            row = cur.execute(
                "SELECT verified_anchor, basis, evidence FROM main.anchor_promotion "
                "WHERE historical_anchor = ?",
                [str(hist)],
            ).fetchone()
        if row is None:
            return None
        ver_str, basis_raw, ev_raw = row
        ev_dict = _parse_json_mapping(ev_raw) if ev_raw is not None else None
        return (Digest.parse(str(ver_str)), str(basis_raw), ev_dict)

    def resolve_effective_identity(
        self,
        recorded_anchor: Digest | str,
    ) -> tuple[Identity, Digest | None, Digest | None]:
        """Resolve `(effective_identity, recorded_anchor_when_promoted, asserted_or_verified_anchor)`."""
        rec_digest = _coerce_digest(recorded_anchor)
        if rec_digest.kind is not Kind.HISTORICAL:
            return (self.get_identity(rec_digest), None, None)

        promo = self.get_promotion(rec_digest)
        if promo is None:
            return (Identity(anchor=rec_digest), None, None)

        verified_anchor, basis, _evidence = promo
        if basis in ("v1_fast_recomputed", "caller_supplied_match"):
            return (self.get_identity(verified_anchor), rec_digest, verified_anchor)
        if basis == "asserted":
            return (Identity(anchor=rec_digest), None, verified_anchor)
        return (Identity(anchor=rec_digest), None, None)

    @staticmethod
    def _observation_from_location_row(
        row: Mapping[str, Any],
        identity: Identity,
    ) -> Observation:
        return Observation(
            location=Location(
                uri=str(row["uri"]),
                member=str(row["member"]) if row.get("member") is not None else None,
            ),
            identity=identity,
            access_driver=str(row["access_driver"]),
            witness=row.get("witness"),
            basis=row["basis"],
            site=str(row["site"]),
            scope_key=(
                str(row["scope_key"]) if row.get("scope_key") is not None else None
            ),
            mount_instance_id=(
                str(row["mount_instance_id"])
                if row.get("mount_instance_id") is not None
                else None
            ),
            observed_at_ns=int(row["observed_at_ns"]),
        )

    def _hydrate_artifact_rows(
        self,
        rows: Sequence[tuple[Any, ...]],
        *,
        include_locations: bool = True,
    ) -> list[Artifact]:
        from consist.artifact import Artifact

        if not rows:
            return []

        with self._lock:
            raw_digests = [_coerce_digest(str(r[3])) for r in rows]
            hist_strs = sorted(
                {str(d) for d in raw_digests if d.kind is Kind.HISTORICAL}
            )
            promo_by_hist: dict[str, tuple[Digest, str]] = {}
            if hist_strs:
                with self.exec_cursor() as cur:
                    promo_rows = cur.execute(
                        "SELECT historical_anchor, verified_anchor, basis "
                        "FROM main.anchor_promotion "
                        "WHERE historical_anchor IN (SELECT UNNEST(?))",
                        [hist_strs],
                    ).fetchall()
                promo_by_hist = {
                    str(h): (Digest.parse(str(v)), str(b)) for h, v, b in promo_rows
                }

            resolved_anchors: list[tuple[Digest, Digest | None, Digest | None]] = []
            needed_identity_digests: list[Digest] = []
            uncached_loc_digests: list[Digest] = []
            seen_uncached: set[str] = set()

            for raw_d in raw_digests:
                if raw_d.kind is not Kind.HISTORICAL:
                    triple: tuple[Digest, Digest | None, Digest | None] = (
                        raw_d,
                        None,
                        None,
                    )
                else:
                    promo = promo_by_hist.get(str(raw_d))
                    if promo is None:
                        triple = (raw_d, None, None)
                    elif promo[1] in ("v1_fast_recomputed", "caller_supplied_match"):
                        triple = (promo[0], raw_d, promo[0])
                    elif promo[1] == "asserted":
                        triple = (raw_d, None, promo[0])
                    else:
                        triple = (raw_d, None, None)

                resolved_anchors.append(triple)
                eff_anchor, rec_opt, ver_opt = triple
                needed_identity_digests.append(eff_anchor)

                if include_locations:
                    cand_anchors = (
                        (ver_opt, eff_anchor, rec_opt)
                        if eff_anchor.kind is Kind.HISTORICAL
                        else (eff_anchor, ver_opt, rec_opt)
                    )
                    for cand in cand_anchors:
                        if cand is None:
                            continue
                        needed_identity_digests.append(cand)
                        cand_str = str(cand)
                        if (
                            not self._closed
                            and cand_str not in self._memoized_observations
                            and cand_str not in seen_uncached
                        ):
                            seen_uncached.add(cand_str)
                            uncached_loc_digests.append(cand)

            identities: dict[str, Identity] = {
                str(d): Identity(anchor=d)
                for d in needed_identity_digests
                if d.kind is Kind.HISTORICAL
            }
            verifiable_by_str = {
                str(d): d
                for d in needed_identity_digests
                if d.kind is not Kind.HISTORICAL
            }
            if verifiable_by_str:
                ver_strs = sorted(verifiable_by_str)
                with self.exec_cursor() as cur:
                    cd_rows = cur.execute(
                        "SELECT anchor, scheme, value FROM main.content_digest "
                        "WHERE anchor IN (SELECT UNNEST(?))",
                        [ver_strs],
                    ).fetchall()
                claims_by_anchor: dict[str, dict[str, Digest]] = {
                    a_str: {d.scheme: d} for a_str, d in verifiable_by_str.items()
                }
                for a_raw, scheme_raw, val_raw in cd_rows:
                    d = Digest(scheme=str(scheme_raw), hex=str(val_raw))
                    claims_by_anchor[str(a_raw)].setdefault(d.scheme, d)
                for (ov_anchor, ov_scheme), (
                    ov_val,
                    ov_basis,
                ) in self._overlay_digests.items():
                    if ov_anchor in claims_by_anchor:
                        d = Digest(scheme=ov_scheme, hex=ov_val)
                        if (
                            ov_basis == "computed"
                            or d.scheme not in claims_by_anchor[ov_anchor]
                        ):
                            claims_by_anchor[ov_anchor][d.scheme] = d
                for a_str, d in verifiable_by_str.items():
                    identities[a_str] = Identity(
                        anchor=d,
                        claims=tuple(claims_by_anchor[a_str].values()),
                    )

            if include_locations and uncached_loc_digests and not self._closed:
                loc_rows = self.locate_current(
                    uncached_loc_digests,
                    include_evicted=False,
                )
                reconstructed_by_anchor: dict[str, list[Observation]] = {
                    str(d): [] for d in uncached_loc_digests
                }
                for lrow in loc_rows:
                    l_anchor_str = str(lrow["anchor"])
                    bucket = reconstructed_by_anchor.get(l_anchor_str)
                    if bucket is None or len(bucket) >= 4:
                        continue
                    row_id = identities.get(l_anchor_str) or self.get_identity(
                        l_anchor_str
                    )
                    bucket.append(self._observation_from_location_row(lrow, row_id))
                for a_str, bucket in reconstructed_by_anchor.items():
                    self._memoized_observations[a_str] = tuple(bucket)

            origin_ref = self.ref
            hydrated: list[Artifact] = []
            for row, (eff_anchor, recorded_anchor_opt, verified_anchor_opt) in zip(
                rows, resolved_anchors, strict=True
            ):
                (
                    raw_id,
                    key,
                    driver,
                    _raw_anchor,
                    run_id,
                    raw_parent_id,
                    schema_id,
                    raw_meta,
                    created_at,
                ) = row
                art_uuid = _coerce_uuid(raw_id)
                parent_uuid = (
                    _coerce_uuid(raw_parent_id) if raw_parent_id is not None else None
                )
                effective_identity = identities[str(eff_anchor)]
                meta_dict = _parse_json_mapping(raw_meta)

                hint_obs: list[Observation] = []
                if include_locations:
                    cand_anchors = (
                        (verified_anchor_opt, eff_anchor, recorded_anchor_opt)
                        if eff_anchor.kind is Kind.HISTORICAL
                        else (eff_anchor, verified_anchor_opt, recorded_anchor_opt)
                    )
                    for cand_anchor in cand_anchors:
                        if cand_anchor is None or len(hint_obs) >= 4:
                            continue
                        for obs in self._memoized_observations.get(
                            str(cand_anchor), ()
                        )[: 4 - len(hint_obs)]:
                            if obs not in hint_obs:
                                hint_obs.append(obs)

                hydrated.append(
                    Artifact(
                        id=art_uuid,
                        key=str(key),
                        driver=str(driver),
                        identity=effective_identity,
                        recorded_anchor=recorded_anchor_opt,
                        origin=origin_ref,
                        run_id=str(run_id) if run_id is not None else None,
                        parent_artifact_id=parent_uuid,
                        schema_id=str(schema_id) if schema_id is not None else None,
                        created_at=(
                            created_at if isinstance(created_at, datetime) else None
                        ),
                        meta=FrozenMeta(meta_dict),
                        hints=tuple(hint_obs[:4]),
                    )
                )
            return hydrated

    def _hydrate_artifact_row(
        self,
        row: tuple[Any, ...],
        *,
        include_locations: bool = True,
    ) -> Artifact:
        return self._hydrate_artifact_rows(
            [row],
            include_locations=include_locations,
        )[0]

    def load_artifact(
        self,
        artifact_id: uuid.UUID | str,
        *,
        include_locations: bool = True,
    ) -> Artifact | None:
        """Load a frozen `Artifact` handle by `id`, resolving any `anchor_promotion`."""
        art_uuid = _coerce_uuid(artifact_id)
        with self.exec_cursor() as cur:
            row = cur.execute(
                "SELECT id, key, driver, anchor, run_id, parent_artifact_id, "
                "schema_id, meta, created_at FROM main.artifact WHERE id = ?",
                [art_uuid],
            ).fetchone()
        if row is None:
            return None
        return self._hydrate_artifact_row(row, include_locations=include_locations)

    def get_artifact(
        self,
        identifier: uuid.UUID | str,
        *,
        run_id: str | None = None,
        include_locations: bool = True,
    ) -> Artifact | None:
        """Look up a frozen `Artifact` by UUID, `(run_id, role/key)`, anchor string, or `key`."""
        parsed_uuid = _try_parse_uuid(identifier)
        if parsed_uuid is not None:
            found = self.load_artifact(parsed_uuid, include_locations=include_locations)
            if found is not None:
                return found

        ident_str = str(identifier).strip()
        if not ident_str:
            return None

        with self.exec_cursor() as cur:
            if run_id is not None:
                bind_row = cur.execute(
                    "SELECT artifact_id FROM main.run_binding "
                    "WHERE run_id = ? AND (role = ? OR slot = ?) "
                    "ORDER BY direction = 'output' DESC, occurrence_index DESC LIMIT 1",
                    [run_id, ident_str, ident_str],
                ).fetchone()
                if bind_row is not None:
                    return self.load_artifact(
                        bind_row[0], include_locations=include_locations
                    )

                art_row = cur.execute(
                    "SELECT id FROM main.artifact WHERE run_id = ? AND key = ? "
                    "ORDER BY created_at DESC LIMIT 1",
                    [run_id, ident_str],
                ).fetchone()
                if art_row is not None:
                    return self.load_artifact(
                        art_row[0], include_locations=include_locations
                    )
                return None

            if ":" in ident_str:
                anchor_row = cur.execute(
                    "SELECT id FROM main.artifact WHERE anchor = ? "
                    "ORDER BY created_at DESC LIMIT 1",
                    [ident_str],
                ).fetchone()
                if anchor_row is not None:
                    return self.load_artifact(
                        anchor_row[0], include_locations=include_locations
                    )

            key_row = cur.execute(
                "SELECT id FROM main.artifact WHERE key = ? "
                "ORDER BY created_at DESC LIMIT 1",
                [ident_str],
            ).fetchone()
        if key_row is not None:
            return self.load_artifact(key_row[0], include_locations=include_locations)
        return None

    def get_artifacts_for_run(self, run_id: str) -> RunArtifacts:
        """Return `RunArtifacts(inputs=..., outputs=...)` for `run_id`."""
        from consist.models.run import RunArtifacts

        with self.exec_cursor() as cur:
            bind_rows = cur.execute(
                "SELECT direction, slot, role, artifact_id FROM main.run_binding "
                "WHERE run_id = ? ORDER BY occurrence_index ASC, ordinal ASC",
                [run_id],
            ).fetchall()
            art_rows = cur.execute(
                "SELECT id, key, driver, anchor, run_id, parent_artifact_id, "
                "schema_id, meta, created_at FROM main.artifact "
                "WHERE run_id = ? OR id IN ("
                "SELECT artifact_id FROM main.run_binding WHERE run_id = ?"
                ") ORDER BY created_at ASC",
                [run_id, run_id],
            ).fetchall()

        hydrated = self._hydrate_artifact_rows(art_rows)
        art_by_id = {art.id: art for art in hydrated}

        inputs_map: dict[str, Any] = {}
        outputs_map: dict[str, Any] = {}
        bound_ids: set[uuid.UUID] = set()

        for direction, slot, role, raw_art_id in bind_rows:
            art = art_by_id.get(_coerce_uuid(raw_art_id))
            if art is None:
                continue
            bound_ids.add(art.id)
            bind_key = str(role or slot or art.key)
            if str(direction) in ("input", "depends_on"):
                inputs_map[bind_key] = art
            elif str(direction) == "output":
                outputs_map[bind_key] = art

        for prod_art in hydrated:
            if (
                prod_art.run_id == run_id
                and prod_art.id not in bound_ids
                and prod_art.key not in outputs_map
            ):
                outputs_map[prod_art.key] = prod_art

        return RunArtifacts.model_construct(inputs=inputs_map, outputs=outputs_map)

    def get_child_artifacts(self, parent_id: uuid.UUID | str) -> list[Artifact]:
        """Return child `Artifact` handles with `parent_artifact_id == parent_id`."""
        parent_uuid = _coerce_uuid(parent_id)
        with self.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT id, key, driver, anchor, run_id, parent_artifact_id, "
                "schema_id, meta, created_at FROM main.artifact "
                "WHERE parent_artifact_id = ? ORDER BY created_at ASC, id ASC",
                [parent_uuid],
            ).fetchall()
        return self._hydrate_artifact_rows(rows)

    @overload
    def update_artifact_meta(
        self,
        artifact_id: uuid.UUID | str,
        meta: FrozenMeta | Mapping[str, Any],
        *,
        merge: bool = False,
        schema_id: str | None | _UnsetType = _UNSET,
        facet_index: bool | None = None,
    ) -> Artifact: ...

    @overload
    def update_artifact_meta(
        self,
        artifact_id: Artifact,
        meta: FrozenMeta | Mapping[str, Any] | None = None,
        *,
        merge: bool = True,
        schema_id: str | None | _UnsetType = _UNSET,
        facet_index: bool | None = None,
        **changes: Any,
    ) -> Artifact: ...

    def update_artifact_meta(
        self,
        artifact_id: Artifact | uuid.UUID | str,
        meta: FrozenMeta | Mapping[str, Any] | None = None,
        *,
        merge: bool | None = None,
        schema_id: str | None | _UnsetType = _UNSET,
        facet_index: bool | None = None,
        **changes: Any,
    ) -> Artifact:
        """Persist updated `FrozenMeta` (and optional `schema_id` + `facet_kv`) for an artifact."""
        from consist.artifact import Artifact

        self._require_writable("update_artifact_meta")
        with self._lock:
            if isinstance(artifact_id, Artifact):
                current_art: Artifact | None = artifact_id
                effective_merge = True if merge is None else bool(merge)
            else:
                art_uuid = _coerce_uuid(artifact_id)
                current_art = self.load_artifact(art_uuid)
                if current_art is None:
                    raise KeyError(f"Artifact not found in StoreLedger: {art_uuid}")
                effective_merge = False if merge is None else bool(merge)

            incoming: dict[str, Any] = {}
            if meta is not None:
                incoming.update(meta)
            if changes:
                incoming.update(changes)

            merged_dict = merge_artifact_output_meta(
                current_art.meta,
                incoming,
                merge=effective_merge,
            )
            updated = current_art.with_meta(merged_dict, merge=False)
            if not isinstance(schema_id, _UnsetType):
                updated = updated.with_schema(schema_id)

            meta_payload = updated.meta.to_dict()
            meta_json = json.dumps(meta_payload, sort_keys=True, separators=(",", ":"))
            facet_kv_json = _build_facet_kv_json(
                meta_payload,
                include_json_leaves=False,
                facet_index=facet_index,
            )
            with self.exec_cursor() as cur:
                if isinstance(schema_id, _UnsetType):
                    cur.execute(
                        "UPDATE main.artifact SET meta = ?, facet_kv = ? WHERE id = ?",
                        [meta_json, facet_kv_json, updated.id],
                    )
                else:
                    cur.execute(
                        "UPDATE main.artifact SET meta = ?, facet_kv = ?, schema_id = ? WHERE id = ?",
                        [meta_json, facet_kv_json, schema_id, updated.id],
                    )
            return updated

    def record_artifact(
        self,
        *,
        artifact_id: uuid.UUID,
        key: str,
        driver: str,
        anchor: Digest | str,
        run_id: str | None = None,
        parent_artifact_id: uuid.UUID | str | None = None,
        schema_id: str | None = None,
        meta: Mapping[str, Any] | None = None,
        facet_index: bool | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert an `artifact` handle row with JSON-validated `FrozenMeta` and `facet_kv`."""
        self._require_writable("record_artifact")
        anchor_digest = _coerce_digest(anchor)
        parent_uuid = (
            _coerce_uuid(parent_artifact_id) if parent_artifact_id is not None else None
        )
        meta_payload = FrozenMeta(meta or {}).to_dict()
        if parent_uuid is None and isinstance(
            meta_payload.get("parent_artifact_id"), str
        ):
            parent_uuid = _try_parse_uuid(meta_payload["parent_artifact_id"])
        meta_json = json.dumps(meta_payload, sort_keys=True, separators=(",", ":"))
        facet_kv_json = _build_facet_kv_json(
            meta_payload,
            include_json_leaves=False,
            facet_index=facet_index,
        )
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        with self.exec_cursor() as cur:
            cur.execute(
                "INSERT INTO main.artifact ("
                "id, key, driver, anchor, run_id, parent_artifact_id, "
                "schema_id, meta, facet_kv, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (id) DO UPDATE SET "
                "run_id = COALESCE(artifact.run_id, EXCLUDED.run_id), "
                "meta = EXCLUDED.meta, "
                "facet_kv = EXCLUDED.facet_kv, "
                "parent_artifact_id = COALESCE(EXCLUDED.parent_artifact_id, artifact.parent_artifact_id), "
                "schema_id = COALESCE(EXCLUDED.schema_id, artifact.schema_id)",
                [
                    artifact_id,
                    key,
                    driver,
                    str(anchor_digest),
                    run_id,
                    parent_uuid,
                    schema_id,
                    meta_json,
                    facet_kv_json,
                    now,
                ],
            )

    def record_run(
        self,
        *,
        run_id: str,
        model_name: str,
        status: str = "completed",
        cache_status: Literal["live", "historical", "ineligible"] = "live",
        description: str | None = None,
        year: int | None = None,
        iteration: int | None = None,
        stage: str | None = None,
        phase: str | None = None,
        parent_run_id: str | None = None,
        code_hash: str | None = None,
        git_hash: str | None = None,
        config_hash: str | None = None,
        input_hash: str | None = None,
        signature: str | None = None,
        signature_scheme: str | None = None,
        tags: Sequence[str] | None = None,
        meta: Mapping[str, Any] | None = None,
        facet_index: bool | None = None,
        started_at: datetime | None = None,
        ended_at: datetime | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> None:
        """Insert or update a `run` row, automatically indexing `facet_kv` when `<= 500` keys."""
        self._require_writable("record_run")
        if cache_status not in ("live", "historical", "ineligible"):
            raise ValueError(f"Invalid cache_status {cache_status!r}")
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        eff_updated = updated_at if updated_at is not None else (ended_at or now)
        tags_json = (
            json.dumps(list(tags), separators=(",", ":")) if tags is not None else None
        )
        meta_payload = FrozenMeta(meta or {}).to_dict()
        eff_stage = (
            stage
            if stage is not None
            else (
                meta_payload["stage"]
                if isinstance(meta_payload.get("stage"), str)
                else None
            )
        )
        eff_phase = (
            phase
            if phase is not None
            else (
                meta_payload["phase"]
                if isinstance(meta_payload.get("phase"), str)
                else None
            )
        )
        eff_code_hash = code_hash if code_hash is not None else git_hash
        eff_git_hash = git_hash if git_hash is not None else code_hash
        meta_json = json.dumps(meta_payload, sort_keys=True, separators=(",", ":"))
        facet_kv_json = _build_facet_kv_json(
            meta_payload,
            include_json_leaves=True,
            facet_index=facet_index,
        )
        with self.exec_cursor() as cur:
            cur.execute(
                "INSERT INTO main.run ("
                "id, model_name, description, year, iteration, stage, phase, "
                "parent_run_id, status, code_hash, git_hash, config_hash, input_hash, "
                "signature, signature_scheme, cache_status, tags, meta, facet_kv, "
                "started_at, ended_at, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (id) DO UPDATE SET "
                "description = COALESCE(EXCLUDED.description, run.description), "
                "year = COALESCE(EXCLUDED.year, run.year), "
                "iteration = COALESCE(EXCLUDED.iteration, run.iteration), "
                "stage = COALESCE(EXCLUDED.stage, run.stage), "
                "phase = COALESCE(EXCLUDED.phase, run.phase), "
                "parent_run_id = COALESCE(EXCLUDED.parent_run_id, run.parent_run_id), "
                "status = EXCLUDED.status, "
                "code_hash = COALESCE(EXCLUDED.code_hash, run.code_hash), "
                "git_hash = COALESCE(EXCLUDED.git_hash, run.git_hash), "
                "config_hash = COALESCE(EXCLUDED.config_hash, run.config_hash), "
                "input_hash = COALESCE(EXCLUDED.input_hash, run.input_hash), "
                "signature = COALESCE(EXCLUDED.signature, run.signature), "
                "signature_scheme = COALESCE(EXCLUDED.signature_scheme, run.signature_scheme), "
                "cache_status = EXCLUDED.cache_status, "
                "tags = COALESCE(EXCLUDED.tags, run.tags), "
                "meta = EXCLUDED.meta, "
                "facet_kv = EXCLUDED.facet_kv, "
                "started_at = COALESCE(EXCLUDED.started_at, run.started_at), "
                "ended_at = COALESCE(EXCLUDED.ended_at, run.ended_at), "
                "updated_at = COALESCE(EXCLUDED.updated_at, run.updated_at)",
                [
                    run_id,
                    model_name,
                    description,
                    year,
                    iteration,
                    eff_stage,
                    eff_phase,
                    parent_run_id,
                    status,
                    eff_code_hash,
                    eff_git_hash,
                    config_hash,
                    input_hash,
                    signature,
                    signature_scheme,
                    cache_status,
                    tags_json,
                    meta_json,
                    facet_kv_json,
                    started_at,
                    ended_at,
                    now,
                    eff_updated,
                ],
            )

    def upsert_run(
        self,
        run: Any,
        *,
        cache_status: Literal["live", "historical", "ineligible"] | None = None,
        signature_scheme: str | None = None,
        facet_index: bool | None = None,
    ) -> None:
        """Persist a `Run` domain model into `main.run`."""
        self._require_writable("upsert_run")
        if not isinstance(run, _RunRecordProtocol):
            raise TypeError(f"Expected Run model instance, got {type(run).__name__}")

        meta_map = dict(run.meta) if isinstance(run.meta, Mapping) else {}
        resolved_cache_status: Literal["live", "historical", "ineligible"]
        if cache_status is not None:
            resolved_cache_status = cache_status
        elif meta_map.get("cache_status") in ("live", "historical", "ineligible"):
            resolved_cache_status = meta_map["cache_status"]
        else:
            resolved_cache_status = "live"

        resolved_sig_scheme = (
            signature_scheme
            if signature_scheme is not None
            else (
                str(meta_map["signature_scheme"])
                if meta_map.get("signature_scheme") is not None
                else ("sig-v2" if run.signature is not None else None)
            )
        )
        code_hash_meta = (
            str(meta_map["code_hash"])
            if isinstance(meta_map.get("code_hash"), str)
            else run.git_hash
        )

        self.record_run(
            run_id=run.id,
            model_name=run.model_name,
            status=run.status,
            cache_status=resolved_cache_status,
            description=run.description,
            year=run.year,
            iteration=run.iteration,
            stage=run.stage,
            phase=run.phase,
            parent_run_id=run.parent_run_id,
            code_hash=code_hash_meta,
            git_hash=run.git_hash,
            config_hash=run.config_hash,
            input_hash=run.input_hash,
            signature=run.signature,
            signature_scheme=resolved_sig_scheme,
            tags=run.tags,
            meta=meta_map,
            facet_index=facet_index,
            started_at=run.started_at,
            ended_at=run.ended_at,
            created_at=run.created_at,
            updated_at=run.updated_at,
        )

    @staticmethod
    def _hydrate_run_row(row: tuple[Any, ...]) -> Run:
        from consist.models.run import Run

        (
            run_id,
            model_name,
            description,
            year,
            iteration,
            stage,
            phase,
            parent_run_id,
            status,
            code_hash,
            git_hash,
            config_hash,
            input_hash,
            signature,
            _signature_scheme,
            _cache_status,
            raw_tags,
            raw_meta,
            started_at,
            ended_at,
            created_at,
            updated_at,
        ) = row
        meta_dict = _parse_json_mapping(raw_meta)
        tags_list = _parse_json_list_str(raw_tags)
        eff_created = (
            created_at
            if isinstance(created_at, datetime)
            else datetime.now(timezone.utc)
        )
        eff_started = started_at if isinstance(started_at, datetime) else eff_created
        eff_updated = (
            updated_at
            if isinstance(updated_at, datetime)
            else (ended_at if isinstance(ended_at, datetime) else eff_created)
        )
        return Run(
            id=str(run_id),
            parent_run_id=str(parent_run_id) if parent_run_id is not None else None,
            status=str(status),
            model_name=str(model_name),
            description=str(description) if description is not None else None,
            year=int(year) if year is not None else None,
            iteration=int(iteration) if iteration is not None else None,
            stage=str(stage) if stage is not None else None,
            phase=str(phase) if phase is not None else None,
            tags=tags_list,
            config_hash=str(config_hash) if config_hash is not None else None,
            git_hash=(
                str(git_hash)
                if git_hash is not None
                else (str(code_hash) if code_hash is not None else None)
            ),
            input_hash=str(input_hash) if input_hash is not None else None,
            signature=str(signature) if signature is not None else None,
            meta=meta_dict,
            started_at=eff_started,
            ended_at=ended_at if isinstance(ended_at, datetime) else None,
            created_at=eff_created,
            updated_at=eff_updated,
        )

    _RUN_SELECT_COLUMNS: str = (
        "id, model_name, description, year, iteration, stage, phase, parent_run_id, "
        "status, code_hash, git_hash, config_hash, input_hash, signature, "
        "signature_scheme, cache_status, tags, meta, started_at, ended_at, "
        "created_at, updated_at"
    )

    def get_run(self, run_id: str) -> Run | None:
        """Fetch a `Run` by `run_id`, or return `None` if absent."""
        with self.exec_cursor() as cur:
            row = cur.execute(
                f"SELECT {self._RUN_SELECT_COLUMNS} FROM main.run WHERE id = ?",
                [run_id],
            ).fetchone()
        if row is None:
            return None
        return self._hydrate_run_row(row)

    def find_runs(
        self,
        *,
        model: str | None = None,
        status: str | None = None,
        year: int | None = None,
        iteration: int | None = None,
        stage: str | None = None,
        phase: str | None = None,
        parent_run_id: str | None = None,
        signature: str | None = None,
        cache_status: str | None = None,
        tags: Sequence[str] | None = None,
        facet: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        limit: int | None = 100,
        offset: int = 0,
    ) -> list[Run]:
        """Query `main.run` with optional lifecycle, context, tag, and facet filters."""
        clauses: list[str] = []
        params: list[Any] = []
        if model is not None:
            clauses.append("r.model_name = ?")
            params.append(model)
        if status is not None:
            clauses.append("r.status = ?")
            params.append(status)
        if year is not None:
            clauses.append("r.year = ?")
            params.append(int(year))
        if iteration is not None:
            clauses.append("r.iteration = ?")
            params.append(int(iteration))
        if stage is not None:
            clauses.append("r.stage = ?")
            params.append(stage)
        if phase is not None:
            clauses.append("r.phase = ?")
            params.append(phase)
        if parent_run_id is not None:
            clauses.append("r.parent_run_id = ?")
            params.append(parent_run_id)
        if signature is not None:
            clauses.append("r.signature = ?")
            params.append(signature)
        if cache_status is not None:
            clauses.append("r.cache_status = ?")
            params.append(cache_status)

        if facet:

            def _add_facet_clause(key_path: str, val: Any) -> None:
                if isinstance(val, bool):
                    clauses.append(
                        "EXISTS (SELECT 1 FROM main.run_config_kv kv "
                        "WHERE kv.run_id = r.id AND kv.key = ? AND kv.value_bool = ?)"
                    )
                    params.extend([key_path, val])
                elif isinstance(val, (int, float)):
                    clauses.append(
                        "EXISTS (SELECT 1 FROM main.run_config_kv kv "
                        "WHERE kv.run_id = r.id AND kv.key = ? AND kv.value_num = ?)"
                    )
                    params.extend([key_path, float(val)])
                elif val is None:
                    clauses.append(
                        "EXISTS (SELECT 1 FROM main.run_config_kv kv "
                        "WHERE kv.run_id = r.id AND kv.key = ? AND kv.value_type = 'null')"
                    )
                    params.append(key_path)
                elif isinstance(val, str):
                    clauses.append(
                        "EXISTS (SELECT 1 FROM main.run_config_kv kv "
                        "WHERE kv.run_id = r.id AND kv.key = ? AND kv.value_str = ?)"
                    )
                    params.extend([key_path, val])
                else:
                    unfrozen = FrozenMeta({"v": val}).to_dict()["v"]
                    clauses.append(
                        "EXISTS (SELECT 1 FROM main.run_config_kv kv "
                        "WHERE kv.run_id = r.id AND kv.key = ? "
                        "AND kv.value_type = 'json' AND kv.value_json = CAST(? AS JSON))"
                    )
                    params.extend(
                        [
                            key_path,
                            json.dumps(unfrozen, sort_keys=True, separators=(",", ":")),
                        ]
                    )

            for f_key, f_val in facet.items():
                key_str = str(f_key)
                if isinstance(f_val, Mapping):
                    for sr in flatten_facet_values(
                        facet_dict=FrozenMeta(f_val).to_dict(),
                        include_json_leaves=True,
                    ):
                        full_key = (
                            f"{key_str}.{sr.key_path}" if key_str else sr.key_path
                        )
                        if sr.value_type == "null":
                            _add_facet_clause(full_key, None)
                        elif sr.value_type == "bool":
                            _add_facet_clause(full_key, sr.value_bool)
                        elif sr.value_type in ("int", "float"):
                            _add_facet_clause(full_key, sr.value_num)
                        elif sr.value_type == "str":
                            _add_facet_clause(full_key, sr.value_str)
                        else:
                            _add_facet_clause(full_key, sr.value_json)
                else:
                    _add_facet_clause(key_str, f_val)

        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            f"SELECT {self._RUN_SELECT_COLUMNS} FROM main.run r"
            f"{where_sql} ORDER BY r.created_at DESC, r.id DESC"
        )
        start_idx = max(0, int(offset))
        push_pagination = not tags and not metadata
        if push_pagination:
            if limit is not None:
                sql += " LIMIT ? OFFSET ?"
                params.extend([max(0, int(limit)), start_idx])
            elif start_idx > 0:
                sql += " OFFSET ?"
                params.append(start_idx)

        with self.exec_cursor() as cur:
            rows = cur.execute(sql, params).fetchall()

        runs = [self._hydrate_run_row(row) for row in rows]
        if push_pagination:
            return runs
        if tags:
            req_tags = set(tags)
            runs = [rn for rn in runs if req_tags.issubset(rn.tags)]
        if metadata:
            runs = [
                rn
                for rn in runs
                if all(rn.meta.get(mk) == mv for mk, mv in metadata.items())
            ]
        if limit is None:
            return runs[start_idx:]
        return runs[start_idx : start_idx + max(0, int(limit))]

    def find_cached_run(
        self,
        signature: str,
        *,
        model_name: str | None = None,
        signature_scheme: str | None = None,
    ) -> Run | None:
        """Return the most recent completed cache-eligible (`cache_status='live'`) run matching `signature`."""
        clauses = [
            "signature = ?",
            "cache_status = 'live'",
            "status = 'completed'",
        ]
        params: list[Any] = [signature]
        if model_name is not None:
            clauses.append("model_name = ?")
            params.append(model_name)
        if signature_scheme is not None:
            clauses.append("signature_scheme = ?")
            params.append(signature_scheme)

        sql = (
            f"SELECT {self._RUN_SELECT_COLUMNS} FROM main.run "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY ended_at DESC NULLS LAST, created_at DESC LIMIT 1"
        )
        with self.exec_cursor() as cur:
            row = cur.execute(sql, params).fetchone()
        if row is None:
            return None
        return self._hydrate_run_row(row)

    def record_run_binding(
        self,
        *,
        run_id: str,
        direction: Literal["input", "output", "depends_on"],
        occurrence_index: int,
        slot: str,
        artifact_id: uuid.UUID,
        anchor: Digest | str,
        ordinal: int = 0,
        role: str | None = None,
        contract_scheme: str | None = None,
        sig_scheme: str | None = None,
        sig_hex: str | None = None,
    ) -> None:
        """Insert a canonical input/output/dependency binding row into `run_binding`."""
        self._require_writable("record_run_binding")
        if direction not in ("input", "output", "depends_on"):
            raise ValueError(
                f"Invalid binding direction {direction!r}; expected 'input', 'output', or 'depends_on'"
            )
        anchor_digest = _coerce_digest(anchor)
        with self.exec_cursor() as cur:
            cur.execute(
                "INSERT INTO main.run_binding ("
                "run_id, direction, occurrence_index, slot, ordinal, "
                "artifact_id, role, anchor, contract_scheme, sig_scheme, sig_hex"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (run_id, direction, slot, ordinal) DO NOTHING",
                [
                    run_id,
                    direction,
                    int(occurrence_index),
                    slot,
                    int(ordinal),
                    artifact_id,
                    role,
                    str(anchor_digest),
                    contract_scheme,
                    sig_scheme,
                    sig_hex,
                ],
            )

    def record_binding_invocation(
        self,
        invocation: Any = None,
        *,
        invocation_id: uuid.UUID | None = None,
        requested_run_id: str | None = None,
        execution_run_id: str | None = None,
        cache_source_run_id: str | None = None,
        cache_outcome: str | None = None,
        binding_json: str | None = None,
        created_at: datetime | None = None,
    ) -> uuid.UUID:
        """Record a `RunBindingInvocation` evidence row in `main.run_binding_invocation`."""
        self._require_writable("record_binding_invocation")
        if isinstance(invocation, _BindingInvocationProtocol):
            inv_id = _coerce_uuid(invocation.id)
            req_id = invocation.requested_run_id
            exec_id = invocation.execution_run_id
            src_id = invocation.cache_source_run_id
            outcome = invocation.cache_outcome
            b_json = invocation.binding_json
            ts = invocation.created_at
        else:
            if (
                requested_run_id is None
                or execution_run_id is None
                or cache_outcome is None
                or binding_json is None
            ):
                raise ValueError(
                    "record_binding_invocation requires either a RunBindingInvocation "
                    "instance or requested_run_id, execution_run_id, cache_outcome, and binding_json"
                )
            inv_id = invocation_id if invocation_id is not None else uuid.uuid4()
            req_id = requested_run_id
            exec_id = execution_run_id
            src_id = cache_source_run_id
            outcome = cache_outcome
            b_json = binding_json
            ts = created_at if created_at is not None else datetime.now(timezone.utc)

        with self.exec_cursor() as cur:
            cur.execute(
                "INSERT INTO main.run_binding_invocation ("
                "id, requested_run_id, execution_run_id, cache_source_run_id, "
                "cache_outcome, binding_json, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (id) DO NOTHING",
                [inv_id, req_id, exec_id, src_id, outcome, b_json, ts],
            )
        return inv_id

    def list_binding_invocations(
        self,
        *,
        requested_run_id: str | None = None,
        execution_run_id: str | None = None,
        cache_source_run_id: str | None = None,
        limit: int | None = None,
    ) -> list[RunBindingInvocation]:
        """List `RunBindingInvocation` rows ordered chronologically."""
        from consist.models.run import RunBindingInvocation

        clauses: list[str] = []
        params: list[Any] = []
        if requested_run_id is not None:
            clauses.append("requested_run_id = ?")
            params.append(requested_run_id)
        if execution_run_id is not None:
            clauses.append("execution_run_id = ?")
            params.append(execution_run_id)
        if cache_source_run_id is not None:
            clauses.append("cache_source_run_id = ?")
            params.append(cache_source_run_id)

        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        limit_sql = f" LIMIT {max(0, int(limit))}" if limit is not None else ""
        with self.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT id, requested_run_id, execution_run_id, cache_source_run_id, "
                "cache_outcome, binding_json, created_at "
                f"FROM main.run_binding_invocation{where_sql} "
                f"ORDER BY created_at ASC, id ASC{limit_sql}",
                params,
            ).fetchall()

        return [
            RunBindingInvocation(
                id=_coerce_uuid(r[0]),
                requested_run_id=str(r[1]),
                execution_run_id=str(r[2]),
                cache_source_run_id=str(r[3]) if r[3] is not None else None,
                cache_outcome=str(r[4]),
                binding_json=str(r[5]),
                created_at=(
                    r[6] if isinstance(r[6], datetime) else datetime.now(timezone.utc)
                ),
            )
            for r in rows
        ]

    def record_coupler_snapshots(
        self,
        *,
        scenario_run_id: str | None = None,
        run_id: str | None = None,
        phase: str | None = None,
        step_ordinal: int = 0,
        artifacts: Mapping[str, Any] | None = None,
        snapshots: Sequence[Any] | None = None,
        updated_at: datetime | None = None,
    ) -> None:
        """Replace coupler snapshot rows for `(run_id, phase)` in `main.run_coupler_snapshot`."""
        self._require_writable("record_coupler_snapshots")
        now = updated_at if updated_at is not None else datetime.now(timezone.utc)

        with self.transaction() as cur:
            if snapshots is not None:
                seen_pairs: set[tuple[str, str]] = set()
                for snap in snapshots:
                    if not isinstance(snap, _CouplerSnapshotProtocol):
                        raise TypeError(
                            f"Expected RunCouplerSnapshot, got {type(snap).__name__}"
                        )
                    pair = (snap.run_id, snap.phase)
                    if pair not in seen_pairs:
                        cur.execute(
                            "DELETE FROM main.run_coupler_snapshot WHERE run_id = ? AND phase = ?",
                            [snap.run_id, snap.phase],
                        )
                        seen_pairs.add(pair)
                    cur.execute(
                        "INSERT INTO main.run_coupler_snapshot ("
                        "run_id, phase, role_key, scenario_run_id, step_ordinal, "
                        "artifact_id, producer_run_id, updated_at"
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            snap.run_id,
                            snap.phase,
                            snap.role_key,
                            snap.scenario_run_id,
                            int(snap.step_ordinal),
                            _coerce_uuid(snap.artifact_id),
                            snap.producer_run_id,
                            snap.updated_at,
                        ],
                    )
                return

            if run_id is None or phase is None or scenario_run_id is None:
                raise ValueError(
                    "record_coupler_snapshots requires scenario_run_id, run_id, and phase "
                    "when snapshots is not provided"
                )
            cur.execute(
                "DELETE FROM main.run_coupler_snapshot WHERE run_id = ? AND phase = ?",
                [run_id, phase],
            )
            if not artifacts:
                return
            for role_key, item in artifacts.items():
                if isinstance(item, _ArtifactIdProtocol):
                    art_uuid = _coerce_uuid(item.id)
                    prod_run_id = item.run_id
                else:
                    art_uuid = _coerce_uuid(item)
                    prod_run_id = None
                cur.execute(
                    "INSERT INTO main.run_coupler_snapshot ("
                    "run_id, phase, role_key, scenario_run_id, step_ordinal, "
                    "artifact_id, producer_run_id, updated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        run_id,
                        phase,
                        str(role_key),
                        scenario_run_id,
                        int(step_ordinal),
                        art_uuid,
                        prod_run_id,
                        now,
                    ],
                )

    def get_coupler_snapshots(
        self,
        run_id: str | None = None,
        *,
        scenario_run_id: str | None = None,
        phase: str | None = "post",
        must_exist: bool = False,
    ) -> list[RunCouplerSnapshot]:
        """Query `RunCouplerSnapshot` rows for `run_id` / `scenario_run_id` and `phase`."""
        from consist.models.run import RunCouplerSnapshot

        clauses: list[str] = []
        params: list[Any] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if scenario_run_id is not None:
            clauses.append("scenario_run_id = ?")
            params.append(scenario_run_id)
        if phase is not None:
            clauses.append("phase = ?")
            params.append(phase)

        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT run_id, phase, role_key, scenario_run_id, step_ordinal, "
                "artifact_id, producer_run_id, updated_at "
                f"FROM main.run_coupler_snapshot{where_sql} "
                "ORDER BY step_ordinal ASC, role_key ASC",
                params,
            ).fetchall()

        if not rows and must_exist:
            raise KeyError(
                f"No coupler snapshot found for run_id={run_id!r}, phase={phase!r}"
            )

        return [
            RunCouplerSnapshot(
                run_id=str(r[0]),
                phase=str(r[1]),
                role_key=str(r[2]),
                scenario_run_id=str(r[3]),
                step_ordinal=int(r[4]) if r[4] is not None else 0,
                artifact_id=_coerce_uuid(r[5]),
                producer_run_id=str(r[6]) if r[6] is not None else None,
                updated_at=(
                    r[7] if isinstance(r[7], datetime) else datetime.now(timezone.utc)
                ),
            )
            for r in rows
        ]

    def upsert_artifact_schema(
        self,
        schema: Any,
        fields: Sequence[Any] = (),
        relations: Sequence[Any] | None = None,
        *,
        observation: Any | None = None,
    ) -> str:
        """Upsert `artifact_schema`, normalized `artifact_schema_field` rows, relations, and optional observation."""
        self._require_writable("upsert_artifact_schema")
        if not isinstance(schema, _SchemaRecordProtocol):
            raise TypeError(
                f"Expected ArtifactSchema instance, got {type(schema).__name__}"
            )
        summary_str = json.dumps(
            dict(schema.summary_json or {}),
            sort_keys=True,
            separators=(",", ":"),
        )
        profile_str = (
            json.dumps(
                dict(schema.profile_json),
                sort_keys=True,
                separators=(",", ":"),
            )
            if schema.profile_json is not None
            else None
        )
        created_ts = (
            schema.created_at
            if isinstance(schema.created_at, datetime)
            else datetime.now(timezone.utc)
        )

        with self.transaction() as cur:
            cur.execute(
                "INSERT INTO main.artifact_schema "
                "(id, profile_version, summary_json, profile_json, created_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT (id) DO UPDATE SET "
                "profile_version = EXCLUDED.profile_version, "
                "summary_json = EXCLUDED.summary_json, "
                "profile_json = COALESCE(EXCLUDED.profile_json, artifact_schema.profile_json)",
                [
                    schema.id,
                    int(schema.profile_version),
                    summary_str,
                    profile_str,
                    created_ts,
                ],
            )

            cur.execute(
                "DELETE FROM main.artifact_schema_field WHERE schema_id = ?",
                [schema.id],
            )
            for fld in fields:
                if not isinstance(fld, _SchemaFieldProtocol):
                    raise TypeError(
                        f"Expected ArtifactSchemaField instance, got {type(fld).__name__}"
                    )
                stats_str = (
                    json.dumps(
                        dict(fld.stats_json), sort_keys=True, separators=(",", ":")
                    )
                    if fld.stats_json is not None
                    else None
                )
                enums_str = (
                    json.dumps(list(fld.enum_values_json), separators=(",", ":"))
                    if fld.enum_values_json is not None
                    else None
                )
                cur.execute(
                    "INSERT INTO main.artifact_schema_field ("
                    "schema_id, ordinal_position, name, logical_type, nullable, "
                    "stats_json, is_enum, enum_values_json"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        schema.id,
                        int(fld.ordinal_position)
                        if fld.ordinal_position is not None
                        else None,
                        fld.name,
                        fld.logical_type,
                        bool(fld.nullable),
                        stats_str,
                        bool(fld.is_enum),
                        enums_str,
                    ],
                )

            if relations is not None:
                cur.execute(
                    "DELETE FROM main.artifact_schema_relation WHERE schema_id = ?",
                    [schema.id],
                )
                for rel in relations:
                    if not isinstance(rel, _SchemaRelationProtocol):
                        raise TypeError(
                            f"Expected ArtifactSchemaRelation instance, got {type(rel).__name__}"
                        )
                    cur.execute(
                        "INSERT INTO main.artifact_schema_relation ("
                        "schema_id, from_field, to_table, to_field, relationship_type, cardinality"
                        ") VALUES (?, ?, ?, ?, ?, ?)",
                        [
                            schema.id,
                            rel.from_field,
                            rel.to_table,
                            rel.to_field,
                            rel.relationship_type,
                            rel.cardinality,
                        ],
                    )

            if observation is not None:
                if not isinstance(observation, _SchemaObservationProtocol):
                    raise TypeError(
                        f"Expected ArtifactSchemaObservation instance, got {type(observation).__name__}"
                    )
                obs_ts = (
                    observation.observed_at
                    if isinstance(observation.observed_at, datetime)
                    else datetime.now(timezone.utc)
                )
                cur.execute(
                    "INSERT INTO main.artifact_schema_observation ("
                    "id, artifact_id, schema_id, run_id, source, sample_rows, observed_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (id) DO NOTHING",
                    [
                        _coerce_uuid(observation.id),
                        _coerce_uuid(observation.artifact_id),
                        observation.schema_id,
                        observation.run_id,
                        observation.source,
                        int(observation.sample_rows)
                        if observation.sample_rows is not None
                        else None,
                        obs_ts,
                    ],
                )
        return schema.id

    @overload
    def get_artifact_schema(
        self,
        schema_id: str,
        *,
        include_relations: Literal[False] = False,
    ) -> tuple[ArtifactSchema, list[ArtifactSchemaField]] | None: ...

    @overload
    def get_artifact_schema(
        self,
        schema_id: str,
        *,
        include_relations: Literal[True],
    ) -> (
        tuple[
            ArtifactSchema,
            list[ArtifactSchemaField],
            list[ArtifactSchemaRelation],
        ]
        | None
    ): ...

    def get_artifact_schema(
        self,
        schema_id: str,
        *,
        include_relations: bool = False,
    ) -> (
        tuple[ArtifactSchema, list[ArtifactSchemaField]]
        | tuple[
            ArtifactSchema,
            list[ArtifactSchemaField],
            list[ArtifactSchemaRelation],
        ]
        | None
    ):
        """Fetch `(ArtifactSchema, list[ArtifactSchemaField])` (and optional relations) by `schema_id`."""
        from consist.models.artifact_schema import (
            ArtifactSchema,
            ArtifactSchemaField,
            ArtifactSchemaRelation,
        )

        with self.exec_cursor() as cur:
            s_row = cur.execute(
                "SELECT id, profile_version, summary_json, profile_json, created_at "
                "FROM main.artifact_schema WHERE id = ?",
                [schema_id],
            ).fetchone()
            if s_row is None:
                return None

            f_rows = cur.execute(
                "SELECT schema_id, ordinal_position, name, logical_type, nullable, "
                "stats_json, is_enum, enum_values_json "
                "FROM main.artifact_schema_field WHERE schema_id = ? "
                "ORDER BY ordinal_position IS NULL ASC, ordinal_position ASC, name ASC",
                [schema_id],
            ).fetchall()
            r_rows = (
                cur.execute(
                    "SELECT schema_id, from_field, to_table, to_field, "
                    "relationship_type, cardinality "
                    "FROM main.artifact_schema_relation WHERE schema_id = ? "
                    "ORDER BY from_field ASC",
                    [schema_id],
                ).fetchall()
                if include_relations
                else []
            )

        schema_obj = ArtifactSchema(
            id=str(s_row[0]),
            profile_version=int(s_row[1]),
            summary_json=_parse_json_mapping(s_row[2]),
            profile_json=(
                _parse_json_mapping(s_row[3]) if s_row[3] is not None else None
            ),
            created_at=(
                s_row[4]
                if isinstance(s_row[4], datetime)
                else datetime.now(timezone.utc)
            ),
        )
        field_objs = [
            ArtifactSchemaField(
                schema_id=str(fr[0]),
                ordinal_position=int(fr[1]) if fr[1] is not None else None,
                name=str(fr[2]),
                logical_type=str(fr[3]),
                nullable=bool(fr[4]),
                stats_json=(_parse_json_mapping(fr[5]) if fr[5] is not None else None),
                is_enum=bool(fr[6]),
                enum_values_json=(
                    _parse_json_list_str(fr[7]) if fr[7] is not None else None
                ),
            )
            for fr in f_rows
        ]
        if not include_relations:
            return (schema_obj, field_objs)

        rel_objs = [
            ArtifactSchemaRelation(
                schema_id=str(rr[0]),
                from_field=str(rr[1]),
                to_table=str(rr[2]),
                to_field=str(rr[3]),
                relationship_type=str(rr[4]),
                cardinality=str(rr[5]) if rr[5] is not None else None,
            )
            for rr in r_rows
        ]
        return (schema_obj, field_objs, rel_objs)

    def list_artifact_schemas(
        self,
        *,
        artifact_id: uuid.UUID | str | None = None,
        run_id: str | None = None,
        source: str | None = None,
        limit: int | None = None,
    ) -> list[ArtifactSchema]:
        """List stored `ArtifactSchema` rows, optionally filtered by observation metadata."""
        from consist.models.artifact_schema import ArtifactSchema

        obs_clauses: list[str] = ["o.schema_id = s.id"]
        params: list[Any] = []
        if artifact_id is not None:
            obs_clauses.append("o.artifact_id = ?")
            params.append(_coerce_uuid(artifact_id))
        if run_id is not None:
            obs_clauses.append("o.run_id = ?")
            params.append(run_id)
        if source is not None:
            obs_clauses.append("o.source = ?")
            params.append(source)

        where_sql = (
            f" WHERE EXISTS (SELECT 1 FROM main.artifact_schema_observation o "
            f"WHERE {' AND '.join(obs_clauses)})"
            if len(obs_clauses) > 1
            else ""
        )
        limit_sql = f" LIMIT {max(0, int(limit))}" if limit is not None else ""
        with self.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT s.id, s.profile_version, s.summary_json, s.profile_json, s.created_at "
                f"FROM main.artifact_schema s{where_sql} "
                f"ORDER BY s.created_at DESC, s.id ASC{limit_sql}",
                params,
            ).fetchall()

        return [
            ArtifactSchema(
                id=str(r[0]),
                profile_version=int(r[1]),
                summary_json=_parse_json_mapping(r[2]),
                profile_json=_parse_json_mapping(r[3]) if r[3] is not None else None,
                created_at=(
                    r[4] if isinstance(r[4], datetime) else datetime.now(timezone.utc)
                ),
            )
            for r in rows
        ]

    def record_observation(
        self,
        observation: Observation,
        *,
        observation_id: uuid.UUID | None = None,
        repr_kind: str | None = "file",
        schema_id: str | None = None,
        writer: str | None = None,
    ) -> uuid.UUID:
        """Record an `Observation` into `artifact_location` (or in-memory overlay if `read_only=True`)."""
        obs_id = observation_id if observation_id is not None else uuid.uuid4()
        effective_writer = writer or self.writer

        with self._lock:
            if repr_kind is not None:
                obs_basis_map: dict[str, DigestBasis] = {
                    "computed": "computed",
                    "derived": "computed",
                    "implied": "implied",
                    "declared": "declared",
                    "migrated_v1": "declared",
                    "trusted_foreign": "trusted_foreign",
                }
                claim_basis = obs_basis_map.get(observation.basis, "computed")
                self.record_identity(
                    observation.identity,
                    repr_kind=repr_kind,
                    schema_id=schema_id,
                    basis=claim_basis,
                    source=effective_writer,
                )

            witness_json = serialize_witness_json(observation.witness)
            self._invalidate_memo_for_observation(observation)
            if self.read_only:
                self._overlay_observations.setdefault(
                    obs_id,
                    {
                        "observation_id": obs_id,
                        "anchor": str(observation.identity.anchor),
                        "uri": observation.location.uri,
                        "member": observation.location.member,
                        "access_driver": observation.access_driver,
                        "witness_json": witness_json,
                        "witness": observation.witness,
                        "basis": observation.basis,
                        "site": observation.site,
                        "scope_key": observation.scope_key,
                        "mount_instance_id": observation.mount_instance_id,
                        "observed_at_ns": observation.observed_at_ns,
                        "writer": effective_writer,
                    },
                )
                return obs_id

            with self.exec_cursor() as cur:
                cur.execute(
                    "INSERT INTO main.artifact_location ("
                    "observation_id, anchor, uri, member, access_driver, witness_json, "
                    "basis, site, scope_key, mount_instance_id, observed_at_ns, writer"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (observation_id) DO NOTHING",
                    [
                        obs_id,
                        str(observation.identity.anchor),
                        observation.location.uri,
                        observation.location.member,
                        observation.access_driver,
                        witness_json,
                        observation.basis,
                        observation.site,
                        observation.scope_key,
                        observation.mount_instance_id,
                        observation.observed_at_ns,
                        effective_writer,
                    ],
                )
            return obs_id

    def _invalidate_memo_for_observation(self, observation: Observation) -> None:
        place = observation.place_key
        obs_site = observation.site
        obs_uri = observation.location.uri
        obs_is_container = observation.location.member is None
        anchor_str = str(observation.identity.anchor)

        self._memoized_observations.pop(anchor_str, None)
        for a_key, obs_tuple in list(self._memoized_observations.items()):
            if any(
                existing.place_key == place
                or (
                    obs_is_container
                    and existing.site == obs_site
                    and existing.location.uri == obs_uri
                )
                for existing in obs_tuple
            ):
                self._memoized_observations.pop(a_key, None)

        if obs_is_container:
            self._memoized_locations.clear()
        else:
            for loc_key in list(self._memoized_locations.keys()):
                if loc_key[0] == anchor_str or loc_key[2] == obs_site:
                    self._memoized_locations.pop(loc_key, None)

    def memoized_candidates(
        self,
        anchor: Digest | str,
        *,
        limit: int = 4,
    ) -> tuple[Observation, ...]:
        """Return up to `limit` current `Observation` candidates for `anchor`."""
        max_items = max(0, int(limit))
        if max_items == 0:
            return ()
        anchor_digest = _coerce_digest(anchor)
        anchor_str = str(anchor_digest)

        with self._lock:
            cached = self._memoized_observations.get(anchor_str)
            if cached is not None and max_items <= 4:
                return cached[:max_items]
            if self._closed:
                return cached[:max_items] if cached is not None else ()

            rows = self.locate_current([anchor_digest], include_evicted=False)
            reconstructed: list[Observation] = []
            identity = self.get_identity(anchor_digest)
            fetch_cap = max(4, max_items)
            for row in rows:
                reconstructed.append(self._observation_from_location_row(row, identity))
                if len(reconstructed) >= fetch_cap:
                    break

            self._memoized_observations[anchor_str] = tuple(reconstructed[:4])
            return tuple(reconstructed[:max_items])

    def memoize_location(
        self,
        anchor: Digest | str,
        resolved: Any,
        *,
        member: str | None = None,
        site: str = "local",
    ) -> None:
        """Memoize a verified `ResolvedLocation` for `(anchor, member, site)`."""
        with self._lock:
            self._memoized_locations[(str(anchor), member or "", site)] = resolved

    def get_memoized_location(
        self,
        anchor: Digest | str,
        *,
        member: str | None = None,
        site: str = "local",
    ) -> Any | None:
        """Return a memoized `ResolvedLocation` for `(anchor, member, site)`, if any."""
        with self._lock:
            return self._memoized_locations.get((str(anchor), member or "", site))

    def record_archive_copy(
        self,
        *,
        src_uri: str,
        dst_uri: str,
        site: str,
        dst_path: Path | None = None,
        scope_key: str | None = None,
        mount_instance_id: str | None = None,
        observed_at_ns: int | None = None,
        writer: str | None = None,
        exclude_anchor_member: tuple[str, str | None] | None = None,
    ) -> list[uuid.UUID]:
        """Carry along active parent and container-child observations from `src_uri` to `dst_uri`."""
        ts_ns = time.time_ns() if observed_at_ns is None else int(observed_at_ns)
        effective_writer = writer or self.writer

        with self._lock:
            current_rows = [
                row
                for row in self.query_location_current(
                    site=site,
                    uri=src_uri,
                    include_evicted=False,
                )
                if str(row.get("status", "active")) == "active"
            ]
            if not current_rows:
                return []

            parent_stat: FileStat | None = None
            if dst_path is not None and Path(dst_path).is_file():
                parent_stat = FileStat.capture(dst_path, observed_at_ns=ts_ns)
            else:
                for row in current_rows:
                    w = row.get("witness")
                    if isinstance(w, FileStat):
                        parent_stat = w
                        break
                    if isinstance(w, MemberOf):
                        parent_stat = w.parent
                        break

            src_scheme = src_uri.split("://", 1)[0] if "://" in src_uri else ""
            dst_scheme = dst_uri.split("://", 1)[0] if "://" in dst_uri else ""
            same_scheme = bool(src_scheme and src_scheme == dst_scheme)

            current_rows.sort(
                key=lambda r: (r.get("member") is not None, str(r.get("member") or ""))
            )
            new_obs_ids: list[uuid.UUID] = []
            for row in current_rows:
                row_anchor_str = str(row["anchor"])
                row_member = (
                    str(row["member"]) if row.get("member") is not None else None
                )
                if exclude_anchor_member is not None and (
                    row_anchor_str == exclude_anchor_member[0]
                    and row_member == exclude_anchor_member[1]
                ):
                    continue
                row_identity = self.get_identity(row_anchor_str)
                old_witness = row.get("witness")
                if row_member is None:
                    if dst_path is not None and Path(dst_path).is_dir():
                        new_witness: Witness | None = DirTreeStat.capture(
                            dst_path,
                            observed_at_ns=ts_ns,
                        )
                        repr_kind = "tree"
                    else:
                        new_witness = (
                            parent_stat if parent_stat is not None else old_witness
                        )
                        repr_kind = (
                            "tree" if isinstance(new_witness, DirTreeStat) else "file"
                        )
                else:
                    new_witness = (
                        MemberOf(parent=parent_stat, member=row_member)
                        if parent_stat is not None
                        else old_witness
                    )
                    repr_kind = "member"

                derived_obs = Observation(
                    location=Location(uri=dst_uri, member=row_member),
                    identity=row_identity,
                    access_driver=str(row["access_driver"]),
                    witness=new_witness,
                    basis="derived",
                    site=site,
                    scope_key=(
                        scope_key
                        if scope_key is not None
                        else (
                            str(row["scope_key"])
                            if same_scheme and row.get("scope_key") is not None
                            else None
                        )
                    ),
                    mount_instance_id=(
                        mount_instance_id
                        if mount_instance_id is not None
                        else (
                            str(row["mount_instance_id"])
                            if same_scheme and row.get("mount_instance_id") is not None
                            else None
                        )
                    ),
                    observed_at_ns=ts_ns,
                )
                new_id = self.record_observation(
                    derived_obs,
                    repr_kind=repr_kind,
                    writer=effective_writer,
                )
                new_obs_ids.append(new_id)

            return new_obs_ids

    def record_location_event(
        self,
        *,
        observation_id: uuid.UUID,
        status: LocationStatus,
        status_rank: int | None = None,
        transfer_ref: str | None = None,
        at_ns: int | None = None,
        writer: str | None = None,
        event_id: uuid.UUID | None = None,
    ) -> uuid.UUID:
        """Record a monotonic status event into `location_event` (or in-memory overlay if `read_only=True`)."""
        ev_id = event_id if event_id is not None else uuid.uuid4()
        rank = resolve_status_rank(status, status_rank)
        ts_ns = time.time_ns() if at_ns is None else int(at_ns)
        effective_writer = writer or self.writer

        with self._lock:
            self._memoized_observations.clear()
            self._memoized_locations.clear()

            if self.read_only:
                if not any(e["event_id"] == ev_id for e in self._overlay_events):
                    self._overlay_events.append(
                        {
                            "event_id": ev_id,
                            "observation_id": observation_id,
                            "status": status,
                            "status_rank": rank,
                            "transfer_ref": transfer_ref,
                            "at_ns": ts_ns,
                            "writer": effective_writer,
                        }
                    )
                return ev_id

            with self.exec_cursor() as cur:
                cur.execute(
                    "INSERT INTO main.location_event ("
                    "event_id, observation_id, status, status_rank, transfer_ref, at_ns, writer"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (event_id) DO NOTHING",
                    [
                        ev_id,
                        observation_id,
                        status,
                        rank,
                        transfer_ref,
                        ts_ns,
                        effective_writer,
                    ],
                )
            return ev_id

    @staticmethod
    def _rows_to_location_dicts(
        description: Sequence[Any] | None,
        rows: Sequence[tuple[Any, ...]],
    ) -> list[dict[str, Any]]:
        if not description:
            return []
        col_names = [str(col[0]) for col in description]
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(zip(col_names, row))
            raw_witness = item.get("witness_json")
            item["witness"] = deserialize_witness_json(raw_witness)
            out.append(item)
        return out

    def _compute_location_current_with_overlay(
        self,
        *,
        anchors: set[str] | None = None,
        site: str | None = None,
        uri: str | None = None,
        include_evicted: bool = True,
    ) -> list[dict[str, Any]]:
        """Merge DuckDB `artifact_location` + `location_event` with the in-memory overlay."""
        with self._lock:
            with self.exec_cursor() as cur:
                if anchors is not None:
                    anchor_list = sorted(anchors)
                    if not anchor_list:
                        return []
                    place_clauses = ["al.anchor = req.anchor"]
                    place_params: list[Any] = [anchor_list]
                    if site is not None:
                        place_clauses.append("al.site = ?")
                        place_params.append(site)
                    if uri is not None:
                        place_clauses.append("al.uri = ?")
                        place_params.append(uri)
                    db_place_rows = cur.execute(
                        "SELECT DISTINCT al.site, al.uri "
                        "FROM main.artifact_location al "
                        "JOIN (SELECT UNNEST(?) AS anchor) req ON "
                        + " AND ".join(place_clauses),
                        place_params,
                    ).fetchall()
                    candidate_site_uris: set[tuple[str, str]] = {
                        (str(r[0]), str(r[1])) for r in db_place_rows
                    }
                    for ov in self._overlay_observations.values():
                        if str(ov["anchor"]) not in anchors:
                            continue
                        if site is not None and str(ov["site"]) != site:
                            continue
                        if uri is not None and str(ov["uri"]) != uri:
                            continue
                        candidate_site_uris.add((str(ov["site"]), str(ov["uri"])))

                    if not candidate_site_uris:
                        return []
                    sorted_pairs = sorted(candidate_site_uris)
                    sites_col = [s for s, _ in sorted_pairs]
                    uris_col = [u for _, u in sorted_pairs]
                    loc_rel = cur.execute(
                        "SELECT al.* FROM main.artifact_location al "
                        "JOIN (SELECT UNNEST(?) AS site, UNNEST(?) AS uri) p "
                        "ON al.site = p.site AND al.uri = p.uri",
                        [sites_col, uris_col],
                    )
                    db_obs = self._rows_to_location_dicts(
                        loc_rel.description,
                        loc_rel.fetchall(),
                    )
                    ov_obs = [
                        dict(v)
                        for v in self._overlay_observations.values()
                        if (str(v["site"]), str(v["uri"])) in candidate_site_uris
                    ]
                else:
                    where_parts: list[str] = []
                    where_params: list[Any] = []
                    if site is not None:
                        where_parts.append("site = ?")
                        where_params.append(site)
                    if uri is not None:
                        where_parts.append("uri = ?")
                        where_params.append(uri)
                    where_clause = (
                        f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
                    )
                    loc_rel = cur.execute(
                        f"SELECT * FROM main.artifact_location{where_clause}",
                        where_params,
                    )
                    db_obs = self._rows_to_location_dicts(
                        loc_rel.description,
                        loc_rel.fetchall(),
                    )
                    ov_obs = [dict(v) for v in self._overlay_observations.values()]

                all_obs = db_obs + ov_obs
                latest_by_place: dict[
                    tuple[str, str | None, str | None, str, str | None],
                    dict[str, Any],
                ] = {}
                for obs in all_obs:
                    place = (
                        str(obs["site"]),
                        str(obs["scope_key"])
                        if obs.get("scope_key") is not None
                        else None,
                        str(obs["mount_instance_id"])
                        if obs.get("mount_instance_id") is not None
                        else None,
                        str(obs["uri"]),
                        str(obs["member"]) if obs.get("member") is not None else None,
                    )
                    obs_uuid = _coerce_uuid(obs["observation_id"])
                    obs["observation_id"] = obs_uuid
                    key = (int(obs["observed_at_ns"]), obs_uuid.int)
                    prev = latest_by_place.get(place)
                    if prev is None or key > (
                        int(prev["observed_at_ns"]),
                        prev["observation_id"].int,
                    ):
                        latest_by_place[place] = obs

                if not latest_by_place:
                    return []

                cand_obs_ids = [
                    str(obs["observation_id"]) for obs in latest_by_place.values()
                ]
                ev_rows = cur.execute(
                    "SELECT event_id, observation_id, status, status_rank, transfer_ref, at_ns "
                    "FROM main.location_event "
                    "WHERE observation_id IN (SELECT CAST(UNNEST(?) AS UUID))",
                    [cand_obs_ids],
                ).fetchall()

            cand_uuid_set = {obs["observation_id"] for obs in latest_by_place.values()}
            all_events = list(ev_rows) + [
                (
                    ov_ev["event_id"],
                    ov_ev["observation_id"],
                    ov_ev["status"],
                    ov_ev["status_rank"],
                    ov_ev.get("transfer_ref"),
                    ov_ev["at_ns"],
                )
                for ov_ev in self._overlay_events
                if ov_ev["observation_id"] in cand_uuid_set
            ]
            best_event_by_obs: dict[
                uuid.UUID, tuple[tuple[int, int, int], str, str | None]
            ] = {}
            for ev_id, obs_id, status, rank, tref, at_ns in all_events:
                ev_uuid = _coerce_uuid(ev_id)
                obs_uuid = _coerce_uuid(obs_id)
                sort_key = (int(rank), int(at_ns), ev_uuid.int)
                prev_ev = best_event_by_obs.get(obs_uuid)
                if prev_ev is None or sort_key > prev_ev[0]:
                    best_event_by_obs[obs_uuid] = (
                        sort_key,
                        str(status),
                        str(tref) if tref is not None else None,
                    )

            results: list[dict[str, Any]] = []
            for place_key, obs in latest_by_place.items():
                p_site, p_scope, p_mount_inst, p_uri, p_member = place_key
                if p_member is not None:
                    container_obs = latest_by_place.get(
                        (p_site, p_scope, p_mount_inst, p_uri, None)
                    )
                    if container_obs is not None and int(
                        container_obs["observed_at_ns"]
                    ) > int(obs["observed_at_ns"]):
                        c_w = container_obs.get("witness")
                        m_w = obs.get("witness")
                        m_parent = m_w.parent if isinstance(m_w, MemberOf) else None
                        if c_w is None or m_parent is None or c_w != m_parent:
                            continue

                if anchors is not None and str(obs["anchor"]) not in anchors:
                    continue
                if site is not None and p_site != site:
                    continue
                if uri is not None and p_uri != uri:
                    continue
                ev_info = best_event_by_obs.get(obs["observation_id"])
                status = ev_info[1] if ev_info is not None else "active"
                tref = ev_info[2] if ev_info is not None else None
                if not include_evicted and status in ("evicted", "failed_transfer"):
                    continue
                row = dict(obs)
                row["status"] = status
                row["transfer_ref"] = tref
                results.append(row)

            results.sort(
                key=lambda r: (int(r["observed_at_ns"]), r["observation_id"].int),
                reverse=True,
            )
            return results

    def _query_sql_locations(
        self,
        source_sql: str,
        params: list[Any],
        *,
        anchor: str | None = None,
        site: str | None = None,
        uri: str | None = None,
        include_evicted: bool = True,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        if anchor is not None:
            clauses.append("anchor = ?")
            params.append(anchor)
        if site is not None:
            clauses.append("site = ?")
            params.append(site)
        if uri is not None:
            clauses.append("uri = ?")
            params.append(uri)
        if not include_evicted:
            clauses.append("status != 'evicted' AND status != 'failed_transfer'")

        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.exec_cursor() as cur:
            rel = cur.execute(
                f"SELECT * FROM {source_sql}{where_sql} "
                "ORDER BY observed_at_ns DESC, observation_id DESC",
                params,
            )
            return self._rows_to_location_dicts(rel.description, rel.fetchall())

    def locate_current(
        self,
        anchors: Sequence[Digest | str],
        *,
        site: str | None = None,
        include_evicted: bool = False,
    ) -> list[dict[str, Any]]:
        """Query the `locate_current(anchors)` table macro with place supersession."""
        anchor_list = [str(a) for a in anchors]
        if not anchor_list:
            return []

        with self._lock:
            if self._overlay_observations or self._overlay_events:
                return self._compute_location_current_with_overlay(
                    anchors=set(anchor_list),
                    site=site,
                    include_evicted=include_evicted,
                )

            return self._query_sql_locations(
                "main.locate_current(?)",
                [anchor_list],
                site=site,
                include_evicted=include_evicted,
            )

    def query_location_current(
        self,
        *,
        anchor: Digest | str | None = None,
        site: str | None = None,
        uri: str | None = None,
        include_evicted: bool = True,
    ) -> list[dict[str, Any]]:
        """Query the `artifact_location_current` view."""
        with self._lock:
            if self._overlay_observations or self._overlay_events:
                return self._compute_location_current_with_overlay(
                    anchors={str(anchor)} if anchor is not None else None,
                    site=site,
                    uri=uri,
                    include_evicted=include_evicted,
                )

            return self._query_sql_locations(
                "main.artifact_location_current",
                [],
                anchor=str(anchor) if anchor is not None else None,
                site=site,
                uri=uri,
                include_evicted=include_evicted,
            )

    def query_df(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a single read-only `SELECT` statement and return a `pandas.DataFrame`."""
        _validate_select_only_sql(sql)
        with self.exec_cursor() as cur:
            rel = cur.execute(sql, params) if params is not None else cur.execute(sql)
            return rel.df()

    def query_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        model: type[_ModelT],
    ) -> list[_ModelT]:
        """Execute a single read-only `SELECT` statement and validate rows with `model`."""
        _validate_select_only_sql(sql)
        with self.exec_cursor() as cur:
            rel = cur.execute(sql, params) if params is not None else cur.execute(sql)
            desc = rel.description or ()
            col_specs = [(str(d[0]), str(d[1]) == "JSON") for d in desc]
            raw_rows = rel.fetchall()

        dict_rows = [
            {
                col_name: (_load_json_if_str(val) if is_json else val)
                for (col_name, is_json), val in zip(col_specs, row)
            }
            for row in raw_rows
        ]
        adapter: TypeAdapter[list[_ModelT]] = TypeAdapter(
            list[model]  # ty: ignore[invalid-type-form]
        )
        return adapter.validate_python(dict_rows)


def open_v2_store(
    db_path: Path | str,
    *,
    read_only: bool = False,
    auto_migrate: bool | str | None = None,
    mounts: Sequence[Mount] | Mapping[str, Mount | Path | str] | None = None,
    site: str = "local",
) -> StoreLedger:
    """Open a v2 `StoreLedger`, running or refusing v1 migration according to `auto_migrate`."""
    from consist.migrations.v1.migrator import open_v2_store as _open_v2_store

    return _open_v2_store(
        db_path,
        read_only=read_only,
        auto_migrate=auto_migrate,
        mounts=mounts,
        site=site,
    )


__all__ = [
    "DigestBasis",
    "LOCATION_STATUS_RANKS",
    "LocationStatus",
    "StoreLedger",
    "deserialize_witness_json",
    "open_v2_store",
    "resolve_status_rank",
    "serialize_witness_json",
]
