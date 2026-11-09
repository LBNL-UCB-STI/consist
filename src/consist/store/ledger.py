"""
`StoreLedger`: v2 DuckDB store manager with live-ledger registration,
in-memory session overlay for read-only connections, and place-superseded
location queries.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import time
from typing import Any, Literal, TypeAlias
import uuid

import duckdb

from consist.digest import Digest, Identity, Kind, SchemeRegistry
from consist.location import (
    LedgerRef,
    Observation,
    Witness,
    register_live_ledger,
    unregister_live_ledger,
    witness_from_json,
    witness_to_json,
)
from consist.meta import FrozenMeta
from consist.store.detect import SchemaState
from consist.store.schema import init_v2_schema

LocationStatus: TypeAlias = Literal[
    "pending_transfer", "active", "evicted", "failed_transfer"
]

LOCATION_STATUS_RANKS: dict[str, int] = {
    "pending_transfer": 0,
    "active": 1,
    "evicted": 2,
    "failed_transfer": 2,
}


def resolve_status_rank(status: str, status_rank: int | None = None) -> int:
    """Resolve or validate monotonic status rank for a `location_event`."""
    if status not in LOCATION_STATUS_RANKS:
        raise ValueError(
            f"Unknown location status {status!r}; expected one of {sorted(LOCATION_STATUS_RANKS)}"
        )
    if status_rank is not None:
        return int(status_rank)
    return LOCATION_STATUS_RANKS[status]


def serialize_witness_json(witness: Witness | None) -> str | None:
    """Serialize a `Witness` into a single JSON string for `artifact_location.witness_json`."""
    kind, payload = witness_to_json(witness)
    if kind is None or payload is None:
        return None
    merged = {"kind": kind, **payload}
    return json.dumps(merged, sort_keys=True, separators=(",", ":"))


def deserialize_witness_json(raw: str | Mapping[str, Any] | None) -> Witness | None:
    """Reconstruct a `Witness` from `artifact_location.witness_json`."""
    if raw is None:
        return None
    data = json.loads(raw) if isinstance(raw, str) else dict(raw)
    if not isinstance(data, Mapping) or not isinstance(kind := data.get("kind"), str):
        return None
    return witness_from_json(kind, data)


def _coerce_digest(val: Digest | str) -> Digest:
    return val if isinstance(val, Digest) else Digest.parse(str(val))


def _default_writer() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


class StoreLedger:
    """Manages a Consist v2 `.duckdb` connection with an in-memory overlay when `read_only=True`."""

    def __init__(
        self,
        db_path: Path | str = ":memory:",
        *,
        read_only: bool = False,
        db_uid: str | None = None,
        instance_key: str | None = None,
        writer: str | None = None,
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

        # In-memory session overlay used when read_only=True
        self._overlay_observations: dict[uuid.UUID, dict[str, Any]] = {}
        self._overlay_events: list[dict[str, Any]] = []
        self._overlay_digests: dict[tuple[str, str], str] = {}
        self._overlay_digest_conflicts: list[dict[str, Any]] = []

        target = ":memory:" if self.db_path is None else str(self.db_path)
        if self.read_only and self.db_path is not None and not self.db_path.exists():
            raise FileNotFoundError(
                f"Database file does not exist for read-only access: {self.db_path}"
            )

        self._con: duckdb.DuckDBPyConnection = duckdb.connect(
            target,
            read_only=self.read_only,
        )

        if not self.read_only:
            self.db_uid: str = init_v2_schema(self._con, db_uid=db_uid)
        else:
            row = self._con.execute(
                "SELECT value FROM consist_meta WHERE key = 'db_uid'"
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
        return tuple(self._overlay_digest_conflicts)

    def __enter__(self) -> StoreLedger:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def close(self) -> None:
        """Unregister from `_live_ledgers` and close the underlying DuckDB connection."""
        if self._closed:
            return
        self._closed = True
        unregister_live_ledger(
            self,
            instance_key=self.instance_key,
            db_path=self.db_path,
        )
        self._con.close()

    def record_content(
        self,
        anchor: Digest | str,
        repr_kind: str,
        *,
        schema_id: str | None = None,
        created_at: datetime | None = None,
    ) -> Digest:
        """Insert a `content` hub row (and primary `content_digest` claim if verifiable)."""
        anchor_digest = _coerce_digest(anchor)
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        if not self.read_only:
            effective_repr = (
                "historical" if anchor_digest.kind is Kind.HISTORICAL else repr_kind
            )
            self._con.execute(
                "INSERT INTO content (anchor, repr_kind, schema_id, created_at) "
                "VALUES (?, ?, ?, ?) ON CONFLICT (anchor) DO NOTHING",
                [str(anchor_digest), effective_repr, schema_id, now],
            )
        if anchor_digest.kind is not Kind.HISTORICAL:
            self.record_content_digest(
                anchor_digest,
                anchor_digest,
                basis="computed",
                source=self.writer,
                computed_at=now,
            )
        return anchor_digest

    def record_content_digest(
        self,
        anchor: Digest | str,
        digest: Digest | str,
        *,
        basis: Literal["computed", "implied"] = "computed",
        repr_kind: str | None = None,
        source: str | None = None,
        computed_at: datetime | None = None,
    ) -> bool:
        """Record a `content_digest` claim or audit a conflict in `content_digest_conflict`.

        When `read_only=True`, stores the claim in the in-memory session overlay
        without writing any files to disk.
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
        if basis not in ("computed", "implied"):
            raise ValueError(
                f"content_digest basis must be 'computed' or 'implied', got {basis!r}"
            )
        now = computed_at if computed_at is not None else datetime.now(timezone.utc)
        src = source or self.writer
        anchor_str = str(anchor_digest)
        scheme_str = SchemeRegistry.get(claim_digest.scheme).name
        val_str = claim_digest.hex

        if self.read_only:
            db_row = self._con.execute(
                "SELECT value FROM content_digest WHERE anchor = ? AND scheme = ?",
                [anchor_str, scheme_str],
            ).fetchone()
            kept_val = (
                str(db_row[0])
                if db_row is not None
                else self._overlay_digests.get((anchor_str, scheme_str))
            )
            if kept_val is None:
                self._overlay_digests[(anchor_str, scheme_str)] = val_str
                return True
            if kept_val == val_str:
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
            self.record_content(anchor_digest, repr_kind, created_at=now)

        existing_row = self._con.execute(
            "SELECT value FROM content_digest WHERE anchor = ? AND scheme = ?",
            [anchor_str, scheme_str],
        ).fetchone()
        if existing_row is None:
            self._con.execute(
                "INSERT INTO content_digest (anchor, scheme, value, basis, computed_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [anchor_str, scheme_str, val_str, basis, now],
            )
            return True

        kept_value = str(existing_row[0])
        if kept_value == val_str:
            return True

        self._con.execute(
            "INSERT INTO content_digest_conflict "
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
        rows = self._con.execute(
            "SELECT scheme, value FROM content_digest WHERE anchor = ?",
            [anchor_str],
        ).fetchall()
        claims_by_scheme: dict[str, Digest] = {
            anchor_digest.scheme: anchor_digest,
        }
        for scheme, value in rows:
            d = Digest(scheme=str(scheme), hex=str(value))
            claims_by_scheme.setdefault(d.scheme, d)
        for (ov_anchor, ov_scheme), ov_val in self._overlay_digests.items():
            if ov_anchor == anchor_str:
                d = Digest(scheme=ov_scheme, hex=ov_val)
                claims_by_scheme.setdefault(d.scheme, d)
        return Identity(anchor=anchor_digest, claims=tuple(claims_by_scheme.values()))

    def record_identity(
        self,
        identity: Identity,
        repr_kind: str = "file",
        *,
        schema_id: str | None = None,
        basis: Literal["computed", "implied"] = "computed",
        source: str | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Record `identity.anchor` in `content` and all verifiable `identity.claims` in `content_digest`."""
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        self.record_content(
            identity.anchor,
            repr_kind,
            schema_id=schema_id,
            created_at=now,
        )
        if identity.verifiable:
            src = source or self.writer
            for claim in identity.claims:
                claim_basis: Literal["computed", "implied"] = (
                    "computed" if claim == identity.anchor else basis
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
        anchor_digest = _coerce_digest(anchor)
        self._con.execute(
            "INSERT INTO content_manifest "
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

    def record_digest_policy(
        self,
        policy_sha256: str,
        policy_json: Mapping[str, Any] | str,
        *,
        created_at: datetime | None = None,
    ) -> None:
        """Insert a filter policy definition into `digest_policy`."""
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        payload = (
            policy_json
            if isinstance(policy_json, str)
            else json.dumps(dict(policy_json), sort_keys=True, separators=(",", ":"))
        )
        self._con.execute(
            "INSERT INTO digest_policy (policy_sha256, policy_json, created_at) "
            "VALUES (?, ?, ?) ON CONFLICT (policy_sha256) DO NOTHING",
            [policy_sha256, payload, now],
        )

    def record_content_derivation(
        self,
        src_anchor: Digest | str,
        dst_anchor: Digest | str,
        kind: str,
        *,
        created_at: datetime | None = None,
    ) -> None:
        """Insert a cross-representation derivation link into `content_derivation`."""
        src = _coerce_digest(src_anchor)
        dst = _coerce_digest(dst_anchor)
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        self._con.execute(
            "INSERT INTO content_derivation (src_anchor, dst_anchor, kind, created_at) "
            "VALUES (?, ?, ?, ?) ON CONFLICT (src_anchor, dst_anchor, kind) DO NOTHING",
            [str(src), str(dst), kind, now],
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
        """Insert a promotion from a `Kind.HISTORICAL` tombstone to a verified anchor.

        First promotion wins; returns `True` if the promotion matches or was newly
        inserted, or `False` if a different `verified_anchor` was already recorded.
        """
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
        existing = self._con.execute(
            "SELECT verified_anchor FROM anchor_promotion WHERE historical_anchor = ?",
            [str(hist)],
        ).fetchone()
        if existing is not None:
            return str(existing[0]) == str(ver)

        self._con.execute(
            "INSERT INTO anchor_promotion "
            "(historical_anchor, verified_anchor, basis, asserted_by, evidence, promoted_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [str(hist), str(ver), basis, asserted_by, ev_json, now],
        )
        return True

    def record_artifact(
        self,
        *,
        artifact_id: uuid.UUID,
        key: str,
        driver: str,
        anchor: Digest | str,
        run_id: str | None = None,
        schema_id: str | None = None,
        meta: Mapping[str, Any] | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert an `artifact` handle row with JSON-validated `FrozenMeta`."""
        anchor_digest = _coerce_digest(anchor)
        frozen_meta = FrozenMeta(meta or {})
        meta_json = json.dumps(dict(frozen_meta), sort_keys=True, separators=(",", ":"))
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        self._con.execute(
            "INSERT INTO artifact "
            "(id, key, driver, anchor, run_id, schema_id, meta, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (id) DO UPDATE SET "
            "meta = EXCLUDED.meta, schema_id = COALESCE(EXCLUDED.schema_id, artifact.schema_id)",
            [
                artifact_id,
                key,
                driver,
                str(anchor_digest),
                run_id,
                schema_id,
                meta_json,
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
        parent_run_id: str | None = None,
        code_hash: str | None = None,
        config_hash: str | None = None,
        input_hash: str | None = None,
        signature: str | None = None,
        signature_scheme: str | None = None,
        tags: Sequence[str] | None = None,
        meta: Mapping[str, Any] | None = None,
        started_at: datetime | None = None,
        ended_at: datetime | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Insert or update a `run` row."""
        if cache_status not in ("live", "historical", "ineligible"):
            raise ValueError(f"Invalid cache_status {cache_status!r}")
        now = created_at if created_at is not None else datetime.now(timezone.utc)
        tags_json = (
            json.dumps(list(tags), separators=(",", ":")) if tags is not None else None
        )
        frozen_meta = FrozenMeta(meta or {})
        meta_json = json.dumps(dict(frozen_meta), sort_keys=True, separators=(",", ":"))
        self._con.execute(
            "INSERT INTO run ("
            "id, model_name, description, year, iteration, parent_run_id, status, "
            "code_hash, config_hash, input_hash, signature, signature_scheme, "
            "cache_status, tags, meta, started_at, ended_at, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (id) DO UPDATE SET "
            "status = EXCLUDED.status, "
            "code_hash = COALESCE(EXCLUDED.code_hash, run.code_hash), "
            "config_hash = COALESCE(EXCLUDED.config_hash, run.config_hash), "
            "input_hash = COALESCE(EXCLUDED.input_hash, run.input_hash), "
            "signature = COALESCE(EXCLUDED.signature, run.signature), "
            "signature_scheme = COALESCE(EXCLUDED.signature_scheme, run.signature_scheme), "
            "cache_status = EXCLUDED.cache_status, "
            "tags = COALESCE(EXCLUDED.tags, run.tags), "
            "meta = EXCLUDED.meta, "
            "ended_at = COALESCE(EXCLUDED.ended_at, run.ended_at)",
            [
                run_id,
                model_name,
                description,
                year,
                iteration,
                parent_run_id,
                status,
                code_hash,
                config_hash,
                input_hash,
                signature,
                signature_scheme,
                cache_status,
                tags_json,
                meta_json,
                started_at,
                ended_at,
                now,
            ],
        )

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
        if direction not in ("input", "output", "depends_on"):
            raise ValueError(
                f"Invalid binding direction {direction!r}; expected 'input', 'output', or 'depends_on'"
            )
        anchor_digest = _coerce_digest(anchor)
        self._con.execute(
            "INSERT INTO run_binding ("
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

        if repr_kind is not None:
            claim_basis: Literal["computed", "implied"] = (
                "implied" if observation.basis == "implied" else "computed"
            )
            self.record_identity(
                observation.identity,
                repr_kind=repr_kind,
                schema_id=schema_id,
                basis=claim_basis,
                source=effective_writer,
            )

        witness_json = serialize_witness_json(observation.witness)
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

        self._con.execute(
            "INSERT INTO artifact_location ("
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

        self._con.execute(
            "INSERT INTO location_event ("
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
        include_evicted: bool = True,
    ) -> list[dict[str, Any]]:
        """Merge DuckDB `artifact_location` + `location_event` with the in-memory overlay."""
        cur = self._con.execute("SELECT * FROM artifact_location")
        db_obs = self._rows_to_location_dicts(cur.description, cur.fetchall())
        all_obs = db_obs + [dict(v) for v in self._overlay_observations.values()]

        latest_by_place: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        for obs in all_obs:
            place = (
                str(obs["site"]),
                str(obs.get("scope_key") or ""),
                str(obs.get("mount_instance_id") or ""),
                str(obs["uri"]),
                str(obs.get("member") or ""),
            )
            key = (int(obs["observed_at_ns"]), obs["observation_id"].int)
            prev = latest_by_place.get(place)
            if prev is None or key > (
                int(prev["observed_at_ns"]),
                prev["observation_id"].int,
            ):
                latest_by_place[place] = obs

        ev_rows = self._con.execute(
            "SELECT event_id, observation_id, status, status_rank, transfer_ref, at_ns "
            "FROM location_event"
        ).fetchall()
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
        ]
        best_event_by_obs: dict[
            uuid.UUID, tuple[tuple[int, int, int], str, str | None]
        ] = {}
        for ev_id, obs_id, status, rank, tref, at_ns in all_events:
            sort_key = (int(rank), int(at_ns), ev_id.int)
            prev_ev = best_event_by_obs.get(obs_id)
            if prev_ev is None or sort_key > prev_ev[0]:
                best_event_by_obs[obs_id] = (
                    sort_key,
                    str(status),
                    str(tref) if tref is not None else None,
                )

        results: list[dict[str, Any]] = []
        for obs in latest_by_place.values():
            if anchors is not None and str(obs["anchor"]) not in anchors:
                continue
            if site is not None and str(obs["site"]) != site:
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
        include_evicted: bool = True,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        if anchor is not None:
            clauses.append("anchor = ?")
            params.append(anchor)
        if site is not None:
            clauses.append("site = ?")
            params.append(site)
        if not include_evicted:
            clauses.append("status != 'evicted' AND status != 'failed_transfer'")

        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        cur = self._con.execute(
            f"SELECT * FROM {source_sql}{where_sql} "
            "ORDER BY observed_at_ns DESC, observation_id DESC",
            params,
        )
        return self._rows_to_location_dicts(cur.description, cur.fetchall())

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

        if self._overlay_observations or self._overlay_events:
            return self._compute_location_current_with_overlay(
                anchors=set(anchor_list),
                site=site,
                include_evicted=include_evicted,
            )

        return self._query_sql_locations(
            "locate_current(?)",
            [anchor_list],
            site=site,
            include_evicted=include_evicted,
        )

    def query_location_current(
        self,
        *,
        anchor: Digest | str | None = None,
        site: str | None = None,
        include_evicted: bool = True,
    ) -> list[dict[str, Any]]:
        """Query the `artifact_location_current` view."""
        if self._overlay_observations or self._overlay_events:
            return self._compute_location_current_with_overlay(
                anchors={str(anchor)} if anchor is not None else None,
                site=site,
                include_evicted=include_evicted,
            )

        return self._query_sql_locations(
            "artifact_location_current",
            [],
            anchor=str(anchor) if anchor is not None else None,
            site=site,
            include_evicted=include_evicted,
        )


__all__ = [
    "LOCATION_STATUS_RANKS",
    "LocationStatus",
    "StoreLedger",
    "deserialize_witness_json",
    "resolve_status_rank",
    "serialize_witness_json",
]
