from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any
import uuid

from consist.digest import (
    Digest,
    Identity,
    Kind,
    canonical_json_str,
    digest_bytes,
)
from consist.engine import (
    _build_path_observation,
    _normalize_mounts,
    resolve_uri_to_path,
    verify_path_digest,
)
from consist.location import (
    DirTreeStat,
    EMPTY_META,
    FileStat,
    Freshness,
    FrozenMeta,
    Location,
    MemberOf,
    Mount,
    Observation,
    SiteContext,
    Witness,
    witness_from_json,
)
from consist.meta import strip_system_meta
from consist.store.ledger import StoreLedger, deserialize_witness_json

SCHEMA_VERSION = 2

_RUN_FIELD_ORDER: tuple[str, ...] = (
    "id",
    "model_name",
    "description",
    "year",
    "iteration",
    "parent_run_id",
    "status",
    "code_hash",
    "config_hash",
    "input_hash",
    "signature",
    "signature_scheme",
    "cache_status",
    "tags",
    "meta",
    "started_at",
    "ended_at",
    "created_at",
)

_INTERNAL_RUN_CONFIG_KEY = "_consist_manifest_config"
_VALID_PROMOTION_BASES = (
    "v1_fast_recomputed",
    "caller_supplied_match",
    "asserted",
)


def _strip_system_meta(raw: Mapping[str, Any] | None) -> FrozenMeta:
    return strip_system_meta(raw, extra_keys=(_INTERNAL_RUN_CONFIG_KEY,))


def _coerce_json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw:
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, Mapping):
                return dict(loaded)
        except ValueError:
            return {}
    return {}


def _is_64_hex(val: str) -> bool:
    return len(val) == 64 and all(c in "0123456789abcdef" for c in val)


def _normalize_cache_status(
    raw_cache_status: Any,
    *,
    has_historical_binding: bool = False,
) -> str:
    if has_historical_binding:
        return "historical"
    norm = str(raw_cache_status or "").strip().lower()
    if norm == "ineligible":
        return "ineligible"
    if norm == "historical":
        return "historical"
    return "live"


def _format_ts(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    return str(val)


def _parse_ts(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str) and val.strip():
        try:
            return datetime.fromisoformat(val.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _encode_manifest_witness_json(witness_json: Any) -> str | None:
    if witness_json is None:
        return None
    if isinstance(witness_json, str):
        try:
            payload = json.loads(witness_json)
        except ValueError:
            return None
    elif isinstance(witness_json, Mapping):
        payload = dict(witness_json)
    else:
        return None
    return canonical_json_str(payload)


def _decode_manifest_witness_json(raw_witness_json: str | None) -> Any:
    if not raw_witness_json:
        return None
    parsed_witness = deserialize_witness_json(raw_witness_json)
    if parsed_witness is not None:
        return parsed_witness
    try:
        loaded = json.loads(raw_witness_json)
    except ValueError:
        return None
    if not isinstance(loaded, Mapping):
        return None
    if isinstance(loaded.get("kind"), str) and isinstance(
        loaded.get("payload"), Mapping
    ):
        return witness_from_json(loaded["kind"], loaded["payload"])
    if "file_count" in loaded and "entries" in loaded:
        return witness_from_json("dir_tree_stat", loaded)
    if "parent" in loaded and "member" in loaded:
        return witness_from_json("member_of", loaded)
    if "token" in loaded:
        return witness_from_json("db_table_change_token", loaded)
    if "etag" in loaded:
        return witness_from_json("object_etag", loaded)
    if "size" in loaded and "mtime_ns" in loaded:
        return witness_from_json("file_stat", loaded)
    return None


@dataclass(frozen=True, slots=True)
class ManifestLocationEntry:
    """Serialized physical location observation inside a v2 `RunManifest`."""

    uri: str
    member: str | None = None
    access_driver: str = "file"
    site: str = "local"
    scope_key: str | None = None
    mount_instance_id: str | None = None
    basis: str = "computed"
    observed_at_ns: int = 0
    witness_json: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.uri,
            "member": self.member,
            "access_driver": self.access_driver,
            "site": self.site,
            "scope_key": self.scope_key,
            "mount_instance_id": self.mount_instance_id,
            "basis": self.basis,
            "observed_at_ns": int(self.observed_at_ns),
            "witness_json": self.witness_json,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ManifestLocationEntry:
        raw_witness = payload.get("witness_json")
        if raw_witness is not None and not isinstance(raw_witness, str):
            raw_witness = canonical_json_str(raw_witness)
        return cls(
            uri=str(payload["uri"]),
            member=(
                str(payload["member"]) if payload.get("member") is not None else None
            ),
            access_driver=str(payload.get("access_driver") or "file"),
            site=str(payload.get("site") or "local"),
            scope_key=(
                str(payload["scope_key"])
                if payload.get("scope_key") is not None
                else None
            ),
            mount_instance_id=(
                str(payload["mount_instance_id"])
                if payload.get("mount_instance_id") is not None
                else None
            ),
            basis=str(payload.get("basis") or "computed"),
            observed_at_ns=int(payload.get("observed_at_ns") or 0),
            witness_json=raw_witness,
        )


@dataclass(frozen=True, slots=True)
class ManifestArtifactEntry:
    """Serialized input or output artifact entry inside a v2 `RunManifest`."""

    artifact_id: str
    key: str
    driver: str
    slot: str
    ordinal: int = 0
    role: str | None = None
    anchor: str = ""
    repr_kind: str = "file"
    claims: tuple[str, ...] = ()
    recorded_anchor: str | None = None
    run_id: str | None = None
    schema_id: str | None = None
    contract_scheme: str | None = None
    sig_scheme: str | None = None
    sig_hex: str | None = None
    meta: FrozenMeta = EMPTY_META
    locations: tuple[ManifestLocationEntry, ...] = ()
    manifest_body_b64: str | None = None
    direction: str | None = None
    promotion_basis: str | None = None
    promotion_asserted_by: str | None = None
    manifest_format: str | None = None
    manifest_policy_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.anchor:
            raise ValueError("ManifestArtifactEntry.anchor must be a non-empty string")
        parsed_anchor = Digest.parse(self.anchor)
        object.__setattr__(self, "anchor", str(parsed_anchor))
        norm_claims = tuple(str(Digest.parse(str(c))) for c in (self.claims or ()))
        object.__setattr__(self, "claims", norm_claims)
        if not isinstance(self.meta, FrozenMeta):
            object.__setattr__(self, "meta", _strip_system_meta(self.meta))
        norm_locs = tuple(
            loc
            if isinstance(loc, ManifestLocationEntry)
            else ManifestLocationEntry.from_dict(loc)
            for loc in (self.locations or ())
        )
        object.__setattr__(self, "locations", norm_locs)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "artifact_id": self.artifact_id,
            "key": self.key,
            "driver": self.driver,
            "slot": self.slot,
            "ordinal": int(self.ordinal),
            "role": self.role,
            "anchor": self.anchor,
            "repr_kind": self.repr_kind,
            "claims": list(self.claims),
            "recorded_anchor": self.recorded_anchor,
            "run_id": self.run_id,
            "schema_id": self.schema_id,
            "contract_scheme": self.contract_scheme,
            "sig_scheme": self.sig_scheme,
            "sig_hex": self.sig_hex,
            "meta": self.meta.to_dict(),
            "locations": [loc.to_dict() for loc in self.locations],
            "manifest_body_b64": self.manifest_body_b64,
        }
        if self.direction is not None:
            payload["direction"] = self.direction
        if self.promotion_basis is not None:
            payload["promotion_basis"] = self.promotion_basis
        if self.promotion_asserted_by is not None:
            payload["promotion_asserted_by"] = self.promotion_asserted_by
        if self.manifest_format is not None:
            payload["manifest_format"] = self.manifest_format
        if self.manifest_policy_sha256 is not None:
            payload["manifest_policy_sha256"] = self.manifest_policy_sha256
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ManifestArtifactEntry:
        raw_meta = payload.get("meta")
        meta_map = raw_meta if isinstance(raw_meta, Mapping) else {}
        raw_locs = payload.get("locations") or ()
        locs = tuple(
            loc
            if isinstance(loc, ManifestLocationEntry)
            else ManifestLocationEntry.from_dict(loc)
            for loc in raw_locs
        )
        raw_claims = payload.get("claims") or ()
        return cls(
            artifact_id=str(payload["artifact_id"]),
            key=str(payload["key"]),
            driver=str(payload.get("driver") or "file"),
            slot=str(payload.get("slot") or payload["key"]),
            ordinal=int(payload.get("ordinal") or 0),
            role=str(payload["role"]) if payload.get("role") is not None else None,
            anchor=str(payload["anchor"]),
            repr_kind=str(payload.get("repr_kind") or "file"),
            claims=tuple(str(c) for c in raw_claims),
            recorded_anchor=(
                str(payload["recorded_anchor"])
                if payload.get("recorded_anchor") is not None
                else None
            ),
            run_id=(
                str(payload["run_id"]) if payload.get("run_id") is not None else None
            ),
            schema_id=(
                str(payload["schema_id"])
                if payload.get("schema_id") is not None
                else None
            ),
            contract_scheme=(
                str(payload["contract_scheme"])
                if payload.get("contract_scheme") is not None
                else None
            ),
            sig_scheme=(
                str(payload["sig_scheme"])
                if payload.get("sig_scheme") is not None
                else None
            ),
            sig_hex=(
                str(payload["sig_hex"]) if payload.get("sig_hex") is not None else None
            ),
            meta=_strip_system_meta(meta_map),
            locations=locs,
            manifest_body_b64=(
                str(payload["manifest_body_b64"])
                if payload.get("manifest_body_b64") is not None
                else None
            ),
            direction=(
                str(payload["direction"])
                if payload.get("direction") is not None
                else None
            ),
            promotion_basis=(
                str(payload["promotion_basis"])
                if payload.get("promotion_basis") is not None
                else None
            ),
            promotion_asserted_by=(
                str(payload["promotion_asserted_by"])
                if payload.get("promotion_asserted_by") is not None
                else None
            ),
            manifest_format=(
                str(payload["manifest_format"])
                if payload.get("manifest_format") is not None
                else None
            ),
            manifest_policy_sha256=(
                str(payload["manifest_policy_sha256"])
                if payload.get("manifest_policy_sha256") is not None
                else None
            ),
        )


def _extract_v1_logical_claim(meta_map: Mapping[str, Any]) -> str | None:
    candidates: list[Any] = [meta_map.get("logical_identity")]
    raw_sem = meta_map.get("hash_semantics")
    if isinstance(raw_sem, Mapping):
        candidates.append(raw_sem.get("logical_hash"))
    for cand in candidates:
        if isinstance(cand, Mapping):
            cand = cand.get("digest") or cand.get("hash") or cand.get("hex")
        if not isinstance(cand, str) or not cand.strip():
            continue
        norm = cand.strip().lower()
        if norm.startswith("sha256:logical_matrices:v1:"):
            hex_part = norm.split("sha256:logical_matrices:v1:", 1)[1]
            if _is_64_hex(hex_part):
                return f"sha256:logical_matrices:v1:{hex_part}"
        elif norm.startswith("sha256:logical_matrices:"):
            hex_part = norm.split("sha256:logical_matrices:", 1)[1]
            if _is_64_hex(hex_part):
                return f"sha256:logical_matrices:v1:{hex_part}"
        elif _is_64_hex(norm):
            return f"sha256:logical_matrices:v1:{norm}"
    return None


def _extract_v1_admission_anchor(meta_map: Mapping[str, Any]) -> tuple[str, str] | None:
    for key_name in ("admission_file_identity", "admission_identity"):
        val = meta_map.get(key_name)
        if isinstance(val, Mapping):
            val = val.get("digest") or val.get("anchor")
        if not isinstance(val, str) or not val.strip():
            continue
        norm = val.strip().lower()
        if norm.startswith("sha256:file:"):
            hex_part = norm.split("sha256:file:", 1)[1]
            if _is_64_hex(hex_part):
                return (f"sha256:file:{hex_part}", "file")
        if norm.startswith("sha256:manifest-v1:"):
            hex_part = norm.split("sha256:manifest-v1:", 1)[1]
            if _is_64_hex(hex_part):
                return (f"sha256:manifest-v1:{hex_part}", "tree")
    return None


def _convert_v1_artifact_dict(
    art_dict: Mapping[str, Any],
    *,
    ordinal: int = 0,
) -> ManifestArtifactEntry:
    raw_id = art_dict.get("id")
    art_id = str(raw_id) if raw_id else str(uuid.uuid4())
    key = str(art_dict.get("key") or "artifact")
    driver = str(art_dict.get("driver") or "file")
    slot = str(art_dict.get("slot") or key)
    role = str(art_dict.get("role") or slot)
    run_id = str(art_dict["run_id"]) if art_dict.get("run_id") is not None else None
    raw_meta = art_dict.get("meta")
    meta_map: dict[str, Any] = dict(raw_meta) if isinstance(raw_meta, Mapping) else {}
    schema_id = (
        str(art_dict["schema_id"])
        if art_dict.get("schema_id") is not None
        else (
            str(meta_map["schema_id"])
            if meta_map.get("schema_id") is not None
            else None
        )
    )

    raw_hash = str(art_dict.get("hash") or "").strip().lower()
    anchor: str
    repr_kind = "file"
    manifest_body_b64: str | None = None
    claims_list: list[str] = []

    dir_manifest = meta_map.get("directory_manifest")
    if isinstance(dir_manifest, Mapping) and isinstance(
        dir_manifest.get("entries"), list
    ):
        identity_payload = {
            "version": int(dir_manifest.get("version", 1)),
            "entries": dir_manifest["entries"],
        }
        body_bytes = canonical_json_str(identity_payload).encode("utf-8")
        tree_hash = str(
            dir_manifest.get("tree_hash") or digest_bytes(body_bytes)
        ).lower()
        anchor = f"sha256:manifest-v1:{tree_hash}"
        repr_kind = "tree"
        manifest_body_b64 = base64.b64encode(body_bytes).decode("ascii")
        claims_list.append(anchor)
    elif ":" in raw_hash:
        try:
            parsed = Digest.parse(raw_hash)
            anchor = str(parsed)
            if parsed.kind is Kind.HISTORICAL:
                repr_kind = "historical"
            elif parsed.scheme == "sha256:manifest-v1":
                repr_kind = "tree"
            elif parsed.scheme in (
                "sha256:logical_matrices:v1",
                "sha256:logical_matrix:v1",
            ):
                repr_kind = "matrix_set"
            if parsed.kind is not Kind.HISTORICAL:
                claims_list.append(anchor)
        except ValueError:
            hex_fallback = digest_bytes(f"{art_id}:{key}:{raw_hash}".encode("utf-8"))
            anchor = f"legacy-v1:unverified:{hex_fallback}"
            repr_kind = "historical"
    else:
        raw_sem_obj = meta_map.get("hash_semantics")
        is_64_hex = _is_64_hex(raw_hash)
        sem_contract = ""
        sem_source = ""
        sem_kind = ""
        hash_sem = ""
        if isinstance(raw_sem_obj, Mapping):
            sem_contract = str(raw_sem_obj.get("digest_contract") or "").strip().lower()
            sem_source = str(raw_sem_obj.get("source") or "").strip().lower()
            sem_kind = str(raw_sem_obj.get("kind") or "").strip().lower()
        else:
            hash_sem = str(raw_sem_obj or "").strip().lower()
        hash_algo = str(meta_map.get("hash_algorithm") or "").strip().lower()
        adm_anchor = _extract_v1_admission_anchor(meta_map)

        if is_64_hex and (
            hash_sem in ("full_sha256", "sha256", "file_sha256", "sha256:file")
            or hash_algo in ("sha256_full", "full_sha256")
            or (
                sem_contract == "raw_file_bytes"
                and (sem_source in ("", "computed_full") or sem_kind in ("", "bytes"))
            )
        ):
            anchor = f"sha256:file:{raw_hash}"
            repr_kind = "file"
            claims_list.append(anchor)
        elif is_64_hex and (
            hash_sem in ("manifest-v1", "directory_manifest")
            or sem_contract == "directory_manifest_v1"
        ):
            anchor = f"sha256:manifest-v1:{raw_hash}"
            repr_kind = "tree"
            claims_list.append(anchor)
        elif adm_anchor is not None:
            anchor, repr_kind = adm_anchor
            claims_list.append(anchor)
        elif is_64_hex and hash_sem in (
            "logical_matrices",
            "sha256:logical_matrices",
            "sha256:logical_matrices:v1",
        ):
            anchor = f"sha256:logical_matrices:v1:{raw_hash}"
            repr_kind = "matrix_set"
            claims_list.append(anchor)
        else:
            hex_val = (
                raw_hash
                if is_64_hex
                else digest_bytes(f"{art_id}:{key}:{raw_hash}".encode("utf-8"))
            )
            anchor = f"legacy-v1:unverified:{hex_val}"
            repr_kind = "historical"

    logical_claim = _extract_v1_logical_claim(meta_map)
    if logical_claim is not None and repr_kind != "historical":
        if logical_claim not in claims_list:
            claims_list.append(logical_claim)

    ctd_raw = meta_map.get("canonical_table_digest")
    if isinstance(ctd_raw, str) and ctd_raw.strip() and repr_kind != "historical":
        ctd_str = ctd_raw.strip().lower()
        if not ctd_str.startswith("ctd1:sha256:"):
            if _is_64_hex(ctd_str):
                ctd_str = f"ctd1:sha256:{ctd_str}"
        try:
            ctd_digest = str(Digest.parse(ctd_str))
            if ctd_digest not in claims_list:
                claims_list.append(ctd_digest)
        except ValueError:
            pass

    locations: list[ManifestLocationEntry] = []
    container_uri = art_dict.get("container_uri")
    table_path = art_dict.get("table_path")
    member = (
        str(table_path)
        if (table_path is not None and str(table_path).strip())
        else None
    )
    if isinstance(container_uri, str) and container_uri.strip():
        locations.append(
            ManifestLocationEntry(
                uri=container_uri.strip(),
                member=member,
                access_driver=driver,
                site="local",
                basis="migrated_v1",
                observed_at_ns=0,
            )
        )
    recovery_roots = meta_map.get("recovery_roots")
    if isinstance(recovery_roots, Sequence) and not isinstance(
        recovery_roots, (str, bytes)
    ):
        base_name = (
            Path(str(container_uri).split("://", 1)[-1]).name
            if isinstance(container_uri, str) and container_uri.strip()
            else key
        )
        for root_entry in recovery_roots:
            if isinstance(root_entry, str) and root_entry.strip():
                cand_uri = str(Path(root_entry.strip()) / base_name)
                locations.append(
                    ManifestLocationEntry(
                        uri=cand_uri,
                        member=member,
                        access_driver=driver,
                        site="local",
                        basis="migrated_v1",
                        observed_at_ns=0,
                    )
                )

    return ManifestArtifactEntry(
        artifact_id=art_id,
        key=key,
        driver=driver,
        slot=slot,
        ordinal=ordinal,
        role=role,
        anchor=anchor,
        repr_kind=repr_kind,
        claims=tuple(claims_list),
        recorded_anchor=None,
        run_id=run_id,
        schema_id=schema_id,
        meta=_strip_system_meta(meta_map),
        locations=tuple(locations),
        manifest_body_b64=manifest_body_b64,
    )


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Deterministic v2 `consist.json` run provenance snapshot."""

    schema_version: int = SCHEMA_VERSION
    run: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    inputs: tuple[ManifestArtifactEntry, ...] = ()
    outputs: tuple[ManifestArtifactEntry, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "schema_version", int(self.schema_version))
        object.__setattr__(self, "run", dict(self.run or {}))
        object.__setattr__(self, "config", dict(self.config or {}))
        object.__setattr__(
            self,
            "inputs",
            tuple(
                e
                if isinstance(e, ManifestArtifactEntry)
                else ManifestArtifactEntry.from_dict(e)
                for e in (self.inputs or ())
            ),
        )
        object.__setattr__(
            self,
            "outputs",
            tuple(
                e
                if isinstance(e, ManifestArtifactEntry)
                else ManifestArtifactEntry.from_dict(e)
                for e in (self.outputs or ())
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        ordered_run: dict[str, Any] = {}
        for k in _RUN_FIELD_ORDER:
            if k in self.run:
                ordered_run[k] = self.run[k]
        for k, v in self.run.items():
            if k not in ordered_run:
                ordered_run[k] = v
        return {
            "schema_version": self.schema_version,
            "run": ordered_run,
            "config": dict(self.config),
            "inputs": [entry.to_dict() for entry in self.inputs],
            "outputs": [entry.to_dict() for entry in self.outputs],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> RunManifest:
        raw_version = payload.get("schema_version")
        raw_run = payload.get("run")
        run_map = dict(raw_run) if isinstance(raw_run, Mapping) else {}
        raw_config = payload.get("config")
        config_map = dict(raw_config) if isinstance(raw_config, Mapping) else {}

        if raw_version == SCHEMA_VERSION:
            inputs = tuple(
                ManifestArtifactEntry.from_dict(item)
                for item in (payload.get("inputs") or ())
            )
            outputs = tuple(
                ManifestArtifactEntry.from_dict(item)
                for item in (payload.get("outputs") or ())
            )
            has_hist = any(
                Digest.parse(e.anchor).kind is Kind.HISTORICAL
                for e in (*inputs, *outputs)
            )
            norm_run = _normalize_run_dict(
                run_map,
                has_historical_binding=has_hist,
            )
            return cls(
                schema_version=SCHEMA_VERSION,
                run=norm_run,
                config=config_map,
                inputs=inputs,
                outputs=outputs,
            )

        # Legacy v1 consist.json conversion
        raw_inputs = payload.get("inputs") or ()
        raw_outputs = payload.get("outputs") or ()
        inputs_v2 = tuple(
            _convert_v1_artifact_dict(item, ordinal=idx)
            for idx, item in enumerate(raw_inputs)
            if isinstance(item, Mapping)
        )
        outputs_v2 = tuple(
            _convert_v1_artifact_dict(item, ordinal=idx)
            for idx, item in enumerate(raw_outputs)
            if isinstance(item, Mapping)
        )
        has_hist = any(
            Digest.parse(e.anchor).kind is Kind.HISTORICAL
            for e in (*inputs_v2, *outputs_v2)
        )
        norm_run = _normalize_run_dict(
            run_map,
            has_historical_binding=has_hist,
        )
        return cls(
            schema_version=SCHEMA_VERSION,
            run=norm_run,
            config=config_map,
            inputs=inputs_v2,
            outputs=outputs_v2,
        )


def _normalize_run_dict(
    raw_run: Mapping[str, Any],
    *,
    has_historical_binding: bool = False,
) -> dict[str, Any]:
    meta_dict = _coerce_json_dict(raw_run.get("meta"))
    meta_dict.pop(_INTERNAL_RUN_CONFIG_KEY, None)
    raw_tags = raw_run.get("tags")
    if isinstance(raw_tags, str) and raw_tags:
        try:
            tags_list = list(json.loads(raw_tags))
        except ValueError:
            tags_list = [raw_tags]
    elif isinstance(raw_tags, Sequence) and not isinstance(raw_tags, (str, bytes)):
        tags_list = [str(t) for t in raw_tags]
    else:
        tags_list = []

    code_hash = raw_run.get("code_hash")
    if code_hash is None and raw_run.get("git_hash") is not None:
        code_hash = raw_run.get("git_hash")

    return {
        "id": str(raw_run.get("id") or ""),
        "model_name": str(raw_run.get("model_name") or "default"),
        "description": raw_run.get("description"),
        "year": int(raw_run["year"]) if raw_run.get("year") is not None else None,
        "iteration": (
            int(raw_run["iteration"]) if raw_run.get("iteration") is not None else None
        ),
        "parent_run_id": raw_run.get("parent_run_id"),
        "status": str(raw_run.get("status") or "completed"),
        "code_hash": code_hash,
        "config_hash": raw_run.get("config_hash"),
        "input_hash": raw_run.get("input_hash"),
        "signature": raw_run.get("signature"),
        "signature_scheme": str(raw_run.get("signature_scheme") or "sig-v2"),
        "cache_status": _normalize_cache_status(
            raw_run.get("cache_status"),
            has_historical_binding=has_historical_binding,
        ),
        "tags": tags_list,
        "meta": meta_dict,
        "started_at": _format_ts(raw_run.get("started_at")),
        "ended_at": _format_ts(raw_run.get("ended_at")),
        "created_at": _format_ts(raw_run.get("created_at")),
    }


def write_run_manifest(
    path: str | Path,
    manifest: RunManifest | Mapping[str, Any],
) -> Path:
    """Atomically write canonical JSON for `manifest` to `path`."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    run_manifest = (
        manifest
        if isinstance(manifest, RunManifest)
        else RunManifest.from_dict(manifest)
    )
    json_text = run_manifest.to_json(indent=2) + "\n"
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json_text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, target)
    except Exception:
        Path(tmp_name).unlink(missing_ok=True)
        raise
    return target


def load_run_manifest(
    source: str | Path | RunManifest | Mapping[str, Any],
) -> RunManifest:
    """Load a `RunManifest` from a JSON file path, JSON string, dict, or `RunManifest`."""
    if isinstance(source, RunManifest):
        return source
    if isinstance(source, Mapping):
        return RunManifest.from_dict(source)
    if isinstance(source, Path):
        payload = json.loads(source.expanduser().read_text(encoding="utf-8"))
        return RunManifest.from_dict(payload)
    if isinstance(source, str):
        stripped = source.lstrip()
        if stripped.startswith("{"):
            payload = json.loads(stripped)
            return RunManifest.from_dict(payload)
        file_path = Path(source).expanduser()
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        return RunManifest.from_dict(payload)
    raise TypeError(f"Unsupported manifest source type: {type(source).__name__}")


def export_run_manifest(
    ledger: StoreLedger,
    run_id: str,
    path: str | Path | None = None,
) -> RunManifest:
    """Export a deterministic v2 `RunManifest` for `run_id` from `ledger`."""
    con = ledger.connection
    run_row = con.execute(
        "SELECT id, model_name, description, year, iteration, parent_run_id, status, "
        "code_hash, config_hash, input_hash, signature, signature_scheme, "
        "cache_status, tags, meta, started_at, ended_at, created_at "
        "FROM run WHERE id = ?",
        [run_id],
    ).fetchone()
    if run_row is None:
        raise KeyError(f"Run {run_id!r} not found in StoreLedger")

    (
        r_id,
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
        raw_tags,
        raw_meta,
        started_at,
        ended_at,
        created_at,
    ) = run_row

    parsed_meta = _coerce_json_dict(raw_meta)

    config_dict: dict[str, Any] = {}
    if isinstance(parsed_meta.get(_INTERNAL_RUN_CONFIG_KEY), Mapping):
        config_dict = dict(parsed_meta.pop(_INTERNAL_RUN_CONFIG_KEY))
    elif isinstance(parsed_meta.get("config"), Mapping):
        config_dict = dict(parsed_meta["config"])

    binding_rows = con.execute(
        "SELECT rb.direction, rb.occurrence_index, rb.slot, rb.ordinal, "
        "rb.artifact_id, rb.role, rb.anchor, rb.contract_scheme, rb.sig_scheme, rb.sig_hex, "
        "a.key, a.driver, a.anchor AS art_anchor, a.run_id AS art_run_id, a.schema_id, a.meta "
        "FROM run_binding rb "
        "LEFT JOIN artifact a ON a.id = rb.artifact_id "
        "WHERE rb.run_id = ? "
        "ORDER BY rb.direction, rb.occurrence_index, rb.ordinal, rb.slot",
        [run_id],
    ).fetchall()

    resolved_bindings: list[tuple[Any, Identity, str, str | None, list[str]]] = []
    all_effective_anchors: set[str] = set()
    all_recorded_anchors: set[str] = set()
    all_lookup_anchors: set[str] = set()

    for brow in binding_rows:
        rb_anchor = brow[6]
        art_anchor = brow[12]
        base_anchor_str = str(art_anchor if art_anchor is not None else rb_anchor)
        effective_identity, recorded_anchor_opt, verified_anchor_opt = (
            ledger.resolve_effective_identity(base_anchor_str)
        )
        effective_anchor_str = str(effective_identity.anchor)
        recorded_anchor_str = (
            str(recorded_anchor_opt) if recorded_anchor_opt is not None else None
        )
        lookup_anchors = [effective_anchor_str]
        if recorded_anchor_str and recorded_anchor_str not in lookup_anchors:
            lookup_anchors.append(recorded_anchor_str)
            all_recorded_anchors.add(recorded_anchor_str)
        if (
            verified_anchor_opt is not None
            and str(verified_anchor_opt) not in lookup_anchors
        ):
            lookup_anchors.append(str(verified_anchor_opt))

        all_effective_anchors.add(effective_anchor_str)
        all_lookup_anchors.update(lookup_anchors)
        resolved_bindings.append(
            (
                brow,
                effective_identity,
                effective_anchor_str,
                recorded_anchor_str,
                lookup_anchors,
            )
        )

    content_by_anchor: dict[str, tuple[str | None, str | None]] = {}
    manifest_by_anchor: dict[str, tuple[str | None, str | None, bytes | None]] = {}
    if all_effective_anchors:
        eff_list = sorted(all_effective_anchors)
        eff_placeholders = ", ".join("?" for _ in eff_list)
        for crow in con.execute(
            f"SELECT anchor, repr_kind, schema_id FROM content WHERE anchor IN ({eff_placeholders})",
            eff_list,
        ).fetchall():
            content_by_anchor[str(crow[0])] = (
                str(crow[1]) if crow[1] is not None else None,
                str(crow[2]) if crow[2] is not None else None,
            )
        for mrow in con.execute(
            f"SELECT anchor, format, policy_sha256, body FROM content_manifest WHERE anchor IN ({eff_placeholders})",
            eff_list,
        ).fetchall():
            manifest_by_anchor[str(mrow[0])] = (
                str(mrow[1]) if mrow[1] is not None else None,
                str(mrow[2]) if mrow[2] is not None else None,
                bytes(mrow[3]) if mrow[3] is not None else None,
            )

    promotion_by_recorded: dict[str, tuple[str | None, str | None]] = {}
    if all_recorded_anchors:
        rec_list = sorted(all_recorded_anchors)
        rec_placeholders = ", ".join("?" for _ in rec_list)
        for prow in con.execute(
            f"SELECT historical_anchor, basis, asserted_by FROM anchor_promotion WHERE historical_anchor IN ({rec_placeholders})",
            rec_list,
        ).fetchall():
            promotion_by_recorded[str(prow[0])] = (
                str(prow[1]) if prow[1] is not None else None,
                str(prow[2]) if prow[2] is not None else None,
            )

    locs_by_anchor: dict[str, list[dict[str, Any]]] = {}
    if all_lookup_anchors:
        for lrow in ledger.locate_current(
            sorted(all_lookup_anchors),
            include_evicted=False,
        ):
            locs_by_anchor.setdefault(str(lrow["anchor"]), []).append(lrow)

    inputs: list[ManifestArtifactEntry] = []
    outputs: list[ManifestArtifactEntry] = []

    for (
        brow,
        effective_identity,
        effective_anchor_str,
        recorded_anchor_str,
        lookup_anchors,
    ) in resolved_bindings:
        (
            direction,
            _occ_idx,
            slot,
            ordinal,
            art_id,
            role,
            _rb_anchor,
            contract_scheme,
            sig_scheme,
            sig_hex,
            art_key,
            art_driver,
            _art_anchor,
            art_run_id,
            art_schema_id,
            art_raw_meta,
        ) = brow

        c_repr_kind, c_schema_id = content_by_anchor.get(
            effective_anchor_str, (None, None)
        )
        repr_kind = (
            c_repr_kind
            if c_repr_kind is not None
            else (
                "historical"
                if effective_identity.anchor.kind is Kind.HISTORICAL
                else "file"
            )
        )
        effective_schema_id = (
            str(art_schema_id) if art_schema_id is not None else c_schema_id
        )

        claims = (
            tuple(str(d) for d in effective_identity.all_digests)
            if effective_identity.verifiable
            else ()
        )

        m_format, m_policy_sha256, m_body = manifest_by_anchor.get(
            effective_anchor_str,
            (None, None, None),
        )
        manifest_body_b64 = (
            base64.b64encode(m_body).decode("ascii") if m_body is not None else None
        )

        promo_basis: str | None = None
        promo_asserted_by: str | None = None
        if recorded_anchor_str is not None:
            promo_basis, promo_asserted_by = promotion_by_recorded.get(
                recorded_anchor_str,
                (None, None),
            )

        matched_loc_rows: list[dict[str, Any]] = []
        seen_obs_ids: set[str] = set()
        for l_anchor in lookup_anchors:
            for lrow in locs_by_anchor.get(l_anchor, ()):
                obs_key = str(
                    lrow.get("observation_id")
                    or f"{lrow.get('uri')}:{lrow.get('member')}"
                )
                if obs_key not in seen_obs_ids:
                    seen_obs_ids.add(obs_key)
                    matched_loc_rows.append(lrow)

        matched_loc_rows.sort(
            key=lambda r: (
                -int(r.get("observed_at_ns") or 0),
                str(r.get("observation_id") or ""),
            )
        )

        locations = tuple(
            ManifestLocationEntry(
                uri=str(lrow["uri"]),
                member=str(lrow["member"]) if lrow.get("member") is not None else None,
                access_driver=str(lrow.get("access_driver") or art_driver or "file"),
                site=str(lrow.get("site") or "local"),
                scope_key=(
                    str(lrow["scope_key"])
                    if lrow.get("scope_key") is not None
                    else None
                ),
                mount_instance_id=(
                    str(lrow["mount_instance_id"])
                    if lrow.get("mount_instance_id") is not None
                    else None
                ),
                basis=str(lrow.get("basis") or "computed"),
                observed_at_ns=int(lrow.get("observed_at_ns") or 0),
                witness_json=_encode_manifest_witness_json(lrow.get("witness_json")),
            )
            for lrow in matched_loc_rows
        )

        art_meta_map = _coerce_json_dict(art_raw_meta)
        dir_str = str(direction)

        entry = ManifestArtifactEntry(
            artifact_id=str(art_id),
            key=str(art_key if art_key is not None else slot),
            driver=str(art_driver if art_driver is not None else "file"),
            slot=str(slot),
            ordinal=int(ordinal or 0),
            role=str(role) if role is not None else None,
            anchor=effective_anchor_str,
            repr_kind=repr_kind,
            claims=claims,
            recorded_anchor=recorded_anchor_str,
            run_id=str(art_run_id) if art_run_id is not None else None,
            schema_id=effective_schema_id,
            contract_scheme=(
                str(contract_scheme) if contract_scheme is not None else None
            ),
            sig_scheme=str(sig_scheme) if sig_scheme is not None else None,
            sig_hex=str(sig_hex) if sig_hex is not None else None,
            meta=_strip_system_meta(art_meta_map),
            locations=locations,
            manifest_body_b64=manifest_body_b64,
            direction="depends_on" if dir_str == "depends_on" else None,
            promotion_basis=promo_basis,
            promotion_asserted_by=promo_asserted_by,
            manifest_format=(
                m_format
                if m_body is not None and m_format not in (None, "manifest-v1")
                else None
            ),
            manifest_policy_sha256=(m_policy_sha256 if m_body is not None else None),
        )
        if dir_str == "output":
            outputs.append(entry)
        else:
            inputs.append(entry)

    has_hist = any(
        Digest.parse(e.anchor).kind is Kind.HISTORICAL for e in (*inputs, *outputs)
    )
    run_dict = _normalize_run_dict(
        {
            "id": r_id,
            "model_name": model_name,
            "description": description,
            "year": year,
            "iteration": iteration,
            "parent_run_id": parent_run_id,
            "status": status,
            "code_hash": code_hash,
            "config_hash": config_hash,
            "input_hash": input_hash,
            "signature": signature,
            "signature_scheme": signature_scheme,
            "cache_status": cache_status,
            "tags": raw_tags,
            "meta": parsed_meta,
            "started_at": started_at,
            "ended_at": ended_at,
            "created_at": created_at,
        },
        has_historical_binding=has_hist,
    )

    manifest = RunManifest(
        schema_version=SCHEMA_VERSION,
        run=run_dict,
        config=config_dict,
        inputs=tuple(inputs),
        outputs=tuple(outputs),
    )
    if path is not None:
        write_run_manifest(path, manifest)
    return manifest


def _discover_candidate_paths(
    entry: ManifestArtifactEntry,
    *,
    mount_map: Mapping[str, Mount],
    reobserve_roots: Sequence[Path],
) -> list[tuple[Path, str | None]]:
    candidates: list[tuple[Path, str | None]] = []
    seen: set[tuple[Path, str | None]] = set()

    def _add(p: Path, member: str | None) -> None:
        try:
            resolved = p.expanduser().resolve()
        except OSError:
            return
        key = (resolved, member)
        if key not in seen and resolved.exists():
            seen.add(key)
            candidates.append(key)

    for loc in entry.locations:
        resolved_uri_path, _matched_mount = resolve_uri_to_path(
            loc.uri, mounts=mount_map
        )
        if resolved_uri_path is not None:
            _add(resolved_uri_path, loc.member)
        rel_part = (
            loc.uri.split("://", 1)[1] if "://" in loc.uri else loc.uri.lstrip("/")
        )
        base_name = Path(rel_part).name
        for root in reobserve_roots:
            if rel_part:
                _add(root / rel_part, loc.member)
            if base_name:
                _add(root / base_name, loc.member)

    if not candidates and reobserve_roots:
        for root in reobserve_roots:
            _add(root / entry.key, None)

    return candidates


def import_run_manifest(
    ledger: StoreLedger,
    source: str | Path | RunManifest | Mapping[str, Any],
    *,
    reobserve_roots: Sequence[str | Path] | None = None,
    mounts: Sequence[Mount] | Mapping[str, str | Path | Mount] | None = None,
    _verified_path_cache: dict[tuple[Path, str | None, str], Witness] | None = None,
) -> RunManifest:
    """Import a `RunManifest` (v2 or legacy v1) into `ledger` and optionally re-observe surviving files."""
    manifest = load_run_manifest(source)
    con = ledger.connection
    mount_map = _normalize_mounts(mounts)
    if mount_map:
        ledger.mounts.update(mount_map)
    effective_mounts = {**ledger.mounts, **mount_map}

    roots: list[Path] = []
    if reobserve_roots is not None:
        for r in reobserve_roots:
            rp = Path(r).expanduser().resolve()
            if rp.exists():
                roots.append(rp)
    if isinstance(source, (str, Path)) and not str(source).lstrip().startswith("{"):
        src_path = Path(source).expanduser().resolve()
        if src_path.parent.exists() and src_path.parent not in roots:
            roots.append(src_path.parent)

    r = manifest.run
    run_id = str(r.get("id") or "")
    if not run_id:
        raise ValueError("RunManifest.run['id'] must be a non-empty string")

    stored_run_meta = dict(r.get("meta") or {})
    if manifest.config:
        stored_run_meta[_INTERNAL_RUN_CONFIG_KEY] = dict(manifest.config)

    tags_json = canonical_json_str(list(r.get("tags") or []))
    meta_json = canonical_json_str(stored_run_meta)
    now_dt = datetime.now(timezone.utc)
    started_at = _parse_ts(r.get("started_at"))
    ended_at = _parse_ts(r.get("ended_at"))
    created_at = _parse_ts(r.get("created_at")) or started_at or now_dt

    has_historical_binding = any(
        Digest.parse(e.anchor).kind is Kind.HISTORICAL
        for e in (*manifest.inputs, *manifest.outputs)
    )
    effective_cache_status = _normalize_cache_status(
        r.get("cache_status"),
        has_historical_binding=has_historical_binding,
    )

    verified_cache = _verified_path_cache if _verified_path_cache is not None else {}
    site_ctx = SiteContext.detect(mounts=effective_mounts)

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            "INSERT INTO run ("
            "id, model_name, description, year, iteration, parent_run_id, status, "
            "code_hash, config_hash, input_hash, signature, signature_scheme, "
            "cache_status, tags, meta, started_at, ended_at, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (id) DO UPDATE SET "
            "model_name = EXCLUDED.model_name, "
            "description = EXCLUDED.description, "
            "year = EXCLUDED.year, "
            "iteration = EXCLUDED.iteration, "
            "parent_run_id = EXCLUDED.parent_run_id, "
            "status = EXCLUDED.status, "
            "code_hash = EXCLUDED.code_hash, "
            "config_hash = EXCLUDED.config_hash, "
            "input_hash = EXCLUDED.input_hash, "
            "signature = EXCLUDED.signature, "
            "signature_scheme = EXCLUDED.signature_scheme, "
            "cache_status = EXCLUDED.cache_status, "
            "tags = EXCLUDED.tags, "
            "meta = EXCLUDED.meta, "
            "started_at = EXCLUDED.started_at, "
            "ended_at = EXCLUDED.ended_at",
            [
                run_id,
                str(r.get("model_name") or "default"),
                r.get("description"),
                r.get("year"),
                r.get("iteration"),
                r.get("parent_run_id"),
                str(r.get("status") or "completed"),
                r.get("code_hash"),
                r.get("config_hash"),
                r.get("input_hash"),
                r.get("signature"),
                str(r.get("signature_scheme") or "sig-v2"),
                effective_cache_status,
                tags_json,
                meta_json,
                started_at,
                ended_at,
                created_at,
            ],
        )

        # Ensure idempotency when re-importing a manifest with modified bindings
        con.execute("DELETE FROM run_binding WHERE run_id = ?", [run_id])

        for list_direction, entries in (
            ("input", manifest.inputs),
            ("output", manifest.outputs),
        ):
            occ_by_dir: dict[str, int] = {}
            for entry in entries:
                effective_dir = (
                    "depends_on"
                    if list_direction == "input" and entry.direction == "depends_on"
                    else list_direction
                )
                occ_idx = occ_by_dir.get(effective_dir, 0)
                occ_by_dir[effective_dir] = occ_idx + 1

                anchor_digest = Digest.parse(entry.anchor)
                claim_digests = tuple(
                    d
                    for c in entry.claims
                    if (d := Digest.parse(c)) != anchor_digest
                    and d.kind is not Kind.HISTORICAL
                )
                identity = (
                    Identity(anchor=anchor_digest, claims=claim_digests)
                    if anchor_digest.kind is not Kind.HISTORICAL
                    else Identity(anchor=anchor_digest)
                )
                ledger.record_identity(
                    identity,
                    repr_kind=entry.repr_kind,
                    schema_id=entry.schema_id,
                )

                recorded_digest: Digest | None = None
                if entry.recorded_anchor is not None:
                    rec_d = Digest.parse(entry.recorded_anchor)
                    if rec_d != anchor_digest and rec_d.kind is Kind.HISTORICAL:
                        recorded_digest = rec_d
                        ledger.record_identity(
                            Identity(anchor=rec_d),
                            repr_kind="historical",
                            schema_id=entry.schema_id,
                        )
                        raw_promo_basis = entry.promotion_basis or "v1_fast_recomputed"
                        promo_basis_lit: Any = (
                            raw_promo_basis
                            if raw_promo_basis in _VALID_PROMOTION_BASES
                            else "v1_fast_recomputed"
                        )
                        ledger.record_anchor_promotion(
                            rec_d,
                            anchor_digest,
                            basis=promo_basis_lit,
                            asserted_by=entry.promotion_asserted_by
                            or "consist.manifest",
                        )

                if entry.manifest_body_b64 is not None:
                    body_bytes = base64.b64decode(
                        entry.manifest_body_b64.encode("ascii")
                    )
                    try:
                        parsed_body = json.loads(body_bytes.decode("utf-8"))
                        entries_list = parsed_body.get("entries") or []
                        entry_count = len(entries_list)
                        total_bytes = sum(
                            int(e.get("size") or 0)
                            for e in entries_list
                            if isinstance(e, Mapping)
                        )
                    except (ValueError, UnicodeDecodeError):
                        entry_count = 0
                        total_bytes = len(body_bytes)
                    ledger.record_content_manifest(
                        anchor_digest,
                        format=entry.manifest_format or "manifest-v1",
                        entry_count=entry_count,
                        total_bytes=total_bytes,
                        body=body_bytes,
                        policy_sha256=entry.manifest_policy_sha256,
                    )

                art_uuid = uuid.UUID(str(entry.artifact_id))
                stored_art_anchor = (
                    recorded_digest if recorded_digest is not None else anchor_digest
                )
                ledger.record_artifact(
                    artifact_id=art_uuid,
                    key=entry.key,
                    driver=entry.driver,
                    anchor=stored_art_anchor,
                    run_id=entry.run_id,
                    schema_id=entry.schema_id,
                    meta=entry.meta,
                )
                ledger.record_run_binding(
                    run_id=run_id,
                    direction=effective_dir,
                    occurrence_index=occ_idx,
                    slot=entry.slot,
                    artifact_id=art_uuid,
                    anchor=stored_art_anchor,
                    ordinal=entry.ordinal,
                    role=entry.role,
                    contract_scheme=entry.contract_scheme,
                    sig_scheme=entry.sig_scheme,
                    sig_hex=entry.sig_hex,
                )

                fresh_imported_paths: set[tuple[Path, str | None]] = set()
                for loc in entry.locations:
                    witness_obj = _decode_manifest_witness_json(loc.witness_json)
                    obs = Observation(
                        location=Location(uri=loc.uri, member=loc.member),
                        identity=identity,
                        access_driver=loc.access_driver,
                        witness=witness_obj,
                        basis=loc.basis,
                        site=loc.site,
                        scope_key=loc.scope_key,
                        mount_instance_id=loc.mount_instance_id,
                        observed_at_ns=loc.observed_at_ns,
                    )
                    ledger.record_observation(obs, repr_kind=None)
                    if isinstance(witness_obj, (FileStat, DirTreeStat, MemberOf)):
                        loc_path, _ = resolve_uri_to_path(
                            loc.uri, mounts=effective_mounts
                        )
                        if loc_path is not None and loc_path.exists():
                            try:
                                resolved_loc_p = loc_path.expanduser().resolve()
                                if witness_obj.check(resolved_loc_p) is Freshness.FRESH:
                                    fresh_imported_paths.add(
                                        (resolved_loc_p, loc.member)
                                    )
                            except OSError:
                                pass

                if (roots or mounts is not None or ledger.mounts) and (
                    anchor_digest.kind is not Kind.HISTORICAL
                ):
                    for cand_path, cand_member in _discover_candidate_paths(
                        entry,
                        mount_map=effective_mounts,
                        reobserve_roots=roots,
                    ):
                        if (cand_path, cand_member) in fresh_imported_paths:
                            continue
                        cache_key = (cand_path, cand_member, str(anchor_digest))
                        cached_witness = verified_cache.get(cache_key)
                        if cached_witness is not None:
                            matched = True
                            live_witness: Witness | None = cached_witness
                        else:
                            matched, _actual, live_witness = verify_path_digest(
                                cand_path,
                                anchor_digest,
                                driver=entry.driver,
                                member=cand_member,
                                ledger=ledger,
                            )
                            if matched and live_witness is not None:
                                verified_cache[cache_key] = live_witness
                        if matched and live_witness is not None:
                            live_obs = _build_path_observation(
                                cand_path,
                                identity=identity,
                                driver=entry.driver,
                                witness=live_witness,
                                basis="computed",
                                member=cand_member,
                                site_context=site_ctx,
                                mounts=effective_mounts,
                                observed_at_ns=time.time_ns(),
                            )
                            ledger.record_observation(
                                live_obs,
                                repr_kind=None,
                            )
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise

    return manifest


def rebuild_store_from_manifests(
    db_path: str | Path,
    snapshot_dirs: Sequence[str | Path],
    *,
    mounts: Sequence[Mount] | Mapping[str, str | Path | Mount] | None = None,
    reobserve_roots: Sequence[str | Path] | None = None,
) -> list[RunManifest]:
    """Reconstruct or populate a v2 `StoreLedger` at `db_path` from `consist.json` snapshots."""
    discovered_files: list[Path] = []
    seen_files: set[Path] = set()
    search_roots: list[Path] = []

    for raw_dir in snapshot_dirs:
        root_dir = Path(raw_dir).expanduser().resolve()
        if not root_dir.exists():
            continue
        if root_dir.is_file():
            if root_dir not in seen_files:
                seen_files.add(root_dir)
                discovered_files.append(root_dir)
            if root_dir.parent not in search_roots:
                search_roots.append(root_dir.parent)
            continue

        search_roots.append(root_dir)
        candidates = sorted(
            {
                *root_dir.rglob("consist.json"),
                *root_dir.rglob("consist_runs/*.json"),
            }
        )
        for cand in candidates:
            resolved_cand = cand.resolve()
            if resolved_cand not in seen_files and resolved_cand.is_file():
                seen_files.add(resolved_cand)
                discovered_files.append(resolved_cand)

    effective_reobserve = (
        list(reobserve_roots) if reobserve_roots is not None else search_roots
    )
    imported_by_run_id: dict[str, RunManifest] = {}
    verified_path_cache: dict[tuple[Path, str | None, str], Witness] = {}

    with StoreLedger(Path(db_path).expanduser(), mounts=mounts) as ledger:
        for json_file in discovered_files:
            loaded = load_run_manifest(json_file)
            run_id = str(loaded.run.get("id") or "")
            if not run_id or run_id in imported_by_run_id:
                continue
            imported = import_run_manifest(
                ledger,
                loaded,
                reobserve_roots=[json_file.parent, *effective_reobserve],
                mounts=mounts,
                _verified_path_cache=verified_path_cache,
            )
            imported_by_run_id[run_id] = imported

    return list(imported_by_run_id.values())


__all__ = [
    "SCHEMA_VERSION",
    "ManifestLocationEntry",
    "ManifestArtifactEntry",
    "RunManifest",
    "export_run_manifest",
    "write_run_manifest",
    "load_run_manifest",
    "import_run_manifest",
    "rebuild_store_from_manifests",
]
