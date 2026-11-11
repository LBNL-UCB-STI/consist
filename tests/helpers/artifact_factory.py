"""Test helper for constructing synthetic frozen `consist.artifact.Artifact` handles."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
import hashlib
from pathlib import Path
import re
from typing import Any
import uuid

from consist.artifact import Artifact
from consist.digest import Digest, Identity, Kind
from consist.location import (
    FrozenMeta,
    LedgerRef,
    Location,
    Observation,
    ObservationBasis,
)

_HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")


def make_artifact(
    *,
    id: uuid.UUID | str | None = None,
    key: str,
    container_uri: str | Path | None = None,
    driver: str = "other",
    hash: str | Digest | None = None,
    identity: Identity | None = None,
    table_path: str | None = None,
    array_path: str | None = None,
    member: str | None = None,
    run_id: str | None = None,
    parent_artifact_id: uuid.UUID | str | None = None,
    schema_id: str | None = None,
    created_at: datetime | str | None = None,
    meta: Mapping[str, Any] | None = None,
    origin: LedgerRef | None = None,
    hints: Sequence[Observation] | None = None,
    basis: ObservationBasis = "declared",
) -> Artifact:
    """Build a synthetic frozen `Artifact` for unit tests without touching disk."""
    art_id = (
        id
        if isinstance(id, uuid.UUID)
        else (uuid.UUID(str(id)) if id is not None else uuid.uuid4())
    )
    meta_dict = dict(meta) if meta is not None else {}
    has_manifest_meta = (
        meta_dict.get("directory_manifest") is not None
        or meta_dict.get("file_bundle_manifest") is not None
        or meta_dict.get("directory_artifact") is True
        or meta_dict.get("file_bundle_artifact") is True
    )
    trusted_file_semantics = {
        "version": 1,
        "algorithm": "sha256",
        "kind": "file",
        "digest_contract": "raw_file_bytes",
        "source": "computed_full",
    }
    has_untrusted_semantics = (
        "hash_semantics" in meta_dict
        and meta_dict.get("hash_semantics") != trusted_file_semantics
    )
    default_scheme = (
        "sha256:manifest-v1"
        if has_manifest_meta
        else ("legacy-v1:unverified" if has_untrusted_semantics else "sha256:file")
    )

    if identity is not None:
        eff_identity = identity
    elif isinstance(hash, Digest):
        claims = () if hash.kind is Kind.HISTORICAL else (hash,)
        eff_identity = Identity(anchor=hash, claims=claims)
    elif isinstance(hash, str) and ":" in hash:
        parsed = Digest.parse(hash)
        claims = () if parsed.kind is Kind.HISTORICAL else (parsed,)
        eff_identity = Identity(anchor=parsed, claims=claims)
    elif isinstance(hash, str) and _HEX64_RE.fullmatch(hash.strip()):
        d = Digest(default_scheme, hash.strip().lower())
        claims = () if d.kind is Kind.HISTORICAL else (d,)
        eff_identity = Identity(anchor=d, claims=claims)
    elif isinstance(hash, str) and hash:
        derived_hex = hashlib.sha256(hash.encode("utf-8")).hexdigest()
        d = Digest(default_scheme, derived_hex)
        claims = () if d.kind is Kind.HISTORICAL else (d,)
        eff_identity = Identity(anchor=d, claims=claims)
    else:
        eff_identity = Identity(anchor=Digest("legacy-v1:unverified", "0" * 64))

    eff_member = (
        member
        if member is not None
        else (table_path if table_path is not None else array_path)
    )

    obs_list: list[Observation] = list(hints) if hints is not None else []
    if container_uri is not None and str(container_uri):
        uri_str = (
            Path(container_uri).as_posix()
            if isinstance(container_uri, Path)
            else str(container_uri)
        )
        if (
            not uri_str.startswith(("file://", "workspace://", "db://"))
            and "://" not in uri_str
        ):
            p = Path(uri_str)
            if p.is_absolute():
                uri_str = p.as_uri()
        obs_list.append(
            Observation(
                location=Location(uri=uri_str, member=eff_member),
                identity=eff_identity,
                access_driver=driver,
                basis=basis,
            )
        )

    if meta_dict.get("is_ingested") is True:
        tbl_name = str(meta_dict.get("dlt_table_name") or key)
        db_uri = f"db://global_tables/{tbl_name}"
        if not any(o.location.uri == db_uri for o in obs_list):
            obs_list.append(
                Observation(
                    location=Location(uri=db_uri),
                    identity=eff_identity,
                    access_driver="duckdb",
                    basis="declared",
                )
            )

    return Artifact(
        id=art_id,
        key=key,
        driver=driver,
        identity=eff_identity,
        origin=origin,
        run_id=run_id,
        parent_artifact_id=parent_artifact_id,  # type: ignore[arg-type]
        schema_id=schema_id,
        created_at=created_at,  # type: ignore[arg-type]
        meta=FrozenMeta(meta_dict),
        hints=tuple(obs_list),
    )
