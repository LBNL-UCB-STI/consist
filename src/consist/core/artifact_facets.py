from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from consist.artifact import Artifact
from consist.core.facet_common import (
    canonical_facet_json_and_id,
    flatten_facet_values,
    infer_schema_name_from_facet,
    normalize_facet_like,
)
from consist.models.artifact_kv import ArtifactKV
from consist.types import FacetLike

if TYPE_CHECKING:
    from consist.core.identity import IdentityManager
    from consist.core.persistence import DatabaseManager


@dataclass(frozen=True)
class PreparedArtifactFacetBundle:
    facet_id: str
    meta_updates: Dict[str, Any]
    index_kv: bool
    kv_rows: Optional[List[ArtifactKV]] = None


class ArtifactFacetManager:
    """
    Persist and index artifact facets.

    Artifact facets are optional structured metadata payloads attached to
    individual artifacts and indexed in ``StoreLedger`` (`main.artifact_kv`).
    """

    def __init__(
        self,
        *,
        db: Optional[DatabaseManager],
        identity: Optional[IdentityManager] = None,
    ) -> None:
        self._db = db
        self._identity = identity

    def infer_schema_name(self, facet: Optional[FacetLike]) -> Optional[str]:
        return infer_schema_name_from_facet(facet)

    def resolve_facet_dict(
        self, facet: Optional[FacetLike]
    ) -> Optional[Dict[str, Any]]:
        if facet is None:
            return None
        return normalize_facet_like(facet=facet)

    def prepare_facet_bundle(
        self,
        *,
        artifact: Artifact,
        namespace: Optional[str],
        facet_dict: Dict[str, Any],
        schema_name: Optional[str],
        schema_version: Optional[Union[str, int]],
        index_kv: bool,
        max_facet_bytes: int = 16_384,
        max_kv_rows: int = 500,
    ) -> Optional[PreparedArtifactFacetBundle]:
        canonical, facet_id = canonical_facet_json_and_id(
            facet_dict=facet_dict,
        )
        facet_bytes = len(canonical.encode("utf-8"))
        if facet_bytes > max_facet_bytes:
            logging.info(
                "[Consist] Skipping artifact facet persistence for artifact %s "
                "(facet too large: %d bytes).",
                artifact.id,
                facet_bytes,
            )
            return None

        updates: Dict[str, Any] = {
            "facet": facet_dict,
            "artifact_facet_id": facet_id,
            "artifact_facet_namespace": namespace,
            "facet_namespace": namespace,
            "artifact_facet_schema": schema_name,
        }
        if schema_version is not None:
            updates["artifact_facet_schema_version"] = schema_version
            updates["facet_schema_version"] = str(schema_version)

        effective_index_kv = bool(index_kv)
        kv_rows: Optional[List[ArtifactKV]] = None
        if effective_index_kv:
            kv_rows = self.flatten_facet_to_kv_rows(
                artifact_id=artifact.id,
                facet_id=facet_id,
                namespace=namespace,
                facet_dict=facet_dict,
            )
            if len(kv_rows) > max_kv_rows:
                logging.info(
                    "[Consist] Skipping artifact facet KV indexing for artifact %s "
                    "(too many keys: %d).",
                    artifact.id,
                    len(kv_rows),
                )
                effective_index_kv = False
                kv_rows = None

        if not effective_index_kv:
            updates["artifact_facet_index"] = False

        return PreparedArtifactFacetBundle(
            facet_id=facet_id,
            meta_updates=updates,
            index_kv=effective_index_kv,
            kv_rows=kv_rows,
        )

    def persist_facet(
        self,
        *,
        artifact: Artifact,
        namespace: Optional[str],
        facet_dict: Dict[str, Any],
        schema_name: Optional[str],
        schema_version: Optional[Union[str, int]],
        index_kv: bool,
        max_facet_bytes: int = 16_384,
        max_kv_rows: int = 500,
    ) -> Artifact:
        bundle = self.prepare_facet_bundle(
            artifact=artifact,
            namespace=namespace,
            facet_dict=facet_dict,
            schema_name=schema_name,
            schema_version=schema_version,
            index_kv=index_kv,
            max_facet_bytes=max_facet_bytes,
            max_kv_rows=max_kv_rows,
        )
        if bundle is None:
            return artifact

        updated = artifact.with_meta(bundle.meta_updates, merge=True)
        if self._db is not None:
            persisted = self._db.update_artifact_meta(
                updated,
                bundle.meta_updates,
                merge=True,
                facet_index=bundle.index_kv,
            )
            if persisted is not None:
                return persisted
        return updated

    def flatten_facet_to_kv_rows(
        self,
        *,
        artifact_id: Any,
        facet_id: str,
        namespace: Optional[str],
        facet_dict: Dict[str, Any],
    ) -> List[ArtifactKV]:
        flattened = flatten_facet_values(
            facet_dict=facet_dict,
            include_json_leaves=False,
        )
        rows: List[ArtifactKV] = []
        for row in flattened:
            rows.append(
                ArtifactKV(
                    artifact_id=artifact_id,
                    facet_id=facet_id,
                    namespace=namespace,
                    key_path=row.key_path,
                    value_type=row.value_type,
                    value_str=row.value_str,
                    value_num=row.value_num,
                    value_bool=row.value_bool,
                )
            )
        return rows
