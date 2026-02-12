from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from consist.core.facet_common import (
    canonical_facet_json_and_id,
    flatten_facet_values,
    infer_schema_name_from_facet,
    normalize_facet_like,
)
from consist.core.identity import IdentityManager
from consist.core.persistence import DatabaseManager
from consist.models.artifact import Artifact
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV
from consist.types import FacetLike


class ArtifactFacetManager:
    """
    Persist and index artifact facets.

    Artifact facets are optional structured metadata payloads attached to
    individual artifacts and persisted separately from artifact key/path naming.
    """

    def __init__(
        self,
        *,
        db: Optional[DatabaseManager],
        identity: IdentityManager,
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
        return normalize_facet_like(identity=self._identity, facet=facet)

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
    ) -> None:
        if not self._db:
            return

        canonical, facet_id = canonical_facet_json_and_id(
            identity=self._identity,
            facet_dict=facet_dict,
        )
        facet_bytes = len(canonical.encode("utf-8"))
        if facet_bytes > max_facet_bytes:
            logging.info(
                "[Consist] Skipping artifact facet persistence for artifact %s "
                "(facet too large: %d bytes).",
                getattr(artifact, "id", None),
                facet_bytes,
            )
            return
        self._db.upsert_artifact_facet(
            ArtifactFacet(
                id=facet_id,
                namespace=namespace,
                schema_name=schema_name,
                schema_version=(
                    str(schema_version) if schema_version is not None else None
                ),
                facet_json=facet_dict,
            )
        )

        updates: Dict[str, Any] = {
            "artifact_facet_id": facet_id,
            "artifact_facet_namespace": namespace,
            "artifact_facet_schema": schema_name,
        }
        if schema_version is not None:
            updates["artifact_facet_schema_version"] = schema_version

        if artifact.meta is None:
            artifact.meta = {}
        artifact.meta.update(updates)
        self._db.update_artifact_meta(artifact, updates)

        if not index_kv:
            return

        rows = self.flatten_facet_to_kv_rows(
            artifact_id=artifact.id,
            facet_id=facet_id,
            namespace=namespace,
            facet_dict=facet_dict,
        )
        if len(rows) > max_kv_rows:
            logging.info(
                "[Consist] Skipping artifact facet KV indexing for artifact %s "
                "(too many keys: %d).",
                getattr(artifact, "id", None),
                len(rows),
            )
            return
        self._db.insert_artifact_kv_bulk(rows)

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
