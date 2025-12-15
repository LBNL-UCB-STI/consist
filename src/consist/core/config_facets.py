import hashlib
import logging
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel

from consist.core.identity import IdentityManager
from consist.core.persistence import DatabaseManager
from consist.models.config_facet import ConfigFacet
from consist.models.run import Run
from consist.models.run_config_kv import RunConfigKV
from consist.types import FacetLike, HasConsistFacet


class ConfigFacetManager:
    def __init__(self, *, db: Optional[DatabaseManager], identity: IdentityManager) -> None:
        self._db = db
        self._identity = identity

    def infer_schema_name(
        self, raw_config_model: Optional[BaseModel], facet: Optional[FacetLike]
    ) -> str:
        if isinstance(facet, BaseModel):
            return facet.__class__.__name__
        if raw_config_model is not None:
            return raw_config_model.__class__.__name__
        return "dict"

    def resolve_facet_dict(
        self,
        *,
        model: str,
        raw_config_model: Optional[BaseModel],
        facet: Optional[FacetLike],
        run_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if facet is not None:
            if isinstance(facet, BaseModel):
                return (
                    facet.model_dump(mode="json")
                    if hasattr(facet, "model_dump")
                    else facet.model_dump()
                )
            return facet

        if raw_config_model is not None and isinstance(raw_config_model, HasConsistFacet):
            try:
                extracted = raw_config_model.to_consist_facet()
            except Exception as exc:
                logging.warning(
                    "[Consist] to_consist_facet() failed for model=%s run=%s: %s",
                    model,
                    run_id,
                    exc,
                )
                extracted = None
            if extracted is not None:
                if isinstance(extracted, BaseModel):
                    return (
                        extracted.model_dump(mode="json")
                        if hasattr(extracted, "model_dump")
                        else extracted.model_dump()
                    )
                return extracted

        return None

    def persist_facet(
        self,
        *,
        run: Run,
        model: str,
        facet_dict: Dict[str, Any],
        schema_name: str,
        schema_version: Optional[Union[str, int]],
        index_kv: bool,
        max_facet_bytes: int = 16_384,
        max_kv_rows: int = 500,
    ) -> None:
        if not self._db:
            return

        canonical = self._identity.canonical_json_str(facet_dict)
        if len(canonical.encode("utf-8")) > max_facet_bytes:
            logging.info(
                "[Consist] Skipping facet persistence for run %s (facet too large: %d bytes).",
                run.id,
                len(canonical.encode("utf-8")),
            )
            return

        facet_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

        self._db.upsert_config_facet(
            ConfigFacet(
                id=facet_id,
                namespace=model,
                schema_name=schema_name,
                schema_version=str(schema_version) if schema_version is not None else None,
                facet_json=facet_dict,
            )
        )

        run.meta["config_facet_id"] = facet_id
        run.meta["config_facet_namespace"] = model
        run.meta["config_facet_schema"] = schema_name
        if schema_version is not None:
            run.meta["config_facet_schema_version"] = schema_version

        if not index_kv:
            return

        rows = self.flatten_facet_to_kv_rows(
            run_id=run.id,
            facet_id=facet_id,
            namespace=model,
            facet_dict=facet_dict,
        )
        if len(rows) > max_kv_rows:
            logging.info(
                "[Consist] Skipping facet KV indexing for run %s (too many keys: %d).",
                run.id,
                len(rows),
            )
            return
        self._db.insert_run_config_kv_bulk(rows)

    def flatten_facet_to_kv_rows(
        self,
        *,
        run_id: str,
        facet_id: str,
        namespace: str,
        facet_dict: Dict[str, Any],
    ) -> List[RunConfigKV]:
        rows: List[RunConfigKV] = []

        def walk(prefix: str, value: Any) -> None:
            if isinstance(value, dict):
                for k, v in value.items():
                    key_part = str(k).replace(".", "\\.")
                    new_prefix = f"{prefix}.{key_part}" if prefix else key_part
                    walk(new_prefix, v)
                return

            if value is None:
                rows.append(
                    RunConfigKV(
                        run_id=run_id,
                        facet_id=facet_id,
                        namespace=namespace,
                        key=prefix,
                        value_type="null",
                    )
                )
                return
            if isinstance(value, bool):
                rows.append(
                    RunConfigKV(
                        run_id=run_id,
                        facet_id=facet_id,
                        namespace=namespace,
                        key=prefix,
                        value_type="bool",
                        value_bool=value,
                    )
                )
                return
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                rows.append(
                    RunConfigKV(
                        run_id=run_id,
                        facet_id=facet_id,
                        namespace=namespace,
                        key=prefix,
                        value_type="float" if isinstance(value, float) else "int",
                        value_num=float(value),
                    )
                )
                return
            if isinstance(value, str):
                rows.append(
                    RunConfigKV(
                        run_id=run_id,
                        facet_id=facet_id,
                        namespace=namespace,
                        key=prefix,
                        value_type="str",
                        value_str=value,
                    )
                )
                return

            rows.append(
                RunConfigKV(
                    run_id=run_id,
                    facet_id=facet_id,
                    namespace=namespace,
                    key=prefix,
                    value_type="json",
                    value_json=value,
                )
            )

        walk("", facet_dict)
        return [r for r in rows if r.key]

