from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel

from consist.core.identity import IdentityManager
from consist.types import FacetLike


@dataclass(frozen=True)
class FlattenedFacetValue:
    key_path: str
    value_type: Literal["null", "bool", "int", "float", "str", "json"]
    value_str: Optional[str] = None
    value_num: Optional[float] = None
    value_bool: Optional[bool] = None
    value_json: Optional[Any] = None


def infer_schema_name_from_facet(
    facet: Optional[FacetLike], *, fallback: Optional[str] = None
) -> Optional[str]:
    """Infer schema name from a facet payload when available."""
    if isinstance(facet, BaseModel):
        return facet.__class__.__name__
    return fallback


def normalize_facet_like(
    *,
    identity: IdentityManager,
    facet: FacetLike,
) -> Dict[str, Any]:
    """Normalize a facet payload (Pydantic model or mapping) to JSON-safe dict."""
    if isinstance(facet, BaseModel):
        normalized = (
            facet.model_dump(mode="json")
            if hasattr(facet, "model_dump")
            else facet.model_dump()
        )
        return identity.normalize_json(normalized)
    return identity.normalize_json(facet)


def canonical_facet_json_and_id(
    *,
    identity: IdentityManager,
    facet_dict: Dict[str, Any],
) -> tuple[str, str]:
    """Return canonical facet JSON and stable SHA256 facet id."""
    canonical = identity.canonical_json_str(facet_dict)
    facet_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return canonical, facet_id


def flatten_facet_values(
    *,
    facet_dict: Dict[str, Any],
    include_json_leaves: bool,
) -> List[FlattenedFacetValue]:
    """
    Flatten a facet mapping into typed rows using escaped dotted key paths.

    Keys containing literal dots are escaped as ``\\.``.
    """
    rows: List[FlattenedFacetValue] = []

    def walk(prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                key_part = str(k).replace(".", "\\.")
                new_prefix = f"{prefix}.{key_part}" if prefix else key_part
                walk(new_prefix, v)
            return

        if value is None:
            rows.append(FlattenedFacetValue(key_path=prefix, value_type="null"))
            return

        if isinstance(value, bool):
            rows.append(
                FlattenedFacetValue(
                    key_path=prefix,
                    value_type="bool",
                    value_bool=value,
                )
            )
            return

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            rows.append(
                FlattenedFacetValue(
                    key_path=prefix,
                    value_type="float" if isinstance(value, float) else "int",
                    value_num=float(value),
                )
            )
            return

        if isinstance(value, str):
            rows.append(
                FlattenedFacetValue(
                    key_path=prefix,
                    value_type="str",
                    value_str=value,
                )
            )
            return

        if include_json_leaves:
            rows.append(
                FlattenedFacetValue(
                    key_path=prefix,
                    value_type="json",
                    value_json=value,
                )
            )

    walk("", facet_dict)
    return [row for row in rows if row.key_path]
