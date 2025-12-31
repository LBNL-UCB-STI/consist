from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Mapping

from consist.models.artifact import Artifact
from consist.types import ArtifactRef


def coerce_input_map(inputs: Mapping[Any, Any]) -> Dict[str, ArtifactRef]:
    coerced: Dict[str, ArtifactRef] = {}
    for key, value in inputs.items():
        if not isinstance(key, str):
            raise TypeError(f"inputs mapping keys must be str (got {type(key)}).")
        if not isinstance(value, (Artifact, str, Path)):
            raise TypeError(
                "inputs mapping values must be Artifact, Path, or str "
                f"(got {type(value)})."
            )
        coerced[key] = value
    return coerced
