"""Internal helpers for run/trace input and output resolution."""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from datetime import datetime, timezone
import importlib
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Union,
    cast,
)

from consist.artifact import Artifact as V2Artifact
from consist.core.error_messages import format_problem_cause_fix
from consist.models.artifact import Artifact, set_tracker_ref
from consist.models.run import Run, RunResult, resolve_run_result_output
from consist.types import ArtifactRef, RunInputRef

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


UTC = timezone.utc


def convert_v2_artifact_to_legacy(
    tracker: "Tracker",
    v2_art: V2Artifact,
    key_override: Optional[str] = None,
) -> Artifact:
    """Bridge a ``consist.artifact.Artifact`` into a ``consist.models.artifact.Artifact``."""
    effective_key = key_override or v2_art.key or "artifact"
    located = v2_art.locate(mounts=tracker.mounts or None)
    resolved_abs: Optional[str] = str(located) if located is not None else None
    hint_uri: Optional[str] = (
        v2_art.hints[0].location.uri
        if (v2_art.hints and v2_art.hints[0].location.uri)
        else None
    )
    if hint_uri is not None and not hint_uri.startswith("file://"):
        container_uri = hint_uri
    elif resolved_abs is not None:
        container_uri = tracker.fs.virtualize_path(resolved_abs)
    elif hint_uri is not None:
        container_uri = hint_uri
    else:
        container_uri = f"inputs://{effective_key}"

    meta_dict: Dict[str, Any] = dict(v2_art.meta)
    anchor = v2_art.identity.anchor
    meta_dict["v2_identity_anchor"] = str(anchor)
    if not v2_art.identity.verifiable:
        meta_dict["v2_historical_unverified"] = True
        meta_dict.pop("hash_semantics", None)
        meta_dict.pop("content_identity", None)
    else:
        meta_dict["v2_historical_unverified"] = False
        if (
            anchor.scheme.startswith("sha256:manifest-v1")
            or anchor.scheme == "sha256:dir_manifest"
        ):
            meta_dict["directory_artifact"] = True
            meta_dict["directory_manifest"] = {
                "version": 1,
                "tree_hash": anchor.hex,
            }
            meta_dict["content_identity"] = f"sha256:manifest-v1:{anchor.hex}"
        elif anchor.scheme == "sha256:file":
            meta_dict["hash_semantics"] = {
                "version": 1,
                "algorithm": "sha256",
                "kind": "file",
                "digest_contract": "raw_file_bytes",
                "source": "computed_full",
            }
            meta_dict["content_identity"] = f"sha256:file:{anchor.hex}"
        else:
            meta_dict["content_identity"] = str(anchor)

    legacy = Artifact(
        id=v2_art.id,
        key=effective_key,
        container_uri=container_uri,
        driver=v2_art.driver,
        hash=anchor.hex,
        run_id=v2_art.run_id,
        meta=meta_dict,
        abs_path=resolved_abs,
    )
    set_tracker_ref(legacy, tracker)
    return legacy


@dataclass(frozen=True, slots=True)
class InputBindingRole:
    """Caller-visible role for one resolved ordinary input."""

    kind: str
    role: str | int
    input_index: int


def build_input_binding_roles(
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
    depends_on: Optional[List[RunInputRef]],
) -> list[InputBindingRole]:
    """Build canonical non-hashing role evidence for invocation inputs."""
    roles: list[InputBindingRole] = []
    input_index = 0
    if inputs is not None:
        if isinstance(inputs, MappingABC):
            named_roles: list[InputBindingRole] = []
            for key in inputs:
                if not isinstance(key, str):
                    raise TypeError(
                        f"inputs mapping keys must be str (got {type(key)})."
                    )
                named_roles.append(
                    InputBindingRole(kind="named", role=key, input_index=input_index)
                )
                input_index += 1
            roles.extend(sorted(named_roles, key=lambda entry: str(entry.role)))
        else:
            for ordinal, _ in enumerate(inputs):
                roles.append(
                    InputBindingRole(
                        kind="positional", role=ordinal, input_index=input_index
                    )
                )
                input_index += 1

    if depends_on:
        for ordinal, _ in enumerate(depends_on):
            roles.append(
                InputBindingRole(
                    kind="dependency", role=ordinal, input_index=input_index
                )
            )
            input_index += 1
    return roles


def resolve_input_reference(
    tracker: "Tracker", ref: RunInputRef, key: Optional[str]
) -> ArtifactRef:
    return resolve_input_reference_configured(
        tracker,
        ref,
        key,
        type_label="inputs",
        missing_path_error=format_problem_cause_fix(
            problem="Input path does not exist: {path!s}",
            cause="The provided input path is missing or not accessible.",
            fix=(
                "Pass an existing file/directory path or a valid Artifact/RunResult "
                "reference."
            ),
        ),
    )


def resolve_input_reference_configured(
    tracker: "Tracker",
    ref: RunInputRef,
    key: Optional[str],
    *,
    type_label: str,
    missing_path_error: str,
    missing_string_error: Optional[str] = None,
    string_ref_resolver: Optional[Callable[[str], Optional[ArtifactRef]]] = None,
) -> ArtifactRef:
    if isinstance(ref, Artifact):
        return ref
    if isinstance(ref, V2Artifact):
        return convert_v2_artifact_to_legacy(tracker, ref, key_override=key)
    if isinstance(ref, RunResult):
        return resolve_run_result_output(ref)

    if isinstance(ref, str):
        if string_ref_resolver is not None:
            resolved_ref = string_ref_resolver(ref)
            if resolved_ref is not None:
                if isinstance(resolved_ref, V2Artifact):
                    return convert_v2_artifact_to_legacy(
                        tracker, resolved_ref, key_override=key
                    )
                return resolved_ref
            else:
                ref_str = ref
        else:
            ref_str = ref
    elif isinstance(ref, Path):
        ref_str = str(ref)
    else:
        raise TypeError(
            format_problem_cause_fix(
                problem=(
                    f"{type_label} must be Artifact, RunResult, Path, or str "
                    f"(got {type(ref)})."
                ),
                cause="An unsupported input reference type was provided.",
                fix=(
                    "Pass Artifact/RunResult references, or filesystem paths as str/Path."
                ),
            )
        )

    resolved = (
        Path(tracker.resolve_uri(ref_str))
        if isinstance(ref, str) and "://" in ref_str
        else Path(ref_str)
    )
    if not resolved.exists():
        if isinstance(ref, str) and missing_string_error is not None:
            raise ValueError(missing_string_error.format(value=ref, path=resolved))
        raise ValueError(missing_path_error.format(path=resolved))
    if key is None:
        return resolved
    return tracker.artifacts.create_artifact(
        resolved, run_id=None, key=key, direction="input"
    )


def resolve_input_ref(
    tracker: "Tracker", ref: RunInputRef, key: Optional[str]
) -> ArtifactRef:
    return resolve_input_reference(tracker, ref, key)


def is_xarray_dataset(value: Any) -> bool:
    try:
        xr = importlib.import_module("xarray")
    except ImportError:
        return False
    return isinstance(value, xr.Dataset)


def write_xarray_dataset(dataset: Any, path: Path) -> None:
    try:
        xr = importlib.import_module("xarray")
    except ImportError as exc:
        raise ImportError("xarray is required to log xarray.Dataset outputs.") from exc
    if not isinstance(dataset, xr.Dataset):
        raise TypeError(f"Expected xarray.Dataset, got {type(dataset)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_zarr(path, mode="w")


def resolve_input_refs(
    tracker: "Tracker",
    inputs: Optional[Union[Mapping[str, RunInputRef], Iterable[RunInputRef]]],
    depends_on: Optional[List[RunInputRef]],
    *,
    include_keyed_artifacts: bool,
) -> tuple[List[ArtifactRef], Dict[str, Artifact]]:
    resolved_inputs: List[ArtifactRef] = []
    input_artifacts_by_key: Dict[str, Artifact] = {}
    if inputs is not None:
        if isinstance(inputs, MappingABC):
            typed_inputs = cast(Mapping[str, RunInputRef], inputs)
            for key, ref in typed_inputs.items():
                if not isinstance(key, str):
                    raise TypeError(
                        f"inputs mapping keys must be str (got {type(key)})."
                    )
                resolved = resolve_input_ref(tracker, ref, key)
                resolved_inputs.append(resolved)
                if include_keyed_artifacts and isinstance(resolved, Artifact):
                    input_artifacts_by_key[key] = resolved
        else:
            for ref in list(inputs):
                resolved_inputs.append(resolve_input_ref(tracker, ref, None))

    if depends_on:
        for dep in depends_on:
            resolved_inputs.append(resolve_input_ref(tracker, dep, None))

    return resolved_inputs, input_artifacts_by_key


def preview_run_artifact_dir(
    tracker: "Tracker",
    *,
    run_id: str,
    model: str,
    description: Optional[str],
    year: Optional[int],
    iteration: Optional[int],
    parent_run_id: Optional[str],
    tags: Optional[List[str]],
) -> Path:
    preview = Run(
        id=run_id,
        model_name=model,
        description=description,
        year=year,
        iteration=iteration,
        parent_run_id=parent_run_id,
        tags=tags or [],
        status="running",
        config_hash=None,
        git_hash=None,
        meta={},
        started_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
    )
    return tracker.run_artifact_dir(preview)


def resolve_output_path(tracker: "Tracker", ref: ArtifactRef, base_dir: Path) -> Path:
    if isinstance(ref, Artifact):
        raw = ref.path or tracker.resolve_uri(ref.container_uri)
        return Path(raw)
    if isinstance(ref, V2Artifact):
        located = ref.locate(mounts=tracker.mounts)
        if located is not None:
            return located
        if ref.primary_location is not None:
            return Path(tracker.resolve_uri(ref.primary_location.uri))
        return ref.path
    ref_str = str(ref)
    if isinstance(ref, str) and "://" in ref_str:
        return Path(tracker.resolve_uri(ref_str))
    ref_path = Path(ref_str)
    if not ref_path.is_absolute():
        return base_dir / ref_path
    return ref_path
