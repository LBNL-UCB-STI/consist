"""Internal helpers for run/trace input and output resolution."""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
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

from consist.core.error_messages import format_problem_cause_fix
from consist.models.artifact import Artifact
from consist.models.run import Run, RunResult, resolve_run_result_output
from consist.types import ArtifactRef, RunInputRef

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


UTC = timezone.utc


def resolve_input_reference(
    tracker: "Tracker", ref: RunInputRef, key: Optional[str]
) -> ArtifactRef:
    return resolve_input_reference_configured(
        tracker,
        ref,
        key,
        type_label="inputs",
        missing_path_error=(
            "Problem: Input path does not exist: {path!s}\n"
            "Cause: The provided input path is missing or not accessible.\n"
            "Fix: Pass an existing file/directory path or a valid Artifact/RunResult "
            "reference."
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
    if isinstance(ref, RunResult):
        return resolve_run_result_output(ref)

    if isinstance(ref, str):
        if string_ref_resolver is not None:
            resolved_ref = string_ref_resolver(ref)
            if resolved_ref is not None:
                return resolved_ref
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
    ref_str = str(ref)
    if isinstance(ref, str) and "://" in ref_str:
        return Path(tracker.resolve_uri(ref_str))
    ref_path = Path(ref_str)
    if not ref_path.is_absolute():
        return base_dir / ref_path
    return ref_path
