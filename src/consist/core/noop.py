from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
import inspect
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
    cast,
)
import uuid

from consist.core.coupler import CouplerSchemaBase, DeclaredOutput
from consist.core.validation import validate_artifact_key
from consist.protocols import TrackerLike

if TYPE_CHECKING:
    from consist.core.coupler import Coupler
from consist.models.run import Run

SchemaT = TypeVar("SchemaT", bound=CouplerSchemaBase)


@dataclass
class NoopArtifact:
    """
    Minimal artifact placeholder for noop mode.

    Provides stable accessors for `path` and `get_path()` so integrations can
    rely on a consistent artifact interface when Consist is disabled.
    """

    key: str
    path: Path
    uri: str
    meta: Dict[str, Any] = field(default_factory=dict)
    driver: str = "file"
    run_id: Optional[str] = None

    def get_path(self) -> Path:
        return self.path


@dataclass
class NoopRunResult:
    """
    Minimal RunResult-compatible payload for noop mode.
    """

    outputs: Dict[str, NoopArtifact] = field(default_factory=dict)
    cache_hit: bool = False
    run: Optional[Run] = None

    @property
    def output(self) -> Optional[NoopArtifact]:
        if not self.outputs:
            return None
        return next(iter(self.outputs.values()))

    def as_dict(self) -> Dict[str, NoopArtifact]:
        return dict(self.outputs)


class NoopCoupler:
    """
    Coupler-compatible helper for noop mode.
    """

    def __init__(self) -> None:
        self._artifacts: Dict[str, Any] = {}
        self._declared_outputs: Dict[str, DeclaredOutput] = {}

    def set(self, key: str, artifact: Any) -> Any:
        validate_artifact_key(key)
        self._artifacts[key] = artifact
        return artifact

    def set_from_artifact(self, key: str, value: Any) -> Any:
        """
        Set an artifact, accepting both Artifact objects and artifact-like values.

        This method is useful when integrating with optional dependencies where you may
        receive either real Artifacts or artifact-like objects with .path/.uri properties.

        All forms are stored in the coupler and can be retrieved with get() or require().
        """
        self._artifacts[key] = value
        return value

    def update(
        self, artifacts: Optional[Dict[str, Any]] = None, /, **kwargs: Any
    ) -> None:
        if artifacts:
            self._artifacts.update(artifacts)
        if kwargs:
            self._artifacts.update(kwargs)

    def get(self, key: str) -> Optional[Any]:
        validate_artifact_key(key)
        return self._artifacts.get(key)

    def require(self, key: str) -> Any:
        artifact = self.get(key)
        if artifact is None:
            available = ", ".join(sorted(self._artifacts.keys())) or "<none>"
            raise KeyError(
                f"Coupler missing key={key!r}. Available keys: {available}. "
                f"Did you forget to call coupler.set({key!r}, ...)?"
            )
        return artifact

    def pop(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        return self._artifacts.pop(key, default)

    def keys(self) -> Sequence[str]:
        return list(self._artifacts.keys())

    def items(self) -> Sequence[tuple[str, Any]]:
        return list(self._artifacts.items())

    def values(self) -> Sequence[Any]:
        return list(self._artifacts.values())

    def __contains__(self, key: object) -> bool:
        return key in self._artifacts

    def __getitem__(self, key: str) -> Any:
        return self.require(key)

    def __setitem__(self, key: str, artifact: Any) -> None:
        self.set(key, artifact)

    def path(self, key: str, *, required: bool = True) -> Optional[Path]:
        artifact = self.require(key) if required else self.get(key)
        if artifact is None:
            return None
        if hasattr(artifact, "get_path"):
            return Path(artifact.get_path())
        if hasattr(artifact, "path"):
            return Path(artifact.path)
        if hasattr(artifact, "uri"):
            return Path(artifact.uri)
        return None

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        # NOTE: Keep in sync with SchemaValidatingCoupler.declare_outputs.
        if not names:
            return
        if isinstance(required, Mapping):
            required_map = {str(k): bool(v) for k, v in required.items()}
            default_required = False
        else:
            required_map = {}
            default_required = bool(required)
        description_map = {str(k): v for k, v in (description or {}).items()}
        for name in names:
            if not isinstance(name, str):
                raise TypeError("Coupler output names must be strings.")
            key = name.strip()
            if not key:
                raise ValueError("Coupler output names cannot be empty.")
            validate_artifact_key(key)
            entry = self._declared_outputs.get(key, DeclaredOutput())
            entry.required = entry.required or required_map.get(key, default_required)
            if key in description_map:
                entry.description = description_map[key]
            self._declared_outputs[key] = entry

    def missing_declared_outputs(self) -> list[str]:
        return sorted(
            [
                key
                for key, entry in self._declared_outputs.items()
                if entry.required
                and (key not in self._artifacts or self._artifacts[key] is None)
            ]
        )

    def collect_by_keys(
        self, artifacts: Mapping[str, Any], *keys: str, prefix: str = ""
    ) -> Dict[str, Any]:
        if not isinstance(artifacts, Mapping):
            raise TypeError("collect_by_keys expects a mapping of artifacts.")
        collected: Dict[str, Any] = {}
        for key in keys:
            if not isinstance(key, str):
                raise TypeError("collect_by_keys keys must be strings.")
            if key not in artifacts:
                raise KeyError(f"Missing artifact for key {key!r}.")
            coupler_key = f"{prefix}{key}"
            artifact = artifacts[key]
            self.set(coupler_key, artifact)
            collected[coupler_key] = artifact
        return collected


class NoopRunContext:
    """
    Minimal run context for noop mode that returns artifact placeholders.
    """

    def __init__(self) -> None:
        self._artifacts: Dict[str, NoopArtifact] = {}

    def log_artifact(
        self,
        path: Any,
        key: Optional[str] = None,
        content_hash: Optional[str] = None,
        force_hash_override: bool = False,
        validate_content_hash: bool = False,
        **meta: Any,
    ) -> NoopArtifact:
        _ = content_hash
        _ = force_hash_override
        _ = validate_content_hash
        if key is None:
            if isinstance(path, NoopArtifact):
                key = path.key
            else:
                key = Path(path).stem if path is not None else "artifact"
        artifact = _build_noop_artifact(path, key=key, meta=meta)
        self._artifacts[key] = artifact
        return artifact

    def log_input(
        self, path: Any, key: Optional[str] = None, **meta: Any
    ) -> NoopArtifact:
        return self.log_artifact(path, key=key, **meta)

    def log_output(
        self, path: Any, key: Optional[str] = None, **meta: Any
    ) -> NoopArtifact:
        return self.log_artifact(path, key=key, **meta)

    def log_artifacts(
        self, outputs: Mapping[str, Any], **meta: Any
    ) -> Dict[str, NoopArtifact]:
        artifacts: Dict[str, NoopArtifact] = {}
        for key, value in outputs.items():
            artifacts[key] = self.log_artifact(value, key=key, **meta)
        return artifacts


class NoopScenarioContext:
    """
    Scenario-like context that executes without provenance tracking.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        self.name = name
        self.kwargs = kwargs
        self.coupler = NoopCoupler()
        self._inputs: Dict[str, NoopArtifact] = {}
        self._output_base_dir = Path(kwargs.get("output_base_dir") or Path.cwd())

    @property
    def run_id(self) -> str:
        return self.name

    @property
    def inputs(self) -> Mapping[str, NoopArtifact]:
        return dict(self._inputs)

    def add_input(self, path: Any, key: str, **kwargs: Any) -> NoopArtifact:
        artifact = _build_noop_artifact(path, key=key, meta=kwargs)
        self._inputs[key] = artifact
        return artifact

    def declare_outputs(
        self,
        *names: str,
        required: bool | Mapping[str, bool] = False,
        description: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.coupler.declare_outputs(*names, required=required, description=description)

    def collect_by_keys(
        self, artifacts: Mapping[str, Any], *keys: str, prefix: str = ""
    ) -> Dict[str, Any]:
        return self.coupler.collect_by_keys(artifacts, *keys, prefix=prefix)

    def coupler_schema(self, schema: Type[SchemaT]) -> SchemaT:
        return schema(cast("Coupler", self.coupler))

    def run(
        self,
        fn: Optional[Any] = None,
        name: Optional[str] = None,
        *,
        run_id: Optional[str] = None,
        outputs: Optional[Sequence[str]] = None,
        output_paths: Optional[Mapping[str, Any]] = None,
        runtime_kwargs: Optional[Dict[str, Any]] = None,
        inject_context: bool | str = False,
        **kwargs: Any,
    ) -> NoopRunResult:
        return _run_noop_step(
            fn=fn,
            name=name,
            run_id=run_id,
            outputs=outputs,
            output_paths=output_paths,
            runtime_kwargs=runtime_kwargs,
            inject_context=inject_context,
            default_name=self.name,
            on_outputs=self.coupler.update,
            output_base_dir=self._output_base_dir,
        )

    @contextmanager
    def trace(self, name: str, **kwargs: Any) -> Iterator[NoopRunContext]:
        yield NoopRunContext()


class NoopTracker(TrackerLike):
    """
    Tracker-compatible noop implementation with parity-focused validation.
    """

    def __init__(self, *, output_base_dir: Optional[Path] = None) -> None:
        self._warned = False
        self._output_base_dir = output_base_dir or Path.cwd()

    def _warn_disabled(self) -> None:
        if not self._warned:
            logging.warning(
                "[Consist] Provenance tracking disabled; using noop tracker."
            )
            self._warned = True

    def log_output(
        self, path: Any, key: Optional[str] = None, **metadata: Any
    ) -> NoopArtifact:
        self._warn_disabled()
        return NoopRunContext().log_output(path, key=key, **metadata)

    def log_input(
        self, path: Any, key: Optional[str] = None, **metadata: Any
    ) -> NoopArtifact:
        self._warn_disabled()
        return NoopRunContext().log_input(path, key=key, **metadata)

    def log_artifacts(
        self, outputs: Mapping[str, Any], **metadata: Any
    ) -> Dict[str, NoopArtifact]:
        self._warn_disabled()
        return NoopRunContext().log_artifacts(outputs, **metadata)

    @contextmanager
    def scenario(self, name: str, **kwargs: Any) -> Iterator[NoopScenarioContext]:
        self._warn_disabled()
        yield NoopScenarioContext(name, **kwargs)

    def run(
        self,
        fn: Optional[Any] = None,
        name: Optional[str] = None,
        *,
        run_id: Optional[str] = None,
        outputs: Optional[Sequence[str]] = None,
        output_paths: Optional[Mapping[str, Any]] = None,
        runtime_kwargs: Optional[Dict[str, Any]] = None,
        inject_context: bool | str = False,
        **kwargs: Any,
    ) -> NoopRunResult:
        self._warn_disabled()
        return _run_noop_step(
            fn=fn,
            name=name,
            run_id=run_id,
            outputs=outputs,
            output_paths=output_paths,
            runtime_kwargs=runtime_kwargs,
            inject_context=inject_context,
            output_base_dir=self._output_base_dir,
        )


def _build_noop_artifact(
    value: Any, *, key: str, meta: Optional[Dict[str, Any]] = None
) -> NoopArtifact:
    validate_artifact_key(key)
    if isinstance(value, NoopArtifact):
        return value
    if hasattr(value, "path"):
        try:
            path = Path(value.path)
            return NoopArtifact(
                key=key,
                path=path,
                uri=str(path),
                meta=dict(meta or {}),
            )
        except Exception:
            pass
    path = Path(value) if value is not None else Path(key)
    return NoopArtifact(
        key=key,
        path=path,
        uri=str(path),
        meta=dict(meta or {}),
    )


def _run_noop_step(
    *,
    fn: Optional[Any],
    name: Optional[str],
    run_id: Optional[str],
    outputs: Optional[Sequence[str]],
    output_paths: Optional[Mapping[str, Any]],
    runtime_kwargs: Optional[Dict[str, Any]],
    inject_context: bool | str,
    default_name: Optional[str] = None,
    on_outputs: Optional[Any] = None,
    output_base_dir: Optional[Path] = None,
) -> NoopRunResult:
    if fn is None and name is None:
        raise ValueError("Noop run requires name when fn is None.")
    resolved_name = name or getattr(fn, "__name__", "run")
    base = default_name or resolved_name
    resolved_run_id = run_id or f"{base}_{resolved_name}_{uuid.uuid4().hex[:8]}"

    call_kwargs: Dict[str, Any] = dict(runtime_kwargs or {})
    required_runtime = getattr(fn, "__consist_runtime_required__", ()) if fn else ()
    if required_runtime:
        missing = [name for name in required_runtime if name not in call_kwargs]
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise ValueError(
                f"Missing runtime_kwargs for {resolved_name!r}: {missing_list}. "
                "Provide them via runtime_kwargs={...} or remove "
                "@consist.require_runtime_kwargs."
            )

    if fn is not None:
        sig = inspect.signature(fn)
        params = sig.parameters
        has_var_kw = any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
        )
        if inject_context:
            ctx_name = (
                inject_context if isinstance(inject_context, str) else "_consist_ctx"
            )
            if ctx_name in params or has_var_kw:
                call_kwargs[ctx_name] = NoopRunContext()
            else:
                raise ValueError(
                    f"inject_context requested '{ctx_name}', but fn does not accept it."
                )
        try:
            sig.bind_partial(**call_kwargs)
        except TypeError as exc:
            raise TypeError(
                f"Noop run could not bind arguments for {resolved_name!r}: {exc}"
            ) from exc

    result = None
    if fn is not None:
        result = fn(**call_kwargs)

    outputs_map: Dict[str, NoopArtifact] = {}
    if output_paths:
        for key, ref in output_paths.items():
            outputs_map[str(key)] = _build_noop_artifact(ref, key=str(key))

    if outputs and result is not None:
        if isinstance(result, Mapping):
            for key, value in result.items():
                outputs_map[str(key)] = _build_noop_artifact(value, key=str(key))
        elif isinstance(result, (list, tuple)) and len(result) == len(outputs):
            for key, value in zip(outputs, result):
                outputs_map[str(key)] = _build_noop_artifact(value, key=str(key))
        else:
            if len(outputs) == 1:
                outputs_map[str(outputs[0])] = _build_noop_artifact(
                    result, key=str(outputs[0])
                )

    if outputs_map and on_outputs is not None:
        on_outputs(outputs_map)

    return NoopRunResult(
        run=Run(id=resolved_run_id, model_name=resolved_name, status="completed"),
        outputs=outputs_map,
        cache_hit=False,
    )


def _resolve_noop_output_path(ref: Any, base_dir: Path) -> Path:
    if isinstance(ref, NoopArtifact):
        return ref.path
    ref_path = getattr(ref, "path", None)
    if ref_path is not None:
        return Path(ref_path)
    ref_str = str(ref)
    if isinstance(ref, str) and "://" in ref_str:
        return Path(ref_str)
    candidate = Path(ref_str)
    if not candidate.is_absolute():
        return base_dir / candidate
    return candidate
