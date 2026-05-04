from __future__ import annotations

import inspect
import logging
import re
from collections.abc import Mapping as MappingABC
from dataclasses import replace
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    TYPE_CHECKING,
    Union,
    cast,
)

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.core.config_canonicalization import (
    CanonicalConfig,
    CanonicalizationResult,
    ConfigAdapter,
    ConfigAdapterOptions,
    ConfigContribution,
    ConfigPlan,
    validate_config_plan,
)
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.artifact import Artifact
    from consist.core.step_context import StepContext


_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_identifier(identifier: str) -> bool:
    return bool(_SAFE_IDENTIFIER_RE.fullmatch(identifier))


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _persist_config_identity_meta(
    run: Run,
    *,
    adapter_name: str,
    adapter_version: Optional[str],
    contribution: ConfigContribution,
) -> None:
    if run.meta is None:
        run.meta = {}
    run.meta["config_bundle_hash"] = contribution.identity_hash
    run.meta["config_adapter"] = adapter_name
    if adapter_version is not None:
        run.meta["config_adapter_version"] = adapter_version
    run.meta["config_identity_manifest"] = contribution.identity.to_meta_dict()


class TrackerConfigPlanService(_TrackerServiceBase):
    """
    Config planning and apply-time helpers extracted from ``Tracker``.

    This service owns adapter-driven config discovery, planning, and application
    logic. It remains intentionally tracker-backed for now, so callers should
    treat ``Tracker`` as the public API surface and this class as an internal
    organization boundary.
    """

    def _adapter_accepts_options(
        self, adapter: ConfigAdapter, method_name: str
    ) -> bool:
        method = getattr(adapter, method_name, None)
        if method is None:
            return False
        try:
            signature = inspect.signature(method)
        except (TypeError, ValueError):
            return False
        if "options" in signature.parameters:
            return True
        return any(
            param.kind == param.VAR_KEYWORD for param in signature.parameters.values()
        )

    def _discover_config(
        self,
        adapter: ConfigAdapter,
        config_dir_paths: list[Path],
        strict: bool,
        options: Optional[ConfigAdapterOptions],
    ) -> CanonicalConfig:
        kwargs: dict[str, Any] = {}
        if options is not None and self._adapter_accepts_options(adapter, "discover"):
            kwargs["options"] = options
        return adapter.discover(
            config_dir_paths, identity=self.identity, strict=strict, **kwargs
        )

    def _canonicalize_config(
        self,
        adapter: ConfigAdapter,
        canonical: CanonicalConfig,
        *,
        run: Optional[Run],
        tracker: Optional["Tracker"],
        strict: bool,
        plan_only: bool,
        options: Optional[ConfigAdapterOptions],
    ) -> CanonicalizationResult:
        kwargs: dict[str, Any] = {}
        if options is not None and self._adapter_accepts_options(
            adapter, "canonicalize"
        ):
            kwargs["options"] = options
        result = adapter.canonicalize(
            canonical,
            run=run,
            tracker=tracker,
            strict=strict,
            plan_only=plan_only,
            **kwargs,
        )
        if not result.identity.identity_hash:
            raise ValueError(
                "ConfigAdapter.canonicalize() returned an empty identity_hash."
            )
        if result.identity.adapter_name != adapter.model_name:
            raise ValueError(
                "ConfigAdapter.canonicalize() returned identity for "
                f"{result.identity.adapter_name!r}, expected {adapter.model_name!r}."
            )
        return result

    def canonicalize_config(
        self,
        adapter: ConfigAdapter,
        config_dirs: Iterable[Union[str, Path]],
        *,
        run: Optional[Run] = None,
        run_id: Optional[str] = None,
        strict: bool = False,
        ingest: bool = True,
        profile_schema: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> ConfigContribution:
        if run is not None and run_id is not None:
            raise ValueError("Provide either run= or run_id=, not both.")
        if options is not None and (strict is not False or ingest is not True):
            raise ValueError(
                "When options= is provided, do not pass strict= or ingest=."
            )
        if options is not None:
            strict = options.strict
            ingest = options.ingest

        target_run = run
        if target_run is None and run_id is not None:
            if self.current_consist and self.current_consist.run.id == run_id:
                target_run = self.current_consist.run
            else:
                raise RuntimeError(
                    "canonicalize_config requires an active run matching run_id=."
                )
        if target_run is None and self.current_consist:
            target_run = self.current_consist.run
        if target_run is None:
            raise RuntimeError("canonicalize_config requires an active run or run=.")

        config_dir_paths = [Path(p).resolve() for p in config_dirs]
        canonical = self._discover_config(adapter, config_dir_paths, strict, options)
        result = self._canonicalize_config(
            adapter,
            canonical,
            run=target_run,
            tracker=self._tracker,
            strict=strict,
            plan_only=False,
            options=options,
        )

        contribution = ConfigContribution(
            identity=result.identity,
            adapter_version=getattr(adapter, "adapter_version", None),
            artifacts=result.artifacts,
            ingestables=result.ingestables,
            meta={
                "adapter": adapter.model_name,
                "config_dirs": [str(p) for p in config_dir_paths],
            },
        )

        self._apply_config_contribution(
            contribution,
            run=target_run,
            ingest=ingest,
            profile_schema=profile_schema,
        )

        _persist_config_identity_meta(
            target_run,
            adapter_name=adapter.model_name,
            adapter_version=contribution.adapter_version,
            contribution=contribution,
        )

        return contribution

    def prepare_config(
        self,
        adapter: ConfigAdapter,
        config_dirs: Iterable[Union[str, Path]],
        *,
        strict: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
        validate_only: bool = False,
        facet_spec: Optional[Dict[str, Any]] = None,
        facet_schema_name: Optional[str] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
    ) -> ConfigPlan:
        if options is not None and strict is not False:
            raise ValueError("When options= is provided, do not pass strict=.")
        if options is not None:
            strict = options.strict
        config_dir_paths = [Path(p).resolve() for p in config_dirs]
        canonical = self._discover_config(adapter, config_dir_paths, strict, options)
        result = self._canonicalize_config(
            adapter,
            canonical,
            run=None,
            tracker=None,
            strict=strict,
            plan_only=True,
            options=options,
        )
        facet_data = None
        if facet_spec is not None:
            if hasattr(adapter, "build_facet"):
                facet_data = adapter.build_facet(canonical, facet_spec=facet_spec)
                if facet_data is not None:
                    facet_data = self.identity.normalize_json(facet_data)
            else:
                raise ValueError(
                    "facet_spec provided but adapter does not support build_facet()."
                )

        plan = ConfigPlan(
            adapter_name=adapter.model_name,
            adapter_version=getattr(adapter, "adapter_version", None),
            canonical=canonical,
            artifacts=result.artifacts,
            ingestables=result.ingestables,
            identity=result.identity,
            facet=facet_data,
            facet_schema_name=facet_schema_name,
            facet_schema_version=facet_schema_version,
            facet_index=facet_index,
            meta={
                "adapter": adapter.model_name,
                "config_dirs": [str(p) for p in config_dir_paths],
            },
            adapter=adapter,
        )
        if validate_only:
            diagnostics = validate_config_plan(plan)
            return replace(plan, diagnostics=diagnostics)
        return plan

    def prepare_config_resolver(
        self,
        adapter: ConfigAdapter,
        *,
        config_dirs: Optional[Iterable[Union[str, Path]]] = None,
        config_dirs_from: Optional[
            Union[str, Callable[["StepContext"], Iterable[Union[str, Path]]]]
        ] = None,
        strict: bool = False,
        options: Optional[ConfigAdapterOptions] = None,
        validate_only: bool = False,
        facet_spec: Optional[Dict[str, Any]] = None,
        facet_schema_name: Optional[str] = None,
        facet_schema_version: Optional[Union[str, int]] = None,
        facet_index: Optional[bool] = None,
    ) -> Callable[["StepContext"], ConfigPlan]:
        if (config_dirs is None) == (config_dirs_from is None):
            raise ValueError(
                "prepare_config_resolver requires exactly one of "
                "config_dirs= or config_dirs_from=."
            )

        static_dirs = tuple(config_dirs) if config_dirs is not None else None

        def _resolve_runtime_path(ctx: "StepContext", path: str) -> object:
            parts = [part for part in path.split(".") if part]
            if not parts:
                raise ValueError(
                    "config_dirs_from path must be non-empty (e.g., "
                    "'settings.config_dirs')."
                )
            value: object = ctx.require_runtime(parts[0])
            for part in parts[1:]:
                if isinstance(value, MappingABC):
                    mapping_value = cast(Mapping[str, object], value)
                    if part not in mapping_value:
                        raise ValueError(
                            f"Missing runtime mapping key {part!r} while resolving "
                            f"config_dirs_from={path!r}."
                        )
                    value = mapping_value[part]
                    continue
                if not hasattr(value, part):
                    raise ValueError(
                        f"Missing runtime attribute {part!r} while resolving "
                        f"config_dirs_from={path!r}."
                    )
                value = getattr(value, part)
            return value

        def _resolve_dirs(ctx: "StepContext") -> Iterable[Union[str, Path]]:
            if static_dirs is not None:
                return static_dirs
            source = config_dirs_from
            if isinstance(source, str):
                candidate = _resolve_runtime_path(ctx, source)
            else:
                source_from_ctx = cast(
                    Callable[["StepContext"], Iterable[Union[str, Path]]], source
                )
                candidate = source_from_ctx(ctx)
            if isinstance(candidate, (str, Path)):
                raise ValueError(
                    "Resolved config_dirs must be an iterable of paths, not a single "
                    f"value: {candidate!r}."
                )
            return cast(Iterable[Union[str, Path]], candidate)

        def _resolver(ctx: "StepContext") -> ConfigPlan:
            return self._tracker.prepare_config(
                adapter=adapter,
                config_dirs=_resolve_dirs(ctx),
                strict=strict,
                options=options,
                validate_only=validate_only,
                facet_spec=facet_spec,
                facet_schema_name=facet_schema_name,
                facet_schema_version=facet_schema_version,
                facet_index=facet_index,
            )

        return _resolver

    def apply_config_plan(
        self,
        plan: ConfigPlan,
        *,
        run: Optional[Run] = None,
        ingest: bool = True,
        profile_schema: bool = False,
        adapter: Optional[ConfigAdapter] = None,
        options: Optional[ConfigAdapterOptions] = None,
    ) -> ConfigContribution:
        if options is not None and ingest is not True:
            raise ValueError("When options= is provided, do not pass ingest=.")
        if options is not None:
            ingest = options.ingest

        target_run = run
        if target_run is None and self.current_consist:
            target_run = self.current_consist.run
        if target_run is None:
            raise RuntimeError("apply_config_plan requires an active run or run=.")

        artifacts = list(plan.artifacts)
        adapter_ref = adapter or plan.adapter
        if adapter_ref is not None and (options is None or options.bundle):
            bundle_artifact = getattr(adapter_ref, "bundle_artifact", None)
            if callable(bundle_artifact):
                bundle_spec = bundle_artifact(
                    plan.canonical, run=target_run, tracker=self._tracker
                )
                if bundle_spec is not None:
                    artifacts.append(bundle_spec)

        contribution = ConfigContribution(
            identity=plan.identity,
            adapter_version=plan.adapter_version,
            artifacts=artifacts,
            ingestables=plan.ingestables,
            facet=plan.facet,
            facet_schema_name=plan.facet_schema_name,
            facet_schema_version=plan.facet_schema_version,
            meta=dict(plan.meta or {}),
        )

        self._apply_config_contribution(
            contribution,
            run=target_run,
            ingest=ingest,
            profile_schema=profile_schema,
        )

        _persist_config_identity_meta(
            target_run,
            adapter_name=plan.adapter_name,
            adapter_version=plan.adapter_version,
            contribution=contribution,
        )

        if plan.facet is not None:
            facet_dict = plan.facet
            if self.current_consist is not None:
                self.current_consist.facet = facet_dict
            schema_name = (
                plan.facet_schema_name
                or self.config_facets.infer_schema_name(None, facet_dict)
            )
            self.config_facets.persist_facet(
                run=target_run,
                model=target_run.model_name,
                facet_dict=facet_dict,
                schema_name=schema_name,
                schema_version=plan.facet_schema_version,
                index_kv=plan.facet_index if plan.facet_index is not None else True,
            )

        return contribution

    def identity_from_config_plan(self, plan: ConfigPlan) -> str:
        return plan.identity_hash

    def _apply_config_contribution(
        self,
        contribution: ConfigContribution,
        *,
        run: Run,
        ingest: bool,
        profile_schema: bool,
    ) -> Dict[str, "Artifact"]:
        artifacts_by_key: Dict[str, "Artifact"] = {}
        with self.persistence.batch_artifact_writes():
            for spec in contribution.artifacts:
                art = self.log_artifact(
                    spec.path,
                    key=spec.key,
                    direction=spec.direction,
                    **spec.meta,
                )
                artifacts_by_key[spec.key] = art

        if ingest:
            for spec in contribution.ingestables:
                source_key = spec.source
                artifact = (
                    artifacts_by_key.get(source_key)
                    if source_key
                    else next(iter(artifacts_by_key.values()), None)
                )
                if artifact is None:
                    logging.warning(
                        "[Consist] Skipping ingest for %s; no source artifact found.",
                        spec.table_name,
                    )
                    continue
                if spec.rows is None:
                    logging.warning(
                        "[Consist] Skipping ingest for %s; no rows provided.",
                        spec.table_name,
                    )
                    continue
                if spec.dedupe_on_hash and spec.content_hash:
                    if self._ingest_cache_hit(spec.table_name, spec.content_hash):
                        logging.info(
                            "[Consist] Skipping ingest for %s; cache hit for %s.",
                            spec.table_name,
                            spec.content_hash,
                        )
                        continue
                if source_key is None:
                    logging.warning(
                        "[Consist] Ingest spec for %s missing source; using %s.",
                        spec.table_name,
                        artifact.key,
                    )
                rows = spec.materialize_rows(run.id)
                self.ingest(
                    artifact,
                    data=rows,
                    schema=spec.schema,
                    run=run,
                    profile_schema=profile_schema,
                )

        return artifacts_by_key

    def _ingest_cache_hit(self, table_name: str, content_hash: str) -> bool:
        store = getattr(self, "hot_data_store", None)
        if store is not None:
            return store.ingest_cache_hit(table_name, content_hash)

        if self.engine is None:
            return False
        if not _is_safe_identifier(table_name):
            return False
        table_ref = f"{_quote_ident('global_tables')}.{_quote_ident(table_name)}"
        try:
            with self.engine.begin() as connection:
                result = connection.exec_driver_sql(
                    f"SELECT 1 FROM {table_ref} WHERE content_hash = ? LIMIT 1",
                    (content_hash,),
                ).fetchone()
            return result is not None
        except Exception:
            return False
