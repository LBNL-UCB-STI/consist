"""
Consist Container API Module

This module provides a high-level API for executing containerized steps
(e.g., Docker, Singularity/Apptainer) with automatic provenance tracking
and caching through Consist. It abstracts away the complexities of
interacting directly with container runtimes and integrates seamlessly
with Consist's `Tracker` to log container execution details, input
dependencies, and output artifacts.

Key functionalities include:
-   **Container Execution with Provenance**: Wraps container execution
    within a Consist `start_run` context, ensuring that container image
    identity, commands, environment, and file I/O are fully tracked.
-   **Backend Agnosticism**: Supports different container runtimes
    (Docker, Singularity/Apptainer) via a unified interface.
-   **Automated Input/Output Logging**: Automatically logs host-side
    files as inputs and scans specified paths for outputs, linking them
    to the container run.
"""

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from sqlmodel import Session, select

from consist.core.tracker import Tracker
from consist.core.materialize import materialize_artifacts
from consist.models.artifact import Artifact
from consist.integrations.containers.models import ContainerDefinition
from consist.integrations.containers.backends import DockerBackend, SingularityBackend
from consist.models.run import RunArtifactLink
from consist.types import ArtifactRef

logger = logging.getLogger(__name__)


@dataclass
class ContainerResult:
    """Return value for run_container with cached output artifacts."""

    artifacts: Dict[str, Artifact]
    cache_hit: bool
    cache_source: Optional[str] = None
    manifest: Optional[Dict[str, Any]] = None
    manifest_hash: Optional[str] = None
    image_digest: Optional[str] = None

    @property
    def output(self) -> Optional[Artifact]:
        """Convenience: return the first (or only) output artifact if present."""
        return next(iter(self.artifacts.values()), None)


def _resolve_image_digest(backend, image: str, pull_latest: bool) -> str:
    """Resolve image digest (backend handles pull_latest internally if supported)."""
    if getattr(backend, "resolve_image_digest", None):
        return backend.resolve_image_digest(image)
    return image


def _hash_inputs(tracker: Tracker, items: List[ArtifactRef]) -> List[str]:
    """Return deterministic hashes for container inputs."""
    hashes: List[str] = []
    for item in items:
        if isinstance(item, Artifact):
            sig_parts: List[str] = []
            if item.run_id:
                run = tracker.get_run(item.run_id)
                if run:
                    if run.config_hash:
                        sig_parts.append(f"conf:{run.config_hash}")
            if item.hash:
                sig_parts.append(f"hash:{item.hash}")
            if not sig_parts:
                try:
                    abs_path = Path(tracker.resolve_uri(item.uri))
                    file_hash = tracker.identity.compute_file_checksum(abs_path)
                    sig_parts.append(f"file:{file_hash}")
                except Exception:
                    sig_parts.append(f"uri:{item.uri}")
            hashes.append("|".join(sig_parts))
        else:
            p = Path(item).resolve()
            if not p.exists():
                hashes.append(f"missing:{p}")
            else:
                # Use identity manager file checksum (fast/ full based on strategy)
                checksum = tracker.identity.compute_file_checksum(p)
                hashes.append(checksum)
    return sorted(hashes)


def _container_signature(
    defn: ContainerDefinition,
    inputs: List[ArtifactRef],
    tracker: Tracker,
) -> str:
    logger.debug(
        "[container.signature] hashable_config=%s",
        defn.to_hashable_config(),
    )
    input_hashes = _hash_inputs(tracker, inputs)
    logger.debug("[container.signature] input_hashes=%s", input_hashes)
    payload = {
        "config": defn.to_hashable_config(),
        "inputs": input_hashes,
    }
    sig = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    logger.debug("[container.signature] signature=%s payload=%s", sig, payload)
    return sig


def _build_container_manifest(
    *,
    image: str,
    image_digest: str,
    command: List[str],
    environment: Dict[str, str],
    working_dir: Optional[str],
    backend_type: str,
    volumes: Dict[str, str],
) -> Dict[str, Any]:
    """
    Build a stable manifest of container semantics for upstream hashing.

    Host paths are intentionally excluded because they often contain run-specific
    directories and are already covered by step-level inputs/outputs.
    """
    container_mounts = sorted(set(volumes.values()))
    env_items = sorted(environment.items())
    return {
        "image": image,
        "image_digest": image_digest,
        "command": command,
        "environment": env_items,
        "working_dir": working_dir,
        "backend": backend_type,
        "container_mounts": container_mounts,
    }


def _container_manifest_hash(manifest: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode("utf-8")
    ).hexdigest()


def _reuse_or_execute_container(
    tracker: Tracker,
    defn: ContainerDefinition,
    inputs: List[ArtifactRef],
    run_id: str,
    execute_fn,
    output_key_map: Optional[Dict[str, str]] = None,
):
    """Cache-aware wrapper. Executes execute_fn on miss; reuses artifacts on hit."""
    signature = _container_signature(defn, inputs, tracker)
    requested_outputs = defn.declared_outputs or []

    cached = tracker.db.find_run_by_signature(signature) if tracker.db else None

    if cached:
        logger.debug(
            "[container.cache] signature match run=%s requested_outputs=%s",
            cached.id,
            requested_outputs,
        )
        cached_outputs = (cached.meta or {}).get("declared_outputs", [])
        if not cached_outputs:
            # If older run lacks declared outputs metadata, assume coverage to avoid false misses
            cached_outputs = requested_outputs
        logger.debug(
            "[container.cache] cached_outputs=%s requested=%s",
            cached_outputs,
            requested_outputs,
        )
        if set(requested_outputs).issubset(set(cached_outputs)):
            logger.info(f"✅ [Consist] Container cache hit: {run_id} -> {cached.id}")
            # Re-link cached artifacts to current run for provenance (inputs + outputs).
            if tracker.db and tracker.engine:
                with Session(tracker.engine) as session:
                    links = session.exec(
                        select(RunArtifactLink).where(
                            RunArtifactLink.run_id == cached.id
                        )
                    ).all()
                for link in links:
                    tracker.db.link_artifact_to_run(
                        artifact_id=link.artifact_id,
                        run_id=run_id,
                        direction=link.direction,
                    )

            # Materialize requested outputs onto the host so callers see expected files.
            # This is copy-only physical materialization (bytes-on-disk), not DB reconstruction.
            cached_items = tracker.get_artifacts_for_run(cached.id)
            output_arts = list(cached_items.outputs.values())
            items: list[tuple[Artifact, Path]] = []

            for host_out in requested_outputs:
                target = Path(host_out).resolve()
                match = next(
                    (
                        a
                        for a in output_arts
                        if a.key == target.name
                        or Path(a.uri).name == target.name
                        or (output_key_map and output_key_map.get(str(target)) == a.key)
                    ),
                    None,
                )
                if not match:
                    logger.warning(
                        "⚠️ [Consist] Cache hit but no matching artifact for requested output: %s",
                        host_out,
                    )
                    continue
                items.append((match, target))

            materialized = materialize_artifacts(
                tracker=tracker, items=items, on_missing="warn"
            )
            db = tracker.db
            if db is None:
                raise RuntimeError("Cannot update run metadata without a database.")
            db.update_run_meta(
                run_id,
                {
                    "cache_hit": True,
                    "cache_source": cached.id,
                    "declared_outputs": requested_outputs,
                    "materialized_outputs": materialized,
                },
            )
            db.update_run_signature(run_id, signature)
            # Keep in-memory run in sync to avoid later overwrite during end_run
            current_consist = tracker.current_consist
            if current_consist is not None:
                run_obj = current_consist.run
                run_obj.signature = signature
                meta = run_obj.meta or {}
                meta.update(
                    {
                        "cache_hit": True,
                        "cache_source": cached.id,
                        "declared_outputs": requested_outputs,
                        "materialized_outputs": materialized,
                    }
                )
                run_obj.meta = meta
            return True

    logger.debug("[container.cache] miss_or_reexec for run=%s", run_id)
    # Cache miss (or missing outputs) -> execute
    execute_fn()
    db = tracker.db
    if db is not None:
        db.update_run_meta(
            run_id,
            {
                "cache_hit": False,
                "declared_outputs": requested_outputs,
                "signature": signature,
            },
        )
        db.update_run_signature(run_id, signature)
    # Sync in-memory run to avoid overwrite on end_run
    current_consist = tracker.current_consist
    if current_consist is not None:
        run_obj = current_consist.run
        run_obj.signature = signature
        meta = run_obj.meta or {}
        meta.update(
            {
                "cache_hit": False,
                "declared_outputs": requested_outputs,
                "signature": signature,
            }
        )
        run_obj.meta = meta
    return False


def run_container(
    tracker: Tracker,
    run_id: str,
    image: str,
    command: Union[str, List[str]],
    volumes: Dict[str, str],
    inputs: List[ArtifactRef],
    outputs: Union[
        List[Union[str, Path]], Dict[str, Union[str, Path]]
    ],  # Allow key->path mapping
    environment: Optional[Dict[str, str]] = None,
    working_dir: Optional[str] = None,
    backend_type: str = "docker",
    pull_latest: bool = False,
    lineage_mode: Literal["full", "none"] = "full",
) -> ContainerResult:
    """
    Executes a containerized step with optional provenance tracking and caching via Consist.

    This function acts as a high-level wrapper that integrates container execution
    with Consist's `Tracker`. In lineage mode "full" it initiates a `Consist` run
    (or attaches to an active run), uses the container's image and command as part
    of the run's identity (code/config), and tracks host-side files as inputs and
    outputs. In lineage mode "none" it only executes the container and returns a
    stable manifest/hash for callers to incorporate into an enclosing step's identity.

    Parameters
    ----------
    tracker : Tracker
        The active Consist `Tracker` instance to use for provenance logging.
    run_id : str
        A unique identifier for this container execution run within Consist.
    image : str
        The container image to use (e.g., "ubuntu:latest", "my_repo/my_image:tag").
    command : Union[str, List[str]]
        The command to execute inside the container. Can be a string or a list of strings
        (for exec form).
    volumes : Dict[str, str]
        A dictionary mapping host paths to container paths for volume mounts.
        Example: `{"/host/path": "/container/path"}`.
    inputs : List[ArtifactRef]
        A list of paths (str/Path) or `Artifact` objects on the host machine that serve
        as inputs to the containerized process. These are logged as Consist inputs.
    outputs : List[str]
        A list of paths on the host machine that are expected to be generated or
        modified by the containerized process. These paths will be scanned and
        logged as Consist output artifacts.
    outputs : Dict[str, str]
        Alternatively, pass a mapping of logical output keys to host paths.
        The artifact will be logged with the provided key instead of the filename.
    environment : Optional[Dict[str, str]], optional
        A dictionary of environment variables to set inside the container. Defaults to empty.
    working_dir : Optional[str], optional
        The working directory inside the container where the command will be executed.
        If None, the default working directory of the container image will be used.
    backend_type : str, default "docker"
        The container runtime backend to use. Currently supports "docker" and "singularity".
    pull_latest : bool, default False
        If True, the Docker backend will attempt to pull the latest image before execution.
        (Applicable only for 'docker' backend).
    lineage_mode : Literal["full", "none"], default "full"
        "full" performs Consist provenance tracking, caching, and output scanning.
        "none" skips Consist logging/caching and does not scan outputs.

    Returns
    -------
    ContainerResult
        Structured result containing logged output artifacts and cache metadata.

    Raises
    ------
    ValueError
        If an unknown `backend_type` is specified.
    RuntimeError
        If the container execution itself fails (e.g., non-zero exit code).
        If the underlying backend fails to resolve image digest or run the container.
    """
    environment = environment or {}

    # 1. Initialize Backend
    if backend_type == "docker":
        backend = DockerBackend(pull_latest=pull_latest)
    elif backend_type == "singularity":
        backend = SingularityBackend()
    else:
        raise ValueError(f"Unknown backend type: {backend_type}")

    # 2. Resolve Container Identity (Image Digest)
    image_digest = _resolve_image_digest(backend, image, pull_latest=pull_latest)
    cmd_list = command.split() if isinstance(command, str) else command
    # Normalize outputs into (key, path) tuples
    output_specs: List[tuple[str, str]] = []
    if isinstance(outputs, dict):
        for logical_key, host_path in outputs.items():
            output_specs.append((str(logical_key), str(host_path)))
    else:
        output_specs = [(Path(o).name, str(o)) for o in outputs]

    outputs_str = [p for _, p in output_specs]

    manifest = _build_container_manifest(
        image=image,
        image_digest=image_digest,
        command=cmd_list,
        environment=environment,
        working_dir=working_dir,
        backend_type=backend_type,
        volumes=volumes,
    )
    manifest_hash = _container_manifest_hash(manifest)

    # 3. Create Config Object for Hashing
    config = ContainerDefinition(
        image=image,
        image_digest=image_digest,
        command=cmd_list,
        environment=environment,
        backend=backend_type,
        working_dir=working_dir,
        volumes=volumes,
        declared_outputs=outputs_str,
        extra_args={},
    )

    # 4. Resolve Input Paths for Consist Hashing
    # Consist needs absolute paths on the HOST to compute input hashes
    resolved_inputs = []
    for i in inputs:
        if isinstance(i, Artifact):
            resolved_inputs.append(i)
        else:
            # Assume string path
            p = Path(i).resolve()
            resolved_inputs.append(str(p))

    # Helper: The actual work of execution
    def _execute_backend_and_log_outputs(active_tracker: Tracker):
        # Ensure output directories exist on HOST before mounting
        # (Docker often creates them as root if they don't exist, causing permission issues)
        for host_path in volumes.keys():
            Path(host_path).mkdir(parents=True, exist_ok=True)

        logger.info(f"🔄 [Consist] Executing Container: {run_id}")
        success = backend.run(
            image=image,
            command=cmd_list,
            volumes=volumes,
            env=environment,
            working_dir=working_dir,
        )

        if not success:
            raise RuntimeError(f"Container execution failed for run_id: {run_id}")

        # Scan expected output paths on HOST and log them
        for output_key, host_out in output_specs:
            path_obj = Path(host_out).resolve()
            if path_obj.exists():
                # Log artifact (Consist handles auto-detecting file vs dir)
                logged = active_tracker.log_artifact(
                    path_obj,
                    key=output_key or path_obj.name,
                    direction="output",
                )
                try:
                    object.__setattr__(logged, "_abs_path", str(path_obj))
                except Exception:
                    pass
            else:
                logger.warning(f"⚠️ [Consist] Expected output not found: {host_out}")

    def _collect_result(
        active_tracker: Tracker, run_identifier: str
    ) -> ContainerResult:
        """
        Collect cache metadata and output artifacts for a run.

        Prefer in-memory state when the run is still active because DB writes may
        not have flushed yet inside the context manager.
        """
        run_meta = {}
        if (
            active_tracker.current_consist
            and active_tracker.current_consist.run.id == run_identifier
        ):
            run_meta = active_tracker.current_consist.run.meta or {}
        else:
            run_obj = active_tracker.get_run(run_identifier)
            run_meta = run_obj.meta if run_obj else {}

        cache_hit = (run_meta or {}).get("cache_hit", False)
        cache_source = (run_meta or {}).get("cache_source")
        artifacts = active_tracker.get_artifacts_for_run(run_identifier).outputs
        return ContainerResult(
            artifacts=artifacts,
            cache_hit=cache_hit,
            cache_source=cache_source,
            manifest=manifest,
            manifest_hash=manifest_hash,
            image_digest=image_digest,
        )

    # 5. Execute Run Context

    # CASE A: Nested Mode (Run already active)
    if tracker.current_consist:
        logger.info(
            f"🔄 [Consist] Container '{run_id}' running as step in active run '{tracker.current_consist.run.id}'"
        )

        if lineage_mode == "none":
            # Pure execution mode: do not log inputs/outputs/metadata or use caching.
            for host_path in volumes.keys():
                Path(host_path).mkdir(parents=True, exist_ok=True)
            logger.info(f"🔄 [Consist] Executing Container (no-lineage): {run_id}")
            success = backend.run(
                image=image,
                command=cmd_list,
                volumes=volumes,
                env=environment,
                working_dir=working_dir,
            )
            if not success:
                raise RuntimeError(
                    f"Container execution failed for run_id (no-lineage): {run_id}"
                )
            return ContainerResult(
                artifacts={},
                cache_hit=False,
                cache_source=None,
                manifest=manifest,
                manifest_hash=manifest_hash,
                image_digest=image_digest,
            )

        # 1. Log Inputs to current run
        for item in resolved_inputs:
            if isinstance(item, Artifact):
                tracker.log_artifact(item, direction="input")
            else:
                # resolved_inputs are strings (already resolved in step 4)
                p = Path(item)
                tracker.log_artifact(str(p), key=p.stem, direction="input")

        # 2. Log Config to Meta (so we don't lose the container context)
        # Use a collision-resistant key: container calls can happen multiple times per second
        # (e.g., retries or tight orchestration loops), and Consist meta keys must be unique
        # to avoid overwriting prior container context.
        step_key = f"container_step_{uuid.uuid4().hex[:10]}"
        # Record human-readable image tag alongside hashable config
        meta_cfg = config.to_hashable_config()
        meta_cfg["image"] = image
        tracker.log_meta(**{step_key: meta_cfg})

        # 3. Execute with cache-aware wrapper
        _reuse_or_execute_container(
            tracker=tracker,
            defn=config,
            inputs=resolved_inputs,
            run_id=tracker.current_consist.run.id,
            execute_fn=lambda: _execute_backend_and_log_outputs(tracker),
            output_key_map={p: k for k, p in output_specs},
        )
        return _collect_result(tracker, tracker.current_consist.run.id)

    # CASE B: Standalone Mode (Start new run)
    if lineage_mode == "none":
        for host_path in volumes.keys():
            Path(host_path).mkdir(parents=True, exist_ok=True)
        logger.info(f"🔄 [Consist] Executing Container (no-lineage): {run_id}")
        success = backend.run(
            image=image,
            command=cmd_list,
            volumes=volumes,
            env=environment,
            working_dir=working_dir,
        )
        if not success:
            raise RuntimeError(
                f"Container execution failed for run_id (no-lineage): {run_id}"
            )
        return ContainerResult(
            artifacts={},
            cache_hit=False,
            cache_source=None,
            manifest=manifest,
            manifest_hash=manifest_hash,
            image_digest=image_digest,
        )

    with tracker.start_run(
        run_id=run_id,
        model="container_step",
        config=config.to_hashable_config(),
        inputs=resolved_inputs,
        # Container runs implement their own caching (based on container signature),
        # so we disable core Tracker caching to avoid double-cache interactions.
        cache_mode="overwrite",
    ) as t:
        _reuse_or_execute_container(
            tracker=t,
            defn=config,
            inputs=resolved_inputs,
            run_id=run_id,
            execute_fn=lambda: _execute_backend_and_log_outputs(t),
            output_key_map={p: k for k, p in output_specs},
        )

        return _collect_result(tracker, run_id)
