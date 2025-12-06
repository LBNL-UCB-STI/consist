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

import logging
import time
from pathlib import Path
from typing import List, Dict, Union, Optional

from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.integrations.containers.models import ContainerDefinition
from consist.integrations.containers.backends import DockerBackend, SingularityBackend

logger = logging.getLogger(__name__)


def run_container(
    tracker: Tracker,
    run_id: str,
    image: str,
    command: Union[str, List[str]],
    volumes: Dict[str, str],
    inputs: List[Union[str, Artifact]],
    outputs: List[str],
    environment: Optional[Dict[str, str]] = None,
    working_dir: Optional[str] = None,
    backend_type: str = "docker",
    pull_latest: bool = False,
) -> bool:
    """
    Executes a containerized step with full provenance tracking and caching via Consist.

    This function acts as a high-level wrapper that integrates container execution
    with Consist's `Tracker`. It initiates a `Consist` run, uses the container's
    image and command as part of the run's identity (code/config), and tracks
    host-side files as inputs and outputs. It supports different container backends
    like Docker and Singularity.

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
    inputs : List[Union[str, Artifact]]
        A list of paths (str) or `Artifact` objects on the host machine that serve
        as inputs to the containerized process. These are logged as Consist inputs.
    outputs : List[str]
        A list of paths on the host machine that are expected to be generated or
        modified by the containerized process. These paths will be scanned and
        logged as Consist output artifacts.
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

    Returns
    -------
    bool
        True if the container execution and all Consist logging were successful, False otherwise.

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
    image_digest = backend.resolve_image_digest(image)
    cmd_list = command.split() if isinstance(command, str) else command

    # 3. Create Config Object for Hashing
    config = ContainerDefinition(
        image=image,
        image_digest=image_digest,
        command=cmd_list,
        environment=environment,
        backend=backend_type,
        extra_args={"volumes": volumes, "working_dir": working_dir},
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
        for host_out in outputs:
            path_obj = Path(host_out).resolve()
            if path_obj.exists():
                # Log artifact (Consist handles auto-detecting file vs dir)
                active_tracker.log_artifact(
                    path_obj, key=path_obj.name, direction="output"
                )
            else:
                logger.warning(f"⚠️ [Consist] Expected output not found: {host_out}")

    # 5. Execute Run Context

    # CASE A: Nested Mode (Run already active)
    if tracker.current_consist:
        logger.info(
            f"🔄 [Consist] Container '{run_id}' running as step in active run '{tracker.current_consist.run.id}'"
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
        step_key = f"container_step_{int(time.time())}"
        tracker.log_meta(**{step_key: config.to_hashable_config()})

        # 3. Execute
        _execute_backend_and_log_outputs(tracker)
        return True

    # CASE B: Standalone Mode (Start new run)
    with tracker.start_run(
        run_id=run_id,
        model="container_step",
        config=config.to_hashable_config(),
        inputs=resolved_inputs,
    ) as t:

        # 1. Cache Hit Check
        if t.is_cached:
            logger.info(f"✅ [Consist] Container Cache Hit: {run_id} ({image})")
            # Outputs automatically hydrated (virtualized) by Consist
            return True

        # 2. Execute
        _execute_backend_and_log_outputs(t)

    return True
