import logging
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
    environment: Dict[str, str] = None,
    working_dir: Optional[str] = None,
    backend_type: str = "docker",
    pull_latest: bool = False,
) -> bool:
    """
    Executes a containerized step with full provenance tracking.

    Acts as a wrapper around `tracker.start_run`. The container's Image + Command
    acts as the "Code/Config", and the host-side files act as Inputs/Outputs.
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

    # 5. Execute Run Context
    with tracker.start_run(
        run_id=run_id,
        model="container_step",
        config=config.to_hashable_config(),
        inputs=resolved_inputs,
    ) as t:

        # A. Cache Hit
        if t.is_cached:
            logger.info(f"✅ [Consist] Container Cache Hit: {run_id} ({image})")
            # Outputs automatically hydrated (virtualized) by Consist
            return True

        # B. Execution
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

        # C. Log Outputs
        # Scan expected output paths on HOST and log them
        for host_out in outputs:
            path_obj = Path(host_out).resolve()
            if path_obj.exists():
                # Log artifact (Consist handles auto-detecting file vs dir)
                t.log_artifact(path_obj, key=path_obj.name, direction="output")
            else:
                logger.warning(f"⚠️ [Consist] Expected output not found: {host_out}")

    return True
