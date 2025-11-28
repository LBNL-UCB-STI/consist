"""
This module provides utilities for integrating Consist with containerized workflows,
specifically Docker and Singularity. It allows users to execute container commands
while automatically tracking provenance and leveraging Consist's caching mechanisms.
"""

import logging
from typing import List, Dict
from pathlib import Path
import subprocess

from consist import Tracker


# Assuming 'tracker' is your initialized Consist Tracker


def run_container_step(
    tracker: Tracker,
    run_id: str,
    image: str,
    command: str,
    volumes: Dict[str, str],  # { "/host/path": "/container/path" }
    inputs: List[str],  # List of HOST paths to input files
    outputs: List[str],  # List of HOST paths to output files
    environment: Dict[str, str] = None,
    client=None,  # Docker client (optional)
    cache_mode: str = "reuse",
):
    """
    Executes a containerized model step, integrating it with Consist's provenance tracking and intelligent caching.

    This function acts as a wrapper for **"Container-Native Workflows"** within Consist,
    where the container execution itself becomes a traceable "Run".
    The **"Config"** for this run is dynamically defined by the container's `image`,
    `command`, and `volumes` mapping, as well as its environment variables.
    Consist tracks input and output artifacts based on their **"Host Paths"**, ensuring
    reproducibility and cache invalidation if any of these components change.

    Args:
        tracker (Tracker): The Consist `Tracker` instance.
        run_id (str): A unique identifier for this container execution run.
        image (str): The Docker or Singularity image name/tag to run.
        command (str): The command to execute inside the container.
        volumes (Dict[str, str]): A dictionary mapping host paths to container paths
                                   (e.g., `{"/host/data": "/container/data"}`).
                                   These are crucial for Consist to track inputs/outputs
                                   on the host filesystem.
        inputs (List[str]): A list of absolute host paths to input files/directories
                            that the container will use. Consist will hash these to
                            determine input provenance.
        outputs (List[str]): A list of absolute host paths where the container is expected
                             to write output files/directories. Consist will log these
                             as output artifacts if they exist after execution.
        environment (Optional[Dict[str, str]]): Environment variables to pass to the container.
        client: An optional Docker client instance. If None, assumes Singularity (or other
                command-line based container runtime).
        cache_mode (str): Consist caching behavior ("reuse", "overwrite", "readonly").

    Returns:
        bool: True if the container execution was successful or retrieved from cache, False otherwise.

    Raises:
        RuntimeError: If the container execution fails.
    """

    # 1. Define the Identity of this execution (Container as "Config")
    # This defines the immutable "Config" component of the run's Merkle DAG identity.
    container_config = {
        "image": image,
        "command": command,
        "volumes": volumes,
        "environment": environment or {},
        "backend": "docker" if client else "singularity",
    }

    # 2. Start Consist Run Context
    # Consist will use the provided 'inputs' (host paths) to compute the input_hash
    # and check for cache hits based on the combined signature.
    with tracker.start_run(
        run_id,
        model="container_op",  # Generic model name for container operations
        config=container_config,
        inputs=inputs,  # Pass host paths for Consist to hash actual files
        cache_mode=cache_mode,
    ) as t:

        # --- A. Check Cache (Consist handles this automatically within start_run) ---
        if t.is_cached:
            logging.info(
                f"✅ Consist Cache HIT! Skipping container execution for {image}."
            )
            # Outputs are already "hydrated" by Consist, no need to re-execute or re-log
            return True

        # --- B. Execute Container Backend (Actual Computation) ---
        logging.info(f"🔄 Executing container: {image} with command: '{command}'")
        success = _execute_container_backend(
            client, image, volumes, command, environment
        )

        if not success:
            # If container execution fails, Consist run will be marked as "failed" by finally block
            raise RuntimeError(
                f"Container '{image}' (run_id: {run_id}) failed execution."
            )

        # --- C. Log Outputs (After Successful Container Execution) ---
        # The container finished. We now tell Consist where the outputs are on the HOST filesystem.
        for host_out_path in outputs:
            if Path(host_out_path).exists():
                t.log_artifact(
                    host_out_path, direction="output", key=Path(host_out_path).name
                )
            else:
                logging.warning(
                    f"⚠️ Warning: Expected output '{host_out_path}' was not created by container '{image}'."
                )

    return True


def _execute_container_backend(client, image, volumes, command, environment) -> bool:
    """
    Executes the container using either the Docker client or a Singularity subprocess.

    This is a placeholder for the actual container orchestration logic. It abstracts
    the underlying container runtime (Docker or Singularity) and executes the specified
    `image` with the given `command`, `volumes`, and `environment` variables.

    Args:
        client: An optional Docker client instance. If None, Singularity is assumed.
        image (str): The container image to run.
        volumes (Dict[str, str]): Volume mappings.
        command (str): The command to run inside the container.
        environment (Dict[str, str]): Environment variables for the container.

    Returns:
        bool: True if the container command exited successfully (status code 0), False otherwise.
    """
    if client:
        # Docker Logic
        run_kwargs = {
            "volumes": volumes,
            "command": command,
            "environment": environment,
            "detach": True,
        }
        try:
            container = client.containers.run(image, **run_kwargs)
            result = container.wait()
            container.remove()
            return result["StatusCode"] == 0
        except Exception as e:
            logging.error(f"Docker Error: {e}")
            return False
    else:
        # Singularity Logic
        # Convert volume dict to Singularity format "-B host:container"
        binds = ",".join([f"{k}:{v}" for k, v in volumes.items()])
        cmd = ["singularity", "run", "-B", binds, image] + command.split()
        res = subprocess.run(cmd)
        return res.returncode == 0
