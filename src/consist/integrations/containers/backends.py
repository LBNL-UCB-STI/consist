"""
Consist Container Backends Module

This module defines abstract and concrete implementations for various container
backends (e.g., Docker, Singularity/Apptainer). These backends allow Consist
to execute code within isolated and reproducible environments, which is crucial
for ensuring the portability and consistency of scientific workflows.

Key features include:
-   **Abstract Interface (`ContainerBackend`)**: Defines a common API for running
    containerized commands and resolving image digests.
-   **Docker Integration (`DockerBackend`)**: Provides functionality to interact
    with Docker daemon, running containers, and pulling images.
-   **Singularity/Apptainer Integration (`SingularityBackend`)**: Optimized for
    High-Performance Computing (HPC) environments, including logic for managing
    cache directories on fast local storage.
"""

import abc
import os
import logging
import os
import shlex
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Union, Any

# Optional Docker Import
try:
    import docker
except ImportError:
    docker = None

logger = logging.getLogger(__name__)


class ContainerBackend(abc.ABC):
    @abc.abstractmethod
    def run(
        self,
        image: str,
        command: Union[str, List[str]],
        volumes: Dict[str, str],
        env: Dict[str, str],
        working_dir: Optional[str] = None,
    ) -> bool:
        """
        Abstract method to run a command within a container.

        This method must be implemented by concrete container backend classes
        to execute a specified command inside a container, mounting volumes,
        setting environment variables, and specifying a working directory.

        Parameters
        ----------
        image : str
            The container image to use (e.g., "ubuntu:latest", "my_repo/my_image:tag").
        command : Union[str, List[str]]
            The command to execute inside the container. Can be a string or a list of strings
            (for exec form).
        volumes : Dict[str, str]
            A dictionary mapping host paths to container paths for volume mounts.
            Example: `{"/host/path": "/container/path"}`.
        env : Dict[str, str]
            A dictionary of environment variables to set inside the container.
        working_dir : Optional[str], optional
            The working directory inside the container where the command will be executed.
            If None, the default working directory of the container image will be used.

        Returns
        -------
        bool
            True if the container command executed successfully (exit code 0), False otherwise.
        """
        pass

    @abc.abstractmethod
    def resolve_image_digest(self, image: str) -> str:
        """
        Abstract method to resolve a container image to a unique, content-addressable identifier.

        This method must be implemented by concrete container backend classes to
        return a stable identifier for a given image, ideally a content digest (SHA)
        rather than a mutable tag. This is crucial for reproducibility.

        Parameters
        ----------
        image : str
            The container image name or reference (e.g., "ubuntu:latest").

        Returns
        -------
        str
            A unique identifier for the image (e.g., a SHA digest). If a digest cannot
            be resolved, the original image string or a best-effort identifier is returned.
        """
        pass


class DockerBackend(ContainerBackend):
    """
    A concrete implementation of `ContainerBackend` for Docker containers.

    This backend interacts with the Docker daemon to manage and execute containerized
    workloads. It provides functionality to run commands, resolve image digests,
    and handle volume mounts and environment variables specific to the Docker engine.

    Attributes
    ----------
    client : docker.client.DockerClient
        The Docker client instance used to communicate with the Docker daemon.
    pull_latest : bool
        If True, the Docker image will be pulled before attempting to resolve its digest
        or run a container.
    """

    def __init__(self, client: Optional[Any] = None, pull_latest: bool = False) -> None:
        """
        Initializes the DockerBackend.

        Parameters
        ----------
        client : Optional[Any], optional
            An existing Docker client instance. If None, a client will be created
            using `docker.from_env()`.
        pull_latest : bool, default False
            If True, Docker images will be pulled to ensure the latest version
            before resolving digests or running containers.

        Raises
        ------
        ImportError
            If the 'docker' Python package is not installed.
        """
        if client is not None:
            # Allow unit tests to inject a mock client even if the docker SDK isn't installed.
            self.client = client
        elif not docker:
            # Allow instantiation without the docker SDK (e.g., non-docker environments).
            # Actual execution will error later if a real client is required.
            logger.warning("Docker SDK not installed; DockerBackend will be inert.")
            self.client = None
        else:
            self.client = docker.from_env()
        self.pull_latest = pull_latest

    def resolve_image_digest(self, image: str) -> str:
        """
        Resolves a Docker image reference to its content-addressable SHA digest.

        This method attempts to get the immutable `RepoDigest` for a Docker image,
        ensuring that a specific, reproducible version of the image is identified.
        If `pull_latest` is enabled, it will first attempt to pull the image.

        Parameters
        ----------
        image : str
            The Docker image name or reference (e.g., "ubuntu:latest", "my_repo/my_image:tag").

        Returns
        -------
        str
            The SHA digest of the image if available (RepoDigest), or the local image ID
            as a fallback. If resolution fails, the original image string is returned.
        """
        try:
            if self.pull_latest:
                self.client.images.pull(image)

            img_obj = self.client.images.get(image)
            # Try to get RepoDigests (immutable content addressable ID)
            if img_obj.attrs.get("RepoDigests"):
                return img_obj.attrs["RepoDigests"][0]
            return img_obj.id  # Fallback to local ID
        except Exception as e:
            logger.warning(f"Could not resolve docker digest for {image}: {e}")
            return image

    def run(
        self,
        image: str,
        command: Union[str, List[str]],
        volumes: Dict[str, str],
        env: Dict[str, str],
        working_dir: Optional[str] = None,
    ) -> bool:
        """
        Runs a command within a Docker container.

        This method executes the specified `command` inside a new Docker container
        created from the given `image`. It configures volume mounts, environment variables,
        and the working directory as specified. Container logs are streamed to stdout,
        and the container is automatically removed after execution.

        Parameters
        ----------
        image : str
            The Docker image to use (e.g., "ubuntu:latest").
        command : Union[str, List[str]]
            The command to execute inside the container. Can be a string or a list of strings
            (for exec form).
        volumes : Dict[str, str]
            A dictionary mapping host paths to container paths for volume mounts.
            Example: `{"/host/path": "/container/path"}`.
        env : Dict[str, str]
            A dictionary of environment variables to set inside the container.
        working_dir : Optional[str], optional
            The working directory inside the container where the command will be executed.
            If None, the default working directory of the container image will be used.

        Returns
        -------
        bool
            True if the Docker container ran successfully and exited with code 0, False otherwise.
        """
        if not self.client:
            raise RuntimeError(
                "Docker client not available. Install the 'docker' package or provide a client."
            )
        # FIX: Pass command directly to Docker (it handles lists correctly for exec form).
        # Joining list to string breaks commands like ["sh", "-c", "complex > redirect"]
        run_command = command

        # Ensure volume map is in Docker format: {host: {'bind': container, 'mode': 'rw'}}
        docker_volumes = {
            host: {"bind": cont, "mode": "rw"} for host, cont in volumes.items()
        }

        try:
            logger.info(f"🐳 Running Docker: {image} {command}")
            container = self.client.containers.run(
                image,
                command=run_command,
                volumes=docker_volumes,
                environment=env,
                working_dir=working_dir,
                detach=True,
                stderr=True,
                stdout=True,
            )

            # Stream logs
            for line in container.logs(stream=True):
                print(line.decode("utf-8", errors="replace").strip())

            result = container.wait()
            exit_code = result.get("StatusCode", 1)

            container.remove()
            return exit_code == 0
        except Exception as e:
            logger.error(f"Docker execution failed: {e}")
            return False


class SingularityBackend(ContainerBackend):
    """
    A concrete implementation of `ContainerBackend` for Singularity/Apptainer containers.

    This backend is optimized for High-Performance Computing (HPC) environments
    where Singularity (now Apptainer) is commonly used. It includes logic to
    intelligently set up cache directories on fast local storage for improved performance.

    Attributes
    ----------
    cache_base_options : List[str]
        A list of preferred base directories for Singularity/Apptainer cache,
        ordered by preference (e.g., local scratch, TMPDIR).
    """

    def __init__(self, cache_base_options: Optional[List[str]] = None) -> None:
        """
        Initializes the SingularityBackend.

        This constructor sets up the priority list for Singularity cache directories
        and calls `_setup_cache_dirs` to configure the environment.

        Parameters
        ----------
        cache_base_options : Optional[List[str]], optional
            A list of paths that will be checked, in order, for suitable fast local storage
            to use as Singularity/Apptainer cache directories. If None, default options
            (e.g., /local, TMPDIR, /tmp) are used.
        """
        self.cache_base_options = cache_base_options or [
            "/local",
            os.environ.get("TMPDIR"),
            "/tmp",
        ]
        self._setup_cache_dirs()

    def _setup_cache_dirs(self) -> None:
        """
        Sets up Apptainer/Singularity cache directories, prioritizing fast local storage.

        This method iterates through `cache_base_options` to find a suitable directory
        with write permissions and sufficient free space (at least 20GB). If found,
        it sets the `APPTAINER_CACHEDIR`, `APPTAINER_TMPDIR`, `SINGULARITY_CACHEDIR`,
        and `SINGULARITY_TMPDIR` environment variables to point to subdirectories
        within the chosen base path. If no suitable location is found, it falls back
        to the current working directory.
        """
        cache_base = None

        # Find suitable storage
        for option in self.cache_base_options:
            if option and os.path.exists(option) and os.access(option, os.W_OK):
                try:
                    # Check for ~20GB free space
                    stat = os.statvfs(option)
                    free_gb = (stat.f_bavail * stat.f_frsize) / (1024**3)
                    if free_gb >= 20:
                        cache_base = option
                        break
                except Exception:
                    continue

        if not cache_base:
            # Fallback to current working dir or user home if nothing else
            cache_base = os.getcwd()
            logger.warning(
                f"No fast local storage found. Using {cache_base} for Singularity cache."
            )

        # Define and create paths
        dirs = {
            "APPTAINER_CACHEDIR": os.path.join(cache_base, ".apptainer", "cache"),
            "APPTAINER_TMPDIR": os.path.join(cache_base, ".apptainer", "tmp"),
            "SINGULARITY_CACHEDIR": os.path.join(cache_base, ".singularity", "cache"),
            "SINGULARITY_TMPDIR": os.path.join(cache_base, ".singularity", "tmp"),
        }

        for var, path in dirs.items():
            os.makedirs(path, exist_ok=True)
            os.environ[var] = path

        logger.debug(f"Singularity cache configured at: {cache_base}")

    def resolve_image_digest(self, image: str) -> str:
        """
        Resolves a Singularity/Apptainer image to a unique identifier.

        For Singularity, if the image is a local SIF file, its absolute path is used.
        Otherwise, the original image string (e.g., a Docker Hub reference) is returned,
        as Singularity's internal image resolution often happens at runtime.

        Parameters
        ----------
        image : str
            The Singularity/Apptainer image name or path (e.g., "library://ubuntu:latest",
            "/path/to/my_image.sif").

        Returns
        -------
        str
            A unique identifier for the image, typically its resolved absolute path if
            it's a local file, or the original image string.
        """
        # If image is a local file (.sif), we could hash it.
        # For now, we rely on the image string/path.
        p = Path(image)
        if p.exists() and p.is_file():
            return str(p.resolve())
        return image

    def run(
        self,
        image: str,
        command: Union[str, List[str]],
        volumes: Dict[str, str],
        env: Dict[str, str],
        working_dir: Optional[str] = None,
    ) -> bool:
        """
        Runs a command within a Singularity/Apptainer container.

        This method constructs and executes a `singularity run` command, binding
        specified volumes, setting environment variables, and configuring the
        working directory. It uses `subprocess.run` to execute the command.

        Parameters
        ----------
        image : str
            The Singularity/Apptainer image to use (e.g., "library://ubuntu:latest",
            "/path/to/my_image.sif").
        command : Union[str, List[str]]
            The command to execute inside the container. Can be a string or a list of strings.
        volumes : Dict[str, str]
            A dictionary mapping host paths to container paths for bind mounts.
            Example: `{"/host/path": "/container/path"}`. Host paths are created if they
            do not exist.
        env : Dict[str, str]
            A dictionary of environment variables to set inside the container.
        working_dir : Optional[str], optional
            The working directory inside the container where the command will be executed.
            If None, the default working directory of the container image will be used.

        Returns
        -------
        bool
            True if the Singularity container command executed successfully (exit code 0),
            False otherwise.

        Raises
        ------
        FileNotFoundError
            If the `singularity` executable is not found in the system's PATH.
        Exception
            Any other exception raised during the `subprocess.run` execution.
        """
        # Prepare Bind Mounts "-B host:container,host2:container2"
        bind_list = []
        for host, cont in volumes.items():
            Path(host).mkdir(parents=True, exist_ok=True)
            bind_list.append(f"{host}:{cont}")

        bind_str = ",".join(bind_list)

        # Prepare environment variables.
        #
        # Singularity/Apptainer's `--env` flag parses its argument(s) as a list of
        # `KEY=VALUE` pairs, and values containing spaces can get split/invalidated.
        # For robust handling (e.g., JAVA_OPTS with many space-separated JVM args),
        # pass such variables via the SINGULARITYENV_/APPTAINERENV_ host environment
        # instead of `--env`.
        env_args: List[str] = []
        passthrough_env: Dict[str, str] = {}
        for k, v in (env or {}).items():
            value = "" if v is None else str(v)
            if any(ch.isspace() for ch in value):
                passthrough_env[f"SINGULARITYENV_{k}"] = value
                passthrough_env[f"APPTAINERENV_{k}"] = value
            else:
                env_args.extend(["--env", f"{k}={value}"])

        cmd_list = ["singularity", "run", "--cleanenv", "--writable-tmpfs"]
        if bind_str:
            cmd_list.extend(["-B", bind_str])
        if working_dir:
            cmd_list.extend(["--pwd", working_dir])

        cmd_list.extend(env_args)
        cmd_list.append(image)

        # Handle command
        if isinstance(command, list):
            cmd_list.extend(command)
        else:
            cmd_list.extend(shlex.split(command))

        cmd_str = " ".join(cmd_list)
        logger.info(f"🔮 Running Singularity: {cmd_str}")
        if passthrough_env:
            logger.info(
                "🔮 Passing %d env var(s) via SINGULARITYENV_/APPTAINERENV_ "
                "(values contain whitespace): %s",
                len(passthrough_env) // 2,
                ", ".join(sorted({k.replace("SINGULARITYENV_", "") for k in passthrough_env if k.startswith("SINGULARITYENV_")})),
            )

        try:
            process_env = os.environ.copy()
            process_env.update(passthrough_env)
            res = subprocess.run(cmd_list, check=False, env=process_env)
            return res.returncode == 0
        except FileNotFoundError:
            logger.error("Singularity executable not found.")
            return False
        except Exception as e:
            logger.error(f"Singularity execution failed: {e}")
            return False
