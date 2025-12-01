import abc
import os
import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Union

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
        pass

    @abc.abstractmethod
    def resolve_image_digest(self, image: str) -> str:
        """Returns a unique identifier for the image (SHA) if possible."""
        pass


class DockerBackend(ContainerBackend):
    def __init__(self, client=None, pull_latest: bool = False):
        if not docker:
            raise ImportError(
                "The 'docker' python package is required for the DockerBackend."
            )
        self.client = client or docker.from_env()
        self.pull_latest = pull_latest

    def resolve_image_digest(self, image: str) -> str:
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
    Singularity/Apptainer backend optimized for HPC environments.
    Includes logic to set up cache directories on fast local storage.
    """

    def __init__(self, cache_base_options: List[str] = None):
        self.cache_base_options = cache_base_options or [
            "/local",
            os.environ.get("TMPDIR"),
            "/tmp",
        ]
        self._setup_cache_dirs()

    def _setup_cache_dirs(self):
        """
        Sets up Apptainer/Singularity cache directories, prioritizing fast local storage.
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
        # Prepare Bind Mounts "-B host:container,host2:container2"
        bind_list = []
        for host, cont in volumes.items():
            Path(host).mkdir(parents=True, exist_ok=True)
            bind_list.append(f"{host}:{cont}")

        bind_str = ",".join(bind_list)

        # Prepare Environment "--env KEY=VAL"
        env_args = []
        for k, v in env.items():
            env_args.extend(["--env", f"{k}={v}"])

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
            cmd_list.extend(command.split())

        cmd_str = " ".join(cmd_list)
        logger.info(f"🔮 Running Singularity: {cmd_str}")

        try:
            res = subprocess.run(cmd_list, check=False)
            return res.returncode == 0
        except FileNotFoundError:
            logger.error("Singularity executable not found.")
            return False
        except Exception as e:
            logger.error(f"Singularity execution failed: {e}")
            return False
