import hashlib
import json
from typing import Dict, List, Optional, Any
from pydantic import BaseModel


class ContainerDefinition(BaseModel):
    """
    Represents the 'Configuration' of a container run for hashing purposes.

    This model captures all relevant parameters that define a containerized execution,
    allowing Consist to compute a canonical hash for the container's configuration.
    This hash is critical for determining cache hits and ensuring reproducibility.

    Attributes
    ----------
    image : str
        The name or reference of the container image (e.g., "ubuntu:latest").
    image_digest : Optional[str]
        A content-addressable SHA digest of the container image, used for precise
        reproducibility. If None, the image tag is used.
    command : List[str]
        The command and its arguments to execute inside the container, represented
        as a list of strings (exec form).
    environment : Dict[str, str]
        A dictionary of environment variables passed to the container. Values are not
        persisted in run metadata; only a deterministic hash is stored for caching.
    backend : str
        The container backend used to execute this container (e.g., "docker", "singularity").
    extra_args : Dict[str, Any]
        Additional arguments or configuration specific to the container backend
        that might influence the execution but are not part of the core identity
        (e.g., resource limits, specific volume options).
    """

    image: str
    image_digest: Optional[str] = None  # Specific SHA for reproducibility
    command: List[str]
    environment: Dict[str, str]
    backend: str

    working_dir: Optional[str] = None
    volumes: Dict[str, str] = {}
    declared_outputs: Optional[List[str]] = None

    # Extra args that might affect execution (e.g. resource limits)
    extra_args: Dict[str, Any] = {}

    def to_hashable_config(self) -> Dict[str, Any]:
        """
        Returns a clean dictionary representation of the container configuration suitable for hashing.

        This method generates a dictionary that excludes `None` values, ensuring a
        canonical representation of the configuration for consistent hash computation.
        This is crucial for Consist's caching mechanism.

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the essential configuration parameters of the
            container, stripped of any `None` values, ready for hashing.
        """
        # Deterministic representation for hashing
        cfg = {
            "image_digest": self.image_digest,
            "command": tuple(self.command or ()),
            "environment_hash": _hash_environment(self.environment or {}),
            "volumes": tuple(sorted((self.volumes or {}).items())),
            "working_dir": self.working_dir,
            "backend": self.backend,
            "extra_args": tuple(sorted((self.extra_args or {}).items())),
        }
        return cfg


def _hash_environment(environment: Dict[str, str]) -> str:
    items = sorted(
        (str(k), "" if v is None else str(v)) for k, v in (environment or {}).items()
    )
    payload = json.dumps(items, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
