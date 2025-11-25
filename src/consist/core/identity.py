import hashlib
import json
import time
from typing import Dict, List, Any, Optional, Callable, Union, Set
from pathlib import Path

# Try importing git, handle error if missing (optional dependency)
try:
    import git
except ImportError:
    git = None

# Try importing numpy for type checking
try:
    import numpy as np
except ImportError:
    np = None

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from consist.models.artifact import Artifact


class IdentityManager:
    """
    Manages the cryptographic identity of a Run (Merkle DAG approach).

    To achieve reproducibility and reliable caching ("Forking"), a Run's identity
    is defined as a composite hash:

    H_run = SHA256( H_code + H_config + H_inputs )
    """

    def __init__(self, project_root: str = ".", hashing_strategy: str = "full"):
        """
        Args:
            project_root (str): Path to the root of the code repository.
            hashing_strategy (str): 'full' or 'fast'.
                                    - 'full': Reads entire file content (SHA256). Secure but slow for large files.
                                    - 'fast': Hashes file metadata (Size + MTime). Instant, but relies on filesystem timestamps.
        """
        self.project_root = Path(project_root)
        self.hashing_strategy = hashing_strategy

    def calculate_run_signature(
            self, code_hash: str, config_hash: str, input_hash: str
    ) -> str:
        """
        Computes the final cryptographic signature (cache key) for a run.

        This signature is a composite hash of the code version, configuration, and input data,
        forming the Merkle-DAG identity of the run.

        Args:
            code_hash (str): The hash representing the code version (e.g., Git commit SHA).
            config_hash (str): The hash representing the run's configuration.
            input_hash (str): The hash representing the state of all input artifacts.

        Returns:
            str: A SHA256 hex digest representing the unique signature of the run.
        """
        # We use a separator to prevent collision attacks
        composite = f"code:{code_hash}|conf:{config_hash}|in:{input_hash}"
        return hashlib.sha256(composite.encode("utf-8")).hexdigest()

    def compute_config_hash(
            self, config: Dict[str, Any], exclude_keys: Optional[List[str]] = None
    ) -> str:
        """
        Generates a deterministic hash of the configuration.
        Handles Numpy types, removes noise keys, and sorts keys canonically.
        """
        if exclude_keys is None:
            exclude_keys = []

        # 1. Clean and Canonicalize
        cleaned_config = self._clean_structure(config, set(exclude_keys))

        # 2. Serialize with deterministic sorting
        # ensure_ascii=True ensures locale independence
        json_str = json.dumps(cleaned_config, sort_keys=True, ensure_ascii=True)

        # 3. Hash
        return hashlib.sha256(json_str.encode("utf-8")).hexdigest()

    def compute_input_hash(
            self,
            inputs: List["Artifact"],
            path_resolver: Optional[Callable[[str], str]] = None
    ) -> str:
        """
        Generates a deterministic hash of the input state.

        Args:
            inputs: List of Artifact objects.
            path_resolver: A function that takes an Artifact URI and returns an absolute file path.
                           Required for hashing raw file contents.
        """
        if not inputs:
            # Hash of an empty set
            return hashlib.sha256(b"empty_inputs").hexdigest()

        signatures = []

        for artifact in inputs:
            if artifact.run_id:
                # Scenario A: Provenance Exists (Artifact from previous run)
                # We trust the run_id as the signature of this input's state.
                sig = f"run:{artifact.run_id}"
            else:
                # Scenario B: Raw File (External input)
                # We must compute the checksum of the physical file.
                if not path_resolver:
                    raise ValueError(
                        f"Cannot hash raw artifact '{artifact.uri}' without a path_resolver."
                    )

                abs_path = path_resolver(artifact.uri)
                file_hash = self._compute_file_checksum(abs_path)
                sig = f"file:{file_hash}"

            signatures.append(sig)

        # 2. Sort signatures to ensure order-independence (Inputs A,B == Inputs B,A)
        signatures.sort()

        # 3. Hash the joined signatures
        composite = "|".join(signatures)
        return hashlib.sha256(composite.encode("utf-8")).hexdigest()

    def get_code_version(self) -> str:
        """
        Retrieves the Identity of the Model Logic via Git.
        """
        if git is None:
            return "no_git_module_found"

        try:
            repo = git.Repo(self.project_root, search_parent_directories=True)
            sha = repo.head.object.hexsha

            if repo.is_dirty():
                # If dirty, we cannot rely on the commit SHA alone.
                # We append a timestamp/nonce to ensure this "dirty" run
                # doesn't collide with the clean commit or other dirty states.
                nonce = int(time.time())
                return f"{sha}-dirty-{nonce}"

            return sha
        except (git.InvalidGitRepositoryError, git.NoSuchPathError):
            return "unknown_code_version"

    # --- Internals ---

    def _clean_structure(self, obj: Any, exclude_keys: Set[str]) -> Any:
        """
        Recursively cleans a structure:
        - Removes excluded keys from dicts.
        - Converts Numpy types to native Python types.
        """
        if isinstance(obj, dict):
            return {
                k: self._clean_structure(v, exclude_keys)
                for k, v in obj.items()
                if k not in exclude_keys
            }
        elif isinstance(obj, (list, tuple)):
            return [self._clean_structure(x, exclude_keys) for x in obj]

        # Handle Numpy conversions
        if np:
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, np.bool_):
                return bool(obj)

        return obj

    def _compute_file_checksum(self, file_path: str) -> str:
        """
        Computes identifier for a file based on configured strategy.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Input artifact not found at: {file_path}")

        if self.hashing_strategy == "fast":
            # Fast Mode: Hash(Size + MTime)
            stat = path.stat()
            # We use string concatenation of size and mtime_ns for precision
            meta_str = f"{stat.st_size}_{stat.st_mtime_ns}"
            return hashlib.sha256(meta_str.encode("utf-8")).hexdigest()

        else:
            # Full Mode: Hash Content
            sha256 = hashlib.sha256()
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(65536)  # 64KB chunks
                    if not chunk:
                        break
                    sha256.update(chunk)
            return sha256.hexdigest()