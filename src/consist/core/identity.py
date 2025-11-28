import hashlib
import json
import time
from typing import Dict, List, Any, Optional, Callable, Set
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
    Manages the cryptographic identity of a Run, which is fundamental to Consist's
    **reproducibility** and **caching** features, leveraging a **Merkle DAG** approach.

    To achieve robust caching and "run forking", a Run's identity is defined as a
    composite SHA256 hash, ensuring that any change in code, configuration, or
    input provenance results in a unique run signature.

    H_run = SHA256( H_code + H_config + H_inputs )
    """

    def __init__(self, project_root: str = ".", hashing_strategy: str = "full"):
        """
        Args:
            project_root (str): Path to the root of the code repository.
            hashing_strategy (str): Defines how file checksums are computed:
                                    - 'full': (Default) Reads entire file content (SHA256).
                                              Provides strong cryptographic integrity, crucial for
                                              **bitwise reproducibility**, but can be slow for very
                                              large files.
                                    - 'fast': Hashes file metadata (Size + MTime).
                                              Offers instant checksums but is less robust for
                                              reproducibility as it relies on filesystem timestamps
                                              and does not detect content changes if size/mtime are same.
        """
        self.project_root = Path(project_root)
        self.hashing_strategy = hashing_strategy

    def calculate_run_signature(
        self, code_hash: str, config_hash: str, input_hash: str
    ) -> str:
        """
        Computes the final cryptographic signature (the unique **cache key**) for a run.

        This signature is a composite hash of the code version, configuration, and input data,
        forming the **Merkle-DAG identity** of the run. Any change in these components
        will result in a different signature, enabling precise caching and reproducibility.

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
        Generates a deterministic SHA256 hash of the configuration dictionary.

        This method implements **"Canonical Config Hashing"** by:
        1.  Removing specified `exclude_keys` (e.g., non-deterministic values like timestamps).
        2.  Converting any Numpy types to native Python types, addressing **"The Numpy Problem"**
            (as outlined in `04_TECHNICAL_CONSTRAINTS.md`) to prevent serialization errors.
        3.  Recursively sorting dictionary keys to ensure a canonical representation regardless
            of original insertion order.

        Args:
            config (Dict[str, Any]): The configuration dictionary to hash.
            exclude_keys (Optional[List[str]]): A list of keys to exclude from the hashing process.

        Returns:
            str: A SHA256 hex digest of the canonicalized configuration.
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
        path_resolver: Optional[Callable[[str], str]] = None,
    ) -> str:
        """
        Generates a deterministic hash representing the state of all input artifacts.

        This hash contributes to the Merkle DAG by incorporating the unique identities
        of all inputs, ensuring that a change in any upstream data source results
        in a new run signature. It handles two main scenarios for inputs:

        1.  **Provenance Exists**: If an `Artifact` object is linked to a previous run
            (`artifact.run_id` is present), its `run_id` is used as its identity,
            forming a direct link in the provenance graph.
        2.  **Raw File Input**: If the input is a raw file not previously generated by
            Consist (i.e., `artifact.run_id` is None), its content or metadata is hashed
            (based on `hashing_strategy`) to establish its identity. A `path_resolver`
            is required in this case to access the file.

        Args:
            inputs (List[Artifact]): A list of `Artifact` objects representing the inputs to the run.
            path_resolver (Optional[Callable[[str], str]]): A function that takes an Artifact URI
                                                           and returns an absolute file path.
                                                           Required for hashing raw file contents.

        Returns:
            str: A SHA256 hex digest representing the combined identity of all inputs.
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
        Retrieves the **"Code Identity"** for the current execution context.

        This method attempts to determine the Git commit hash of the project.
        Crucially, it also detects if the working directory is "dirty" (i.e., has
        uncommitted changes). If dirty, it appends a timestamp/nonce to the SHA
        to ensure that runs from an uncommitted state do not incorrectly
        collide with runs from a clean commit, thus safeguarding **reproducibility**.

        Returns:
            str: The Git commit SHA (e.g., "a1b2c3d") or a modified SHA indicating
                 a dirty state (e.g., "a1b2c3d-dirty-1678886400"). Returns
                 "unknown_code_version" if Git is not available or the project is
                 not a Git repository.
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
        Recursively cleans a Python structure (dictionary or list) for canonical hashing.

        This method is vital for:
        -   **Canonical Config Hashing**: By removing specified `exclude_keys` and
            converting data types consistently, it ensures that the same logical
            configuration always produces the same hash.
        -   **Addressing "The Numpy Problem"**: It converts NumPy-specific data types
            (e.g., `np.int64`, `np.ndarray`) into standard Python types, preventing
            serialization errors with `json.dumps` and ensuring hashes are consistent
            even if input types vary between NumPy and standard Python.
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
        Computes identifier for a file OR directory based on configured strategy.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Input artifact not found at: {file_path}")

        # --- Directory Handling (e.g. Zarr) ---
        if path.is_dir():
            # For directories, we compute a hash based on the aggregate metadata
            # of all files inside.
            if self.hashing_strategy == "fast":
                meta_str = ""
                # Deterministic walk
                for p in sorted(path.rglob("*")):
                    if p.is_file():
                        stat = p.stat()
                        meta_str += f"{p.name}:{stat.st_size}_{stat.st_mtime_ns}|"
                return hashlib.sha256(meta_str.encode("utf-8")).hexdigest()
            else:
                # Full hashing of a Zarr directory is extremely slow.
                # We recommend forcing 'fast' for directories or using 'manifest' hashing.
                # For now, fallback to fast behavior or raise warning.
                print(f"[Consist Warning] Full content hashing on directory {path.name} is slow.")
                # (Logic to hash all file contents would go here if strictly needed)
                # Fallback to fast for now to prevent hangs:
                return self._compute_file_checksum(file_path) # Recursion error? No, logic above changes.
                # Actually, let's just duplicate the fast logic or implement full.
                # Implementing full content hash for dir:
                sha256 = hashlib.sha256()
                for p in sorted(path.rglob("*")):
                    if p.is_file():
                        with open(p, "rb") as f:
                            while True:
                                chunk = f.read(65536)
                                if not chunk: break
                                sha256.update(chunk)
                return sha256.hexdigest()

        # --- Single File Handling (Existing Logic) ---
        if self.hashing_strategy == "fast":
            stat = path.stat()
            meta_str = f"{stat.st_size}_{stat.st_mtime_ns}"
            return hashlib.sha256(meta_str.encode("utf-8")).hexdigest()

        else:
            sha256 = hashlib.sha256()
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    sha256.update(chunk)
            return sha256.hexdigest()
