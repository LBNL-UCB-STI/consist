"""
consist/core/identity.py

Manages the cryptographic identity of Runs and Artifacts.
"""

import logging
import hashlib
import json
import time
import inspect
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
    **reproducibility** and **caching** features.

    To achieve robust caching and "run forking", a Run's identity is defined as a
    composite SHA256 hash, ensuring that any change in code, configuration, or
    input provenance results in a unique run signature.

    H_run = SHA256( H_code + H_config + H_inputs )
    """

    def __init__(self, project_root: str = ".", hashing_strategy: str = "full") -> None:
        """
        Parameters
        ----------
        project_root : str
            Path to the root of the code repository.
        hashing_strategy : str
            'full' (content-based) or 'fast' (metadata-based).
        """
        self.project_root = Path(project_root).resolve()
        self.hashing_strategy = hashing_strategy

    # --- Run Signature Calculation ---

    def calculate_run_signature(
        self, code_hash: str, config_hash: str, input_hash: str
    ) -> str:
        """
        Computes the final cryptographic signature (cache key) for a run.
        """
        composite = f"code:{code_hash}|conf:{config_hash}|in:{input_hash}"
        return hashlib.sha256(composite.encode("utf-8")).hexdigest()

    # --- Component 1: Code Identity ---

    def get_code_version(self) -> str:
        """
        Retrieves the global 'Code Identity' using the Git Commit SHA.

        This uses GitPython directly to avoid subprocess overhead and parsing fragility.
        """
        if git is None:
            return "no_git_module_found"

        try:
            # search_parent_directories=True helps if running from a subdir
            repo = git.Repo(self.project_root, search_parent_directories=True)
            sha = repo.head.object.hexsha

            if repo.is_dirty():
                # If dirty, we append a nonce.
                # improvement: We could hash the diff here for a "Dirty Content Hash",
                # but a timestamp is safer to prevent false cache hits during dev.
                nonce = int(time.time())
                return f"{sha}-dirty-{nonce}"

            return sha
        except (git.InvalidGitRepositoryError, git.NoSuchPathError):
            return "unknown_code_version"

    def compute_callable_hash(
        self,
        func: Callable,
        strategy: str = "module",
        extra_deps: Optional[List[str]] = None,
    ) -> str:
        """
        Computes a hash for a specific Python function/callable.

        This allows for granular caching (ignoring global repo changes) by focusing
        on the relevant code.

        Strategies:
        -----------
        'source':
            Hashes ONLY the function's source code (via `inspect.getsource`).
            Use this for pure functions with no external dependencies.
        'module':
            Hashes the entire file (.py) where the function is defined.
            This is the robust "in-between": it captures helper functions and
            constants in the same file, but ignores changes in unrelated files.

        Parameters
        ----------
        func : Callable
            The function to hash.
        strategy : str, default "module"
            The hashing strategy ("source" or "module").
        extra_deps : List[str], optional
            List of additional file paths (relative to project root) that this
            function depends on. Their content will be mixed into the hash.
        """
        hashes = []

        # 1. Base Strategy
        try:
            if strategy == "source":
                src = inspect.getsource(func)
                hashes.append(f"src:{hashlib.sha256(src.encode('utf-8')).hexdigest()}")

            elif strategy == "module":
                module_path = inspect.getfile(func)
                # We reuse the file checksum logic
                file_hash = self._compute_file_checksum(module_path)
                hashes.append(f"mod:{file_hash}")

            else:
                raise ValueError(f"Unknown code hashing strategy: {strategy}")

        except (OSError, TypeError) as e:
            # Fallback if inspect fails (e.g. dynamically defined functions, REPL)
            logging.warning(
                f"Could not inspect source for {func}: {e}. Fallback to timestamp."
            )
            hashes.append(f"fallback:{time.time()}")

        # 2. Extra Dependencies (e.g., utils.py, config files)
        if extra_deps:
            for dep in extra_deps:
                # Resolve relative to project root
                full_path = self.project_root / dep
                if full_path.exists():
                    d_hash = self._compute_file_checksum(str(full_path))
                    hashes.append(f"dep:{dep}:{d_hash}")
                else:
                    # If dependency is missing, we must affect the hash to warn or fail?
                    # For caching safety, a missing dependency changes the hash.
                    hashes.append(f"dep:{dep}:MISSING")

        # 3. Composite Hash
        composite = "|".join(sorted(hashes))
        return hashlib.sha256(composite.encode("utf-8")).hexdigest()

    # --- Component 2: Config Identity ---

    def compute_config_hash(
        self, config: Dict[str, Any], exclude_keys: Optional[List[str]] = None
    ) -> str:
        """
        Generates a deterministic SHA256 hash of the configuration dictionary.

        This method implements **"Canonical Config Hashing"** by:
        1.  Removing specified `exclude_keys` (e.g., non-deterministic values like timestamps,
            or sensitive information that should not affect reproducibility).
        2.  Converting any NumPy types to native Python types, addressing **"The NumPy Problem"**
            (as outlined in `04_TECHNICAL_CONSTRAINTS.md`) to prevent serialization errors
            and ensure consistent hashing across different environments.
        3.  Recursively sorting dictionary keys to guarantee a canonical JSON representation
            regardless of the original insertion order.

        Parameters
        ----------
        config : Dict[str, Any]
            The configuration dictionary to hash.
        exclude_keys : Optional[List[str]], optional
            A list of keys whose values should be excluded from the hashing process.
            Defaults to an empty list.

        Returns
        -------
        str
            A SHA256 hex digest of the canonicalized configuration.
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

    # --- Component 3: Input Identity ---

    def compute_input_hash(
        self,
        inputs: List["Artifact"],
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], str]] = None,
    ) -> str:
        """
        Generates a deterministic hash representing the state of all input artifacts.

        This hash contributes to the Merkle DAG by incorporating the unique identities
        of all inputs, ensuring that a change in any upstream data source results
        in a new run signature. It handles two main scenarios for inputs:

        1.  **Provenance Exists**: If an `Artifact` object is linked to a previous run
            (`artifact.run_id` is present), its `run_id` is used as its identity,
            forming a direct link in the provenance graph. This enables "run forking"
            and efficient cache invalidation.
        2.  **Raw File Input**: If the input is a raw file not previously generated by
            Consist (i.e., `artifact.run_id` is None), its content or metadata is hashed
            (based on `hashing_strategy`) to establish its identity. A `path_resolver`
            is required in this case to access the file on the local filesystem.

        Parameters
        ----------
        inputs : List[Artifact]
            A list of `Artifact` objects representing the inputs to the run.
        path_resolver : Optional[Callable[[str], str]], optional
            A function that takes an `Artifact` URI (a portable string like "inputs://data.csv")
            and returns an absolute file path on the local filesystem. This is required
            for hashing the content of raw files.
        signature_lookup : Optional[Callable[[str], str]], optional

        Returns
        -------
        str
            A SHA256 hex digest representing the combined and ordered identity of all inputs.

        Raises
        ------
        ValueError
            If a raw file artifact needs to be hashed but no `path_resolver` function is provided.
        """
        if not inputs:
            # Hash of an empty set
            return hashlib.sha256(b"empty_inputs").hexdigest()

        signatures = []

        for artifact in inputs:
            if artifact.run_id:
                # Scenario A: Consist-produced artifact (Provenance Link)
                tmp = None
                if signature_lookup:
                    tmp = signature_lookup(artifact.run_id)

                if tmp:
                    # Link to the signature of the producing run (Merkle Link)
                    sig = f"sig:{tmp}"
                else:
                    # Fallback to run_id if signature unknown
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

    # --- Internal Utilities ---

    def _clean_structure(self, obj: Any, exclude_keys: Set[str]) -> Any:
        """
        Recursively cleans a Python structure (dictionary, list, tuple, Pydantic model) for canonical hashing.

        This method is vital for:
        -   **Canonical Config Hashing**: By recursively removing specified `exclude_keys`
            from dictionaries and consistently converting data types, it ensures that
            the same logical configuration always produces the same hash, regardless
            of minor structural or type variations.
        -   **Addressing "The NumPy Problem"**: It handles NumPy-specific data types
            (e.g., `np.int64`, `np.ndarray`) by converting them into standard Python types
            (e.g., `int`, `float`, `list`). This prevents serialization errors with
            `json.dumps` and ensures that hashes are consistent even if input types
            vary between NumPy and standard Python, which is common in scientific computing.

        Updates:
        - Handles Pydantic models (v1 and v2)
        - Handles Sets (converts to sorted list for determinism)
        - Handles NumPy types
        """
        # 1. Handle Pydantic Models (Native Support)
        # Check for v2 'model_dump' first, then v1 'dict'
        if hasattr(obj, "model_dump"):
            return self._clean_structure(obj.model_dump(mode='json'), exclude_keys)
        elif hasattr(obj, "dict") and hasattr(obj, "json"):  # Pydantic v1 heuristic
            return self._clean_structure(obj.dict(), exclude_keys)

        # 2. Handle Dictionaries
        if isinstance(obj, dict):
            return {
                k: self._clean_structure(v, exclude_keys)
                for k, v in obj.items()
                if k not in exclude_keys
            }

        # 3. Handle Lists and Tuples
        elif isinstance(obj, (list, tuple)):
            return [self._clean_structure(x, exclude_keys) for x in obj]

        # 4. Handle Sets (CRITICAL for hashing)
        # Sets must be sorted to ensure the hash is identical regardless of memory layout
        elif isinstance(obj, set):
            try:
                # Attempt to sort; requires items to be comparable
                return sorted([self._clean_structure(x, exclude_keys) for x in obj])
            except TypeError:
                # Fallback if items aren't comparable (rare in configs, but possible)
                # We convert to list to allow JSON serialization, but warn about non-determinism
                logging.warning("Consist: Encountered unsortable set in config. Hash stability not guaranteed.")
                return [self._clean_structure(x, exclude_keys) for x in obj]

        # 5. Handle Numpy conversions (Existing logic)
        if np:
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                # Recursive call ensures arrays of Pydantic objects or sets are handled
                return self._clean_structure(obj.tolist(), exclude_keys)
            elif isinstance(obj, np.bool_):
                return bool(obj)

        return obj

    def _compute_file_checksum(self, file_path: str) -> str:
        """
        Computes a cryptographic identifier for a given file or directory based on the configured hashing strategy.

        This method is critical for establishing the unique identity of raw file-based
        inputs to a Consist run. It supports two main strategies: 'full' (content-based)
        and 'fast' (metadata-based), and handles both single files and directories.

        Parameters
        ----------
        file_path : str
            The absolute path to the file or directory for which to compute the checksum.

        Returns
        -------
        str
            A SHA256 hex digest representing the checksum or identity of the file/directory.

        Raises
        ------
        FileNotFoundError
            If the specified `file_path` does not exist on the filesystem.

        Warns
        -----
        UserWarning
            If 'full' content hashing is performed on a directory, as this can be
            computationally expensive for large directories.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found for hashing: {file_path}")

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
                # Default to full content hashing for directories if not 'fast'.
                # This can be slow for large directories.
                logging.warning(
                    f"[Consist Warning] Performing full content hashing on directory '{path.name}'. "
                    "This can be slow. Consider using 'fast' hashing_strategy or pre-computed hashes for directories."
                )
                sha256 = hashlib.sha256()
                for p in sorted(path.rglob("*")):
                    if p.is_file():
                        with open(p, "rb") as f:
                            while True:
                                chunk = f.read(65536)
                                if not chunk:
                                    break
                                sha256.update(chunk)
                return sha256.hexdigest()

        # Single File Handling
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
