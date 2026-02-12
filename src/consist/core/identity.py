"""
consist/core/identity.py

Manages the cryptographic identity of Runs and Artifacts.
"""

import logging
import hashlib
import json
import time
import inspect
import fnmatch
from importlib import import_module
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Any,
    Optional,
    Callable,
    Set,
    Union,
    Literal,
)
from pathlib import Path

# Try importing git, handle error if missing (optional dependency)
git: Optional[ModuleType]
try:
    git = import_module("git")
except ImportError:
    git = None

# Try importing numpy for type checking
np: Optional[ModuleType]
try:
    np = import_module("numpy")
except ImportError:
    np = None

if TYPE_CHECKING:
    from consist.models.artifact import Artifact
    from consist.types import HashInput


class IdentityManager:
    """
    Manage the cryptographic identity and Merkle-tree state of simulation workflows.

    The IdentityManager is responsible for generating deterministic signatures for
    Runs and Artifacts, forming the core of Consist's reproducibility engine.
    By synthesizing code version (Git), configuration parameters, and input
    provenance into composite SHA256 hashes, it ensures that any divergence in
    computational logic or data state results in a unique identity.

    The primary run signature is defined by the following composition:
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

    # --- Canonical JSON utilities ---

    _ZARR_METADATA_FILES = frozenset(
        {
            ".zarray",
            ".zattrs",
            ".zgroup",
            ".zmetadata",
            "zarr.json",
        }
    )

    def canonical_json_str(self, obj: Any) -> str:
        """
        Return a stable JSON string for hashing/IDs.

        Uses `_clean_structure` to normalize types and then dumps with deterministic
        key ordering and compact separators.
        """
        cleaned = self._clean_structure(obj, set())
        return json.dumps(
            cleaned, sort_keys=True, ensure_ascii=True, separators=(",", ":")
        )

    def canonical_json_sha256(self, obj: Any) -> str:
        """SHA256 hex digest of `canonical_json_str(obj)`."""
        return hashlib.sha256(self.canonical_json_str(obj).encode("utf-8")).hexdigest()

    def normalize_json(self, obj: Any) -> Any:
        """
        Normalize Python structures into JSON-friendly types.

        This mirrors the canonical hashing cleanup but preserves the full structure
        without excluding any keys.
        """
        return self._clean_structure(obj, set())

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

        # NOTE:
        # Tests patch `consist.core.identity.git` with a `MagicMock`. In that case,
        # attributes like `git.InvalidGitRepositoryError` are *not* real exception
        # classes, so catching them in an `except (...)` tuple raises:
        #   "TypeError: catching classes that do not inherit from BaseException"
        #
        # To keep the production behavior and still support mocking, we defensively
        # collect only real exception types from the module.
        git_error_types: tuple[type[BaseException], ...] = tuple(
            exc_type
            for exc_type in (
                getattr(git, "InvalidGitRepositoryError", None),
                getattr(git, "NoSuchPathError", None),
            )
            if isinstance(exc_type, type) and issubclass(exc_type, BaseException)
        )

        try:
            # search_parent_directories=True helps if running from a subdir
            repo = git.Repo(self.project_root, search_parent_directories=True)
            sha = repo.head.object.hexsha

            # IMPORTANT:
            # We intentionally ignore untracked files when computing code identity.
            #
            # In typical Consist usage, runs create many untracked files (artifacts, DBs,
            # notebooks outputs) inside a repo. Including untracked filenames in the
            # code hash would make `git_hash` change during the workflow itself and
            # effectively disable caching.
            #
            # Tracked modifications (diff vs HEAD / staged diff) still invalidate caches.
            if repo.is_dirty(untracked_files=False):
                # If dirty, append a stable content hash of the working tree.
                #
                # Rationale: a time-based nonce prevents false cache hits during dev,
                # but it also disables caching entirely for notebooks/local iteration.
                # Hashing the diff keeps cache keys stable until the working tree changes.
                # Only include Python file diffs in the dirty hash to keep cache keys
                # stable when non-code files (e.g., notebooks) change.
                try:
                    diff_head = repo.git.diff("HEAD", "--", "*.py")
                except Exception:
                    diff_head = repo.git.diff("--", "*.py")
                try:
                    diff_cached = repo.git.diff("--cached", "--", "*.py")
                except Exception:
                    diff_cached = ""
                # NOTE:
                # `repo.git.diff(...)` should return strings, but when `git`/`repo` is
                # mocked, these can be `MagicMock` instances. Coerce to `str` so the
                # join/hash logic is stable and doesn't crash under tests.
                dirty_payload = "\n\n".join([sha, str(diff_cached), str(diff_head)])
                dirty_hash = hashlib.sha256(
                    dirty_payload.encode("utf-8", errors="replace")
                ).hexdigest()[:12]
                return f"{sha}-dirty-{dirty_hash}"

            return sha
        except Exception as e:
            # NOTE:
            # In production, we mainly care about "not a git repo" style failures;
            # in tests, the `git` module may be mocked which can change exception
            # semantics. We keep this conservative (never crash) and return a stable
            # fallback string when anything goes wrong.
            if git_error_types and isinstance(e, git_error_types):
                return "unknown_code_version"
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

    def resolve_code_version(
        self,
        *,
        mode: Literal["repo_git", "callable_module", "callable_source"] = "repo_git",
        func: Optional[Callable] = None,
        extra_deps: Optional[List[str]] = None,
    ) -> str:
        """
        Resolve the run code identity hash according to the selected mode.

        Parameters
        ----------
        mode : {"repo_git", "callable_module", "callable_source"}, default "repo_git"
            Code identity strategy.
        func : Optional[Callable], optional
            Callable used by callable-scoped modes.
        extra_deps : Optional[List[str]], optional
            Additional dependency file paths folded into callable-scoped hashes.
        """
        if mode == "repo_git":
            return self.get_code_version()
        if func is None:
            raise ValueError(f"code identity mode {mode!r} requires a callable.")
        strategy = "module" if mode == "callable_module" else "source"
        return self.compute_callable_hash(
            func,
            strategy=strategy,
            extra_deps=extra_deps,
        )

    # --- Component 2: Config Identity ---

    def compute_config_hash(
        self, config: Dict[str, Any], exclude_keys: Optional[List[str]] = None
    ) -> str:
        """
        Generate a deterministic cryptographic hash of a configuration structure.

        This method implements canonical configuration hashing by normalizing
        Python dictionaries, lists, and Pydantic models into a stable state.
        It explicitly addresses the 'NumPy Problem' by converting numerical
        primitives into native Python types and ensures order-independence through
        recursive key sorting.

        Parameters
        ----------
        config : Dict[str, Any]
            The configuration dictionary or Pydantic model to hash.
        exclude_keys : Optional[List[str]], optional
            A collection of keys to be omitted from the identity calculation
            (e.g., non-deterministic timestamps or local file paths).

        Returns
        -------
        str
            A SHA256 hex digest representing the canonical configuration identity.
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

    def compute_run_config_hash(
        self,
        *,
        config: Dict[str, Any],
        model: str,
        year: Any = None,
        iteration: Any = None,
        cache_epoch: Optional[int] = None,
        cache_version: Optional[int] = None,
    ) -> str:
        """
        Compute a config hash for a run, mixing in identity-relevant run fields.

        Tracker persists `config` for human inspection, but caching identity needs to
        include some run context fields that are frequently semantically relevant,
        such as `year`, `iteration`, and cache versioning.
        """
        payload = dict(config)
        run_fields = {
            "model": model,
            "year": year,
            "iteration": iteration,
        }
        if cache_epoch is not None:
            run_fields["cache_epoch"] = cache_epoch
        if cache_version is not None:
            run_fields["cache_version"] = cache_version
        payload["__consist_run_fields__"] = run_fields
        return self.compute_config_hash(payload)

    # --- Component 3: Input Identity ---

    def compute_input_hash(
        self,
        inputs: List["Artifact"],
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], Optional[str]]] = None,
    ) -> str:
        """
        Synthesize a deterministic hash representing the aggregate state of all input artifacts.

        This calculation is fundamental to the construction of the Merkle DAG. It
        incorporates the unique identities of all inputs to ensure that upstream
        data mutations correctly invalidate downstream caches.

        The identity of an input is determined by its provenance:
        1. **Managed Artifacts**: If the artifact was produced by a previous
           Consist run, its identity is derived from the producing run's
           cryptographic signature.
        2. **Exogenous Files**: If the input is a raw file, its identity is
           established through a physical content or metadata hash of the
           filesystem object.

        Parameters
        ----------
        inputs : List[Artifact]
            A collection of Artifact instances representing the run's dependencies.
        path_resolver : Optional[Callable[[str], str]], optional
            A function to resolve virtualized URIs to absolute filesystem paths,
            required for hashing exogenous files.
        signature_lookup : Optional[Callable[[str], Optional[str]]], optional
            A function to retrieve the run signatures of producing runs,
            facilitating Merkle-link construction.

        Returns
        -------
        str
            A SHA256 hex digest representing the combined input identity.

        Raises
        ------
        ValueError
            If an exogenous file requires hashing but no path_resolver is provided.
        """
        if not inputs:
            # Hash of an empty set
            return hashlib.sha256(b"empty_inputs").hexdigest()

        signatures = []

        for artifact in inputs:
            if artifact.run_id:
                # Scenario A: Consist-produced artifact (Provenance Link)
                sig_parts = [
                    f"driver:{getattr(artifact, 'driver', None)}",
                    f"key:{getattr(artifact, 'key', None)}",
                ]
                table_path = getattr(artifact, "table_path", None)
                array_path = getattr(artifact, "array_path", None)
                if table_path:
                    sig_parts.append(f"table_path:{table_path}")
                if array_path:
                    sig_parts.append(f"array_path:{array_path}")
                tmp = None
                if signature_lookup:
                    tmp = signature_lookup(artifact.run_id)

                if tmp:
                    # Link to the signature of the producing run (Merkle Link)
                    sig_parts.append(f"sig:{tmp}")
                else:
                    # Fallback to run_id if signature unknown
                    sig_parts.append(f"run:{artifact.run_id}")

                # Mix in the artifact content hash if available to catch diverging data
                if getattr(artifact, "hash", None):
                    sig_parts.append(f"hash:{artifact.hash}")
                sig = "|".join(sig_parts)
            else:
                # Scenario B: Raw File (External input)
                # We must compute the checksum of the physical file.
                if not path_resolver:
                    raise ValueError(
                        f"Cannot hash raw artifact '{artifact.container_uri}' without a path_resolver."
                    )

                abs_path = path_resolver(artifact.container_uri)
                file_hash = self._compute_file_checksum(abs_path)
                sig_parts = [
                    f"driver:{getattr(artifact, 'driver', None)}",
                    f"container_uri:{artifact.container_uri}",
                ]
                table_path = getattr(artifact, "table_path", None)
                array_path = getattr(artifact, "array_path", None)
                if table_path:
                    sig_parts.append(f"table_path:{table_path}")
                if array_path:
                    sig_parts.append(f"array_path:{array_path}")
                sig_parts.append(f"file:{file_hash}")
                sig = "|".join(sig_parts)

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

        if isinstance(obj, Path):
            return str(obj)

        # 1. Handle Pydantic Models (Native Support)
        # Check for v2 'model_dump' first, then v1 'dict'
        if hasattr(obj, "model_dump"):
            return self._clean_structure(obj.model_dump(mode="json"), exclude_keys)
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
                logging.warning(
                    "Consist: Encountered unsortable set in config. Hash stability not guaranteed."
                )
                return [self._clean_structure(x, exclude_keys) for x in obj]

        # 5. Handle Numpy conversions (Existing logic)
        if np:
            if isinstance(obj, np.ndarray):
                # Recursive call ensures arrays of Pydantic objects or sets are handled
                return self._clean_structure(obj.tolist(), exclude_keys)
            if isinstance(obj, np.generic):
                return self._clean_structure(obj.item(), exclude_keys)

        return obj

    def compute_file_checksum(self, file_path: Union[str, Path]) -> str:
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
        path = file_path if isinstance(file_path, Path) else Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found for hashing: {path}")

        # --- Directory Handling (e.g. Zarr) ---
        if path.is_dir():
            if self._is_zarr_store(path):
                if self.hashing_strategy != "fast":
                    logging.warning(
                        "[Consist Warning] Performing full content hashing on Zarr store '%s'. "
                        "This can be slow. Consider using 'fast' hashing_strategy for metadata-based hashing.",
                        path.name,
                    )
                return self._hash_zarr_store(path)
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

    # Backwards-compatible alias (internal callers / integrations).
    def _compute_file_checksum(self, file_path: Union[str, Path]) -> str:
        return self.compute_file_checksum(file_path)

    # --- External "hash-only" config inputs ---

    def label_for_hash_input(self, path: Union[str, Path]) -> str:
        """
        Create a stable, human-friendly label for a hash input path.

        This is used when recording inputs that are represented only by their
        hash (e.g., "hash-only" config inputs). The method prefers a path that
        is relative to ``project_root`` for readability and portability, and
        falls back to the original string if it cannot be made relative.

        Parameters
        ----------
        path : Union[str, Path]
            A file or directory path used as a hash input.

        Returns
        -------
        str
            A string label suitable for logs and provenance records.
        """
        p = path if isinstance(path, Path) else Path(path)
        try:
            return str(p.resolve().relative_to(self.project_root))
        except Exception:
            return str(p)

    def digest_path(
        self,
        path: Union[str, Path],
        *,
        ignore_dotfiles: bool = True,
        allowlist: Optional[List[str]] = None,
    ) -> str:
        """
        Digest a file or directory with optional filtering.

        - Files: delegated to `compute_file_checksum` (honors hashing_strategy).
        - Directories: deterministic digest over relative paths + (content or metadata).

        Parameters
        ----------
        path : Union[str, Path]
            The filesystem path to be digested. If a directory is provided,
            the method computes an aggregate identity across all contained
            files based on the active hashing strategy.
        ignore_dotfiles : bool, default True
            If True, ignore any file whose relative path includes a component starting with '.'.
        allowlist : Optional[List[str]], optional
            If provided, only include files whose relative path matches at least one glob pattern.
        """
        resolved = (path if isinstance(path, Path) else Path(path)).resolve()
        if not resolved.exists():
            raise FileNotFoundError(str(resolved))

        if resolved.is_file():
            return self.compute_file_checksum(resolved)

        if allowlist is None and self._is_zarr_store(resolved):
            if self.hashing_strategy != "fast":
                logging.warning(
                    "[Consist Warning] Performing full content hashing on Zarr store '%s'. "
                    "This can be slow. Consider using 'fast' hashing_strategy for metadata-based hashing.",
                    resolved.name,
                )
            return self._hash_zarr_store(resolved)

        sha = hashlib.sha256()
        for file_path in sorted(resolved.rglob("*")):
            if not file_path.is_file():
                continue

            rel = file_path.relative_to(resolved).as_posix()
            if ignore_dotfiles and any(
                part.startswith(".") for part in Path(rel).parts
            ):
                continue
            if allowlist is not None and not any(
                fnmatch.fnmatch(rel, pat) for pat in allowlist
            ):
                continue

            if self.hashing_strategy == "fast":
                stat = file_path.stat()
                leaf = f"{rel}:{stat.st_size}:{stat.st_mtime_ns}"
                sha.update(leaf.encode("utf-8"))
            else:
                sha.update(f"{rel}:".encode("utf-8"))
                with open(file_path, "rb") as f:
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        sha.update(chunk)
        return sha.hexdigest()

    def _is_zarr_store(self, path: Path) -> bool:
        if path.suffix == ".zarr":
            return True
        for marker in self._ZARR_METADATA_FILES:
            if (path / marker).exists():
                return True
        return False

    def _hash_zarr_store(self, path: Path) -> str:
        if self.hashing_strategy == "fast":
            return self._hash_zarr_store_fast(path)
        return self._hash_zarr_store_full(path)

    def _hash_zarr_store_full(self, path: Path) -> str:
        sha = hashlib.sha256()
        for file_path in sorted(path.rglob("*")):
            if not file_path.is_file():
                continue
            rel = file_path.relative_to(path).as_posix()
            sha.update(f"{rel}:".encode("utf-8"))
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    sha.update(chunk)
        return sha.hexdigest()

    def _hash_zarr_store_fast(self, path: Path) -> str:
        sha = hashlib.sha256()
        files = [p for p in sorted(path.rglob("*")) if p.is_file()]
        consolidated = path / ".zmetadata"
        zarr_json = path / "zarr.json"

        if consolidated.exists():
            sha.update(b".zmetadata:")
            self._update_hash_from_file(sha, consolidated)
        elif zarr_json.exists():
            sha.update(b"zarr.json:")
            self._update_hash_from_file(sha, zarr_json)
        else:
            for file_path in files:
                if file_path.name in self._ZARR_METADATA_FILES:
                    rel = file_path.relative_to(path).as_posix()
                    sha.update(f"{rel}:".encode("utf-8"))
                    self._update_hash_from_file(sha, file_path)

        for file_path in files:
            name = file_path.name
            if name in self._ZARR_METADATA_FILES:
                continue
            if name.startswith("."):
                continue
            rel = file_path.relative_to(path).as_posix()
            stat = file_path.stat()
            leaf = f"{rel}:{stat.st_size}:{stat.st_mtime_ns}|"
            sha.update(leaf.encode("utf-8"))

        return sha.hexdigest()

    def _update_hash_from_file(self, sha: "hashlib._Hash", path: Path) -> None:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                sha.update(chunk)

    def compute_hash_inputs_digests(
        self,
        hash_inputs: List["HashInput"],
        *,
        ignore_dotfiles: bool = True,
        allowlist: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """
        Compute digests for external "hash-only" config inputs (files or directories).

        Items may be:
        - A path (str/Path): label derived from project-relative path when possible.
        - A (label, path) tuple: explicit label.
        """
        digest_map: Dict[str, str] = {}

        def to_path(p: Union[str, Path]) -> Path:
            return p if isinstance(p, Path) else Path(p)

        for item in hash_inputs:
            if isinstance(item, tuple):
                label, p = item
                path_obj = to_path(p)
            else:
                path_obj = to_path(item)
                label = self.label_for_hash_input(path_obj)

            try:
                digest_map[label] = self.digest_path(
                    path_obj,
                    ignore_dotfiles=ignore_dotfiles,
                    allowlist=allowlist,
                )
            except Exception as exc:
                digest_map[label] = f"ERROR:{exc}"
                logging.warning(
                    "[Consist] Failed to compute hash_input digest for %s (%s): %s",
                    label,
                    path_obj,
                    exc,
                )

        return digest_map
