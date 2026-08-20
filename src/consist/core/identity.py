"""
consist/core/identity.py

Manages the cryptographic identity of Runs and Artifacts.
"""

import logging
import hashlib
import json
import inspect
import fnmatch
from dataclasses import dataclass
from collections.abc import Mapping
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
    cast,
)
from pathlib import Path

from consist.types import CodeIdentityMode
from consist.core.resolved_binding import ArtifactIdentity

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
    from consist.core.run_resolution import InputBindingRole
    from consist.models.artifact import Artifact
    from consist.types import HashInput


class CodeIdentityUnavailableError(RuntimeError):
    """Raised when Consist cannot derive a safe identity for executed code."""

    def __init__(self, *, mode: str, reason: str) -> None:
        self.mode = mode
        self.reason = reason
        super().__init__(
            f"Code identity unavailable for mode {mode!r}: {reason} "
            "Run inside a Git repository or select/provide a supported callable "
            "identity."
        )


@dataclass(frozen=True, slots=True)
class CodeIdentityResolution:
    """The actual code-identity mode and digest used for a run."""

    mode: CodeIdentityMode
    digest: str

    def __post_init__(self) -> None:
        if self.mode not in {"repo_git", "callable_module", "callable_source"}:
            raise ValueError(f"unknown code identity mode: {self.mode!r}")
        if not isinstance(self.digest, str) or not self.digest.strip():
            raise ValueError("code identity digest must not be empty")


@dataclass(frozen=True, slots=True)
class CodeIdentityDescriptor:
    """Self-describing resolved code identity used by action-v2."""

    version: Literal[1]
    mode: Literal["repo_git", "callable_module", "callable_source"]
    digest: str

    def as_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "mode": self.mode,
            "digest": self.digest,
        }


@dataclass(frozen=True, slots=True)
class ActionBindingIdentity:
    """Identity evidence selected for one role-aware action input."""

    kind: str
    role: str | int
    mode: Literal["content-v1", "legacy-provenance-v1"]
    value: str

    def as_payload(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "role": self.role,
            "mode": self.mode,
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class ActionInputIdentity:
    """The domain-separated input identity and evidence used to derive it."""

    value: str
    code: CodeIdentityDescriptor
    bindings: tuple[ActionBindingIdentity, ...]
    strict_binding_identity: str | None = None


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
        self._repo_git_code_version_cache: Optional[str] = None
        self._repo_git_code_version_fingerprint_cache: Optional[str] = None
        self._repo_git_repo_cache: Optional[Any] = None

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

    def _safe_repo_git_diff(self, repo: Any, *args: str) -> Optional[str]:
        try:
            return str(repo.git.diff(*args))
        except Exception:
            return None

    def canonical_json_str(self, obj: Any) -> str:
        """
        Return a stable JSON string for hashing/IDs.

        Uses `_clean_structure` to normalize types and then dumps with deterministic
        key ordering and compact separators.
        """
        cleaned = self._clean_structure(obj, set())
        return self._canonical_json_token(cleaned)

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

    @staticmethod
    def _canonical_json_token(obj: Any) -> str:
        """Serialize a cleaned value using the canonical JSON token settings."""
        return json.dumps(obj, sort_keys=True, ensure_ascii=True, separators=(",", ":"))

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

    def _repo_git_code_version(self, repo: Any) -> tuple[str, str]:
        sha = str(repo.head.object.hexsha)

        # IMPORTANT:
        # We intentionally ignore untracked files when computing code identity.
        #
        # In typical Consist usage, runs create many untracked files (artifacts, DBs,
        # notebooks outputs) inside a repo. Including untracked filenames in the
        # code hash would make `git_hash` change during the workflow itself and
        # effectively disable caching.
        #
        # Tracked modifications (diff vs HEAD / staged diff) still invalidate caches.
        if not repo.is_dirty(untracked_files=False):
            return sha, f"clean:{sha}"

        # If dirty, append a stable content hash of the working tree.
        #
        # Rationale: a time-based nonce prevents false cache hits during dev,
        # but it also disables caching entirely for notebooks/local iteration.
        # Hashing the diff keeps cache keys stable until the working tree changes.
        # Only include Python file diffs in the dirty hash to keep cache keys
        # stable when non-code files (e.g., notebooks) change.
        diff_head = self._safe_repo_git_diff(repo, "HEAD", "--", "*.py")
        if diff_head is None:
            diff_head = self._safe_repo_git_diff(repo, "--", "*.py")
        diff_cached = self._safe_repo_git_diff(repo, "--cached", "--", "*.py") or ""
        # NOTE:
        # `repo.git.diff(...)` should return strings, but when `git`/`repo` is
        # mocked, these can be `MagicMock` instances. Coerce to `str` so the
        # join/hash logic is stable and doesn't crash under tests.
        dirty_payload = "\n\n".join([sha, str(diff_cached), str(diff_head)])
        dirty_hash = hashlib.sha256(
            dirty_payload.encode("utf-8", errors="replace")
        ).hexdigest()[:12]
        code_version = f"{sha}-dirty-{dirty_hash}"
        return code_version, f"dirty:{code_version}"

    def get_code_version(self) -> str:
        """
        Retrieves the global 'Code Identity' using the Git Commit SHA.

        This uses GitPython directly to avoid subprocess overhead and parsing fragility.
        Repo identity is cached per manager, but each lookup validates a cheap
        repository fingerprint so long-lived processes do not silently reuse stale
        code identity after HEAD or tracked Python diffs change.

        Raises
        ------
        CodeIdentityUnavailableError
            If GitPython is unavailable or ``project_root`` is not inside a
            repository that can be inspected.
        """
        code_version: str
        if git is None:
            raise CodeIdentityUnavailableError(
                mode="repo_git",
                reason="GitPython is not installed.",
            )

        try:
            # search_parent_directories=True helps if running from a subdir
            if self._repo_git_repo_cache is None:
                self._repo_git_repo_cache = git.Repo(
                    self.project_root, search_parent_directories=True
                )
            code_version, fingerprint = self._repo_git_code_version(
                self._repo_git_repo_cache
            )
            if (
                self._repo_git_code_version_cache is not None
                and self._repo_git_code_version_fingerprint_cache == fingerprint
            ):
                return self._repo_git_code_version_cache
        except Exception as exc:
            raise CodeIdentityUnavailableError(
                mode="repo_git",
                reason=f"repository lookup failed: {exc}",
            ) from exc

        self._repo_git_code_version_cache = code_version
        self._repo_git_code_version_fingerprint_cache = fingerprint
        return code_version

    def clear_code_version_cache(self) -> None:
        """Clear cached repo Git code identity for this identity manager."""
        self._repo_git_code_version_cache = None
        self._repo_git_code_version_fingerprint_cache = None
        self._repo_git_repo_cache = None

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

        Transparent decorators are unwrapped before inspection so orchestration
        wrappers created with ``functools.wraps`` retain the identity of the
        callable they execute.
        """
        hashes = []

        # 1. Base Strategy
        try:
            identity_callable = inspect.unwrap(func)
            if strategy == "source":
                src = inspect.getsource(identity_callable)
                hashes.append(f"src:{hashlib.sha256(src.encode('utf-8')).hexdigest()}")

            elif strategy == "module":
                module_path = inspect.getfile(identity_callable)
                # We reuse the file checksum logic
                file_hash = self._compute_file_checksum(module_path)
                hashes.append(f"mod:{file_hash}")

            else:
                raise ValueError(f"Unknown code hashing strategy: {strategy}")

        except (OSError, TypeError, ValueError) as e:
            mode = "callable_module" if strategy == "module" else "callable_source"
            raise CodeIdentityUnavailableError(
                mode=mode,
                reason=f"callable inspection failed: {e}",
            ) from e

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
        mode: CodeIdentityMode = "repo_git",
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
        return self.resolve_code_identity(
            mode=mode, func=func, extra_deps=extra_deps
        ).digest

    def resolve_code_identity(
        self,
        *,
        mode: CodeIdentityMode = "repo_git",
        func: Optional[Callable] = None,
        extra_deps: Optional[List[str]] = None,
    ) -> CodeIdentityResolution:
        """Resolve the truthful code identity used by a run.

        ``repo_git`` is preferred. If it is unavailable and a callable is known,
        the resolution falls back to a digest of that callable's defining module.
        Explicit callable modes never fall back to repository identity.
        """
        if mode not in {"repo_git", "callable_module", "callable_source"}:
            raise ValueError(f"Unknown code identity mode: {mode!r}")

        if mode == "repo_git":
            try:
                return CodeIdentityResolution(
                    mode="repo_git", digest=self.get_code_version()
                )
            except CodeIdentityUnavailableError:
                if func is None:
                    raise
                return CodeIdentityResolution(
                    mode="callable_module",
                    digest=self.compute_callable_hash(
                        func,
                        strategy="module",
                        extra_deps=extra_deps,
                    ),
                )

        if func is None:
            raise CodeIdentityUnavailableError(
                mode=mode,
                reason="the selected callable identity mode requires a callable.",
            )

        strategy = "module" if mode == "callable_module" else "source"
        return CodeIdentityResolution(
            mode=mode,
            digest=self.compute_callable_hash(
                func,
                strategy=strategy,
                extra_deps=extra_deps,
            ),
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

    def describe_code_identity(
        self,
        *,
        mode: Literal["repo_git", "callable_module", "callable_source"],
        digest: str,
    ) -> CodeIdentityDescriptor:
        """Return a typed descriptor for an already-resolved code identity."""
        if not isinstance(digest, str) or not digest.strip():
            raise ValueError("code identity digest must not be empty")
        return CodeIdentityDescriptor(version=1, mode=mode, digest=digest)

    def _coerce_code_identity_descriptor(
        self, code_identity: CodeIdentityDescriptor | Mapping[str, object]
    ) -> CodeIdentityDescriptor:
        if isinstance(code_identity, CodeIdentityDescriptor):
            return code_identity
        version = code_identity.get("version")
        mode = code_identity.get("mode")
        digest = code_identity.get("digest")
        if version != 1:
            raise ValueError("action-v2 code identity version must be 1")
        if mode not in {"repo_git", "callable_module", "callable_source"}:
            raise ValueError(f"unsupported action-v2 code identity mode: {mode!r}")
        if not isinstance(digest, str) or not digest.strip():
            raise ValueError("action-v2 code identity digest must not be empty")
        return CodeIdentityDescriptor(
            version=1,
            mode=cast(Literal["repo_git", "callable_module", "callable_source"], mode),
            digest=digest,
        )

    @staticmethod
    def _input_selector(artifact: "Artifact") -> dict[str, str | None]:
        """Return the part of an artifact reference that changes input meaning."""
        return {
            "driver": artifact.driver,
            "table_path": artifact.table_path,
            "array_path": artifact.array_path,
        }

    def _compute_legacy_input_signature(
        self,
        artifact: "Artifact",
        *,
        path_resolver: Optional[Callable[[str], str]],
        signature_lookup: Optional[Callable[[str], Optional[str]]],
    ) -> str:
        """Build conservative provenance evidence for untrusted action inputs."""
        if artifact.run_id:
            sig_parts = [f"driver:{artifact.driver}", f"key:{artifact.key}"]
            if artifact.table_path:
                sig_parts.append(f"table_path:{artifact.table_path}")
            if artifact.array_path:
                sig_parts.append(f"array_path:{artifact.array_path}")
            producer_signature = (
                signature_lookup(artifact.run_id) if signature_lookup else None
            )
            if producer_signature:
                sig_parts.append(f"sig:{producer_signature}")
            else:
                sig_parts.append(f"run:{artifact.run_id}")
            if artifact.hash:
                sig_parts.append(f"hash:{artifact.hash}")
            return "|".join(sig_parts)

        if not path_resolver:
            raise ValueError(
                f"Cannot hash raw artifact '{artifact.container_uri}' without a path_resolver."
            )
        file_hash = self._compute_file_checksum(path_resolver(artifact.container_uri))
        sig_parts = [
            f"driver:{artifact.driver}",
            f"container_uri:{artifact.container_uri}",
        ]
        if artifact.table_path:
            sig_parts.append(f"table_path:{artifact.table_path}")
        if artifact.array_path:
            sig_parts.append(f"array_path:{artifact.array_path}")
        sig_parts.append(f"file:{file_hash}")
        return "|".join(sig_parts)

    def compute_action_input_identity(
        self,
        *,
        inputs: List["Artifact"],
        binding_roles: List["InputBindingRole"],
        code_identity: CodeIdentityDescriptor | Mapping[str, object],
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], Optional[str]]] = None,
        strict_binding_identity: str | None = None,
    ) -> ActionInputIdentity:
        """Compose a role-aware, content-first action-v2 input identity.

        Trusted immutable artifact identities can be reused across producer runs.
        All other inputs retain conservative provenance evidence; weak local
        observations deliberately do not become a cache-reuse identity here.
        """
        descriptor = self._coerce_code_identity_descriptor(code_identity)
        if len(binding_roles) != len(inputs):
            raise ValueError(
                "action-v2 binding role protocol mismatch: role count does not "
                "match input count"
            )
        if sorted(role.input_index for role in binding_roles) != list(
            range(len(inputs))
        ):
            raise ValueError(
                "action-v2 binding role protocol mismatch: roles must cover each "
                "input index exactly once"
            )
        if strict_binding_identity is not None and not strict_binding_identity.strip():
            raise ValueError("strict binding identity must not be empty")

        payload_bindings: list[dict[str, object]] = []
        resolved_bindings: list[ActionBindingIdentity] = []
        for role in binding_roles:
            artifact = inputs[role.input_index]
            try:
                value = str(ArtifactIdentity.from_artifact(artifact))
                mode: Literal["content-v1", "legacy-provenance-v1"] = "content-v1"
            except ValueError:
                value = self._compute_legacy_input_signature(
                    artifact,
                    path_resolver=path_resolver,
                    signature_lookup=signature_lookup,
                )
                mode = "legacy-provenance-v1"
            binding = ActionBindingIdentity(
                kind=role.kind,
                role=role.role,
                mode=mode,
                value=value,
            )
            resolved_bindings.append(binding)
            payload_bindings.append(
                {
                    **binding.as_payload(),
                    "selector": self._input_selector(artifact),
                }
            )

        payload: dict[str, object] = {
            "version": 2,
            "code": descriptor.as_payload(),
            "bindings": payload_bindings,
        }
        if strict_binding_identity is not None:
            payload["strict_binding"] = {
                "version": 1,
                "identity": strict_binding_identity,
            }
        return ActionInputIdentity(
            value=f"sha256:action-v2:{self.canonical_json_sha256(payload)}",
            code=descriptor,
            bindings=tuple(resolved_bindings),
            strict_binding_identity=strict_binding_identity,
        )

    def compute_input_hash(
        self,
        inputs: List["Artifact"],
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], Optional[str]]] = None,
        binding_roles: Optional[List["InputBindingRole"]] = None,
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
        binding_roles : Optional[List[InputBindingRole]], optional
            Caller-visible input roles preserved for the future role-aware composer.
            Phase 2 deliberately does not include them in the legacy hash payload.

        Returns
        -------
        str
            A SHA256 hex digest representing the combined input identity.

        Raises
        ------
        ValueError
            If an exogenous file requires hashing but no path_resolver is provided.
        """
        del binding_roles

        if not inputs:
            # Hash of an empty set
            return hashlib.sha256(b"empty_inputs").hexdigest()

        signatures = [
            self._compute_legacy_input_signature(
                artifact,
                path_resolver=path_resolver,
                signature_lookup=signature_lookup,
            )
            for artifact in inputs
        ]

        # 2. Sort signatures to ensure order-independence (Inputs A,B == Inputs B,A)
        signatures.sort()

        # 3. Hash the joined signatures
        composite = "|".join(signatures)
        return hashlib.sha256(composite.encode("utf-8")).hexdigest()

    def compute_resolved_binding_input_hash(
        self,
        *,
        ordinary_inputs: List["Artifact"],
        strict_binding_identity: str,
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], Optional[str]]] = None,
    ) -> str:
        """Compute the input hash for a strict resolved binding.

        Strict binding inputs are represented by their frozen binding identity,
        while all remaining inputs retain the ordinary provenance-Merkle
        semantics used by :meth:`compute_input_hash`.

        Parameters
        ----------
        ordinary_inputs : list[Artifact]
            Logged inputs outside the ordered strict-binding prefix. Their
            producer signatures remain part of the resulting hash.
        strict_binding_identity : str
            Non-empty SHA-256 digest of the validated strict binding contract.
        path_resolver : callable, optional
            Resolves artifact paths before ordinary input hashing.
        signature_lookup : callable, optional
            Resolves a producing run signature for an ordinary artifact.

        Returns
        -------
        str
            SHA-256 input hash with the ``resolved-binding-content-v1`` domain
            separator.

        Raises
        ------
        ValueError
            If ``strict_binding_identity`` is empty or not a string.
        """
        if (
            not isinstance(strict_binding_identity, str)
            or not strict_binding_identity.strip()
        ):
            raise ValueError("strict binding identity must not be empty")
        ordinary_hash = self.compute_input_hash(
            ordinary_inputs,
            path_resolver=path_resolver,
            signature_lookup=signature_lookup,
        )
        payload = (
            "resolved-binding-content-v1"
            f"|ordinary:{ordinary_hash}"
            f"|binding:{strict_binding_identity}"
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def compute_resolved_binding_action_identity(
        self,
        *,
        ordinary_inputs: List["Artifact"],
        ordinary_binding_roles: List["InputBindingRole"],
        strict_binding_identity: str,
        code_identity: CodeIdentityDescriptor | Mapping[str, object],
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], Optional[str]]] = None,
    ) -> ActionInputIdentity:
        """Apply the action-v2 composer to the ordinary suffix of a strict run."""
        return self.compute_action_input_identity(
            inputs=ordinary_inputs,
            binding_roles=ordinary_binding_roles,
            code_identity=code_identity,
            path_resolver=path_resolver,
            signature_lookup=signature_lookup,
            strict_binding_identity=strict_binding_identity,
        )

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
        elif isinstance(obj, set):
            cleaned_members = [self._clean_structure(x, exclude_keys) for x in obj]
            # Preserve legacy natural ordering for comparable cleaned values so
            # existing sortable-set hashes remain stable. Heterogeneous sets use
            # canonical tokens to remove process-local iteration dependence.
            try:
                return sorted(cleaned_members)
            except TypeError:
                tokenized_members = [
                    (
                        self._canonical_json_token(member),
                        member,
                    )
                    for member in cleaned_members
                ]
                tokenized_members.sort(key=lambda item: item[0])
                return [member for _, member in tokenized_members]

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

    def compute_fast_directory_observation(
        self, directory_path: Union[str, Path]
    ) -> str:
        """Return a versioned metadata observation for one directory.

        Unlike the legacy fast directory checksum, this observation includes
        root-relative member paths.  It is deliberately not a content identity
        and is not consumed by the legacy cache-input composer.
        """
        directory = Path(directory_path)
        if not directory.is_dir():
            raise ValueError(
                f"Fast directory observation requires a directory: {directory}"
            )

        digest = hashlib.sha256()
        for member in sorted(directory.rglob("*")):
            if not member.is_file():
                continue
            stat = member.stat()
            relative_path = member.relative_to(directory).as_posix()
            digest.update(
                f"{relative_path}:{stat.st_size}:{stat.st_mtime_ns}|".encode("utf-8")
            )
        return f"stat-v2:directory:{digest.hexdigest()}"

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
        hashing_strategy_override: Optional[str] = None,
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
        original_strategy = self.hashing_strategy
        if hashing_strategy_override is not None:
            self.hashing_strategy = hashing_strategy_override
        try:
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
        finally:
            self.hashing_strategy = original_strategy

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
