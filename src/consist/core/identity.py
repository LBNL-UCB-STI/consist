"""
consist/core/identity.py

Manages the cryptographic identity of Runs and Artifacts.
"""

import logging
import hashlib
import json
import inspect
import fnmatch
import subprocess
import warnings
from dataclasses import dataclass
from collections.abc import Mapping, Sequence
from importlib import import_module
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Any,
    Optional,
    Callable,
    Union,
)
from pathlib import Path

from consist.types import CodeIdentityMode
from consist.digest import (
    Digest,
    canonical_json_sha256,
    canonical_json_str,
    compute_digest,
    file_sha256,
    normalize_json,
)

# Try importing git lazily on first use, handle error if missing (optional dependency)
_UNSET = object()
git: Any = _UNSET


def _get_git() -> Optional[ModuleType]:
    global git
    if git is _UNSET:
        try:
            git = import_module("git")
        except ImportError:
            git = None
    return git


if TYPE_CHECKING:
    from consist.core.run_resolution import InputBindingRole
    from consist.models.artifact import Artifact
    from consist.types import HashInput


class CodeIdentityUnavailableError(RuntimeError):
    """Raised when Consist cannot derive a safe identity for executed code."""

    def __init__(
        self, *, mode: str, reason: str, guidance: Optional[str] = None
    ) -> None:
        self.mode = mode
        self.reason = reason
        self.guidance = guidance or (
            "Run inside a Git repository or select/provide a supported callable "
            "identity."
        )
        super().__init__(
            f"Code identity unavailable for mode {mode!r}: {reason} {self.guidance}"
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

        Deprecated: use ``consist.digest.canonical_json_str`` directly.
        """
        warnings.warn(
            "IdentityManager.canonical_json_str is deprecated; "
            "use consist.digest.canonical_json_str instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return canonical_json_str(obj)

    def canonical_json_sha256(self, obj: Any) -> str:
        """
        SHA256 hex digest of ``canonical_json_str(obj)``.

        Deprecated: use ``consist.digest.canonical_json_sha256`` directly.
        """
        warnings.warn(
            "IdentityManager.canonical_json_sha256 is deprecated; "
            "use consist.digest.canonical_json_sha256 instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return canonical_json_sha256(obj)

    def normalize_json(self, obj: Any) -> Any:
        """
        Normalize Python structures into JSON-friendly types.

        Deprecated: use ``consist.digest.normalize_json`` directly.
        """
        warnings.warn(
            "IdentityManager.normalize_json is deprecated; "
            "use consist.digest.normalize_json instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return normalize_json(obj)

    # --- Run Signature Calculation ---

    def calculate_run_signature(
        self, code_hash: str, config_hash: str, input_hash: str
    ) -> str:
        """
        Computes the canonical cryptographic signature (cache key) for a run.
        """
        composite = f"{code_hash}:{config_hash}:{input_hash}"
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

    def _cli_git_run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(self.project_root), *args],
            capture_output=True,
            text=True,
            check=False,
        )

    def _cli_git_code_version(self) -> tuple[str, str]:
        try:
            rev = self._cli_git_run("rev-parse", "--verify", "HEAD")
        except (FileNotFoundError, OSError) as exc:
            raise CodeIdentityUnavailableError(
                mode="repo_git",
                reason=f"GitPython is not installed and git CLI is unavailable: {exc}.",
                guidance=(
                    'Install GitPython with `pip install "consist[git]"` or ensure '
                    "`git` is on PATH inside a Git repository with at least one commit."
                ),
            ) from exc

        if rev.returncode != 0:
            detail = (rev.stderr or rev.stdout or "").strip()
            reason = (
                f"Could not resolve a Git commit from project root {self.project_root}"
            )
            if detail and detail != str(self.project_root):
                reason += f": {detail}"
            raise CodeIdentityUnavailableError(
                mode="repo_git",
                reason=f"{reason}.",
                guidance=(
                    "Set project_root to a Git repository with at least one commit."
                ),
            )

        sha = rev.stdout.strip()
        status = self._cli_git_run("status", "--porcelain", "--untracked-files=no")
        if status.returncode != 0:
            detail = (status.stderr or status.stdout or "").strip()
            raise CodeIdentityUnavailableError(
                mode="repo_git",
                reason=(
                    f"Could not inspect Git working tree at {self.project_root}: {detail}."
                ),
                guidance=(
                    "Set project_root to a Git repository with at least one commit."
                ),
            )

        if not status.stdout.strip():
            return sha, f"clean:{sha}"

        diff_head_proc = self._cli_git_run("diff", "HEAD", "--", "*.py")
        if diff_head_proc.returncode == 0:
            diff_head = diff_head_proc.stdout.rstrip("\r\n")
        else:
            fallback_proc = self._cli_git_run("diff", "--", "*.py")
            diff_head = (
                fallback_proc.stdout.rstrip("\r\n")
                if fallback_proc.returncode == 0
                else ""
            )
        diff_cached_proc = self._cli_git_run("diff", "--cached", "--", "*.py")
        diff_cached = (
            diff_cached_proc.stdout.rstrip("\r\n")
            if diff_cached_proc.returncode == 0
            else ""
        )
        dirty_payload = "\n\n".join([sha, diff_cached, diff_head])
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
        git_module = _get_git()
        if git_module is None:
            raise CodeIdentityUnavailableError(
                mode="repo_git",
                reason="GitPython is not installed.",
                guidance=(
                    'Install GitPython with `pip install "consist[git]"`, then set '
                    "project_root to a Git repository with at least one commit."
                ),
            )

        try:
            # search_parent_directories=True helps if running from a subdir
            if self._repo_git_repo_cache is None:
                self._repo_git_repo_cache = git_module.Repo(
                    self.project_root, search_parent_directories=True
                )
            code_version, _ = self._repo_git_code_version(self._repo_git_repo_cache)
            return code_version
        except Exception as exc:
            detail = str(exc)
            reason = (
                f"Could not resolve a Git commit from project root {self.project_root}"
            )
            if detail != str(self.project_root):
                reason += f": {detail}"
            raise CodeIdentityUnavailableError(
                mode="repo_git",
                reason=f"{reason}.",
                guidance=(
                    "Set project_root to a Git repository with at least one commit."
                ),
            ) from exc

    def clear_code_version_cache(self) -> None:
        """Clear cached repo Git object for this identity manager."""
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
                file_hash = self.compute_file_checksum(module_path)
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
                    d_hash = self.compute_file_checksum(str(full_path))
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

    def resolve_fallback_code_identity_without_gitpython(
        self,
        *,
        extra_deps: Optional[List[str]] = None,
    ) -> CodeIdentityResolution:
        """Best-effort code identity when optional ``consist[git]`` is not installed."""
        try:
            code_version, _ = self._cli_git_code_version()
            return CodeIdentityResolution(mode="repo_git", digest=code_version)
        except CodeIdentityUnavailableError:
            pass

        consist_pkg_dir = Path(__file__).resolve().parents[1]
        frame = inspect.currentframe()
        try:
            cur = frame.f_back if frame is not None else None
            while cur is not None:
                filename = cur.f_code.co_filename
                if filename and not filename.startswith("<"):
                    candidate = Path(filename)
                    try:
                        resolved = candidate.resolve()
                    except OSError:
                        resolved = None
                    if (
                        resolved is not None
                        and resolved.is_file()
                        and consist_pkg_dir not in resolved.parents
                    ):
                        hashes = [f"mod:{self.compute_file_checksum(str(resolved))}"]
                        if extra_deps:
                            for dep in extra_deps:
                                full_path = self.project_root / dep
                                if full_path.exists():
                                    hashes.append(
                                        f"dep:{dep}:{self.compute_file_checksum(str(full_path))}"
                                    )
                                else:
                                    hashes.append(f"dep:{dep}:MISSING")
                        composite = "|".join(sorted(hashes))
                        digest = hashlib.sha256(composite.encode("utf-8")).hexdigest()
                        return CodeIdentityResolution(
                            mode="callable_module",
                            digest=digest,
                        )
                cur = cur.f_back
        finally:
            del frame

        return CodeIdentityResolution(mode="repo_git", digest="untracked")

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
        cleaned_config = normalize_json(config, exclude_keys=set(exclude_keys))

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

    _WEAK_DIGEST_SCHEMES = frozenset(
        {"stat_file_v1:sha256", "stat_dir_v1:sha256", "caller_supplied_v1:sha256"}
    )

    @classmethod
    def _trusted_input_digest_reason(
        cls, artifact: "Artifact"
    ) -> tuple[Digest | None, str | None]:
        """Return ``(digest, None)`` when ``artifact.digest`` is content-addressed."""
        meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        semantics = meta.get("hash_semantics")
        if isinstance(semantics, Mapping):
            source = semantics.get("source")
            if source == "caller_supplied":
                return None, "caller_supplied_hash"
            if source == "computed_fast":
                return None, "fast_file_observation"
            if source == "computed_fast_directory":
                return None, "fast_directory_observation"
            if (
                semantics.get("kind") == "directory"
                and semantics.get("digest_contract") == "legacy_directory_content"
            ):
                return None, "directory_without_manifest"

        digest = artifact.digest
        if not isinstance(digest, Digest):
            return None, "missing_digest"
        if digest.scheme == "caller_supplied_v1:sha256":
            return None, "caller_supplied_hash"
        if digest.scheme == "stat_file_v1:sha256":
            return None, "fast_file_observation"
        if digest.scheme == "stat_dir_v1:sha256":
            return None, "fast_directory_observation"
        if digest.scheme in cls._WEAK_DIGEST_SCHEMES:
            return None, "weak_observation"
        return digest, None

    def _resolve_input_token(
        self,
        artifact: "Artifact",
        *,
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], Optional[str]]] = None,
    ) -> tuple[str, str, str | None]:
        """Return ``(token, mode, fallback_reason)`` for a single input artifact."""
        digest, fallback_reason = self._trusted_input_digest_reason(artifact)
        if digest is not None:
            token = (
                f"{digest}#member={artifact.member}"
                if artifact.member is not None
                else str(digest)
            )
            return token, "content", None

        if artifact.run_id:
            sig_parts = [f"driver:{artifact.driver}", f"key:{artifact.key}"]
            if artifact.member is not None:
                sig_parts.append(f"member:{artifact.member}")
            producer_signature = (
                signature_lookup(artifact.run_id) if signature_lookup else None
            )
            if producer_signature:
                sig_parts.append(f"sig:{producer_signature}")
            else:
                sig_parts.append(f"run:{artifact.run_id}")
            if artifact.digest is not None:
                sig_parts.append(f"digest:{artifact.digest}")
            return "provenance_v1:" + "|".join(sig_parts), "provenance", fallback_reason

        resolved_path = path_resolver(artifact.uri) if path_resolver else None
        has_local_target = False
        if resolved_path is not None:
            try:
                has_local_target = Path(resolved_path).exists()
            except OSError:
                has_local_target = False
        if (
            not has_local_target
            and artifact.digest is not None
            and artifact.digest.scheme == "caller_supplied_v1:sha256"
        ):
            base = f"caller_supplied_v1:{artifact.digest.hex}|uri:{artifact.uri}"
            token = (
                f"{base}#member={artifact.member}"
                if artifact.member is not None
                else base
            )
            return token, "provenance", "caller_supplied_hash"
        if resolved_path is None:
            raise ValueError(
                f"Cannot hash raw artifact '{artifact.uri}' without a path_resolver."
            )
        computed = compute_digest(
            resolved_path,
            driver=artifact.driver,
            member=artifact.member,
            options={"hashing_strategy": self.hashing_strategy},
        )
        if computed.scheme in self._WEAK_DIGEST_SCHEMES:
            base = f"{computed}|uri:{artifact.uri}"
            mode = "provenance"
            reason = (
                "fast_directory_observation"
                if computed.scheme == "stat_dir_v1:sha256"
                else "fast_file_observation"
            )
        else:
            base = str(computed)
            mode = "content"
            reason = None
        token = (
            f"{base}#member={artifact.member}" if artifact.member is not None else base
        )
        return token, mode, reason

    @staticmethod
    def _validate_binding_roles(
        inputs: Sequence["Artifact"],
        binding_roles: Sequence["InputBindingRole"],
    ) -> None:
        if len(binding_roles) != len(inputs):
            raise ValueError(
                "input binding role protocol mismatch: role count does not "
                "match input count"
            )
        if sorted(role.input_index for role in binding_roles) != list(
            range(len(inputs))
        ):
            raise ValueError(
                "input binding role protocol mismatch: roles must cover each "
                "input index exactly once"
            )

    @staticmethod
    def _format_binding_role(role: "InputBindingRole") -> str:
        return f"{role.kind}:{role.role}"

    def describe_input_bindings(
        self,
        inputs: Sequence["Artifact"],
        *,
        binding_roles: Sequence["InputBindingRole"],
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], Optional[str]]] = None,
    ) -> list[dict[str, Any]]:
        """Build diagnostic binding metadata for ``run.meta['input_identity']``."""
        self._validate_binding_roles(inputs, binding_roles)
        records: list[dict[str, Any]] = []
        for role in binding_roles:
            artifact = inputs[role.input_index]
            token, mode, fallback_reason = self._resolve_input_token(
                artifact,
                path_resolver=path_resolver,
                signature_lookup=signature_lookup,
            )
            evidence: dict[str, Any] = {"identity_strength": mode}
            if fallback_reason is not None:
                evidence["fallback_reason"] = fallback_reason
            records.append(
                {
                    "kind": role.kind,
                    "role": role.role,
                    "mode": mode,
                    "value": token,
                    "artifact_id": str(artifact.id),
                    "selector": {
                        "driver": artifact.driver,
                        "member": artifact.member,
                    },
                    "evidence": evidence,
                }
            )
        return records

    def compute_input_hash(
        self,
        inputs: Sequence["Artifact"],
        path_resolver: Optional[Callable[[str], str]] = None,
        signature_lookup: Optional[Callable[[str], Optional[str]]] = None,
        binding_roles: Optional[Sequence["InputBindingRole"]] = None,
        strict_binding_identity: Optional[str] = None,
    ) -> str:
        """
        Compute the canonical ``v0.5`` SHA-256 hash over sorted ``(role, token)`` pairs.
        """
        if strict_binding_identity is not None and (
            not isinstance(strict_binding_identity, str)
            or not strict_binding_identity.strip()
        ):
            raise ValueError("strict binding identity must not be empty")

        pairs: list[tuple[str, str]] = []
        if binding_roles is not None:
            self._validate_binding_roles(inputs, binding_roles)
            for role in binding_roles:
                artifact = inputs[role.input_index]
                token, _mode, _reason = self._resolve_input_token(
                    artifact,
                    path_resolver=path_resolver,
                    signature_lookup=signature_lookup,
                )
                pairs.append((self._format_binding_role(role), token))
        else:
            for artifact in inputs:
                token, _mode, _reason = self._resolve_input_token(
                    artifact,
                    path_resolver=path_resolver,
                    signature_lookup=signature_lookup,
                )
                pairs.append((artifact.key, token))

        if strict_binding_identity is not None:
            pairs.append(
                ("__strict_binding__", f"strict_binding_v1:{strict_binding_identity}")
            )

        pairs.sort()
        return canonical_json_sha256(pairs)

    # --- Internal Utilities ---

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
                        self._update_hash_from_file(sha256, p)
                return sha256.hexdigest()

        # Single File Handling
        if self.hashing_strategy == "fast":
            stat = path.stat()
            meta_str = f"{stat.st_size}_{stat.st_mtime_ns}"
            return hashlib.sha256(meta_str.encode("utf-8")).hexdigest()

        else:
            return file_sha256(path)

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
                    self._update_hash_from_file(sha, file_path)
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
            self._update_hash_from_file(sha, file_path)
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
            hashlib.file_digest(f, lambda: sha)

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
