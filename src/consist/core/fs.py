import os
import time
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional, Union, cast

from consist.location import (
    ArtifactLocationRow,
    LocationSite,
    LocationStatus,
    Witness,
    capture_witness,
    workspace_mount_instance_id,
)


def capture_artifact_witness(
    path: str | Path,
    artifact: Any,
    *,
    observed_at_ns: Optional[int] = None,
) -> Optional[Witness]:
    """Capture a ``FileStat`` or ``DirTreeStat`` witness matching ``artifact``'s layout."""
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
    driver = artifact.driver if isinstance(artifact.driver, str) else None
    path_obj = Path(path)
    is_dir = bool(
        meta.get("is_dir")
        or meta.get("directory_artifact") is True
        or meta.get("artifact_set") is True
        or driver in {"zarr", "directory", "artifact_set"}
        or path_obj.is_dir()
    )
    fb_manifest = meta.get("file_bundle_manifest")
    is_shp = bool(
        driver == "shapefile"
        or meta.get("file_bundle_artifact") is True
        or (
            isinstance(fb_manifest, Mapping)
            and fb_manifest.get("bundle_kind") == "shapefile"
        )
        or path_obj.suffix.lower() == ".shp"
    )
    entry_name: Optional[str] = None
    if is_shp:
        raw_entry = meta.get("file_bundle_entry") or (
            fb_manifest.get("entry") if isinstance(fb_manifest, Mapping) else None
        )
        if isinstance(raw_entry, str) and raw_entry:
            entry_name = raw_entry
        elif path_obj.is_file():
            entry_name = path_obj.name
        elif isinstance(artifact.uri, str) and artifact.uri.lower().endswith(".shp"):
            entry_name = Path(artifact.uri.split("://", 1)[-1]).name
    return capture_witness(
        path,
        driver=driver,
        is_directory=is_dir,
        is_shapefile_bundle=is_shp,
        bundle_entry=entry_name,
        observed_at_ns=observed_at_ns,
    )


def build_verified_location_row(
    fs: "FileSystemManager",
    artifact: Any,
    *,
    local_path: str | Path | None = None,
    uri: str | None = None,
    archive_root: str | os.PathLike[str] | None = None,
    site: Optional[LocationSite] = None,
    status: LocationStatus = "active",
    capture_local_witness: bool = True,
    observed_at_ns: Optional[int] = None,
) -> ArtifactLocationRow:
    """Construct a canonical ``ArtifactLocationRow`` (with optional stat witness)."""
    obs_ns = time.time_ns() if observed_at_ns is None else int(observed_at_ns)
    target_uri = uri if uri is not None else str(artifact.uri)
    if archive_root is not None:
        m_name, m_inst, canonical_uri, target_path = fs.archive_coordinate_for_root(
            target_uri, archive_root
        )
        effective_site: LocationSite = site if site is not None else "archive"
        resolved_local: Optional[Path] = (
            Path(local_path) if local_path is not None else target_path
        )
    else:
        m_name, m_inst, canonical_uri = fs.location_coordinate_for_uri(target_uri)
        effective_site = (
            site
            if site is not None
            else ("workspace" if m_name in ("workspace", "runs") else "external")
        )
        resolved_local = Path(local_path) if local_path is not None else None

    witness: Optional[Witness] = None
    if (
        capture_local_witness
        and resolved_local is not None
        and not str(resolved_local).startswith("/__unmounted__/")
    ):
        witness = capture_artifact_witness(
            resolved_local, artifact, observed_at_ns=obs_ns
        )

    return ArtifactLocationRow.create(
        digest=str(artifact.digest) if artifact.digest is not None else None,
        artifact_id=artifact.id,
        site=effective_site,
        mount_name=m_name,
        mount_instance_id=m_inst,
        uri=canonical_uri,
        member=artifact.member or "",
        status=status,
        witness=witness,
        observed_at_ns=obs_ns,
    )


def resolve_historical_artifact_path(
    fs: Any,
    artifact: Any,
    run: Any,
    *,
    fallback_to_current_uri: bool = True,
) -> Optional[Path]:
    """Resolve an artifact's historical path using metadata from ``run`` and ``artifact``."""
    run_meta = run.meta if run is not None and isinstance(run.meta, Mapping) else {}
    original_run_dir_raw = run_meta.get("_physical_run_dir")
    original_run_dir = (
        original_run_dir_raw
        if isinstance(original_run_dir_raw, str) and original_run_dir_raw
        else None
    )
    raw_mounts = run_meta.get("mounts")
    mounts_snapshot = (
        {
            str(k): str(v)
            for k, v in raw_mounts.items()
            if isinstance(k, str) and isinstance(v, str)
        }
        if isinstance(raw_mounts, Mapping)
        else None
    )
    artifact_meta = (
        artifact.meta
        if artifact is not None and isinstance(artifact.meta, Mapping)
        else {}
    )
    raw_mount_root = artifact_meta.get("mount_root")
    artifact_mount_root = (
        raw_mount_root if isinstance(raw_mount_root, str) and raw_mount_root else None
    )
    if not fallback_to_current_uri and not (
        original_run_dir or mounts_snapshot or artifact_mount_root
    ):
        return None
    return Path(
        fs.resolve_historical_path(
            artifact.uri,
            original_run_dir,
            mounts_snapshot=mounts_snapshot,
            artifact_mount_root=artifact_mount_root,
        )
    )


class FileSystemManager:
    """
    Virtualizes file system paths for run portability and capture helpers.

    Responsibilities include run directory management, mount-aware URIs, and
    helpers for resolving historical artifact locations during hydration.
    """

    def __init__(
        self,
        run_dir: Optional[Union[str, Path]] = None,
        mounts: Optional[Dict[str, str]] = None,
        *,
        create_run_dir: bool = True,
    ):
        """
        Initialize run directory and optional mounts.

        Parameters
        ----------
        run_dir : Optional[Union[str, Path]], optional
            Directory where run logs and artifacts are stored. When ``None``,
            falls back to the current working directory without creating a
            directory on disk (used by Ephemeral Mode).
        mounts : Optional[Dict[str, str]], optional
            Mapping from scheme name (e.g., ``"inputs"``) to absolute path.
        create_run_dir : bool, default True
            Whether to create ``run_dir`` on disk during initialization.
        """
        self._has_explicit_run_dir = run_dir is not None
        # Force absolute resolve to prevent /var vs /private/var mismatches
        self.run_dir = (
            Path(run_dir).resolve() if run_dir is not None else Path.cwd().resolve()
        )
        if create_run_dir and self._has_explicit_run_dir:
            self.run_dir.mkdir(parents=True, exist_ok=True)
        self.mounts = mounts or {}
        self._ws_id_cache: Optional[tuple[Path, str]] = None
        self._mounts_key_cache: Optional[
            tuple[tuple[tuple[str, str], ...], tuple[tuple[str, str], ...]]
        ] = None

    def resolve_uri(self, uri: str) -> str:
        """
        Convert a Consist URI into an absolute file-system path.

        Parameters
        ----------
        uri : str
            Portable URI created via ``virtualize_path``.

        Returns
        -------
        str
            Absolute resolved path on the local filesystem.
            For mounted URIs, Consist rejects path traversal that escapes the mount root.
        """
        path_str = uri

        # 1. Check schemes (mounts)
        if "://" in uri:
            scheme, rel_path = uri.split("://", 1)
            if scheme in self.mounts:
                mount_root = Path(self.mounts[scheme]).resolve()
                resolved = (mount_root / rel_path).resolve()
                try:
                    resolved.relative_to(mount_root)
                except ValueError as exc:
                    raise ValueError(
                        f"Path traversal detected in URI {uri!r}. "
                        f"Resolved to {resolved}, outside mount {mount_root}."
                    ) from exc
                path_str = str(resolved)
            elif scheme == "workspace":
                workspace_root = self.run_dir.resolve()
                resolved = (workspace_root / rel_path).resolve()
                try:
                    resolved.relative_to(workspace_root)
                except ValueError as exc:
                    raise ValueError(
                        f"Path traversal detected in URI {uri!r}. "
                        f"Resolved to {resolved}, outside mount {workspace_root}."
                    ) from exc
                path_str = str(resolved)
            elif scheme == "file":
                path_str = rel_path

        # 2. Check relative run paths
        elif uri.startswith("./"):
            path_str = str(self.run_dir / uri[2:])

        # Ensure we always return absolute, resolved paths
        return str(Path(path_str).resolve())

    def virtualize_path(self, path: str) -> str:
        """
        Create a portable URI for the supplied path, honoring mounts and workspace.

        Parameters
        ----------
        path : str
            Absolute path to transform into a Consist URI.

        Returns
        -------
        str
            URI referencing the path via a mount scheme or relative workspace path.
        """
        path_obj = Path(path)

        # 1. Identify Candidates
        # Candidate A: The Logical Path (Absolute, but symlinks preserved)
        # This matches paths like 'PILATES/pilates/beam/skims.omx' -> 'inputs://...'
        candidates = [path_obj.absolute()]

        # Candidate B: The Physical Path (Symlinks followed)
        # This matches paths where Python code already resolved the symlink
        resolved_path = path_obj.resolve()
        if resolved_path != candidates[0]:
            candidates.append(resolved_path)

        # 2. Sort mounts by specificity (longest paths first)
        sorted_mounts = sorted(
            self.mounts.items(), key=lambda x: len(x[1]), reverse=True
        )

        # 3. Check Candidates against Mounts
        for candidate in candidates:
            # Convert to string for matching against string mounts
            candidate_abs = str(candidate)

            for name, root in sorted_mounts:
                root_path = Path(root).resolve()
                root_str = str(root_path)

                # Check if candidate is inside root
                # (Logic handles /data vs /database prefix issues)
                if candidate_abs.startswith(root_str):
                    if (
                        len(candidate_abs) == len(root_str)
                        or candidate_abs[len(root_str)] == os.sep
                    ):
                        rel_path = os.path.relpath(candidate_abs, root_str)
                        # Normalize slashes for portable URI
                        return f"{name}://{Path(rel_path).as_posix()}"

        # 4. Check Run Directory (Workspace)
        # We use the resolved path for workspace checks to ensure we aren't
        # accidentally aliasing the workspace itself.
        try:
            rel = resolved_path.relative_to(self.run_dir.resolve())
            return f"./{rel.as_posix()}"
        except ValueError:
            return str(resolved_path)

        # 5. Fallback to absolute
        return str(resolved_path)

    def scan_directory(
        self, directory: Union[str, Path], pattern: str = "*", recursive: bool = False
    ) -> Dict[Path, int]:
        """
        Collect modification timestamps for files in a directory.

        Parameters
        ----------
        directory : Union[str, Path]
            Directory to traverse.
        pattern : str, default "*"
            Glob pattern to match files.
        recursive : bool, default False
            Whether to scan subdirectories recursively.

        Returns
        -------
        Dict[Path, int]
            Mapping from file paths to their last modification time (ns).
        """
        dir_path = Path(directory).resolve()
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)

        files = {}
        iterator = dir_path.rglob(pattern) if recursive else dir_path.glob(pattern)

        for f in iterator:
            if f.is_file():
                files[f] = f.stat().st_mtime_ns
        return files

    def resolve_historical_path(
        self,
        uri: str,
        original_run_dir: Optional[str | Path],
        *,
        mounts_snapshot: Optional[Mapping[str, str]] = None,
        artifact_mount_root: Optional[str] = None,
    ) -> str:
        """
        Resolve an artifact URI relative to a historical run directory.

        Parameters
        ----------
        uri : str
            Portable URI recorded by a previous run.
        original_run_dir : Optional[str | Path]
            Absolute workspace path of the original run.
        mounts_snapshot : Optional[Mapping[str, str]], optional
            Historical mount table recorded in ``run.meta["mounts"]``.
        artifact_mount_root : Optional[str], optional
            Historical mount root recorded in ``artifact.meta["mount_root"]``.

        Returns
        -------
        str
            Absolute path where the artifact lived during the historical run.
        """
        if "://" in uri:
            scheme, rel_raw = uri.split("://", 1)
            if scheme != "file":
                rel_path = rel_raw.lstrip("/")
                if mounts_snapshot and isinstance(mounts_snapshot.get(scheme), str):
                    return str(Path(mounts_snapshot[scheme]) / rel_path)
                if artifact_mount_root:
                    return str(Path(artifact_mount_root) / rel_path)

        # Case 1: Workspace scheme (Ephemeral)
        # If the URI is virtualized to the workspace, its physical location
        # depends entirely on where that run happened unless "workspace" is
        # explicitly mounted in the current config.
        if (
            uri.startswith("workspace://")
            and original_run_dir
            and "workspace" not in self.mounts
        ):
            rel_path = uri.replace("workspace://", "").lstrip("/")
            return str(Path(original_run_dir) / rel_path)

        # Case 2: Relative path (Ephemeral)
        # If it was stored as relative ("./outputs/foo.csv"), it's relative to that run's root.
        if uri.startswith("./") and original_run_dir:
            return str(Path(original_run_dir) / uri[2:])

        # Case 3: Shared Mounts or Absolute Paths
        # If it's "inputs://data.csv" and "inputs" points to a shared /data drive
        # that is mounted in the current config, standard resolution works fine.
        return self.resolve_uri(uri)

    def resolve_historical_artifact_path(
        self,
        artifact: Any,
        run: Any,
        *,
        fallback_to_current_uri: bool = True,
    ) -> Optional[Path]:
        """Resolve an artifact's historical path using metadata from ``run`` and ``artifact``."""
        return resolve_historical_artifact_path(
            self,
            artifact,
            run,
            fallback_to_current_uri=fallback_to_current_uri,
        )

    @staticmethod
    def normalize_recovery_roots(
        roots: str | os.PathLike[str] | Sequence[str | os.PathLike[str]] | None,
        *,
        resolve: bool = True,
    ) -> list[str]:
        """Normalize advisory recovery roots into a deduplicated ordered list.

        Parameters
        ----------
        roots : path-like, sequence of path-like, or None
            Roots to normalize. ``None`` produces an empty list.
        resolve : bool, default True
            Whether to resolve symlinks. Set ``False`` when a caller must
            inspect raw path components before following them.

        Returns
        -------
        list[str]
            Absolute normalized roots in first-occurrence order.

        Raises
        ------
        ValueError
            If ``roots`` or an entry is not path-like.
        """
        if roots is None:
            return []

        raw_roots: list[str | os.PathLike[str]] | Sequence[str | os.PathLike[str]]
        if isinstance(roots, (str, os.PathLike)):
            raw_roots = (cast(str | os.PathLike[str], roots),)
        elif isinstance(roots, Sequence):
            raw_roots = roots
        else:
            raise ValueError(
                "recovery_roots must be a path-like value or a sequence of path-like values."
            )

        normalized: list[str] = []
        seen: set[str] = set()
        for root in raw_roots:
            if not isinstance(root, (str, os.PathLike)):
                raise ValueError(
                    "recovery_roots entries must be strings or pathlib.Path values."
                )
            raw_str = os.fspath(root)
            if isinstance(root, str) and "://" in raw_str:
                scheme, rest = raw_str.split("://", 1)
                if scheme == "file":
                    normalized_root = (
                        Path(rest).resolve() if resolve else Path(rest).absolute()
                    )
                    root_string = str(normalized_root)
                else:
                    root_string = f"{scheme}://{rest.strip('/')}"
            else:
                normalized_root = (
                    Path(root).resolve() if resolve else Path(root).absolute()
                )
                root_string = str(normalized_root)
            if root_string not in seen:
                seen.add(root_string)
                normalized.append(root_string)
        return normalized

    @staticmethod
    def normalize_remappable_relative_path(rel_path: str) -> Optional[Path]:
        """
        Validate and normalize a remappable URI-relative path.

        Returns ``None`` for absolute or path-traversing values because those do
        not have a safe, portable layout contract for rematerialization.
        """
        candidate = rel_path.strip()
        if not candidate:
            return None

        pure_path = PurePosixPath(candidate)
        if pure_path.is_absolute():
            return None
        if ".." in pure_path.parts:
            return None

        normalized_parts = [part for part in pure_path.parts if part not in {"", "."}]
        if not normalized_parts:
            return None
        return Path(*normalized_parts)

    def get_remappable_relative_path(
        self,
        uri: str,
        *,
        scheme_prefixes: Optional[Mapping[str, Union[str, Path]]] = None,
    ) -> Optional[Path]:
        """
        Derive a portable relative layout for a rematerializable artifact URI.
        """
        if uri.startswith("workspace://"):
            rel_candidate = uri.replace("workspace://", "", 1).lstrip("/")
            if scheme_prefixes is not None and "workspace" in scheme_prefixes:
                prefix = scheme_prefixes["workspace"]
                return self.normalize_remappable_relative_path(
                    str(Path(prefix) / rel_candidate)
                )
            return self.normalize_remappable_relative_path(rel_candidate)

        if uri.startswith("./"):
            return self.normalize_remappable_relative_path(uri[2:])

        if "://" not in uri:
            return None

        scheme, rel_path = uri.split("://", 1)
        if scheme == "file":
            return None

        if scheme == "workspace":
            rel_candidate = rel_path.lstrip("/")
            if scheme_prefixes is not None and "workspace" in scheme_prefixes:
                prefix = scheme_prefixes["workspace"]
                return self.normalize_remappable_relative_path(
                    str(Path(prefix) / rel_candidate)
                )
            return self.normalize_remappable_relative_path(rel_candidate)

        prefix: Optional[Union[str, Path]] = None
        if scheme_prefixes is not None and scheme in scheme_prefixes:
            prefix = scheme_prefixes[scheme]
        elif scheme in self.mounts and self.run_dir:
            try:
                mount_resolved = Path(self.mounts[scheme]).resolve()
                run_resolved = Path(self.run_dir).resolve()
                rel = mount_resolved.relative_to(run_resolved)
                if rel != Path("."):
                    prefix = rel
            except (ValueError, RuntimeError):
                pass

        if prefix is not None:
            clean_rel = rel_path.lstrip("/")
            candidate = Path(prefix) / clean_rel
            return self.normalize_remappable_relative_path(str(candidate))

        if scheme_prefixes is not None:
            return None

        return self.normalize_remappable_relative_path(rel_path)

    def get_historical_root(
        self,
        uri: str,
        *,
        original_run_dir: Optional[str],
        mounts_snapshot: Optional[Mapping[str, str]] = None,
        artifact_mount_root: Optional[str] = None,
        resolve: bool = True,
    ) -> Optional[Path]:
        """Resolve the most specific recorded historical root for a URI.

        Parameters
        ----------
        uri : str
            Artifact container URI whose owning root is requested.
        original_run_dir : str | None
            Historical workspace root for ``workspace://`` and relative URIs.
        mounts_snapshot : mapping of str to str | None, optional
            Historical mount roots recorded by URI scheme.
        artifact_mount_root : str | None, optional
            Artifact-specific fallback mount root.
        resolve : bool, default True
            Whether to resolve symlinks in the returned root. Set ``False``
            when a caller needs to apply its own symlink policy.

        Returns
        -------
        Path | None
            Most specific applicable root, or ``None`` for unmappable URIs.
        """
        if uri.startswith("./"):
            if not original_run_dir:
                return None
            path = Path(original_run_dir)
            return path.resolve() if resolve else path.absolute()

        if "://" not in uri:
            return None

        scheme, _ = uri.split("://", 1)
        if scheme == "file":
            return None

        if mounts_snapshot and isinstance(mounts_snapshot.get(scheme), str):
            mount_path = Path(mounts_snapshot[scheme])
            if original_run_dir and scheme != "workspace":
                try:
                    orig_dir_path = Path(original_run_dir)
                    rel = mount_path.resolve().relative_to(orig_dir_path.resolve())
                    if rel != Path("."):
                        return (
                            orig_dir_path.resolve()
                            if resolve
                            else orig_dir_path.absolute()
                        )
                except (ValueError, RuntimeError):
                    pass
            return mount_path.resolve() if resolve else mount_path.absolute()

        if artifact_mount_root:
            mount_path = Path(artifact_mount_root)
            if original_run_dir and scheme != "workspace":
                try:
                    orig_dir_path = Path(original_run_dir)
                    rel = mount_path.resolve().relative_to(orig_dir_path.resolve())
                    if rel != Path("."):
                        return (
                            orig_dir_path.resolve()
                            if resolve
                            else orig_dir_path.absolute()
                        )
                except (ValueError, RuntimeError):
                    pass
            return mount_path.resolve() if resolve else mount_path.absolute()

        if scheme == "workspace" and original_run_dir:
            path = Path(original_run_dir)
            return path.resolve() if resolve else path.absolute()

        return None

    def get_historical_remap(
        self,
        uri: str,
        *,
        original_run_dir: Optional[str],
        mounts_snapshot: Optional[Dict[str, str]] = None,
        artifact_mount_root: Optional[str] = None,
        scheme_prefixes: Optional[Mapping[str, Union[str, Path]]] = None,
    ) -> Optional[tuple[Path, Path]]:
        """Derive a historical root plus portable relative URI layout.

        Parameters
        ----------
        uri : str
            Artifact container URI to remap.
        original_run_dir : str | None
            Historical workspace root for workspace-relative URIs.
        mounts_snapshot : dict[str, str] | None, optional
            Historical mount roots recorded by URI scheme.
        artifact_mount_root : str | None, optional
            Artifact-specific fallback mount root.
        scheme_prefixes : mapping of str to str | Path, optional
            Prefix overrides by scheme for relative path derivation.

        Returns
        -------
        tuple[Path, Path] | None
            Historical root and safe relative layout, or ``None`` when the URI
            is not recovery-remappable.

        Notes
        -----
        This is stricter than :meth:`resolve_historical_path` because it only
        returns layouts safe for recovery-oriented materialization.
        """
        relative_path = self.get_remappable_relative_path(
            uri, scheme_prefixes=scheme_prefixes
        )
        if relative_path is None:
            return None

        historical_root = self.get_historical_root(
            uri,
            original_run_dir=original_run_dir,
            mounts_snapshot=mounts_snapshot,
            artifact_mount_root=artifact_mount_root,
        )
        if historical_root is None:
            return None

        return historical_root, relative_path

    @property
    def workspace_instance_id(self) -> str:
        """Return deterministic 16-hex instance ID for ``self.run_dir``."""
        cached = self._ws_id_cache
        if cached is not None and cached[0] == self.run_dir:
            return cached[1]
        ws_id = workspace_mount_instance_id(self.run_dir)
        self._ws_id_cache = (self.run_dir, ws_id)
        return ws_id

    @property
    def sorted_mounts_key(self) -> tuple[tuple[str, str], ...]:
        """Return canonical sorted ``(name, realpath)`` tuple for memoization."""
        raw_snapshot = tuple((str(k), str(v)) for k, v in self.mounts.items())
        cached = self._mounts_key_cache
        if cached is not None and cached[0] == raw_snapshot:
            return cached[1]
        computed = tuple(
            sorted(
                (name, os.path.realpath(Path(root).expanduser()))
                for name, root in raw_snapshot
            )
        )
        self._mounts_key_cache = (raw_snapshot, computed)
        return computed

    def location_coordinate_for_uri(self, uri: str) -> tuple[str, str, str]:
        """Map an artifact URI to ``(mount_name, mount_instance_id, canonical_uri)``."""
        if "://" in uri:
            scheme, rel_raw = uri.split("://", 1)
            if scheme in ("workspace", "runs"):
                inst_id = (
                    workspace_mount_instance_id(self.mounts[scheme])
                    if scheme in self.mounts
                    else self.workspace_instance_id
                )
                return scheme, inst_id, f"{scheme}://{rel_raw.lstrip('/')}"
            if scheme in self.mounts:
                return scheme, "", f"{scheme}://{rel_raw.lstrip('/')}"
            if scheme == "file":
                return "abs", "", f"file://{rel_raw}"
            return scheme, "", f"{scheme}://{rel_raw.lstrip('/')}"

        if uri.startswith("./"):
            mount_name = "runs" if "workspace" in self.mounts else "workspace"
            return mount_name, self.workspace_instance_id, uri

        path_obj = Path(uri)
        if path_obj.is_absolute():
            return "abs", "", f"file://{path_obj}"
        mount_name = "runs" if "workspace" in self.mounts else "workspace"
        return mount_name, self.workspace_instance_id, f"./{uri.lstrip('/')}"

    def archive_coordinate_for_root(
        self,
        artifact_uri: str,
        root: str | os.PathLike[str],
        *,
        resolve: bool = True,
    ) -> tuple[str, str, str, Path]:
        """Map an archive/recovery root + artifact URI to ``(mount_name, '', uri, target_path)``."""
        rel = self.get_remappable_relative_path(artifact_uri) if artifact_uri else None
        raw_root_str = os.fspath(root)
        if "://" in raw_root_str:
            scheme, root_rel_raw = raw_root_str.split("://", 1)
            if scheme == "file":
                raw_root_str = root_rel_raw
            else:
                clean_root_rel = root_rel_raw.strip("/")
                rel_posix = rel.as_posix() if rel is not None else ""
                combined = "/".join(p for p in (clean_root_rel, rel_posix) if p)
                uri_str = f"{scheme}://{combined}"
                if scheme in self.mounts:
                    mount_base = (
                        Path(self.mounts[scheme]).resolve()
                        if resolve
                        else Path(self.mounts[scheme]).absolute()
                    )
                    target = (mount_base / combined) if combined else mount_base
                else:
                    target = Path(f"/__unmounted__/{scheme}/{combined}")
                return scheme, "", uri_str, target

        root_path = (
            Path(raw_root_str).resolve() if resolve else Path(raw_root_str).absolute()
        )
        target_path = (root_path / rel) if rel is not None else root_path
        target_str = str(target_path)

        sorted_mounts = sorted(
            (
                (name, Path(mount_root).resolve())
                for name, mount_root in self.mounts.items()
                if name not in ("workspace", "runs")
            ),
            key=lambda item: len(str(item[1])),
            reverse=True,
        )
        for name, mount_root in sorted_mounts:
            mount_str = str(mount_root)
            if target_str == mount_str:
                return name, "", f"{name}://", target_path
            if target_str.startswith(mount_str + os.sep):
                rel_in_mount = Path(os.path.relpath(target_str, mount_str)).as_posix()
                return name, "", f"{name}://{rel_in_mount}", target_path

        return "abs", "", f"file://{target_str}", target_path

    def resolve_location_coordinate(
        self,
        mount_name: str,
        mount_instance_id: str,
        uri: str,
        *,
        historical_run_dir: str | Path | None = None,
    ) -> Optional[Path]:
        """Resolve ``(mount_name, mount_instance_id, uri)`` to a host ``Path`` or ``None``."""
        if mount_name == "abs":
            raw = uri[len("file://") :] if uri.startswith("file://") else uri
            return Path(raw) if raw else None

        if mount_name in ("workspace", "runs"):
            if uri.startswith("workspace://"):
                rel = uri[len("workspace://") :].lstrip("/")
            elif uri.startswith("./"):
                rel = uri[2:]
            elif "://" in uri:
                rel = uri.split("://", 1)[1].lstrip("/")
            else:
                rel = uri

            base_root: Optional[Path] = None
            if mount_name in self.mounts:
                mount_path = Path(self.mounts[mount_name]).expanduser()
                if mount_instance_id in ("", workspace_mount_instance_id(mount_path)):
                    base_root = Path(os.path.realpath(mount_path))
                elif historical_run_dir is not None:
                    hist_path = Path(historical_run_dir).expanduser()
                    if workspace_mount_instance_id(hist_path) == mount_instance_id:
                        base_root = Path(os.path.realpath(hist_path))
            elif mount_instance_id in ("", self.workspace_instance_id):
                base_root = Path(os.path.realpath(self.run_dir))
            elif historical_run_dir is not None:
                hist_path = Path(historical_run_dir).expanduser()
                if workspace_mount_instance_id(hist_path) == mount_instance_id:
                    base_root = Path(os.path.realpath(hist_path))

            if base_root is None:
                return None
            candidate = base_root / rel
            resolved_candidate = (
                Path(os.path.realpath(candidate.parent)) / candidate.name
            )
            try:
                resolved_candidate.relative_to(base_root)
            except ValueError:
                return None
            return resolved_candidate

        if mount_name in self.mounts or historical_run_dir is not None:
            raw_mount_root = (
                historical_run_dir
                if historical_run_dir is not None
                else self.mounts[mount_name]
            )
            mount_root = Path(os.path.realpath(Path(raw_mount_root).expanduser()))
            rel = (
                uri.split("://", 1)[1].lstrip("/") if "://" in uri else uri.lstrip("./")
            )
            candidate = mount_root / rel if rel else mount_root
            resolved = (
                Path(os.path.realpath(candidate.parent)) / candidate.name
                if rel
                else mount_root
            )
            try:
                resolved.relative_to(mount_root)
            except ValueError:
                return None
            return resolved

        return None

    def resolve_location_row_path(
        self,
        row: ArtifactLocationRow,
        *,
        historical_run_dir: str | Path | None = None,
    ) -> Optional[Path]:
        """Resolve an ``ArtifactLocationRow`` to a host ``Path`` or ``None`` if unmounted/foreign."""
        return self.resolve_location_coordinate(
            row.mount_name,
            row.mount_instance_id,
            row.uri,
            historical_run_dir=historical_run_dir,
        )

    def extract_recovery_root_from_location_row(
        self,
        artifact_uri: str,
        row: ArtifactLocationRow,
        *,
        resolve: bool = True,
    ) -> Optional[str]:
        """Reconstruct the normalized recovery root directory or URI from an archive/advisory row."""
        rel = self.get_remappable_relative_path(artifact_uri)
        target = self.resolve_location_row_path(row)
        if target is None:
            if "://" not in row.uri or row.mount_name in ("workspace", "runs", "abs"):
                return None
            scheme, uri_rel = row.uri.split("://", 1)
            clean_rel = uri_rel.strip("/")
            uri_parts = PurePosixPath(clean_rel).parts if clean_rel else ()
            rel_parts = rel.parts if rel is not None else ()
            if (
                rel_parts
                and len(uri_parts) >= len(rel_parts)
                and uri_parts[-len(rel_parts) :] == rel_parts
            ):
                prefix = "/".join(uri_parts[: -len(rel_parts)])
            else:
                prefix = "/".join(uri_parts)
            return f"{scheme}://{prefix}"
        resolved_target = (
            Path(os.path.realpath(target)) if resolve else target.absolute()
        )
        if rel is not None and len(rel.parts) > 0:
            if len(resolved_target.parts) > len(rel.parts) and (
                resolved_target.parts[-len(rel.parts) :] == rel.parts
            ):
                return str(resolved_target.parents[len(rel.parts) - 1])
        return str(resolved_target)
