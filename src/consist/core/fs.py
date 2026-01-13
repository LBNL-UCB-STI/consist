import os
from pathlib import Path
from typing import Optional, Union, Dict


class FileSystemManager:
    """
    Virtualizes file system paths for run portability and capture helpers.

    Responsibilities include run directory management, mount-aware URIs, and
    helpers for resolving historical artifact locations during hydration.
    """

    def __init__(
        self, run_dir: Union[str, Path], mounts: Optional[Dict[str, str]] = None
    ):
        """
        Initialize run directory and optional mounts.

        Parameters
        ----------
        run_dir : Union[str, Path]
            Directory where run logs and artifacts are stored.
        mounts : Optional[Dict[str, str]], optional
            Mapping from scheme name (e.g., ``"inputs"``) to absolute path.
        """
        # Force absolute resolve to prevent /var vs /private/var mismatches
        self.run_dir = Path(run_dir).resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.mounts = mounts or {}

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
            pass

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

    def resolve_historical_path(self, uri: str, original_run_dir: Optional[str]) -> str:
        """
        Resolve an artifact URI relative to a historical run directory.

        Parameters
        ----------
        uri : str
            Portable URI recorded by a previous run.
        original_run_dir : Optional[str]
            Absolute workspace path of the original run.

        Returns
        -------
        str
            Absolute path where the artifact lived during the historical run.
        """
        # Case 1: Workspace scheme (Ephemeral)
        # If the URI is virtualized to the workspace, its physical location
        # depends entirely on where that run happened.
        if uri.startswith("workspace://") and original_run_dir:
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
