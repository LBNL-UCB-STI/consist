import os
from pathlib import Path
from typing import Optional, Union, Dict


class FileSystemManager:
    """
    Service responsible for File System abstractions.

    It handles:
    1. Managing the root run directory.
    2. Managing "Mounts" (virtualizing paths).
    3. bidirectional conversion between Absolute Paths and Portable URIs.
    """

    def __init__(
        self, run_dir: Union[str, Path], mounts: Optional[Dict[str, str]] = None
    ):
        """
        Args:
            run_dir: The root directory for run logs and outputs.
            mounts: Dictionary mapping schemes (e.g., 'inputs') to absolute paths.
        """
        # Force absolute resolve to prevent /var vs /private/var mismatches
        self.run_dir = Path(run_dir).resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.mounts = mounts or {}

    def resolve_uri(self, uri: str) -> str:
        """
        Converts a portable Consist URI back into an absolute file system path.
        Inverse of virtualize_path.
        """
        path_str = uri

        # 1. Check schemes (mounts)
        if "://" in uri:
            scheme, rel_path = uri.split("://", 1)
            if scheme in self.mounts:
                path_str = str(Path(self.mounts[scheme]) / rel_path)
            elif scheme == "file":
                path_str = rel_path

        # 2. Check relative run paths
        elif uri.startswith("./"):
            path_str = str(self.run_dir / uri[2:])

        # Ensure we always return absolute, resolved paths
        return str(Path(path_str).resolve())

    def virtualize_path(self, path: str) -> str:
        """
        Converts an absolute file system path into a portable Consist URI.
        Attempts to match configured mounts or make relative to run_dir.
        """
        abs_path = str(Path(path).resolve())

        # 1. Check Mounts (Sort by length descending to match most specific mount first)
        for name, root in sorted(
            self.mounts.items(), key=lambda x: len(x[1]), reverse=True
        ):
            root_abs = str(Path(root).resolve())
            if abs_path.startswith(root_abs):
                return f"{name}://{os.path.relpath(abs_path, root_abs)}"

        # 2. Check Run Directory
        try:
            rel = os.path.relpath(abs_path, self.run_dir)
            if not rel.startswith(".."):
                return f"./{rel}"
        except ValueError:
            pass

        # 3. Fallback to absolute
        return abs_path

    def scan_directory(
        self, directory: Union[str, Path], pattern: str = "*", recursive: bool = False
    ) -> Dict[Path, int]:
        """
        Scans a directory and returns a map of {Path: mtime_ns}.
        Used for change detection (capture_outputs).
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
