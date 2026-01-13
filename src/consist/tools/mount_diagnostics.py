from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class MountResolutionHint:
    """
    Diagnostic details for a missing artifact caused by mount misconfiguration.

    Attributes
    ----------
    scheme : str
        The URI scheme extracted from ``artifact.uri`` (e.g., ``"inputs"``).
    rel_path : str
        The path portion after ``scheme://``.
    configured_root : Optional[str]
        The current configured root for this scheme (if present).
    recorded_root : Optional[str]
        The root recorded at log time in ``artifact.meta["mount_root"]`` (if present).
    """

    scheme: str
    rel_path: str
    configured_root: Optional[str]
    recorded_root: Optional[str]


def parse_mount_uri(uri: str) -> Optional[tuple[str, str]]:
    """
    Parse a Consist mount URI into (scheme, rel_path).

    Parameters
    ----------
    uri : str
        An artifact URI, typically created by mount virtualization
        (e.g., ``inputs://data/raw.csv``).

    Returns
    -------
    Optional[tuple[str, str]]
        ``(scheme, rel_path)`` if ``uri`` looks like a scheme URI, else None.
    """
    if "://" not in uri:
        return None
    scheme, rel_path = uri.split("://", 1)
    if not scheme or not rel_path:
        return None
    rel_posix = PurePosixPath(rel_path)
    if rel_posix.is_absolute():
        return None
    if any(part == ".." for part in rel_posix.parts):
        return None
    return scheme, rel_posix.as_posix()


def build_mount_resolution_hint(
    uri: str,
    *,
    artifact_meta: Optional[Mapping[str, Any]] = None,
    mounts: Optional[Mapping[str, str]] = None,
) -> Optional[MountResolutionHint]:
    """
    Build a diagnostic hint for a mount-based URI that cannot be resolved on disk.

    Parameters
    ----------
    uri : str
        The artifact's persisted URI.
    artifact_meta : Optional[Mapping[str, Any]], optional
        Artifact metadata that may contain mount info recorded at log time.
    mounts : Optional[Mapping[str, str]], optional
        Current mount configuration (scheme -> absolute root).

    Returns
    -------
    Optional[MountResolutionHint]
        A populated hint when ``uri`` is mount-shaped, else None.
    """
    parsed = parse_mount_uri(uri)
    if parsed is None:
        return None

    scheme, rel_path = parsed
    configured_root = None
    if mounts and scheme in mounts:
        configured_root = str(Path(mounts[scheme]).resolve())

    recorded_root = None
    if artifact_meta:
        maybe_root = artifact_meta.get("mount_root")
        if isinstance(maybe_root, str) and maybe_root:
            recorded_root = maybe_root

    return MountResolutionHint(
        scheme=scheme,
        rel_path=rel_path,
        configured_root=configured_root,
        recorded_root=recorded_root,
    )


def format_missing_artifact_mount_help(
    hint: MountResolutionHint, *, resolved_path: Optional[str] = None
) -> str:
    """
    Format a short, actionable message for missing mount-backed artifacts.

    Parameters
    ----------
    hint : MountResolutionHint
        Diagnostic details produced by ``build_mount_resolution_hint``.
    resolved_path : Optional[str], optional
        The locally resolved absolute path that was checked and found missing.

    Returns
    -------
    str
        A human-readable, multi-line help message.
    """
    lines: list[str] = []
    if resolved_path:
        lines.append(f"Resolved path: {resolved_path}")

    if hint.configured_root is None:
        lines.append(
            f"Mount '{hint.scheme}://' is not configured in this Tracker (no root for scheme '{hint.scheme}')."
        )
        lines.append(
            "Fix: pass `mounts={...}` when creating the Tracker (or configure mounts for your CLI session)."
        )
        if hint.recorded_root:
            lines.append(f"Logged with mount_root={hint.recorded_root!r}.")
        return "\n".join(lines)

    expected = str(Path(hint.configured_root) / hint.rel_path)
    lines.append(f"Expected under mount root: {expected}")

    if hint.recorded_root and hint.recorded_root != hint.configured_root:
        lines.append(
            f"Mount root mismatch: logged mount_root={hint.recorded_root!r}, current mount_root={hint.configured_root!r}."
        )
        lines.append(
            "Fix: update your mounts so the scheme points at the same shared location used when the artifact was logged."
        )
    else:
        lines.append(
            "If this file exists elsewhere, your mounts may be pointing at the wrong location for this machine."
        )

    return "\n".join(lines)
