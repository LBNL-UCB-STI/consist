"""Unified artifact location candidate and mount precedence resolver.

Provides lower-layer candidate enumeration and mount precedence resolution
shared across tracker recovery, cache hydration/validation, materialization,
and archiving without circular imports.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeAlias

from consist.core.directory_artifacts import (
    file_bundle_entry_name,
    is_file_bundle_artifact,
)
from consist.core.fs import (
    FileSystemManager,
    _canonical_containment_target,
    _checked_uri_relative_parts,
)
from consist.location import ArtifactLocationRow, TranslatedCoordinate
from consist.models.artifact import Artifact
from consist.core.persistence import DatabaseManager

if TYPE_CHECKING:
    from consist.models.run import Run

CandidateSource: TypeAlias = Literal[
    "explicit_root",
    "runtime_abs",
    "ledger_primary",
    "ledger_historical",
    "historical_uri",
    "managed_mount",
    "stub_recovery_root",
]


@dataclass(frozen=True, slots=True)
class ResolvedCandidate:
    """A containment-checked filesystem candidate path for an artifact."""

    source: CandidateSource
    lexical_path: Path
    mount_canonical_lexical_path: Path
    canonical_path: Path
    entry_lexical_path: Path
    entry_canonical_path: Path
    row: ArtifactLocationRow | None = None


def _candidate_from_coordinate(
    coord: TranslatedCoordinate,
    source: CandidateSource,
    artifact: Artifact,
    *,
    row: ArtifactLocationRow | None = None,
) -> ResolvedCandidate | None:
    if not coord.is_resolved:
        return None
    assert coord.lexical_path is not None
    assert coord.mount_canonical_lexical_path is not None
    assert coord.canonical_path is not None

    lexical_path = coord.lexical_path
    mount_canonical = coord.mount_canonical_lexical_path
    canonical_path = coord.canonical_path

    entry_lexical = lexical_path
    entry_canonical = canonical_path
    if is_file_bundle_artifact(artifact) and canonical_path.is_dir():
        entry_name = file_bundle_entry_name(artifact)
        if entry_name is not None:
            entry_lexical = lexical_path / entry_name
            target_entry = canonical_path / entry_name
            entry_canonical = (
                target_entry.resolve()
                if target_entry.exists()
                else Path(os.path.realpath(canonical_path)) / entry_name
            )

    return ResolvedCandidate(
        source=source,
        lexical_path=lexical_path,
        mount_canonical_lexical_path=mount_canonical,
        canonical_path=canonical_path,
        entry_lexical_path=entry_lexical,
        entry_canonical_path=entry_canonical,
        row=row,
    )


def _candidate_from_root(
    root: Path | str,
    rel: Path,
    source: CandidateSource,
    artifact: Artifact,
    *,
    row: ArtifactLocationRow | None = None,
) -> ResolvedCandidate | None:
    lexical_root = Path(root).expanduser()
    if not lexical_root.is_absolute():
        lexical_root = lexical_root.resolve()
    canonical_root = Path(os.path.realpath(lexical_root))
    rel_parts = _checked_uri_relative_parts(rel.as_posix())
    if rel_parts is None:
        return None
    lexical_path = lexical_root.joinpath(*rel_parts) if rel_parts else lexical_root
    mount_canonical = (
        canonical_root.joinpath(*rel_parts) if rel_parts else canonical_root
    )
    target = _canonical_containment_target(mount_canonical)
    if target is None or not target.is_relative_to(canonical_root):
        return None
    canonical_path = (
        Path(os.path.realpath(mount_canonical.parent)) / mount_canonical.name
        if rel_parts
        else canonical_root
    )
    entry_lexical = lexical_path
    entry_canonical = canonical_path
    if is_file_bundle_artifact(artifact) and canonical_path.is_dir():
        entry_name = file_bundle_entry_name(artifact)
        if entry_name is not None:
            entry_lexical = lexical_path / entry_name
            target_entry = canonical_path / entry_name
            entry_canonical = (
                target_entry.resolve()
                if target_entry.exists()
                else Path(os.path.realpath(canonical_path)) / entry_name
            )

    return ResolvedCandidate(
        source=source,
        lexical_path=lexical_path,
        mount_canonical_lexical_path=mount_canonical,
        canonical_path=canonical_path,
        entry_lexical_path=entry_lexical,
        entry_canonical_path=entry_canonical,
        row=row,
    )


class MountPrecedenceResolver:
    """Centralized, memoized mount precedence and historical root resolver."""

    def __init__(
        self,
        fs: FileSystemManager,
        *,
        db: Any = None,
        get_run: Callable[[str], Run | None] | None = None,
    ) -> None:
        self.fs = fs
        self.db: DatabaseManager | None = (
            db if isinstance(db, DatabaseManager) else None
        )
        self.get_run = get_run
        self._run_mount_memo: dict[tuple[str, str], str | None] = {}
        self._historical_roots_memo: dict[str, tuple[str, ...]] = {}

    @classmethod
    def from_tracker(cls, tracker: Any) -> "MountPrecedenceResolver":
        fs: FileSystemManager = getattr(tracker, "fs")
        raw_db = getattr(tracker, "db", None)
        get_run = getattr(tracker, "get_run", None)
        if get_run is None and raw_db is not None:
            get_run = getattr(raw_db, "get_run", None)
        return cls(fs, db=raw_db, get_run=get_run)

    def seed_run_meta(self, run: Run) -> None:
        """Seed known run metadata for historical mount resolution."""
        run_meta = run.meta if isinstance(run.meta, Mapping) else {}
        mounts_meta = run_meta.get("mounts")
        arch_meta = run_meta.get("archive_mounts")
        phys = run_meta.get("_physical_run_dir")
        rid = str(run.id)
        if isinstance(mounts_meta, Mapping):
            for m, r in mounts_meta.items():
                if isinstance(r, str):
                    self._run_mount_memo[(rid, m)] = r
        if isinstance(arch_meta, Mapping):
            for m, r in arch_meta.items():
                if isinstance(r, str) and (rid, m) not in self._run_mount_memo:
                    self._run_mount_memo[(rid, m)] = r
        if isinstance(phys, str):
            for m in ("workspace", "runs"):
                if (rid, m) not in self._run_mount_memo:
                    self._run_mount_memo[(rid, m)] = phys

    def get_historical_mount_roots(self, mount_name: str) -> tuple[str, ...]:
        """Return memoized DB historical mount roots for ``mount_name``."""
        if mount_name in self._historical_roots_memo:
            return self._historical_roots_memo[mount_name]
        if self.db is None or not self.db.has_artifact_location_table():
            self._historical_roots_memo[mount_name] = ()
            return ()
        roots = tuple(self.db.lookup_historical_mount_roots(mount_name))
        self._historical_roots_memo[mount_name] = roots
        return roots

    def has_historical_root(self, mount_name: str) -> bool:
        """Return whether at least one existing historical root directory is known for ``mount_name``."""
        for cand_root in self.get_historical_mount_roots(mount_name):
            try:
                if Path(cand_root).exists():
                    return True
            except OSError:
                continue
        return False

    def _lookup_owning_run_mount(
        self, run_id: str, mount_name: str, site: str
    ) -> str | None:
        memo_key = (run_id, mount_name)
        if memo_key in self._run_mount_memo:
            return self._run_mount_memo[memo_key]
        result: str | None = None
        if self.get_run is not None:
            owning_run = self.get_run(run_id)
            run_meta = (
                owning_run.meta
                if owning_run is not None and isinstance(owning_run.meta, Mapping)
                else {}
            )
            mounts_meta = run_meta.get("mounts")
            arch_meta = run_meta.get("archive_mounts")
            m_over = (
                mounts_meta.get(mount_name)
                if isinstance(mounts_meta, Mapping)
                else None
            ) or (arch_meta.get(mount_name) if isinstance(arch_meta, Mapping) else None)
            if isinstance(m_over, str) and m_over:
                result = m_over
            elif mount_name in ("workspace", "runs") and site in (
                "workspace",
                "external",
            ):
                phys = run_meta.get("_physical_run_dir")
                if isinstance(phys, str) and phys:
                    result = phys
        self._run_mount_memo[memo_key] = result
        return result

    def resolve_row_coordinates(
        self,
        row: ArtifactLocationRow,
        artifact: Artifact | None = None,
    ) -> tuple[TranslatedCoordinate, tuple[TranslatedCoordinate, ...]]:
        """Resolve a row into ``(primary_coord, historical_coords_tuple)``.

        For named mounts, historical coordinates are always computed even if
        primary resolves cleanly, handling remounted workspaces.
        """
        primary_coord = self.fs.translate_location_coordinate(
            row.mount_name, row.mount_instance_id, row.uri
        )

        if row.mount_name == "abs":
            return (primary_coord, ())

        if row.mount_name in ("workspace", "runs"):
            if primary_coord.status == "resolved":
                return (primary_coord, ())
            if primary_coord.status == "rejected":
                return (primary_coord, ())
            # Unmounted workspace/runs: try owning run physical directory.
            if artifact is not None and artifact.run_id is not None:
                phys_dir = self._lookup_owning_run_mount(
                    str(artifact.run_id), row.mount_name, row.site
                )
                if phys_dir is not None:
                    hist_coord = self.fs.translate_location_coordinate(
                        row.mount_name,
                        row.mount_instance_id,
                        row.uri,
                        historical_run_dir=phys_dir,
                    )
                    if hist_coord.is_resolved:
                        return (hist_coord, ())
            return (primary_coord, ())

        # Named mounts: compute deduplicated historical fallback(s).
        hist_roots: list[str] = []
        seen_roots: set[str] = set()

        def _add_hist_root(cand_root: str | None) -> None:
            if not cand_root or cand_root in seen_roots:
                return
            # Don't re-test current mount root as a historical root
            if (
                row.mount_name in self.fs.mounts
                and self.fs.mounts[row.mount_name] == cand_root
            ):
                return
            seen_roots.add(cand_root)
            hist_roots.append(cand_root)

        # 1. Owning run metadata
        if artifact is not None and artifact.run_id is not None:
            run_mount = self._lookup_owning_run_mount(
                str(artifact.run_id), row.mount_name, row.site
            )
            _add_hist_root(run_mount)

        # 2. Artifact metadata fallback
        if (
            artifact is not None
            and isinstance(artifact.meta, Mapping)
            and row.mount_name not in ("workspace", "runs")
        ):
            art_mount_scheme = artifact.meta.get("mount_scheme")
            if art_mount_scheme == row.mount_name or (
                row.site == "external" and art_mount_scheme is None
            ):
                art_mount_root = artifact.meta.get("mount_root")
                if isinstance(art_mount_root, str) and art_mount_root:
                    _add_hist_root(art_mount_root)

        # 3. DB historical mount index
        for db_root in self.get_historical_mount_roots(row.mount_name):
            _add_hist_root(db_root)

        if not hist_roots:
            return (primary_coord, ())

        translated_historical: list[tuple[int, int, TranslatedCoordinate]] = []
        seen_canonical: set[Path] = set()
        if primary_coord.is_resolved and primary_coord.canonical_path is not None:
            seen_canonical.add(primary_coord.canonical_path)

        for idx, hist_root in enumerate(hist_roots):
            coord = self.fs.translate_location_coordinate(
                row.mount_name,
                row.mount_instance_id,
                row.uri,
                historical_run_dir=hist_root,
            )
            if not coord.is_resolved:
                continue
            assert coord.canonical_path is not None
            if coord.canonical_path in seen_canonical:
                continue
            seen_canonical.add(coord.canonical_path)

            # Priority rank:
            # 0: target (or artifact entry) exists on disk
            # 1: extracted root/prefix exists on disk
            # 2: remaining non-rejected translations
            rank = 2
            try:
                probe_entry = coord.canonical_path
                if artifact is not None and is_file_bundle_artifact(artifact):
                    entry_name = file_bundle_entry_name(artifact)
                    if entry_name is not None and probe_entry.is_dir():
                        probe_entry = probe_entry / entry_name
                if probe_entry.exists():
                    rank = 0
                elif Path(hist_root).exists():
                    rank = 1
            except OSError:
                pass
            translated_historical.append((rank, idx, coord))

        translated_historical.sort(key=lambda item: (item[0], item[1]))
        ordered_coords = tuple(item[2] for item in translated_historical)
        return (primary_coord, ordered_coords)

    def resolve_historical_recovery_root(
        self,
        artifact_uri: str,
        row: ArtifactLocationRow,
        *,
        resolve: bool = True,
    ) -> str | None:
        """Single-root projection for ``Tracker.artifact_recovery_roots``.

        Selects the best historical root under the priority:
        target exists -> prefix exists -> first non-rejected translation.
        """
        hist_roots = self.get_historical_mount_roots(row.mount_name)
        if not hist_roots:
            return None

        first_translated: str | None = None
        for historical_root in hist_roots:
            candidate = self.fs.extract_recovery_root_from_location_row(
                artifact_uri,
                row,
                resolve=resolve,
                historical_run_dir=historical_root,
            )
            if candidate is None or "://" in candidate:
                continue
            if first_translated is None:
                first_translated = candidate
            try:
                if Path(candidate).exists():
                    return candidate
            except OSError:
                continue
        return first_translated


def iter_artifact_candidates(
    tracker: Any,
    artifact: Artifact,
    *,
    relative_path: Path | None = None,
    rows: Sequence[ArtifactLocationRow] | None = None,
    source_root: str | Path | None = None,
    explicit_roots: Sequence[str | Path] | None = None,
    historical_run: Run | None = None,
    historical_run_dir: str | Path | None = None,
    include_runtime_abs: bool = False,
    resolver: MountPrecedenceResolver | None = None,
) -> Iterator[ResolvedCandidate]:
    """Enumerate ordered, deduplicated, containment-checked candidate locations for an artifact."""
    seen_canonical: set[Path] = set()
    res = resolver or MountPrecedenceResolver.from_tracker(tracker)
    if historical_run is not None:
        res.seed_run_meta(historical_run)
    rel = relative_path
    if rel is None:
        inferred_prefix: Path | None = None
        if artifact.run_id is not None and "://" in artifact.uri:
            scheme = artifact.uri.split("://", 1)[0]
            run_mount = res._lookup_owning_run_mount(
                str(artifact.run_id), scheme, "workspace"
            )
            phys_dir = res._lookup_owning_run_mount(
                str(artifact.run_id), "workspace", "workspace"
            )
            if run_mount is not None and phys_dir is not None:
                try:
                    p_mount = Path(run_mount).resolve()
                    p_phys = Path(phys_dir).resolve()
                    sub_rel = p_mount.relative_to(p_phys)
                    if sub_rel != Path("."):
                        inferred_prefix = sub_rel
                except (ValueError, RuntimeError):
                    pass
        prefixes_map = (
            {artifact.uri.split("://", 1)[0]: str(inferred_prefix)}
            if inferred_prefix is not None
            else None
        )
        try:
            rel = tracker.fs.get_remappable_relative_path(
                artifact.uri, scheme_prefixes=prefixes_map
            )
        except Exception:
            rel = tracker.fs.get_remappable_relative_path(artifact.uri)

    # 1. Explicit source root
    if source_root is not None and rel is not None:
        cand = _candidate_from_root(source_root, rel, "explicit_root", artifact)
        if cand is not None and cand.canonical_path not in seen_canonical:
            seen_canonical.add(cand.canonical_path)
            yield cand

    # 2. Additional explicit roots
    if explicit_roots is not None and rel is not None:
        for exp_root in explicit_roots:
            cand = _candidate_from_root(exp_root, rel, "explicit_root", artifact)
            if cand is not None and cand.canonical_path not in seen_canonical:
                seen_canonical.add(cand.canonical_path)
                yield cand

    # 3. Runtime abs path (if requested)
    if include_runtime_abs and artifact.abs_path:
        p = Path(artifact.abs_path)
        canonical = Path(os.path.realpath(p))
        if canonical not in seen_canonical:
            seen_canonical.add(canonical)
            entry_lexical = p
            entry_canonical = canonical
            if is_file_bundle_artifact(artifact) and canonical.is_dir():
                entry_name = file_bundle_entry_name(artifact)
                if entry_name is not None:
                    entry_lexical = p / entry_name
                    target_entry = canonical / entry_name
                    entry_canonical = (
                        target_entry.resolve()
                        if target_entry.exists()
                        else Path(os.path.realpath(canonical)) / entry_name
                    )
            yield ResolvedCandidate(
                source="runtime_abs",
                lexical_path=p,
                mount_canonical_lexical_path=p,
                canonical_path=canonical,
                entry_lexical_path=entry_lexical,
                entry_canonical_path=entry_canonical,
            )

    # 4. Ledger rows
    candidate_rows = rows
    db = getattr(tracker, "db", None)
    is_real_db = isinstance(db, DatabaseManager)

    if candidate_rows is None and is_real_db:
        if artifact.id is not None or artifact.digest is not None:
            candidate_rows = db.get_active_artifact_locations(
                artifact_id=artifact.id,
                digest=artifact.digest,
                member=artifact.member,
            )

    if candidate_rows:
        for row in candidate_rows:
            primary_coord, hist_coords = res.resolve_row_coordinates(row, artifact)
            if primary_coord.is_resolved:
                cand = _candidate_from_coordinate(
                    primary_coord, "ledger_primary", artifact, row=row
                )
                if cand is not None and cand.canonical_path not in seen_canonical:
                    seen_canonical.add(cand.canonical_path)
                    yield cand
            for hist_coord in hist_coords:
                cand = _candidate_from_coordinate(
                    hist_coord, "ledger_historical", artifact, row=row
                )
                if cand is not None and cand.canonical_path not in seen_canonical:
                    seen_canonical.add(cand.canonical_path)
                    yield cand

    # 5. Direct translation and DB-less / stub-tracker fallback
    if not candidate_rows:
        hist_dir = historical_run_dir
        if (
            hist_dir is None
            and historical_run is not None
            and isinstance(historical_run.meta, Mapping)
        ):
            hist_dir = historical_run.meta.get("_physical_run_dir")
        if hist_dir is not None and rel is not None:
            cand = _candidate_from_root(hist_dir, rel, "historical_uri", artifact)
            if cand is not None and cand.canonical_path not in seen_canonical:
                seen_canonical.add(cand.canonical_path)
                yield cand

        try:
            p_mount, p_inst, p_uri, _ = tracker.fs.location_coordinate_for_uri(
                artifact.uri
            )
            coord = tracker.fs.translate_location_coordinate(p_mount, p_inst, p_uri)
            if coord.is_resolved:
                cand = _candidate_from_coordinate(coord, "managed_mount", artifact)
                if cand is not None and cand.canonical_path not in seen_canonical:
                    seen_canonical.add(cand.canonical_path)
                    yield cand
        except Exception:
            pass

        if not is_real_db and rel is not None:
            rec_roots_fn = getattr(tracker, "artifact_recovery_roots", None)
            if callable(rec_roots_fn):
                try:
                    raw_roots = rec_roots_fn(artifact, resolve=False)
                except TypeError:
                    raw_roots = rec_roots_fn(artifact)
                normalized = tracker.fs.normalize_recovery_roots(
                    raw_roots, resolve=False
                )
                for r in normalized:
                    if "://" in r and not r.startswith("file://"):
                        continue
                    cand = _candidate_from_root(r, rel, "stub_recovery_root", artifact)
                    if cand is not None and cand.canonical_path not in seen_canonical:
                        seen_canonical.add(cand.canonical_path)
                        yield cand
