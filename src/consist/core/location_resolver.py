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
import uuid

from consist.core.directory_artifacts import (
    file_bundle_entry_name,
    is_file_bundle_artifact,
)
from consist.core.fs import FileSystemManager
from consist.core.persistence import DatabaseManager
from consist.location import ArtifactLocationRow, TranslatedCoordinate
from consist.models.artifact import Artifact

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.models.run import Run

CandidateSource: TypeAlias = Literal[
    "explicit_root",
    "runtime_abs",
    "ledger_primary",
    "ledger_historical",
    "historical_uri",
    "managed_mount",
]


@dataclass(frozen=True, slots=True)
class ResolvedCandidate:
    """A containment-checked filesystem candidate path for an artifact."""

    source: CandidateSource
    lexical_path: Path
    mount_canonical_lexical_path: Path
    canonical_path: Path
    entry_canonical_path: Path
    row: ArtifactLocationRow | None = None


def _entry_canonical_path(artifact: Artifact, canonical_path: Path) -> Path:
    """Resolve the bundle-member entry path for directory file bundles."""
    if is_file_bundle_artifact(artifact) and canonical_path.is_dir():
        entry_name = file_bundle_entry_name(artifact)
        if entry_name is not None:
            target_entry = canonical_path / entry_name
            return (
                target_entry.resolve()
                if target_entry.exists()
                else Path(os.path.realpath(canonical_path)) / entry_name
            )
    return canonical_path


def _artifact_rows_key(
    artifact: Artifact,
) -> tuple[uuid.UUID | None, str | None, str]:
    return (
        artifact.id,
        str(artifact.digest) if artifact.digest is not None else None,
        artifact.member or "",
    )


def _candidate_from_coordinate(
    coord: TranslatedCoordinate,
    source: CandidateSource,
    artifact: Artifact,
    *,
    row: ArtifactLocationRow | None = None,
    on_rejected: Callable[[TranslatedCoordinate], None] | None = None,
) -> ResolvedCandidate | None:
    if coord.is_rejected:
        if on_rejected is not None:
            on_rejected(coord)
        return None
    if not coord.is_resolved:
        return None
    assert coord.lexical_path is not None
    assert coord.mount_canonical_lexical_path is not None
    assert coord.canonical_path is not None

    return ResolvedCandidate(
        source=source,
        lexical_path=coord.lexical_path,
        mount_canonical_lexical_path=coord.mount_canonical_lexical_path,
        canonical_path=coord.canonical_path,
        entry_canonical_path=_entry_canonical_path(artifact, coord.canonical_path),
        row=row,
    )


def _candidate_from_root(
    root: Path | str,
    rel: Path,
    source: CandidateSource,
    artifact: Artifact,
    *,
    fs: FileSystemManager,
    row: ArtifactLocationRow | None = None,
    on_rejected: Callable[[TranslatedCoordinate], None] | None = None,
) -> ResolvedCandidate | None:
    coord = fs.translate_raw_root(root, rel)
    return _candidate_from_coordinate(
        coord,
        source,
        artifact,
        row=row,
        on_rejected=on_rejected,
    )


class LocationResolver:
    """Centralized, epoch-synchronized mount precedence and historical root resolver."""

    def __init__(
        self,
        fs: FileSystemManager,
        *,
        db: DatabaseManager | None = None,
        get_run: Callable[[str], Run | None] | None = None,
        get_active_run_id: Callable[[], str | None] | None = None,
    ) -> None:
        self.fs = fs
        self.db: DatabaseManager | None = db
        self.get_run = get_run
        self.get_active_run_id = get_active_run_id
        self._seeded_run_ids: set[str] = set()
        self._run_obj_memo: dict[str, Run | None] = {}
        self._rows_by_key_memo: dict[
            tuple[uuid.UUID | None, str | None, str], tuple[ArtifactLocationRow, ...]
        ] = {}
        self._historical_roots_memo: dict[str, tuple[str, ...]] = {}
        self._live_hist_roots_memo: dict[str, tuple[str, ...]] = {}
        self._has_hist_root_memo: dict[str, bool] = {}
        self._epoch_key: tuple[Any, ...] | None = None

    @classmethod
    def from_tracker(cls, tracker: Tracker) -> LocationResolver:
        """Return the tracker's epoch-synchronized ``LocationResolver``."""
        return tracker.location_resolver

    def invalidate_live_probes(self) -> None:
        """Clear cached filesystem existence probes so fresh invocations re-stat roots."""
        self._live_hist_roots_memo.clear()
        self._has_hist_root_memo.clear()

    def sync_epoch(self) -> None:
        """Synchronize memoized DB and mount state with the current epoch."""
        db = self.db
        epoch = (
            db.effective_location_epoch if db is not None else None,
            db.effective_mount_epoch if db is not None else None,
            self.fs.workspace_instance_id,
            self.fs.sorted_mounts_key,
        )
        if epoch != self._epoch_key:
            self._epoch_key = epoch
            self._rows_by_key_memo.clear()
            self._historical_roots_memo.clear()
            self._live_hist_roots_memo.clear()
            self._has_hist_root_memo.clear()
            if self._seeded_run_ids:
                self._run_obj_memo = {
                    rid: self._run_obj_memo[rid]
                    for rid in self._seeded_run_ids
                    if rid in self._run_obj_memo
                }
            else:
                self._run_obj_memo.clear()

    def seed_run_meta(self, run: Run) -> None:
        """Seed known run metadata for historical mount resolution."""
        rid = str(run.id)
        self._seeded_run_ids.add(rid)
        self._run_obj_memo[rid] = run

    def prefetch(
        self,
        artifacts: Sequence[Artifact],
        *,
        runs: Sequence[Run] = (),
    ) -> None:
        """Bulk-populate location rows and owning runs for ``artifacts``."""
        self.sync_epoch()
        for run in runs:
            self.seed_run_meta(run)

        db = self.db
        if db is None or not artifacts:
            return

        missing_artifacts = [
            art
            for art in artifacts
            if art.id is not None
            and _artifact_rows_key(art) not in self._rows_by_key_memo
        ]
        if missing_artifacts:
            bulk_rows = db.get_active_artifact_locations_bulk(missing_artifacts)
            for art in missing_artifacts:
                assert art.id is not None
                self._rows_by_key_memo[_artifact_rows_key(art)] = tuple(
                    bulk_rows.get(art.id, ())
                )

        missing_run_ids = list(
            dict.fromkeys(
                str(art.run_id)
                for art in artifacts
                if art.run_id and str(art.run_id) not in self._run_obj_memo
            )
        )
        if missing_run_ids:
            fetched_runs = db.get_runs_by_ids_chronological(missing_run_ids)
            by_id = {str(run.id): run for run in fetched_runs}
            for rid in missing_run_ids:
                self._run_obj_memo[rid] = by_id.get(rid)

    def _get_run(self, run_id: str) -> Run | None:
        if run_id not in self._run_obj_memo:
            self._run_obj_memo[run_id] = (
                self.get_run(run_id) if self.get_run is not None else None
            )
        return self._run_obj_memo[run_id]

    def _get_run_meta(self, run_id: str) -> Mapping[str, Any]:
        run = self._get_run(run_id)
        return run.meta if run is not None and isinstance(run.meta, Mapping) else {}

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

    def get_live_historical_mount_roots(self, mount_name: str) -> tuple[str, ...]:
        """Return memoized historical mount roots for ``mount_name`` that exist on disk."""
        if mount_name in self._live_hist_roots_memo:
            return self._live_hist_roots_memo[mount_name]
        live: list[str] = []
        for cand_root in self.get_historical_mount_roots(mount_name):
            try:
                if os.path.exists(cand_root):
                    live.append(cand_root)
            except OSError:
                continue
        result = tuple(live)
        self._live_hist_roots_memo[mount_name] = result
        return result

    def has_historical_root(self, mount_name: str) -> bool:
        """Return whether at least one existing historical root directory is known for ``mount_name``."""
        if mount_name in self._has_hist_root_memo:
            return self._has_hist_root_memo[mount_name]
        result = bool(self.get_live_historical_mount_roots(mount_name))
        self._has_hist_root_memo[mount_name] = result
        return result

    def _lookup_owning_run_mount(
        self, run_id: str, mount_name: str, site: str
    ) -> str | None:
        run_meta = self._get_run_meta(run_id)
        mounts_meta = run_meta.get("mounts")
        arch_meta = run_meta.get("archive_mounts")
        m_over = (
            mounts_meta.get(mount_name) if isinstance(mounts_meta, Mapping) else None
        ) or (arch_meta.get(mount_name) if isinstance(arch_meta, Mapping) else None)
        if isinstance(m_over, str) and m_over:
            return m_over
        if mount_name in ("workspace", "runs") and site in (
            "workspace",
            "external",
        ):
            phys = run_meta.get("_physical_run_dir")
            if isinstance(phys, str) and phys:
                return phys
        return None

    def remappable_relative_path(
        self,
        artifact: Artifact,
        *,
        scheme_prefixes: Mapping[str, str | Path] | None = None,
    ) -> Path | None:
        """Derive a portable relative path for ``artifact``, inferring nested mount prefixes."""
        prefixes_map: dict[str, str | Path] | None = (
            dict(scheme_prefixes) if scheme_prefixes is not None else None
        )
        if (
            prefixes_map is None
            and artifact.run_id is not None
            and "://" in artifact.uri
        ):
            scheme = artifact.uri.split("://", 1)[0]
            if scheme != "file":
                run_mount = self._lookup_owning_run_mount(
                    str(artifact.run_id), scheme, "workspace"
                )
                phys_dir = self._lookup_owning_run_mount(
                    str(artifact.run_id), "workspace", "workspace"
                )
                if run_mount is not None and phys_dir is not None:
                    sub_rel = self.fs.nested_mount_prefix(run_mount, phys_dir)
                    if sub_rel is not None:
                        prefixes_map = {scheme: str(sub_rel)}

        rel = self.fs.get_remappable_relative_path(
            artifact.uri, scheme_prefixes=prefixes_map
        )
        if rel is not None:
            return rel
        if prefixes_map is not None:
            rel = self.fs.get_remappable_relative_path(artifact.uri)
            if rel is not None:
                return rel
        return self.fs.unprefixed_uri_relative_path(artifact.uri)

    def resolve_primary_coordinate(
        self,
        row: ArtifactLocationRow,
        artifact: Artifact | None = None,
    ) -> TranslatedCoordinate:
        """Resolve only the primary coordinate for a location row."""
        primary_coord = self.fs.translate_location_coordinate(
            row.mount_name, row.mount_instance_id, row.uri
        )
        if (
            row.mount_name == "abs"
            and row.site in ("archive", "advisory")
            and artifact is not None
            and primary_coord.is_resolved
            and primary_coord.lexical_path is not None
        ):
            rel = self.remappable_relative_path(artifact)
            if rel is not None:
                root = self.fs.invert_relative_layout(primary_coord.lexical_path, rel)
                if root is not None:
                    return self.fs.translate_raw_root(root, rel)
        return primary_coord

    def iter_historical_coordinates(
        self,
        row: ArtifactLocationRow,
        artifact: Artifact | None = None,
        *,
        exclude_canonical: Path | None = None,
    ) -> Iterator[TranslatedCoordinate]:
        """Lazily resolve and rank historical coordinates for ``row``."""
        if row.mount_name == "abs":
            return

        if row.mount_name in ("workspace", "runs"):
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
                    if hist_coord.is_resolved and (
                        exclude_canonical is None
                        or hist_coord.canonical_path != exclude_canonical
                    ):
                        yield hist_coord
            return

        hist_roots: list[str] = []
        seen_roots: set[str] = set()

        def _add_hist_root(cand_root: str | None) -> None:
            if not cand_root or cand_root in seen_roots:
                return
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

        # 3. DB historical mount index (live roots first, then remaining roots)
        live_db_roots = set(self.get_live_historical_mount_roots(row.mount_name))
        for db_root in self.get_live_historical_mount_roots(row.mount_name):
            _add_hist_root(db_root)
        for db_root in self.get_historical_mount_roots(row.mount_name):
            _add_hist_root(db_root)

        if not hist_roots:
            return

        translated_historical: list[tuple[int, int, TranslatedCoordinate]] = []
        seen_canonical: set[Path] = set()
        if exclude_canonical is not None:
            seen_canonical.add(exclude_canonical)

        all_db_roots = set(self.get_historical_mount_roots(row.mount_name))

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
            root_is_known_dead = (
                hist_root in all_db_roots and hist_root not in live_db_roots
            )
            if not root_is_known_dead:
                try:
                    probe_entry = coord.canonical_path
                    if artifact is not None and is_file_bundle_artifact(artifact):
                        entry_name = file_bundle_entry_name(artifact)
                        if entry_name is not None and probe_entry.is_dir():
                            probe_entry = probe_entry / entry_name
                    if probe_entry.exists():
                        rank = 0
                    elif hist_root in live_db_roots or os.path.exists(hist_root):
                        rank = 1
                except OSError:
                    pass
            translated_historical.append((rank, idx, coord))

        translated_historical.sort(key=lambda item: (item[0], item[1]))
        for _, _, coord in translated_historical:
            yield coord

    def resolve_row_coordinates(
        self,
        row: ArtifactLocationRow,
        artifact: Artifact | None = None,
    ) -> tuple[TranslatedCoordinate, tuple[TranslatedCoordinate, ...]]:
        """Resolve a row into ``(primary_coord, historical_coords_tuple)``."""
        primary_coord = self.resolve_primary_coordinate(row, artifact)
        exclude = primary_coord.canonical_path if primary_coord.is_resolved else None
        return (
            primary_coord,
            tuple(
                self.iter_historical_coordinates(
                    row, artifact, exclude_canonical=exclude
                )
            ),
        )

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

    def extract_recovery_root(
        self,
        artifact_uri: str,
        row: ArtifactLocationRow,
        *,
        resolve: bool = True,
    ) -> str | None:
        """Extract a normalized recovery root for ``row``, falling back to historical mount roots."""
        self.sync_epoch()
        extracted = self.fs.extract_recovery_root_from_location_row(
            artifact_uri, row, resolve=resolve
        )
        if (
            extracted is not None
            and "://" in extracted
            and not extracted.startswith("file://")
            and row.mount_name not in self.fs.mounts
        ):
            if self.get_historical_mount_roots(row.mount_name):
                return self.resolve_historical_recovery_root(
                    artifact_uri, row, resolve=resolve
                )
            return extracted
        return extracted

    def iter_candidates(
        self,
        artifact: Artifact,
        *,
        relative_path: Path | None = None,
        rows: Sequence[ArtifactLocationRow] | None = None,
        source_root: str | Path | Sequence[str | Path] | None = None,
        historical_run: Run | None = None,
        historical_run_dir: str | Path | None = None,
        include_runtime_abs: bool = False,
        include_current_uri: bool = False,
        on_rejected: Callable[[TranslatedCoordinate], None] | None = None,
    ) -> Iterator[ResolvedCandidate]:
        """Enumerate ordered, deduplicated, containment-checked candidate locations for an artifact."""
        self.sync_epoch()
        if historical_run is not None:
            self.seed_run_meta(historical_run)

        seen_canonical: set[Path] = set()

        def _emit(cand: ResolvedCandidate | None) -> Iterator[ResolvedCandidate]:
            if cand is not None and cand.canonical_path not in seen_canonical:
                seen_canonical.add(cand.canonical_path)
                yield cand

        rel_computed = relative_path is not None
        cached_rel: Path | None = relative_path

        def _get_rel() -> Path | None:
            nonlocal rel_computed, cached_rel
            if not rel_computed:
                cached_rel = self.remappable_relative_path(artifact)
                rel_computed = True
            return cached_rel

        # 1. Explicit source roots
        if source_root is not None:
            rel = _get_rel()
            if rel is not None:
                source_roots = (
                    [source_root]
                    if isinstance(source_root, (str, Path))
                    else list(source_root)
                )
                for src_root in source_roots:
                    yield from _emit(
                        _candidate_from_root(
                            src_root,
                            rel,
                            "explicit_root",
                            artifact,
                            fs=self.fs,
                            on_rejected=on_rejected,
                        )
                    )

        # 2. Runtime abs path (if requested)
        if include_runtime_abs and artifact.abs_path:
            p = Path(artifact.abs_path)
            canonical = Path(os.path.realpath(p))
            if canonical not in seen_canonical:
                seen_canonical.add(canonical)
                yield ResolvedCandidate(
                    source="runtime_abs",
                    lexical_path=p,
                    mount_canonical_lexical_path=p,
                    canonical_path=canonical,
                    entry_canonical_path=_entry_canonical_path(artifact, canonical),
                )

        # 4. Ledger rows (Tier 4a: all primaries first, Tier 4b: lazy historical)
        candidate_rows: Sequence[ArtifactLocationRow] | None = rows
        if candidate_rows is None and self.db is not None:
            key = _artifact_rows_key(artifact)
            if key in self._rows_by_key_memo:
                candidate_rows = self._rows_by_key_memo[key]
            elif artifact.id is not None or artifact.digest is not None:
                fetched = tuple(
                    self.db.get_active_artifact_locations(
                        artifact_id=artifact.id,
                        digest=artifact.digest,
                        member=artifact.member,
                    )
                )
                self._rows_by_key_memo[key] = fetched
                candidate_rows = fetched

        if candidate_rows:
            primary_canonicals: list[Path | None] = []
            for row in candidate_rows:
                primary_coord = self.resolve_primary_coordinate(row, artifact)
                primary_canonicals.append(
                    primary_coord.canonical_path if primary_coord.is_resolved else None
                )
                yield from _emit(
                    _candidate_from_coordinate(
                        primary_coord,
                        "ledger_primary",
                        artifact,
                        row=row,
                        on_rejected=on_rejected,
                    )
                )

            for row, exclude_canonical in zip(candidate_rows, primary_canonicals):
                for hist_coord in self.iter_historical_coordinates(
                    row, artifact, exclude_canonical=exclude_canonical
                ):
                    yield from _emit(
                        _candidate_from_coordinate(
                            hist_coord,
                            "ledger_historical",
                            artifact,
                            row=row,
                            on_rejected=on_rejected,
                        )
                    )

        # 5. Direct translation and historical URI fallbacks
        # 5a. Artifact mount_root candidate
        art_meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}
        mr = art_meta.get("mount_root")
        suffix = self.fs.unprefixed_uri_relative_path(artifact.uri)
        if isinstance(mr, str) and mr and suffix is not None:
            yield from _emit(
                _candidate_from_root(
                    mr,
                    suffix,
                    "historical_uri",
                    artifact,
                    fs=self.fs,
                    on_rejected=on_rejected,
                )
            )

        # 5b. Owning-run historical root
        owning_run = (
            self._get_run(str(artifact.run_id)) if artifact.run_id is not None else None
        )
        if owning_run is None:
            owning_run = historical_run

        hist_dir = historical_run_dir
        if hist_dir is None and owning_run is not None:
            run_meta = owning_run.meta if isinstance(owning_run.meta, Mapping) else {}
            arch_meta = run_meta.get("archive_mounts")
            mounts_meta = run_meta.get("mounts")
            merged_mounts: dict[str, str] = {}
            if isinstance(arch_meta, Mapping):
                for k, v in arch_meta.items():
                    if isinstance(k, str) and isinstance(v, str):
                        merged_mounts[k] = v
            if isinstance(mounts_meta, Mapping):
                for k, v in mounts_meta.items():
                    if isinstance(k, str) and isinstance(v, str):
                        merged_mounts[k] = v
            mounts_snapshot = merged_mounts or None
            hist_root = self.fs.get_historical_root(
                artifact.uri,
                original_run_dir=run_meta.get("_physical_run_dir"),
                mounts_snapshot=mounts_snapshot,
                artifact_mount_root=art_meta.get("mount_root"),
                resolve=True,
            )
            if hist_root is not None:
                hist_dir = hist_root

        if hist_dir is not None:
            rel = _get_rel()
            if rel is not None:
                yield from _emit(
                    _candidate_from_root(
                        hist_dir,
                        rel,
                        "historical_uri",
                        artifact,
                        fs=self.fs,
                        on_rejected=on_rejected,
                    )
                )

        # 5c. Current-run managed mount fallback (or explicit current URI fallback)
        active_run_id = (
            self.get_active_run_id() if self.get_active_run_id is not None else None
        )
        is_current_run = artifact.run_id is None or (
            active_run_id is not None and str(artifact.run_id) == str(active_run_id)
        )
        if (not candidate_rows and is_current_run) or include_current_uri:
            p_mount, p_inst, p_uri = self.fs.location_coordinate_for_uri(artifact.uri)
            coord = self.fs.translate_location_coordinate(p_mount, p_inst, p_uri)
            yield from _emit(
                _candidate_from_coordinate(
                    coord,
                    "managed_mount",
                    artifact,
                    on_rejected=on_rejected,
                )
            )


MountPrecedenceResolver = LocationResolver


def iter_artifact_candidates(
    tracker: Tracker,
    artifact: Artifact,
    *,
    relative_path: Path | None = None,
    rows: Sequence[ArtifactLocationRow] | None = None,
    source_root: str | Path | Sequence[str | Path] | None = None,
    historical_run: Run | None = None,
    historical_run_dir: str | Path | None = None,
    include_runtime_abs: bool = False,
    resolver: LocationResolver | None = None,
    include_rejected: bool = False,
    include_current_uri: bool = False,
    on_rejected: Callable[[TranslatedCoordinate], None] | None = None,
) -> Iterator[ResolvedCandidate]:
    """Enumerate ordered, deduplicated, containment-checked candidate locations for an artifact."""
    _ = include_rejected
    active_resolver = resolver or tracker.location_resolver
    return active_resolver.iter_candidates(
        artifact,
        relative_path=relative_path,
        rows=rows,
        source_root=source_root,
        historical_run=historical_run,
        historical_run_dir=historical_run_dir,
        include_runtime_abs=include_runtime_abs,
        include_current_uri=include_current_uri,
        on_rejected=on_rejected,
    )
