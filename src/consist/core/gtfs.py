from __future__ import annotations

import copy
import hashlib
import io
import json
import zipfile
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Optional,
    Sequence,
    get_args,
    get_origin,
)

import pandas as pd

from consist.core.config_canonicalization import IngestSpec
from consist.digest import (
    canonical_json_sha256,
    digest_bytes,
    file_sha256,
    memoize_with_stat_token,
    register_stat_cache,
)
from consist.models.gtfs import GTFS_SCHEMA_BY_TABLE_NAME

if TYPE_CHECKING:
    from consist.core.identity import IdentityManager

_GTFS_BUNDLE_CACHE = register_stat_cache(max_entries=4)

GTFS_TABLE_EXTENSIONS = (".txt", ".csv")
GTFS_BUNDLE_SOURCE_KEY = "consist_gtfs_bundle"
GTFS_JUNK_BASENAMES = {
    "license",
    "license.txt",
    "readme",
    "readme.txt",
    "readme.md",
}
GTFS_CORE_TABLE_NAMES = tuple(GTFS_SCHEMA_BY_TABLE_NAME)


@dataclass(frozen=True)
class GtfsFeedSnapshot:
    feed_key: str
    source_path: Path
    source_kind: str
    member_names: tuple[str, ...]
    table_hashes: dict[str, str]
    source_feed_hash: str
    tables: dict[str, pd.DataFrame]


@dataclass(frozen=True)
class GtfsCanonicalizationResult:
    source_feed_hashes: dict[str, str]
    source_bundle_hash: str
    service_slice_hash: Optional[str]
    manifest: dict[str, Any]
    identity_payload: dict[str, Any]
    ingestables: list[IngestSpec]
    snapshots: tuple[GtfsFeedSnapshot, ...]
    selected_tables: dict[str, pd.DataFrame]
    selected_service_artifact: Any = None
    manifest_artifact: Any = None
    source_artifacts: tuple[Any, ...] = ()
    table_artifacts: dict[str, Any] = field(default_factory=dict)


def _normalize_member_name(name: str) -> str:
    normalized = name.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _basename(name: str) -> str:
    return Path(_normalize_member_name(name)).name.lower()


def _is_gtfs_junk_name(name: str) -> bool:
    normalized = _normalize_member_name(name)
    parts = normalized.split("/")
    base = Path(normalized).name.lower()
    return (
        "__MACOSX" in parts
        or any(part.startswith(".") for part in parts)
        or base.startswith("._")
        or base in GTFS_JUNK_BASENAMES
        or base.startswith(("readme", "license"))
    )


def _looks_like_table_name(name: str) -> bool:
    base = _basename(name)
    return any(
        base.endswith(ext) for ext in GTFS_TABLE_EXTENSIONS
    ) and not _is_gtfs_junk_name(base)


def _read_member_bytes(source_path: Path, member_name: str) -> bytes:
    member_name = _normalize_member_name(member_name)
    if zipfile.is_zipfile(source_path):
        with zipfile.ZipFile(source_path) as zf:
            with zf.open(member_name) as fh:
                return fh.read()
    return (source_path / member_name).read_bytes()


def _first_non_empty_line(text_stream: io.TextIOBase) -> str:
    for line in text_stream:
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def _read_member_first_line(
    source_path: Path,
    member_name: str,
    *,
    zf: zipfile.ZipFile | None = None,
) -> str:
    member_name = _normalize_member_name(member_name)
    if zf is not None or zipfile.is_zipfile(source_path):
        with (
            nullcontext(zf)
            if zf is not None
            else zipfile.ZipFile(source_path) as archive
        ):
            assert archive is not None
            with archive.open(member_name) as fh:
                with io.TextIOWrapper(
                    fh, encoding="utf-8-sig", errors="replace"
                ) as text:
                    return _first_non_empty_line(text)
    with (source_path / member_name).open(
        "r", encoding="utf-8-sig", errors="replace"
    ) as text:
        return _first_non_empty_line(text)


def _looks_like_csv_table(
    source_path: Path,
    member_name: str,
    *,
    zf: zipfile.ZipFile | None = None,
) -> bool:
    if not _looks_like_table_name(member_name):
        return False
    first_line = _read_member_first_line(source_path, member_name, zf=zf)
    if not first_line:
        return False
    if "," in first_line or "\t" in first_line:
        return True
    # Some vendor tables can be a little odd, but plain prose docs should not be
    # promoted to GTFS relations.
    return False


def discover_gtfs_members(source_path: Path) -> list[str]:
    """
    Discover GTFS table members inside a zip file or feed directory.

    Non-table junk files such as license/readme text files are filtered out.
    """
    path = Path(source_path)

    def _discover() -> list[str]:
        if not path.exists():
            raise FileNotFoundError(path)

        if path.is_dir():
            members: list[str] = []
            for file_path in sorted(path.rglob("*")):
                if not file_path.is_file():
                    continue
                rel = file_path.relative_to(path).as_posix()
                if _looks_like_csv_table(path, rel):
                    members.append(_normalize_member_name(rel))
            return members

        if zipfile.is_zipfile(path):
            members = []
            with zipfile.ZipFile(path) as zf:
                for info in sorted(zf.infolist(), key=lambda item: item.filename):
                    if info.is_dir():
                        continue
                    name = _normalize_member_name(info.filename)
                    if _looks_like_csv_table(path, name, zf=zf):
                        members.append(name)
            return members

        raise ValueError(f"GTFS source must be a directory or zip file: {path}")

    return memoize_with_stat_token(
        ("gtfs_discover_members",),
        [path],
        _discover,
        clone_fn=list,
    )


def has_gtfs_core_members(source_path: Path) -> bool:
    members = discover_gtfs_members(source_path)
    table_names = {Path(member).stem.lower() for member in members}
    return bool(table_names & set(GTFS_CORE_TABLE_NAMES))


def load_gtfs_member_df(source_path: Path | str, member_name: str) -> pd.DataFrame:
    """
    Load one GTFS member file into a Pandas DataFrame.

    The loader is intentionally dependency-light and uses Pandas for both zip and
    directory sources so the same code path works for the driver and the canonicalizer.
    """
    path = Path(source_path)
    member_name = _normalize_member_name(member_name)
    read_csv_kwargs = {
        "dtype": str,
        "keep_default_na": False,
        "na_values": [""],
    }
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as zf:
            with zf.open(member_name) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8-sig")
                df = pd.read_csv(  # ty: ignore[no-matching-overload]
                    text, **read_csv_kwargs
                )
                return _coerce_gtfs_member_types(df, member_name)
    member_path = path / member_name
    df = pd.read_csv(  # ty: ignore[no-matching-overload]
        member_path, encoding="utf-8-sig", **read_csv_kwargs
    )
    return _coerce_gtfs_member_types(df, member_name)


def load_gtfs_member_relation(
    source_path: Path | str,
    member_name: str,
    conn: Any,
    *,
    columns: Optional[Sequence[str]] = None,
) -> Any:
    df = load_gtfs_member_df(source_path, member_name)
    if columns:
        missing = [column for column in columns if column not in df.columns]
        if missing:
            raise KeyError(
                f"Missing GTFS columns for load: {', '.join(sorted(missing))}"
            )
        df = df.loc[:, list(columns)]
    duckdb_df = df.copy()
    string_columns = [
        column
        for column in duckdb_df.columns
        if pd.api.types.is_string_dtype(duckdb_df[column])
    ]
    if string_columns:
        duckdb_df = duckdb_df.astype({column: object for column in string_columns})
    return conn.from_df(duckdb_df)


def hash_gtfs_member(source_path: Path, member_name: str) -> str:
    return digest_bytes(_read_member_bytes(source_path, member_name))


def hash_gtfs_feed(source_path: Path, member_names: Sequence[str]) -> str:
    normalized_names = sorted(_normalize_member_name(name) for name in member_names)
    if zipfile.is_zipfile(source_path):
        with zipfile.ZipFile(source_path) as zf:
            members = []
            for name in normalized_names:
                with zf.open(name) as fh:
                    hasher = hashlib.sha256()
                    while chunk := fh.read(1024 * 1024):
                        hasher.update(chunk)
                    members.append(
                        {
                            "name": name,
                            "sha256": hasher.hexdigest(),
                        }
                    )
    else:
        members = [
            {
                "name": name,
                "sha256": file_sha256(source_path / name),
            }
            for name in normalized_names
        ]
    return canonical_json_sha256({"members": members})


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    args = [arg for arg in get_args(annotation) if arg is not type(None)]
    if len(args) == 1:
        return args[0]
    return annotation


def _coerce_gtfs_member_types(df: pd.DataFrame, member_name: str) -> pd.DataFrame:
    schema_model = GTFS_SCHEMA_BY_TABLE_NAME.get(Path(member_name).stem.lower())
    if schema_model is None:
        return df

    frame = df.copy()
    for field_name, field_info in schema_model.model_fields.items():
        if field_name == "feed_key" or field_name not in frame.columns:
            continue
        annotation = _unwrap_optional(getattr(field_info, "annotation", None))
        if annotation is int:
            frame[field_name] = pd.to_numeric(
                frame[field_name], errors="coerce"
            ).astype("Int64")
        elif annotation is float:
            frame[field_name] = pd.to_numeric(
                frame[field_name], errors="coerce"
            ).astype("Float64")
    return frame


def _stable_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _stable_json(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_stable_json(item) for item in value]
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _stable_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    stable = _stable_json(manifest)
    feeds = stable.get("feeds")
    if isinstance(feeds, list):
        for feed in feeds:
            if isinstance(feed, dict):
                feed.pop("source_path", None)
    return stable


def _rows_hash(rows: list[dict[str, Any]]) -> str:
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _frame_to_nullable_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {str(col): (None if pd.isna(value) else value) for col, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def _table_rows(df: pd.DataFrame, *, feed_key: str) -> list[dict[str, Any]]:
    frame = df.copy()
    if "feed_key" not in frame.columns:
        frame.insert(0, "feed_key", feed_key)
    records = _frame_to_nullable_records(frame)
    return sorted(
        records,
        key=lambda row: json.dumps(
            row, sort_keys=True, separators=(",", ":"), default=str
        ),
    )


def _ingest_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.dropna(axis=1, how="all")


def _normalize_date(value: date | datetime | str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)).date()


def _parse_gtfs_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError:
        return None


def _active_service_ids(
    calendar: Optional[pd.DataFrame],
    calendar_dates: Optional[pd.DataFrame],
    service_date: date,
) -> set[str]:
    active: set[str] = set()
    if calendar is not None and not calendar.empty:
        dow_columns = [
            ("monday", 0),
            ("tuesday", 1),
            ("wednesday", 2),
            ("thursday", 3),
            ("friday", 4),
            ("saturday", 5),
            ("sunday", 6),
        ]
        weekday = service_date.weekday()
        for _, row in calendar.iterrows():
            start = _parse_gtfs_date(row.get("start_date"))
            end = _parse_gtfs_date(row.get("end_date"))
            if start is not None and service_date < start:
                continue
            if end is not None and service_date > end:
                continue
            active_for_day = False
            for column, idx in dow_columns:
                if idx != weekday:
                    continue
                try:
                    active_for_day = bool(int(row.get(column, 0)))
                except (TypeError, ValueError):
                    active_for_day = str(row.get(column, "0")).strip() in {
                        "1",
                        "true",
                        "True",
                    }
                break
            if active_for_day:
                active.add(str(row.get("service_id")))

    if calendar_dates is not None and not calendar_dates.empty:
        target_date = service_date.strftime("%Y%m%d")
        added_service_ids: set[str] = set()
        removed_service_ids: set[str] = set()
        for _, row in calendar_dates.iterrows():
            if str(row.get("date")) != target_date:
                continue
            service_id = str(row.get("service_id"))
            exception_type = int(row.get("exception_type", 0))
            if exception_type == 1:
                added_service_ids.add(service_id)
            elif exception_type == 2:
                removed_service_ids.add(service_id)

        # Apply calendar_dates independently of row order. If malformed feeds list
        # both add and remove exceptions for the same service/date, removals win.
        active.update(added_service_ids)
        active.difference_update(removed_service_ids)

    return active


def _select_related_rows(
    tables: dict[str, pd.DataFrame],
    *,
    feed_key: str,
    service_date: Optional[date],
) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    raw_tables = {name: frame.copy() for name, frame in tables.items()}
    for frame in raw_tables.values():
        frame.insert(0, "feed_key", feed_key)

    if service_date is None:
        manifest = {
            "service_date": None,
            "feed_key": feed_key,
            "active_service_ids": [],
            "included_tables": sorted(raw_tables),
        }
        return raw_tables, manifest

    calendar = raw_tables.get("calendar")
    calendar_dates = raw_tables.get("calendar_dates")
    active_service_ids = _active_service_ids(
        calendar, calendar_dates, service_date=service_date
    )

    selected: dict[str, pd.DataFrame] = {}
    if "trips" in raw_tables:
        trips = raw_tables["trips"]
        trips = trips[trips["service_id"].astype(str).isin(active_service_ids)]
        selected["trips"] = trips
    active_trip_ids = set()
    if "trips" in selected and not selected["trips"].empty:
        active_trip_ids = set(selected["trips"]["trip_id"].astype(str))

    if "stop_times" in raw_tables:
        stop_times = raw_tables["stop_times"]
        stop_times = stop_times[stop_times["trip_id"].astype(str).isin(active_trip_ids)]
        selected["stop_times"] = stop_times

    if "routes" in raw_tables and "trips" in selected:
        route_ids = set(selected["trips"]["route_id"].astype(str))
        selected["routes"] = raw_tables["routes"][
            raw_tables["routes"]["route_id"].astype(str).isin(route_ids)
        ]

    if "stops" in raw_tables and "stop_times" in selected:
        stop_ids = set(selected["stop_times"]["stop_id"].dropna().astype(str).tolist())
        selected["stops"] = raw_tables["stops"][
            raw_tables["stops"]["stop_id"].astype(str).isin(stop_ids)
        ]

    if (
        "shapes" in raw_tables
        and "trips" in selected
        and "shape_id" in selected["trips"].columns
    ):
        shape_ids = set(selected["trips"]["shape_id"].dropna().astype(str).tolist())
        selected["shapes"] = raw_tables["shapes"][
            raw_tables["shapes"]["shape_id"].astype(str).isin(shape_ids)
        ]

    if "calendar" in raw_tables:
        selected["calendar"] = raw_tables["calendar"][
            raw_tables["calendar"]["service_id"].astype(str).isin(active_service_ids)
        ]

    if "calendar_dates" in raw_tables:
        target_date = service_date.strftime("%Y%m%d")
        selected["calendar_dates"] = raw_tables["calendar_dates"][
            raw_tables["calendar_dates"]["date"].astype(str) == target_date
        ]

    if "frequencies" in raw_tables and active_trip_ids:
        selected["frequencies"] = raw_tables["frequencies"][
            raw_tables["frequencies"]["trip_id"].astype(str).isin(active_trip_ids)
        ]

    manifest = {
        "service_date": service_date.isoformat(),
        "feed_key": feed_key,
        "active_service_ids": sorted(active_service_ids),
        "included_tables": sorted(selected),
    }
    return selected, manifest


def _gtfs_rows_factory(
    frame_for_ingest: pd.DataFrame, table_content_hash: Optional[str]
) -> Callable[[str], Iterable[dict[str, Any]]]:
    private_frame = frame_for_ingest.copy(deep=True)

    def _build(_run_id: Optional[str] = None) -> list[dict[str, Any]]:
        rows = _frame_to_nullable_records(private_frame)
        if table_content_hash is not None:
            for row in rows:
                row["content_hash"] = table_content_hash
        return rows

    return _build


def _compute_gtfs_bundle(
    feed_paths: Sequence[Path],
    *,
    normalized_service_date: Optional[date],
    feed_keys: Optional[Sequence[str]],
    source_key: str,
    include_tables: bool,
) -> GtfsCanonicalizationResult:
    snapshots: list[GtfsFeedSnapshot] = []
    selected_tables: dict[str, list[pd.DataFrame]] = {}
    source_feed_hashes: dict[str, str] = {}
    manifest_feeds: list[dict[str, Any]] = []
    used_feed_keys: set[str] = set()

    for index, feed_path in enumerate(feed_paths):
        path = Path(feed_path)
        proposed_feed_key = (
            str(feed_keys[index])
            if feed_keys is not None
            else path.stem or f"feed_{index + 1}"
        )
        feed_key = proposed_feed_key
        if feed_keys is not None:
            if feed_key in used_feed_keys:
                raise ValueError(f"feed_keys must be unique: {feed_key}")
        else:
            suffix = 1
            while feed_key in used_feed_keys:
                suffix += 1
                feed_key = f"{proposed_feed_key}_{suffix}"
        used_feed_keys.add(feed_key)
        members = discover_gtfs_members(path)
        tables = {
            Path(member).stem: load_gtfs_member_df(path, member) for member in members
        }
        raw_hash = hash_gtfs_feed(path, members)
        source_feed_hashes[feed_key] = raw_hash

        selected_by_feed, feed_manifest = _select_related_rows(
            tables,
            feed_key=feed_key,
            service_date=normalized_service_date,
        )
        for table_name, frame in selected_by_feed.items():
            selected_tables.setdefault(table_name, []).append(frame)

        table_hashes = {
            table_name: _rows_hash(_table_rows(frame, feed_key=feed_key))
            for table_name, frame in selected_by_feed.items()
        }
        snapshots.append(
            GtfsFeedSnapshot(
                feed_key=feed_key,
                source_path=path,
                source_kind="zip" if path.is_file() else "directory",
                member_names=tuple(members),
                table_hashes=table_hashes,
                source_feed_hash=raw_hash,
                tables=tables if include_tables else {},
            )
        )
        manifest_feeds.append(
            {
                "feed_key": feed_key,
                "source_path": path.as_posix(),
                "member_names": list(members),
                "source_feed_hash": raw_hash,
                "selection": feed_manifest,
            }
        )

    bundle_payload = {
        "feed_keys": [snapshot.feed_key for snapshot in snapshots],
        "source_feed_hashes": source_feed_hashes,
    }
    source_bundle_hash = canonical_json_sha256(bundle_payload)

    combined_selected_frames: dict[str, pd.DataFrame] = {}
    selected_table_hashes: dict[str, str] = {}
    combined_selected_rows: dict[str, list[dict[str, Any]]] = {}
    for table_name, frames in sorted(selected_tables.items()):
        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined_selected_frames[table_name] = combined
        rows = _table_rows(combined, feed_key="bundle")
        combined_selected_rows[table_name] = rows
        selected_table_hashes[table_name] = _rows_hash(rows)

    service_slice_hash: Optional[str] = None
    if normalized_service_date is not None:
        slice_payload = {
            "service_date": normalized_service_date.isoformat(),
            "feed_keys": [snapshot.feed_key for snapshot in snapshots],
            "tables": {
                table_name: {
                    "hash": selected_table_hashes[table_name],
                    "rows": _stable_json(combined_selected_rows[table_name]),
                }
                for table_name in sorted(combined_selected_frames)
            },
        }
        service_slice_hash = canonical_json_sha256(slice_payload)

    manifest = {
        "service_date": normalized_service_date.isoformat()
        if normalized_service_date is not None
        else None,
        "feed_keys": [snapshot.feed_key for snapshot in snapshots],
        "feeds": manifest_feeds,
        "included_tables": sorted(combined_selected_frames),
        "table_hashes": selected_table_hashes,
        "source_bundle_hash": source_bundle_hash,
        "service_slice_hash": service_slice_hash,
    }

    ingestables: list[IngestSpec] = []
    for table_name, frame in sorted(combined_selected_frames.items()):
        schema_model = GTFS_SCHEMA_BY_TABLE_NAME.get(table_name)
        if schema_model is None:
            continue
        frame_for_ingest = _ingest_frame(frame)
        table_content_hash = selected_table_hashes.get(table_name)
        ingestables.append(
            IngestSpec(
                table_name=table_name,
                schema=schema_model,
                rows=_gtfs_rows_factory(frame_for_ingest, table_content_hash),
                source_path=snapshots[0].source_path if snapshots else None,
                source=source_key,
                content_hash=table_content_hash,
                dedupe_on_hash=True,
            )
        )

    identity_manifest = _stable_manifest(manifest)
    identity_payload = {
        "source_feed_hashes": source_feed_hashes,
        "source_bundle_hash": source_bundle_hash,
        "service_slice_hash": service_slice_hash,
        "manifest": identity_manifest,
    }

    return GtfsCanonicalizationResult(
        source_feed_hashes=source_feed_hashes,
        source_bundle_hash=source_bundle_hash,
        service_slice_hash=service_slice_hash,
        manifest=manifest,
        identity_payload=identity_payload,
        ingestables=ingestables,
        snapshots=tuple(snapshots),
        selected_tables=combined_selected_frames if include_tables else {},
    )


def canonicalize_gtfs_bundle(
    feed_paths: Sequence[Path],
    *,
    identity: Optional[IdentityManager] = None,
    service_date: Optional[date | datetime | str] = None,
    feed_keys: Optional[Sequence[str]] = None,
    source_key: str = GTFS_BUNDLE_SOURCE_KEY,
    include_tables: bool = True,
) -> GtfsCanonicalizationResult:
    """
    Canonicalize one or more GTFS feeds into a deterministic bundle identity.

    The canonicalizer keeps raw feed hashes, a bundle hash that respects feed-key
    namespace boundaries, and an optional service-date slice hash that is computed
    from the selected active service rows.
    """
    del identity
    if feed_keys is not None and len(feed_keys) != len(feed_paths):
        raise ValueError("feed_keys must match feed_paths length.")

    normalized_service_date = (
        _normalize_date(service_date) if service_date is not None else None
    )
    feed_path_objs = [Path(p) for p in feed_paths]
    namespace = (
        "gtfs_bundle",
        tuple(fp.as_posix() for fp in feed_path_objs),
        normalized_service_date.isoformat()
        if normalized_service_date is not None
        else None,
        tuple(str(k) for k in feed_keys) if feed_keys is not None else None,
        source_key,
        bool(include_tables),
    )
    return memoize_with_stat_token(
        namespace,
        feed_path_objs,
        lambda: _compute_gtfs_bundle(
            feed_path_objs,
            normalized_service_date=normalized_service_date,
            feed_keys=feed_keys,
            source_key=source_key,
            include_tables=include_tables,
        ),
        clone_fn=copy.deepcopy,
        cache=_GTFS_BUNDLE_CACHE,
    )


__all__ = [
    "GTFS_CORE_TABLE_NAMES",
    "GTFS_JUNK_BASENAMES",
    "GTFS_TABLE_EXTENSIONS",
    "GtfsCanonicalizationResult",
    "GtfsFeedSnapshot",
    "canonicalize_gtfs_bundle",
    "discover_gtfs_members",
    "hash_gtfs_feed",
    "hash_gtfs_member",
    "load_gtfs_member_df",
    "load_gtfs_member_relation",
]
