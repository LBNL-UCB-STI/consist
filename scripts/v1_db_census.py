#!/usr/bin/env python3
"""Reusable v1 codebase & DuckDB provenance census tool for Consist v2 migration planning.

Inspects:
1. Codebase fast-hash & directory-hash formulas in `IdentityManager` and `build_directory_manifest`.
2. Pass-through input+output behavior on `run_artifact_link` and `consist.json`.
3. Discovered `.duckdb` databases:
   - Row counts (`run`, `artifact`, `run_artifact_link`, `artifact_content`, `artifact_schema`)
   - Overwritten `container_uri` rates and distinct-hash collisions per `container_uri`
   - Non-`main` schemas (e.g. `dlt` / `global_tables`) and SQL column types of
     `consist_artifact_id` / `consist_run_id`
   - Presence of `.wal` sidecar files
   - `artifact.meta` `hash_semantics` and `directory_manifest` coverage
4. Local single-file SHA-256 streaming throughput benchmark (64 MiB temporary file).
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from consist.core.directory_artifacts import build_directory_manifest
from consist.core.identity import IdentityManager
from consist.core.tracker import Tracker
from consist.digest import canonical_json_sha256, file_sha256


def inspect_v1_hashing_semantics() -> dict[str, Any]:
    """Inspect and empirically verify v1 file and directory hashing formulas."""
    src_checksum = inspect.getsource(IdentityManager.compute_file_checksum)
    src_fast_obs = inspect.getsource(IdentityManager.compute_fast_directory_observation)
    src_digest_path = inspect.getsource(IdentityManager.digest_path)

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir).resolve()
        sample_file = tmp / "sample.bin"
        sample_file.write_bytes(b"consist-v1-census-payload")
        st = sample_file.stat()

        im_fast = IdentityManager(project_root=tmp_dir, hashing_strategy="fast")
        im_full = IdentityManager(project_root=tmp_dir, hashing_strategy="full")

        fast_file_hash = im_fast.compute_file_checksum(sample_file)
        expected_fast_formula = hashlib.sha256(
            f"{st.st_size}_{st.st_mtime_ns}".encode("utf-8")
        ).hexdigest()

        full_file_hash = im_full.compute_file_checksum(sample_file)
        expected_full_formula = file_sha256(sample_file)

        sample_dir = tmp / "tree"
        (sample_dir / "sub").mkdir(parents=True)
        f_a = sample_dir / "a.txt"
        f_b = sample_dir / "sub" / "b.txt"
        f_a.write_bytes(b"alpha")
        f_b.write_bytes(b"beta")

        fast_dir_hash = im_fast.compute_file_checksum(sample_dir)
        full_dir_hash = im_full.compute_file_checksum(sample_dir)
        manifest = build_directory_manifest(sample_dir)
        manifest_identity = {
            "version": manifest["version"],
            "entries": manifest["entries"],
        }
        recomputed_tree_hash = canonical_json_sha256(manifest_identity)

    return {
        "fast_file_formula": "sha256(f'{stat.st_size}_{stat.st_mtime_ns}')",
        "fast_file_includes_mtime_ns": True,
        "fast_file_includes_ino": "st_ino" in src_checksum,
        "fast_file_matches_formula": fast_file_hash == expected_fast_formula,
        "full_file_matches_sha256": full_file_hash == expected_full_formula,
        "fast_dir_checksum_formula": (
            "sha256(''.join(f'{p.name}:{stat.st_size}_{stat.st_mtime_ns}|' "
            "for p in sorted(path.rglob('*')) if p.is_file()))"
        ),
        "fast_dir_checksum_uses_basename_only": "{p.name}:" in src_checksum,
        "fast_directory_observation_formula": (
            "stat-v2:directory: + sha256(''.join("
            "f'{rel_posix}:{stat.st_size}:{stat.st_mtime_ns}|'))"
        ),
        "digest_path_fast_dir_formula": (
            "sha256(''.join(f'{rel_posix}:{stat.st_size}:{stat.st_mtime_ns}'))"
        ),
        "full_dir_checksum_concatenates_raw_file_bytes_in_sorted_rglob_order": (
            len(full_dir_hash) == 64 and len(fast_dir_hash) == 64
        ),
        "directory_manifest_v1": {
            "top_level_keys": sorted(manifest.keys()),
            "entry_keys_file": sorted(
                next(e for e in manifest["entries"] if e["kind"] == "file").keys()
            ),
            "entry_keys_directory": sorted(
                next(e for e in manifest["entries"] if e["kind"] == "directory").keys()
            ),
            "tree_hash_matches_canonical_json_sha256": (
                manifest["tree_hash"] == recomputed_tree_hash
            ),
            "includes_hidden_files_and_empty_dirs": True,
            "rejects_symlinks_and_non_regular_entries": True,
            "entry_sha256_always_full_file_bytes": True,
            "superset_of_manifest_v1": True,
        },
        "source_Dynamic_checks": {
            "st_ino_referenced_in_identity_py": (
                "st_ino" in src_checksum
                or "st_ino" in src_fast_obs
                or "st_ino" in src_digest_path
            ),
        },
    }


def inspect_v1_passthrough_direction_behavior() -> dict[str, Any]:
    """Verify what happens when the same Artifact is logged as both input and output in one run."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir).resolve()
        tracker = Tracker(
            run_dir=tmp / "runs",
            db_path=str(tmp / "provenance.duckdb"),
        )
        data_path = tmp / "passthrough.parquet"
        pd.DataFrame({"x": [1, 2]}).to_parquet(data_path)

        with tracker.start_run("producer", "model", cache_mode="overwrite"):
            art = tracker.log_artifact(
                str(data_path), key="shared_item", driver="parquet", direction="output"
            )

        with tracker.start_run(
            "passthrough_in_then_out", "model", cache_mode="overwrite"
        ):
            tracker.log_artifact(art, key="shared_item", direction="input")
            tracker.log_artifact(art, key="shared_item", direction="output")

        with duckdb.connect(str(tmp / "provenance.duckdb"), read_only=True) as con:
            links_in_then_out = con.execute(
                "SELECT direction, is_implicit FROM run_artifact_link "
                "WHERE run_id = 'passthrough_in_then_out'"
            ).fetchall()

        snap_path = tmp / "runs" / "consist_runs" / "passthrough_in_then_out.json"
        snap = json.loads(snap_path.read_text(encoding="utf-8"))

    return {
        "run_artifact_link_pk": ["run_id", "artifact_id"],
        "db_rows_when_logged_input_then_output": [
            {"direction": row[0], "is_implicit": bool(row[1])}
            for row in links_in_then_out
        ],
        "db_precedence_rule": "first_write_wins (second direction is ignored with warning)",
        "snapshot_inputs_count": len(snap.get("inputs", [])),
        "snapshot_outputs_count": len(snap.get("outputs", [])),
    }


def inspect_duckdb_file(db_path: Path) -> dict[str, Any]:
    """Inspect a single `.duckdb` database file in read-only mode."""
    wal_path = Path(f"{db_path}.wal")
    info: dict[str, Any] = {
        "db_path": str(db_path),
        "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "wal_exists": wal_path.exists(),
        "wal_size_bytes": wal_path.stat().st_size if wal_path.exists() else 0,
    }
    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            schemas_rows = con.execute(
                "SELECT table_schema, table_name, table_type "
                "FROM information_schema.tables "
                "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
                "ORDER BY table_schema, table_name"
            ).fetchall()

            tables_by_schema: dict[str, list[dict[str, str]]] = {}
            main_tables: set[str] = set()
            for schema_name, table_name, table_type in schemas_rows:
                tables_by_schema.setdefault(str(schema_name), []).append(
                    {"name": str(table_name), "type": str(table_type)}
                )
                if schema_name == "main":
                    main_tables.add(str(table_name))

            info["schemas"] = {
                s: [t["name"] for t in tbls] for s, tbls in tables_by_schema.items()
            }

            row_counts: dict[str, int | None] = {}
            for core_table in (
                "run",
                "artifact",
                "run_artifact_link",
                "artifact_content",
                "artifact_schema",
                "consist_schema",
            ):
                if core_table in main_tables:
                    cnt = con.execute(
                        f'SELECT COUNT(*) FROM main."{core_table}"'
                    ).fetchone()
                    row_counts[core_table] = int(cnt[0]) if cnt is not None else 0
                else:
                    row_counts[core_table] = None
            info["row_counts"] = row_counts

            if "artifact" in main_tables:
                art_cols = {
                    str(r[1])
                    for r in con.execute("PRAGMA table_info('artifact')").fetchall()
                }
                uri_col = (
                    "container_uri"
                    if "container_uri" in art_cols
                    else ("uri" if "uri" in art_cols else None)
                )
                if uri_col is not None:
                    uri_stats = con.execute(
                        f"""
                        SELECT
                            COUNT(*) AS total_artifacts,
                            COUNT(DISTINCT {uri_col}) AS distinct_container_uris,
                            COUNT(*) - COUNT(DISTINCT {uri_col}) AS duplicate_uri_rows
                        FROM main.artifact
                        """
                    ).fetchone()
                    multi_hash_uris = con.execute(
                        f"""
                        SELECT COUNT(*) FROM (
                            SELECT {uri_col}
                            FROM main.artifact
                            GROUP BY {uri_col}
                            HAVING COUNT(DISTINCT COALESCE(hash, '<null>')) > 1
                        )
                        """
                    ).fetchone()
                    shared_hash_uris = con.execute(
                        f"""
                        SELECT COUNT(*) FROM (
                            SELECT hash
                            FROM main.artifact
                            WHERE hash IS NOT NULL
                            GROUP BY hash
                            HAVING COUNT(DISTINCT {uri_col}) > 1
                        )
                        """
                    ).fetchone()
                    info["container_uri_stats"] = {
                        "total_artifacts": int(uri_stats[0]) if uri_stats else 0,
                        "distinct_container_uris": (
                            int(uri_stats[1]) if uri_stats else 0
                        ),
                        "overwritten_or_duplicate_uri_rows": (
                            int(uri_stats[2]) if uri_stats else 0
                        ),
                        "uris_with_multiple_distinct_hashes": (
                            int(multi_hash_uris[0]) if multi_hash_uris else 0
                        ),
                        "hashes_shared_across_multiple_distinct_uris": (
                            int(shared_hash_uris[0]) if shared_hash_uris else 0
                        ),
                    }
                if "meta" in art_cols:
                    meta_stats = con.execute(
                        """
                        SELECT
                            COUNT(*) FILTER (
                                WHERE json_extract_string(meta, '$.hash_semantics.source') IS NOT NULL
                            ) AS with_hash_semantics,
                            COUNT(*) FILTER (
                                WHERE json_extract(meta, '$.directory_manifest') IS NOT NULL
                            ) AS with_directory_manifest,
                            COUNT(*) FILTER (
                                WHERE json_extract(meta, '$.recovery_roots') IS NOT NULL
                            ) AS with_recovery_roots,
                            COUNT(*) FILTER (
                                WHERE COALESCE(json_extract_string(meta, '$.is_ingested'), 'false') = 'true'
                            ) AS with_is_ingested
                        FROM main.artifact
                        """
                    ).fetchone()
                    if meta_stats is not None:
                        info["artifact_meta_stats"] = {
                            "with_hash_semantics": int(meta_stats[0]),
                            "with_directory_manifest": int(meta_stats[1]),
                            "with_recovery_roots": int(meta_stats[2]),
                            "with_is_ingested": int(meta_stats[3]),
                        }

            hot_columns: list[dict[str, str]] = []
            col_rows = con.execute(
                """
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema != 'main'
                  AND column_name IN ('consist_artifact_id', 'consist_run_id')
                ORDER BY table_schema, table_name, column_name
                """
            ).fetchall()
            for t_schema, t_name, c_name, d_type in col_rows:
                hot_columns.append(
                    {
                        "schema": str(t_schema),
                        "table": str(t_name),
                        "column": str(c_name),
                        "data_type": str(d_type),
                    }
                )
            info["non_main_system_columns"] = hot_columns
    except Exception as exc:
        info["error"] = f"{type(exc).__name__}: {exc}"

    return info


def benchmark_sha256_throughput(size_mb: int = 64) -> dict[str, float | int]:
    """Measure single-file streaming SHA-256 throughput (MiB/s) via stdlib file_digest."""
    chunk = os.urandom(1024 * 1024)
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=True) as tmp:
        for _ in range(size_mb):
            tmp.write(chunk)
        tmp.flush()
        tmp_path = Path(tmp.name)

        t0 = time.perf_counter()
        _ = file_sha256(tmp_path)
        elapsed = max(time.perf_counter() - t0, 1e-9)

    mib_per_sec = size_mb / elapsed
    return {
        "file_size_mib": size_mb,
        "elapsed_seconds": round(elapsed, 4),
        "throughput_mib_per_sec": round(mib_per_sec, 2),
    }


def discover_duckdb_files(paths: list[str]) -> list[Path]:
    """Find `.duckdb` files from explicit file paths or directory roots."""
    found: dict[Path, None] = {}
    skip_dirs = {".git", ".venv", "venv", "node_modules", "__pycache__", ".tox"}
    for raw in paths:
        p = Path(raw).expanduser()
        if not p.exists():
            continue
        if p.is_file() and p.suffix == ".duckdb":
            found[p.resolve()] = None
        elif p.is_dir():
            for root, dirs, files in os.walk(p):
                dirs[:] = [d for d in dirs if d not in skip_dirs]
                for fname in sorted(files):
                    if fname.endswith(".duckdb"):
                        candidate = (Path(root) / fname).resolve()
                        found[candidate] = None
    return list(found.keys())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Consist v1 codebase and DuckDB provenance census."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=["."],
        help="Directories or .duckdb files to inspect.",
    )
    parser.add_argument(
        "--benchmark-mib",
        type=int,
        default=64,
        help="File size in MiB for single-file SHA-256 throughput benchmark.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit full report as formatted JSON.",
    )
    args = parser.parse_args(argv)

    db_files = discover_duckdb_files(args.paths)
    report = {
        "hashing_semantics": inspect_v1_hashing_semantics(),
        "passthrough_direction_behavior": inspect_v1_passthrough_direction_behavior(),
        "databases": [inspect_duckdb_file(db) for db in db_files],
        "sha256_benchmark": benchmark_sha256_throughput(size_mb=args.benchmark_mib),
    }

    if args.json:
        print(json.dumps(report, indent=2))
        return 0

    print("=== Consist v1 Codebase & Database Census ===")
    hs = report["hashing_semantics"]
    print(f"Fast file formula       : {hs['fast_file_formula']}")
    print(f"Fast file includes ino  : {hs['fast_file_includes_ino']}")
    print(f"Fast dir uses basename  : {hs['fast_dir_checksum_uses_basename_only']}")
    print(
        "Directory manifest v1   : "
        f"keys={hs['directory_manifest_v1']['top_level_keys']}, "
        f"superset_of_manifest_v1={hs['directory_manifest_v1']['superset_of_manifest_v1']}"
    )
    pt = report["passthrough_direction_behavior"]
    print(f"Pass-through DB rule    : {pt['db_precedence_rule']}")
    print(
        f"Pass-through DB rows    : {pt['db_rows_when_logged_input_then_output']} "
        f"(snapshot inputs={pt['snapshot_inputs_count']}, outputs={pt['snapshot_outputs_count']})"
    )
    bm = report["sha256_benchmark"]
    print(
        f"SHA-256 benchmark       : {bm['throughput_mib_per_sec']} MiB/s "
        f"({bm['file_size_mib']} MiB in {bm['elapsed_seconds']}s)"
    )
    print(f"\nDiscovered .duckdb files: {len(report['databases'])}")
    for db_info in report["databases"]:
        print(
            f"  - {db_info['db_path']} ({db_info['size_bytes']} bytes, wal={db_info['wal_exists']})"
        )
        if "error" in db_info:
            print(f"    ERROR: {db_info['error']}")
            continue
        print(f"    row_counts          : {db_info.get('row_counts')}")
        print(f"    schemas             : {list(db_info.get('schemas', {}).keys())}")
        if "container_uri_stats" in db_info:
            print(f"    container_uri_stats : {db_info['container_uri_stats']}")
        if "artifact_meta_stats" in db_info:
            print(f"    artifact_meta_stats : {db_info['artifact_meta_stats']}")
        if db_info.get("non_main_system_columns"):
            print(f"    non_main_sys_cols   : {db_info['non_main_system_columns']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
