from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, Tuple, TypedDict, cast
from uuid import UUID, uuid4

from consist.core.persistence import DatabaseManager

UTC = timezone.utc


def _chunked(iterable: Iterable, size: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


class SeedSample(TypedDict):
    scale_sizes: Dict[str, int]
    cache_tuple: Tuple[str, str, str]
    scenario_id: str
    lookup_key: str
    lookup_hash: str
    total_runs: int
    scenario_count: int


def _seed_base_dataset(
    db: DatabaseManager,
    *,
    total_runs: int,
    runs_per_scenario: int,
    chunk_size: int,
) -> SeedSample:
    del chunk_size
    base_time = datetime.now(UTC)

    size_10k = min(10_000, total_runs)
    size_50k = min(50_000, total_runs)
    size_100k = total_runs

    with db.transaction() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE _seed_batch AS
            SELECT
                i,
                'run_' || lpad(CAST(i AS VARCHAR), 7, '0') AS run_id,
                'scenario_' || lpad(CAST(i // ? AS VARCHAR), 5, '0') AS scenario_id,
                'model_' || CAST(i % 5 AS VARCHAR) AS model_name,
                uuid() AS art_id,
                'artifact_' || CAST(i AS VARCHAR) AS art_key,
                'outputs://artifact_' || CAST(i AS VARCHAR) || '.parquet' AS art_uri,
                'sha256:file:' || lpad(CAST(i AS VARCHAR), 64, '0') AS art_digest
            FROM range(0, ?) t(i)
            """,
            [runs_per_scenario, total_runs],
        )
        try:
            cur.execute(
                """
                INSERT INTO main.run (
                    id, parent_run_id, status, cacheable, model_name, year, iteration,
                    tags, config_hash, input_hash, git_hash, signature, meta,
                    started_at, ended_at, created_at, updated_at
                )
                SELECT
                    run_id,
                    scenario_id,
                    'completed',
                    TRUE,
                    model_name,
                    2020 + (i % 3),
                    i % ?,
                    to_json(list_filter([
                        'scale_100k',
                        CASE WHEN i < ? THEN 'scale_50k' END,
                        CASE WHEN i < ? THEN 'scale_10k' END,
                        CASE WHEN i % 7 = 0 THEN 'tagged' END
                    ], lambda x: x IS NOT NULL)),
                    'cfg_' || CAST(i % 200 AS VARCHAR),
                    'in_' || CAST(i % 500 AS VARCHAR),
                    'git_' || CAST(i % 3 AS VARCHAR),
                    'sig_' || CAST(i % 1000 AS VARCHAR),
                    CASE
                        WHEN i % 10 = 0 THEN CAST('{"region":"west"}' AS JSON)
                        ELSE CAST('{"region":"east"}' AS JSON)
                    END,
                    ?, ?, ?, ?
                FROM _seed_batch
                """,
                [
                    runs_per_scenario,
                    size_50k,
                    size_10k,
                    base_time,
                    base_time,
                    base_time,
                    base_time,
                ],
            )
            cur.execute(
                """
                INSERT INTO main.artifact (
                    id, key, uri, driver, digest, member, run_id,
                    parent_artifact_id, facet_id, meta, created_at
                )
                SELECT
                    art_id,
                    art_key,
                    art_uri,
                    'parquet',
                    art_digest,
                    NULL,
                    run_id,
                    NULL,
                    NULL,
                    CAST('{}' AS JSON),
                    ?
                FROM _seed_batch
                """,
                [base_time],
            )
            cur.execute(
                """
                INSERT INTO main.run_artifact_link (
                    run_id, artifact_id, direction, role, is_implicit
                )
                SELECT
                    run_id,
                    art_id,
                    'output',
                    art_key,
                    FALSE
                FROM _seed_batch
                """
            )
        finally:
            cur.execute("DROP TABLE IF EXISTS _seed_batch")

    scenario_count = total_runs // runs_per_scenario
    if total_runs % runs_per_scenario:
        scenario_count += 1

    mid = total_runs // 2
    sample: Dict[str, object] = {
        "scale_sizes": {
            "scale_10k": size_10k,
            "scale_50k": size_50k,
            "scale_100k": size_100k,
        },
        "cache_tuple": ("cfg_0", "in_0", "git_0"),
        "scenario_id": "scenario_00000",
        "lookup_key": f"artifact_{mid}",
        "lookup_hash": f"sha256:file:{mid:064d}",
        "total_runs": total_runs,
        "scenario_count": scenario_count,
    }
    return cast(SeedSample, sample)


def _seed_deep_lineage(db: DatabaseManager, *, depth: int) -> UUID:
    base_time = datetime.now(UTC)
    prev_art_id: UUID | None = None
    prev_key: str | None = None

    with db.transaction() as cur:
        for i in range(depth):
            run_id = f"lineage_run_{i:03d}"
            out_id = uuid4()
            out_key = f"lineage_out_{i:03d}"
            cur.execute(
                """
                INSERT INTO main.run (
                    id, status, cacheable, model_name, tags, meta,
                    started_at, ended_at, created_at, updated_at
                )
                VALUES (?, 'completed', TRUE, 'lineage', CAST('["lineage"]' AS JSON), CAST('{}' AS JSON), ?, ?, ?, ?)
                """,
                [run_id, base_time, base_time, base_time, base_time],
            )
            cur.execute(
                """
                INSERT INTO main.artifact (
                    id, key, uri, driver, digest, run_id, meta, created_at
                )
                VALUES (?, ?, ?, 'parquet', ?, ?, CAST('{}' AS JSON), ?)
                """,
                [
                    out_id,
                    out_key,
                    f"outputs://lineage_out_{i:03d}.parquet",
                    f"sha256:file:{i:064x}",
                    run_id,
                    base_time,
                ],
            )
            cur.execute(
                """
                INSERT INTO main.run_artifact_link (
                    run_id, artifact_id, direction, role, is_implicit
                )
                VALUES (?, ?, 'output', ?, FALSE)
                """,
                [run_id, out_id, out_key],
            )
            if prev_art_id is not None and prev_key is not None:
                cur.execute(
                    """
                    INSERT INTO main.run_artifact_link (
                        run_id, artifact_id, direction, role, is_implicit
                    )
                    VALUES (?, ?, 'input', ?, FALSE)
                    """,
                    [run_id, prev_art_id, prev_key],
                )
            prev_art_id = out_id
            prev_key = out_key

    if prev_art_id is None:
        raise RuntimeError("Failed to create deep lineage artifacts.")
    return prev_art_id


def _seed_wide_lineage(db: DatabaseManager, *, input_count: int) -> UUID:
    base_time = datetime.now(UTC)
    run_id = "wide_run"
    out_id = uuid4()

    with db.transaction() as cur:
        cur.execute(
            """
            INSERT INTO main.run (
                id, status, cacheable, model_name, tags, meta,
                started_at, ended_at, created_at, updated_at
            )
            VALUES (?, 'completed', TRUE, 'wide_inputs', CAST('["wide"]' AS JSON), CAST('{}' AS JSON), ?, ?, ?, ?)
            """,
            [run_id, base_time, base_time, base_time, base_time],
        )
        cur.execute(
            """
            INSERT INTO main.artifact (
                id, key, uri, driver, digest, run_id, meta, created_at
            )
            VALUES (?, 'wide_output', 'outputs://wide_output.parquet', 'parquet', 'sha256:file:' || repeat('f', 64), ?, CAST('{}' AS JSON), ?)
            """,
            [out_id, run_id, base_time],
        )
        cur.execute(
            """
            INSERT INTO main.run_artifact_link (
                run_id, artifact_id, direction, role, is_implicit
            )
            VALUES (?, ?, 'output', 'wide_output', FALSE)
            """,
            [run_id, out_id],
        )

        for i in range(input_count):
            in_id = uuid4()
            in_key = f"wide_input_{i:03d}"
            cur.execute(
                """
                INSERT INTO main.artifact (
                    id, key, uri, driver, digest, run_id, meta, created_at
                )
                VALUES (?, ?, ?, 'parquet', ?, NULL, CAST('{}' AS JSON), ?)
                """,
                [
                    in_id,
                    in_key,
                    f"inputs://wide_input_{i:03d}.parquet",
                    f"sha256:file:{i:064x}",
                    base_time,
                ],
            )
            cur.execute(
                """
                INSERT INTO main.run_artifact_link (
                    run_id, artifact_id, direction, role, is_implicit
                )
                VALUES (?, ?, 'input', ?, FALSE)
                """,
                [run_id, in_id, in_key],
            )

    return out_id


def _seed_diff_runs(db: DatabaseManager, *, num_keys: int) -> Tuple[str, str, int]:
    base_time = datetime.now(UTC)
    run_a_id = "diff_run_a"
    run_b_id = "diff_run_b"
    model_name = "diff_model"
    changed_count = (num_keys + 9) // 10 if num_keys > 0 else 0

    with db.transaction() as cur:
        for rid in (run_a_id, run_b_id):
            facet_id = f"facet_{rid}"
            cur.execute(
                """
                INSERT INTO main.run (
                    id, status, cacheable, model_name, facet_id, tags, meta,
                    started_at, ended_at, created_at, updated_at
                )
                VALUES (?, 'completed', TRUE, ?, ?, CAST('["diff"]' AS JSON), CAST('{}' AS JSON), ?, ?, ?, ?)
                """,
                [rid, model_name, facet_id, base_time, base_time, base_time, base_time],
            )
            cur.execute(
                """
                INSERT INTO main.config_facet (
                    id, namespace, schema_name, schema_version, json, created_at
                )
                VALUES (?, ?, 'diff_schema', '1', CAST('{}' AS JSON), ?)
                ON CONFLICT (id) DO NOTHING
                """,
                [facet_id, model_name, base_time],
            )
        cur.execute(
            """
            INSERT INTO main.run_config_kv (
                run_id, facet_id, namespace, key, value_type,
                value_str, value_num, value_bool, value_json, created_at
            )
            SELECT
                ?,
                ?,
                ?,
                'param_' || lpad(CAST(i AS VARCHAR), 4, '0'),
                'int',
                NULL,
                CAST(i AS DOUBLE),
                NULL,
                NULL,
                ?
            FROM range(0, ?) t(i)
            """,
            [run_a_id, f"facet_{run_a_id}", model_name, base_time, num_keys],
        )
        cur.execute(
            """
            INSERT INTO main.run_config_kv (
                run_id, facet_id, namespace, key, value_type,
                value_str, value_num, value_bool, value_json, created_at
            )
            SELECT
                ?,
                ?,
                ?,
                'param_' || lpad(CAST(i AS VARCHAR), 4, '0'),
                'int',
                NULL,
                CASE WHEN i % 10 = 0 THEN CAST(i + 1 AS DOUBLE) ELSE CAST(i AS DOUBLE) END,
                NULL,
                NULL,
                ?
            FROM range(0, ?) t(i)
            """,
            [run_b_id, f"facet_{run_b_id}", model_name, base_time, num_keys],
        )

    return run_a_id, run_b_id, changed_count


def _lineage_run_depth(node: Dict[str, object]) -> int:
    producing = node.get("producing_run")
    if not isinstance(producing, dict):
        return 0
    raw_inputs = producing.get("inputs")
    inputs = raw_inputs if isinstance(raw_inputs, list) else []
    depth = 1
    child_depths = [
        _lineage_run_depth(child) for child in inputs if isinstance(child, dict)
    ]
    if child_depths:
        depth += max(child_depths)
    return depth


def _count_artifacts_keyset(db: DatabaseManager, *, batch_size: int) -> int:
    last_created = None
    last_id = None
    total = 0
    while True:
        if last_created is None or last_id is None:
            batch = db.fetchall(
                "SELECT id, created_at FROM main.artifact "
                "ORDER BY created_at ASC, id ASC LIMIT ?",
                [batch_size],
            )
        else:
            batch = db.fetchall(
                "SELECT id, created_at FROM main.artifact "
                "WHERE created_at > ? OR (created_at = ? AND id > ?) "
                "ORDER BY created_at ASC, id ASC LIMIT ?",
                [last_created, last_created, last_id, batch_size],
            )
        if not batch:
            break
        total += len(batch)
        last_id = batch[-1][0]
        last_created = batch[-1][1]
    return total
