from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Dict, Iterable, Tuple, TypedDict, cast
from uuid import UUID, uuid4

from consist.digest import Digest, Identity
from consist.location import Location, Observation
from consist.store.ledger import StoreLedger

UTC = timezone.utc


def _chunked(iterable: Iterable[Any], size: int):
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


def _flush_seed_batch(
    ledger: StoreLedger,
    *,
    run_rows: list[tuple[Any, ...]],
    content_rows: list[tuple[Any, ...]],
    digest_rows: list[tuple[Any, ...]],
    artifact_rows: list[tuple[Any, ...]],
    location_rows: list[tuple[Any, ...]],
    binding_rows: list[tuple[Any, ...]],
) -> None:
    if not run_rows and not artifact_rows and not binding_rows:
        return
    with ledger.transaction() as cur:
        if run_rows:
            cur.executemany(
                "INSERT INTO main.run ("
                "id, model_name, parent_run_id, status, year, iteration, "
                "code_hash, git_hash, config_hash, input_hash, signature, "
                "signature_scheme, cache_status, tags, meta, "
                "started_at, ended_at, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                run_rows,
            )
        if content_rows:
            cur.executemany(
                "INSERT INTO main.content (anchor, repr_kind, created_at) "
                "VALUES (?, ?, ?) ON CONFLICT (anchor) DO NOTHING",
                content_rows,
            )
        if digest_rows:
            cur.executemany(
                "INSERT INTO main.content_digest "
                "(anchor, scheme, value, basis, computed_at) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT (anchor, scheme) DO NOTHING",
                digest_rows,
            )
        if artifact_rows:
            cur.executemany(
                "INSERT INTO main.artifact "
                "(id, key, driver, anchor, run_id, meta, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                artifact_rows,
            )
        if location_rows:
            cur.executemany(
                "INSERT INTO main.artifact_location ("
                "observation_id, anchor, uri, access_driver, basis, site, "
                "observed_at_ns, writer"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                location_rows,
            )
        if binding_rows:
            cur.executemany(
                "INSERT INTO main.run_binding ("
                "run_id, direction, occurrence_index, slot, ordinal, "
                "artifact_id, role, anchor"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (run_id, direction, slot, ordinal) DO NOTHING",
                binding_rows,
            )
    run_rows.clear()
    content_rows.clear()
    digest_rows.clear()
    artifact_rows.clear()
    location_rows.clear()
    binding_rows.clear()


def _seed_base_dataset(
    ledger: StoreLedger,
    *,
    total_runs: int,
    runs_per_scenario: int,
    chunk_size: int,
) -> SeedSample:
    base_time = datetime.now(UTC)
    base_ns = int(base_time.timestamp() * 1_000_000_000)

    size_10k = min(10_000, total_runs)
    size_50k = min(50_000, total_runs)
    size_100k = total_runs

    sample: Dict[str, object] = {
        "scale_sizes": {
            "scale_10k": size_10k,
            "scale_50k": size_50k,
            "scale_100k": size_100k,
        }
    }

    run_rows: list[tuple[Any, ...]] = []
    content_rows: list[tuple[Any, ...]] = []
    digest_rows: list[tuple[Any, ...]] = []
    artifact_rows: list[tuple[Any, ...]] = []
    location_rows: list[tuple[Any, ...]] = []
    binding_rows: list[tuple[Any, ...]] = []

    for i in range(total_runs):
        run_id = f"run_{i:07d}"
        scenario_id = f"scenario_{i // runs_per_scenario:05d}"
        model_name = f"model_{i % 5}"

        tags = ["scale_100k"]
        if i < size_50k:
            tags.append("scale_50k")
        if i < size_10k:
            tags.append("scale_10k")
        if i % 7 == 0:
            tags.append("tagged")

        meta = {"region": "west"} if i % 10 == 0 else {"region": "east"}
        cfg_hash = f"cfg_{i % 200}"
        in_hash = f"in_{i % 500}"
        git_hash = f"git_{i % 3}"
        sig_hash = f"sig_{i % 1000}"

        run_rows.append(
            (
                run_id,
                model_name,
                scenario_id,
                "completed",
                2020 + (i % 3),
                i % runs_per_scenario,
                git_hash,
                git_hash,
                cfg_hash,
                in_hash,
                sig_hash,
                "sig-v2",
                "live",
                json.dumps(tags, separators=(",", ":")),
                json.dumps(meta, separators=(",", ":")),
                base_time,
                base_time,
                base_time,
                base_time,
            )
        )

        art_id = uuid4()
        art_key = f"artifact_{i}"
        hex_digest = f"{i + 1:064x}"
        anchor = f"sha256:file:{hex_digest}"
        uri = f"outputs://artifact_{i}.parquet"

        content_rows.append((anchor, "file", base_time))
        digest_rows.append((anchor, "sha256:file", hex_digest, "computed", base_time))
        artifact_rows.append(
            (art_id, art_key, "parquet", anchor, run_id, "{}", base_time)
        )
        location_rows.append(
            (
                uuid4(),
                anchor,
                uri,
                "parquet",
                "computed",
                "local",
                base_ns + i,
                ledger.writer,
            )
        )
        binding_rows.append((run_id, "output", 0, art_key, 0, art_id, art_key, anchor))

        if i == 0:
            sample["cache_tuple"] = (cfg_hash, in_hash, git_hash)
            sample["scenario_id"] = scenario_id
        if i == total_runs // 2:
            sample["lookup_key"] = art_key
            sample["lookup_hash"] = anchor

        if len(run_rows) >= chunk_size:
            _flush_seed_batch(
                ledger,
                run_rows=run_rows,
                content_rows=content_rows,
                digest_rows=digest_rows,
                artifact_rows=artifact_rows,
                location_rows=location_rows,
                binding_rows=binding_rows,
            )

    _flush_seed_batch(
        ledger,
        run_rows=run_rows,
        content_rows=content_rows,
        digest_rows=digest_rows,
        artifact_rows=artifact_rows,
        location_rows=location_rows,
        binding_rows=binding_rows,
    )

    scenario_count = total_runs // runs_per_scenario
    if total_runs % runs_per_scenario:
        scenario_count += 1

    sample["total_runs"] = total_runs
    sample["scenario_count"] = scenario_count
    return cast(SeedSample, sample)


def _seed_deep_lineage(ledger: StoreLedger, *, depth: int) -> UUID:
    base_time = datetime.now(UTC)
    base_ns = int(base_time.timestamp() * 1_000_000_000)
    prev_art_id: UUID | None = None
    prev_anchor: str | None = None
    prev_key: str | None = None

    with ledger.transaction():
        for i in range(depth):
            run_id = f"lineage_run_{i:03d}"
            ledger.record_run(
                run_id=run_id,
                model_name="lineage",
                status="completed",
                tags=["lineage"],
                started_at=base_time,
                ended_at=base_time,
                created_at=base_time,
                updated_at=base_time,
            )

            art_id = uuid4()
            out_key = f"lineage_out_{i:03d}"
            hex_digest = f"{10_000_000 + i:064x}"
            anchor = f"sha256:file:{hex_digest}"
            ledger.record_content(anchor, "file", created_at=base_time)
            ledger.record_artifact(
                artifact_id=art_id,
                key=out_key,
                driver="parquet",
                anchor=anchor,
                run_id=run_id,
                created_at=base_time,
            )
            ledger.record_observation(
                Observation(
                    location=Location(uri=f"outputs://lineage_out_{i:03d}.parquet"),
                    identity=Identity(Digest.parse(anchor)),
                    access_driver="parquet",
                    basis="computed",
                    site="local",
                    observed_at_ns=base_ns + i,
                ),
                repr_kind=None,
            )
            ledger.record_run_binding(
                run_id=run_id,
                direction="output",
                occurrence_index=0,
                slot=out_key,
                artifact_id=art_id,
                role=out_key,
                anchor=anchor,
            )

            if (
                prev_art_id is not None
                and prev_anchor is not None
                and prev_key is not None
            ):
                ledger.record_run_binding(
                    run_id=run_id,
                    direction="input",
                    occurrence_index=0,
                    slot=prev_key,
                    artifact_id=prev_art_id,
                    role=prev_key,
                    anchor=prev_anchor,
                )

            prev_art_id = art_id
            prev_anchor = anchor
            prev_key = out_key

    if prev_art_id is None:
        raise RuntimeError("Failed to create deep lineage artifacts.")
    return prev_art_id


def _seed_wide_lineage(ledger: StoreLedger, *, input_count: int) -> UUID:
    base_time = datetime.now(UTC)
    base_ns = int(base_time.timestamp() * 1_000_000_000)
    run_id = "wide_run"

    out_id = uuid4()
    out_hex = f"{20_000_000:064x}"
    out_anchor = f"sha256:file:{out_hex}"

    content_rows: list[tuple[Any, ...]] = [(out_anchor, "file", base_time)]
    digest_rows: list[tuple[Any, ...]] = [
        (out_anchor, "sha256:file", out_hex, "computed", base_time)
    ]
    artifact_rows: list[tuple[Any, ...]] = [
        (out_id, "wide_output", "parquet", out_anchor, run_id, "{}", base_time)
    ]
    location_rows: list[tuple[Any, ...]] = [
        (
            uuid4(),
            out_anchor,
            "outputs://wide_output.parquet",
            "parquet",
            "computed",
            "local",
            base_ns,
            ledger.writer,
        )
    ]
    binding_rows: list[tuple[Any, ...]] = [
        (run_id, "output", 0, "wide_output", 0, out_id, "wide_output", out_anchor)
    ]

    for i in range(input_count):
        in_id = uuid4()
        in_key = f"wide_input_{i:03d}"
        in_hex = f"{20_000_001 + i:064x}"
        in_anchor = f"sha256:file:{in_hex}"
        content_rows.append((in_anchor, "file", base_time))
        digest_rows.append((in_anchor, "sha256:file", in_hex, "computed", base_time))
        artifact_rows.append(
            (in_id, in_key, "parquet", in_anchor, None, "{}", base_time)
        )
        location_rows.append(
            (
                uuid4(),
                in_anchor,
                f"inputs://wide_input_{i:03d}.parquet",
                "parquet",
                "computed",
                "local",
                base_ns + i + 1,
                ledger.writer,
            )
        )
        binding_rows.append((run_id, "input", i, in_key, 0, in_id, in_key, in_anchor))

    run_rows: list[tuple[Any, ...]] = [
        (
            run_id,
            "wide_inputs",
            None,
            "completed",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "live",
            json.dumps(["wide"], separators=(",", ":")),
            "{}",
            base_time,
            base_time,
            base_time,
            base_time,
        )
    ]
    _flush_seed_batch(
        ledger,
        run_rows=run_rows,
        content_rows=content_rows,
        digest_rows=digest_rows,
        artifact_rows=artifact_rows,
        location_rows=location_rows,
        binding_rows=binding_rows,
    )
    return out_id


def _seed_diff_runs(ledger: StoreLedger, *, num_keys: int) -> Tuple[str, str, int]:
    base_time = datetime.now(UTC)
    run_a_id = "diff_run_a"
    run_b_id = "diff_run_b"
    model_name = "diff_model"

    kv_a: dict[str, dict[str, Any]] = {}
    kv_b: dict[str, dict[str, Any]] = {}
    changed_count = 0
    for i in range(num_keys):
        key = f"param_{i:04d}"
        value_a = float(i)
        value_b = float(i + 1) if i % 10 == 0 else float(i)
        if i % 10 == 0:
            changed_count += 1
        kv_a[key] = {"t": "int", "n": value_a}
        kv_b[key] = {"t": "int", "n": value_b}

    with ledger.transaction() as cur:
        cur.executemany(
            "INSERT INTO main.run ("
            "id, model_name, status, cache_status, tags, meta, facet_kv, "
            "started_at, ended_at, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    run_a_id,
                    model_name,
                    "completed",
                    "live",
                    json.dumps(["diff"], separators=(",", ":")),
                    json.dumps({"config_facet_id": f"facet_{run_a_id}"}),
                    json.dumps(kv_a, sort_keys=True, separators=(",", ":")),
                    base_time,
                    base_time,
                    base_time,
                    base_time,
                ),
                (
                    run_b_id,
                    model_name,
                    "completed",
                    "live",
                    json.dumps(["diff"], separators=(",", ":")),
                    json.dumps({"config_facet_id": f"facet_{run_b_id}"}),
                    json.dumps(kv_b, sort_keys=True, separators=(",", ":")),
                    base_time,
                    base_time,
                    base_time,
                    base_time,
                ),
            ],
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


def _count_artifacts_keyset(ledger: StoreLedger, *, batch_size: int) -> int:
    last_created = None
    last_id = None
    total = 0
    while True:
        with ledger.exec_cursor() as cur:
            if last_created is None or last_id is None:
                batch = cur.execute(
                    "SELECT id, created_at FROM main.artifact "
                    "ORDER BY created_at ASC, id ASC LIMIT ?",
                    [batch_size],
                ).fetchall()
            else:
                batch = cur.execute(
                    "SELECT id, created_at FROM main.artifact "
                    "WHERE created_at > ? OR (created_at = ? AND id > ?) "
                    "ORDER BY created_at ASC, id ASC LIMIT ?",
                    [last_created, last_created, last_id, batch_size],
                ).fetchall()
        if not batch:
            break
        total += len(batch)
        last_id = batch[-1][0]
        last_created = batch[-1][1]
    return total
