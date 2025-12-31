from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, Tuple
from uuid import UUID

from sqlmodel import Session, and_, or_, select

from consist.models.artifact import Artifact
from consist.models.run import Run, RunArtifactLink
from consist.models.run_config_kv import RunConfigKV

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


def _seed_base_dataset(
    session: Session,
    *,
    total_runs: int,
    runs_per_scenario: int,
    chunk_size: int,
) -> Dict[str, object]:
    base_time = datetime.now(UTC)

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

    runs: list[Run] = []
    artifacts: list[Artifact] = []
    links: list[RunArtifactLink] = []

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

        run = Run(
            id=run_id,
            parent_run_id=scenario_id,
            status="completed",
            model_name=model_name,
            year=2020 + (i % 3),
            iteration=i % runs_per_scenario,
            tags=tags,
            config_hash=f"cfg_{i % 200}",
            input_hash=f"in_{i % 500}",
            git_hash=f"git_{i % 3}",
            signature=f"sig_{i % 1000}",
            meta=meta,
            started_at=base_time,
            ended_at=base_time,
            created_at=base_time,
            updated_at=base_time,
        )
        runs.append(run)

        artifact = Artifact(
            key=f"artifact_{i}",
            uri=f"outputs://artifact_{i}.parquet",
            driver="parquet",
            hash=f"hash_{i}",
            run_id=run_id,
            created_at=base_time,
        )
        artifacts.append(artifact)
        links.append(
            RunArtifactLink(
                run_id=run_id, artifact_id=artifact.id, direction="output"
            )
        )

        if i == 0:
            sample["cache_tuple"] = (run.config_hash, run.input_hash, run.git_hash)
            sample["scenario_id"] = scenario_id
        if i == total_runs // 2:
            sample["lookup_key"] = artifact.key
            sample["lookup_hash"] = artifact.hash

        if len(runs) >= chunk_size:
            session.add_all(runs)
            session.add_all(artifacts)
            session.add_all(links)
            session.commit()
            runs.clear()
            artifacts.clear()
            links.clear()

    if runs:
        session.add_all(runs)
        session.add_all(artifacts)
        session.add_all(links)
        session.commit()

    scenario_count = total_runs // runs_per_scenario
    if total_runs % runs_per_scenario:
        scenario_count += 1

    sample["total_runs"] = total_runs
    sample["scenario_count"] = scenario_count
    return sample


def _seed_deep_lineage(session: Session, *, depth: int) -> UUID:
    base_time = datetime.now(UTC)
    prev_artifact: Artifact | None = None

    for i in range(depth):
        run_id = f"lineage_run_{i:03d}"
        run = Run(
            id=run_id,
            status="completed",
            model_name="lineage",
            tags=["lineage"],
            created_at=base_time,
            updated_at=base_time,
            started_at=base_time,
            ended_at=base_time,
        )

        output_artifact = Artifact(
            key=f"lineage_out_{i:03d}",
            uri=f"outputs://lineage_out_{i:03d}.parquet",
            driver="parquet",
            hash=f"lineage_hash_{i:03d}",
            run_id=run_id,
            created_at=base_time,
        )

        session.add(run)
        session.add(output_artifact)
        session.add(
            RunArtifactLink(
                run_id=run_id, artifact_id=output_artifact.id, direction="output"
            )
        )

        if prev_artifact is not None:
            session.add(
                RunArtifactLink(
                    run_id=run_id, artifact_id=prev_artifact.id, direction="input"
                )
            )

        prev_artifact = output_artifact

    session.commit()
    if prev_artifact is None:
        raise RuntimeError("Failed to create deep lineage artifacts.")
    return prev_artifact.id


def _seed_wide_lineage(session: Session, *, input_count: int) -> UUID:
    base_time = datetime.now(UTC)
    run_id = "wide_run"

    run = Run(
        id=run_id,
        status="completed",
        model_name="wide_inputs",
        tags=["wide"],
        created_at=base_time,
        updated_at=base_time,
        started_at=base_time,
        ended_at=base_time,
    )
    output_artifact = Artifact(
        key="wide_output",
        uri="outputs://wide_output.parquet",
        driver="parquet",
        hash="wide_output_hash",
        run_id=run_id,
        created_at=base_time,
    )

    session.add(run)
    session.add(output_artifact)
    session.add(
        RunArtifactLink(
            run_id=run_id, artifact_id=output_artifact.id, direction="output"
        )
    )

    inputs: list[Artifact] = []
    links: list[RunArtifactLink] = []
    for i in range(input_count):
        input_artifact = Artifact(
            key=f"wide_input_{i:03d}",
            uri=f"inputs://wide_input_{i:03d}.parquet",
            driver="parquet",
            hash=f"wide_input_hash_{i:03d}",
            created_at=base_time,
        )
        inputs.append(input_artifact)
        links.append(
            RunArtifactLink(
                run_id=run_id, artifact_id=input_artifact.id, direction="input"
            )
        )

    session.add_all(inputs)
    session.add_all(links)
    session.commit()

    return output_artifact.id


def _seed_diff_runs(session: Session, *, num_keys: int) -> Tuple[str, str, int]:
    base_time = datetime.now(UTC)
    run_a_id = "diff_run_a"
    run_b_id = "diff_run_b"
    model_name = "diff_model"

    run_a = Run(
        id=run_a_id,
        status="completed",
        model_name=model_name,
        tags=["diff"],
        created_at=base_time,
        updated_at=base_time,
        started_at=base_time,
        ended_at=base_time,
    )
    run_b = Run(
        id=run_b_id,
        status="completed",
        model_name=model_name,
        tags=["diff"],
        created_at=base_time,
        updated_at=base_time,
        started_at=base_time,
        ended_at=base_time,
    )
    session.add(run_a)
    session.add(run_b)

    rows: list[RunConfigKV] = []
    changed_count = 0
    for i in range(num_keys):
        key = f"param_{i:04d}"
        value_a = float(i)
        value_b = float(i + 1) if i % 10 == 0 else float(i)
        if i % 10 == 0:
            changed_count += 1

        rows.append(
            RunConfigKV(
                run_id=run_a_id,
                facet_id=f"facet_{run_a_id}",
                namespace=model_name,
                key=key,
                value_type="int",
                value_num=value_a,
                created_at=base_time,
            )
        )
        rows.append(
            RunConfigKV(
                run_id=run_b_id,
                facet_id=f"facet_{run_b_id}",
                namespace=model_name,
                key=key,
                value_type="int",
                value_num=value_b,
                created_at=base_time,
            )
        )

    for batch in _chunked(rows, 5_000):
        session.add_all(batch)
        session.commit()

    return run_a_id, run_b_id, changed_count


def _lineage_run_depth(node: Dict[str, object]) -> int:
    producing = node.get("producing_run")
    if not isinstance(producing, dict):
        return 0
    inputs = producing.get("inputs") or []
    depth = 1
    child_depths = [
        _lineage_run_depth(child)
        for child in inputs
        if isinstance(child, dict)
    ]
    if child_depths:
        depth += max(child_depths)
    return depth


def _count_artifacts_keyset(session: Session, *, batch_size: int) -> int:
    last_created = None
    last_id = None
    total = 0
    while True:
        stmt = (
            select(Artifact.id, Artifact.created_at)
            .order_by(Artifact.created_at, Artifact.id)
            .limit(batch_size)
        )
        if last_created is not None and last_id is not None:
            stmt = stmt.where(
                or_(
                    Artifact.created_at > last_created,
                    and_(
                        Artifact.created_at == last_created,
                        Artifact.id > last_id,
                    ),
                )
            )
        batch = session.exec(stmt).all()
        if not batch:
            break
        total += len(batch)
        last_id = batch[-1][0]
        last_created = batch[-1][1]
    return total
