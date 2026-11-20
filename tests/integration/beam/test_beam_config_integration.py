from pathlib import Path
import shutil

from sqlmodel import Field, SQLModel

from consist.integrations.beam import (
    BeamConfigAdapter,
    BeamIngestSpec,
    BeamReferencePolicy,
)
from consist.types import CacheOptions, ExecutionOptions
from tests.helpers.beam_fixtures import build_beam_test_configs


def test_beam_ingest_and_query(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    run = tracker.begin_run("beam_integration", "beam")
    tracker.canonicalize_config(adapter, [case_dir], run=run)
    tracker.end_run()

    rows = tracker.run_query(
        """
        SELECT c.key
        FROM global_tables.beam_config_cache c
        JOIN global_tables.beam_config_ingest_run_link l
          ON l.content_hash = c.content_hash
        WHERE l.run_id = ?
          AND c.key = ?
        """,
        [run.id, "beam.agentsim.agentSampleSizeAsFractionOfPopulation"],
    )
    assert rows


def test_beam_facet_extraction(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    plan = tracker.prepare_config(
        adapter,
        [case_dir],
        facet_spec={"keys": ["beam.agentsim.simulationName"]},
        facet_schema_name="beam_config",
    )
    assert plan.facet["beam.agentsim.simulationName"] == "beam-test"


class BeamVehicleTypesCache(SQLModel, table=True):
    __tablename__ = "beam_vehicletypes_cache"
    __table_args__ = {"schema": "global_tables"}

    id: int | None = Field(default=None, primary_key=True)
    value: str
    content_hash: str = Field(index=True)


def test_beam_tabular_ingest_by_key(tracker, tmp_path: Path):
    assert tracker.db is not None
    tracker.db.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(
        primary_config=overlay_conf,
        ingest_specs=[
            BeamIngestSpec(
                key="beam.agentsim.agents.vehicles.vehicleTypesFilePath",
                table_name="beam_vehicletypes_cache",
                schema=BeamVehicleTypesCache,
            )
        ],
    )
    run = tracker.begin_run("beam_tabular_ingest", "beam")
    tracker.canonicalize_config(adapter, [case_dir], run=run)
    tracker.end_run()

    rows = tracker.run_query("SELECT value FROM global_tables.beam_vehicletypes_cache")
    assert rows


def test_beam_delegated_reference_cold_workspace_cache_hit_and_artifact_invalidation(
    tracker, tmp_path: Path
):
    key = "beam.exchange.scenario.folder"
    durable_input = tmp_path / "durable" / "scenario.txt"
    durable_input.parent.mkdir()
    durable_input.write_bytes(b"original scenario")
    calls: list[bytes] = []

    def build_adapter(workspace: Path, *, materialized: bool) -> BeamConfigAdapter:
        config_root = workspace / "config"
        config_root.mkdir(parents=True)
        scenario = workspace / "inputs" / "scenario"
        if materialized:
            scenario.mkdir(parents=True)
            (scenario / "scenario.txt").write_bytes(durable_input.read_bytes())
        primary = config_root / "beam.conf"
        primary.write_text(
            f'{key} = "{scenario}"\n',
            encoding="utf-8",
        )
        return BeamConfigAdapter(
            root_dirs=[config_root],
            primary_config=primary,
            path_aliases={"beam_workspace": workspace},
            reference_policies={
                key: BeamReferencePolicy(
                    identity_policy="delegated_to_artifacts",
                    delegated_artifact_keys=("scenario_input",),
                )
            },
        )

    def step(ctx, scenario_input: Path) -> None:
        content = scenario_input.read_bytes()
        calls.append(content)
        (ctx.run_dir / "summary.txt").write_bytes(content)

    workspace_a = tmp_path / "workspace-a"
    first = tracker.run(
        name="beam_delegated_replay",
        model="beam",
        fn=step,
        adapter=build_adapter(workspace_a, materialized=True),
        inputs={"scenario_input": durable_input},
        output_paths={"summary": "summary.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(cache_mode="overwrite"),
    )
    original_output = tracker.run_artifact_dir(first.run) / "summary.txt"
    assert original_output.read_bytes() == b"original scenario"
    shutil.rmtree(workspace_a)

    workspace_b = tmp_path / "workspace-b"
    adapter_b = build_adapter(workspace_b, materialized=False)
    replay = tracker.run(
        name="beam_delegated_replay",
        model="beam",
        fn=step,
        adapter=adapter_b,
        inputs={"scenario_input": durable_input},
        output_paths={"summary": "summary.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(
            cache_mode="reuse",
            cache_hydration="outputs-requested",
            validate_cached_outputs="eager",
        ),
    )

    assert replay.cache_hit is True
    assert calls == [b"original scenario"]
    hydrated_output = tracker.run_artifact_dir(replay.run) / "summary.txt"
    assert hydrated_output.read_bytes() == b"original scenario"

    durable_input.write_bytes(b"changed scenario")
    invalidated = tracker.run(
        name="beam_delegated_replay",
        model="beam",
        fn=step,
        adapter=adapter_b,
        inputs={"scenario_input": durable_input},
        output_paths={"summary": "summary.txt"},
        execution_options=ExecutionOptions(inject_context="ctx"),
        cache_options=CacheOptions(
            cache_mode="reuse",
            cache_hydration="outputs-requested",
            validate_cached_outputs="eager",
        ),
    )

    assert invalidated.cache_hit is False
    assert calls == [b"original scenario", b"changed scenario"]
