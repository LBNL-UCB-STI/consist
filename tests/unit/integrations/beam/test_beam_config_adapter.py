import logging
from pathlib import Path

from sqlalchemy import text
from sqlmodel import Session, SQLModel

from consist.integrations.beam import BeamConfigAdapter, BeamConfigOverrides
from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink
from tests.helpers.beam_fixtures import build_beam_test_configs


def test_beam_models_register_tables(tracker):
    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")
    with tracker.engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        SQLModel.metadata.create_all(
            connection,
            tables=[
                BeamConfigCache.__table__,
                BeamConfigIngestRunLink.__table__,
            ],
        )
    with Session(tracker.engine) as session:
        session.exec(text("SELECT count(*) FROM global_tables.beam_config_cache"))


def test_beam_discover_includes_and_hash(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    assert canonical.primary_config == overlay_conf
    assert any(p.name == "common.conf" for p in canonical.config_files)
    assert canonical.content_hash


def test_beam_canonicalize_artifacts_and_rows(tracker, tmp_path: Path, caplog):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_unit", "beam")
    with caplog.at_level(logging.WARNING):
        result = adapter.canonicalize(canonical, run=run, tracker=tracker)
    artifact_keys = {spec.key for spec in result.artifacts}
    assert any("base.conf" in key for key in artifact_keys)
    assert any("sample.csv" in key for key in artifact_keys)
    assert any("missing/does-not-exist.csv" in r.message for r in caplog.records)
    ingest_tables = {spec.table_name for spec in result.ingestables}
    assert "beam_config_cache" in ingest_tables
    assert "beam_config_ingest_run_link" in ingest_tables


def test_beam_materialize_overrides(tracker, tmp_path: Path):
    from pyhocon import ConfigFactory

    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    overrides = BeamConfigOverrides(
        values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.75}
    )
    output_dir = tmp_path / "materialized"
    canonical = adapter.materialize(
        [case_dir],
        overrides,
        output_dir=output_dir,
        identity=tracker.identity,
        strict=True,
    )
    config = ConfigFactory.parse_file(str(canonical.primary_config), resolve=True)
    assert config.get("beam.agentsim.agentSampleSizeAsFractionOfPopulation") == 0.75


def test_beam_materialize_from_plan_uses_config_dirs(tracker, tmp_path: Path):
    from pyhocon import ConfigFactory

    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    plan = tracker.prepare_config(adapter, [case_dir])
    overrides = BeamConfigOverrides(
        values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.9}
    )
    output_dir = tmp_path / "materialized_plan"
    canonical = adapter.materialize_from_plan(
        plan,
        overrides,
        output_dir=output_dir,
        identity=tracker.identity,
        strict=True,
    )
    config = ConfigFactory.parse_file(str(canonical.primary_config), resolve=True)
    assert config.get("beam.agentsim.agentSampleSizeAsFractionOfPopulation") == 0.9
