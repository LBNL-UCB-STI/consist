from pathlib import Path

from sqlmodel import Field, SQLModel

from consist.integrations.beam import BeamConfigAdapter, BeamIngestSpec
from tests.helpers.beam_fixtures import build_beam_test_configs


def test_beam_ingest_and_query(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    run = tracker.begin_run("beam_integration", "beam")
    tracker.canonicalize_config(adapter, [case_dir], run=run)
    tracker.end_run()

    df = tracker.query_df(
        "SELECT c.key FROM global_tables.beam_config_cache c "
        "JOIN global_tables.beam_config_ingest_run_link l "
        "ON l.content_hash = c.content_hash "
        "WHERE l.run_id = ? AND c.key = ?",
        [run.id, "beam.agentsim.agentSampleSizeAsFractionOfPopulation"],
    )
    assert not df.empty


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
    assert tracker.ledger is not None
    with tracker.ledger.exec_cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
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

    rows = tracker.query_rows(
        "SELECT id, value, content_hash FROM global_tables.beam_vehicletypes_cache",
        model=BeamVehicleTypesCache,
    )
    assert rows
