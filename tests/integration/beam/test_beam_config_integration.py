from pathlib import Path

from sqlmodel import Field, Session, SQLModel, select

from consist.integrations.beam import BeamConfigAdapter, BeamIngestSpec
from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink
from tests.helpers.beam_fixtures import build_beam_test_configs


def test_beam_ingest_and_query(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    run = tracker.begin_run("beam_integration", "beam")
    tracker.canonicalize_config(adapter, [case_dir], run=run)
    tracker.end_run()

    with Session(tracker.engine) as session:
        rows = session.exec(
            select(BeamConfigCache.key)
            .join(
                BeamConfigIngestRunLink,
                BeamConfigIngestRunLink.content_hash == BeamConfigCache.content_hash,
            )
            .where(BeamConfigIngestRunLink.run_id == run.id)
            .where(
                BeamConfigCache.key
                == "beam.agentsim.agentSampleSizeAsFractionOfPopulation"
            )
        ).all()
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

    id: int = Field(primary_key=True)
    value: str
    content_hash: str = Field(index=True)


def test_beam_tabular_ingest_by_key(tracker, tmp_path: Path):
    with tracker.engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
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

    with Session(tracker.engine) as session:
        rows = session.exec(select(BeamVehicleTypesCache.id)).all()
    assert rows
