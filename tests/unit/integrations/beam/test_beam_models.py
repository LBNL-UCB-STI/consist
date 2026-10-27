from sqlalchemy import func
from sqlmodel import Session, SQLModel, select

from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink


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
        session.exec(select(func.count()).select_from(BeamConfigCache))
