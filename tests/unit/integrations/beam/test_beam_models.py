from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink


def test_beam_models_register_tables(tracker):
    assert tracker.ledger is not None
    assert BeamConfigCache.__table__.schema == "global_tables"
    assert BeamConfigIngestRunLink.__table__.schema == "global_tables"
    with tracker.ledger.transaction() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS global_tables.beam_config_cache ("
            "content_hash VARCHAR, key VARCHAR)"
        )
    df = tracker.query_df("SELECT COUNT(*) AS n FROM global_tables.beam_config_cache")
    assert int(df.iloc[0]["n"]) == 0
