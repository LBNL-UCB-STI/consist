from __future__ import annotations

from datetime import datetime, timezone

from consist.core.persistence import DatabaseManager
from consist.models.run import Run
from consist.models.run_config_kv import RunConfigKV


def test_insert_run_config_kv_bulk_upserts_duplicate_keys_idempotently(
    tmp_path,
) -> None:
    """Duplicate RunConfigKV inserts in a batch upsert cleanly without crashing."""
    db = DatabaseManager(str(tmp_path / "provenance.duckdb"))
    try:
        row_a = RunConfigKV(
            run_id="run_1",
            facet_id="facet_a",
            namespace="model",
            key="param",
            value_type="str",
            value_str="alpha",
        )
        row_b = RunConfigKV(
            run_id="run_1",
            facet_id="facet_a",
            namespace="model",
            key="param",
            value_type="str",
            value_str="beta",
        )

        db.insert_run_config_kv_bulk([row_a, row_b])

        rows = db.query_rows(
            "SELECT * FROM run_config_kv WHERE run_id = ?",
            ["run_1"],
            model=RunConfigKV,
        )
        assert len(rows) == 1
        assert rows[0].key == "param"
        assert rows[0].value_str == "beta"
    finally:
        db.close()


def test_sync_run_with_facet_bundle_replaces_kv_rows_idempotently(tmp_path) -> None:
    """Re-syncing Run config facets replaces KV rows cleanly without duplicates."""
    db = DatabaseManager(str(tmp_path / "provenance_facet_sync.duckdb"))
    try:
        now = datetime.now(timezone.utc)
        run = Run(
            id="run_1",
            model_name="model",
            status="completed",
            created_at=now,
            started_at=now,
            ended_at=now,
        )
        db.sync_run(run)
        db.insert_run_config_kv_bulk(
            [
                RunConfigKV(
                    run_id="run_1",
                    facet_id="facet_a",
                    namespace="model",
                    key="param",
                    value_type="str",
                    value_str="alpha",
                )
            ]
        )
        db.insert_run_config_kv_bulk(
            [
                RunConfigKV(
                    run_id="run_1",
                    facet_id="facet_a",
                    namespace="model",
                    key="param",
                    value_type="str",
                    value_str="beta",
                )
            ]
        )

        rows = db.query_rows(
            "SELECT * FROM run_config_kv WHERE run_id = ?",
            ["run_1"],
            model=RunConfigKV,
        )
        assert len(rows) == 1
        assert rows[0].value_str == "beta"
    finally:
        db.close()
