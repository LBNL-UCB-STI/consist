import datetime

from consist.core.tracker import Tracker
from consist.models.run import Run


def test_parent_status_update_not_blocked(tracker: Tracker):
    """
    Verifies the run.parent_run_id relationship is not enforced by a blocking FK in DuckDB
    so parent statuses can be updated while children reference them.
    """
    db = tracker.db
    assert db is not None

    now = datetime.datetime.now(datetime.timezone.utc)

    parent = Run(
        id="parent_run",
        model_name="model",
        config_hash=None,
        git_hash=None,
        status="running",
        tags=[],
        started_at=now,
        created_at=now,
    )
    child = Run(
        id="child_run",
        model_name="model",
        config_hash=None,
        git_hash=None,
        status="completed",
        parent_run_id="parent_run",
        tags=[],
        started_at=now,
        created_at=now,
    )
    db.sync_run(parent)
    db.sync_run(child)

    # Attempt to change parent status; this previously failed with DuckDB FK enforcement.
    refreshed = db.get_run("parent_run")
    assert refreshed is not None
    refreshed.status = "completed"
    db.sync_run(refreshed)

    rows = db.query_rows(
        "SELECT * FROM main.run WHERE id = ?",
        ["parent_run"],
        model=Run,
    )
    assert len(rows) == 1
    assert rows[0].status == "completed"
