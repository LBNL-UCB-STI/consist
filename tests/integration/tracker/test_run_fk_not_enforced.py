import datetime
from sqlmodel import Session, select

from consist.core.tracker import Tracker
from consist.models.run import Run


def test_parent_status_update_not_blocked(tracker: Tracker):
    """
    Verifies the run.parent_run_id constraint is NOT ENFORCED in DuckDB so parent
    statuses can be updated while children reference them.
    """
    db = tracker.db
    assert db is not None
    # Ensure constraint is dropped/relaxed before inserting
    db._relax_run_parent_fk()

    now = datetime.datetime.now(datetime.timezone.utc)

    with Session(db.engine) as session:
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
        session.add(parent)
        session.add(child)
        session.commit()
        session.expunge(parent)

    # Attempt to change parent status; this previously failed with DuckDB FK enforcement.
    refreshed = db.get_run("parent_run")
    refreshed.status = "completed"
    db.sync_run(refreshed)

    with Session(db.engine) as session:
        refreshed = session.exec(select(Run).where(Run.id == "parent_run")).first()

    assert refreshed is not None
    assert refreshed.status == "completed"
