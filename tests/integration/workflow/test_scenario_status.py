import pytest
from sqlmodel import Session, select

from consist.core.tracker import Tracker
from consist.models.run import Run
from consist.models.artifact import Artifact


def test_scenario_failure_marks_header_and_clears_state(tracker: Tracker):
    with pytest.raises(RuntimeError):
        with tracker.scenario("scenario_fail") as sc:
            with sc.trace("explode"):
                raise RuntimeError("boom")

    header = tracker.get_run("scenario_fail")
    assert header.status == "failed"
    assert header.meta.get("failed_step") == "explode"
    assert "boom" in header.meta.get("failed_with", "")
    assert tracker._last_consist.run.status == "failed"
    assert tracker.current_consist is None


def test_simple_run_status_persists(tracker: Tracker):
    with tracker.start_run("simple_status", "demo"):
        pass

    run = tracker.get_run("simple_status")
    assert run.status == "completed"


def test_scenario_status_persistence_debug(tracker: Tracker):
    """Diagnostic coverage: scenario header status persists after steps."""
    with tracker.scenario("scenario_status_debug") as sc:
        with sc.trace("s1"):
            pass

    header_db = tracker.get_run("scenario_status_debug")
    last_status = getattr(getattr(tracker, "_last_consist", None), "run", None)
    last_status_val = last_status.status if last_status else None

    db_status = None
    if tracker.db:
        with Session(tracker.db.engine) as session:
            db_row = session.exec(
                select(Run).where(Run.id == "scenario_status_debug")
            ).first()
            db_status = db_row.status if db_row else None

    assert header_db.status == "completed", (
        f"Header status mismatch: header_db={header_db.status}, "
        f"_last_consist={last_status_val}, db_row={db_status}"
    )


def test_scenario_missing_declared_outputs_marks_failed(tracker: Tracker):
    with pytest.raises(RuntimeError, match="missing declared outputs"):
        with tracker.scenario("scenario_missing_outputs") as sc:
            sc.declare_outputs("expected", required=True)

    header = tracker.get_run("scenario_missing_outputs")
    assert header.status == "failed"
    assert "expected" in header.meta.get("missing_outputs", [])


def test_scenario_declared_outputs_satisfied(tracker: Tracker):
    with tracker.scenario("scenario_declared_ok") as sc:
        sc.declare_outputs("expected", required=True)
        sc.coupler.set(
            "expected",
            Artifact(
                key="expected", container_uri="workspace://expected.csv", driver="csv"
            ),
        )

    header = tracker.get_run("scenario_declared_ok")
    assert header.status == "completed"
