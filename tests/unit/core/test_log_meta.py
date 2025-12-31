import pytest


def test_log_meta_normalizes_numpy_types(tracker):
    np = pytest.importorskip("numpy")

    tracker.begin_run("run_meta", "simulate")
    tracker.log_meta(
        alpha=np.float64(0.5),
        count=np.int64(2),
        enabled=np.bool_(True),
        values=np.array([1, 2, 3], dtype=np.int64),
    )
    tracker.end_run()

    run = tracker.get_run("run_meta")
    assert run is not None
    assert run.meta["alpha"] == pytest.approx(0.5)
    assert run.meta["count"] == pytest.approx(2)
    assert run.meta["enabled"] is True
    assert run.meta["values"] == [1, 2, 3]
