import pytest
import multiprocessing
import time
from consist.core.tracker import Tracker


def worker_routine(args):
    """
    A worker function that runs in a separate process.
    It creates a tracker, starts a run, logs artifacts, and ingests data.
    """
    run_dir, db_path, worker_id = args

    # Each process instantiates its own Tracker (simulating separate scripts)
    tracker = Tracker(run_dir=run_dir, db_path=db_path)

    run_id = f"worker_{worker_id}"

    try:
        # Retry logic might be needed here in the future
        with tracker.start_run(run_id, model="stress_test"):
            # Work simulation
            time.sleep(0.1)

            # Log an artifact
            fpath = run_dir / f"{run_id}.txt"
            fpath.write_text(f"data from {worker_id}")
            tracker.log_artifact(fpath, key=f"data_{worker_id}")

            # Simulate DB write contention
            tracker._sync_run_to_db(tracker.current_consist.run)

        return True, ""
    except Exception as e:
        return False, str(e)


@pytest.mark.timeout(30)  # Fail if deadlocked
def test_multiprocess_contention(tmp_path):
    """
    Spawns multiple processes to hammering the same DuckDB file.
    Verifies that Consist handles (or we identify the need to handle) locking.
    """
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    db_path = str(tmp_path / "provenance.duckdb")

    # Initialize DB schema once in main process
    t = Tracker(run_dir=run_dir, db_path=db_path)
    del t  # Close connection

    num_workers = 4
    pool = multiprocessing.Pool(processes=num_workers)

    # Prepare args
    args = [(run_dir, db_path, i) for i in range(num_workers)]

    # Run in parallel
    results = pool.map(worker_routine, args)

    pool.close()
    pool.join()

    # Analyze results
    failures = [err for success, err in results if not success]

    if failures:
        pytest.fail("Concurrency failures detected:\n" + "\n".join(failures))

    # Verify DB integrity
    t_verify = Tracker(run_dir=run_dir, db_path=db_path)
    from consist.models.run import Run
    from sqlmodel import select, Session

    with Session(t_verify.engine) as session:
        runs = session.exec(select(Run)).all()
        assert len(runs) == num_workers
