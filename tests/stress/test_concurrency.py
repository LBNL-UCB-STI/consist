from pathlib import Path
from typing import Tuple

import pytest
import multiprocessing
import time
from consist.core.tracker import Tracker


def worker_routine(args: Tuple[Path, str, int]) -> Tuple[bool, str]:
    """
    A worker function designed to run in a separate process to simulate concurrent Consist usage.

    This function sets up its own `Tracker`, starts a run, simulates some work,
    logs an artifact, and explicitly disposes of the database engine to release
    file locks. It's used to stress-test Consist's handling of simultaneous database
    access from multiple processes.

    Parameters
    ----------
    args : Tuple[Path, str, int]
        A tuple containing:
        - `run_dir` (Path): The base directory for run logs and artifacts.
        - `db_path` (str): The file path to the shared DuckDB database.
        - `worker_id` (int): A unique identifier for the worker process, used to
          create unique `run_id`s and artifact keys.

    Returns
    -------
    Tuple[bool, str]
        A tuple where the first element is `True` if the routine completed successfully
        without exceptions, and `False` otherwise. The second element is an empty string
        on success, or an error message on failure.
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

            # Simulate explicit sync trigger (though start_run handles it)
            tracker._sync_run_to_db(tracker.current_consist.run)

        # CRITICAL FIX: Explicitly dispose engine to release file lock immediately
        if tracker.engine:
            tracker.engine.dispose()

        return True, ""
    except Exception as e:
        return False, str(e)


@pytest.mark.timeout(60)  # Increased timeout for safer concurrency checks
def test_multiprocess_contention(tmp_path: Path):
    """
    Tests Consist's robustness and handling of concurrent database access from multiple processes.

    This stress test spawns several worker processes that simultaneously attempt to
    perform Consist operations (starting runs, logging artifacts) against a single
    shared DuckDB provenance file. It verifies that these operations complete
    without deadlocks or data corruption, highlighting Consist's ability to
    handle concurrent writes.

    What happens:
    1. A temporary directory is set up for run logs and the shared DuckDB file.
    2. The DuckDB schema is initialized once by a main `Tracker` instance to prevent
       initial race conditions.
    3. A pool of `num_workers` processes is created.
    4. Each worker process executes `worker_routine`, which involves:
       - Instantiating its own `Tracker`.
       - Starting a Consist run.
       - Simulating work and logging an artifact.
       - Explicitly disposing the database engine to release file locks.
    5. The main process waits for all worker processes to complete.

    What's checked:
    - No worker process reports an error during its execution, implying successful
      handling of concurrent database operations.
    - After all workers complete, a verification `Tracker` instance confirms that
      the DuckDB contains `num_workers` completed `Run` entries, indicating that
      all runs were successfully persisted.
    """
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    db_path = str(tmp_path / "provenance.duckdb")

    # Initialize DB schema once in main process to reduce initial startup race
    t = Tracker(run_dir=run_dir, db_path=db_path)
    if t.engine:
        t.engine.dispose()
    del t  # Close connection

    num_workers = 4
    pool = multiprocessing.Pool(processes=num_workers)

    # Prepare args
    # Ensure arguments are picklable (Path objects are fine)
    args = [(run_dir, str(db_path), i) for i in range(num_workers)]

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
