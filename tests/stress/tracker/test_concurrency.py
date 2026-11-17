from concurrent.futures import ThreadPoolExecutor
import multiprocessing
from pathlib import Path
import time
from typing import Optional, Tuple

import pytest

from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import Run


def worker_routine(args: Tuple[Path, str, int]) -> Tuple[bool, str]:
    """
    A worker function designed to run in a separate process to simulate concurrent Consist usage.

    This function sets up its own `Tracker`, starts a run, simulates some work,
    logs an artifact, and explicitly closes the tracker to release file locks.
    It's used to stress-test Consist's handling of simultaneous database
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
    run_id = f"worker_{worker_id}"
    tracker: Optional[Tracker] = None

    try:
        # Each process instantiates its own Tracker (simulating separate scripts)
        tracker = Tracker(run_dir=run_dir, db_path=db_path)

        with tracker.start_run(run_id, model="stress_test"):
            # Work simulation
            time.sleep(0.1)

            # Log an artifact
            fpath = run_dir / f"{run_id}.txt"
            fpath.write_text(f"data from {worker_id}")
            tracker.log_artifact(fpath, key=f"data_{worker_id}")

            # Simulate explicit sync trigger (though start_run handles it)
            if tracker.current_consist is not None:
                tracker._sync_run_to_db(tracker.current_consist.run)

        return True, ""
    except Exception as e:
        return False, str(e)
    finally:
        if tracker is not None:
            tracker.close()


@pytest.mark.timeout(60)
def test_multithread_shared_tracker_concurrency(tmp_path: Path):
    """
    Tests multi-thread concurrency on a single shared `Tracker` / `DatabaseManager` in one process.

    Verifies that concurrent writes (`sync_run`, `sync_artifact`) and read queries (`query_df`)
    serialized via `DatabaseManager._lock` complete without errors or lost records.
    """
    run_dir = tmp_path / "mt_runs"
    run_dir.mkdir()
    db_path = str(tmp_path / "mt_provenance.duckdb")

    num_threads = 8
    tracker = Tracker(run_dir=run_dir, db_path=db_path)
    try:

        def _thread_worker(worker_id: int) -> None:
            run_id = f"mt_worker_{worker_id}"
            fpath = run_dir / f"{run_id}.txt"
            fpath.write_text(f"payload {worker_id}")

            run = Run(
                id=run_id,
                model_name="mt_stress_test",
                config_hash=None,
                git_hash=None,
                status="completed",
            )
            assert tracker.db is not None
            tracker.db.sync_run(run)

            from consist.digest import Digest

            art = Artifact(
                key=f"mt_data_{worker_id}",
                uri=f"workspace://{run_id}.txt",
                driver="txt",
                digest=Digest("sha256:file", f"{worker_id:064x}"),
                run_id=run_id,
            )
            tracker.db.sync_artifact(art, run_id=run_id, direction="output")

            cnt_df = tracker.query_df("SELECT COUNT(*) AS cnt FROM main.run")
            assert int(cnt_df["cnt"].iloc[0]) >= 1

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(_thread_worker, i) for i in range(num_threads)]
            for fut in futures:
                fut.result()

        runs = tracker.query_rows("SELECT * FROM main.run", model=Run)
        assert len(runs) == num_threads

        art_df = tracker.query_df("SELECT id FROM main.artifact")
        assert len(art_df) == num_threads
    finally:
        tracker.close()


@pytest.mark.timeout(60)
def test_multiprocess_contention(tmp_path: Path):
    """
    Tests Consist's robustness and handling of concurrent database access from multiple processes.

    This stress test spawns several worker processes that simultaneously attempt to
    perform Consist operations (starting runs, logging artifacts) against a single
    shared DuckDB provenance file. It verifies that these operations complete
    without deadlocks or data corruption, highlighting Consist's ability to
    handle concurrent writes.
    """
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    db_path = str(tmp_path / "provenance.duckdb")

    # Initialize DB schema once in main process to reduce initial startup race
    t = Tracker(run_dir=run_dir, db_path=db_path)
    t.close()
    del t

    num_workers = 4
    ctx = multiprocessing.get_context("spawn")
    pool = ctx.Pool(processes=num_workers)

    # Prepare args
    args = [(run_dir, str(db_path), i) for i in range(num_workers)]

    try:
        results = pool.map(worker_routine, args)
    finally:
        pool.close()
        pool.join()

    # Analyze results
    failures = [err for success, err in results if not success]

    if failures:
        pytest.fail("Concurrency failures detected:\n" + "\n".join(failures))

    # Verify DB integrity
    t_verify = Tracker(run_dir=run_dir, db_path=db_path)
    try:
        runs = t_verify.query_rows("SELECT * FROM main.run", model=Run)
        assert len(runs) == num_workers
    finally:
        t_verify.close()
