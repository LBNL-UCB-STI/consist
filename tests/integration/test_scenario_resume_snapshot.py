from __future__ import annotations

from pathlib import Path
from typing import Tuple
import pytest
from sqlmodel import col, delete, select

import consist
from consist.core.tracker import Tracker
from consist.models.run import RunCouplerSnapshot


@pytest.fixture
def tracker_setup(tmp_path: Path) -> Tuple[Tracker, Path, Path]:
    run_dir = tmp_path / "runs"
    run_dir.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "provenance.duckdb"
    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    return tracker, run_dir, db_path


def test_scenario_mid_workflow_resumption(tmp_path: Path) -> None:
    tmp_db = tmp_path / "mid_workflow.duckdb"
    run_dir = tmp_path / "runs"
    run_dir.mkdir(parents=True, exist_ok=True)

    # 1. Initialize Tracker(db_path=tmp_db)
    tracker1 = Tracker(db_path=str(tmp_db), run_dir=run_dir)

    # 2. Run Scenario S1:
    # - Seed bootstrap artifact raw_data.
    raw_file = tmp_path / "raw.csv"
    raw_file.write_text("raw,data\n10,20\n")
    with tracker1.trace("seed_raw"):
        raw_art = tracker1.log_artifact(raw_file, key="raw_data")

    clean_file = tmp_path / "clean.csv"
    model_file = tmp_path / "model.csv"

    step1_executed = 0
    step2_executed = 0
    step3_executed = 0

    def step1_fn() -> None:
        nonlocal step1_executed
        step1_executed += 1
        clean_file.write_text("clean,data\n10,20\n")
        tracker1.log_artifact(clean_file, key="clean_data")

    def step2_fn() -> None:
        nonlocal step2_executed
        step2_executed += 1
        model_file.write_text("model_result\n42\n")
        tracker1.log_artifact(model_file, key="model_results")

    with tracker1.scenario("S1") as s1:
        s1.coupler.set("raw_data", raw_art)
        s1.run(step1_fn, name="step_1")
        res2 = s1.run(step2_fn, name="step_2")
        step2_run_id = res2.run.id
        step2_hash = res2.outputs["model_results"].hash

    assert step1_executed == 1
    assert step2_executed == 1

    # 3. Close tracker / simulate fresh process by creating a new Tracker(db_path=tmp_db)
    # Reset execution counters to ensure step1 and step2 are never executed during resumption
    step1_executed = 0
    step2_executed = 0

    tracker2 = Tracker(db_path=str(tmp_db), run_dir=run_dir)

    summary_file = tmp_path / "summary.csv"

    def step3_fn() -> None:
        nonlocal step3_executed
        step3_executed += 1
        summary_file.write_text("summary,score\n42,100\n")
        tracker2.log_artifact(summary_file, key="summary_report")

    # 4. Open tracker.scenario("S1_resumed", resume_from_run_id=step2_run_id):
    with tracker2.scenario(
        "S1_resumed",
        resume_from_run_id=step2_run_id,
        resume_phase="post",
    ) as s1_resumed:
        # - Verify scenario.coupler.get("model_results") is immediately present and identical in hash.
        model_res = s1_resumed.coupler.get("model_results")
        assert model_res is not None
        assert model_res.hash == step2_hash

        # - Execute Step 3 consuming model_results and raw_data via scenario.run(...)
        assert "raw_data" in s1_resumed.coupler
        res3 = s1_resumed.run(step3_fn, name="step_3")

    # - Assert Step 3 succeeds without executing Step 1 or Step 2.
    assert step3_executed == 1
    assert step1_executed == 0
    assert step2_executed == 0
    assert "summary_report" in res3.outputs


def test_scenario_resume_from_run_id_post(
    tracker_setup: Tuple[Tracker, Path, Path], tmp_path: Path
) -> None:
    tracker, _, _ = tracker_setup

    file_1 = tmp_path / "step1_out.csv"
    file_2 = tmp_path / "step2_out.csv"

    def step1_fn() -> None:
        file_1.write_text("data_step_1\n")
        tracker.log_artifact(file_1, key="data_1")

    def step2_fn() -> None:
        file_2.write_text("data_step_2\n")
        tracker.log_artifact(file_2, key="data_2")

    with tracker.scenario("original_scenario") as sc:
        res1 = sc.run(step1_fn, name="step_1")
        res2 = sc.run(step2_fn, name="step_2")

    step1_run_id = res1.run.id
    step2_run_id = res2.run.id
    assert step2_run_id != step1_run_id

    # Resume from step 1 post snapshot in a new scenario
    file_2_resumed = tmp_path / "step2_resumed.csv"

    def step2_resumed_fn() -> None:
        file_2_resumed.write_text("data_step_2_resumed\n")
        tracker.log_artifact(file_2_resumed, key="data_2_resumed")

    with tracker.scenario(
        "resumed_scenario",
        resume_from_run_id=step1_run_id,
        resume_phase="post",
    ) as sc_resumed:
        # Coupler should contain step 1 outputs, but not step 2 outputs
        assert "data_1" in sc_resumed.coupler
        assert sc_resumed.coupler.require("data_1").id == res1.outputs["data_1"].id
        assert "data_2" not in sc_resumed.coupler

        # Ordinal tracking should continue
        assert sc_resumed._step_ordinal >= 1

        # Execute next step in resumed scenario consuming data_1
        res2_new = sc_resumed.run(step2_resumed_fn, name="step_2_resumed")

    db = tracker.db
    assert db is not None

    # Verify snapshots for the resumed step
    snap_pre = db.get_run_coupler_snapshot(res2_new.run.id, phase="pre")
    snap_post = db.get_run_coupler_snapshot(res2_new.run.id, phase="post")

    assert "data_1" in snap_pre
    assert "data_2_resumed" not in snap_pre

    assert "data_1" in snap_post
    assert "data_2_resumed" in snap_post


def test_scenario_resume_from_run_id_pre(
    tracker_setup: Tuple[Tracker, Path, Path], tmp_path: Path
) -> None:
    tracker, _, _ = tracker_setup

    seed_file = tmp_path / "seed.csv"
    seed_file.write_text("seed_value\n")
    with tracker.trace("seed_step"):
        seed_art = tracker.log_artifact(seed_file, key="seed_data")

    step1_file = tmp_path / "step1.csv"

    def step1_fn() -> None:
        step1_file.write_text("step1_val\n")
        tracker.log_artifact(step1_file, key="step1_data")

    with tracker.scenario("scen_pre_test") as sc:
        sc.coupler.set("seed_data", seed_art)
        res1 = sc.run(step1_fn, name="step_1")

    # Resume with resume_phase="pre"
    with tracker.scenario(
        "scen_pre_resumed",
        resume_from_run_id=res1.run.id,
        resume_phase="pre",
    ) as sc_resumed:
        # Before step 1 executed: seed_data was in coupler, step1_data was not
        assert "seed_data" in sc_resumed.coupler
        assert "step1_data" not in sc_resumed.coupler


def test_scenario_resume_clear_existing(
    tracker_setup: Tuple[Tracker, Path, Path], tmp_path: Path
) -> None:
    tracker, _, _ = tracker_setup

    out_file = tmp_path / "output.csv"

    def run_step() -> None:
        out_file.write_text("content\n")
        tracker.log_artifact(out_file, key="output_key")

    with tracker.scenario("base_scen") as sc:
        res = sc.run(run_step, name="base_step")

    seed_file = tmp_path / "unrelated.csv"
    seed_file.write_text("unrelated\n")
    with tracker.trace("unrelated_trace"):
        unrelated_art = tracker.log_artifact(seed_file, key="unrelated")

    # Test overlay (clear_existing=False)
    with tracker.scenario("overlay_scen") as sc_overlay:
        sc_overlay.coupler.set("unrelated", unrelated_art)
        sc_overlay.restore_coupler_snapshot(
            run_id=res.run.id, phase="post", clear_existing=False
        )
        assert "unrelated" in sc_overlay.coupler
        assert "output_key" in sc_overlay.coupler

    # Test clear (clear_existing=True)
    with tracker.scenario("clear_scen") as sc_clear:
        sc_clear.coupler.set("unrelated", unrelated_art)
        sc_clear.restore_coupler_snapshot(
            run_id=res.run.id, phase="post", clear_existing=True
        )
        assert "unrelated" not in sc_clear.coupler
        assert "output_key" in sc_clear.coupler


def test_scenario_resume_fallback_to_completed_outputs(
    tracker_setup: Tuple[Tracker, Path, Path], tmp_path: Path
) -> None:
    tracker, _, _ = tracker_setup
    db = tracker.db
    assert db is not None

    f1 = tmp_path / "fallback_1.csv"
    f2 = tmp_path / "fallback_2.csv"

    def step1_fn() -> None:
        f1.write_text("1\n")
        tracker.log_artifact(f1, key="role_1")

    def step2_fn() -> None:
        f2.write_text("2\n")
        tracker.log_artifact(f2, key="role_2")

    with tracker.scenario("legacy_scen") as sc:
        sc.run(step1_fn, name="s1")
        res2 = sc.run(step2_fn, name="s2")
        legacy_scen_id = sc.run_id

    # Wipe run_coupler_snapshot rows for this scenario to simulate legacy runs
    with db.session_scope() as session:
        session.exec(
            delete(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.scenario_run_id) == legacy_scen_id
            )
        )
        session.commit()

    # Verify no snapshots exist
    assert (
        db.get_run_coupler_snapshot(res2.run.id, phase="post", must_exist=False) == {}
    )

    # Restore in a new scenario using fallback_to_completed_outputs
    with tracker.scenario("new_scen") as sc_new:
        restored = sc_new.restore_coupler_snapshot(
            run_id=res2.run.id,
            phase="post",
            fallback_to_completed_outputs=True,
        )
        assert "role_1" in restored
        assert "role_2" in restored
        assert "role_1" in sc_new.coupler
        assert "role_2" in sc_new.coupler

    # Verify fallback backfilled the snapshot in DuckDB under original scenario ID
    backfilled = db.get_run_coupler_snapshot(
        res2.run.id, phase="post", must_exist=False
    )
    assert "role_1" in backfilled
    assert "role_2" in backfilled

    with db.session_scope() as session:
        rows = session.exec(
            select(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.run_id) == res2.run.id,
                col(RunCouplerSnapshot.phase) == "post",
            )
        ).all()
        assert len(rows) == 2
        for r in rows:
            assert r.scenario_run_id == legacy_scen_id


def test_scenario_resume_fallback_pre_phase_excludes_step_outputs(
    tracker_setup: Tuple[Tracker, Path, Path], tmp_path: Path
) -> None:
    tracker, _, _ = tracker_setup
    db = tracker.db
    assert db is not None

    f1 = tmp_path / "step1.txt"
    f2 = tmp_path / "step2.txt"

    def step1_fn() -> None:
        f1.write_text("s1\n")
        tracker.log_artifact(f1, key="role_1")

    def step2_fn() -> None:
        f2.write_text("s2\n")
        tracker.log_artifact(f2, key="role_2")

    with tracker.scenario("legacy_scen_pre") as sc:
        sc.run(step1_fn, name="s1")
        res2 = sc.run(step2_fn, name="s2")
        legacy_scen_id = sc.run_id

    # Wipe run_coupler_snapshot rows for this scenario
    with db.session_scope() as session:
        session.exec(
            delete(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.scenario_run_id) == legacy_scen_id
            )
        )
        session.commit()

    # Restore with phase="pre" on res2.run.id
    # It should include role_1 (from s1), but NOT role_2 (from s2 itself)
    with tracker.scenario("resumed_scen_pre") as sc_new:
        restored = sc_new.restore_coupler_snapshot(
            run_id=res2.run.id,
            phase="pre",
            fallback_to_completed_outputs=True,
        )
        assert "role_1" in restored
        assert "role_2" not in restored
        assert "role_1" in sc_new.coupler
        assert "role_2" not in sc_new.coupler


def test_scenario_resume_fallback_preserves_execution_order(
    tracker_setup: Tuple[Tracker, Path, Path], tmp_path: Path
) -> None:
    tracker, _, _ = tracker_setup
    db = tracker.db
    assert db is not None

    f_z = tmp_path / "z_file.txt"
    f_a = tmp_path / "a_file.txt"

    # Step "z_first" runs FIRST and writes "shared_role"
    def step_z() -> None:
        f_z.write_text("from_z\n")
        tracker.log_artifact(f_z, key="shared_role")

    # Step "a_second" runs SECOND and overwrites "shared_role"
    def step_a() -> None:
        f_a.write_text("from_a\n")
        tracker.log_artifact(f_a, key="shared_role")

    with tracker.scenario("order_scen") as sc:
        sc.run(step_z, name="z_first")
        res_a = sc.run(step_a, name="a_second")
        scen_id = sc.run_id

    # Wipe snapshots
    with db.session_scope() as session:
        session.exec(
            delete(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.scenario_run_id) == scen_id
            )
        )
        session.commit()

    # Fallback restore for the scenario should respect execution order:
    # "a_second" was executed after "z_first", so its version of "shared_role" should win
    with tracker.scenario("resumed_order") as sc_new:
        restored = sc_new.restore_coupler_snapshot(
            scenario_run_id=scen_id,
            fallback_to_completed_outputs=True,
        )
        assert "shared_role" in restored
        assert restored["shared_role"].hash == res_a.outputs["shared_role"].hash


def test_scenario_diff_snapshots_across_scenarios(
    tracker_setup: Tuple[Tracker, Path, Path], tmp_path: Path
) -> None:
    tracker, _, _ = tracker_setup

    file_a_comm = tmp_path / "comm_a.txt"
    file_a_only = tmp_path / "only_a.txt"
    file_b_comm = tmp_path / "comm_b.txt"
    file_b_only = tmp_path / "only_b.txt"

    def step_a() -> None:
        file_a_comm.write_text("content_alpha\n")
        file_a_only.write_text("only_a_data\n")
        tracker.log_artifact(file_a_comm, key="shared_role")
        tracker.log_artifact(file_a_only, key="role_a")

    def step_b() -> None:
        file_b_comm.write_text("content_alpha_modified\n")
        file_b_only.write_text("only_b_data\n")
        tracker.log_artifact(file_b_comm, key="shared_role")
        tracker.log_artifact(file_b_only, key="role_b")

    with tracker.scenario("scenario_a") as sc_a:
        res_a = sc_a.run(step_a, name="step_a")

    with tracker.scenario("scenario_b") as sc_b:
        res_b = sc_b.run(step_b, name="step_b")

    # Use tracker convenience method and top-level consist API
    diff_df = tracker.diff_coupler_snapshots(res_a.run.id, res_b.run.id)
    diff_api = consist.diff_coupler_snapshots(
        res_a.run.id, res_b.run.id, tracker=tracker
    )

    assert diff_df.equals(diff_api)

    indexed = diff_df.set_index("role_key")
    assert indexed.loc["shared_role", "status"] == "modified"
    assert (
        indexed.loc["shared_role", "left_hash"]
        != indexed.loc["shared_role", "right_hash"]
    )
    assert indexed.loc["role_a", "status"] == "removed"
    assert indexed.loc["role_b", "status"] == "added"


def test_scenario_resume_across_tracker_instances(tmp_path: Path) -> None:
    run_dir = tmp_path / "shared_runs"
    db_path = str(tmp_path / "provenance.duckdb")

    # Process 1: run simulation year 1 and year 2 step 1
    t1 = Tracker(run_dir=run_dir, db_path=db_path)

    skims_file = tmp_path / "skims.omx"
    skims_file.write_text("skims_v1\n")

    datastore_file_y1 = tmp_path / "datastore_y1.h5"
    datastore_file_y1.write_text("h5_year1_data\n")

    datastore_file_y2 = tmp_path / "datastore_y2.h5"
    datastore_file_y2.write_text("h5_year2_mutated_data\n")

    def year1_step() -> None:
        t1.log_artifact(skims_file, key="final_skims_omx")
        t1.log_artifact(datastore_file_y1, key="usim_datastore_h5")

    def year2_step1() -> None:
        # In year 2, usim_datastore_h5 is mutated and republished
        t1.log_artifact(datastore_file_y2, key="usim_datastore_h5")

    with t1.scenario("pilates_2017_2019") as sc1:
        sc1.run(year1_step, name="year1_step")
        res_y2_s1 = sc1.run(year2_step1, name="year2_step1")
        y2_s1_id = res_y2_s1.run.id
        y2_s1_hash = res_y2_s1.outputs["usim_datastore_h5"].hash

    # Simulate process death / restart by initializing a new Tracker
    # Process 2: new tracker, new scenario resuming from year2_step1
    t2 = Tracker(run_dir=run_dir, db_path=db_path)

    final_report_file = tmp_path / "final_report.csv"

    def year2_step2() -> None:
        final_report_file.write_text("finished_successfully\n")
        t2.log_artifact(final_report_file, key="final_report")

    with t2.scenario(
        "pilates_2017_2019_resumed",
        resume_from_run_id=y2_s1_id,
        resume_phase="post",
    ) as sc2:
        # Verify skims_omx from year 1 is restored
        assert "final_skims_omx" in sc2.coupler
        # Verify usim_datastore_h5 is the year 2 version
        assert "usim_datastore_h5" in sc2.coupler
        restored_ds = sc2.coupler.require("usim_datastore_h5")
        assert restored_ds.hash == y2_s1_hash

        res_final = sc2.run(year2_step2, name="year2_step2")

    # Verify final step recorded snapshots containing all accumulated roles
    db = t2.db
    assert db is not None
    post_snap = db.get_run_coupler_snapshot(res_final.run.id, phase="post")
    assert "final_skims_omx" in post_snap
    assert "usim_datastore_h5" in post_snap
    assert "final_report" in post_snap
