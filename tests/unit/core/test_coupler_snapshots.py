from __future__ import annotations

from pathlib import Path
import uuid

import pytest
from sqlmodel import col, delete, select

from consist.core.coupler import Coupler
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact
from consist.models.run import RunCouplerSnapshot


def _create_artifact(
    *, key: str, uri: str = "workspace://test.csv", content_hash: str = "hash1"
) -> Artifact:
    return Artifact(
        id=uuid.uuid4(),
        key=key,
        container_uri=uri,
        driver="csv",
        hash=content_hash,
    )


def test_run_coupler_snapshot_table_init(tmp_path: Path) -> None:
    db_path = tmp_path / "test_init.duckdb"
    run_dir = tmp_path / "runs"
    run_dir.mkdir(parents=True, exist_ok=True)
    tracker = Tracker(run_dir=run_dir, db_path=str(db_path))
    db = tracker.db
    assert db is not None
    with db.session_scope() as session:
        rows = session.exec(select(RunCouplerSnapshot)).all()
        assert len(rows) == 0


def test_coupler_mapping_protocol_methods() -> None:
    coupler = Coupler()
    art1 = _create_artifact(key="data1")
    art2 = _create_artifact(key="data2")

    coupler["data1"] = art1
    coupler["data2"] = art2

    assert len(coupler) == 2
    assert set(iter(coupler)) == {"data1", "data2"}
    assert set(coupler.keys()) == {"data1", "data2"}
    assert [a.id for a in coupler.values()] == [art1.id, art2.id]
    assert dict(coupler.items()) == {"data1": art1, "data2": art2}

    coupler.clear()
    assert len(coupler) == 0
    assert list(coupler.keys()) == []


def test_coupler_view_mapping_protocol_methods() -> None:
    coupler = Coupler()
    art1 = _create_artifact(key="a")
    art2 = _create_artifact(key="b")
    view = coupler.view("ns")
    view["a"] = art1
    view["b"] = art2

    assert len(view) == 2
    assert set(iter(view)) == {"a", "b"}
    assert set(view.keys()) == {"a", "b"}
    assert [a.id for a in view.values()] == [art1.id, art2.id]
    assert dict(view.items()) == {"a": art1, "b": art2}

    view.clear()
    assert len(view) == 0
    assert len(coupler) == 0


def test_sync_and_get_snapshot(tracker: Tracker) -> None:
    db = tracker.db
    assert db is not None

    art1 = _create_artifact(key="matrix", content_hash="hash_m1")
    art2 = _create_artifact(key="table", content_hash="hash_t1")

    # Persist artifacts first
    with db.session_scope() as session:
        session.add(art1)
        session.add(art2)
        session.commit()
        session.refresh(art1)
        session.refresh(art2)
        session.expunge_all()

    # Test artifacts argument
    db.sync_run_coupler_snapshot(
        run_id="step_1",
        phase="post",
        scenario_run_id="scenario_root",
        step_ordinal=1,
        artifacts={"matrix": art1, "table": art2},
    )

    # Fetch snapshot
    restored = db.get_run_coupler_snapshot("step_1", phase="post")
    assert len(restored) == 2
    assert "matrix" in restored
    assert "table" in restored
    assert str(restored["matrix"].id) == str(art1.id)
    assert restored["matrix"].hash == "hash_m1"

    # Verify session expunge: accessing attributes outside session succeeds
    assert restored["table"].key == "table"

    # Non-existent snapshot
    assert (
        db.get_run_coupler_snapshot("non_existent", phase="post", must_exist=False)
        == {}
    )
    with pytest.raises(KeyError):
        db.get_run_coupler_snapshot("non_existent", phase="post", must_exist=True)


def test_sync_run_coupler_snapshot_overwrite(tracker: Tracker) -> None:
    db = tracker.db
    assert db is not None

    art1 = _create_artifact(key="output_v1", content_hash="h1")
    art2 = _create_artifact(key="output_v2", content_hash="h2")

    with db.session_scope() as session:
        session.add(art1)
        session.add(art2)
        session.commit()
        session.refresh(art1)
        session.refresh(art2)
        session.expunge_all()

    db.sync_run_coupler_snapshot(
        run_id="step_a",
        phase="post",
        scenario_run_id="scen_a",
        step_ordinal=1,
        artifacts={"role1": art1},
    )
    s1 = db.get_run_coupler_snapshot("step_a", phase="post")
    assert set(s1.keys()) == {"role1"}
    assert s1["role1"].hash == "h1"

    # Overwrite with art2 under role2
    db.sync_run_coupler_snapshot(
        run_id="step_a",
        phase="post",
        scenario_run_id="scen_a",
        step_ordinal=1,
        artifacts={"role2": art2},
    )
    s2 = db.get_run_coupler_snapshot("step_a", phase="post")
    assert set(s2.keys()) == {"role2"}
    assert s2["role2"].hash == "h2"


def test_get_max_coupler_snapshot_ordinal(tracker: Tracker) -> None:
    db = tracker.db
    assert db is not None

    assert db.get_max_coupler_snapshot_ordinal("scenario_empty") == 0

    art = _create_artifact(key="art")
    with db.session_scope() as session:
        session.add(art)
        session.commit()
        session.refresh(art)
        session.expunge_all()

    db.sync_run_coupler_snapshot(
        run_id="step_1",
        phase="pre",
        scenario_run_id="scen_1",
        step_ordinal=1,
        artifacts={"art": art},
    )
    db.sync_run_coupler_snapshot(
        run_id="step_2",
        phase="post",
        scenario_run_id="scen_1",
        step_ordinal=5,
        artifacts={"art": art},
    )

    assert db.get_max_coupler_snapshot_ordinal("scen_1") == 5


def test_diff_coupler_snapshots(tracker: Tracker) -> None:
    db = tracker.db
    assert db is not None

    art_common = _create_artifact(key="common", content_hash="unchanged_hash")
    art_left_mod = _create_artifact(key="mod", content_hash="left_hash")
    art_right_mod = _create_artifact(key="mod", content_hash="right_hash")
    art_left_only = _create_artifact(key="left_only", content_hash="left_only_hash")
    art_right_only = _create_artifact(key="right_only", content_hash="right_only_hash")

    with db.session_scope() as session:
        session.add_all(
            [art_common, art_left_mod, art_right_mod, art_left_only, art_right_only]
        )
        session.commit()
        for a in [
            art_common,
            art_left_mod,
            art_right_mod,
            art_left_only,
            art_right_only,
        ]:
            session.refresh(a)
        session.expunge_all()

    db.sync_run_coupler_snapshot(
        run_id="run_left",
        phase="post",
        scenario_run_id="scenario_diff",
        step_ordinal=1,
        artifacts={
            "common": art_common,
            "modified": art_left_mod,
            "removed_in_right": art_left_only,
        },
    )

    db.sync_run_coupler_snapshot(
        run_id="run_right",
        phase="post",
        scenario_run_id="scenario_diff",
        step_ordinal=2,
        artifacts={
            "common": art_common,
            "modified": art_right_mod,
            "added_in_right": art_right_only,
        },
    )

    diff_df = db.diff_coupler_snapshots("run_left", "run_right")
    assert len(diff_df) == 4

    indexed = diff_df.set_index("role_key")
    assert indexed.loc["common", "status"] == "unchanged"
    assert indexed.loc["modified", "status"] == "modified"
    assert indexed.loc["removed_in_right", "status"] == "removed"
    assert indexed.loc["added_in_right", "status"] == "added"


def test_scenario_snapshot_lifecycle(tracker: Tracker, tmp_path: Path) -> None:
    # Seed coupler with bootstrap artifact logged via a real run
    seed_file = tmp_path / "seed.csv"
    seed_file.write_text("col\n1\n")
    with tracker.trace("bootstrap"):
        seed_art = tracker.log_artifact(seed_file, key="seed_data")

    def step1_fn() -> None:
        out1 = tmp_path / "out1.csv"
        out1.write_text("col\n2\n")
        tracker.log_artifact(out1, key="step1_out")

    def step2_fn() -> None:
        out2 = tmp_path / "out2.csv"
        out2.write_text("col\n3\n")
        tracker.log_artifact(out2, key="step2_out")

    with tracker.scenario("test_scen_lifecycle") as sc:
        sc.coupler.set("seed_data", seed_art)

        res1 = sc.run(step1_fn, name="step_1")
        res2 = sc.run(step2_fn, name="step_2")
        scen_id = sc.run_id

    db = tracker.db
    assert db is not None

    # Check init snapshot
    init_snap = db.get_run_coupler_snapshot(scen_id, phase="init")
    assert "seed_data" in init_snap

    # Check step 1 pre and post snapshots
    step1_pre = db.get_run_coupler_snapshot(res1.run.id, phase="pre")
    assert "seed_data" in step1_pre
    assert "step1_out" not in step1_pre

    step1_post = db.get_run_coupler_snapshot(res1.run.id, phase="post")
    assert "seed_data" in step1_post
    assert "step1_out" in step1_post

    # Check step 2 pre and post snapshots
    step2_pre = db.get_run_coupler_snapshot(res2.run.id, phase="pre")
    assert "step1_out" in step2_pre
    assert "step2_out" not in step2_pre

    step2_post = db.get_run_coupler_snapshot(res2.run.id, phase="post")
    assert "seed_data" in step2_post
    assert "step1_out" in step2_post
    assert "step2_out" in step2_post

    # Verify run_coupler_snapshot contains rows for:
    # (scenario_id, "init"), (step1_id, "pre"), (step1_id, "post"), (step2_id, "pre"), (step2_id, "post")
    # and verify step_ordinal strictly increases (0, 1, 1, 2, 2)
    with db.session_scope() as session:
        init_rows = session.exec(
            select(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.run_id) == scen_id,
                col(RunCouplerSnapshot.phase) == "init",
            )
        ).all()
        assert len(init_rows) > 0

        s1_pre_rows = session.exec(
            select(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.run_id) == res1.run.id,
                col(RunCouplerSnapshot.phase) == "pre",
            )
        ).all()
        assert len(s1_pre_rows) > 0

        s1_post_rows = session.exec(
            select(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.run_id) == res1.run.id,
                col(RunCouplerSnapshot.phase) == "post",
            )
        ).all()
        assert len(s1_post_rows) > 0

        s2_pre_rows = session.exec(
            select(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.run_id) == res2.run.id,
                col(RunCouplerSnapshot.phase) == "pre",
            )
        ).all()
        assert len(s2_pre_rows) > 0

        s2_post_rows = session.exec(
            select(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.run_id) == res2.run.id,
                col(RunCouplerSnapshot.phase) == "post",
            )
        ).all()
        assert len(s2_post_rows) > 0

        observed_ordinals = [
            init_rows[0].step_ordinal,
            s1_pre_rows[0].step_ordinal,
            s1_post_rows[0].step_ordinal,
            s2_pre_rows[0].step_ordinal,
            s2_post_rows[0].step_ordinal,
        ]
        assert observed_ordinals == [0, 1, 1, 2, 2]


def test_cache_hit_captures_snapshots(tracker: Tracker, tmp_path: Path) -> None:
    # First execution to populate cache
    out_file = tmp_path / "cache_out.csv"

    def work_fn() -> None:
        out_file.write_text("val\n42\n")
        tracker.log_artifact(out_file, key="cached_val")

    with tracker.scenario("first_run_scen") as sc:
        sc.run(work_fn, name="worker_step")

    # Second execution - cache hit
    with tracker.scenario("second_run_scen") as sc:
        hit_res = sc.run(work_fn, name="worker_step")

    assert hit_res.cache_hit
    db = tracker.db
    assert db is not None

    hit_pre = db.get_run_coupler_snapshot(hit_res.run.id, phase="pre")
    hit_post = db.get_run_coupler_snapshot(hit_res.run.id, phase="post")
    assert "cached_val" not in hit_pre
    assert "cached_val" in hit_post


def test_tracker_get_coupler_snapshot_attaches_tracker_ref(
    tracker: Tracker, tmp_path: Path
) -> None:
    from consist.models.artifact import get_tracker_ref

    out_file = tmp_path / "ref_test.csv"

    def work_fn() -> None:
        out_file.write_text("col\n123\n")
        tracker.log_artifact(out_file, key="ref_art")

    with tracker.scenario("ref_test_scen") as sc:
        res = sc.run(work_fn, name="ref_step")

    snapshot = tracker.get_coupler_snapshot(res.run.id, phase="post", must_exist=True)
    assert "ref_art" in snapshot
    art = snapshot["ref_art"]
    tracker_ref = get_tracker_ref(art)
    assert tracker_ref is not None
    assert tracker_ref() is tracker
    assert art.path == out_file.resolve()


def test_noop_scenario_context_snapshot_methods() -> None:
    from consist.core.noop import NoopTracker

    noop = NoopTracker()
    with noop.scenario("noop_scen") as sc:
        # capture
        sc.capture_coupler_snapshot(phase="post", run_id="r1", step_ordinal=1)
        # restore
        restored = sc.restore_coupler_snapshot(run_id="r1", phase="post")
        assert restored == {}
        # find_latest
        found = sc.find_latest_coupler_snapshot(scenario_run_id="noop_scen")
        assert found is None

    # NoopTracker methods
    snap = noop.get_coupler_snapshot("r1", phase="post")
    assert snap == {}
    diff = noop.diff_coupler_snapshots("r1", "r2")
    assert diff.empty


def test_noop_diff_schema_parity(tracker: Tracker) -> None:
    from consist.core.noop import NoopTracker

    db = tracker.db
    assert db is not None
    real_diff = db.diff_coupler_snapshots("r1", "r2")
    noop = NoopTracker()
    noop_diff = noop.diff_coupler_snapshots("r1", "r2")
    assert list(noop_diff.columns) == list(real_diff.columns)


def test_noop_scenario_context_resume_attrs() -> None:
    from consist.core.noop import NoopTracker

    noop = NoopTracker()
    with noop.scenario("s", resume_from_run_id="run_123", resume_phase="pre") as sc:
        assert sc.resume_from_run_id == "run_123"
        assert sc.resume_phase == "pre"


def test_synthesize_non_existent_run_returns_empty(tracker: Tracker) -> None:
    db = tracker.db
    assert db is not None
    artifacts, ordinal = db.synthesize_coupler_snapshot_from_history(
        run_id="does_not_exist_run_id",
        scenario_run_id="some_scenario",
        phase="post",
    )
    assert artifacts == {}
    assert ordinal == 0
    # Ensure nothing was backfilled
    assert (
        db.get_run_coupler_snapshot(
            "does_not_exist_run_id", phase="post", must_exist=False
        )
        == {}
    )


def test_synthesize_filtered_does_not_backfill(
    tracker: Tracker, tmp_path: Path
) -> None:
    db = tracker.db
    assert db is not None

    f1 = tmp_path / "f1.csv"
    f2 = tmp_path / "f2.csv"

    def step1_fn() -> None:
        f1.write_text("1\n")
        tracker.log_artifact(f1, key="step1_key")

    def step2_fn() -> None:
        f2.write_text("2\n")
        tracker.log_artifact(f2, key="step2_key")

    with tracker.scenario("synth_test_scen") as sc:
        sc.run(step1_fn, name="s1", year=2020)
        sc.run(step2_fn, name="s2", year=2025)
        scen_id = sc.run_id

    # Wipe snapshots
    with db.session_scope() as session:
        session.exec(
            delete(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.scenario_run_id) == scen_id
            )
        )
        session.commit()

    # Synthesize with year=2020 filter
    arts, ord_val = db.synthesize_coupler_snapshot_from_history(
        scenario_run_id=scen_id,
        year=2020,
        backfill=True,
    )
    assert "step1_key" in arts
    assert "step2_key" not in arts

    # Verify no backfilled snapshots exist in DB because it was a filtered synthesis
    with db.session_scope() as session:
        rows = session.exec(
            select(RunCouplerSnapshot).where(
                col(RunCouplerSnapshot.scenario_run_id) == scen_id
            )
        ).all()
        assert len(rows) == 0


def test_scenario_find_latest_coupler_snapshot_attaches_tracker_ref(
    tracker: Tracker, tmp_path: Path
) -> None:
    from consist.models.artifact import get_tracker_ref

    f = tmp_path / "find_latest.csv"

    def step_fn() -> None:
        f.write_text("find_latest\n")
        tracker.log_artifact(f, key="find_key")

    with tracker.scenario("find_scen") as sc:
        sc.run(step_fn, name="step")
        found = sc.find_latest_coupler_snapshot()
        assert found is not None
        _, _, arts = found
        assert "find_key" in arts
        art = arts["find_key"]
        tracker_ref = get_tracker_ref(art)
        assert tracker_ref is not None
        assert tracker_ref() is tracker
        assert art.path == f.resolve()


def test_scenario_resume_pre_phase_ordinal_continuation(
    tracker: Tracker, tmp_path: Path
) -> None:
    db = tracker.db
    assert db is not None

    f1 = tmp_path / "step1.csv"
    f2 = tmp_path / "step2.csv"
    f2_resumed = tmp_path / "step2_resumed.csv"

    def step1_fn() -> None:
        f1.write_text("1\n")
        tracker.log_artifact(f1, key="step1_art")

    def step2_fn() -> None:
        f2.write_text("2\n")
        tracker.log_artifact(f2, key="step2_art")

    with tracker.scenario("scen_ordinal_test") as sc:
        sc.run(step1_fn, name="s1")
        res2 = sc.run(step2_fn, name="s2")

    # res2 was step 2 (step_ordinal=2)
    # When resuming with resume_phase="pre" from res2.run.id:
    # effective_ordinal should be 1 (2 - 1 = 1)
    # Then running the resumed step 2 should increment to ordinal 2 (not 3)
    def step2_resumed_fn() -> None:
        f2_resumed.write_text("2_resumed\n")
        tracker.log_artifact(f2_resumed, key="step2_resumed_art")

    with tracker.scenario(
        "scen_ordinal_resumed",
        resume_from_run_id=res2.run.id,
        resume_phase="pre",
    ) as sc_resumed:
        # Before executing resumed step: _step_ordinal should be 1
        assert sc_resumed._step_ordinal == 1
        res_resumed = sc_resumed.run(step2_resumed_fn, name="s2_rerun")
        # After executing s2_rerun: _step_ordinal should be 2
        assert sc_resumed._step_ordinal == 2

    resumed_snap_pre = db.get_run_coupler_snapshot_details(
        res_resumed.run.id, phase="pre"
    )
    assert resumed_snap_pre[1] == 2
