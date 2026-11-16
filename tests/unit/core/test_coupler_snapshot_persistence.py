from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

import consist
from consist.core.coupler import Coupler
from consist.core.tracker import Tracker
from consist.models.artifact import Artifact


def test_lazy_seeded_coupler_artifacts_persist_and_restore(tmp_path: Path) -> None:
    ws1 = tmp_path / "ws1"
    ws2 = tmp_path / "ws2"
    db_path = tmp_path / "prov.duckdb"

    ws1.mkdir(parents=True, exist_ok=True)
    ws2.mkdir(parents=True, exist_ok=True)

    persons_path = ws1 / "persons.parquet"
    households_path = ws1 / "households.parquet"
    step_out_path = ws1 / "out.parquet"

    pd.DataFrame({"pid": [1, 2]}).to_parquet(persons_path)
    pd.DataFrame({"hhid": [10, 20]}).to_parquet(households_path)
    pd.DataFrame({"res": [99]}).to_parquet(step_out_path)

    # Session 1: seed raw Path and str in coupler, run step
    t1 = Tracker(run_dir=ws1, db_path=str(db_path))
    scenario_id: str
    step_id: str
    try:
        with t1.scenario("sim_scenario") as sc:
            scenario_id = sc.run_id
            sc.coupler.set("persons", persons_path)
            sc.coupler.set_from_artifact("households", str(households_path))

            # Accessing items resolves lazy entries
            persons_art = sc.coupler["persons"]
            households_art = sc.coupler["households"]
            assert isinstance(persons_art, Artifact)
            assert isinstance(households_art, Artifact)
            persons_id = str(persons_art.id)
            households_id = str(households_art.id)

            with sc.trace("model_step") as step_t:
                assert step_t.current_consist is not None
                step_id = step_t.current_consist.run.id
                # Only log out.parquet, do NOT re-log persons or households
                step_t.log_output(step_out_path, key="out")
    finally:
        t1.close()

    # Verify both seeded artifacts exist in main.artifact and artifact_location
    con = duckdb.connect(str(db_path))
    try:
        for aid in (persons_id, households_id):
            art_count = con.execute(
                "SELECT COUNT(*) FROM artifact WHERE id = ?", [aid]
            ).fetchone()[0]
            assert art_count == 1, f"Artifact {aid} missing from main.artifact"

            loc_count = con.execute(
                "SELECT COUNT(*) FROM artifact_location WHERE artifact_id = ?", [aid]
            ).fetchone()[0]
            assert loc_count >= 1, f"Artifact {aid} missing from artifact_location"
    finally:
        con.close()

    # Session 2: Fresh Tracker, restore coupler snapshot
    t2 = Tracker(run_dir=ws2, db_path=str(db_path))
    try:
        with t2.scenario("inspect_scenario") as sc2:
            res_init = sc2.restore_coupler_snapshot(run_id=scenario_id, phase="init")
            assert "persons" in res_init.artifacts
            assert "households" in res_init.artifacts
            assert isinstance(res_init.artifacts["persons"], Artifact)
            assert isinstance(res_init.artifacts["households"], Artifact)

            res_post = sc2.restore_coupler_snapshot(run_id=step_id, phase="post")
            assert "persons" in res_post.artifacts
            assert "households" in res_post.artifacts
            assert "out" in res_post.artifacts
    finally:
        t2.close()


def test_scenario_context_seed_artifact(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    db_path = tmp_path / "prov.duckdb"
    ws.mkdir(parents=True, exist_ok=True)

    skims_path = ws / "skims.parquet"
    pd.DataFrame({"zone": [1, 2], "cost": [1.5, 2.5]}).to_parquet(skims_path)

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    try:
        with tracker.scenario("seed_demo") as sc:
            # Seed before any step started (register_input=False default)
            seeded = sc.seed_artifact("skims", skims_path, driver="parquet", year=2019)
            assert isinstance(seeded, Artifact)
            assert seeded.key == "skims"
            assert seeded.meta.get("year") == 2019
            assert sc.coupler["skims"] == seeded
            assert "skims" not in sc.inputs

            # Verify in DB immediately
            assert tracker.db is not None
            persisted = tracker.db.get_artifact(seeded.id)
            assert persisted is not None
            locs = tracker.db.get_active_artifact_locations(artifact_id=seeded.id)
            assert len(locs) >= 1

            # consist.load works
            loaded_df = consist.load(sc.coupler["skims"], tracker=tracker)
            assert len(loaded_df) == 2

            # Seed another with register_input=True
            network_path = ws / "network.parquet"
            pd.DataFrame({"link": [100]}).to_parquet(network_path)
            seeded_input = sc.seed_artifact(
                "network", network_path, register_input=True
            )
            assert sc.coupler["network"] == seeded_input
            assert "network" in sc.inputs

            with sc.trace("step_1"):
                # Seed another artifact between/after steps
                extra_path = ws / "extra.parquet"
                pd.DataFrame({"e": [5]}).to_parquet(extra_path)
                seeded_extra = sc.seed_artifact("extra", extra_path)
                assert sc.coupler["extra"] == seeded_extra

            # Seed from an existing Artifact with content_hash, driver, and meta overrides
            overridden = sc.seed_artifact(
                "skims_override",
                seeded,
                driver="parquet",
                content_hash="a" * 64,
                variant="calibrated",
            )
            assert isinstance(overridden, Artifact)
            assert overridden.id != seeded.id
            assert sc.coupler["skims_override"] == overridden
            assert overridden.meta.get("variant") == "calibrated"
            assert overridden.digest is not None
            assert overridden.digest.hex == "a" * 64
            assert tracker.db.get_artifact(overridden.id) is not None
    finally:
        tracker.close()


def test_ephemeral_and_read_only_coupler_raw_paths(tmp_path: Path) -> None:
    ws = tmp_path / "ephemeral"
    ws.mkdir(parents=True, exist_ok=True)
    p = ws / "data.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(p)

    # Ephemeral tracker (db=None)
    t_ephem = Tracker(run_dir=ws)
    try:
        with t_ephem.scenario("ephem") as sc:
            sc.coupler.set("data", p)
            art = sc.coupler["data"]
            assert isinstance(art, Artifact)
            # seed_artifact on ephemeral tracker
            seeded = sc.seed_artifact("data_seeded", p)
            assert isinstance(seeded, Artifact)
    finally:
        t_ephem.close()

    # Read-only tracker with DB
    db_path = tmp_path / "ro.duckdb"
    init_tracker = Tracker(run_dir=ws, db_path=str(db_path))
    init_tracker.close()

    t_ro = Tracker(run_dir=ws, db_path=str(db_path), read_only=True)
    try:
        coupler = Coupler(t_ro)
        coupler.set("data", p)
        art = coupler["data"]
        assert isinstance(art, Artifact)
    finally:
        t_ro.close()


def test_racy_directory_coupler_single_artifact_identity(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    db_path = tmp_path / "prov.duckdb"
    ws.mkdir(parents=True, exist_ok=True)

    persons_path = ws / "persons.parquet"
    pd.DataFrame({"pid": [1, 2]}).to_parquet(persons_path)

    dir_path = ws / "skims_dir"
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / "skim1.txt").write_text("skim-content")

    out1_path = ws / "out1.parquet"
    out2_path = ws / "out2.parquet"
    pd.DataFrame({"a": [1]}).to_parquet(out1_path)
    pd.DataFrame({"b": [2]}).to_parquet(out2_path)

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    try:
        with tracker.scenario("racy_test") as sc:
            sc.coupler.set("persons", persons_path)
            sc.coupler.set("skims", dir_path)

            p_art1 = sc.coupler["persons"]
            s_art1 = sc.coupler["skims"]

            with sc.trace("step1") as st1:
                st1.log_output(out1_path, key="out1")

            p_art2 = sc.coupler["persons"]
            s_art2 = sc.coupler["skims"]
            assert p_art1.id == p_art2.id
            assert s_art1.id == s_art2.id

            with sc.trace("step2") as st2:
                st2.log_output(out2_path, key="out2")

            p_art3 = sc.coupler["persons"]
            s_art3 = sc.coupler["skims"]
            assert p_art1.id == p_art3.id
            assert s_art1.id == s_art3.id

        con = duckdb.connect(str(db_path))
        try:
            # Verify exactly 1 artifact row exists for persons and skims
            p_count = con.execute(
                "SELECT COUNT(*) FROM artifact WHERE key = 'persons'"
            ).fetchone()[0]
            assert p_count == 1
            s_count = con.execute(
                "SELECT COUNT(*) FROM artifact WHERE key = 'skims'"
            ).fetchone()[0]
            assert s_count == 1
        finally:
            con.close()
    finally:
        tracker.close()


def test_coupler_path_does_not_persist_or_hash(tmp_path: Path) -> None:
    ws = tmp_path / "ws"
    db_path = tmp_path / "prov.duckdb"
    ws.mkdir(parents=True, exist_ok=True)

    raw_file = ws / "raw.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(raw_file)

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    try:
        with tracker.scenario("path_test") as sc:
            sc.coupler.set("raw", raw_file)
            p = sc.coupler.path("raw")
            assert p == raw_file.resolve()

        con = duckdb.connect(str(db_path))
        try:
            cnt = con.execute(
                "SELECT COUNT(*) FROM artifact WHERE key = 'raw'"
            ).fetchone()[0]
            assert cnt == 0, (
                f"Expected 0 artifact rows for unaccessed raw path, got {cnt}"
            )
        finally:
            con.close()
    finally:
        tracker.close()


def test_sync_run_coupler_snapshot_does_not_clobber_run_id_or_mutate_existing(
    tmp_path: Path,
) -> None:
    ws = tmp_path / "ws"
    db_path = tmp_path / "prov.duckdb"
    ws.mkdir(parents=True, exist_ok=True)

    f = ws / "f.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(f)

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    try:
        with tracker.start_run("producer_run", model="test"):
            orig_art = tracker.log_output(f, key="f")
        assert orig_art.run_id == "producer_run"

        with tracker.scenario("subsequent_scenario") as sc:
            # Pass a copy with run_id=None or seed alias with metadata
            copy_art = orig_art.model_copy(update={"run_id": None})
            sc.coupler.set("f", copy_art)
            alias_art = sc.seed_artifact("f_alias", orig_art, variant="v2")
            assert alias_art.id != orig_art.id

        con = duckdb.connect(str(db_path))
        try:
            row = con.execute(
                "SELECT run_id, meta FROM artifact WHERE id = ?", [str(orig_art.id)]
            ).fetchone()
            assert row[0] == "producer_run", "Producer run_id was clobbered to NULL!"
            import json

            meta = json.loads(row[1]) if row[1] else {}
            assert "variant" not in meta, (
                "Original artifact was mutated by seed_artifact!"
            )
        finally:
            con.close()
    finally:
        tracker.close()


def test_seed_artifact_in_place_edit_and_unconfigured_mount(tmp_path: Path) -> None:
    import pytest

    ws = tmp_path / "ws"
    db_path = tmp_path / "prov.duckdb"
    ws.mkdir(parents=True, exist_ok=True)

    f = ws / "data.txt"
    f.write_text("v1")

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    try:
        with tracker.scenario("test_edit") as sc:
            art1 = sc.seed_artifact("d", f)
            assert art1.digest is not None

            # Edit file in place outside racy window
            import os
            import time

            f.write_text("v2-modified")
            now = time.time()
            os.utime(f, (now - 10, now - 10))

            art2 = sc.seed_artifact("d_edited", f, reuse_if_unchanged=True)
            assert art2.digest != art1.digest, (
                "Stale digest was returned after in-place edit!"
            )
            assert art2.id != art1.id

            with pytest.raises(ValueError, match="Unconfigured mount scheme"):
                sc.seed_artifact("bad", "unmounted://foo.parquet")
    finally:
        tracker.close()


def test_in_place_edit_of_prior_run_output_does_not_return_stale_artifact(
    tmp_path: Path,
) -> None:
    import os
    import time

    ws = tmp_path / "ws"
    db_path = tmp_path / "prov.duckdb"
    ws.mkdir(parents=True, exist_ok=True)

    full_file = ws / "full.txt"
    fast_file = ws / "fast.txt"
    full_file.write_text("run1-full")
    fast_file.write_text("run1-fast")

    tracker = Tracker(run_dir=ws, db_path=str(db_path))
    try:
        with tracker.start_run("step_1", model="producer"):
            out_full = tracker.log_output(full_file, key="full_out")
            out_fast = tracker.log_output(
                fast_file, key="fast_out", hashing_strategy="fast"
            )
        assert out_full.run_id == "step_1"
        assert out_fast.run_id == "step_1"

        now = time.time()
        os.utime(full_file, (now - 10, now - 10))
        os.utime(fast_file, (now - 10, now - 10))

        with tracker.scenario("sc_edit_check") as sc:
            # Metadata override on unchanged output mints a derived row without mutating step_1
            derived = sc.seed_artifact("full_custom", full_file, tag="override")
            assert derived.id != out_full.id
            assert derived.parent_artifact_id == out_full.id
            orig_db = tracker.get_artifact(out_full.id)
            assert orig_db is not None
            assert orig_db.key == "full_out"
            assert "tag" not in (orig_db.meta or {})

            # reuse_if_unchanged=False on unchanged output mints a fresh standalone artifact
            forced_fresh = sc.seed_artifact(
                "full_forced", full_file, reuse_if_unchanged=False
            )
            assert forced_fresh.id != out_full.id
            assert forced_fresh.run_id is None

            # Edit both files in place
            full_file.write_text("run1-full-MODIFIED")
            fast_file.write_text("run1-fast-MODIFIED-LONGER")
            os.utime(full_file, (now - 5, now - 5))
            os.utime(fast_file, (now - 5, now - 5))

            seeded_full = sc.seed_artifact(
                "full_after_edit", full_file, reuse_if_unchanged=True
            )
            assert seeded_full.id != out_full.id
            assert seeded_full.digest != out_full.digest
            assert seeded_full.run_id is None

            sc.coupler.set("fast_after_edit", fast_file)
            lazy_fast = sc.coupler["fast_after_edit"]
            assert lazy_fast.id != out_fast.id
            assert lazy_fast.digest != out_fast.digest
            assert lazy_fast.run_id is None
    finally:
        tracker.close()
