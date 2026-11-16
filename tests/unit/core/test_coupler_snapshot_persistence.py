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
            locs = tracker.db.get_artifact_locations(seeded.id)
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
