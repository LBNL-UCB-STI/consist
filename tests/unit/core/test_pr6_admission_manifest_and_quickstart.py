from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid

import duckdb
import h5py
import numpy as np
import pandas as pd
import pytest
import zarr

from consist import (
    HydratedAdmittedInput,
    Tracker,
    archive,
    ensure_digest,
    evict,
    hydrate_admitted_input,
    locate,
    observe,
    promote_historical_artifact,
    seal,
)
from consist.store.manifest import (
    export_run_manifest,
    import_run_manifest,
    load_run_manifest,
    rebuild_store_from_manifests,
    write_run_manifest,
)
from consist.admission import (
    AdmissionIdentity,
    DeclaredDigestExpectation,
    PriorRunExpectation,
    check_artifact_identity,
)
from consist.core.views import ViewFactory
from consist.digest import (
    Agreement,
    Digest,
    Identity,
    Kind,
    compute_logical_matrices_sha256,
    file_sha256,
)
from consist.engine import PendingOutput
from consist.location import Mount, SiteContext
from consist.migrations.v1.fasthash import stat_v1_fast_file_hash
from consist.migrations.v1.migrator import migrate_v1_to_v2
from consist.store.detect import ConsistError
from consist.store.ledger import StoreLedger
from tests.helpers.v1_db_builder import create_v1_schema


def test_readme_quick_example_and_quickstart_workflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Execute the exact symmetric inputs/outputs Quick Example from README.md and quickstart.md."""
    monkeypatch.chdir(tmp_path)

    raw_df = pd.DataFrame({"id": [1, 2, 3, 4], "value": [0.1, 0.6, 0.8, 0.2]})
    raw_df.to_parquet(tmp_path / "raw.parquet", index=False)

    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "provenance.duckdb")

    calls = {"clean": 0, "summarize": 0}

    def clean_data(raw: Path, cleaned: Path, threshold: float = 0.5) -> None:
        calls["clean"] += 1
        df = pd.read_parquet(raw)
        df[df["value"] > threshold].to_parquet(cleaned, index=False)

    first = tracker.run(
        fn=clean_data,
        inputs={"raw": "raw.parquet"},
        outputs={"cleaned": "cleaned.parquet"},
        config={"threshold": 0.5},
    )

    second = tracker.run(
        fn=clean_data,
        inputs={"raw": "raw.parquet"},
        outputs={"cleaned": "cleaned.parquet"},
        config={"threshold": 0.5},
    )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert calls["clean"] == 1

    cleaned = second.outputs["cleaned"]
    assert cleaned.path.exists()
    df_loaded = cleaned.as_df()
    assert list(df_loaded["id"]) == [2, 3]

    def summarize(cleaned: Path, summary: Path) -> None:
        calls["summarize"] += 1
        df = pd.read_parquet(cleaned)
        df.describe().to_csv(summary)

    report = tracker.run(
        fn=summarize,
        inputs={"cleaned": first.outputs["cleaned"]},
        outputs={"summary": "summary.csv"},
    )
    assert report.cache_hit is False
    assert calls["summarize"] == 1
    assert report.outputs["summary"].path.exists()

    # Also test the rewritten quickstart.md workflow end-to-end
    (tmp_path / "input.csv").write_text("value\n4\n9\n16\n", encoding="utf-8")
    qs_calls = {"summarize": 0, "make_report": 0}

    def qs_summarize(data: Path, summary: Path) -> None:
        qs_calls["summarize"] += 1
        mean = pd.read_csv(data)["value"].mean()
        summary.write_text(f"mean={mean:.1f}\n", encoding="utf-8")

    qs_first = tracker.run(
        fn=qs_summarize,
        inputs={"data": "input.csv"},
        outputs={"summary": "summary.txt"},
    )
    qs_second = tracker.run(
        fn=qs_summarize,
        inputs={"data": "input.csv"},
        outputs={"summary": "summary.txt"},
    )
    assert qs_first.cache_hit is False
    assert qs_second.cache_hit is True
    assert qs_calls["summarize"] == 1
    assert (
        qs_second.outputs["summary"].path.read_text(encoding="utf-8").strip()
        == "mean=9.7"
    )

    # Change input.csv -> cache miss (mean=12.7)
    (tmp_path / "input.csv").write_text("value\n4\n9\n25\n", encoding="utf-8")
    qs_changed = tracker.run(
        fn=qs_summarize,
        inputs={"data": "input.csv"},
        outputs={"summary": "summary.txt"},
    )
    assert qs_changed.cache_hit is False
    assert qs_calls["summarize"] == 2
    assert (
        qs_changed.outputs["summary"].path.read_text(encoding="utf-8").strip()
        == "mean=12.7"
    )

    # Put original data back -> cache hit (content-based, ignores new timestamp)
    (tmp_path / "input.csv").write_text("value\n4\n9\n16\n", encoding="utf-8")
    qs_reverted = tracker.run(
        fn=qs_summarize,
        inputs={"data": "input.csv"},
        outputs={"summary": "summary.txt"},
    )
    assert qs_reverted.cache_hit is True
    assert qs_calls["summarize"] == 2
    assert (
        qs_reverted.outputs["summary"].path.read_text(encoding="utf-8").strip()
        == "mean=9.7"
    )

    # Connect two steps (make_report consuming qs_reverted.outputs["summary"])
    def make_report(summary: Path, report: Path) -> None:
        qs_calls["make_report"] += 1
        report.write_text("# Report\n\n" + summary.read_text(encoding="utf-8"))

    report_res = tracker.run(
        fn=make_report,
        inputs={"summary": qs_reverted.outputs["summary"]},
        outputs={"report": "report.md"},
    )
    assert report_res.cache_hit is False
    assert qs_calls["make_report"] == 1
    assert (
        report_res.outputs["report"].path.read_text(encoding="utf-8")
        == "# Report\n\nmean=9.7\n"
    )


def test_ghost_mode_lazy_caching_and_downstream_behavior(
    tmp_path: Path,
) -> None:
    """
    Verify Ghost Mode claims in docs/architecture.md:
    1. In pipeline A -> B -> C, deleting A's output on disk while keeping B's
       causes rerunning A -> B -> C to hit cache on all three steps without
       executing A, B, or C.
    2. If C misses (e.g. C's config changes) while A's output is deleted and
       B's output is still present, A and B still hit cache and C executes
       using B's surviving output without rerunning A.
    3. Reading a deleted cached output raises FileNotFoundError, and if B
       itself misses when A's output is deleted and un-archived, B raises a
       clear error naming A's artifact key and producing run_id.
    """
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "provenance.duckdb")
    raw_input = tmp_path / "raw.csv"
    raw_input.write_text("x\n1\n2\n3\n", encoding="utf-8")

    counts = {"a": 0, "b": 0, "c": 0}

    def step_a(raw: Path, a_out: Path) -> None:
        counts["a"] += 1
        df = pd.read_csv(raw)
        df["a"] = df["x"] * 10
        df.to_csv(a_out, index=False)

    def step_b(a_in: Path, b_out: Path, scale: int = 2) -> None:
        counts["b"] += 1
        df = pd.read_csv(a_in)
        df["b"] = df["a"] * scale
        df.to_csv(b_out, index=False)

    def step_c(b_in: Path, c_out: Path, offset: int = 0) -> None:
        counts["c"] += 1
        df = pd.read_csv(b_in)
        total = int(df["b"].sum()) + offset
        c_out.write_text(f"total={total}\n", encoding="utf-8")

    # Initial pipeline run: A -> B -> C all execute
    res_a1 = tracker.run(
        fn=step_a,
        inputs={"raw": raw_input},
        outputs={"a_out": "a.csv"},
    )
    res_b1 = tracker.run(
        fn=step_b,
        inputs={"a_in": res_a1.outputs["a_out"]},
        outputs={"b_out": "b.csv"},
        config={"scale": 2},
    )
    res_c1 = tracker.run(
        fn=step_c,
        inputs={"b_in": res_b1.outputs["b_out"]},
        outputs={"c_out": "c.txt"},
        config={"offset": 0},
    )
    assert (res_a1.cache_hit, res_b1.cache_hit, res_c1.cache_hit) == (
        False,
        False,
        False,
    )
    assert counts == {"a": 1, "b": 1, "c": 1}

    # Delete A's bulky intermediate output on disk ("Ghost Mode")
    a_out_source = res_a1.outputs["a_out"].path
    a_out_source.unlink()
    assert not a_out_source.exists()
    assert res_b1.outputs["b_out"].path.exists()

    # 1. Rerun A -> B -> C unchanged: all three hit cache, none execute!
    res_a2 = tracker.run(
        fn=step_a,
        inputs={"raw": raw_input},
        outputs={"a_out": "a.csv"},
    )
    res_b2 = tracker.run(
        fn=step_b,
        inputs={"a_in": res_a2.outputs["a_out"]},
        outputs={"b_out": "b.csv"},
        config={"scale": 2},
    )
    res_c2 = tracker.run(
        fn=step_c,
        inputs={"b_in": res_b2.outputs["b_out"]},
        outputs={"c_out": "c.txt"},
        config={"offset": 0},
    )
    assert (res_a2.cache_hit, res_b2.cache_hit, res_c2.cache_hit) == (
        True,
        True,
        True,
    )
    assert counts == {"a": 1, "b": 1, "c": 1}
    assert res_c2.outputs["c_out"].path.read_text(encoding="utf-8") == "total=120\n"

    # 2. Change C's config while A's output is still deleted:
    # A and B hit cache, C executes using B's surviving output without rerunning A!
    res_a3 = tracker.run(
        fn=step_a,
        inputs={"raw": raw_input},
        outputs={"a_out": "a.csv"},
    )
    res_b3 = tracker.run(
        fn=step_b,
        inputs={"a_in": res_a3.outputs["a_out"]},
        outputs={"b_out": "b.csv"},
        config={"scale": 2},
    )
    res_c3 = tracker.run(
        fn=step_c,
        inputs={"b_in": res_b3.outputs["b_out"]},
        outputs={"c_out": "c.txt"},
        config={"offset": 5},
    )
    assert (res_a3.cache_hit, res_b3.cache_hit, res_c3.cache_hit) == (
        True,
        True,
        False,
    )
    assert counts == {"a": 1, "b": 1, "c": 2}
    assert res_c3.outputs["c_out"].path.read_text(encoding="utf-8") == "total=125\n"

    # 3. Reading A's deleted output raises FileNotFoundError
    with pytest.raises(FileNotFoundError):
        res_a3.outputs["a_out"].path.read_text(encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        res_a3.outputs["a_out"].as_df()

    # And if B itself misses while A's output is deleted, B raises an error naming
    # A's artifact key ('a_out') and producing run_id
    with pytest.raises(ValueError, match=r"key='a_out'.*produced by run"):
        tracker.run(
            fn=step_b,
            inputs={"a_in": res_a3.outputs["a_out"]},
            outputs={"b_out": "b.csv"},
            config={"scale": 3},
        )


def test_decision_d3_quick_migration_historical_run_promote_and_view_factory(
    tmp_path: Path,
) -> None:
    """
    Decision D3 End-to-End:
    1. Create a v1 database with a fast-hashed Parquet artifact.
    2. Run `migrate_v1_to_v2(..., quick=True)`.
    3. Verify the artifact loads from `StoreLedger` with `Kind.HISTORICAL` (`legacy-v1:unverified`)
       and does not produce false positive comparisons.
    4. Promote via `promote_historical_artifact(db_path, ...)` and verify its `Artifact.identity`
       upgrades to the verified `sha256:file` anchor (preserving `recorded_anchor`), resolves
       `.path` and `.as_df()`, and is queryable through `ViewFactory`.
    """
    db_path = tmp_path / "d3_prov.duckdb"
    parquet_path = tmp_path / "households.parquet"
    df_hh = pd.DataFrame({"hh_id": [10, 20, 30], "income": [55000, 72000, 91000]})
    df_hh.to_parquet(parquet_path, index=False)

    fast_hash, _size, _mtime_ns = stat_v1_fast_file_hash(parquet_path)
    art_id = uuid.uuid4()
    created_at = datetime.now(timezone.utc)

    con = duckdb.connect(str(db_path))
    try:
        create_v1_schema(con)
        con.execute(
            "INSERT INTO run (id, model_name, status, signature, created_at, updated_at, started_at, ended_at, meta, tags) "
            "VALUES ('run_d3_v1', 'synth_pop', 'completed', 'sig_v1', ?, ?, ?, ?, '{}', '[]')",
            [created_at, created_at, created_at, created_at],
        )
        con.execute(
            "INSERT INTO artifact (id, key, container_uri, driver, hash, run_id, meta, created_at) "
            "VALUES (?, 'households', ?, 'parquet', ?, 'run_d3_v1', ?, ?)",
            [
                str(art_id),
                str(parquet_path),
                fast_hash,
                json.dumps(
                    {
                        "hash_semantics": {
                            "version": 1,
                            "algorithm": "sha256",
                            "kind": "file",
                            "digest_contract": "file_metadata",
                            "source": "computed_fast",
                        }
                    }
                ),
                created_at,
            ],
        )
        con.execute(
            "INSERT INTO run_artifact_link (run_id, artifact_id, direction, is_implicit) "
            "VALUES ('run_d3_v1', ?, 'output', false)",
            [str(art_id)],
        )
    finally:
        con.close()

    # Step 2: Run quick migration (no cold file hashing during migration)
    mig_res = migrate_v1_to_v2(db_path, quick=True)
    assert mig_res.verification.ok is True

    # Step 3: Verify Kind.HISTORICAL in StoreLedger before promotion
    with StoreLedger(db_path) as ledger:
        hist_art = ledger.load_artifact(art_id)
        assert hist_art is not None
        assert hist_art.identity.anchor.kind is Kind.HISTORICAL
        assert hist_art.identity.anchor.scheme == "legacy-v1:unverified"
        assert hist_art.identity.verifiable is False
        assert hist_art.recorded_anchor is None

        # Admission check on unverified historical artifact without expected_bytes_path is 'unverified'
        unverified_rep = check_artifact_identity(
            execution_path=parquet_path,
            expected_run_id="run_d3_v1",
            artifact_key="households",
            expected_direction="output",
            source_tracker_db=db_path,
        )
        assert unverified_rep.outcome == "unverified"

        # With a distinct immutable expected_bytes_path matching the historical hash, check corroboration
        # (fast_hash is not sha256:file of raw bytes, so expected_bytes_not_correlated is reported)
        distinct_copy = tmp_path / "households_copy.parquet"
        distinct_copy.write_bytes(parquet_path.read_bytes())
        corrob_rep = check_artifact_identity(
            execution_path=parquet_path,
            expected_run_id="run_d3_v1",
            artifact_key="households",
            expected_direction="output",
            source_tracker_db=db_path,
            expected_bytes_path=distinct_copy,
        )
        assert corrob_rep.outcome == "unverified"
        assert "expected_bytes_not_correlated" in corrob_rep.observations

    # Step 4: Promote historical artifact via promote_historical_artifact
    _hist_anchor, verified_anchor, promo_basis = promote_historical_artifact(
        db_path,
        str(art_id),
        path=parquet_path,
    )
    expected_full_sha = f"sha256:file:{file_sha256(parquet_path)}"
    assert str(verified_anchor) == expected_full_sha
    assert promo_basis == "v1_fast_recomputed"

    with StoreLedger(db_path) as ledger:
        promoted_art = ledger.load_artifact(art_id)
        assert promoted_art is not None
        assert promoted_art.identity.verifiable is True
        assert str(promoted_art.identity.anchor) == expected_full_sha
        assert promoted_art.recorded_anchor is not None
        assert promoted_art.recorded_anchor.kind is Kind.HISTORICAL
        assert promoted_art.path == parquet_path.resolve()
        pd.testing.assert_frame_equal(promoted_art.as_df(), df_hh)

        # Admission check against the v2 StoreLedger now succeeds with 'verified'!
        verified_rep = check_artifact_identity(
            execution_path=parquet_path,
            expected_run_id="run_d3_v1",
            artifact_key="households",
            expected_direction="output",
            source_tracker_db=db_path,
        )
        assert verified_rep.outcome == "verified"
        assert verified_rep.expected_artifact_id == expected_full_sha

    # ViewFactory over the promoted artifact in v2 DB works seamlessly
    tracker_v2 = Tracker(
        run_dir=tmp_path / "runs_d3",
        db_path=db_path,
        auto_migrate=True,
    )
    try:
        vf = ViewFactory(tracker_v2)
        vf.create_hybrid_view("v_households", "households")
        assert tracker_v2.db is not None
        with tracker_v2.db.ledger.exec_cursor() as cur:
            rows = cur.execute(
                "SELECT hh_id, income FROM v_households ORDER BY hh_id"
            ).fetchall()
        assert [(int(r[0]), int(r[1])) for r in rows] == [
            (10, 55000),
            (20, 72000),
            (30, 91000),
        ]
    finally:
        if tracker_v2.db is not None:
            tracker_v2.db.dispose()


def test_decision_d4_manifest_export_db_loss_and_full_reconstruction(
    tmp_path: Path,
) -> None:
    """
    Decision D4 End-to-End:
    1. Record runs, input/output bindings, secondary claims (`ctd1:sha256`), and a directory
       manifest (`sha256:manifest-v1`) in a v2 `StoreLedger`.
    2. Export `consist.json` snapshots via `export_run_manifest`.
    3. Delete the `.duckdb` file completely.
    4. Reconstruct a fresh v2 `StoreLedger` via `rebuild_store_from_manifests` from the
       `consist.json` snapshots + surviving files on disk.
    5. Verify runs, config, bindings, anchors, claims, directory manifests, and `.path` / `.as_df()`
       are completely restored.
    6. Also test importing a legacy v1 `consist.json` snapshot.
    """
    workspace_dir = tmp_path / "workspace"
    snapshots_dir = tmp_path / "snapshots"
    workspace_dir.mkdir()
    snapshots_dir.mkdir()

    mounts = {"workspace": Mount(name="workspace", root=workspace_dir, scope="site")}
    site_ctx = SiteContext(site="perlmutter", node_name="login01")

    in_csv = workspace_dir / "zones.csv"
    pd.DataFrame({"zone_id": [1, 2], "pop": [100, 250]}).to_csv(in_csv, index=False)

    out_parquet = workspace_dir / "trips.parquet"
    df_trips = pd.DataFrame({"trip_id": [1, 2, 3], "dist": [2.5, 5.0, 1.2]})
    df_trips.to_parquet(out_parquet, index=False)

    out_dir = workspace_dir / "skims.zarr"
    out_dir.mkdir()
    (out_dir / ".zgroup").write_text('{"zarr_format":2}\n', encoding="utf-8")
    (out_dir / "part0.bin").write_bytes(b"\x00\x01\x02\x03")

    orig_db_path = tmp_path / "original.duckdb"
    now_dt = datetime.now(timezone.utc)

    with StoreLedger(orig_db_path, mounts=mounts) as ledger:
        ledger.connection.execute(
            "INSERT INTO run ("
            "id, model_name, description, year, iteration, parent_run_id, status, "
            "code_hash, config_hash, input_hash, signature, signature_scheme, "
            "cache_status, tags, meta, started_at, ended_at, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "run_2025_iter1",
                "activitysim",
                "2025 full run",
                2025,
                1,
                None,
                "completed",
                "code_abc",
                "cfg_def",
                "in_ghi",
                "sig_xyz",
                "sig-v2",
                "live",
                json.dumps(["campaign", "baseline"]),
                json.dumps({"region": "bay_area", "config": {"sample_rate": 0.25}}),
                now_dt,
                now_dt,
                now_dt,
            ],
        )

        # Observe and bind input
        in_art = observe(
            in_csv,
            key="zones",
            driver="csv",
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
        )
        ledger.record_run_binding(
            run_id="run_2025_iter1",
            direction="input",
            occurrence_index=0,
            slot="zones",
            artifact_id=in_art.id,
            anchor=in_art.anchor,
            role="zones",
            contract_scheme="sha256:file",
        )

        # Seal outputs (file + directory)
        sealed = seal(
            [
                PendingOutput(
                    role="trips",
                    path=out_parquet,
                    driver="parquet",
                    key="trips",
                    meta={"units": "miles"},
                ),
                PendingOutput(
                    role="skims",
                    path=out_dir,
                    driver="zarr",
                    is_dir=True,
                    key="skims",
                ),
            ],
            ledger=ledger,
            run_id="run_2025_iter1",
            site_context=site_ctx,
            mounts=mounts,
        )
        trips_art = sealed["trips"]
        skims_art = sealed["skims"]

        # Attach secondary ctd1:sha256 claim to trips_art
        trips_art_with_ctd, ctd_digest = ensure_digest(
            trips_art,
            "ctd1:sha256",
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
        )

        # Export manifest to snapshots_dir / "consist.json"
        snap_file = snapshots_dir / "consist.json"
        exported = export_run_manifest(ledger, "run_2025_iter1", path=snap_file)
        assert snap_file.is_file()
        assert exported.schema_version == 2
        assert exported.run["id"] == "run_2025_iter1"
        assert exported.config == {"sample_rate": 0.25}
        assert len(exported.inputs) == 1
        assert len(exported.outputs) == 2

    # Step 3: Delete the original DuckDB file completely!
    orig_db_path.unlink()
    assert not orig_db_path.exists()

    # Step 4: Rebuild a brand-new StoreLedger from snapshots_dir + surviving files
    rebuilt_db_path = tmp_path / "rebuilt.duckdb"
    rebuilt_manifests = rebuild_store_from_manifests(
        rebuilt_db_path,
        [snapshots_dir],
        mounts=mounts,
        reobserve_roots=[workspace_dir],
    )
    assert len(rebuilt_manifests) == 1
    assert rebuilt_manifests[0].run["id"] == "run_2025_iter1"

    # Step 5: Verify restored state in the rebuilt StoreLedger
    with StoreLedger(rebuilt_db_path, mounts=mounts) as rebuilt_ledger:
        reloaded_trips = rebuilt_ledger.load_artifact(trips_art.id)
        reloaded_skims = rebuilt_ledger.load_artifact(skims_art.id)
        reloaded_zones = rebuilt_ledger.load_artifact(in_art.id)

        assert reloaded_trips is not None
        assert reloaded_skims is not None
        assert reloaded_zones is not None

        assert reloaded_trips.anchor == trips_art.anchor
        assert reloaded_trips.identity.get("ctd1:sha256") == ctd_digest
        assert dict(reloaded_trips.meta) == {"units": "miles"}
        assert reloaded_trips.path == out_parquet.resolve()
        pd.testing.assert_frame_equal(reloaded_trips.as_df(), df_trips)

        assert reloaded_skims.anchor == skims_art.anchor
        assert reloaded_skims.path == out_dir.resolve()
        manifest_row = rebuilt_ledger.connection.execute(
            "SELECT format, entry_count FROM content_manifest WHERE anchor = ?",
            [str(reloaded_skims.anchor)],
        ).fetchone()
        assert manifest_row == ("manifest-v1", 2)

        # Re-exporting from the rebuilt ledger produces identical RunManifest run/config/anchors
        re_exported = export_run_manifest(rebuilt_ledger, "run_2025_iter1")
        assert re_exported.run["id"] == "run_2025_iter1"
        assert re_exported.config == {"sample_rate": 0.25}

        # Step 6: Legacy v1 consist.json import strips system meta keys and derives anchors
        v1_snapshot = {
            "run": {
                "id": "legacy_v1_run",
                "model_name": "beam",
                "status": "completed",
                "git_hash": "commit_v1",
                "config_hash": "cfg_v1",
                "input_hash": "in_v1",
                "signature": "sig_v1",
                "tags": ["v1"],
                "meta": {"note": "from_v1"},
            },
            "config": {"beam_iter": 3},
            "inputs": [
                {
                    "id": str(uuid.uuid4()),
                    "key": "zones_v1",
                    "driver": "csv",
                    "container_uri": "workspace://zones.csv",
                    "hash": file_sha256(in_csv),
                    "meta": {
                        "hash_semantics": "full_sha256",
                        "is_ingested": True,
                        "recovery_roots": [str(workspace_dir)],
                        "user_tag": "clean_input",
                    },
                }
            ],
            "outputs": [
                {
                    "id": str(uuid.uuid4()),
                    "key": "unverified_out",
                    "driver": "parquet",
                    "container_uri": "workspace://missing.parquet",
                    "hash": "a" * 64,
                    "meta": {"hash_semantics": "fast_stat", "custom_k": 42},
                }
            ],
        }
        v1_loaded = load_run_manifest(json.dumps(v1_snapshot))
        assert v1_loaded.schema_version == 2
        assert v1_loaded.run["code_hash"] == "commit_v1"
        assert v1_loaded.inputs[0].anchor == f"sha256:file:{file_sha256(in_csv)}"
        assert dict(v1_loaded.inputs[0].meta) == {"user_tag": "clean_input"}
        assert v1_loaded.outputs[0].anchor == f"legacy-v1:unverified:{'a' * 64}"
        assert dict(v1_loaded.outputs[0].meta) == {"custom_k": 42}

        import_run_manifest(
            rebuilt_ledger,
            v1_loaded,
            reobserve_roots=[workspace_dir],
            mounts=mounts,
        )
        v1_in_art = rebuilt_ledger.load_artifact(v1_loaded.inputs[0].artifact_id)
        assert v1_in_art is not None
        assert v1_in_art.path == in_csv.resolve()


def test_unified_hydrate_admitted_input_and_archive_evict(
    tmp_path: Path,
) -> None:
    """
    Test `hydrate_admitted_input` across:
    - Regular files (staging + DeclaredDigestExpectation + PriorRunExpectation)
    - Directory manifests (`sha256:manifest-v1`)
    - Cross-format logical matrix containers (`.omx` vs `.zarr` under container vs logical contracts)
    - `archive` and `evict` engine helpers
    """
    db_path = tmp_path / "admission_v2.duckdb"
    work_dir = tmp_path / "work"
    stage_dir = tmp_path / "staged"
    arch_dir = tmp_path / "archive"
    work_dir.mkdir()
    stage_dir.mkdir()
    arch_dir.mkdir()

    mounts = {
        "workspace": Mount(name="workspace", root=work_dir, scope="site"),
        "archive": Mount(name="archive", root=arch_dir, scope="site"),
    }

    # 1. Create an OMX matrix file and an equivalent Zarr matrix directory with identical logical matrices
    omx_path = work_dir / "skims.omx"
    zarr_path = work_dir / "skims.zarr"
    mat_am = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    mat_pm = np.array([[1.5, 2.5], [3.5, 4.5]], dtype=np.float32)

    with h5py.File(omx_path, "w") as h5f:
        data_grp = h5f.create_group("data")
        data_grp.create_dataset("AM", data=mat_am)
        data_grp.create_dataset("PM", data=mat_pm)

    z_root = zarr.open_group(str(zarr_path), mode="w")
    ds_am = z_root.create_dataset("AM", data=mat_am)
    ds_am.attrs["_ARRAY_DIMENSIONS"] = ["orig", "dest"]
    ds_pm = z_root.create_dataset("PM", data=mat_pm)
    ds_pm.attrs["_ARRAY_DIMENSIONS"] = ["orig", "dest"]

    logical_hex = compute_logical_matrices_sha256({"AM": mat_am, "PM": mat_pm})
    logical_digest = Digest("sha256:logical_matrices:v1", logical_hex)

    with StoreLedger(db_path, mounts=mounts) as ledger:
        now_dt = datetime.now(timezone.utc)
        ledger.record_run(
            run_id="prior_omx_run",
            model_name="skimmer",
            status="completed",
            cache_status="live",
            signature="sig1",
            created_at=now_dt,
        )
        sealed = seal(
            [PendingOutput(role="skims", path=omx_path, driver="omx", key="skims")],
            ledger=ledger,
            run_id="prior_omx_run",
            mounts=mounts,
        )
        omx_art = sealed["skims"]
        omx_art, _ = ensure_digest(
            omx_art,
            "sha256:logical_matrices:v1",
            ledger=ledger,
            mounts=mounts,
        )
        assert omx_art.identity.get("sha256:logical_matrices:v1") == logical_digest

        # PriorRunExpectation.from_artifact extracts the v2 logical_matrices claim automatically
        prior_exp = PriorRunExpectation.from_artifact(
            omx_art,
            source_tracker_db=db_path,
            expected_direction="output",
        )
        assert (
            prior_exp.expected_logical_hash == f"sha256:logical_matrices:{logical_hex}"
        )

        # Hydrating and staging the OMX file to stage_dir / "staged_skims.omx"
        staged_omx_dest = stage_dir / "staged_skims.omx"
        hydrated_omx = hydrate_admitted_input(
            omx_art,
            input_role="skims",
            destination=staged_omx_dest,
            expectation=prior_exp,
            ledger=ledger,
            mounts=mounts,
        )
        assert isinstance(hydrated_omx, HydratedAdmittedInput)
        assert hydrated_omx.path == staged_omx_dest.resolve()
        assert hydrated_omx.path.is_file()
        assert hydrated_omx.report.outcome == "verified"
        assert hydrated_omx.verdict.agreement is Agreement.AGREE

        # Hydrating the Zarr directory against the OMX prior run with auto/logical equivalence succeeds!
        hydrated_zarr_logical = hydrate_admitted_input(
            zarr_path,
            input_role="skims",
            expectation=prior_exp,
            ledger=ledger,
            mounts=mounts,
        )
        assert hydrated_zarr_logical.report.outcome == "verified"
        assert hydrated_zarr_logical.verdict.agreement is Agreement.AGREE
        assert (
            hydrated_zarr_logical.verdict.matched_scheme == "sha256:logical_matrices:v1"
        )
        assert (
            hydrated_zarr_logical.artifact.identity.get("sha256:logical_matrices:v1")
            == logical_digest
        )

        # Hydrating the Zarr directory as a directory manifest (sha256:manifest-v1)
        zarr_art = observe(zarr_path, key="skims_dir", ledger=ledger, mounts=mounts)
        staged_zarr_dest = stage_dir / "staged_skims.zarr"
        hydrated_dir = hydrate_admitted_input(
            zarr_art,
            input_role="skims_dir",
            destination=staged_zarr_dest,
            expectation=str(zarr_art.anchor),
            ledger=ledger,
            mounts=mounts,
        )
        assert hydrated_dir.path == staged_zarr_dest.resolve()
        assert hydrated_dir.report.outcome == "verified"
        assert hydrated_dir.verdict.agreement is Agreement.AGREE
        assert hydrated_dir.verdict.matched_scheme == "sha256:manifest-v1"

        # Mismatched DeclaredDigestExpectation returns outcome="mismatched" and Agreement.DISAGREE
        bad_exp = DeclaredDigestExpectation(
            identity=AdmissionIdentity.parse(f"sha256:file:{'f' * 64}")
        )
        hydrated_mismatch = hydrate_admitted_input(
            omx_art,
            input_role="skims",
            expectation=bad_exp,
            ledger=ledger,
            mounts=mounts,
        )
        assert hydrated_mismatch.report.outcome == "mismatched"
        assert hydrated_mismatch.verdict.agreement is Agreement.DISAGREE

        # Test archive(..., mode="move") and evict(..., require_surviving_copy=True)
        txt_file = work_dir / "notes.txt"
        txt_file.write_text("important provenance note\n", encoding="utf-8")
        txt_art = observe(txt_file, key="notes", ledger=ledger, mounts=mounts)

        # Cannot evict the only copy when require_surviving_copy=True
        with pytest.raises(ConsistError, match="Refusing to evict last active copy"):
            evict(txt_art, path=txt_file, ledger=ledger, mounts=mounts)

        # Archive with mode="move" copies to arch_dir and evicts the work_dir copy
        archived_txt_art = archive(
            txt_art,
            arch_dir / "notes.txt",
            mode="move",
            ledger=ledger,
            mounts=mounts,
        )
        assert not txt_file.exists()
        assert (arch_dir / "notes.txt").exists()
        assert (
            locate(archived_txt_art, ledger=ledger, mounts=mounts).path
            == (arch_dir / "notes.txt").resolve()
        )


def test_pr6_reviewer_regressions(tmp_path: Path) -> None:
    """
    Regression tests for PR 6 reviewer findings (A.1 - A.6):
    1. Promoted anchor (`recorded_anchor != anchor`) and `depends_on` direction round-trip through `export_run_manifest` -> `import_run_manifest`.
    2. Legacy v1 `consist.json` dict `hash_semantics` (`raw_file_bytes` / `computed_full`), `admission_file_identity`, `logical_identity`, and `cache_status` normalization (`live` / `historical`).
    3. `hydrate_admitted_input`:
       - Missing path returns `report.outcome == "unreadable"` and `Agreement.UNKNOWN` without raising `FileNotFoundError`.
       - `ctd1:sha256` expectation verifies without passing `contract` explicitly.
       - `logical_profile="float32_matrices_v1"` threads consistently into both `report` and `verdict` (e.g. float64 vs float32 OMX/Zarr comparison).
    4. `evict` refuses when the surviving copy's file was deleted from disk (`Freshness.MISSING`), and does not delete an unrelated file at `path` when `matching_rows` is empty.
    """
    work_dir = tmp_path / "work"
    arch_dir = tmp_path / "archive"
    work_dir.mkdir()
    arch_dir.mkdir()
    mounts = {
        "workspace": Mount(name="workspace", root=work_dir, scope="site"),
        "archive": Mount(name="archive", root=arch_dir, scope="site"),
    }

    # --- 1. Promoted anchor + depends_on round-trip ---
    csv_file = work_dir / "promoted.csv"
    df = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
    df.to_csv(csv_file, index=False)
    verified_hex = file_sha256(csv_file)
    verified_anchor = Digest("sha256:file", verified_hex)
    hist_anchor = Digest("legacy-v1:unverified", "b" * 64)

    dep_file = work_dir / "dep.txt"
    dep_file.write_text("dep", encoding="utf-8")

    db1 = tmp_path / "db1.duckdb"
    snap_path = tmp_path / "promoted_consist.json"
    with StoreLedger(db1, mounts=mounts) as ledger1:
        now_dt = datetime.now(timezone.utc)
        ledger1.record_run(
            run_id="promoted_run",
            model_name="test_model",
            status="completed",
            cache_status="live",
            signature="sig_p",
            created_at=now_dt,
        )
        ledger1.record_identity(Identity(anchor=hist_anchor), repr_kind="historical")
        ledger1.record_identity(Identity(anchor=verified_anchor), repr_kind="file")
        ledger1.record_anchor_promotion(
            hist_anchor,
            verified_anchor,
            basis="caller_supplied_match",
            asserted_by="unit_test",
        )
        promo_art_id = uuid.uuid4()
        ledger1.record_artifact(
            artifact_id=promo_art_id,
            key="promoted_out",
            driver="csv",
            anchor=hist_anchor,
            run_id="promoted_run",
        )
        ledger1.record_run_binding(
            run_id="promoted_run",
            direction="output",
            occurrence_index=0,
            slot="promoted_out",
            artifact_id=promo_art_id,
            anchor=hist_anchor,
            role="promoted_out",
        )
        dep_art = observe(dep_file, key="dep_in", ledger=ledger1, mounts=mounts)
        ledger1.record_run_binding(
            run_id="promoted_run",
            direction="depends_on",
            occurrence_index=0,
            slot="dep_in",
            artifact_id=dep_art.id,
            anchor=dep_art.anchor,
            role="dep_in",
        )

        exported = export_run_manifest(ledger1, "promoted_run", path=snap_path)
        assert exported.run["cache_status"] == "live"
        assert len(exported.outputs) == 1
        assert exported.outputs[0].anchor == str(verified_anchor)
        assert exported.outputs[0].recorded_anchor == str(hist_anchor)
        assert exported.outputs[0].promotion_basis == "caller_supplied_match"
        assert exported.outputs[0].promotion_asserted_by == "unit_test"
        assert len(exported.inputs) == 1
        assert exported.inputs[0].direction == "depends_on"

    db2 = tmp_path / "db2.duckdb"
    with StoreLedger(db2, mounts=mounts) as ledger2:
        imported = import_run_manifest(ledger2, snap_path)
        assert imported.run["cache_status"] == "live"
        reloaded_promo = ledger2.load_artifact(promo_art_id)
        assert reloaded_promo is not None
        assert reloaded_promo.anchor == verified_anchor
        assert reloaded_promo.recorded_anchor == hist_anchor
        # Verify depends_on direction was preserved
        dir_row = ledger2.connection.execute(
            "SELECT direction FROM run_binding WHERE run_id = 'promoted_run' AND slot = 'dep_in'"
        ).fetchone()
        assert dir_row == ("depends_on",)

        # Idempotent re-import with fewer bindings removes stale bindings
        trimmed_dict = imported.to_dict()
        trimmed_dict["inputs"] = []
        import_run_manifest(ledger2, trimmed_dict)
        rem_inputs = ledger2.connection.execute(
            "SELECT COUNT(*) FROM run_binding WHERE run_id = 'promoted_run' AND direction != 'output'"
        ).fetchone()
        assert rem_inputs == (0,)

    # --- 2. Legacy v1 consist.json dict hash_semantics & logical_identity ---
    v1_dict_sem = {
        "run": {
            "id": "v1_dict_sem_run",
            "model_name": "skimmer",
            "status": "completed",
            "cache_status": "hit",
        },
        "config": {},
        "inputs": [
            {
                "id": str(uuid.uuid4()),
                "key": "in_csv",
                "driver": "csv",
                "container_uri": "workspace://promoted.csv",
                "hash": verified_hex,
                "meta": {
                    "hash_semantics": {
                        "digest_contract": "raw_file_bytes",
                        "source": "computed_full",
                        "kind": "bytes",
                    },
                    "logical_identity": f"sha256:logical_matrices:{'c' * 64}",
                },
            }
        ],
        "outputs": [],
    }
    loaded_v1_sem = load_run_manifest(v1_dict_sem)
    assert loaded_v1_sem.run["cache_status"] == "live"
    assert loaded_v1_sem.inputs[0].anchor == f"sha256:file:{verified_hex}"
    assert f"sha256:logical_matrices:v1:{'c' * 64}" in loaded_v1_sem.inputs[0].claims

    # --- 3. hydrate_admitted_input edge cases (missing path, ctd1, float32_matrices_v1) ---
    with StoreLedger(db2, mounts=mounts) as ledger2:
        missing_hydrated = hydrate_admitted_input(
            work_dir / "does_not_exist.csv",
            input_role="missing_input",
            expectation=f"sha256:file:{verified_hex}",
            ledger=ledger2,
            mounts=mounts,
        )
        assert missing_hydrated.report.outcome == "unreadable"
        assert missing_hydrated.verdict.agreement is Agreement.UNKNOWN

        # ctd1:sha256 expectation without explicit contract=Contract(...)
        csv_art = observe(
            csv_file, key="csv_in", driver="csv", ledger=ledger2, mounts=mounts
        )
        csv_art, ctd_d = ensure_digest(
            csv_art, "ctd1:sha256", ledger=ledger2, mounts=mounts
        )
        hydrated_ctd = hydrate_admitted_input(
            csv_file,
            input_role="csv_in",
            expectation=ctd_d,
            ledger=ledger2,
            mounts=mounts,
        )
        assert hydrated_ctd.verdict.agreement is Agreement.AGREE
        assert hydrated_ctd.verdict.matched_scheme == "ctd1:sha256"
        assert hydrated_ctd.report.outcome == "verified"

        # float32_matrices_v1 profile consistency between float64 OMX and float32 Zarr
        omx_f64 = work_dir / "mat_f64.omx"
        zarr_f32 = work_dir / "mat_f32.zarr"
        arr_f64 = np.array([[1.25, 2.5], [3.75, 5.0]], dtype=np.float64)
        arr_f32 = arr_f64.astype(np.float32)
        with h5py.File(omx_f64, "w") as h5f:
            grp = h5f.create_group("data")
            grp.create_dataset("M1", data=arr_f64)
        zr = zarr.open_group(str(zarr_f32), mode="w")
        ds = zr.create_dataset("M1", data=arr_f32)
        ds.attrs["_ARRAY_DIMENSIONS"] = ["r", "c"]

        f32_hex = compute_logical_matrices_sha256(
            {"M1": arr_f32},
            policy="float32_matrices_v1",
        )
        decl_f32 = DeclaredDigestExpectation(
            identity=f"sha256:logical_matrices:{f32_hex}",
            logical_profile="float32_matrices_v1",
        )
        hydrated_f64 = hydrate_admitted_input(
            omx_f64,
            input_role="mat",
            expectation=decl_f32,
            ledger=ledger2,
            mounts=mounts,
        )
        assert hydrated_f64.report.outcome == "verified"
        assert hydrated_f64.verdict.agreement is Agreement.AGREE
        assert (
            hydrated_f64.report.observed_artifact_id
            == f"sha256:logical_matrices:{f32_hex}"
        )
        assert hydrated_f64.artifact.identity.get("sha256:logical_matrices:v1") is None
        assert (
            ledger2.get_identity(hydrated_f64.artifact.identity.anchor).get(
                "sha256:logical_matrices:v1"
            )
            is None
        )

        # --- 4. evict safety when surviving copy was deleted or target_path is unrelated ---
        ev_src = work_dir / "evict_me.txt"
        ev_src.write_text("evict test content", encoding="utf-8")
        ev_art = observe(ev_src, key="ev_test", ledger=ledger2, mounts=mounts)
        ev_arch_dest = arch_dir / "evict_me.txt"
        ev_art = archive(
            ev_art,
            ev_arch_dest,
            mode="copy",
            ledger=ledger2,
            mounts=mounts,
        )
        # Unlink the archived copy behind Consist's back; evict(ev_src) must refuse!
        ev_arch_dest.unlink()
        with pytest.raises(ConsistError, match="Refusing to evict last active copy"):
            evict(ev_art, path=ev_src, ledger=ledger2, mounts=mounts)
        assert ev_src.exists()

        # Unrelated file passed as path when matching_rows is empty must NOT be deleted
        unrelated_file = work_dir / "unrelated_keep.txt"
        unrelated_file.write_text("do not delete me", encoding="utf-8")
        evict(
            ev_art,
            path=unrelated_file,
            require_surviving_copy=False,
            ledger=ledger2,
            mounts=mounts,
        )
        assert unrelated_file.exists()


def test_phase1_store_manifest_hardening_dedup_and_lazy_imports(
    tmp_path: Path,
) -> None:
    import base64
    import subprocess
    import sys
    import consist
    import consist.store as consist_store
    import consist.store.manifest as store_manifest

    # 1. Top-level consist manifest exports removed; consist.store & consist.store.manifest lazy without duckdb
    for removed_name in (
        "manifest",
        "RunManifest",
        "export_run_manifest",
        "import_run_manifest",
        "rebuild_store_from_manifests",
    ):
        assert removed_name not in consist.__all__
        assert removed_name not in consist.__dict__

    expected_manifest_symbols = (
        "SCHEMA_VERSION",
        "ManifestLocationEntry",
        "ManifestArtifactEntry",
        "RunManifest",
        "write_run_manifest",
        "load_run_manifest",
        "export_run_manifest",
        "import_run_manifest",
        "rebuild_store_from_manifests",
    )
    for sym in expected_manifest_symbols:
        assert sym in store_manifest.__all__
        assert sym in consist_store.__all__

    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, consist, consist.store, consist.store.manifest; "
            "assert 'duckdb' not in sys.modules",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    mounts = {"workspace": Mount(name="workspace", root=work_dir, scope="site")}
    db_path = tmp_path / "manifest_hardening.duckdb"

    # 2. Tampered or invalid-scheme / invalid-JSON manifest_body_b64 raises ValueError and rolls back cleanly
    import hashlib

    dir_anchor = f"sha256:manifest-v1:{'a' * 64}"
    bad_body_b64 = base64.b64encode(b'{"entries":[]}').decode("ascii")
    tampered_manifest = load_run_manifest(
        {
            "schema_version": 2,
            "run": {"id": "tampered_run", "model_name": "m", "status": "completed"},
            "config": {},
            "inputs": [],
            "outputs": [
                {
                    "slot": "bad_dir",
                    "artifact_id": str(uuid.uuid4()),
                    "key": "bad_dir",
                    "driver": "zarr",
                    "anchor": dir_anchor,
                    "manifest_body_b64": bad_body_b64,
                }
            ],
        }
    )
    valid_body_bytes = b'{"entries":[]}'
    valid_body_hex = hashlib.sha256(valid_body_bytes).hexdigest()
    valid_body_b64 = base64.b64encode(valid_body_bytes).decode("ascii")

    no_entries_bytes = b'{"not_entries":true}'
    no_entries_hex = hashlib.sha256(no_entries_bytes).hexdigest()
    no_entries_b64 = base64.b64encode(no_entries_bytes).decode("ascii")

    with StoreLedger(db_path, mounts=mounts) as ledger:
        with pytest.raises(ValueError, match="SHA-256"):
            import_run_manifest(ledger, tampered_manifest, mounts=mounts)
        assert ledger.get_run("tampered_run") is None

        # Non-manifest schemes MUST reject manifest_body_b64 even when sha256(body_bytes) matches!
        for bad_scheme in ("sha256:file", "ctd1:sha256"):
            bad_scheme_manifest = load_run_manifest(
                {
                    "schema_version": 2,
                    "run": {
                        "id": "bad_scheme_run",
                        "model_name": "m",
                        "status": "completed",
                    },
                    "config": {},
                    "inputs": [],
                    "outputs": [
                        {
                            "slot": "bad_scheme",
                            "artifact_id": str(uuid.uuid4()),
                            "key": "bad_scheme",
                            "driver": "zarr",
                            "anchor": f"{bad_scheme}:{valid_body_hex}",
                            "manifest_body_b64": valid_body_b64,
                        }
                    ],
                }
            )
            with pytest.raises(ValueError, match="manifest-v1"):
                import_run_manifest(ledger, bad_scheme_manifest, mounts=mounts)
            assert ledger.get_run("bad_scheme_run") is None

        # Malformed manifest JSON (missing list 'entries') MUST raise ValueError before opening transaction
        bad_shape_manifest = load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "bad_shape_run",
                    "model_name": "m",
                    "status": "completed",
                },
                "config": {},
                "inputs": [],
                "outputs": [
                    {
                        "slot": "bad_shape",
                        "artifact_id": str(uuid.uuid4()),
                        "key": "bad_shape",
                        "driver": "zarr",
                        "anchor": f"sha256:manifest-v1:{no_entries_hex}",
                        "manifest_body_b64": no_entries_b64,
                    }
                ],
            }
        )
        with pytest.raises(ValueError, match="entries"):
            import_run_manifest(ledger, bad_shape_manifest, mounts=mounts)
        assert ledger.get_run("bad_shape_run") is None

        # Valid foreign manifest_body_b64 keeps anchor basis='trusted_foreign' when not verified on local disk
        foreign_dir_anchor = f"sha256:manifest-v1:{valid_body_hex}"
        foreign_dir_manifest = load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "foreign_dir_run",
                    "model_name": "m",
                    "status": "completed",
                },
                "config": {},
                "inputs": [],
                "outputs": [
                    {
                        "slot": "dir_out",
                        "artifact_id": str(uuid.uuid4()),
                        "key": "dir_out",
                        "driver": "zarr",
                        "anchor": foreign_dir_anchor,
                        "manifest_body_b64": valid_body_b64,
                    }
                ],
            }
        )
        import_run_manifest(ledger, foreign_dir_manifest, mounts=mounts)
        with ledger.cursor() as cur:
            dir_basis_row = cur.execute(
                "SELECT basis FROM main.content_digest WHERE anchor = ? AND scheme = 'sha256:manifest-v1'",
                [foreign_dir_anchor],
            ).fetchone()
        assert dir_basis_row == ("trusted_foreign",)

        # Consumer-before-producer import preserves producer artifact.run_id
        shared_art_id = uuid.uuid4()
        shared_anchor = f"sha256:file:{'4' * 64}"
        consumer_manifest = load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "consumer_first",
                    "model_name": "downstream",
                    "status": "completed",
                },
                "config": {},
                "inputs": [
                    {
                        "slot": "in_shared",
                        "artifact_id": str(shared_art_id),
                        "key": "shared_tbl",
                        "driver": "parquet",
                        "anchor": shared_anchor,
                        "run_id": None,
                    }
                ],
                "outputs": [],
            }
        )
        producer_manifest = load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "producer_second",
                    "model_name": "upstream",
                    "status": "completed",
                },
                "config": {},
                "inputs": [],
                "outputs": [
                    {
                        "slot": "out_shared",
                        "artifact_id": str(shared_art_id),
                        "key": "shared_tbl",
                        "driver": "parquet",
                        "anchor": shared_anchor,
                        "run_id": "producer_second",
                    }
                ],
            }
        )
        consumer_after_manifest = load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "consumer_third",
                    "model_name": "downstream2",
                    "status": "completed",
                },
                "config": {},
                "inputs": [
                    {
                        "slot": "in_shared",
                        "artifact_id": str(shared_art_id),
                        "key": "shared_tbl",
                        "driver": "parquet",
                        "anchor": shared_anchor,
                        "run_id": None,
                    }
                ],
                "outputs": [],
            }
        )
        import_run_manifest(ledger, consumer_manifest, mounts=mounts)
        import_run_manifest(ledger, producer_manifest, mounts=mounts)
        loaded_shared = ledger.load_artifact(shared_art_id)
        assert loaded_shared is not None and loaded_shared.run_id == "producer_second"
        import_run_manifest(ledger, consumer_after_manifest, mounts=mounts)
        loaded_shared_after = ledger.load_artifact(shared_art_id)
        assert (
            loaded_shared_after is not None
            and loaded_shared_after.run_id == "producer_second"
        )

        # 3. URI / key '..' path traversal raises ValueError atomically
        for bad_uri, bad_key in (
            ("workspace://../../etc/passwd", "ok_key"),
            ("file:///tmp/../etc/passwd", "ok_key"),
            ("../escape.csv", "ok_key"),
            ("workspace://ok.csv", "../escape_key"),
        ):
            traversal_manifest = load_run_manifest(
                {
                    "schema_version": 2,
                    "run": {"id": "trav_run", "model_name": "m", "status": "completed"},
                    "config": {},
                    "inputs": [],
                    "outputs": [
                        {
                            "slot": "out",
                            "artifact_id": str(uuid.uuid4()),
                            "key": bad_key,
                            "driver": "csv",
                            "anchor": f"sha256:file:{'b' * 64}",
                            "locations": [{"uri": bad_uri}],
                        }
                    ],
                }
            )
            with pytest.raises(ValueError, match=r"\.\.|escapes"):
                import_run_manifest(
                    ledger,
                    traversal_manifest,
                    mounts=mounts,
                    reobserve_roots=[work_dir],
                )
            assert ledger.get_run("trav_run") is None

        # 4. Foreign secondary claims use basis='trusted_foreign' and facet_kv is populated
        art_uuid = uuid.uuid4()
        file_anchor = Digest("sha256:file", "1" * 64)
        foreign_ctd = Digest("ctd1:sha256", "2" * 64)
        computed_ctd = Digest("ctd1:sha256", "3" * 64)
        foreign_manifest = load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "foreign_run",
                    "model_name": "activitysim",
                    "status": "completed",
                    "meta": {"facet": {"sample_rate": 0.1}},
                },
                "config": {"sample_rate": 0.1},
                "inputs": [],
                "outputs": [
                    {
                        "slot": "trips",
                        "artifact_id": str(art_uuid),
                        "key": "trips",
                        "driver": "parquet",
                        "anchor": str(file_anchor),
                        "claims": [str(foreign_ctd)],
                        "meta": {"facet": {"row_count": 500}},
                    }
                ],
            }
        )
        import_run_manifest(ledger, foreign_manifest, mounts=mounts)
        with ledger.cursor() as cur:
            basis_row = cur.execute(
                "SELECT basis, value FROM main.content_digest WHERE anchor = ? AND scheme = 'ctd1:sha256'",
                [str(file_anchor)],
            ).fetchone()
        assert basis_row == ("trusted_foreign", "2" * 64)

        # Local computed digest supersedes trusted_foreign and logs conflict
        ledger.record_content_digest(
            file_anchor,
            computed_ctd,
            basis="computed",
            source="local_reverify",
        )
        assert ledger.get_identity(file_anchor).get("ctd1:sha256") == computed_ctd
        assert (
            len(
                ledger.query_df(
                    "SELECT * FROM run_config_kv WHERE run_id = 'foreign_run' AND key = 'sample_rate'"
                )
            )
            == 1
        )
        assert (
            len(
                ledger.query_df(
                    "SELECT * FROM artifact_kv WHERE artifact_id = ? AND key_path = 'row_count'",
                    [art_uuid],
                )
            )
            == 1
        )

    # 5. rebuild_store_from_manifests deduplicates duplicate run_id by status rank
    # (completed/failed [2] > running [1] > missing/None [0])
    snap_root = tmp_path / "dedup_snaps"
    dir_early = snap_root / "a_early_sort"
    dir_mid = snap_root / "m_mid_sort"
    dir_late = snap_root / "z_late_sort"
    dir_early.mkdir(parents=True)
    dir_mid.mkdir(parents=True)
    dir_late.mkdir(parents=True)

    # Alphabetically LATER paths have running and missing status with newer-looking started_at;
    # completed status MUST still win for dup_run_1, and running MUST beat missing status for dup_run_2!
    write_run_manifest(
        dir_early / "consist.json",
        load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "dup_run_1",
                    "model_name": "beam",
                    "status": "completed",
                    "started_at": "2026-11-10T10:00:00+00:00",
                    "ended_at": "2026-11-10T11:00:00+00:00",
                },
                "config": {"winner": "completed"},
                "inputs": [],
                "outputs": [],
            }
        ),
    )
    write_run_manifest(
        dir_late / "consist.json",
        load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "dup_run_1",
                    "model_name": "beam",
                    "status": "running",
                    "started_at": "2026-11-10T12:00:00+00:00",
                },
                "config": {"winner": "running"},
                "inputs": [],
                "outputs": [],
            }
        ),
    )
    (dir_mid / "consist_runs").mkdir()
    write_run_manifest(
        dir_mid / "consist_runs" / "dup2_running.json",
        load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "dup_run_2",
                    "model_name": "beam",
                    "status": "running",
                    "started_at": "2026-11-10T09:00:00+00:00",
                },
                "config": {"winner": "running"},
                "inputs": [],
                "outputs": [],
            }
        ),
    )
    (dir_late / "consist_runs").mkdir()
    write_run_manifest(
        dir_late / "consist_runs" / "dup2_missing_status.json",
        load_run_manifest(
            {
                "schema_version": 2,
                "run": {
                    "id": "dup_run_2",
                    "model_name": "beam",
                    "started_at": "2026-11-10T15:00:00+00:00",
                },
                "config": {"winner": "missing_status"},
                "inputs": [],
                "outputs": [],
            }
        ),
    )
    dedup_db = tmp_path / "dedup.duckdb"
    rebuilt = rebuild_store_from_manifests(dedup_db, [snap_root], mounts=mounts)
    assert len(rebuilt) == 2
    by_id = {m.run["id"]: m for m in rebuilt}
    assert by_id["dup_run_1"].run["status"] == "completed"
    assert by_id["dup_run_2"].run["status"] == "running"
    with StoreLedger(dedup_db, mounts=mounts) as dedup_ledger:
        winner_run_1 = dedup_ledger.get_run("dup_run_1")
        winner_run_2 = dedup_ledger.get_run("dup_run_2")
        assert winner_run_1 is not None and winner_run_1.status == "completed"
        assert winner_run_2 is not None and winner_run_2.status == "running"


def test_phase2_hydrate_admitted_input_preverifies_before_staging_and_reexports_v2_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os
    import time

    import consist.admission as top_admission
    import consist.artifact as v2_artifact_mod
    import consist.engine as engine_mod

    assert top_admission.Artifact is v2_artifact_mod.Artifact

    ws_root = tmp_path / "ws"
    arch_root = tmp_path / "archive"
    stage_dir = tmp_path / "staged"
    ws_root.mkdir()
    arch_root.mkdir()
    stage_dir.mkdir()
    mounts = {"workspace": ws_root, "archive": arch_root}

    src_file = ws_root / "zones.csv"
    good_bytes = b"zone_id,pop\n1,500\n2,900\n"
    src_file.write_bytes(good_bytes)
    safe_past_ns = time.time_ns() - 10_000_000_000
    os.utime(src_file, ns=(safe_past_ns, safe_past_ns))

    db_path = tmp_path / "phase2_admit.duckdb"
    with StoreLedger(db_path, mounts=mounts) as ledger:
        ledger.record_run(run_id="r_prod", model_name="test", status="completed")
        sealed = seal(
            [PendingOutput(role="zones", path=src_file)],
            ledger=ledger,
            run_id="r_prod",
            mounts=mounts,
            archive_root=arch_root,
        )
        art = sealed["zones"]
        arch_copy = arch_root / "zones.csv"
        os.utime(arch_copy, ns=(safe_past_ns, safe_past_ns))
        expected_good = DeclaredDigestExpectation(
            identity=f"sha256:file:{file_sha256(src_file)}"
        )
        expected_wrong = DeclaredDigestExpectation(identity="sha256:file:" + ("b" * 64))

        # 1. Existing destination file must remain completely untouched if expectation fails
        dest = stage_dir / "zones_in.csv"
        dest.write_bytes(b"existing-sentinel-do-not-clobber")
        orig_dest_ino = dest.stat().st_ino

        failed = hydrate_admitted_input(
            art,
            destination=dest,
            expectation=expected_wrong,
            ledger=ledger,
            mounts=mounts,
        )
        assert failed.report.outcome == "mismatched"
        assert failed.verdict.agreement == Agreement.MISMATCH
        assert dest.read_bytes() == b"existing-sentinel-do-not-clobber"
        assert dest.stat().st_ino == orig_dest_ino

        # 2. Corrupt primary candidate -> skips bad candidate without touching dest, stages from valid archive
        #    and does NOT hash the verified file a second time during atomic staging!
        src_file.write_bytes(b"corrupted-primary-bytes")
        hash_calls = 0
        orig_hash_fd = engine_mod._hash_file_descriptor_pinned

        def _counting_hash_fd(p: Path) -> tuple[Digest, object]:
            nonlocal hash_calls
            hash_calls += 1
            return orig_hash_fd(p)

        monkeypatch.setattr(
            engine_mod, "_hash_file_descriptor_pinned", _counting_hash_fd
        )
        hydrated = hydrate_admitted_input(
            art,
            destination=dest,
            expectation=expected_good,
            ledger=ledger,
            mounts=mounts,
            stage_mode="copy",
        )
        monkeypatch.setattr(engine_mod, "_hash_file_descriptor_pinned", orig_hash_fd)

        assert hydrated.report.outcome == "verified"
        assert hydrated.verdict.agreement == Agreement.AGREE
        assert hydrated.path == dest.resolve()
        assert dest.read_bytes() == good_bytes
        # Primary was hashed once (detected mismatch); archived copy had a fresh non-racy witness + stored identity (0 hashes)
        # and atomic staging did zero extra hashes!
        assert hash_calls <= 1
        assert list(stage_dir.glob(".consist-tmp-*")) == []


def test_phase2_audit_part_a1_and_a6_admission_regressions(
    tmp_path: Path,
) -> None:
    from consist.artifact import Artifact
    from consist.location import FileStat, Location, Observation

    ws_dir = tmp_path / "workspace"
    arch_dir = tmp_path / "archive"
    stage_dir = tmp_path / "stage"
    ws_dir.mkdir()
    arch_dir.mkdir()
    stage_dir.mkdir()
    mounts = [Mount("workspace", ws_dir), Mount("archive", arch_dir)]

    with StoreLedger(tmp_path / "audit_a1_a6.duckdb", mounts=mounts) as ledger:
        # --- Part A1: hydrate_admitted_input(art, expectation=None) rejects corrupted primary ---
        src_file = ws_dir / "skims.bin"
        good_bytes = b"clean-canonical-bytes-v1"
        corrupt_bytes = b"corrupted-primary-bytes!"
        src_file.write_bytes(good_bytes)
        art = observe(src_file, key="skims_bin", ledger=ledger, mounts=mounts)
        art = archive(
            art,
            arch_dir / "skims.bin",
            mode="copy",
            ledger=ledger,
            mounts=mounts,
        )

        # Corrupt primary workspace file behind Consist's back
        src_file.write_bytes(corrupt_bytes)
        dest_with_fallback = stage_dir / "hydrated_fallback.bin"
        hydrated_ok = hydrate_admitted_input(
            art,
            destination=dest_with_fallback,
            expectation=None,
            ledger=ledger,
            mounts=mounts,
            stage_mode="copy",
        )
        assert hydrated_ok.report.outcome == "verified"
        assert hydrated_ok.verdict.agreement is Agreement.AGREE
        assert dest_with_fallback.read_bytes() == good_bytes
        assert hydrated_ok.artifact.identity.anchor == art.identity.anchor

        # Without a clean archive fallback, expectation=None rejects the corrupted primary
        solo_src = ws_dir / "solo.bin"
        solo_src.write_bytes(b"solo-good-payload")
        solo_art = observe(solo_src, key="solo_bin", ledger=ledger, mounts=mounts)
        solo_src.write_bytes(b"solo-corrupted!!!")
        solo_dest = stage_dir / "solo_dest.bin"
        solo_dest.write_bytes(b"preexisting-sentinel")
        hydrated_bad = hydrate_admitted_input(
            solo_art,
            destination=solo_dest,
            expectation=None,
            ledger=ledger,
            mounts=mounts,
            stage_mode="copy",
        )
        assert hydrated_bad.report.outcome == "mismatched"
        assert hydrated_bad.verdict.agreement is Agreement.MISMATCH
        assert solo_dest.read_bytes() == b"preexisting-sentinel"

        # Unverifiable Kind.HISTORICAL Artifact with expectation=None stays unverified
        hist_src = ws_dir / "legacy.csv"
        hist_src.write_text("a,b\n1,2\n", encoding="utf-8")
        hist_anchor = Digest("legacy-v1:unverified", "a" * 64)
        hist_obs = Observation(
            location=Location(uri="workspace://legacy.csv"),
            identity=Identity(hist_anchor),
            access_driver="csv",
            witness=FileStat.capture(hist_src),
            basis="migrated_v1",
            site="local",
        )
        ledger.record_observation(hist_obs, repr_kind="file")
        hist_art = Artifact(
            id=uuid.uuid4(),
            key="legacy_csv",
            driver="csv",
            identity=Identity(hist_anchor),
            hints=(hist_obs,),
        )
        hydrated_hist = hydrate_admitted_input(
            hist_art,
            expectation=None,
            ledger=ledger,
            mounts=mounts,
        )
        assert hydrated_hist.report.outcome == "unverified"
        assert hydrated_hist.verdict.agreement is Agreement.UNKNOWN

        # --- Part A6: lossy float32_matrices_v1 admission does NOT poison StoreLedger ---
        omx_f64 = ws_dir / "skims_f64.omx"
        zarr_f32 = ws_dir / "skims_f32.zarr"
        # Values with non-zero float64 vs float32 rounding delta (1.1 in IEEE-754)
        arr_f64 = np.array([[1.1, 2.2], [3.3, 4.4]], dtype=np.float64)
        arr_f32 = arr_f64.astype(np.float32)
        with h5py.File(omx_f64, "w") as h5f:
            grp = h5f.create_group("data")
            grp.create_dataset("DIST", data=arr_f64)
        zr = zarr.open_group(str(zarr_f32), mode="w")
        zds = zr.create_dataset("DIST", data=arr_f32)
        zds.attrs["_ARRAY_DIMENSIONS"] = ["o", "d"]

        f64_canonical_hex = compute_logical_matrices_sha256(
            {"DIST": arr_f64},
            policy="preserve_canonical",
        )
        f32_lossy_hex = compute_logical_matrices_sha256(
            {"DIST": arr_f32},
            policy="float32_matrices_v1",
        )
        assert f64_canonical_hex != f32_lossy_hex

        prior_db = tmp_path / "prior_f32.duckdb"
        with StoreLedger(prior_db, mounts=mounts) as prior_ledger:
            prior_ledger.record_run(
                run_id="run_f32_ref",
                model_name="skim_gen",
                status="completed",
                cache_status="live",
            )
            seal(
                [
                    PendingOutput(
                        role="skims_zarr",
                        path=zarr_f32,
                        driver="zarr",
                        is_dir=True,
                    )
                ],
                ledger=prior_ledger,
                run_id="run_f32_ref",
                mounts=mounts,
            )

        omx_art = observe(
            omx_f64, key="skims", driver="omx", ledger=ledger, mounts=mounts
        )
        zarr_art = observe(
            zarr_f32, key="skims_zarr", driver="zarr", ledger=ledger, mounts=mounts
        )

        # Admit float64 OMX against float32_matrices_v1 fallback PriorRunExpectation
        prior_f32_exp = PriorRunExpectation(
            expected_run_id="run_f32_ref",
            artifact_key="skims_zarr",
            source_tracker_db=prior_db,
            identity_scheme="logical",
            logical_profile="float32_matrices_v1",
            expected_logical_hash=f"sha256:logical_matrices:{f32_lossy_hex}",
        )
        hyd_omx_lossy = hydrate_admitted_input(
            omx_art,
            input_role="skims",
            expectation=prior_f32_exp,
            ledger=ledger,
            mounts=mounts,
        )
        assert hyd_omx_lossy.report.outcome == "verified"
        assert hyd_omx_lossy.verdict.agreement is Agreement.AGREE
        assert hyd_omx_lossy.artifact.identity.get("sha256:logical_matrices:v1") is None

        hyd_zarr_lossy = hydrate_admitted_input(
            zarr_art,
            input_role="skims_zarr",
            expectation=DeclaredDigestExpectation(
                identity=f"sha256:logical_matrices:{f32_lossy_hex}",
                logical_profile="float32_matrices_v1",
            ),
            ledger=ledger,
            mounts=mounts,
        )
        assert hyd_zarr_lossy.report.outcome == "verified"
        assert (
            hyd_zarr_lossy.artifact.identity.get("sha256:logical_matrices:v1") is None
        )

        for checked_anchor in (omx_art.identity.anchor, zarr_art.identity.anchor):
            assert (
                ledger.get_identity(checked_anchor).get("sha256:logical_matrices:v1")
                is None
            )
            with ledger.exec_cursor() as cur:
                stored_schemes = [
                    r[0]
                    for r in cur.execute(
                        "SELECT scheme FROM main.content_digest WHERE anchor = ?",
                        [str(checked_anchor)],
                    ).fetchall()
                ]
            assert "sha256:logical_matrices:v1" not in stored_schemes

        # Subsequent canonical preserve_canonical admission against f64_canonical_hex succeeds and records canonical claim
        hyd_omx_canon = hydrate_admitted_input(
            omx_art,
            input_role="skims",
            expectation=DeclaredDigestExpectation(
                identity=f"sha256:logical_matrices:{f64_canonical_hex}",
                logical_profile="preserve_canonical",
            ),
            ledger=ledger,
            mounts=mounts,
        )
        assert hyd_omx_canon.report.outcome == "verified"
        assert hyd_omx_canon.verdict.agreement is Agreement.AGREE
        expected_canon_digest = Digest("sha256:logical_matrices:v1", f64_canonical_hex)
        assert (
            hyd_omx_canon.artifact.identity.get("sha256:logical_matrices:v1")
            == expected_canon_digest
        )
        assert (
            ledger.get_identity(omx_art.identity.anchor).get(
                "sha256:logical_matrices:v1"
            )
            == expected_canon_digest
        )


def test_pr6_modules_have_no_getattr_setattr_or_ponytail_comments() -> None:
    root = Path(__file__).resolve().parents[3]
    assert not (root / "src" / "consist" / "manifest.py").exists()
    paths = [
        root / "src" / "consist" / "location.py",
        root / "src" / "consist" / "digest.py",
        root / "src" / "consist" / "meta.py",
        root / "src" / "consist" / "artifact.py",
        root / "src" / "consist" / "store" / "detect.py",
        root / "src" / "consist" / "store" / "schema.py",
        root / "src" / "consist" / "store" / "ledger.py",
        root / "src" / "consist" / "store" / "manifest.py",
        root / "src" / "consist" / "store" / "__init__.py",
        root / "src" / "consist" / "admission.py",
        root / "src" / "consist" / "core" / "admission.py",
        root / "src" / "consist" / "core" / "logical_array_identity.py",
        root / "src" / "consist" / "engine.py",
        root / "src" / "consist" / "__init__.py",
        root / "tests" / "helpers" / "v1_db_builder.py",
    ]
    for path in paths:
        source = path.read_text(encoding="utf-8")
        assert "ponytail" not in source.lower(), f"Found ponytail comment in {path}"
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in (
                    "getattr",
                    "setattr",
                    "hasattr",
                ), f"Forbidden {node.func.id}() call in {path}:{node.lineno}"
