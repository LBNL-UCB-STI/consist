from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError
import os
from pathlib import Path
import pickle
import shutil
import uuid

import h5py
import numpy as np
import pandas as pd
import pytest

from consist.artifact import Artifact, MissingOutputError
from consist.core.directory_artifacts import build_directory_manifest
from consist.core.views import build_v2_hot_artifact_join_sql
from consist.digest import (
    Contract,
    Digest,
    Identity,
    Kind,
    compute_ctd1_digest,
    compute_logical_matrix_digest,
    file_sha256,
)
from consist.engine import (
    PendingOutput,
    ensure_digest,
    locate,
    seal,
    stage,
    verify,
    verify_path_digest,
)
from consist.location import (
    FileStat,
    Freshness,
    FrozenMeta,
    Location,
    MemberOf,
    Mount,
    Observation,
    SiteContext,
    lookup_live_ledger,
)
from consist.store import ConsistError, StoreLedger


def test_frozen_artifact_immutability_equality_pickling_as_df_and_deprecations(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "households.parquet"
    csv_path = tmp_path / "persons.csv"
    df_hh = pd.DataFrame({"hh_id": [1, 2, 3], "income": [50000, 75000, 120000]})
    df_per = pd.DataFrame({"person_id": [10, 20], "age": [34, 41]})
    df_hh.to_parquet(parquet_path, index=False)
    df_per.to_csv(csv_path, index=False)

    anchor_hh = Digest("sha256:file", file_sha256(parquet_path))

    obs_candidates = tuple(
        Observation(
            location=Location(uri=parquet_path.as_uri()),
            identity=Identity(anchor_hh),
            access_driver="parquet",
            witness=FileStat.capture(parquet_path),
            basis="computed",
            site=f"site-{idx}",
            observed_at_ns=1_000_000 + idx,
        )
        for idx in range(6)
    )

    art_id = uuid.uuid4()
    art1 = Artifact(
        id=str(art_id),  # type: ignore[arg-type]
        key="households",
        driver="parquet",
        identity=Identity(anchor_hh),
        meta={"scenario": "baseline", "year": 2025},  # type: ignore[arg-type]
        hints=obs_candidates,
    )
    assert isinstance(art1.id, uuid.UUID)
    assert art1.id == art_id
    assert art1.anchor == anchor_hh
    assert art1.hash == str(anchor_hh)
    assert isinstance(art1.meta, FrozenMeta)

    # 1. Immutability of Artifact and Artifact.meta
    with pytest.raises(FrozenInstanceError):
        art1.key = "mutated"  # type: ignore[misc]
    with pytest.raises(TypeError, match="read-only"):
        art1.meta["scenario"] = "changed"

    # 2. id-only __eq__ and __hash__
    art1_updated = art1.with_meta(year=2030, extra="tag")
    assert art1_updated.meta["year"] == 2030
    assert art1.meta["year"] == 2025
    assert art1 == art1_updated
    assert hash(art1) == hash(art1_updated)
    mapping = {art1: "first"}
    mapping[art1_updated] = "replaced"
    assert len(mapping) == 1
    assert mapping[art1] == "replaced"

    # 3. Pickling bounds hints to <= 4 and works via HintLedger when StoreLedger is closed
    db_path = tmp_path / "pickle_test.duckdb"
    with StoreLedger(db_path) as ledger:
        sealed = seal(
            {"persons": PendingOutput(role="persons", path=csv_path, driver="csv")},
            ledger=ledger,
            run_id="run_pickle",
        )
        per_art = sealed["persons"]
        assert lookup_live_ledger(per_art.origin) is ledger
        pickled_bytes = pickle.dumps(per_art)

    # Now StoreLedger is closed and unregistered from _live_ledgers
    assert lookup_live_ledger(per_art.origin) is None
    unpickled_per: Artifact = pickle.loads(pickled_bytes)
    assert len(unpickled_per.hints) == 1
    assert unpickled_per.path == csv_path.resolve()
    assert locate(unpickled_per).path == csv_path.resolve()

    unpickled_many: Artifact = pickle.loads(pickle.dumps(art1))
    assert len(unpickled_many.hints) == 4

    # 4. as_df() loads Parquet and CSV directly into pandas.DataFrame
    loaded_hh = art1.as_df()
    loaded_per = unpickled_per.as_df()
    pd.testing.assert_frame_equal(loaded_hh, df_hh)
    pd.testing.assert_frame_equal(loaded_per, df_per)

    # 5. Deprecated compatibility properties emit DeprecationWarning
    with pytest.deprecated_call(match="container_uri is deprecated"):
        assert art1.container_uri == parquet_path.as_uri()
    with pytest.deprecated_call(match="abs_path is deprecated"):
        assert art1.abs_path == str(parquet_path.resolve())

    h5_obs = Observation(
        location=Location(uri=parquet_path.as_uri(), member="/households"),
        identity=Identity(anchor_hh),
        access_driver="h5_table",
    )
    h5_art = Artifact(
        id=uuid.uuid4(),
        key="hh_h5",
        driver="h5_table",
        identity=Identity(anchor_hh),
        hints=(h5_obs,),
    )
    omx_art = Artifact(
        id=uuid.uuid4(),
        key="skims_omx",
        driver="omx",
        identity=Identity(anchor_hh),
        hints=(
            Observation(
                location=Location(uri=parquet_path.as_uri(), member="SOV_AM"),
                identity=Identity(anchor_hh),
                access_driver="omx",
            ),
        ),
    )
    with pytest.deprecated_call(match="table_path is deprecated"):
        assert h5_art.table_path == "/households"
    with pytest.deprecated_call(match="array_path is deprecated"):
        assert omx_art.array_path == "SOV_AM"


def test_effective_promotion_evidence_backed_vs_asserted(tmp_path: Path) -> None:
    db_path = tmp_path / "promotion.duckdb"
    restored_file = tmp_path / "restored_2018.parquet"
    df = pd.DataFrame({"zone": [1, 2], "trips": [10.5, 20.0]})
    df.to_parquet(restored_file, index=False)

    verified_anchor = Digest("sha256:file", file_sha256(restored_file))
    ctd_claim = compute_ctd1_digest(restored_file)
    hist_anchor_1 = Digest("legacy-v1:unverified", "1" * 64)
    hist_anchor_2 = Digest("legacy-v1:unverified", "2" * 64)
    art_id_1 = uuid.uuid4()
    art_id_2 = uuid.uuid4()

    with StoreLedger(db_path) as ledger:
        # Record historical artifacts before promotion
        ledger.record_content(hist_anchor_1, "historical")
        ledger.record_content(hist_anchor_2, "historical")
        ledger.record_artifact(
            artifact_id=art_id_1,
            key="trips_2018_recomputed",
            driver="parquet",
            anchor=hist_anchor_1,
            run_id="run_2018",
            meta={"stage": "initial"},
        )
        ledger.record_artifact(
            artifact_id=art_id_2,
            key="trips_2018_asserted",
            driver="parquet",
            anchor=hist_anchor_2,
            run_id="run_2018",
        )

        # 1. Without promotion: anchor is Kind.HISTORICAL, recorded_anchor is None
        unpromoted = ledger.load_artifact(art_id_1)
        assert unpromoted is not None
        assert unpromoted.identity.anchor == hist_anchor_1
        assert unpromoted.identity.anchor.kind is Kind.HISTORICAL
        assert unpromoted.recorded_anchor is None
        assert verify(unpromoted, ledger=ledger) is False
        with pytest.raises(ValueError, match="Kind.HISTORICAL"):
            ensure_digest(unpromoted, "ctd1:sha256", ledger=ledger)

        # Record the verified content + location in the store
        verified_identity = Identity(anchor=verified_anchor, claims=(ctd_claim,))
        ledger.record_observation(
            Observation(
                location=Location(uri=restored_file.as_uri()),
                identity=verified_identity,
                access_driver="parquet",
                witness=FileStat.capture(restored_file),
                basis="computed",
                site="local",
            ),
            repr_kind="file",
        )

        # 2. Evidence-backed promotion ("v1_fast_recomputed"):
        # Effective identity is promoted to verified_anchor + claims; recorded_anchor preserves hist_anchor_1!
        assert ledger.record_anchor_promotion(
            hist_anchor_1,
            verified_anchor,
            basis="v1_fast_recomputed",
            evidence={"v1_fast_hash": "a" * 64},
        )
        promoted_art = ledger.load_artifact(art_id_1)
        assert promoted_art is not None
        assert promoted_art.identity.anchor == verified_anchor
        assert promoted_art.identity.get("ctd1:sha256") == ctd_claim
        assert promoted_art.recorded_anchor == hist_anchor_1
        assert locate(promoted_art, ledger=ledger).path == restored_file.resolve()
        assert verify(promoted_art, ledger=ledger) is True

        # Also test update_artifact_meta persistence
        updated_art = ledger.update_artifact_meta(promoted_art, stage="promoted")
        assert updated_art.meta["stage"] == "promoted"
        reloaded_art = ledger.load_artifact(art_id_1)
        assert reloaded_art is not None
        assert reloaded_art.meta == {"stage": "promoted"}

        # 3. Asserted promotion ("asserted"):
        # Identity remains Kind.HISTORICAL (cannot enter live cache signatures),
        # recorded_anchor is None, yet locate() resolves the file under verified_anchor!
        assert ledger.record_anchor_promotion(
            hist_anchor_2,
            verified_anchor,
            basis="asserted",
            asserted_by="analyst",
        )
        asserted_art = ledger.load_artifact(art_id_2)
        assert asserted_art is not None
        assert asserted_art.identity.anchor == hist_anchor_2
        assert asserted_art.identity.anchor.kind is Kind.HISTORICAL
        assert asserted_art.recorded_anchor is None
        resolved_asserted = locate(asserted_art, ledger=ledger)
        assert resolved_asserted.path == restored_file.resolve()

        # 4. Unpickled asserted-promotion artifact resolves via HintLedger after StoreLedger closes,
        # including when mtime changes and locate() re-verifies against the hint's anchor!
        asserted_pickled = pickle.dumps(asserted_art)

    unpickled_asserted: Artifact = pickle.loads(asserted_pickled)
    assert lookup_live_ledger(unpickled_asserted.origin) is None
    assert locate(unpickled_asserted).path == restored_file.resolve()
    bump_ns = restored_file.stat().st_mtime_ns + 5_000_000_000
    os.utime(restored_file, ns=(bump_ns, bump_ns))
    assert locate(unpickled_asserted).path == restored_file.resolve()


def test_seal_missing_output_error_tee_archive_fallback_and_directory_manifest(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "seal.duckdb"
    scratch_dir = tmp_path / "scratch"
    archive_dir = tmp_path / "archive"
    scratch_dir.mkdir()

    mounts = {
        "workspace": Mount(name="workspace", root=scratch_dir, scope="site"),
        "archive": Mount(name="archive", root=archive_dir, scope="site"),
    }
    site_ctx = SiteContext(site="perlmutter", node_name="nid001001", job_id="42")

    # 1. Declaring an unwritten output raises MissingOutputError naming both role and path
    missing_file = scratch_dir / "unwritten.parquet"
    with pytest.raises(MissingOutputError) as exc_info:
        seal(
            [PendingOutput(role="skims_out", path=missing_file)],
            mounts=mounts,
            site_context=site_ctx,
        )
    assert exc_info.value.role == "skims_out"
    assert exc_info.value.path == missing_file
    assert "skims_out" in str(exc_info.value)
    assert str(missing_file) in str(exc_info.value)

    # 2. Regular file sealed with archive_root: tee-hashes once into primary + archive observations;
    # deleting or overwriting the scratch file transparently falls back to the archive copy!
    out_parquet = scratch_dir / "cleaned.parquet"
    df_clean = pd.DataFrame({"id": [1, 2, 3], "val": [0.5, 1.5, 2.5]})
    df_clean.to_parquet(out_parquet, index=False)

    # Create a multi-file directory output as well
    out_dir = scratch_dir / "bundle.zarr"
    (out_dir / "sub").mkdir(parents=True)
    (out_dir / "empty_dir").mkdir(parents=True)
    (out_dir / ".zgroup").write_bytes(b'{"zarr_format":2}\n')
    (out_dir / "sub" / "chunk.0").write_bytes(b"\x10\x20\x30\x40")

    with StoreLedger(db_path) as ledger:
        ledger.record_run(run_id="run_seal_1", model_name="clean_step")
        sealed = seal(
            {
                "cleaned": PendingOutput(
                    role="cleaned",
                    path=out_parquet,
                    driver="parquet",
                    meta={"rows": 3},
                ),
                "bundle": PendingOutput(
                    role="bundle",
                    path=out_dir,
                    driver="zarr",
                    is_dir=True,
                ),
            },
            ledger=ledger,
            run_id="run_seal_1",
            site_context=site_ctx,
            mounts=mounts,
            archive_root=archive_dir,
            max_workers=4,
        )

        cleaned_art = sealed["cleaned"]
        bundle_art = sealed["bundle"]

        # Primary scratch path is preferred while intact
        loc_primary = locate(
            cleaned_art,
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
        )
        assert loc_primary.path == out_parquet.resolve()
        assert loc_primary.uri == "workspace://cleaned.parquet"

        # Overwrite the scratch file with different bytes -> locate() detects mismatch and falls back to archive!
        out_parquet.write_bytes(b"corrupted-or-overwritten-by-next-year")
        loc_after_overwrite = locate(
            cleaned_art,
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
        )
        assert loc_after_overwrite.path == (archive_dir / "cleaned.parquet").resolve()
        assert loc_after_overwrite.uri == "archive://cleaned.parquet"
        pd.testing.assert_frame_equal(cleaned_art.as_df(), df_clean)

        # Delete the scratch file completely -> locate() still resolves archive!
        out_parquet.unlink()
        assert cleaned_art.path == (archive_dir / "cleaned.parquet").resolve()

        # Verify directory manifest matches build_directory_manifest byte-for-byte and is stored in content_manifest
        v1_manifest = build_directory_manifest(archive_dir / "bundle.zarr")
        assert bundle_art.anchor == Digest(
            "sha256:manifest-v1", v1_manifest["tree_hash"]
        )
        manifest_row = ledger.connection.execute(
            "SELECT format, entry_count, body FROM content_manifest WHERE anchor = ?",
            [str(bundle_art.anchor)],
        ).fetchone()
        assert manifest_row is not None
        assert manifest_row[0] == "manifest-v1"
        assert manifest_row[1] == len(v1_manifest["entries"])

        # Verify run_binding rows were recorded for both outputs
        bindings = ledger.connection.execute(
            "SELECT slot, direction, artifact_id FROM run_binding "
            "WHERE run_id = 'run_seal_1' ORDER BY occurrence_index"
        ).fetchall()
        assert bindings == [
            ("cleaned", "output", cleaned_art.id),
            ("bundle", "output", bundle_art.id),
        ]


def test_container_child_sibling_mutation_refresh_and_archive_carry_along(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "container_children.duckdb"
    scratch_dir = tmp_path / "scratch"
    archive_dir = tmp_path / "archive"
    scratch_dir.mkdir()
    archive_dir.mkdir()

    mounts = {
        "workspace": Mount(name="workspace", root=scratch_dir, scope="site"),
        "archive": Mount(name="archive", root=archive_dir, scope="site"),
    }
    site_ctx = SiteContext(site="lawrencium", node_name="n0042.lr7")

    # Create an HDF5 container with two tables (/households and /persons) and an OMX matrix container
    h5_path = scratch_dir / "pipeline.h5"
    df_hh = pd.DataFrame({"hh_id": [1, 2], "Autos": [1, 2]})
    df_per = pd.DataFrame({"per_id": [101, 102, 103], "hh_id": [1, 1, 2]})
    df_hh.to_hdf(h5_path, key="/households", mode="w", format="fixed")
    df_per.to_hdf(h5_path, key="/persons", mode="a", format="fixed")

    omx_path = scratch_dir / "skims.omx"
    mat_sov = np.array([[1.0, 2.5], [3.5, 4.0]], dtype=np.float32)
    with h5py.File(omx_path, "w") as h5f:
        data_grp = h5f.create_group("data")
        data_grp.create_dataset("SOV_AM", data=mat_sov)

    with StoreLedger(db_path) as ledger:
        sealed = seal(
            [
                PendingOutput(
                    role="households",
                    path=h5_path,
                    driver="h5_table",
                    member="/households",
                ),
                PendingOutput(
                    role="persons",
                    path=h5_path,
                    driver="h5_table",
                    member="/persons",
                ),
                PendingOutput(
                    role="sov_skim",
                    path=omx_path,
                    driver="omx",
                    member="SOV_AM",
                ),
            ],
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
        )

        hh_art = sealed["households"]
        sov_art = sealed["sov_skim"]
        assert hh_art.anchor == compute_ctd1_digest(h5_path, member="/households")
        assert sov_art.anchor == compute_logical_matrix_digest("SOV_AM", mat_sov)
        pd.testing.assert_frame_equal(hh_art.as_df(), df_hh)

        initial_loc = locate(
            hh_art,
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
        )
        assert isinstance(initial_loc.witness, MemberOf)
        old_parent_stat = initial_loc.witness.parent

        # Modify sibling table /persons inside pipeline.h5 so parent file size and mtime_ns change!
        df_per_modified = pd.DataFrame(
            {
                "per_id": list(range(200)),
                "hh_id": [1] * 100 + [2] * 100,
                "extra_col": np.arange(200, dtype=np.float64),
            }
        )
        df_per_modified.to_hdf(h5_path, key="/persons", mode="a", format="fixed")
        bump_ns = old_parent_stat.mtime_ns + 5_000_000_000
        os.utime(h5_path, ns=(bump_ns, bump_ns))

        # Before re-verify, MemberOf(old_parent_stat) is STALE
        assert initial_loc.witness.check(h5_path) is Freshness.STALE
        loc_no_verify = locate(
            hh_art,
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
            verify_on_stale=False,
        )
        assert loc_no_verify.freshness is Freshness.STALE

        # Calling locate(hh_art) re-verifies ONLY /households against its ctd1:sha256 anchor,
        # records a refreshed MemberOf(parent=new_stat) in StoreLedger, and returns FRESH!
        refreshed_loc = locate(
            hh_art,
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
        )
        assert refreshed_loc.freshness is Freshness.FRESH
        assert isinstance(refreshed_loc.witness, MemberOf)
        assert refreshed_loc.witness.parent.mtime_ns == bump_ns

        # Subsequent locate() with verify_on_stale=False is now immediately FRESH without re-hashing!
        second_loc = locate(
            hh_art,
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
            verify_on_stale=False,
        )
        assert second_loc.freshness is Freshness.FRESH

        # Archive pipeline.h5 and call record_archive_copy:
        # active child member observations (/households and /persons) are carried along automatically!
        archived_h5 = archive_dir / "pipeline.h5"
        shutil.copy2(h5_path, archived_h5)
        derived_ids = ledger.record_archive_copy(
            src_uri="workspace://pipeline.h5",
            dst_uri="archive://pipeline.h5",
            site=site_ctx.site,
            dst_path=archived_h5,
        )
        assert len(derived_ids) == 2

        # Delete scratch pipeline.h5 -> locate(hh_art) resolves to archive://pipeline.h5 member=/households!
        h5_path.unlink()
        archived_loc = locate(
            hh_art,
            ledger=ledger,
            site_context=site_ctx,
            mounts=mounts,
        )
        assert archived_loc.path == archived_h5.resolve()
        assert archived_loc.uri == "archive://pipeline.h5"
        assert archived_loc.member == "/households"


def test_verify_ensure_digest_mid_read_mutation_guard_and_stage_contracts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "engine_ops.duckdb"
    src_parquet = tmp_path / "source.parquet"
    df = pd.DataFrame({"k": [1, 2, 3], "v": ["x", "y", "z"]})
    df.to_parquet(src_parquet, index=False)

    with StoreLedger(db_path) as ledger:
        sealed = seal(
            {"tbl": PendingOutput(role="tbl", path=src_parquet, driver="parquet")},
            ledger=ledger,
        )
        art = sealed["tbl"]
        assert art.identity.anchor.scheme == "sha256:file"
        assert art.identity.get("ctd1:sha256") is None
        assert verify(art, ledger=ledger) is True

        # 1. ensure_digest computes and persists secondary ctd1:sha256 claim on demand
        art_with_ctd, ctd_digest = ensure_digest(
            art,
            "ctd1:sha256",
            ledger=ledger,
        )
        assert ctd_digest == compute_ctd1_digest(src_parquet)
        assert art_with_ctd.identity.get("ctd1:sha256") == ctd_digest
        assert ledger.get_identity(art.anchor).get("ctd1:sha256") == ctd_digest

        # Calling ensure_digest again returns the already-attached claim without recomputing
        art_cached, ctd_cached = ensure_digest(
            art_with_ctd,
            "ctd1:sha256",
            ledger=ledger,
        )
        assert art_cached is art_with_ctd
        assert ctd_cached == ctd_digest

        # 2. Descriptor-pinned mid-read mutation guard raises ConsistError if fstat changes during read
        real_fstat = os.fstat
        call_counter = {"count": 0}

        def _mutating_fstat(fd: int) -> os.stat_result:
            st = real_fstat(fd)
            call_counter["count"] += 1
            if call_counter["count"] % 2 == 0:
                # Simulate size/mtime changing between stat_before and stat_after
                return os.stat_result(
                    (
                        st.st_mode,
                        st.st_ino,
                        st.st_dev,
                        st.st_nlink,
                        st.st_uid,
                        st.st_gid,
                        st.st_size + 64,
                        st.st_atime,
                        st.st_mtime + 1,
                        st.st_ctime,
                    )
                )
            return st

        with monkeypatch.context() as mp:
            mp.setattr(os, "fstat", _mutating_fstat)
            with pytest.raises(
                ConsistError, match="Concurrent mid-read mutation detected"
            ):
                verify_path_digest(src_parquet, art.anchor, driver="parquet")

        # 3. stage() with Contract(mutates=False) hardlinks on same filesystem;
        # stage() with Contract(mutates=True) copies so mutating dest never corrupts source!
        dest_readonly = tmp_path / "staged_readonly.parquet"
        staged_ro = stage(
            art,
            dest_readonly,
            contract=Contract(mutates=False),
            ledger=ledger,
        )
        assert staged_ro == dest_readonly.resolve()
        assert dest_readonly.stat().st_ino == src_parquet.stat().st_ino

        dest_mutable = tmp_path / "staged_mutable.parquet"
        staged_mut = stage(
            art,
            dest_mutable,
            contract=Contract(mutates=True),
            ledger=ledger,
        )
        assert staged_mut == dest_mutable.resolve()
        assert dest_mutable.stat().st_ino != src_parquet.stat().st_ino

        # Mutate dest_mutable in place and verify src_parquet is completely untouched
        dest_mutable.write_bytes(b"mutated in place by legacy tool")
        assert verify(art, path=src_parquet, ledger=ledger) is True
        assert verify(art, path=dest_mutable, ledger=ledger) is False

        # Explicitly requesting hardlink or symlink with contract.mutates=True raises ValueError
        with pytest.raises(ValueError, match="contract.mutates=True"):
            stage(
                art,
                tmp_path / "bad_link.parquet",
                mode="hardlink",
                contract=Contract(mutates=True),
                ledger=ledger,
            )
        with pytest.raises(ValueError, match="contract.mutates=True"):
            stage(
                art,
                tmp_path / "bad_symlink.parquet",
                mode="symlink",
                contract=Contract(mutates=True),
                ledger=ledger,
            )


def test_view_factory_v2_uuid_cast_join_with_global_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "views_v2.duckdb"
    art_id = uuid.uuid4()
    anchor = Digest("ctd1:sha256", "a" * 64)

    with StoreLedger(db_path) as ledger:
        ledger.record_run(
            run_id="run_hot_1",
            model_name="ingest_step",
            year=2025,
            iteration=1,
        )
        ledger.record_content(anchor, "table", schema_id="households_v1")
        ledger.record_artifact(
            artifact_id=art_id,
            key="households",
            driver="dlt",
            anchor=anchor,
            run_id="run_hot_1",
            schema_id="households_v1",
        )
        ledger.record_run_binding(
            run_id="run_hot_1",
            direction="output",
            occurrence_index=0,
            slot="households",
            artifact_id=art_id,
            anchor=anchor,
        )

        con = ledger.connection
        con.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        con.execute(
            "CREATE TABLE global_tables.households ("
            "hh_id INTEGER, income DOUBLE, consist_run_id VARCHAR, consist_artifact_id VARCHAR"
            ")"
        )
        con.execute(
            "INSERT INTO global_tables.households VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
            [
                1,
                65000.0,
                "run_hot_1",
                str(art_id),
                2,
                92000.0,
                "run_hot_1",
                str(art_id),
            ],
        )

        join_pred = build_v2_hot_artifact_join_sql(hot_alias="h", artifact_alias="a")
        rows = con.execute(
            f"SELECT h.hh_id, h.income, a.key, ral.direction, r.year "
            f"FROM global_tables.households h "
            f"JOIN artifact a ON {join_pred} "
            f"JOIN run_artifact_link ral ON ral.artifact_id = a.id AND ral.run_id = h.consist_run_id "
            f"JOIN run r ON r.id = ral.run_id "
            f"ORDER BY h.hh_id"
        ).fetchall()
        assert rows == [
            (1, 65000.0, "households", "output", 2025),
            (2, 92000.0, "households", "output", 2025),
        ]


def test_review_regressions_symlinks_archive_subpaths_scope_keys_and_missing_member(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "regressions.duckdb"
    scratch_dir = tmp_path / "scratch"
    archive_dir = tmp_path / "archive"
    (scratch_dir / "am").mkdir(parents=True)
    (scratch_dir / "pm").mkdir(parents=True)
    archive_dir.mkdir(parents=True)

    mounts = {
        "scratch": Mount(name="scratch", root=scratch_dir, scope="job"),
        "archive": Mount(name="archive", root=archive_dir, scope="site"),
    }
    job1_ctx = SiteContext(site="perlmutter", node_name="nid001", job_id="101")
    job2_ctx = SiteContext(site="perlmutter", node_name="nid002", job_id="202")

    am_file = scratch_dir / "am" / "skims.parquet"
    pm_file = scratch_dir / "pm" / "skims.parquet"
    df_am = pd.DataFrame({"od": [1, 2], "time": [12.0, 15.5]})
    df_pm = pd.DataFrame({"od": [1, 2], "time": [18.0, 24.0]})
    df_am.to_parquet(am_file, index=False)
    df_pm.to_parquet(pm_file, index=False)

    h5_path = scratch_dir / "tables.h5"
    df_am.to_hdf(h5_path, key="/am_table", mode="w", format="fixed")

    with StoreLedger(db_path) as ledger:
        # 1. Missing container member inside an existing HDF5 file raises MissingOutputError
        with pytest.raises(MissingOutputError) as exc_info:
            seal(
                [
                    PendingOutput(
                        role="missing_table",
                        path=h5_path,
                        driver="h5_table",
                        member="/nonexistent_table",
                    )
                ],
                ledger=ledger,
                mounts=mounts,
                site_context=job1_ctx,
            )
        assert exc_info.value.role == "missing_table"
        assert exc_info.value.path == h5_path

        # 2. Archiving two outputs in different subdirectories with identical basename preserves subpaths
        sealed = seal(
            {
                "am_skims": PendingOutput(
                    role="am_skims", path=am_file, driver="parquet"
                ),
                "pm_skims": PendingOutput(
                    role="pm_skims", path=pm_file, driver="parquet"
                ),
                "am_h5": PendingOutput(
                    role="am_h5",
                    path=h5_path,
                    driver="h5_table",
                    member="/am_table",
                ),
            },
            ledger=ledger,
            mounts=mounts,
            site_context=job1_ctx,
            archive_root=archive_dir,
        )
        am_art = sealed["am_skims"]
        pm_art = sealed["pm_skims"]
        h5_art = sealed["am_h5"]

        assert (archive_dir / "am" / "skims.parquet").exists()
        assert (archive_dir / "pm" / "skims.parquet").exists()

        # Overwrite am_file in scratch: verify(am_art) falls back to intact archive copy -> True!
        am_file.write_bytes(b"corrupted-scratch-bytes")
        assert verify(am_art, ledger=ledger, site_context=job1_ctx) is True
        pd.testing.assert_frame_equal(am_art.as_df(), df_am)
        pd.testing.assert_frame_equal(pm_art.as_df(), df_pm)

        # Delete both scratch and archive copies of pm_skims: verify(pm_art) returns False (not FileNotFoundError)
        pm_file.unlink()
        (archive_dir / "pm" / "skims.parquet").unlink()
        assert verify(pm_art, ledger=ledger, site_context=job1_ctx) is False

        # 3. record_archive_copy from job-scoped scratch:// to site-scoped archive:// does not inherit job scope_key
        archived_h5_manual = archive_dir / "tables_manual.h5"
        shutil.copy2(h5_path, archived_h5_manual)
        ledger.record_archive_copy(
            src_uri="scratch://tables.h5",
            dst_uri="archive://tables_manual.h5",
            site=job1_ctx.site,
            dst_path=archived_h5_manual,
        )
        h5_path.unlink()
        (archive_dir / "tables.h5").unlink()
        # Job 2 on a different node can still locate the archived child in archive://tables_manual.h5!
        loc_job2 = locate(h5_art, ledger=ledger, site_context=job2_ctx, mounts=mounts)
        assert loc_job2.path == archived_h5_manual.resolve()
        assert loc_job2.member == "/am_table"

        # 4. stage() when dest_path is a symlink pointing to source or another file
        arch_am_path = (archive_dir / "am" / "skims.parquet").resolve()
        symlink_to_src = tmp_path / "link_to_src.parquet"
        os.symlink(arch_am_path, symlink_to_src)
        staged_over_symlink = stage(
            am_art,
            symlink_to_src,
            contract=Contract(mutates=True),
            ledger=ledger,
            site_context=job1_ctx,
        )
        assert staged_over_symlink == symlink_to_src.resolve()
        assert not symlink_to_src.is_symlink()
        assert symlink_to_src.stat().st_ino != arch_am_path.stat().st_ino

        # Staging directly onto source path with Contract(mutates=True) raises ValueError
        with pytest.raises(ValueError, match="own source path"):
            stage(
                am_art,
                arch_am_path,
                contract=Contract(mutates=True),
                ledger=ledger,
                site_context=job1_ctx,
            )


def test_pr3_modules_have_no_getattr_setattr_or_ponytail_comments() -> None:
    root = Path(__file__).resolve().parents[3]
    paths = [
        root / "src" / "consist" / "artifact.py",
        root / "src" / "consist" / "engine.py",
        root / "src" / "consist" / "store" / "ledger.py",
        root / "src" / "consist" / "core" / "views.py",
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
