from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError
import os
from pathlib import Path
import pickle
import shutil
import time
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

    past_ns = time.time_ns() - 20_000_000_000
    os.utime(h5_path, ns=(past_ns, past_ns))

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

        # 3. stage() with mode="auto" clones/copies safely (never hardlinks) so mutable writes
        # cannot corrupt source artifacts; explicit mode="hardlink" links when mutates=False!
        dest_readonly = tmp_path / "staged_readonly.parquet"
        staged_ro = stage(
            art,
            dest_readonly,
            contract=Contract(mutates=False),
            ledger=ledger,
        )
        assert staged_ro == dest_readonly.resolve()
        assert dest_readonly.stat().st_ino != src_parquet.stat().st_ino

        dest_hardlink = tmp_path / "staged_hardlink.parquet"
        staged_hl = stage(
            art,
            dest_hardlink,
            mode="hardlink",
            contract=Contract(mutates=False),
            ledger=ledger,
        )
        assert staged_hl == dest_hardlink.resolve()
        assert dest_hardlink.stat().st_nlink == 2
        assert dest_hardlink.stat().st_ino in {
            src_parquet.stat().st_ino,
            dest_readonly.stat().st_ino,
        }

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


def test_phase1_unified_artifact_hint_ledger_accessibility_pydantic_and_weak_tracker(
    tmp_path: Path,
) -> None:
    from datetime import datetime, timezone
    import gc
    from pydantic import TypeAdapter
    from consist.artifact import (
        ArchivedOutputs,
        ArtifactAccessibility,
        ArtifactLocationStatus,
        HintLedger,
    )

    work_dir = tmp_path / "work"
    arch_dir = tmp_path / "arch"
    work_dir.mkdir()
    arch_dir.mkdir()
    mounts = {
        "workspace": Mount(name="workspace", root=work_dir, scope="site"),
        "archive": Mount(name="archive", root=arch_dir, scope="site"),
    }

    # 1. HintLedger returns () when no hint matches requested anchors (no unsafe fallback)
    primary_file = work_dir / "zones.parquet"
    recovery_file = arch_dir / "zones.parquet"
    df = pd.DataFrame({"z": [1, 2]})
    df.to_parquet(primary_file, index=False)
    df.to_parquet(recovery_file, index=False)

    d_match = Digest("sha256:file", file_sha256(primary_file))
    d_other = Digest("sha256:file", "b" * 64)
    obs_match = Observation(
        location=Location(uri="workspace://zones.parquet", member=None),
        identity=Identity(d_match),
        access_driver="parquet",
        site="local",
        observed_at_ns=100,
    )
    hl = HintLedger(hints=(obs_match,))
    assert hl.memoized_candidates(d_other) == ()
    assert hl.candidates_for(d_other) == ()
    assert hl.locate_current([d_other]) == []
    assert len(hl.memoized_candidates(d_match)) == 1

    # 2. Unified Artifact fields, properties, with_meta / with_schema / with_hints, accessibility, ArchivedOutputs
    parent_id = uuid.uuid4()
    ts = datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc)
    obs_rec = Observation(
        location=Location(uri="archive://zones.parquet", member=None),
        identity=Identity(d_match),
        access_driver="parquet",
        site="local",
        observed_at_ns=200,
    )
    obs_db = Observation(
        location=Location(uri="db://global_tables/zones", member="zones"),
        identity=Identity(d_match),
        access_driver="db",
        site="local",
        observed_at_ns=300,
    )
    art = Artifact(
        id=uuid.uuid4(),
        key="zones",
        identity=Identity(d_match),
        driver="parquet",
        run_id="run_p1",
        parent_artifact_id=parent_id,
        created_at=ts,
        meta=FrozenMeta({"tag": "v1", "schema_profile": {"cols": 1}}),
        hints=(obs_match, obs_rec),
    )
    assert art.hash == str(d_match)
    assert art.parent_artifact_id == parent_id
    assert art.created_at == ts
    assert art.created_at_iso == ts.isoformat()
    assert art.primary_location == Location(
        uri="workspace://zones.parquet", member=None
    )
    assert art.member is None
    assert art.is_ingested is False

    art_ingested = art.with_hints((obs_match, obs_rec, obs_db))
    assert art_ingested.is_ingested is True

    # with_meta preserves system meta when merge=False
    replaced = art.with_meta({"tag": "v2"}, merge=False)
    assert replaced.meta["tag"] == "v2"
    assert replaced.meta["schema_profile"] == {"cols": 1}

    with_sch = replaced.with_schema("sch_zones", summary={"rows": 2})
    assert with_sch.schema_id == "sch_zones"
    assert with_sch.meta["schema_summary"] == {"rows": 2}

    # Weak tracker registry (DB-less runs only) + picklability
    class _DummyDblessTracker:
        def __init__(self, m: dict[str, Mount]) -> None:
            self.db_path = None
            self.mounts = m

    dummy_tracker: _DummyDblessTracker | None = _DummyDblessTracker(mounts)
    art.bind_tracker(dummy_tracker)
    assert art.resolve_bound_tracker() is dummy_tracker
    assert str(arch_dir.resolve()) in art.recovery_roots
    with pytest.deprecated_call(match="abs_path is deprecated"):
        assert art.abs_path == str(primary_file.resolve())

    acc_primary = art.accessibility(dummy_tracker)
    assert isinstance(acc_primary, ArtifactAccessibility)
    assert acc_primary.status == "primary"
    assert acc_primary.accessible is True
    assert isinstance(acc_primary.best_location, ArtifactLocationStatus)

    primary_file.unlink()
    acc_recovery = art.accessibility(dummy_tracker)
    assert acc_recovery.status == "recovery"
    assert acc_recovery.accessible is True
    assert acc_recovery.best_location is not None
    assert acc_recovery.best_location.path == recovery_file.resolve()

    archived_map = ArchivedOutputs({"zones": recovery_file}, {"zones": art})
    assert archived_map["zones"] == recovery_file
    assert archived_map.outputs["zones"] == art
    assert len(archived_map) == 1

    # Pickling stays clean and weak tracker GC drops weak reference
    unpickled = pickle.loads(pickle.dumps(art))
    assert unpickled == art
    dummy_tracker = None
    gc.collect()
    assert art.resolve_bound_tracker() is None

    # 3. Pydantic v2 TypeAdapter round-trip
    adapter = TypeAdapter(Artifact)
    dumped = adapter.dump_python(with_sch, mode="json")
    restored = adapter.validate_python(dumped)
    assert restored.id == with_sch.id
    assert restored.anchor == with_sch.anchor
    assert restored.parent_artifact_id == parent_id
    assert restored.schema_id == "sch_zones"


def test_phase2_engine_uri_confinement_member_schemes_fd_pinned_and_racy_witness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import consist.engine as engine_mod
    from consist.digest import compute_logical_matrices_sha256
    from consist.engine import (
        _RACY_WINDOW_NS,
        _non_racy_witness,
        _run_fd_pinned,
        _tee_hash_and_copy_file,
        compute_scheme_digest,
        resolve_uri_to_path,
    )

    mount_root = tmp_path / "workspace"
    mount_root.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"secret-outside")
    inside = mount_root / "sub" / "ok.txt"
    inside.parent.mkdir()
    inside.write_bytes(b"inside-bytes")

    sym_escape = mount_root / "escape_link"
    sym_escape.symlink_to(tmp_path)
    mounts = [Mount("workspace", mount_root)]

    resolved_ok, matched_m = resolve_uri_to_path("workspace://sub/ok.txt", mounts)
    assert resolved_ok == inside.resolve()
    assert matched_m is not None and matched_m.name == "workspace"

    for bad_uri in (
        "workspace://../outside.txt",
        "workspace://sub/../../outside.txt",
        "workspace://escape_link/outside.txt",
    ):
        with pytest.raises(ValueError, match="escapes mount root|Path traversal"):
            resolve_uri_to_path(bad_uri, mounts)

    # Sub-container member rejects whole-file byte schemes and routes to logical schemes
    h5_path = tmp_path / "skims.h5"
    df_zones = pd.DataFrame({"zone_id": [1, 2], "pop": [100, 200]})
    mat_a = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float64)
    mat_b = np.array([[5.0, 6.0], [7.0, 8.0]], dtype=np.float64)
    with h5py.File(h5_path, "w") as h5f:
        grp = h5f.create_group("data")
        grp.create_dataset("time", data=mat_a)
        grp.create_dataset("dist", data=mat_b)
    h5_table_path = tmp_path / "zones_tbl.h5"
    df_zones.to_hdf(h5_table_path, key="/zones", mode="w", format="table")

    for bad_scheme in ("sha256:file", "blake3:file", "sha256:manifest-v1"):
        with pytest.raises(ValueError, match="sub-container member"):
            compute_scheme_digest(h5_path, bad_scheme, driver="h5", member="/data/time")

    d_ctd1, _ = compute_scheme_digest(
        h5_table_path, "ctd1:sha256", driver="h5_table", member="/zones"
    )
    assert d_ctd1 == compute_ctd1_digest(df_zones)

    d_mat, _ = compute_scheme_digest(
        h5_path, "sha256:logical_matrix:v1", driver="omx", member="/data/time"
    )
    assert d_mat == compute_logical_matrix_digest("time", mat_a)

    d_mats, _ = compute_scheme_digest(
        h5_path,
        "sha256:logical_matrices:v1",
        driver="matrix_set",
        member="/data",
    )
    assert d_mats == Digest(
        "sha256:logical_matrices:v1",
        compute_logical_matrices_sha256({"dist": mat_b, "time": mat_a}),
    )

    # _run_fd_pinned detects atomic os.replace mid-read even though fstat(fd) stays stable
    victim = tmp_path / "victim.bin"
    victim.write_bytes(b"original-inode-payload")
    replacement = tmp_path / "replacement.bin"
    replacement.write_bytes(b"original-inode-payload")

    def _replace_mid_read(fd: int) -> bytes:
        data = os.read(fd, 1024)
        os.replace(replacement, victim)
        return data

    with pytest.raises(ConsistError, match="Concurrent mid-read mutation"):
        _run_fd_pinned(victim, _replace_mid_read)

    # _non_racy_witness retains racy=True inside _RACY_WINDOW_NS and clears only when >= window
    base_mtime_ns = 1_700_000_000_000_000_000
    racy_stat = FileStat(
        size=10,
        mtime_ns=base_mtime_ns,
        ino=123,
        ctime_ns=base_mtime_ns,
        racy=True,
    )
    still_racy = _non_racy_witness(racy_stat, base_mtime_ns + _RACY_WINDOW_NS - 1)
    assert isinstance(still_racy, FileStat) and still_racy.racy is True
    future_racy = _non_racy_witness(racy_stat, base_mtime_ns - 5_000_000_000)
    assert isinstance(future_racy, FileStat) and future_racy.racy is True
    cleared = _non_racy_witness(racy_stat, base_mtime_ns + _RACY_WINDOW_NS)
    assert isinstance(cleared, FileStat) and cleared.racy is False

    # _tee_hash_and_copy_file unlinks sibling .consist-tmp-* on BaseException
    src_copy = tmp_path / "src_stream.bin"
    src_copy.write_bytes(b"stream-payload" * 100)
    dst_dir = tmp_path / "arch_dest"
    dst_copy = dst_dir / "out.bin"

    orig_read = os.read

    def _boom_read(fd: int, n: int) -> bytes:
        _ = orig_read(fd, 8)
        raise KeyboardInterrupt("simulated interrupt")

    monkeypatch.setattr(engine_mod.os, "read", _boom_read)
    with pytest.raises(KeyboardInterrupt, match="simulated interrupt"):
        _tee_hash_and_copy_file(src_copy, dst_copy)
    monkeypatch.setattr(engine_mod.os, "read", orig_read)

    assert not dst_copy.exists()
    assert list(dst_dir.glob(".consist-tmp-*")) == []


def test_phase2_seal_atomicity_bounded_pool_policy_locate_stage_and_archive_move(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import consist.digest as digest_mod
    import consist.engine as engine_mod
    from consist.digest import DigestPolicy, compute_logical_matrices_sha256
    from consist.engine import _surviving_row_is_fresh_on_disk, archive, evict

    ws_root = tmp_path / "ws"
    arch_root = tmp_path / "archive"
    ws_root.mkdir()
    arch_root.mkdir()
    mounts = [Mount("workspace", ws_root), Mount("archive", arch_root)]

    f1 = ws_root / "a.txt"
    f2 = ws_root / "b.txt"
    f1.write_bytes(b"alpha")
    f2.write_bytes(b"beta")

    # 1. Pre-validation of duplicate keys and invalid FrozenMeta before hashing/copying
    hash_calls = 0
    orig_hash_pinned = engine_mod._hash_file_descriptor_pinned

    def _counting_hash(path: Path) -> tuple[Digest, FileStat]:
        nonlocal hash_calls
        hash_calls += 1
        return orig_hash_pinned(path)

    monkeypatch.setattr(engine_mod, "_hash_file_descriptor_pinned", _counting_hash)
    with pytest.raises(ValueError, match="Duplicate output artifact key"):
        seal(
            [
                PendingOutput(role="r1", path=f1, key="dup_key"),
                PendingOutput(role="r2", path=f2, key="dup_key"),
            ],
            archive_root=arch_root / "should_not_exist",
        )
    with pytest.raises((TypeError, ValueError), match="non-finite|NaN|finite|float"):
        seal(
            [PendingOutput(role="r1", path=f1, meta={"bad": float("nan")})],
            archive_root=arch_root / "should_not_exist",
        )
    assert hash_calls == 0
    assert not (arch_root / "should_not_exist").exists()
    monkeypatch.setattr(engine_mod, "_hash_file_descriptor_pinned", orig_hash_pinned)

    # 2. Shared bounded ThreadPoolExecutor across multiple directories + explicit mounts isolation
    d1 = ws_root / "dir1"
    d2 = ws_root / "dir2"
    d1.mkdir()
    d2.mkdir()
    (d1 / "p1.txt").write_bytes(b"d1-p1")
    (d1 / "ignore.tmp").write_bytes(b"temp-bytes")
    (d2 / "p2.txt").write_bytes(b"d2-p2")

    default_mounts_snapshot = dict(engine_mod._default_mounts)
    nested_pools_created = 0
    orig_digest_tpe = digest_mod.ThreadPoolExecutor

    def _forbid_nested_pool(*args: object, **kwargs: object) -> object:
        nonlocal nested_pools_created
        nested_pools_created += 1
        return orig_digest_tpe(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(digest_mod, "ThreadPoolExecutor", _forbid_nested_pool)

    db_path = tmp_path / "phase2_engine.duckdb"
    with StoreLedger(db_path) as ledger:
        ledger.record_run(run_id="run_seal_tx", model_name="test", status="running")
        sealed = seal(
            [
                PendingOutput(role="d1", path=d1, is_dir=True),
                PendingOutput(role="d2", path=d2, is_dir=True),
                PendingOutput(role="f1", path=f1),
            ],
            ledger=ledger,
            run_id="run_seal_tx",
            mounts=mounts,
            max_workers=2,
        )
        assert nested_pools_created == 0
        assert dict(engine_mod._default_mounts) == default_mounts_snapshot
        assert sealed["f1"].primary_location is not None
        assert sealed["f1"].primary_location.uri == "workspace://a.txt"

        # Transaction rollback if StoreLedger fails mid-seal
        tx_id_1 = uuid.uuid4()
        tx_id_2 = uuid.uuid4()
        orig_record_binding = ledger.record_run_binding

        def _fail_second_binding(**kwargs: object) -> None:
            if kwargs.get("slot") == "tx_f2":
                raise RuntimeError("simulated mid-seal failure")
            orig_record_binding(**kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(ledger, "record_run_binding", _fail_second_binding)
        with pytest.raises(RuntimeError, match="simulated mid-seal failure"):
            seal(
                [
                    PendingOutput(
                        role="tx_f1",
                        path=f1,
                        key="tx_only_1",
                        artifact_id=tx_id_1,
                    ),
                    PendingOutput(
                        role="tx_f2",
                        path=f2,
                        key="tx_only_2",
                        artifact_id=tx_id_2,
                    ),
                ],
                ledger=ledger,
                run_id="run_seal_tx",
                mounts=mounts,
            )
        monkeypatch.setattr(ledger, "record_run_binding", orig_record_binding)
        assert ledger.load_artifact(tx_id_1) is None
        assert ledger.load_artifact(tx_id_2) is None

        # 3. ensure_digest recomputes directory manifest when DigestPolicy differs
        dir_art = sealed["d1"]
        policy_ignore_tmp = DigestPolicy(exclude=("*.tmp",))
        updated_dir_art, ignored_digest = ensure_digest(
            dir_art,
            "sha256:manifest-v1",
            ledger=ledger,
            mounts=mounts,
            policy=policy_ignore_tmp,
        )
        assert ignored_digest.scheme == policy_ignore_tmp.scheme
        assert ignored_digest.hex != dir_art.identity.anchor.hex
        assert updated_dir_art.identity.get(policy_ignore_tmp.scheme) == ignored_digest

        # 4. locate() checks each hint's own location.member across multi-hint Artifacts
        h5_multi = ws_root / "multi_hint.h5"
        arr_x = np.array([[10, 20], [30, 40]], dtype=np.int64)
        with h5py.File(h5_multi, "w") as h5f:
            h5f.create_dataset("m_primary", data=arr_x)
            h5f.create_dataset("m_alt", data=arr_x)
        mat_anchor = compute_logical_matrix_digest("m_alt", arr_x)
        parent_st = FileStat.capture(h5_multi)
        obs_missing_first = Observation(
            location=Location(uri="workspace://gone_hint.h5", member="/m_primary"),
            identity=Identity(mat_anchor),
            access_driver="omx",
            witness=MemberOf(parent=parent_st, member="/m_primary"),
            basis="computed",
            site="local",
            observed_at_ns=200,
        )
        obs_valid_second = Observation(
            location=Location(uri="workspace://multi_hint.h5", member="/m_alt"),
            identity=Identity(mat_anchor),
            access_driver="omx",
            witness=MemberOf(parent=parent_st, member="/m_alt"),
            basis="derived",
            site="local",
            observed_at_ns=100,
        )
        multi_hint_art = Artifact(
            id=uuid.uuid4(),
            key="skim_alt",
            identity=Identity(mat_anchor),
            driver="omx",
            hints=(obs_missing_first, obs_valid_second),
        )
        loc_multi = locate(multi_hint_art, mounts=mounts)
        assert loc_multi.path == h5_multi.resolve()
        assert loc_multi.member == "/m_alt"

        # 5. stage(mode="reflink") raises OSError when CoW clone is unsupported
        monkeypatch.setattr(engine_mod, "_try_reflink_clone", lambda _s, _d: False)
        with pytest.raises(OSError):
            stage(
                sealed["f1"],
                tmp_path / "reflink_fail.txt",
                mode="reflink",
                mounts=mounts,
            )

        # 6. _surviving_row_is_fresh_on_disk: witness & witness_json, site visibility, all 5 schemes, False when unverified
        stale_witness_json = FileStat(
            size=999_999,
            mtime_ns=1,
            ino=1,
            ctime_ns=1,
            racy=False,
        )
        from consist.location import serialize_witness_json

        stale_w_str = serialize_witness_json(stale_witness_json)
        # Site mismatch rejected
        assert (
            _surviving_row_is_fresh_on_disk(
                {
                    "uri": "workspace://a.txt",
                    "site": "other-cluster",
                    "witness": FileStat.capture(f1),
                },
                artifact=sealed["f1"],
                effective_mounts={
                    "workspace": Mount("workspace", ws_root, scope="site")
                },
                store_ledger=ledger,
                site_context=SiteContext(site="perlmutter", node_name="n1"),
            )
            is False
        )
        # Fresh witness_json accepted without re-hashing
        assert (
            _surviving_row_is_fresh_on_disk(
                {
                    "uri": "workspace://a.txt",
                    "site": "local",
                    "witness_json": serialize_witness_json(FileStat.capture(f1)),
                },
                artifact=sealed["f1"],
                effective_mounts={"workspace": Mount("workspace", ws_root)},
                store_ledger=ledger,
            )
            is True
        )
        # Stale witness_json falls back to all 5 verifiable schemes
        pq_surv = ws_root / "surv.parquet"
        df_surv = pd.DataFrame({"k": [1, 2], "v": [3, 4]})
        df_surv.to_parquet(pq_surv, index=False)
        ctd_anchor = compute_ctd1_digest(df_surv)

        h5_surv = ws_root / "surv_mats.h5"
        with h5py.File(h5_surv, "w") as h5f:
            g = h5f.create_group("skims")
            g.create_dataset("am", data=arr_x)
        mat1_anchor = compute_logical_matrix_digest("am", arr_x)
        mats_anchor = Digest(
            "sha256:logical_matrices:v1",
            compute_logical_matrices_sha256({"am": arr_x}),
        )

        five_cases = [
            (sealed["f1"], "workspace://a.txt", None, "txt"),
            (sealed["d2"], "workspace://dir2", None, "directory"),
            (
                Artifact(
                    id=uuid.uuid4(),
                    key="pq",
                    identity=Identity(ctd_anchor),
                    driver="parquet",
                ),
                "workspace://surv.parquet",
                None,
                "parquet",
            ),
            (
                Artifact(
                    id=uuid.uuid4(),
                    key="m1",
                    identity=Identity(mat1_anchor),
                    driver="omx",
                ),
                "workspace://surv_mats.h5",
                "/skims/am",
                "omx",
            ),
            (
                Artifact(
                    id=uuid.uuid4(),
                    key="ms",
                    identity=Identity(mats_anchor),
                    driver="matrix_set",
                ),
                "workspace://surv_mats.h5",
                "/skims",
                "matrix_set",
            ),
        ]
        for case_art, case_uri, case_member, case_drv in five_cases:
            assert (
                _surviving_row_is_fresh_on_disk(
                    {
                        "uri": case_uri,
                        "member": case_member,
                        "access_driver": case_drv,
                        "site": "local",
                        "witness_json": stale_w_str,
                    },
                    artifact=case_art,
                    effective_mounts={"workspace": Mount("workspace", ws_root)},
                    store_ledger=ledger,
                )
                is True
            )

        # Unverified historical artifact with stale witness returns False
        hist_art = Artifact(
            id=uuid.uuid4(),
            key="hist_unverified",
            identity=Identity(Digest("legacy-v1:unverified", "a" * 64)),
            driver="txt",
        )
        assert (
            _surviving_row_is_fresh_on_disk(
                {
                    "uri": "workspace://a.txt",
                    "site": "local",
                    "witness_json": stale_w_str,
                },
                artifact=hist_art,
                effective_mounts={"workspace": Mount("workspace", ws_root)},
                store_ledger=ledger,
            )
            is False
        )

        # 7. archive(mode="move") on whole container carries child member observations & evicts src
        move_h5 = ws_root / "move_bundle.h5"
        with h5py.File(move_h5, "w") as h5f:
            h5f.create_dataset("trips", data=arr_x)
        sealed_bundle = seal(
            [
                PendingOutput(role="parent_h5", path=move_h5, driver="h5"),
                PendingOutput(
                    role="child_trips",
                    path=move_h5,
                    driver="omx",
                    member="/trips",
                ),
            ],
            ledger=ledger,
            run_id="run_seal_tx",
            mounts=mounts,
        )
        moved_parent = archive(
            sealed_bundle["parent_h5"],
            arch_root / "moved_bundle.h5",
            mode="move",
            ledger=ledger,
            mounts=mounts,
        )
        assert not move_h5.exists()
        assert (arch_root / "moved_bundle.h5").exists()
        assert (
            locate(moved_parent, ledger=ledger, mounts=mounts).path
            == (arch_root / "moved_bundle.h5").resolve()
        )
        child_loc = locate(sealed_bundle["child_trips"], ledger=ledger, mounts=mounts)
        assert child_loc.path == (arch_root / "moved_bundle.h5").resolve()
        assert child_loc.member == "/trips"
        # Source URI child & parent observations are evicted
        active_src_rows = ledger.query_location_current(
            uri="workspace://move_bundle.h5",
            include_evicted=False,
        )
        assert active_src_rows == []
        # Last copy protection still holds on moved_parent
        with pytest.raises(ConsistError, match="Refusing to evict last active copy"):
            evict(moved_parent, ledger=ledger, mounts=mounts)


def test_phase2_audit_part_a4_a5_and_b5_engine_regressions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    import sys
    import types

    import consist.engine as engine_mod
    from consist.artifact import HintLedger
    from consist.digest import build_manifest_v1
    from consist.engine import archive, evict, observe, stage_verified_source

    ws_root = tmp_path / "ws"
    arch_root = tmp_path / "arch"
    stage_root = tmp_path / "stage"
    scratch_n1 = tmp_path / "scratch_n1"
    scratch_n2 = tmp_path / "scratch_n2"
    for d in (ws_root, arch_root, stage_root, scratch_n1, scratch_n2):
        d.mkdir()

    mounts = [
        Mount("workspace", ws_root),
        Mount("archive", arch_root),
    ]

    with StoreLedger(tmp_path / "audit_engine.duckdb", mounts=mounts) as ledger:
        # --- Part A4: Atomic directory replacement & symlink-to-directory safety ---
        ext_target = tmp_path / "external_precious_dir"
        ext_target.mkdir()
        precious_file = ext_target / "do_not_delete.txt"
        precious_file.write_text("precious-external-data", encoding="utf-8")

        src_dir = ws_root / "bundle_dir"
        (src_dir / "nested").mkdir(parents=True)
        (src_dir / "nested" / "part.txt").write_text("bundle-v2", encoding="utf-8")

        # 1) seal(..., archive_root=...) where archive target is an existing symlink-to-directory
        seal_arch_root = tmp_path / "seal_arch"
        seal_arch_root.mkdir()
        os.symlink(ext_target, seal_arch_root / "bundle_dir")
        sealed_map = seal(
            [PendingOutput(role="bundle", path=src_dir, is_dir=True)],
            ledger=ledger,
            mounts=[*mounts, Mount("archive", seal_arch_root)],
            archive_root=seal_arch_root,
        )
        dir_art = sealed_map["bundle"]
        assert not (seal_arch_root / "bundle_dir").is_symlink()
        assert (seal_arch_root / "bundle_dir" / "nested" / "part.txt").read_text(
            encoding="utf-8"
        ) == "bundle-v2"
        assert precious_file.read_text(encoding="utf-8") == "precious-external-data"

        # 2) archive() overwriting an existing non-empty directory AND an existing symlink-to-directory
        arch_dir_dest = arch_root / "arch_bundle"
        (arch_dir_dest / "old_sub").mkdir(parents=True)
        (arch_dir_dest / "old_sub" / "stale.txt").write_text("old", encoding="utf-8")
        archive(dir_art, arch_dir_dest, mode="copy", ledger=ledger, mounts=mounts)
        assert not (arch_dir_dest / "old_sub").exists()
        assert (arch_dir_dest / "nested" / "part.txt").read_text(
            encoding="utf-8"
        ) == "bundle-v2"

        sym_arch_dest = arch_root / "sym_bundle"
        os.symlink(ext_target, sym_arch_dest)
        archive(dir_art, sym_arch_dest, mode="copy", ledger=ledger, mounts=mounts)
        assert not sym_arch_dest.is_symlink()
        assert (sym_arch_dest / "nested" / "part.txt").read_text(
            encoding="utf-8"
        ) == "bundle-v2"
        assert precious_file.read_text(encoding="utf-8") == "precious-external-data"

        # 3) stage_verified_source overwriting symlink-to-directory + mid-copy failure leaves dest intact
        sym_stage_dest = stage_root / "staged_sym"
        os.symlink(ext_target, sym_stage_dest)
        stage_verified_source(
            dir_art,
            src_dir,
            sym_stage_dest,
            mode="copy",
            ledger=ledger,
            mounts=mounts,
        )
        assert not sym_stage_dest.is_symlink()
        assert (sym_stage_dest / "nested" / "part.txt").read_text(
            encoding="utf-8"
        ) == "bundle-v2"
        assert precious_file.read_text(encoding="utf-8") == "precious-external-data"

        existing_dest = stage_root / "keep_on_fail"
        existing_dest.mkdir()
        (existing_dest / "sentinel.txt").write_text("intact", encoding="utf-8")

        def _boom_copytree(*_args: object, **_kwargs: object) -> str:
            raise OSError("simulated mid-copy disk full")

        orig_copytree = shutil.copytree
        monkeypatch.setattr(shutil, "copytree", _boom_copytree)
        try:
            with pytest.raises(OSError, match="simulated mid-copy"):
                stage_verified_source(
                    dir_art,
                    src_dir,
                    existing_dest,
                    mode="copy",
                    ledger=ledger,
                    mounts=mounts,
                )
        finally:
            monkeypatch.setattr(shutil, "copytree", orig_copytree)

        assert (existing_dest / "sentinel.txt").read_text(encoding="utf-8") == "intact"
        assert list(stage_root.glob(".consist-tmp-*")) == []
        assert list(stage_root.glob(".consist-old-*")) == []

        # --- Part A5: archive(mode="move", site_context=None) child eviction, blake3 survivor, node scope isolation, tx rollback ---
        hpc_ctx = SiteContext(site="perlmutter", node_name="nid01")
        move_dir = ws_root / "hpc_dir_move"
        move_dir.mkdir()
        child_csv = move_dir / "households.csv"
        child_csv.write_text("hh_id\n1\n2\n", encoding="utf-8")
        sealed_hpc = seal(
            [
                PendingOutput(role="parent_dir", path=move_dir, is_dir=True),
                PendingOutput(role="child_csv", path=child_csv, driver="csv"),
            ],
            ledger=ledger,
            site_context=hpc_ctx,
            mounts=mounts,
        )
        # Call archive(..., mode="move", site_context=None) -> resolved.site='perlmutter' propagates!
        moved_hpc_dir = archive(
            sealed_hpc["parent_dir"],
            arch_root / "hpc_dir_moved",
            mode="move",
            site_context=None,
            ledger=ledger,
            mounts=mounts,
        )
        assert not move_dir.exists()
        assert (arch_root / "hpc_dir_moved" / "households.csv").exists()
        assert (
            ledger.query_location_current(
                site="perlmutter",
                uri="workspace://hpc_dir_move",
                include_evicted=False,
            )
            == []
        )
        assert (
            ledger.query_location_current(
                site="perlmutter",
                uri="workspace://hpc_dir_move/households.csv",
                include_evicted=False,
            )
            == []
        )
        loc_child_moved = locate(sealed_hpc["child_csv"], ledger=ledger, mounts=mounts)
        assert (
            loc_child_moved.path
            == (arch_root / "hpc_dir_moved" / "households.csv").resolve()
        )
        assert moved_hpc_dir.identity.anchor == sealed_hpc["parent_dir"].identity.anchor

        # blake3:file survivor eligibility in evict() (both fresh witness & stale witness via blake3 module)
        class _FakeBlake3Hasher:
            def __init__(self, data: bytes = b"") -> None:
                self._h = hashlib.blake2b(digest_size=32)
                if data:
                    self._h.update(data)

            def update(self, data: bytes) -> None:
                self._h.update(data)

            def hexdigest(self) -> str:
                return self._h.hexdigest()

        fake_blake3_mod = types.ModuleType("blake3")
        fake_blake3_mod.blake3 = _FakeBlake3Hasher  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "blake3", fake_blake3_mod)

        b3_ws = ws_root / "b3_data.bin"
        b3_arch = arch_root / "b3_data.bin"
        b3_payload = b"blake3-survivor-payload"
        b3_ws.write_bytes(b3_payload)
        b3_arch.write_bytes(b3_payload)
        b3_hex = _FakeBlake3Hasher(b3_payload).hexdigest()
        b3_anchor = Digest("blake3:file", b3_hex)
        b3_id = Identity(b3_anchor)
        obs_b3_ws = Observation(
            location=Location(uri="workspace://b3_data.bin"),
            identity=b3_id,
            access_driver="bin",
            witness=FileStat.capture(b3_ws),
            basis="computed",
            site="local",
        )
        # Stale witness on archive copy forces _surviving_row_is_fresh_on_disk to verify blake3:file bytes!
        stale_arch_stat = FileStat(
            size=len(b3_payload),
            mtime_ns=1_600_000_000_000_000_000,
            racy=False,
        )
        obs_b3_arch = Observation(
            location=Location(uri="archive://b3_data.bin"),
            identity=b3_id,
            access_driver="bin",
            witness=stale_arch_stat,
            basis="derived",
            site="local",
        )
        ledger.record_observation(obs_b3_ws, repr_kind="file")
        ledger.record_observation(obs_b3_arch, repr_kind="file")
        b3_art = Artifact(
            id=uuid.uuid4(),
            key="b3_art",
            driver="bin",
            identity=b3_id,
            hints=(obs_b3_ws, obs_b3_arch),
        )
        evicted_b3 = evict(
            b3_art,
            path=b3_ws,
            ledger=ledger,
            mounts=mounts,
        )
        assert len(evicted_b3) == 1
        assert not b3_ws.exists()
        assert b3_arch.exists()

        # Two node-local scopes sharing scratch://shared.parquet do NOT evict each other
        f_n1 = scratch_n1 / "shared.parquet"
        f_n2 = scratch_n2 / "shared.parquet"
        shared_bytes = b"node-local-shared-bytes"
        f_n1.write_bytes(shared_bytes)
        f_n2.write_bytes(shared_bytes)
        ctx_n1 = SiteContext(site="perlmutter", node_name="nid01")
        ctx_n2 = SiteContext(site="perlmutter", node_name="nid02")
        mounts_n1 = [
            *mounts,
            Mount("scratch", scratch_n1, scope="node", instance_id="inst-n1"),
        ]
        mounts_n2 = [
            *mounts,
            Mount("scratch", scratch_n2, scope="node", instance_id="inst-n2"),
        ]
        art_n1 = observe(
            f_n1,
            key="node_shared",
            ledger=ledger,
            site_context=ctx_n1,
            mounts=mounts_n1,
        )
        observe(
            f_n2,
            key="node_shared",
            ledger=ledger,
            site_context=ctx_n2,
            mounts=mounts_n2,
        )
        # nid02's copy is invisible to nid01, so require_surviving_copy=True refuses until global archive exists
        with pytest.raises(ConsistError, match="Refusing to evict last active copy"):
            evict(
                art_n1,
                uri="scratch://shared.parquet",
                site_context=ctx_n1,
                mounts=mounts_n1,
                ledger=ledger,
            )
        art_n1 = archive(
            art_n1,
            arch_root / "node_shared.parquet",
            mode="copy",
            site_context=ctx_n1,
            mounts=mounts_n1,
            ledger=ledger,
        )
        evict(
            art_n1,
            uri="scratch://shared.parquet",
            site_context=ctx_n1,
            mounts=mounts_n1,
            ledger=ledger,
        )
        assert not f_n1.exists()
        assert f_n2.exists()
        rem_n2 = ledger.query_location_current(
            site="perlmutter",
            uri="scratch://shared.parquet",
            include_evicted=False,
        )
        assert len(rem_n2) == 1
        assert rem_n2[0]["scope_key"] == "node:nid02"

        # archive(..., mode="move") rolls back DB writes and keeps src on disk if mid-transaction error occurs
        tx_src = ws_root / "tx_rollback.txt"
        tx_src.write_bytes(b"atomic-move-tx-payload")
        tx_art = observe(tx_src, key="tx_move", ledger=ledger, mounts=mounts)
        orig_rec_event = ledger.record_location_event

        def _fail_rec_event(*_a: object, **_kw: object) -> None:
            raise RuntimeError("simulated DB failure during move eviction")

        monkeypatch.setattr(ledger, "record_location_event", _fail_rec_event)
        try:
            with pytest.raises(RuntimeError, match="simulated DB failure"):
                archive(
                    tx_art,
                    arch_root / "tx_rollback_dest.txt",
                    mode="move",
                    ledger=ledger,
                    mounts=mounts,
                )
        finally:
            monkeypatch.setattr(ledger, "record_location_event", orig_rec_event)

        assert tx_src.exists()
        assert (
            ledger.query_location_current(
                uri="archive://tx_rollback_dest.txt",
                include_evicted=False,
            )
            == []
        )
        src_active = ledger.query_location_current(
            uri="workspace://tx_rollback.txt",
            include_evicted=False,
        )
        assert len(src_active) == 1 and src_active[0]["status"] == "active"

        # --- Part B5: Single-pass archive() on sha256:file & per-file CoW directory fallback ---
        sp_src = ws_root / "single_pass.bin"
        sp_src.write_bytes(b"single-pass-archive-bytes" * 100)
        os.utime(sp_src, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000))
        sp_art = observe(sp_src, key="sp_bin", ledger=ledger, mounts=mounts)

        pinned_calls = 0
        orig_pinned = engine_mod._hash_file_descriptor_pinned

        def _counting_pinned(p: Path) -> tuple[Digest, FileStat]:
            nonlocal pinned_calls
            pinned_calls += 1
            return orig_pinned(p)

        monkeypatch.setattr(
            engine_mod, "_hash_file_descriptor_pinned", _counting_pinned
        )
        try:
            sp_archived = archive(
                sp_art,
                arch_root / "single_pass.bin",
                mode="copy",
                ledger=ledger,
                mounts=mounts,
            )
        finally:
            monkeypatch.setattr(engine_mod, "_hash_file_descriptor_pinned", orig_pinned)
        assert pinned_calls == 0
        assert (arch_root / "single_pass.bin").read_bytes() == sp_src.read_bytes()
        assert sp_archived.identity.anchor == sp_art.identity.anchor

        # Mismatched source bytes detected in single-pass tee leaves existing dest untouched
        bad_dest = arch_root / "existing_good_dest.bin"
        bad_dest.write_bytes(b"keep-existing-dest")
        tampered_art = Artifact(
            id=sp_art.id,
            key=sp_art.key,
            driver=sp_art.driver,
            identity=Identity(Digest("sha256:file", "f" * 64)),
            hints=(
                Observation(
                    location=Location(uri="workspace://single_pass.bin"),
                    identity=Identity(Digest("sha256:file", "f" * 64)),
                    access_driver="bin",
                    witness=FileStat.capture(sp_src),
                    basis="computed",
                    site="local",
                ),
            ),
        )
        with pytest.raises(ValueError, match="failed digest verification"):
            archive(
                tampered_art,
                bad_dest,
                mode="copy",
                ledger=HintLedger(tampered_art.hints),
                mounts=mounts,
            )
        assert bad_dest.read_bytes() == b"keep-existing-dest"

        # Per-file CoW directory fallback in stage_verified_source (simulated Linux XFS/Btrfs)
        file_reflink_calls = 0

        def _file_only_reflink(s: Path, d: Path) -> bool:
            nonlocal file_reflink_calls
            if s.is_dir():
                return False
            file_reflink_calls += 1
            shutil.copy2(s, d)
            return True

        orig_try_reflink = engine_mod._try_reflink_clone
        monkeypatch.setattr(engine_mod, "_try_reflink_clone", _file_only_reflink)
        try:
            reflink_dir_dest = stage_root / "per_file_reflink_dir"
            stage_verified_source(
                dir_art,
                src_dir,
                reflink_dir_dest,
                mode="reflink",
                ledger=ledger,
                mounts=mounts,
            )
            assert file_reflink_calls >= 1
            assert (reflink_dir_dest / "nested" / "part.txt").read_text(
                encoding="utf-8"
            ) == "bundle-v2"

            auto_dir_dest = stage_root / "per_file_auto_dir"
            stage_verified_source(
                dir_art,
                src_dir,
                auto_dir_dest,
                mode="auto",
                ledger=ledger,
                mounts=mounts,
            )
            assert (auto_dir_dest / "nested" / "part.txt").read_text(
                encoding="utf-8"
            ) == "bundle-v2"
        finally:
            monkeypatch.setattr(engine_mod, "_try_reflink_clone", orig_try_reflink)

        # When neither directory nor file CoW is supported, mode="reflink" raises OSError while mode="auto" copies
        monkeypatch.setattr(engine_mod, "_try_reflink_clone", lambda _s, _d: False)
        try:
            with pytest.raises(OSError):
                stage_verified_source(
                    dir_art,
                    src_dir,
                    stage_root / "unsupported_reflink_dir",
                    mode="reflink",
                    ledger=ledger,
                    mounts=mounts,
                )
            fallback_auto_dir = stage_root / "fallback_auto_dir"
            stage_verified_source(
                dir_art,
                src_dir,
                fallback_auto_dir,
                mode="auto",
                ledger=ledger,
                mounts=mounts,
            )
            assert (fallback_auto_dir / "nested" / "part.txt").read_text(
                encoding="utf-8"
            ) == "bundle-v2"
        finally:
            monkeypatch.setattr(engine_mod, "_try_reflink_clone", orig_try_reflink)

        # --- Part B1: build_manifest_v1 single-walk symlink rejection & lock-free thread-local worker re-entrancy ---
        sym_tree = tmp_path / "sym_manifest_tree"
        sym_tree.mkdir()
        (sym_tree / "ok.txt").write_text("ok", encoding="utf-8")
        os.symlink(sym_tree / "ok.txt", sym_tree / "bad_sym.txt")
        with pytest.raises(ValueError, match="symlink"):
            build_manifest_v1(sym_tree, max_workers=2)
        (sym_tree / "bad_sym.txt").unlink()
        m_out = build_manifest_v1(sym_tree, max_workers=2)
        assert len(m_out["entries"]) == 1


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
