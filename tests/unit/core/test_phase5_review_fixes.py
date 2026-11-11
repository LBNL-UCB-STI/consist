from __future__ import annotations

from datetime import datetime, timezone
import gc
import hashlib
from pathlib import Path
import pickle
from uuid import uuid4

import duckdb
import pandas as pd
from pydantic import TypeAdapter
import pytest

import consist
from consist.artifact import Artifact
from consist.core.materialize import (
    _detach_artifact_for_hydration,
    materialize_ingested_artifact_from_db,
)
from consist.core.persistence import _infer_repr_kind
from consist.core.tracker import Tracker
from consist.digest import Digest, Identity
from consist.integrations.ibis import (
    _CACHED_BACKENDS,
    ibis_connection,
    suspend_tracker_ibis_connection,
)
from consist.location import (
    Location,
    Observation,
    format_hot_db_member,
    matches_hot_db_info,
    parse_hot_db_member,
    strip_hot_db_member_scope,
)


def _make_identity(seed: int = 1) -> Identity:
    return Identity(Digest.parse(f"sha256:file:{seed:064x}"))


def test_a1_hot_db_member_parsing_and_artifact_stripping() -> None:
    art_id = uuid4()
    other_art_id = uuid4()
    run_id = "run_alpha"

    info_art = parse_hot_db_member(
        "db://global_tables/trips",
        format_hot_db_member("trips", artifact_id=art_id),
    )
    assert info_art is not None
    assert info_art.table_name == "trips"
    assert info_art.scoped_artifact_id == str(art_id)
    assert info_art.scoped_run_id is None
    assert matches_hot_db_info(info_art, artifact_id=art_id, run_id=run_id) == (
        True,
        False,
    )
    assert matches_hot_db_info(info_art, artifact_id=other_art_id, run_id=run_id) == (
        False,
        False,
    )

    info_v1_art = parse_hot_db_member(
        "db://global_tables/trips",
        f"trips?consist_artifact_id={art_id}",
    )
    assert info_v1_art is not None
    assert info_v1_art.scoped_artifact_id == str(art_id)
    assert info_v1_art.scoped_run_id is None
    assert matches_hot_db_info(info_v1_art, artifact_id=art_id, run_id=run_id) == (
        True,
        False,
    )
    assert matches_hot_db_info(
        info_v1_art, artifact_id=other_art_id, run_id=run_id
    ) == (False, False)

    info_v1_run = parse_hot_db_member(
        "db://global_tables/trips",
        f"trips?consist_run_id={run_id}",
    )
    assert info_v1_run is not None
    assert info_v1_run.scoped_artifact_id is None
    assert info_v1_run.scoped_run_id == run_id
    assert matches_hot_db_info(info_v1_run, artifact_id=art_id, run_id=run_id) == (
        True,
        True,
    )
    assert matches_hot_db_info(info_v1_run, artifact_id=art_id, run_id="other_run") == (
        False,
        False,
    )

    # Unrecognized query parameters must never match as unscoped observations.
    info_unknown = parse_hot_db_member(
        "db://global_tables/trips", "trips?unexpected=123"
    )
    assert info_unknown is not None
    assert info_unknown.has_unrecognized_query is True
    assert matches_hot_db_info(info_unknown, artifact_id=art_id, run_id=run_id) == (
        False,
        False,
    )

    assert (
        strip_hot_db_member_scope(
            "db://global_tables/trips", f"trips?consist_run_id={run_id}"
        )
        == "trips"
    )

    ident = _make_identity(10)
    obs = Observation(
        location=Location(
            uri="db://global_tables/trips",
            member=f"trips?consist_run_id={run_id}",
        ),
        identity=ident,
        access_driver="duckdb_table",
        observed_at_ns=1,
    )
    art = Artifact(
        id=art_id,
        key="trips",
        identity=ident,
        driver="h5_table",
        run_id=run_id,
        hints=(obs,),
    )
    assert art.member == "trips"
    with pytest.warns(DeprecationWarning):
        assert art.table_path == "trips"
    assert art.primary_location is not None
    assert art.primary_location.member == "trips"

    adapter = TypeAdapter(Artifact)
    dumped = adapter.dump_python(art)
    assert dumped["member"] == "trips"
    restored = adapter.validate_python(dumped)
    assert restored.hints[0].location.member == f"trips?consist_run_id={run_id}"


def test_a1_active_hot_table_checks_hints_before_sql(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    df = pd.DataFrame({"x": [1, 2]})
    with tracker.start_run("hint_first_run", model="sim"):
        art = tracker.log_dataframe(df, key="items")
        tracker.ingest(art, data=df)

    hydrated = tracker.get_artifacts_for_run("hint_first_run").outputs["items"]
    assert hydrated.is_ingested is True

    assert tracker.ledger is not None

    def _forbid_locate_current(*_args: object, **_kwargs: object) -> tuple[()]:
        raise AssertionError(
            "locate_current should not be queried when hints already hold a matching db:// observation"
        )

    monkeypatch.setattr(tracker.ledger, "locate_current", _forbid_locate_current)
    assert hydrated._active_hot_table_name()[0] == "items"

    # Another artifact sharing the anchor with a mismatched ?consist_artifact_id hint must not match.
    other_obs = Observation(
        location=Location(
            uri="db://global_tables/items",
            member=f"items?consist_artifact_id={uuid4()}",
        ),
        identity=hydrated.identity,
        access_driver="duckdb_table",
        observed_at_ns=999,
    )
    mismatched_art = Artifact(
        id=uuid4(),
        key="items",
        identity=hydrated.identity,
        driver="parquet",
        run_id="other_run",
        hints=(other_obs,),
    )
    assert mismatched_art._active_hot_table_info(None)[0] is None
    tracker.close()


def test_a1_v1_run_scoped_hot_loading_and_views(tmp_path: Path) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    ledger = tracker.ledger
    assert ledger is not None

    now = datetime.now(timezone.utc)
    ledger.record_run(run_id="v1_run_a", model_name="sim", status="completed")
    ledger.record_run(run_id="v1_run_b", model_name="sim", status="completed")

    anchor = "legacy-v1:unverified:" + ("a" * 64)
    art_a_id = uuid4()
    other_art_id = uuid4()
    ledger.record_content(anchor, "table", basis="declared", created_at=now)
    with ledger.exec_cursor() as cur:
        cur.execute(
            "INSERT INTO main.artifact_schema (id, summary_json, created_at) VALUES (?, ?, ?)",
            ["legacy_schema", "{}", now],
        )
    ledger.record_artifact(
        artifact_id=art_a_id,
        key="legacy_concept",
        driver="parquet",
        anchor=anchor,
        run_id="v1_run_a",
        schema_id="legacy_schema",
        meta={},
        created_at=now,
    )
    ledger.record_run_binding(
        run_id="v1_run_a",
        direction="output",
        occurrence_index=0,
        slot="legacy_concept",
        artifact_id=art_a_id,
        role="legacy_concept",
        anchor=anchor,
    )
    ledger.record_observation(
        Observation(
            location=Location(
                uri="db://global_tables/legacy_metrics",
                member="legacy_metrics?consist_run_id=v1_run_a",
            ),
            identity=Identity(Digest.parse(anchor)),
            access_driver="duckdb_table",
            basis="migrated_v1",
            observed_at_ns=100,
        ),
        repr_kind=None,
    )

    with ledger.exec_cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS global_tables")
        cur.execute(
            "CREATE TABLE global_tables.legacy_metrics ("
            "consist_artifact_id VARCHAR, consist_run_id VARCHAR, val INTEGER)"
        )
        cur.executemany(
            "INSERT INTO global_tables.legacy_metrics VALUES (?, ?, ?)",
            [
                (None, "v1_run_a", 42),
                (str(other_art_id), "v1_run_a", 999),
                (None, "v1_run_b", 777),
            ],
        )

    art_a = tracker.get_artifacts_for_run("v1_run_a").outputs["legacy_concept"]
    assert art_a.is_ingested is True

    df_direct = art_a.as_df()
    assert df_direct["val"].tolist() == [42]

    df_api = consist.load_df(art_a, tracker=tracker, db_fallback="always")
    assert df_api["val"].tolist() == [42]

    dest = tmp_path / "hydrated_legacy.parquet"
    materialize_ingested_artifact_from_db(
        artifact=art_a,
        tracker=tracker,
        destination=dest,
    )
    df_mat = pd.read_parquet(dest)
    assert df_mat["val"].tolist() == [42]

    tracker.create_grouped_view("v_legacy_grouped", schema_id="legacy_schema")
    df_grouped = tracker.query_df("SELECT val FROM v_legacy_grouped")
    assert df_grouped["val"].tolist() == [42]

    tracker.create_view("v_legacy_extra", "legacy_concept")
    df_view = tracker.query_df("SELECT val FROM v_legacy_extra")
    assert df_view["val"].tolist() == [42]
    tracker.close()


def test_a1_infer_repr_kind_prefers_cold_hints_and_handles_db_only() -> None:
    ident = _make_identity(20)
    db_obs = Observation(
        location=Location(
            uri="db://global_tables/tbl",
            member=f"tbl?artifact_id={uuid4()}",
        ),
        identity=ident,
        access_driver="duckdb_table",
        observed_at_ns=2,
    )
    file_obs = Observation(
        location=Location(uri="outputs://data.parquet"),
        identity=ident,
        access_driver="parquet",
        observed_at_ns=1,
    )
    member_obs = Observation(
        location=Location(uri="outputs://store.h5", member="/group/tbl"),
        identity=ident,
        access_driver="h5_table",
        observed_at_ns=1,
    )

    cold_and_hot_file = Artifact(
        id=uuid4(),
        key="k1",
        identity=ident,
        driver="parquet",
        hints=(db_obs, file_obs),
    )
    assert _infer_repr_kind(cold_and_hot_file) == "file"

    cold_and_hot_member = Artifact(
        id=uuid4(),
        key="k2",
        identity=ident,
        driver="h5_table",
        hints=(db_obs, member_obs),
    )
    assert _infer_repr_kind(cold_and_hot_member) == "member"

    db_only_art = Artifact(
        id=uuid4(),
        key="k3",
        identity=ident,
        driver="duckdb_table",
        hints=(db_obs,),
    )
    assert _infer_repr_kind(db_only_art) == "table"


def test_a1_hydrate_artifact_rows_sweep_preserves_cold_and_per_run_db_hints(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    shared_df = pd.DataFrame({"step": [1, 2], "pop": [10.0, 12.0]})

    for idx in range(7):
        run_id = f"sweep_run_{idx}"
        with tracker.start_run(run_id, model="sweep_sim", config={"seed": idx}):
            out_path = tracker.run_artifact_dir() / "series.parquet"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            shared_df.to_parquet(out_path, index=False)
            art = tracker.log_artifact(out_path, key="series", direction="output")
            if idx < 6:
                tracker.ingest(art, data=shared_df)

    # Check both cold SQL hydration and warm memoized hydration across all 7 runs.
    for pass_idx in range(2):
        if pass_idx == 1 and tracker.ledger is not None:
            # Prime memoized observations on the shared anchor via locate_current.
            sample = tracker.get_artifacts_for_run("sweep_run_0").outputs["series"]
            tracker.ledger.locate_current([sample.identity.anchor])

        for idx in range(7):
            hydrated = tracker.get_artifacts_for_run(f"sweep_run_{idx}").outputs[
                "series"
            ]
            cold_uris = [
                obs.location.uri
                for obs in hydrated.hints
                if not obs.location.uri.startswith("db://")
            ]
            assert cold_uris, f"Cold observations crowded out on run {idx}"
            if idx < 6:
                assert hydrated.is_ingested is True
                assert hydrated.as_df()["pop"].tolist() == [10.0, 12.0]
            else:
                assert hydrated.is_ingested is False

    tracker.close()


def test_a2_artifact_tracker_weakref_isolation_detachment_and_gc(
    tmp_path: Path,
) -> None:
    assert "_bound_trackers" not in vars(consist.artifact)

    dir_a = tmp_path / "mounts_a"
    dir_b = tmp_path / "mounts_b"
    dir_a.mkdir()
    dir_b.mkdir()

    payload = b"shared_artifact_bytes_payload"
    (dir_a / "result.parquet").write_bytes(payload)
    (dir_b / "result.parquet").write_bytes(payload)
    hex_digest = hashlib.sha256(payload).hexdigest()

    tracker_a = Tracker(
        run_dir=tmp_path / "runs_a",
        db_path=None,
        mounts={"outputs": str(dir_a)},
    )
    tracker_b = Tracker(
        run_dir=tmp_path / "runs_b",
        db_path=None,
        mounts={"outputs": str(dir_b)},
    )

    shared_id = uuid4()
    ident = Identity(Digest.parse(f"sha256:file:{hex_digest}"))
    obs = Observation(
        location=Location(uri="outputs://result.parquet"),
        identity=ident,
        access_driver="parquet",
        observed_at_ns=1,
    )
    art_a = Artifact(
        id=shared_id,
        key="result",
        identity=ident,
        driver="parquet",
        hints=(obs,),
    ).bind_tracker(tracker_a)
    art_b = Artifact(
        id=shared_id,
        key="result",
        identity=ident,
        driver="parquet",
        hints=(obs,),
    ).bind_tracker(tracker_b)

    assert art_a.path == (dir_a / "result.parquet").resolve()
    assert art_b.path == (dir_b / "result.parquet").resolve()
    assert art_a.path != art_b.path

    assert art_a.with_meta(tag="v1").resolve_bound_tracker() is tracker_a
    assert art_a.with_hints((obs,)).resolve_bound_tracker() is tracker_a
    assert art_a.with_schema("schema_1").resolve_bound_tracker() is tracker_a

    detached = _detach_artifact_for_hydration(
        art_a, hydrated_path=tmp_path / "local.parquet"
    )
    assert detached.resolve_bound_tracker() is None

    unpickled = pickle.loads(pickle.dumps(art_a))
    assert unpickled.resolve_bound_tracker() is None

    tracker_b.close()
    tracker_a.close()
    del tracker_a
    gc.collect()
    assert art_a.resolve_bound_tracker() is None


def test_a3_suspend_for_subprocesses_keyboard_interrupt_and_lazy_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    ledger = tracker.ledger
    assert ledger is not None

    # 1. KeyboardInterrupt inside suspended block still reopens the ledger cleanly.
    with pytest.raises(KeyboardInterrupt):
        with ledger.suspend_for_subprocesses():
            raise KeyboardInterrupt()

    assert not ledger._closed
    assert not ledger._suspended_needs_reopen
    assert tracker.query_df("SELECT 1 AS ok")["ok"].tolist() == [1]

    # 2. If reopen retries time out while a lingering worker holds the lock,
    # _suspended_needs_reopen stays True and subsequent access lazily reopens.
    real_connect = duckdb.connect
    fail_reopen = True

    def _flaky_connect(*args: object, **kwargs: object) -> duckdb.DuckDBPyConnection:
        if fail_reopen:
            raise duckdb.IOException("Simulated lingering worker lock")
        return real_connect(*args, **kwargs)  # type: ignore[arg-type]

    times = iter([100.0, 121.0])
    monkeypatch.setattr(duckdb, "connect", _flaky_connect)
    monkeypatch.setattr("consist.store.ledger.time.monotonic", lambda: next(times))

    with pytest.raises(duckdb.IOException, match="Simulated lingering worker lock"):
        with ledger.suspend_for_subprocesses():
            pass

    assert ledger._suspended_needs_reopen is True
    fail_reopen = False
    monkeypatch.setattr(duckdb, "connect", real_connect)

    # Lazy reopen on subsequent connection / query_df access succeeds!
    assert tracker.query_df("SELECT 7 AS val")["val"].tolist() == [7]
    assert ledger._suspended_needs_reopen is False
    assert ledger._closed is False

    # 3. suspend_tracker_ibis_connection(disconnect=True) evicts & closes cached backend.
    _ = ibis_connection(tracker)
    assert tracker in _CACHED_BACKENDS
    with suspend_tracker_ibis_connection(tracker, disconnect=False):
        pass
    assert tracker in _CACHED_BACKENDS
    with suspend_tracker_ibis_connection(tracker, disconnect=True):
        assert tracker not in _CACHED_BACKENDS
    assert tracker not in _CACHED_BACKENDS

    tracker.close()


def test_a4_load_from_db_intra_tx_visibility_and_closed_ledger_fallback(
    tmp_path: Path,
) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    df = pd.DataFrame({"val": [10, 20]})
    with tracker.start_run("tx_load_run", model="sim"):
        art = tracker.log_dataframe(df, key="numbers")
        ingested = tracker.ingest(art, data=df)

    # Remove the cold parquet file so consist.load_df routes through _load_from_db ("Ghost Mode").
    Path(art.path).unlink()

    ledger = tracker.ledger
    assert ledger is not None

    # Uncommitted row inserted inside an active transaction must be visible via _tx_cursor
    # without raising "Cannot allocate an independent cursor() inside an active StoreLedger.transaction()".
    with pytest.raises(RuntimeError, match="rollback_sentinel"):
        with ledger.transaction() as cur:
            col_rows = cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'global_tables' AND table_name = 'numbers'"
            ).fetchall()
            cols = [str(r[0]) for r in col_rows]
            row_map = {
                "val": 30,
                "consist_artifact_id": str(ingested.id),
                "consist_run_id": "tx_load_run",
            }
            insert_cols = [c for c in cols if c in row_map]
            placeholders = ", ".join("?" for _ in insert_cols)
            cur.execute(
                f"INSERT INTO global_tables.numbers ({', '.join(insert_cols)}) VALUES ({placeholders})",
                [row_map[c] for c in insert_cols],
            )
            in_tx_df = consist.load_df(ingested, tracker=tracker, db_fallback="always")
            assert sorted(in_tx_df["val"].tolist()) == [10, 20, 30]
            raise RuntimeError("rollback_sentinel")

    # After rollback and closing the ledger, load_df falls back to read_only duckdb.connect(db_path).
    ledger.close()
    assert ledger._closed is True
    fallback_df = consist.load_df(ingested, tracker=tracker, db_fallback="always")
    assert sorted(fallback_df["val"].tolist()) == [10, 20]


def test_a5_pivot_facets_default_namespace_none(tmp_path: Path) -> None:
    tracker = Tracker(run_dir=tmp_path / "runs", db_path=tmp_path / "prov.duckdb")
    with tracker.start_run("facet_run", model="sim", facet={"alpha": 0.25}):
        pass

    q = consist.pivot_facets(keys=["alpha"])
    df = tracker.query_df(q.sql, q.params)
    assert len(df) == 1
    assert df["run_id"].iloc[0] == "facet_run"
    assert float(df["alpha"].iloc[0]) == pytest.approx(0.25)
    tracker.close()
