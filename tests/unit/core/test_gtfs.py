from __future__ import annotations

import zipfile
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from consist import canonicalize_gtfs_bundle, discover_gtfs_members
from consist.core.drivers import TABLE_DRIVERS, TableInfo
from consist.core.identity import IdentityManager
from consist.core.gtfs import hash_gtfs_feed


def _write_gtfs_feed(root: Path, *, inactive_stop_name: str = "Inactive Stop") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "agency_id": "A1",
                "agency_name": "Transit",
                "agency_url": "https://example.com",
                "agency_timezone": "America/Los_Angeles",
            }
        ]
    ).to_csv(root / "agency.txt", index=False)
    pd.DataFrame(
        [
            {
                "route_id": "R1",
                "agency_id": "A1",
                "route_short_name": "1",
                "route_type": 3,
            }
        ]
    ).to_csv(root / "routes.txt", index=False)
    pd.DataFrame(
        [
            {
                "service_id": "S1",
                "monday": 1,
                "tuesday": 0,
                "wednesday": 0,
                "thursday": 0,
                "friday": 0,
                "saturday": 0,
                "sunday": 0,
                "start_date": "20240101",
                "end_date": "20240101",
            },
            {
                "service_id": "S2",
                "monday": 0,
                "tuesday": 1,
                "wednesday": 0,
                "thursday": 0,
                "friday": 0,
                "saturday": 0,
                "sunday": 0,
                "start_date": "20240102",
                "end_date": "20240102",
            },
        ]
    ).to_csv(root / "calendar.txt", index=False)
    pd.DataFrame(
        [
            {
                "stop_id": "STOP_A",
                "stop_name": "Active Stop",
                "stop_lat": 37.0,
                "stop_lon": -122.0,
            },
            {
                "stop_id": "STOP_B",
                "stop_name": inactive_stop_name,
                "stop_lat": 37.1,
                "stop_lon": -122.1,
            },
        ]
    ).to_csv(root / "stops.txt", index=False)
    pd.DataFrame(
        [
            {"trip_id": "T1", "route_id": "R1", "service_id": "S1"},
            {"trip_id": "T2", "route_id": "R1", "service_id": "S2"},
        ]
    ).to_csv(root / "trips.txt", index=False)
    pd.DataFrame(
        [
            {
                "trip_id": "T1",
                "arrival_time": "08:00:00",
                "departure_time": "08:00:00",
                "stop_id": "STOP_A",
                "stop_sequence": 1,
            },
            {
                "trip_id": "T2",
                "arrival_time": "09:00:00",
                "departure_time": "09:00:00",
                "stop_id": "STOP_B",
                "stop_sequence": 1,
            },
        ]
    ).to_csv(root / "stop_times.txt", index=False)
    (root / "license.txt").write_text(
        "License text, not a GTFS table.", encoding="utf-8"
    )
    return root


def _zip_gtfs_feed(source_dir: Path, zip_path: Path) -> Path:
    with zipfile.ZipFile(zip_path, "w") as zf:
        for file_path in sorted(source_dir.iterdir()):
            zf.write(file_path, arcname=file_path.name)
    return zip_path


def test_gtfs_driver_discovers_and_loads_directory_and_zip(tmp_path: Path) -> None:
    feed_dir = _write_gtfs_feed(tmp_path / "feed_dir")
    feed_zip = _zip_gtfs_feed(feed_dir, tmp_path / "feed.zip")

    expected = {
        "agency.txt",
        "calendar.txt",
        "routes.txt",
        "stop_times.txt",
        "stops.txt",
        "trips.txt",
    }
    assert set(discover_gtfs_members(feed_dir)) == expected
    assert set(discover_gtfs_members(feed_zip)) == expected

    driver = TABLE_DRIVERS.get("gtfs")
    discovered = driver.discover(str(feed_zip))
    assert {info.table_path for info in discovered} == expected

    conn = duckdb.connect()
    relation = driver.load(
        TableInfo(
            role="trips",
            table_path="trips.txt",
            container_uri=str(feed_zip),
            driver="gtfs",
            schema_id=None,
        ),
        conn,
    )
    trips = relation.df()
    assert list(trips.columns) == ["trip_id", "route_id", "service_id"]
    assert trips.to_dict(orient="records") == [
        {"trip_id": "T1", "route_id": "R1", "service_id": "S1"},
        {"trip_id": "T2", "route_id": "R1", "service_id": "S2"},
    ]
    assert "feed_key" not in trips.columns

    with pytest.raises(ValueError, match="table_path"):
        driver.load(
            TableInfo(
                role="trips",
                table_path=None,
                container_uri=str(feed_zip),
                driver="gtfs",
                schema_id=None,
            ),
            conn,
        )


def test_gtfs_member_relation_normalizes_string_dtype_for_duckdb(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from consist.core import gtfs as gtfs_module

    df = pd.DataFrame(
        {
            "route_id": pd.Series(["R1"], dtype="string"),
            "agency_id": pd.Series(["A1"], dtype="string"),
            "route_short_name": pd.Series(["1"], dtype="string"),
            "route_type": [3],
        }
    )
    monkeypatch.setattr(gtfs_module, "load_gtfs_member_df", lambda *_: df)

    conn = duckdb.connect()
    relation = gtfs_module.load_gtfs_member_relation(
        "ignored",
        "routes.txt",
        conn,
    )

    assert relation.df().to_dict(orient="records") == [
        {
            "route_id": "R1",
            "agency_id": "A1",
            "route_short_name": "1",
            "route_type": 3,
        }
    ]


def test_gtfs_canonicalizer_hashes_bundle_and_service_slice(tmp_path: Path) -> None:
    feed_a = _write_gtfs_feed(tmp_path / "feed_a")
    feed_b = _write_gtfs_feed(
        tmp_path / "feed_b",
        inactive_stop_name="Changed Inactive Stop",
    )
    # Inactive-only change should not affect the selected service slice.
    pd.DataFrame(
        [
            {
                "trip_id": "T1",
                "arrival_time": "08:00:00",
                "departure_time": "08:00:00",
                "stop_id": "STOP_A",
                "stop_sequence": 1,
            },
            {
                "trip_id": "T2",
                "arrival_time": "09:30:00",
                "departure_time": "09:30:00",
                "stop_id": "STOP_B",
                "stop_sequence": 1,
            },
        ]
    ).to_csv(feed_b / "stop_times.txt", index=False)

    base = canonicalize_gtfs_bundle(
        [feed_a],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )
    inactive_variant = canonicalize_gtfs_bundle(
        [feed_b],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )

    assert base.source_bundle_hash
    assert base.service_slice_hash
    assert base.source_feed_hashes
    assert base.selected_tables["trips"]["feed_key"].tolist() == ["feed"]
    assert base.selected_tables["stop_times"]["feed_key"].tolist() == ["feed"]
    assert base.source_bundle_hash != inactive_variant.source_bundle_hash
    assert base.service_slice_hash == inactive_variant.service_slice_hash

    changed_active = _write_gtfs_feed(tmp_path / "feed_active_change")
    pd.DataFrame(
        [
            {"trip_id": "T1", "route_id": "R1", "service_id": "S1"},
            {"trip_id": "T2", "route_id": "R1", "service_id": "S1"},
        ]
    ).to_csv(changed_active / "trips.txt", index=False)

    inactive_variant = canonicalize_gtfs_bundle(
        [changed_active],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )
    active_variant = canonicalize_gtfs_bundle(
        [changed_active],
        identity=IdentityManager(),
        service_date="2024-01-02",
        feed_keys=["feed"],
    )

    assert base.service_slice_hash != inactive_variant.service_slice_hash
    assert inactive_variant.service_slice_hash != active_variant.service_slice_hash
    assert inactive_variant.manifest["feeds"][0]["selection"]["active_service_ids"] == [
        "S1"
    ]
    assert active_variant.manifest["feeds"][0]["selection"]["active_service_ids"] == [
        "S2"
    ]


def test_gtfs_canonicalizer_weekday_weekend_service_dates_change_slice(
    tmp_path: Path,
) -> None:
    feed = _write_gtfs_feed(tmp_path / "feed")
    pd.DataFrame(
        [
            {
                "service_id": "S1",
                "monday": 1,
                "tuesday": 0,
                "wednesday": 0,
                "thursday": 0,
                "friday": 0,
                "saturday": 0,
                "sunday": 0,
                "start_date": "20240101",
                "end_date": "20240101",
            },
            {
                "service_id": "S2",
                "monday": 0,
                "tuesday": 0,
                "wednesday": 0,
                "thursday": 0,
                "friday": 0,
                "saturday": 1,
                "sunday": 0,
                "start_date": "20240106",
                "end_date": "20240106",
            },
        ]
    ).to_csv(feed / "calendar.txt", index=False)

    weekday = canonicalize_gtfs_bundle(
        [feed],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )
    weekend = canonicalize_gtfs_bundle(
        [feed],
        identity=IdentityManager(),
        service_date="2024-01-06",
        feed_keys=["feed"],
    )

    assert weekday.service_slice_hash != weekend.service_slice_hash
    assert weekday.manifest["feeds"][0]["selection"]["active_service_ids"] == ["S1"]
    assert weekend.manifest["feeds"][0]["selection"]["active_service_ids"] == ["S2"]


def test_gtfs_canonicalizer_omits_all_null_extension_columns_from_ingest_rows(
    tmp_path: Path,
) -> None:
    feed = _write_gtfs_feed(tmp_path / "feed")
    routes = pd.read_csv(feed / "routes.txt", dtype=str)
    routes["publisher_extension"] = pd.NA
    routes.to_csv(feed / "routes.txt", index=False)

    result = canonicalize_gtfs_bundle(
        [feed],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )

    routes_spec = next(spec for spec in result.ingestables if spec.table_name == "routes")
    rows = list(routes_spec.materialize_rows("run"))

    assert "publisher_extension" in result.selected_tables["routes"].columns
    assert "publisher_extension" not in rows[0]


def test_tracker_canonicalize_gtfs_requires_active_run(
    gtfs_tracker, tmp_path: Path
) -> None:
    feed = _write_gtfs_feed(tmp_path / "feed")

    with pytest.raises(RuntimeError, match="active run"):
        gtfs_tracker.canonicalize_gtfs([feed], service_date="2024-01-01")


def test_tracker_canonicalize_gtfs_logs_manifest_metadata_and_ingests(
    gtfs_tracker, tmp_path: Path
) -> None:
    feed = _write_gtfs_feed(tmp_path / "feed")

    with gtfs_tracker.start_run("gtfs_canonicalize_unit", "gtfs"):
        result = gtfs_tracker.canonicalize_gtfs(
            [feed],
            service_date="2024-01-01",
            feed_keys=["feed"],
        )
        run = gtfs_tracker.current_consist.run

        assert result.manifest_artifact is not None
        assert result.manifest_artifact.key == "consist_gtfs_bundle"
        assert Path(result.manifest_artifact.path).exists()
        assert result.source_artifacts
        assert result.source_artifacts[0].meta["feed_key"] == "feed"
        assert "trips" in result.table_artifacts
        trips_artifact = result.table_artifacts["trips"]
        assert Path(trips_artifact.path).exists()
        assert trips_artifact.meta["gtfs_selected_table"] is True
        assert (
            trips_artifact.meta["gtfs_manifest_artifact_id"]
            == str(result.manifest_artifact.id)
        )
        assert trips_artifact.meta["gtfs_table_name"] == "trips"
        assert trips_artifact.meta["service_slice_hash"] == result.service_slice_hash
        assert run.meta["gtfs_source_bundle_hash"] == result.source_bundle_hash
        assert run.meta["gtfs_service_slice_hash"] == result.service_slice_hash
        assert (
            run.meta["gtfs_identity_manifest"]["service_slice_hash"]
            == result.service_slice_hash
        )

        trips_df = gtfs_tracker.load(trips_artifact).df()
        assert set(trips_df["feed_key"]) == {"feed"}
        assert set(trips_df["trip_id"]) == {"T1"}

        assert gtfs_tracker.engine is not None
        with gtfs_tracker.engine.begin() as connection:
            rows = connection.exec_driver_sql(
                """
                SELECT feed_key, trip_id
                FROM global_tables.trips
                ORDER BY trip_id
                """
            ).fetchall()
            route_rows = connection.exec_driver_sql(
                """
                SELECT feed_key, route_id, consist_artifact_id
                FROM global_tables.routes
                ORDER BY route_id
                """
            ).fetchall()

    assert rows == [("feed", "T1")]
    assert route_rows == [("feed", "R1", str(result.manifest_artifact.id))]


def test_tracker_canonicalize_gtfs_rejects_non_active_run_id(
    gtfs_tracker, tmp_path: Path
) -> None:
    feed = _write_gtfs_feed(tmp_path / "feed")

    with gtfs_tracker.start_run("gtfs_canonicalize_unit", "gtfs"):
        with pytest.raises(RuntimeError, match="run_id=.*active run"):
            gtfs_tracker.canonicalize_gtfs(
                [feed],
                service_date="2024-01-01",
                run_id="other",
            )


def test_gtfs_calendar_dates_selection_is_order_independent(tmp_path: Path) -> None:
    feed_add_then_remove = _write_gtfs_feed(tmp_path / "add_then_remove")
    pd.DataFrame(
        [
            {"service_id": "S1", "date": "20240101", "exception_type": 1},
            {"service_id": "S1", "date": "20240101", "exception_type": 2},
        ]
    ).to_csv(feed_add_then_remove / "calendar_dates.txt", index=False)

    feed_remove_then_add = _write_gtfs_feed(tmp_path / "remove_then_add")
    pd.DataFrame(
        [
            {"service_id": "S1", "date": "20240101", "exception_type": 2},
            {"service_id": "S1", "date": "20240101", "exception_type": 1},
        ]
    ).to_csv(feed_remove_then_add / "calendar_dates.txt", index=False)

    add_then_remove = canonicalize_gtfs_bundle(
        [feed_add_then_remove],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )
    remove_then_add = canonicalize_gtfs_bundle(
        [feed_remove_then_add],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )

    assert add_then_remove.manifest["feeds"][0]["selection"]["active_service_ids"] == []
    assert remove_then_add.manifest["feeds"][0]["selection"]["active_service_ids"] == []
    assert add_then_remove.service_slice_hash == remove_then_add.service_slice_hash
    assert add_then_remove.selected_tables["trips"].empty
    assert add_then_remove.selected_tables["stop_times"].empty


def test_gtfs_canonicalizer_zero_service_slice_filters_related_tables(
    tmp_path: Path,
) -> None:
    feed = _write_gtfs_feed(tmp_path / "feed")

    result = canonicalize_gtfs_bundle(
        [feed],
        identity=IdentityManager(),
        service_date="2024-01-03",
        feed_keys=["feed"],
    )

    assert result.service_slice_hash
    assert result.manifest["feeds"][0]["selection"]["active_service_ids"] == []
    assert result.selected_tables["trips"].empty
    assert result.selected_tables["stop_times"].empty
    assert result.selected_tables["stops"].empty


def test_gtfs_feed_hash_is_stable_across_source_names(tmp_path: Path) -> None:
    feed_a = _write_gtfs_feed(tmp_path / "rename_a")
    feed_b = _write_gtfs_feed(tmp_path / "rename_b")

    assert discover_gtfs_members(feed_a) == discover_gtfs_members(feed_b)
    assert hash_gtfs_feed(feed_a, discover_gtfs_members(feed_a)) == hash_gtfs_feed(
        feed_b, discover_gtfs_members(feed_b)
    )


def test_gtfs_identity_payload_is_stable_across_source_paths(tmp_path: Path) -> None:
    feed_a = _write_gtfs_feed(tmp_path / "identity_a")
    feed_b = _write_gtfs_feed(tmp_path / "identity_b")

    result_a = canonicalize_gtfs_bundle(
        [feed_a],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )
    result_b = canonicalize_gtfs_bundle(
        [feed_b],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )

    assert result_a.identity_payload == result_b.identity_payload


def test_gtfs_shapes_without_trip_shape_id_do_not_crash(tmp_path: Path) -> None:
    feed = _write_gtfs_feed(tmp_path / "shape-less")
    pd.DataFrame(
        [
            {
                "shape_id": "SHAPE_1",
                "shape_pt_lat": 37.0,
                "shape_pt_lon": -122.0,
                "shape_pt_sequence": 1,
            }
        ]
    ).to_csv(feed / "shapes.txt", index=False)

    result = canonicalize_gtfs_bundle(
        [feed],
        identity=IdentityManager(),
        service_date="2024-01-01",
        feed_keys=["feed"],
    )

    assert "shapes" not in result.selected_tables
    assert result.selected_tables["trips"].shape[0] == 1


def test_gtfs_bundle_membership_and_feed_key_namespacing(tmp_path: Path) -> None:
    feed_a = _write_gtfs_feed(tmp_path / "north")
    feed_b = _write_gtfs_feed(tmp_path / "south")

    identity_manager = IdentityManager()
    single = canonicalize_gtfs_bundle(
        [feed_a],
        identity=identity_manager,
        feed_keys=["north"],
    )
    bundled = canonicalize_gtfs_bundle(
        [feed_a, feed_b],
        identity=identity_manager,
        feed_keys=["north", "south"],
    )

    assert single.source_bundle_hash != bundled.source_bundle_hash
    assert len(bundled.selected_tables["trips"]) == 4
    assert set(
        zip(
            bundled.selected_tables["trips"]["feed_key"],
            bundled.selected_tables["trips"]["trip_id"],
        )
    ) == {
        ("north", "T1"),
        ("north", "T2"),
        ("south", "T1"),
        ("south", "T2"),
    }


def test_gtfs_explicit_duplicate_feed_keys_raise(tmp_path: Path) -> None:
    feed_a = _write_gtfs_feed(tmp_path / "dup_a")
    feed_b = _write_gtfs_feed(tmp_path / "dup_b")

    with pytest.raises(ValueError, match="feed_keys must be unique"):
        canonicalize_gtfs_bundle(
            [feed_a, feed_b],
            identity=IdentityManager(),
            feed_keys=["feed", "feed"],
        )
