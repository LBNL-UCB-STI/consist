import logging
import importlib
import json
import os
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest
import pandas as pd
from sqlalchemy import func, select
from sqlmodel import Session, SQLModel

from consist.core.config_canonicalization import (
    ArtifactSpec,
    CanonicalConfigIdentity,
    ConfigAdapterOptions,
    ConfigReference,
)
from consist.integrations.beam import BeamConfigAdapter, BeamConfigOverrides
from consist.integrations.beam.config_adapter import BeamReferencePolicy
from consist.integrations.beam.config_adapter import (
    _build_beam_canonicalization_snapshot,
)
from consist.integrations.beam.config_adapter import _load_config_tree
from consist.integrations.beam.config_adapter import _resolve_reference
from consist.integrations.beam.config_adapter import _resolves_under_config_root
from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink
from consist.types import CacheOptions, ExecutionOptions
from tests.helpers.beam_fixtures import build_beam_test_configs


def _write_gtfs_zip_feed(path: Path, *, route_short_name: str, service_id: str) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(
            "agency.txt",
            pd.DataFrame(
                [
                    {
                        "agency_id": "A1",
                        "agency_name": "Transit",
                        "agency_url": "https://example.com",
                        "agency_timezone": "America/Los_Angeles",
                    }
                ]
            ).to_csv(index=False),
        )
        zf.writestr(
            "routes.txt",
            pd.DataFrame(
                [
                    {
                        "route_id": "R1",
                        "agency_id": "A1",
                        "route_short_name": route_short_name,
                        "route_type": 3,
                    }
                ]
            ).to_csv(index=False),
        )
        zf.writestr(
            "calendar.txt",
            pd.DataFrame(
                [
                    {
                        "service_id": service_id,
                        "monday": 1,
                        "tuesday": 0,
                        "wednesday": 0,
                        "thursday": 0,
                        "friday": 0,
                        "saturday": 0,
                        "sunday": 0,
                        "start_date": "20240101",
                        "end_date": "20240101",
                    }
                ]
            ).to_csv(index=False),
        )
        zf.writestr(
            "stops.txt",
            pd.DataFrame(
                [
                    {
                        "stop_id": "STOP_A",
                        "stop_name": "Active Stop",
                        "stop_lat": 37.0,
                        "stop_lon": -122.0,
                    }
                ]
            ).to_csv(index=False),
        )
        zf.writestr(
            "trips.txt",
            pd.DataFrame(
                [
                    {
                        "trip_id": "T1",
                        "route_id": "R1",
                        "service_id": service_id,
                    }
                ]
            ).to_csv(index=False),
        )
        zf.writestr(
            "stop_times.txt",
            pd.DataFrame(
                [
                    {
                        "trip_id": "T1",
                        "arrival_time": "08:00:00",
                        "departure_time": "08:00:00",
                        "stop_id": "STOP_A",
                        "stop_sequence": 1,
                    }
                ]
            ).to_csv(index=False),
        )
        zf.writestr("license.txt", "license text that should be ignored\n")
    return path


def _write_gtfs_directory_feed(
    path: Path, *, route_short_name: str, service_id: str
) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "agency_id": "A1",
                "agency_name": "Transit",
                "agency_url": "https://example.com",
                "agency_timezone": "America/Los_Angeles",
            }
        ]
    ).to_csv(path / "agency.txt", index=False)
    pd.DataFrame(
        [
            {
                "route_id": "R1",
                "agency_id": "A1",
                "route_short_name": route_short_name,
                "route_type": 3,
            }
        ]
    ).to_csv(path / "routes.txt", index=False)
    pd.DataFrame(
        [
            {
                "service_id": service_id,
                "monday": 1,
                "tuesday": 0,
                "wednesday": 0,
                "thursday": 0,
                "friday": 0,
                "saturday": 0,
                "sunday": 0,
                "start_date": "20240101",
                "end_date": "20240101",
            }
        ]
    ).to_csv(path / "calendar.txt", index=False)
    pd.DataFrame(
        [
            {
                "stop_id": "STOP_A",
                "stop_name": "Active Stop",
                "stop_lat": 37.0,
                "stop_lon": -122.0,
            }
        ]
    ).to_csv(path / "stops.txt", index=False)
    pd.DataFrame(
        [
            {
                "trip_id": "T1",
                "route_id": "R1",
                "service_id": service_id,
            }
        ]
    ).to_csv(path / "trips.txt", index=False)
    pd.DataFrame(
        [
            {
                "trip_id": "T1",
                "arrival_time": "08:00:00",
                "departure_time": "08:00:00",
                "stop_id": "STOP_A",
                "stop_sequence": 1,
            }
        ]
    ).to_csv(path / "stop_times.txt", index=False)
    (path / "license.txt").write_text("license text that should be ignored\n")
    return path


def test_beam_models_register_tables(tracker):
    if tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")
    with tracker.engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS global_tables")
        SQLModel.metadata.create_all(
            connection,
            tables=[
                BeamConfigCache.__table__,
                BeamConfigIngestRunLink.__table__,
            ],
        )
    with Session(tracker.engine) as session:
        session.exec(select(func.count()).select_from(BeamConfigCache))


def test_beam_discover_includes_and_hash(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    assert canonical.primary_config == overlay_conf
    assert any(p.name == "common.conf" for p in canonical.config_files)
    assert canonical.content_hash


def test_beam_load_config_tree_applies_env_overrides_without_env_mutation(
    tmp_path: Path,
):
    config_path = tmp_path / "env.conf"
    config_path.write_text("value = ${TEST_OVERRIDE}\n", encoding="utf-8")

    with patch.object(os._Environ, "__setitem__", autospec=True) as setitem_mock:
        with patch.object(os._Environ, "pop", autospec=True) as pop_mock:
            tree = _load_config_tree(
                config_path,
                resolve=True,
                env_overrides={"TEST_OVERRIDE": "scoped-value"},
            )

    assert tree["value"] == "scoped-value"
    assert "TEST_OVERRIDE" not in tree
    assert setitem_mock.call_count == 0
    assert pop_mock.call_count == 0


def test_beam_canonicalize_artifacts_and_rows(tracker, tmp_path: Path, caplog):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_unit", "beam")
    with caplog.at_level(logging.WARNING):
        result = adapter.canonicalize(canonical, run=run, tracker=tracker)
    artifact_keys = {spec.key for spec in result.artifacts}
    assert any("base.conf" in key for key in artifact_keys)
    assert any("sample.csv" in key for key in artifact_keys)
    assert any("missing/does-not-exist.csv" in r.message for r in caplog.records)
    ingest_tables = {spec.table_name for spec in result.ingestables}
    assert "beam_config_cache" in ingest_tables
    assert "beam_config_ingest_run_link" in ingest_tables
    assert result.identity.adapter_name == "beam"
    assert result.identity.identity_hash
    assert result.canonicalization is not None
    assert (
        tuple(item.reference for item in result.canonicalization.references)
        == result.identity.references
    )
    assert all(
        key in artifact_keys
        for item in result.canonicalization.references
        for key in item.artifact_keys
    )
    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    assert refs_by_key["beam.inputDirectory"].identity_policy == "path_alias"
    assert refs_by_key["beam.inputDirectory"].delegated_artifact_keys
    assert (
        refs_by_key[
            "beam.agentsim.agents.vehicles.vehicleTypesFilePath"
        ].identity_policy
        == "content_hash"
    )
    missing_ref = refs_by_key["beam.agentsim.overridePath"]
    assert missing_ref.status == "missing_required"
    assert missing_ref.canonical_value is not None
    assert missing_ref.canonical_value.endswith("missing/does-not-exist.csv")
    assert any("beam.agentsim.overridePath" in r.message for r in caplog.records)


def test_beam_canonicalize_discovers_gtfs_bundle_from_r5_directory(
    gtfs_tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    gtfs_root = case_dir / "inputs" / "r5" / "sfbay-cbg5500-weakConn-network"
    gtfs_root.mkdir(parents=True, exist_ok=True)
    _write_gtfs_zip_feed(
        gtfs_root / "Caltrain.zip",
        route_short_name="CT",
        service_id="S1",
    )
    _write_gtfs_zip_feed(
        gtfs_root / "SF.zip",
        route_short_name="SF",
        service_id="S1",
    )
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.routing.baseDate = "2024-01-01"\n'
        + '\nbeam.routing.r5.directory = ${beam.inputDirectory}"/r5/sfbay-cbg5500-weakConn-network"\n',
        encoding="utf-8",
    )

    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=gtfs_tracker.identity)
    run = gtfs_tracker.begin_run("beam_gtfs_bundle_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=gtfs_tracker)

    gtfs_payload = result.identity.scalars["gtfs"]
    assert gtfs_payload["source_bundle_hash"]
    assert gtfs_payload["service_slice_hash"]
    assert len(gtfs_payload["source_feed_hashes"]) == 2
    assert gtfs_payload["manifest"]["service_date"] == "2024-01-01"
    assert gtfs_payload["manifest"]["feeds"][0]["feed_key"] == "Caltrain"
    assert {
        spec.table_name
        for spec in result.ingestables
        if spec.table_name != "beam_config_cache"
    } >= {"trips", "stop_times", "routes"}
    assert {
        spec.meta["config_role"]
        for spec in result.artifacts
        if spec.meta.get("config_role") == "gtfs_feed"
    } == {"gtfs_feed"}
    assert {
        spec.path.name
        for spec in result.artifacts
        if spec.meta.get("config_role") == "gtfs_feed"
    } == {
        "Caltrain.zip",
        "SF.zip",
    }
    assert {
        spec.driver
        for spec in result.artifacts
        if spec.meta.get("config_role") == "gtfs_feed"
    } == {"gtfs"}


def test_beam_canonicalize_selects_first_top_level_r5_osm_source(
    gtfs_tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    r5_root = case_dir / "inputs" / "r5" / "sfbay-cbg5500-weakConn-network"
    r5_root.mkdir(parents=True)
    selected_osm = r5_root / "001-network.VEX"
    ignored_osm = r5_root / "002-network.pbf"
    selected_osm.write_bytes(b"selected osm")
    ignored_osm.write_bytes(b"ignored osm")
    nested_osm = r5_root / "nested" / "000-network.pbf"
    nested_osm.parent.mkdir()
    nested_osm.write_bytes(b"nested osm")
    _write_gtfs_zip_feed(
        r5_root / "Caltrain.zip",
        route_short_name="CT",
        service_id="S1",
    )
    legacy_osm = case_dir / "inputs" / "legacy-network.pbf"
    legacy_osm.write_bytes(b"legacy osm")
    mapdb_path = case_dir / "inputs" / "r5" / "osm.mapdb"
    mapdb_path.write_bytes(b"generated cache")
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.routing.baseDate = "2024-01-01"\n'
        + '\nbeam.routing.r5.directory = ${beam.inputDirectory}"/r5/sfbay-cbg5500-weakConn-network"\n'
        + '\nbeam.routing.r5.osmFile = ${beam.inputDirectory}"/legacy-network.pbf"\n'
        + '\nbeam.routing.r5.osmMapdbFile = ${beam.inputDirectory}"/r5/osm.mapdb"\n',
        encoding="utf-8",
    )

    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=gtfs_tracker.identity)
    run = gtfs_tracker.begin_run("beam_r5_osm_source_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=gtfs_tracker)

    osm_spec = next(
        spec
        for spec in result.artifacts
        if spec.meta.get("config_role") == "r5_osm_source"
    )
    assert osm_spec.path == selected_osm
    assert osm_spec.meta == {
        "config_role": "r5_osm_source",
        "config_reference_key": "beam.routing.r5.directory",
        "selection_rule": "r5_top_level_osm_v1",
        "selected_filename": "001-network.VEX",
        "ignored_candidate_filenames": ["002-network.pbf"],
    }
    assert all(spec.path != legacy_osm for spec in result.artifacts)
    assert all(spec.path != mapdb_path for spec in result.artifacts)

    assert result.canonicalization is not None
    r5_reference = next(
        item
        for item in result.canonicalization.references
        if item.reference.config_key == "beam.routing.r5.directory"
    )
    assert osm_spec.key in r5_reference.artifact_keys
    osm_member = next(
        member
        for member in r5_reference.artifact_members
        if member.role == "r5_osm_source"
    )
    assert osm_member.resolved_path == selected_osm.resolve()
    assert osm_member.artifact_key == osm_spec.key
    assert osm_member.artifact_key in r5_reference.artifact_keys
    assert dict(osm_member.metadata) == {
        "selection_rule": "r5_top_level_osm_v1",
        "selected_filename": "001-network.VEX",
        "ignored_candidate_filenames": ("002-network.pbf",),
    }
    with pytest.raises(TypeError):
        osm_member.metadata["selection_rule"] = "other"
    gtfs_feed_keys = {
        spec.key
        for spec in result.artifacts
        if spec.meta.get("config_role") == "gtfs_feed"
    }
    assert gtfs_feed_keys <= set(r5_reference.artifact_keys)
    assert tuple(
        member
        for member in r5_reference.artifact_members
        if member.role == "r5_osm_source"
    ) == (osm_member,)


def test_beam_canonicalize_does_not_select_r5_osm_without_top_level_candidate(
    gtfs_tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    r5_root = case_dir / "inputs" / "r5" / "sfbay-cbg5500-weakConn-network"
    r5_root.mkdir(parents=True)
    nested_osm = r5_root / "nested" / "network.pbf"
    nested_osm.parent.mkdir()
    nested_osm.write_bytes(b"nested osm")
    _write_gtfs_zip_feed(
        r5_root / "Caltrain.zip",
        route_short_name="CT",
        service_id="S1",
    )
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.routing.baseDate = "2024-01-01"\n'
        + '\nbeam.routing.r5.directory = ${beam.inputDirectory}"/r5/sfbay-cbg5500-weakConn-network"\n',
        encoding="utf-8",
    )

    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=gtfs_tracker.identity)
    run = gtfs_tracker.begin_run("beam_r5_no_osm_source_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=gtfs_tracker)

    assert not any(
        spec.meta.get("config_role") == "r5_osm_source" for spec in result.artifacts
    )
    assert result.canonicalization is not None
    r5_reference = next(
        item
        for item in result.canonicalization.references
        if item.reference.config_key == "beam.routing.r5.directory"
    )
    assert r5_reference.artifact_members == ()


def test_beam_canonicalize_ignores_r5_sidecar_csv_and_discovers_zip_feeds(
    gtfs_tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    gtfs_root = case_dir / "inputs" / "r5" / "seattle-cbg120-ferry-weakConn-network"
    gtfs_root.mkdir(parents=True, exist_ok=True)
    (gtfs_root / "clipped_tazs.csv").write_text("taz,x\n1,2\n", encoding="utf-8")
    _write_gtfs_zip_feed(
        gtfs_root / "seattle_gtfs.zip",
        route_short_name="SEA",
        service_id="S1",
    )
    _write_gtfs_zip_feed(
        gtfs_root / "kitsap_transit.zip",
        route_short_name="KIT",
        service_id="S1",
    )
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.routing.baseDate = "2024-01-01"\n'
        + '\nbeam.routing.r5.directory = ${beam.inputDirectory}"/r5/seattle-cbg120-ferry-weakConn-network"\n',
        encoding="utf-8",
    )

    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=gtfs_tracker.identity)
    run = gtfs_tracker.begin_run("beam_gtfs_zip_discovery_with_sidecars", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=gtfs_tracker)

    gtfs_payload = result.identity.scalars["gtfs"]

    assert set(gtfs_payload["source_feed_hashes"]) == {
        "seattle_gtfs",
        "kitsap_transit",
    }
    assert len(gtfs_payload["manifest"]["feeds"]) == 2
    assert all(
        "clipped_tazs.csv" not in feed["member_names"]
        for feed in gtfs_payload["manifest"]["feeds"]
    )
    assert {
        spec.path.name
        for spec in result.artifacts
        if spec.meta.get("config_role") == "gtfs_feed"
    } == {"seattle_gtfs.zip", "kitsap_transit.zip"}


def test_beam_canonicalize_ingests_gtfs_tables(gtfs_tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    gtfs_root = case_dir / "inputs" / "r5" / "sfbay-cbg5500-weakConn-network"
    gtfs_root.mkdir(parents=True, exist_ok=True)
    _write_gtfs_zip_feed(
        gtfs_root / "Caltrain.zip",
        route_short_name="CT",
        service_id="S1",
    )
    _write_gtfs_zip_feed(
        gtfs_root / "SF.zip",
        route_short_name="SF",
        service_id="S1",
    )
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.routing.baseDate = "2024-01-01"\n'
        + '\nbeam.routing.r5.directory = ${beam.inputDirectory}"/r5/sfbay-cbg5500-weakConn-network"\n',
        encoding="utf-8",
    )

    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    run = gtfs_tracker.begin_run("beam_gtfs_ingest_unit", "beam")
    gtfs_tracker.canonicalize_config(adapter, [case_dir], run=run)
    gtfs_tracker.end_run()

    bundle_artifact = gtfs_tracker.get_artifact("consist_gtfs_bundle")
    assert bundle_artifact is not None
    assert bundle_artifact.driver == "gtfs"

    if gtfs_tracker.engine is None:
        raise AssertionError("Tracker engine missing; DB tests require DuckDB.")

    with gtfs_tracker.engine.begin() as connection:
        count = connection.exec_driver_sql(
            "SELECT COUNT(*) FROM global_tables.trips"
        ).scalar_one()
    assert count == 2


def test_beam_canonicalization_snapshot_omits_unlogged_gtfs_directory_delegate(
    tmp_path: Path,
):
    root_dir = tmp_path / "beam_case"
    gtfs_root = root_dir / "inputs" / "r5"
    feed_path = gtfs_root / "Caltrain.zip"
    gtfs_root.mkdir(parents=True)
    feed_path.write_bytes(b"feed")

    reference = ConfigReference(
        config_key="beam.routing.r5.directory",
        raw_value=str(gtfs_root),
        canonical_value="config:inputs/r5",
        status="resolved",
        required=True,
        identity_policy="delegated_to_artifacts",
        delegated_artifact_keys=("config:beam_case/inputs/r5",),
    )
    identity = CanonicalConfigIdentity(
        adapter_name="beam",
        adapter_version="0.1",
        primary_config=None,
        identity_hash="identity",
        references=(reference,),
    )
    artifacts = {
        gtfs_root: ArtifactSpec(
            path=gtfs_root,
            key="consist_gtfs_bundle",
            direction="input",
            meta={"config_role": "gtfs_bundle"},
            driver="gtfs",
        ),
        feed_path: ArtifactSpec(
            path=feed_path,
            key="config:inputs/r5/Caltrain.zip",
            direction="input",
            meta={"config_role": "gtfs_feed"},
            driver="gtfs",
        ),
    }

    snapshot = _build_beam_canonicalization_snapshot(
        identity=identity,
        artifacts_by_path=artifacts,
        root_dirs=[root_dir],
        r5_directory=gtfs_root,
    )

    assert snapshot.references[0].artifact_keys == (
        "consist_gtfs_bundle",
        "config:inputs/r5/Caltrain.zip",
    )


def test_beam_canonicalize_prefers_directory_root_gtfs_bundle(
    gtfs_tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    gtfs_root = case_dir / "inputs" / "r5" / "sfbay-cbg5500-weakConn-network"
    _write_gtfs_directory_feed(
        gtfs_root,
        route_short_name="ROOT",
        service_id="S1",
    )
    nested_feed = gtfs_root / "nested" / "ignored"
    _write_gtfs_directory_feed(
        nested_feed,
        route_short_name="NESTED",
        service_id="S1",
    )
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.routing.baseDate = "2024-01-01"\n'
        + '\nbeam.routing.r5.directory = ${beam.inputDirectory}"/r5/sfbay-cbg5500-weakConn-network"\n',
        encoding="utf-8",
    )

    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=gtfs_tracker.identity)
    run = gtfs_tracker.begin_run("beam_gtfs_directory_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=gtfs_tracker)

    gtfs_payload = result.identity.scalars["gtfs"]
    assert gtfs_payload["manifest"]["feeds"][0]["feed_key"] == gtfs_root.name
    assert len(gtfs_payload["source_feed_hashes"]) == 1
    gtfs_bundle_spec = next(
        spec for spec in result.artifacts if spec.key == "consist_gtfs_bundle"
    )
    assert gtfs_bundle_spec.meta.get("gtfs_bundle")
    assert gtfs_bundle_spec.driver == "gtfs"


def test_beam_canonicalize_normalizes_path_aliases(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    external_root = tmp_path / "machine_local"
    external_root.mkdir()
    data_path = external_root / "scenario.csv"
    data_path.write_text("id,value\n1,2\n", encoding="utf-8")
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + f'\nbeam.agentsim.aliasInput = "{data_path}"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_alias_unit", "beam")
    result = adapter.canonicalize(
        canonical,
        run=run,
        tracker=tracker,
        options=ConfigAdapterOptions(path_aliases={"local_inputs": external_root}),
    )

    alias_ref = next(
        ref
        for ref in result.identity.references
        if ref.config_key == "beam.agentsim.aliasInput"
    )
    assert alias_ref.identity_policy == "content_hash"
    assert alias_ref.canonical_value == "local_inputs/scenario.csv"
    assert alias_ref.hash
    assert result.canonicalization is not None
    alias_item = next(
        item
        for item in result.canonicalization.references
        if item.reference.config_key == "beam.agentsim.aliasInput"
    )
    assert alias_item.reference is alias_ref
    assert alias_item.resolved_path == data_path
    assert alias_item.artifact_keys
    assert result.identity.scalars["options"]["path_aliases"] == {
        "local_inputs": external_root.resolve().as_posix()
    }
    tracker.end_run()

    data_path.write_text("id,value\n1,3\n", encoding="utf-8")
    run_changed = tracker.begin_run("beam_alias_unit_changed", "beam")
    changed = adapter.canonicalize(
        canonical,
        run=run_changed,
        tracker=tracker,
        options=ConfigAdapterOptions(path_aliases={"local_inputs": external_root}),
    )
    assert changed.identity.identity_hash != result.identity.identity_hash


def test_beam_canonicalize_ignores_output_runtime_paths(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    existing_events = tmp_path / "runtime" / "events.xml.gz"
    existing_events.parent.mkdir()
    existing_events.write_text("events\n", encoding="utf-8")
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + f'\nbeam.outputs.eventsFilePath = "{existing_events}"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical_a = adapter.discover([case_dir], identity=tracker.identity)
    run_a = tracker.begin_run("beam_output_identity_a", "beam")
    result_a = adapter.canonicalize(canonical_a, run=run_a, tracker=tracker)
    tracker.end_run()

    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8").replace(
            str(existing_events),
            str(tmp_path / "runtime_other" / "events.xml.gz"),
        ),
        encoding="utf-8",
    )
    canonical_b = adapter.discover([case_dir], identity=tracker.identity)
    run_b = tracker.begin_run("beam_output_identity_b", "beam")
    result_b = adapter.canonicalize(canonical_b, run=run_b, tracker=tracker)

    output_ref = next(
        ref
        for ref in result_b.identity.references
        if ref.config_key == "beam.outputs.eventsFilePath"
    )
    assert output_ref.identity_policy == "output_or_runtime_ignored"
    assert output_ref.required is False
    assert result_b.identity.identity_hash == result_a.identity.identity_hash


def test_beam_canonicalize_does_not_treat_scalar_input_keys_as_paths(
    tracker,
    tmp_path: Path,
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.inputs.networkMode = "car"\n'
        + '\nbeam.agentsim.vehicles.assignment = "household"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_scalar_path_key_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=tracker)

    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    assert "beam.inputs.networkMode" not in refs_by_key
    assert "beam.agentsim.vehicles.assignment" not in refs_by_key


def test_beam_canonicalize_keeps_file_format_scalars_out_of_references(
    tracker,
    tmp_path: Path,
    caplog,
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.exchange.scenario.fileFormat = "parquet"\n'
        + '\nmatsim.modules.controler.eventsFileFormat = "xml"\n'
        + '\nmatsim.modules.controler.overwriteFiles = "overwriteExistingFiles"\n'
        + '\nmatsim.modules.controler.overwriteExistingFiles = "overwriteExistingFiles"\n'
        + '\nbeam.router.skim.activity-sim-skimmer.fileBaseName = "activitySimODSkims"\n'
        + '\nbeam.router.skim.drive-time-skimmer.fileBaseName = "skimsTravelTimeObservedVsSimulated"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(
        primary_config=overlay_conf,
        reference_policies={
            "beam.agentsim.overridePath": BeamReferencePolicy(
                identity_policy="ignored",
                required=False,
                reason="dormant_test_reference",
            )
        },
    )
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_scalar_formats_unit", "beam")
    with caplog.at_level(logging.WARNING):
        result = adapter.canonicalize(
            canonical,
            run=run,
            tracker=tracker,
            strict=True,
        )

    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    for scalar_key in (
        "beam.exchange.scenario.fileFormat",
        "matsim.modules.controler.eventsFileFormat",
        "matsim.modules.controler.overwriteFiles",
        "matsim.modules.controler.overwriteExistingFiles",
        "beam.router.skim.activity-sim-skimmer.fileBaseName",
        "beam.router.skim.drive-time-skimmer.fileBaseName",
    ):
        assert scalar_key not in refs_by_key
        assert scalar_key not in caplog.text


def test_beam_canonicalize_keeps_multiline_scalars_out_of_references(
    tracker,
    tmp_path: Path,
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + """
beam.sim.metric.collector.metrics = \"\"\"
beam-run, beam-iteration, beam-map-envelope,
beam-run-households, beam-run-population-size,
ride-hail-waiting-time, ride-hail-waiting-time-map, ride-hail-trip-distance
\"\"\"
""",
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(
        primary_config=overlay_conf,
        reference_policies={
            "beam.agentsim.overridePath": BeamReferencePolicy(
                identity_policy="ignored",
                required=False,
                reason="dormant_test_reference",
            )
        },
    )
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_multiline_scalar_unit", "beam")
    result = adapter.canonicalize(
        canonical,
        run=run,
        tracker=tracker,
        strict=True,
    )

    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    assert "beam.sim.metric.collector.metrics" not in refs_by_key
    assert refs_by_key["beam.inputDirectory"].status == "resolved"
    assert (
        refs_by_key["beam.agentsim.agents.vehicles.vehicleTypesFilePath"].status
        == "resolved"
    )
    assert refs_by_key["beam.agentsim.overridePath"].status == "missing_ignored"


def test_beam_canonicalize_keeps_long_comma_scalars_out_of_references(
    tracker,
    tmp_path: Path,
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    event_list = ",".join(f"PersonSyntheticEvent{index}" for index in range(40))
    assert len(event_list) > 512
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + f'\nbeam.outputs.events.eventsToWrite = "{event_list}"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_long_comma_scalar_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=tracker)

    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    assert "beam.outputs.events.eventsToWrite" not in refs_by_key


def test_beam_multiline_scalar_changes_scalar_identity_not_reference_identity(
    tracker,
    tmp_path: Path,
):
    results = []
    for suffix, metric_value in (
        ("a", "beam-run, beam-iteration"),
        ("b", "beam-run, beam-iteration, ride-hail-waiting-time"),
    ):
        case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / suffix)
        overlay_conf.write_text(
            overlay_conf.read_text(encoding="utf-8")
            + f'\nbeam.sim.metric.collector.metrics = """\n{metric_value}\n"""\n',
            encoding="utf-8",
        )
        adapter = BeamConfigAdapter(
            primary_config=overlay_conf,
            reference_policies={
                "beam.agentsim.overridePath": BeamReferencePolicy(
                    identity_policy="ignored",
                    required=False,
                    reason="dormant_test_reference",
                )
            },
        )
        canonical = adapter.discover([case_dir], identity=tracker.identity)
        run = tracker.begin_run(f"beam_scalar_identity_{suffix}", "beam")
        results.append(
            adapter.canonicalize(
                canonical,
                run=run,
                tracker=tracker,
                strict=True,
            )
        )
        tracker.end_run()

    assert results[0].identity.scalar_hash != results[1].identity.scalar_hash
    assert results[0].identity.reference_hash == results[1].identity.reference_hash


def test_beam_explicit_policy_overrides_scalar_path_prefilter(
    tracker,
    tmp_path: Path,
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    event_list = "PersonArrivalEvent,PersonDepartureEvent,ActivityEndEvent"
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + f'\nbeam.outputs.events.eventsToWrite = "{event_list}"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(
        primary_config=overlay_conf,
        reference_policies={
            "beam.outputs.events.eventsToWrite": BeamReferencePolicy(
                identity_policy="ignored",
                required=False,
                reason="explicit_event_scalar",
            ),
            "beam.agentsim.overridePath": BeamReferencePolicy(
                identity_policy="ignored",
                required=False,
                reason="dormant_test_reference",
            ),
        },
    )
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_explicit_filtered_scalar_policy_unit", "beam")
    result = adapter.canonicalize(
        canonical,
        run=run,
        tracker=tracker,
        strict=True,
    )

    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    event_ref = refs_by_key["beam.outputs.events.eventsToWrite"]
    assert event_ref.raw_value == event_list
    assert event_ref.identity_policy == "ignored"
    assert event_ref.status == "missing_ignored"
    assert event_ref.reason == "explicit_event_scalar"


def test_beam_path_probe_errors_fail_closed(tmp_path: Path):
    with patch.object(Path, "exists", side_effect=OSError("file name too long")):
        assert _resolves_under_config_root("candidate.csv", [tmp_path]) is False

    with patch.object(Path, "resolve", side_effect=OSError("file name too long")):
        assert _resolve_reference("candidate.csv", [tmp_path]) is None


def test_beam_canonicalize_does_not_use_key_only_path_heuristics_by_default(
    tracker,
    tmp_path: Path,
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.agentsim.customFile = "scenario-a"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(
        primary_config=overlay_conf,
        reference_policies={
            "beam.agentsim.overridePath": BeamReferencePolicy(
                identity_policy="ignored",
                required=False,
                reason="dormant_test_reference",
            )
        },
    )
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_default_no_heuristic_refs_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)

    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    assert "beam.agentsim.customFile" not in refs_by_key
    assert result.identity.scalars["options"]["allow_heuristic_refs"] is False


def test_beam_canonicalize_can_enable_key_based_path_heuristics(
    tracker,
    tmp_path: Path,
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.agentsim.customFile = "scenario-a"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(primary_config=overlay_conf, allow_heuristic_refs=True)
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_enable_heuristic_refs_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=tracker)

    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    assert refs_by_key["beam.agentsim.customFile"].status == "missing_required"
    assert result.identity.scalars["options"]["allow_heuristic_refs"] is True


def test_beam_canonicalize_supports_explicit_reference_policy(tracker, tmp_path: Path):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(
        primary_config=overlay_conf,
        reference_policies={
            "beam.agentsim.overridePath": BeamReferencePolicy(
                identity_policy="ignored",
                required=False,
                reason="dormant_test_reference",
            )
        },
    )
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_policy_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)

    override_ref = next(
        ref
        for ref in result.identity.references
        if ref.config_key == "beam.agentsim.overridePath"
    )
    assert override_ref.identity_policy == "ignored"
    assert override_ref.status == "missing_ignored"
    assert override_ref.reason == "dormant_test_reference"


def test_beam_canonicalize_explicit_reference_policy_forces_bare_scalar(
    tracker,
    tmp_path: Path,
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    overlay_conf.write_text(
        overlay_conf.read_text(encoding="utf-8")
        + '\nbeam.inputs.networkMode = "car"\n'
        + '\nbeam.router.skim.activity-sim-skimmer.fileBaseName = "activitySimODSkims"\n',
        encoding="utf-8",
    )
    adapter = BeamConfigAdapter(
        primary_config=overlay_conf,
        reference_policies={
            "beam.inputs.networkMode": BeamReferencePolicy(
                identity_policy="ignored",
                required=False,
                reason="explicit_scalar_policy",
            ),
            "beam.router.skim.activity-sim-skimmer.fileBaseName": BeamReferencePolicy(
                identity_policy="output_or_runtime_ignored",
                required=False,
                reason="explicit_runtime_basename",
            ),
            "beam.agentsim.overridePath": BeamReferencePolicy(
                identity_policy="ignored",
                required=False,
                reason="dormant_test_reference",
            ),
        },
    )
    canonical = adapter.discover([case_dir], identity=tracker.identity)
    run = tracker.begin_run("beam_explicit_scalar_policy_unit", "beam")
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)

    refs_by_key = {ref.config_key: ref for ref in result.identity.references}
    scalar_ref = refs_by_key["beam.inputs.networkMode"]
    assert scalar_ref.raw_value == "car"
    assert scalar_ref.identity_policy == "ignored"
    assert scalar_ref.status == "missing_ignored"
    assert scalar_ref.reason == "explicit_scalar_policy"

    basename_ref = refs_by_key["beam.router.skim.activity-sim-skimmer.fileBaseName"]
    assert basename_ref.raw_value == "activitySimODSkims"
    assert basename_ref.identity_policy == "output_or_runtime_ignored"
    assert basename_ref.status == "missing_ignored"
    assert basename_ref.reason == "explicit_runtime_basename"


def test_beam_materialize_overrides(tracker, tmp_path: Path):
    ConfigFactory = importlib.import_module("pyhocon").ConfigFactory

    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    overrides = BeamConfigOverrides(
        values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.75}
    )
    output_dir = tmp_path / "materialized"
    canonical = adapter.materialize(
        [case_dir],
        overrides,
        output_dir=output_dir,
        identity=tracker.identity,
        strict=True,
    )
    config = ConfigFactory.parse_file(str(canonical.primary_config), resolve=True)
    assert config.get("beam.agentsim.agentSampleSizeAsFractionOfPopulation") == 0.75


def test_beam_materialize_from_plan_uses_config_dirs(tracker, tmp_path: Path):
    ConfigFactory = importlib.import_module("pyhocon").ConfigFactory

    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)
    plan = tracker.prepare_config(adapter, [case_dir])
    overrides = BeamConfigOverrides(
        values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.9}
    )
    output_dir = tmp_path / "materialized_plan"
    canonical = adapter.materialize_from_plan(
        plan,
        overrides,
        output_dir=output_dir,
        identity=tracker.identity,
        strict=True,
    )
    config = ConfigFactory.parse_file(str(canonical.primary_config), resolve=True)
    assert config.get("beam.agentsim.agentSampleSizeAsFractionOfPopulation") == 0.9


def test_beam_run_with_config_overrides_hit_miss_behavior(tracker, tmp_path: Path):
    adapter = BeamConfigAdapter(primary_config=Path("overlay.conf"))
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case")

    calls: list[Path] = []

    def step(config_dir: Path) -> None:
        calls.append(config_dir)
        assert config_dir.is_dir()
        assert (config_dir / "overlay.conf").exists()
        assert (config_dir / "base.conf").exists()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.35}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="beam_override_run",
        model="beam",
        config={"calibration_step": "sample_size"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.35}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="beam_override_run",
        model="beam",
        config={"calibration_step": "sample_size"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_c = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.7}
        ),
        output_dir=tmp_path / "materialized_overrides",
        fn=step,
        name="beam_override_run",
        model="beam",
        config={"calibration_step": "sample_size"},
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert run_c.cache_hit is False
    assert len(calls) == 2


def test_beam_run_with_config_overrides_additive_identity_inputs_and_auto_metadata(
    tracker, tmp_path: Path
):
    adapter = BeamConfigAdapter(primary_config=Path("overlay.conf"))
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_auto")
    manual_dep = tmp_path / "beam_manual_identity.txt"
    manual_dep.write_text("beam=true\n", encoding="utf-8")
    calls: list[Path] = []

    def step(config_dir: Path) -> None:
        calls.append(config_dir)
        assert (config_dir / "overlay.conf").exists()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_auto",
        fn=step,
        name="beam_override_auto_identity",
        model="beam",
        identity_inputs=[("manual_dep", manual_dep)],
        identity_label="beam_config",
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_auto",
        fn=step,
        name="beam_override_auto_identity",
        model="beam",
        identity_inputs=[("manual_dep", manual_dep)],
        identity_label="beam_config",
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert len(calls) == 1

    for result in (run_a, run_b):
        digest_map = result.run.meta.get("consist_hash_inputs")
        assert isinstance(digest_map, dict)
        assert "manual_dep" in digest_map
        assert "beam_config" in digest_map
        metadata = result.run.meta.get("resolved_config_identity")
        assert metadata == {
            "mode": "auto",
            "adapter": "beam",
            "label": "beam_config",
            "path": metadata["path"],
            "digest": digest_map["beam_config"],
        }
        assert isinstance(metadata["path"], str)
        assert Path(metadata["path"]).is_dir()
        record = tracker.get_run_record(result.run.id)
        assert record is not None
        assert record.run.meta.get("resolved_config_identity") == metadata

    latest_snapshot = tracker.fs.run_dir / "consist.json"
    latest_payload = json.loads(latest_snapshot.read_text(encoding="utf-8"))
    latest_meta = (latest_payload.get("run") or {}).get("meta") or {}
    assert latest_meta.get("resolved_config_identity") == run_b.run.meta.get(
        "resolved_config_identity"
    )


def test_beam_run_with_config_overrides_off_mode_runtime_kwargs_identity_behavior(
    tracker, tmp_path: Path
):
    adapter = BeamConfigAdapter(primary_config=Path("overlay.conf"))
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_off")
    manual_dep = tmp_path / "beam_manual_identity_off.txt"
    manual_dep.write_text("beam=off\n", encoding="utf-8")

    runtime_output_a = tmp_path / "beam_runtime_identity_off_a"
    runtime_output_b = tmp_path / "beam_runtime_identity_off_b"
    runtime_output_c = tmp_path / "beam_runtime_identity_on_c"
    runtime_output_d = tmp_path / "beam_runtime_identity_on_d"
    runtime_output_a.mkdir(parents=True)
    runtime_output_b.mkdir(parents=True)
    runtime_output_c.mkdir(parents=True)
    runtime_output_d.mkdir(parents=True)
    (runtime_output_c / "identity.txt").write_text("c\n", encoding="utf-8")
    (runtime_output_d / "identity.txt").write_text("d\n", encoding="utf-8")

    calls: list[tuple[Path, Path]] = []

    def step(config_dir: Path, output_dir: Path) -> None:
        calls.append((config_dir, output_dir))
        assert (config_dir / "overlay.conf").exists()

    run_a = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="beam_override_off_identity",
        model="beam",
        identity_inputs=[("manual_dep", manual_dep)],
        resolved_config_identity="off",
        identity_label="beam_config",
        runtime_kwargs={"output_dir": runtime_output_a},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_b = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="beam_override_off_identity",
        model="beam",
        identity_inputs=[("manual_dep", manual_dep)],
        resolved_config_identity="off",
        identity_label="beam_config",
        runtime_kwargs={"output_dir": runtime_output_b},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_c = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="beam_override_off_identity",
        model="beam",
        identity_inputs=[
            ("manual_dep", manual_dep),
            ("runtime_output_dir", runtime_output_c),
        ],
        resolved_config_identity="off",
        identity_label="beam_config",
        runtime_kwargs={"output_dir": runtime_output_c},
        cache_options=CacheOptions(cache_mode="reuse"),
    )
    run_d = tracker.run_with_config_overrides(
        adapter=adapter,
        base_config_dirs=[case_dir],
        base_primary_config=overlay_conf,
        overrides=BeamConfigOverrides(values={}),
        output_dir=tmp_path / "materialized_off",
        fn=step,
        name="beam_override_off_identity",
        model="beam",
        identity_inputs=[
            ("manual_dep", manual_dep),
            ("runtime_output_dir", runtime_output_d),
        ],
        resolved_config_identity="off",
        identity_label="beam_config",
        runtime_kwargs={"output_dir": runtime_output_d},
        cache_options=CacheOptions(cache_mode="reuse"),
    )

    assert run_a.cache_hit is False
    assert run_b.cache_hit is True
    assert run_c.cache_hit is False
    assert run_d.cache_hit is False
    assert len(calls) == 3

    for result in (run_a, run_b, run_c, run_d):
        digest_map = result.run.meta.get("consist_hash_inputs")
        assert isinstance(digest_map, dict)
        assert "manual_dep" in digest_map
        assert "beam_config" not in digest_map
        metadata = result.run.meta.get("resolved_config_identity")
        assert metadata == {
            "mode": "off",
            "adapter": "beam",
            "label": "beam_config",
            "path": None,
            "digest": None,
        }


def test_beam_run_with_config_overrides_rejects_multiple_base_selectors(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_dual")
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    with pytest.raises(ValueError, match="exactly one base selector"):
        tracker.run_with_config_overrides(
            adapter=adapter,
            base_run_id="baseline",
            base_config_dirs=[case_dir],
            overrides=BeamConfigOverrides(values={}),
            output_dir=tmp_path / "materialized_dual_selector",
            fn=lambda: None,
            name="beam_override_dual_selector",
            model="beam",
        )


def test_beam_run_with_config_overrides_respects_explicit_runtime_kwargs(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_manual")
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    base_run = tracker.begin_run(
        "beam_override_base_manual",
        "beam",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    explicit_config_dir = tmp_path / "manual_runtime_config"
    explicit_config_dir.mkdir(parents=True)
    seen: list[Path] = []

    def step(config_dir: Path) -> None:
        seen.append(config_dir)

    tracker.run_with_config_overrides(
        adapter=BeamConfigAdapter(),
        base_run_id=base_run.id,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.2}
        ),
        output_dir=tmp_path / "materialized_manual_runtime",
        fn=step,
        name="beam_override_manual_runtime",
        model="beam",
        execution_options=ExecutionOptions(
            runtime_kwargs={"config_dir": explicit_config_dir}
        ),
    )

    assert seen == [explicit_config_dir]


def test_beam_run_with_config_overrides_supports_custom_runtime_kwarg_mapping(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_custom")
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    base_run = tracker.begin_run(
        "beam_override_base_custom",
        "beam",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    seen: list[Path] = []

    def step(config_root: Path) -> None:
        seen.append(config_root)
        assert (config_root / "overlay.conf").exists()

    tracker.run_with_config_overrides(
        adapter=BeamConfigAdapter(),
        base_run_id=base_run.id,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.15}
        ),
        output_dir=tmp_path / "materialized_custom_runtime",
        fn=step,
        name="beam_override_custom_runtime",
        model="beam",
        override_runtime_kwargs={"config_root": "selected_root_dir"},
    )

    assert len(seen) == 1


def test_beam_run_with_config_overrides_merges_top_level_runtime_kwargs(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path / "base_case_alias")
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    base_run = tracker.begin_run(
        "beam_override_base_alias",
        "beam",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    runtime_output_dir = tmp_path / "runtime_output_dir"
    seen: list[tuple[Path, Path]] = []

    def step(config_dir: Path, output_dir: Path) -> None:
        seen.append((config_dir, output_dir))

    tracker.run_with_config_overrides(
        adapter=BeamConfigAdapter(),
        base_run_id=base_run.id,
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.21}
        ),
        output_dir=tmp_path / "materialized_runtime_alias",
        fn=step,
        name="beam_override_runtime_alias",
        model="beam",
        runtime_kwargs={"output_dir": runtime_output_dir},
    )

    assert len(seen) == 1
    assert seen[0][1] == runtime_output_dir
    assert (seen[0][0] / "overlay.conf").exists()


def test_beam_run_with_config_overrides_rejects_dual_runtime_kwargs_sources(
    tracker, tmp_path: Path
):
    case_dir, overlay_conf, _ = build_beam_test_configs(
        tmp_path / "base_case_runtime_conflict"
    )
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    base_run = tracker.begin_run(
        "beam_override_base_runtime_conflict",
        "beam",
        cache_mode="overwrite",
    )
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    with pytest.raises(
        ValueError,
        match="runtime_kwargs= and execution_options.runtime_kwargs=",
    ):
        tracker.run_with_config_overrides(
            adapter=BeamConfigAdapter(),
            base_run_id=base_run.id,
            overrides=BeamConfigOverrides(
                values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.2}
            ),
            output_dir=tmp_path / "materialized_runtime_conflict",
            fn=lambda: None,
            name="beam_override_runtime_conflict",
            model="beam",
            runtime_kwargs={"output_dir": tmp_path / "out_a"},
            execution_options=ExecutionOptions(
                runtime_kwargs={"output_dir": tmp_path / "out_b"}
            ),
        )
