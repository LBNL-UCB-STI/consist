from __future__ import annotations

from pathlib import Path

from sqlmodel import Session, select

from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV
from consist.tools import queries


def test_log_output_persists_artifact_facet_and_kv_index(
    tracker, run_dir: Path
) -> None:
    out = run_dir / "linkstats_iter2.parquet"
    out.write_text("data")

    with tracker.start_run("run_artifact_facet", "beam", year=2030, iteration=7):
        artifact = tracker.log_output(
            out,
            key="linkstats_iter2",
            facet={
                "artifact_family": "linkstats_unmodified_phys_sim_iter_parquet",
                "year": 2030,
                "iteration": 7,
                "beam_sub_iteration": 0,
                "phys_sim_iteration": 2,
            },
            facet_schema_version="v1",
            facet_index=True,
        )

    assert artifact.meta["artifact_facet_id"]
    assert artifact.meta["artifact_facet_namespace"] == "beam"
    assert artifact.meta["artifact_facet_schema_version"] == "v1"

    kv_rows = tracker.get_artifact_kv(artifact)
    keys = {row.key_path for row in kv_rows}
    assert "phys_sim_iteration" in keys
    assert "artifact_family" in keys

    matches = queries.find_artifacts_by_params(
        tracker,
        params=["beam.phys_sim_iteration=2", "beam.year>=2030", "beam.year<=2030"],
        key_prefix="linkstats",
        artifact_family_prefix="linkstats_unmodified",
    )
    assert len(matches) == 1
    assert matches[0]["key"] == "linkstats_iter2"
    assert matches[0]["facet_schema_version"] == "v1"


def test_log_artifacts_facets_are_deduplicated_by_json_hash(
    tracker, run_dir: Path
) -> None:
    a_path = run_dir / "a.parquet"
    b_path = run_dir / "b.parquet"
    a_path.write_text("a")
    b_path.write_text("b")

    facet_payload = {
        "artifact_family": "linkstats_unmodified_phys_sim_iter_parquet",
        "year": 2030,
        "iteration": 7,
        "beam_sub_iteration": 0,
        "phys_sim_iteration": 3,
    }

    with tracker.start_run("run_bulk_artifact_facets", "beam"):
        tracker.log_artifacts(
            {"a": a_path, "b": b_path},
            facets_by_key={"a": facet_payload, "b": facet_payload},
            facet_schema_versions_by_key={"a": "1", "b": "1"},
            facet_index=True,
        )

    assert tracker.engine is not None
    with Session(tracker.engine) as session:
        facets = session.exec(select(ArtifactFacet)).all()
        kv_rows = session.exec(select(ArtifactKV)).all()

    assert len(facets) == 1
    assert len({row.artifact_id for row in kv_rows}) == 2


def test_log_output_uses_single_transaction_for_faceted_artifacts(
    tracker, run_dir: Path, monkeypatch
) -> None:
    out = run_dir / "combined_facet.parquet"
    out.write_text("data")

    combined_calls = 0
    sync_calls = 0
    facet_calls = 0

    original_combined = tracker.db.sync_artifact_with_facet_bundle
    original_sync = tracker.db.sync_artifact
    original_facet = tracker.db.persist_artifact_facet_bundle

    def counting_combined(*args, **kwargs):
        nonlocal combined_calls
        combined_calls += 1
        return original_combined(*args, **kwargs)

    def counting_sync(*args, **kwargs):
        nonlocal sync_calls
        sync_calls += 1
        return original_sync(*args, **kwargs)

    def counting_facet(*args, **kwargs):
        nonlocal facet_calls
        facet_calls += 1
        return original_facet(*args, **kwargs)

    monkeypatch.setattr(
        tracker.db, "sync_artifact_with_facet_bundle", counting_combined
    )
    monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync)
    monkeypatch.setattr(tracker.db, "persist_artifact_facet_bundle", counting_facet)

    with tracker.start_run("run_combined_artifact_facet", "beam"):
        tracker.log_output(
            out,
            key="combined_facet",
            facet={"artifact_family": "beam_output", "iteration": 1},
            facet_index=True,
        )

    assert combined_calls == 1
    assert sync_calls == 0
    assert facet_calls == 0


def test_log_output_without_facet_keeps_legacy_sync_path(
    tracker, run_dir: Path, monkeypatch
) -> None:
    out = run_dir / "plain_output.parquet"
    out.write_text("data")

    combined_calls = 0
    sync_calls = 0

    original_combined = tracker.db.sync_artifact_with_facet_bundle
    original_sync = tracker.db.sync_artifact

    def counting_combined(*args, **kwargs):
        nonlocal combined_calls
        combined_calls += 1
        return original_combined(*args, **kwargs)

    def counting_sync(*args, **kwargs):
        nonlocal sync_calls
        sync_calls += 1
        return original_sync(*args, **kwargs)

    monkeypatch.setattr(
        tracker.db, "sync_artifact_with_facet_bundle", counting_combined
    )
    monkeypatch.setattr(tracker.db, "sync_artifact", counting_sync)

    with tracker.start_run("run_plain_output", "beam"):
        tracker.log_output(out, key="plain_output")

    assert combined_calls == 0
    assert sync_calls == 1


def test_register_artifact_facet_parser_supports_legacy_keys(
    tracker, run_dir: Path
) -> None:
    tracker.register_artifact_facet_parser(
        "legacy_linkstats_",
        lambda key: {
            "artifact_family": "legacy_linkstats",
            "phys_sim_iteration": int(key.rsplit("_", 1)[-1]),
        },
    )

    out = run_dir / "legacy.parquet"
    out.write_text("legacy")

    with tracker.start_run("run_parser_hook", "beam"):
        tracker.log_output(
            out,
            key="legacy_linkstats_4",
            facet_index=True,
        )

    matches = queries.find_artifacts_by_params(
        tracker,
        params=["beam.phys_sim_iteration=4"],
        artifact_family_prefix="legacy_",
    )
    assert len(matches) == 1
    assert matches[0]["key"] == "legacy_linkstats_4"
