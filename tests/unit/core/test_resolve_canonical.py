from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

import consist
from consist.integrations.activitysim import ActivitySimConfigAdapter, ConfigOverrides
from consist.integrations.beam import BeamConfigAdapter, BeamConfigOverrides
from consist.models.run import Run, RunResult
from tests.helpers.activitysim_fixtures import build_activitysim_test_configs
from tests.helpers.beam_fixtures import build_beam_test_configs


def _insert_run(
    tracker,
    run_id: str,
    *,
    model_name: str = "test_model",
    meta: dict | None = None,
) -> Run:
    now = datetime.now(timezone.utc)
    run = Run(
        id=run_id,
        model_name=model_name,
        config_hash="cfg_hash",
        git_hash="git_hash",
        status="completed",
        meta=meta or {},
        created_at=now,
        updated_at=now,
        started_at=now,
        ended_at=now,
    )
    tracker.db.sync_run(run)
    return run


def test_run_and_run_result_cache_properties():
    producer = Run(
        id="producer_1",
        model_name="m",
        config_hash=None,
        git_hash=None,
        meta={},
    )
    assert producer.is_cache_hit is False
    assert producer.cache_source_run_id is None

    hit = Run(
        id="hit_1",
        model_name="m",
        config_hash=None,
        git_hash=None,
        meta={"cache_hit": True, "cache_source": "producer_1"},
    )
    assert hit.is_cache_hit is True
    assert hit.cache_source_run_id == "producer_1"

    empty_source = Run(
        id="hit_empty",
        model_name="m",
        config_hash=None,
        git_hash=None,
        meta={"cache_hit": True, "cache_source": ""},
    )
    assert empty_source.is_cache_hit is True
    assert empty_source.cache_source_run_id is None

    res_producer = RunResult(run=producer, cache_hit=False)
    assert res_producer.is_cache_hit is False
    assert res_producer.cache_source_run_id is None

    res_hit = RunResult(run=hit, cache_hit=False)
    assert res_hit.is_cache_hit is True
    assert res_hit.cache_source_run_id == "producer_1"


def test_resolve_canonical_run_id_single_and_multi_hop(tracker):
    _insert_run(tracker, "run_a", meta={"cache_hit": False})
    _insert_run(
        tracker,
        "run_b",
        meta={"cache_hit": True, "cache_source": "run_a"},
    )
    _insert_run(
        tracker,
        "run_c",
        meta={"cache_hit": True, "cache_source": "run_b"},
    )

    assert tracker.resolve_canonical_run_id("run_a") == "run_a"
    assert tracker.resolve_canonical_run_id("run_b") == "run_a"
    assert tracker.resolve_canonical_run_id("run_c") == "run_a"

    run_c_obj = tracker.get_run("run_c")
    assert run_c_obj is not None
    assert tracker.resolve_canonical_run_id(run_c_obj) == "run_a"

    assert consist.resolve_canonical_run_id("run_c", tracker=tracker) == "run_a"


def test_resolve_canonical_run_id_guards_cycles_and_missing_links(tracker):
    _insert_run(
        tracker,
        "cycle_1",
        meta={"cache_hit": True, "cache_source": "cycle_2"},
    )
    _insert_run(
        tracker,
        "cycle_2",
        meta={"cache_hit": True, "cache_source": "cycle_1"},
    )
    assert tracker.resolve_canonical_run_id("cycle_1") == "cycle_2"

    _insert_run(
        tracker,
        "dangling_hit",
        meta={"cache_hit": True, "cache_source": "missing_upstream"},
    )
    assert tracker.resolve_canonical_run_id("dangling_hit") == "dangling_hit"
    assert tracker.resolve_canonical_run_id("nonexistent_run") == "nonexistent_run"


def test_get_run_and_get_run_result_resolve_canonical(tracker, tmp_path: Path):
    out_file = tmp_path / "out.txt"
    out_file.write_text("hello\n", encoding="utf-8")

    def step() -> dict[str, Path]:
        return {"out": out_file}

    res_a = tracker.run(
        fn=step,
        name="canonical_step",
        model="demo",
        config={"param": 1},
        output_paths={"out": out_file},
    )
    res_b = tracker.run(
        fn=step,
        name="canonical_step",
        model="demo",
        config={"param": 1},
        output_paths={"out": out_file},
    )
    assert res_a.is_cache_hit is False
    assert res_b.is_cache_hit is True
    assert res_b.cache_source_run_id == res_a.run.id

    direct_run = tracker.get_run(res_b.run.id)
    assert direct_run is not None and direct_run.id == res_b.run.id

    canonical_run = tracker.get_run(res_b.run.id, resolve_canonical=True)
    assert canonical_run is not None and canonical_run.id == res_a.run.id

    direct_result = consist.get_run_result(res_b.run.id, tracker=tracker)
    assert direct_result.run.id == res_b.run.id
    assert direct_result.is_cache_hit is True

    canonical_result = consist.get_run_result(
        res_b.run.id, resolve_canonical=True, tracker=tracker
    )
    assert canonical_result.run.id == res_a.run.id
    assert canonical_result.is_cache_hit is False
    assert "out" in canonical_result.outputs


def test_config_bundle_and_config_values_resolve_across_cache_hit_chain(
    tracker, tmp_path: Path
):
    pytest.importorskip("yaml")
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    producer = tracker.begin_run(
        "asim_producer",
        "activitysim",
        config={"sample_rate": 0.25},
        facet={"sample_rate": 0.25},
    )
    tracker.canonicalize_config(adapter, [overlay_dir, base_dir], strict=True)
    tracker.end_run()

    _insert_run(
        tracker,
        "asim_hit_1",
        model_name="activitysim",
        meta={"cache_hit": True, "cache_source": producer.id},
    )
    _insert_run(
        tracker,
        "asim_hit_2",
        model_name="activitysim",
        meta={"cache_hit": True, "cache_source": "asim_hit_1"},
    )

    bundle = tracker.get_config_bundle("asim_hit_2", adapter="activitysim")
    assert bundle is not None and bundle.exists()

    with pytest.raises(FileNotFoundError):
        tracker.get_config_bundle(
            "asim_hit_2",
            adapter="activitysim",
            resolve_canonical=False,
        )

    assert tracker.get_config_value("asim_hit_2", "sample_rate") == 0.25
    assert tracker.get_config_values("asim_hit_2") == {"sample_rate": 0.25}
    assert (
        tracker.get_config_value("asim_hit_2", "sample_rate", resolve_canonical=False)
        is None
    )
    assert tracker.get_config_values("asim_hit_2", resolve_canonical=False) == {}

    staged = adapter.materialize_from_run(
        tracker=tracker,
        run_id="asim_hit_2",
        overrides=ConfigOverrides(
            constants={("settings_local.yaml", "sample_rate"): 0.5}
        ),
        output_dir=tmp_path / "staged_from_hit",
    )
    assert (adapter.select_root_dir(staged) / "settings.yaml").exists()


def test_beam_materialize_from_run_and_run_with_config_overrides_resolve_cache_hit_base(
    tracker, tmp_path: Path
):
    pytest.importorskip("pyhocon")
    case_dir, overlay_conf, _ = build_beam_test_configs(tmp_path)
    adapter = BeamConfigAdapter(primary_config=overlay_conf)

    producer = tracker.begin_run("beam_producer", "beam")
    tracker.canonicalize_config(adapter, [case_dir])
    tracker.end_run()

    _insert_run(
        tracker,
        "beam_hit_1",
        model_name="beam",
        meta={"cache_hit": True, "cache_source": producer.id},
    )
    _insert_run(
        tracker,
        "beam_hit_2",
        model_name="beam",
        meta={"cache_hit": True, "cache_source": "beam_hit_1"},
    )

    staged = adapter.materialize_from_run(
        tracker=tracker,
        run_id="beam_hit_2",
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.35}
        ),
        output_dir=tmp_path / "beam_staged_from_hit",
    )
    assert staged.primary_config is not None and staged.primary_config.exists()

    seen_dirs: list[Path] = []

    def step(config_dir: Path) -> None:
        seen_dirs.append(config_dir)

    tracker.run_with_config_overrides(
        adapter=BeamConfigAdapter(),
        base_run_id="beam_hit_2",
        overrides=BeamConfigOverrides(
            values={"beam.agentsim.agentSampleSizeAsFractionOfPopulation": 0.45}
        ),
        output_dir=tmp_path / "beam_override_from_hit",
        fn=step,
        name="beam_override_step",
        model="beam",
    )
    assert len(seen_dirs) == 1
    assert (seen_dirs[0] / "overlay.conf").exists()
