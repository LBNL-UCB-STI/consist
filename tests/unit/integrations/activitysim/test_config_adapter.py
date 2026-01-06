from __future__ import annotations

import csv
from pathlib import Path

import yaml

import pytest

from consist.integrations.activitysim import ActivitySimConfigAdapter, ConfigOverrides
from tests.helpers.activitysim_fixtures import build_activitysim_test_configs


def _find_ingestable(ingestables, table_name: str):
    for spec in ingestables:
        if spec.table_name == table_name:
            return spec
    return None


def _append_model(settings_path: Path, model_name: str) -> None:
    lines = settings_path.read_text(encoding="utf-8").splitlines()
    try:
        models_index = next(
            idx for idx, line in enumerate(lines) if line.strip() == "models:"
        )
    except StopIteration:
        lines.append("models:")
        models_index = len(lines) - 1
    insert_at = models_index + 1
    while insert_at < len(lines) and lines[insert_at].lstrip().startswith("- "):
        insert_at += 1
    lines.insert(insert_at, f"  - {model_name}")
    settings_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_discover_resolves_models_and_aliases(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )

    assert canonical.primary_config == overlay_dir / "settings.yaml"

    config_names = {path.name for path in canonical.config_files}
    assert "settings.yaml" in config_names
    assert "accessibility.yaml" in config_names
    assert "tour_scheduling_atwork.yaml" in config_names
    assert "trip_scheduling.yaml" in config_names
    assert "stop_frequency.yaml" in config_names
    assert canonical.content_hash


def test_canonicalize_builds_ingestables_and_constants(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    run = tracker.begin_run("activitysim_unit", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    artifact_names = {spec.path.name for spec in result.artifacts}
    assert "settings.yaml" in artifact_names
    assert "accessibility.yaml" in artifact_names
    assert "stop_frequency_coeffs.csv" in artifact_names
    assert "trip_scheduling_coefficients.csv.gz" in artifact_names
    assert any("config_bundle" in name for name in artifact_names)

    constants_spec = _find_ingestable(result.ingestables, "activitysim_constants_cache")
    assert constants_spec is not None

    constants_rows = list(constants_spec.rows)
    assert any(
        row["key"] == "sample_rate" and row["value_num"] == 0.25
        for row in constants_rows
    )
    assert any(
        row["key"] == "CONSTANTS.AUTO_TIME" and row["file_name"] == "accessibility.yaml"
        for row in constants_rows
    )
    assert any(
        row["key"] == "CONSTANTS.TRANSIT_MODES" and row["value_type"] == "json"
        for row in constants_rows
    )

    table_names = {spec.table_name for spec in result.ingestables}
    assert table_names == {
        "activitysim_constants_cache",
        "activitysim_coefficients_cache",
        "activitysim_coefficients_template_refs_cache",
        "activitysim_probabilities_cache",
        "activitysim_probabilities_entries_cache",
        "activitysim_probabilities_meta_entries_cache",
        "activitysim_config_ingest_run_link",
    }


def test_prepare_config_plan_builds_row_factories(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    plan = tracker.prepare_config(adapter, [overlay_dir, base_dir], strict=True)

    assert plan.identity_hash
    assert plan.adapter_name == "activitysim"
    assert any(spec.rows and callable(spec.rows) for spec in plan.ingestables)


def test_config_plan_dataframes(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    plan = tracker.prepare_config(adapter, [overlay_dir, base_dir], strict=True)

    constants_df = plan.constants_df()
    coeffs_df = plan.coefficients_df()
    probs_df = plan.probabilities_df()

    assert not constants_df.empty
    assert not coeffs_df.empty
    assert not probs_df.empty
    assert "file_name" in constants_df.columns
    assert "coefficient_name" in coeffs_df.columns
    assert "row_index" in probs_df.columns


def test_coefficients_and_probabilities_parsing(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter()

    run = tracker.begin_run("activitysim_parse", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    coeff_rows = []
    prob_rows = []
    template_rows = []
    prob_entries = []
    prob_meta_entries = []
    for spec in result.ingestables:
        if spec.table_name == "activitysim_coefficients_cache":
            coeff_rows.extend(list(spec.rows))
        elif spec.table_name == "activitysim_probabilities_cache":
            prob_rows.extend(list(spec.rows))
        elif spec.table_name == "activitysim_coefficients_template_refs_cache":
            template_rows = list(spec.rows)
        elif spec.table_name == "activitysim_probabilities_entries_cache":
            prob_entries.extend(list(spec.rows))
        elif spec.table_name == "activitysim_probabilities_meta_entries_cache":
            prob_meta_entries.extend(list(spec.rows))

    trip_rows = [
        row for row in coeff_rows if row["file_name"].startswith("trip_scheduling")
    ]
    assert any(row["coefficient_name"] == "trip_time" for row in trip_rows)
    assert all(row["source_type"] == "direct" for row in trip_rows)

    dimensional_rows = [
        row
        for row in coeff_rows
        if row["file_name"] == "cdap_interaction_coefficients.csv"
    ]
    assert dimensional_rows
    assert all(row["source_type"] == "dimensional" for row in dimensional_rows)
    assert any(
        row["dims_json"] == '{"activity": "H", "interaction_ptypes": 11}'
        for row in dimensional_rows
    )
    assert template_rows
    assert any(
        row["referenced_coefficient"] == "coef_topology_walk_multiplier_work"
        for row in template_rows
    )
    assert any(row["key"] == "auto" and row["value_num"] == 0.7 for row in prob_entries)
    assert any(
        row["key"] == "depart_range_start" and row["value_num"] == 5.0
        for row in prob_meta_entries
    )

    stop_rows = [
        row for row in coeff_rows if row["file_name"] == "stop_frequency_coeffs.csv"
    ]
    segments = {row["segment"] for row in stop_rows}
    assert segments == {"segment_a", "segment_b"}
    assert all(row["is_constrained"] is True for row in stop_rows)
    assert all(row["source_type"] == "template" for row in stop_rows)

    stop_prob_rows = [
        row for row in prob_rows if row["file_name"] == "stop_frequency_probs.csv"
    ]
    assert stop_prob_rows
    assert stop_prob_rows[0]["dims"] == {"purpose": "work"}
    assert stop_prob_rows[0]["probs"] == {"auto": 0.7, "transit": 0.3}


def test_missing_csv_lenient_logs_warning(tracker, tmp_path: Path, caplog):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    missing_yaml = base_dir / "missing_model.yaml"
    missing_yaml.write_text("SPEC: missing_coefficients.csv\n", encoding="utf-8")
    _append_model(overlay_dir / "settings.yaml", "missing_model")

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_missing_lenient", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=False
    )
    adapter.canonicalize(canonical, run=run, tracker=tracker, strict=False)
    tracker.end_run()

    assert any("Missing referenced CSV" in record.message for record in caplog.records)


def test_missing_csv_strict_raises(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    missing_yaml = base_dir / "missing_model.yaml"
    missing_yaml.write_text("SPEC: missing_coefficients.csv\n", encoding="utf-8")
    _append_model(overlay_dir / "settings.yaml", "missing_model")

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_missing_strict", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=False
    )
    with pytest.raises(FileNotFoundError):
        adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()


def test_missing_include_settings_warns(tracker, tmp_path: Path, caplog):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    include_yaml = base_dir / "include_missing.yaml"
    include_yaml.write_text(
        "include_settings: missing_settings.yaml\n", encoding="utf-8"
    )
    _append_model(overlay_dir / "settings.yaml", "include_missing")

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_missing_include", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=False
    )
    adapter.canonicalize(canonical, run=run, tracker=tracker, strict=False)
    tracker.end_run()

    assert any("include_settings file" in record.message for record in caplog.records)


def test_inherit_settings_respects_primary_only(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path, sample_rate=0.25)
    overlay_settings = overlay_dir / "settings.yaml"
    overlay_settings.write_text(
        "inherit_settings: false\n"
        "models:\n"
        "  - compute_accessibility\n"
        "  - atwork_subtour_scheduling\n"
        "  - trip_scheduling\n"
        "  - stop_frequency\n"
        "sample_rate: 0.9\n",
        encoding="utf-8",
    )

    adapter = ActivitySimConfigAdapter()
    run = tracker.begin_run("activitysim_inherit_false", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    constants_spec = _find_ingestable(result.ingestables, "activitysim_constants_cache")
    assert constants_spec is not None
    constants_rows = list(constants_spec.rows)
    assert any(
        row["key"] == "sample_rate" and row["value_num"] == 0.9
        for row in constants_rows
    )


def test_heuristic_reference_discovery_toggle(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    heuristic_yaml = base_dir / "heuristic_model.yaml"
    heuristic_yaml.write_text("EXTRA_FILE: extra_coeffs.csv\n", encoding="utf-8")
    extra_csv = base_dir / "extra_coeffs.csv"
    extra_csv.write_text("coefficient_name,value\nextra,1.0\n", encoding="utf-8")
    _append_model(overlay_dir / "settings.yaml", "heuristic_model")

    run = tracker.begin_run("activitysim_heuristic_on", "activitysim")
    adapter = ActivitySimConfigAdapter(allow_heuristic_refs=True)
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    artifact_names = {spec.path.name for spec in result.artifacts}
    assert "extra_coeffs.csv" in artifact_names

    run = tracker.begin_run("activitysim_heuristic_off", "activitysim")
    adapter = ActivitySimConfigAdapter(allow_heuristic_refs=False)
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    artifact_names = {spec.path.name for spec in result.artifacts}
    assert "extra_coeffs.csv" not in artifact_names


def test_bundle_artifact_logged(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    adapter = ActivitySimConfigAdapter(bundle_configs=True)

    run = tracker.begin_run("activitysim_bundle", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    bundle_specs = [
        spec for spec in result.artifacts if spec.meta.get("config_role") == "bundle"
    ]
    assert len(bundle_specs) == 1
    assert bundle_specs[0].path.exists()


def test_bundle_cache_dir_reuse(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    cache_dir = tmp_path / "bundle_cache"
    adapter = ActivitySimConfigAdapter(bundle_configs=True, bundle_cache_dir=cache_dir)

    run_a = tracker.begin_run("activitysim_bundle_a", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result_a = adapter.canonicalize(canonical, run=run_a, tracker=tracker, strict=True)
    tracker.end_run()

    bundle_a = next(
        spec for spec in result_a.artifacts if spec.meta.get("config_role") == "bundle"
    )
    assert bundle_a.path.exists()
    mtime = bundle_a.path.stat().st_mtime

    run_b = tracker.begin_run("activitysim_bundle_b", "activitysim")
    canonical_b = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result_b = adapter.canonicalize(
        canonical_b, run=run_b, tracker=tracker, strict=True
    )
    tracker.end_run()

    bundle_b = next(
        spec for spec in result_b.artifacts if spec.meta.get("config_role") == "bundle"
    )
    assert bundle_b.path == bundle_a.path
    assert bundle_b.path.stat().st_mtime == mtime


def test_materialize_applies_overrides(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    cache_dir = tmp_path / "bundle_cache"
    adapter = ActivitySimConfigAdapter(bundle_configs=True, bundle_cache_dir=cache_dir)

    run = tracker.begin_run("activitysim_materialize_base", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    bundle_path = next(
        spec.path
        for spec in result.artifacts
        if spec.meta.get("config_role") == "bundle"
    )
    overrides = ConfigOverrides(
        constants={
            ("settings_local.yaml", "sample_rate"): 0.33,
            ("accessibility.yaml", "CONSTANTS.AUTO_TIME"): 2.5,
        },
        coefficients={
            ("accessibility_coefficients.csv", "time", ""): 5.0,
            ("stop_frequency_coeffs.csv", "coef_1", "segment_a"): 9.0,
        },
    )

    staged_dir = tmp_path / "staged_config"
    staged = adapter.materialize(
        bundle_path,
        overrides,
        output_dir=staged_dir,
        identity=tracker.identity,
    )

    settings_path = next(
        (root / "settings_local.yaml")
        for root in staged.root_dirs
        if (root / "settings_local.yaml").exists()
    )
    settings = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
    assert settings["sample_rate"] == 0.33

    access_path = next(
        (root / "accessibility.yaml")
        for root in staged.root_dirs
        if (root / "accessibility.yaml").exists()
    )
    access_yaml = yaml.safe_load(access_path.read_text(encoding="utf-8"))
    assert access_yaml["CONSTANTS"]["AUTO_TIME"] == 2.5

    coeff_path = next(
        (root / "accessibility_coefficients.csv")
        for root in staged.root_dirs
        if (root / "accessibility_coefficients.csv").exists()
    )
    with coeff_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    updated = [row for row in rows if row["coefficient_name"] == "time"]
    assert updated and updated[0]["value"] == "5.0"

    template_path = next(
        (root / "stop_frequency_coeffs.csv")
        for root in staged.root_dirs
        if (root / "stop_frequency_coeffs.csv").exists()
    )
    with template_path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    updated = [row for row in rows if row["coefficient_name"] == "coef_1"]
    assert updated and updated[0]["segment_a"] == "9.0"


def test_materialize_hash_changes_with_overrides(tracker, tmp_path: Path):
    base_dir, overlay_dir = build_activitysim_test_configs(tmp_path)
    cache_dir = tmp_path / "bundle_cache_hash"
    adapter = ActivitySimConfigAdapter(bundle_configs=True, bundle_cache_dir=cache_dir)

    run = tracker.begin_run("activitysim_materialize_hash", "activitysim")
    canonical = adapter.discover(
        [overlay_dir, base_dir], identity=tracker.identity, strict=True
    )
    result = adapter.canonicalize(canonical, run=run, tracker=tracker, strict=True)
    tracker.end_run()

    bundle_path = next(
        spec.path
        for spec in result.artifacts
        if spec.meta.get("config_role") == "bundle"
    )

    staged_a = adapter.materialize(
        bundle_path,
        ConfigOverrides(constants={("settings_local.yaml", "sample_rate"): 0.11}),
        output_dir=tmp_path / "staged_a",
        identity=tracker.identity,
    )
    staged_b = adapter.materialize(
        bundle_path,
        ConfigOverrides(constants={("settings_local.yaml", "sample_rate"): 0.22}),
        output_dir=tmp_path / "staged_b",
        identity=tracker.identity,
    )

    assert staged_a.content_hash != staged_b.content_hash
