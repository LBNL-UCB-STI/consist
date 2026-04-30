from __future__ import annotations

from pathlib import Path

from consist.core.config_canonicalization import (
    CanonicalConfig,
    CanonicalizationResult,
    canonical_identity_from_config,
)


class _BundleTrackerProbeAdapter:
    model_name = "bundle_probe"

    def __init__(self) -> None:
        self.seen_tracker = None

    def discover(
        self,
        root_dirs: list[Path],
        *,
        identity,
        strict: bool = False,
        options=None,
    ) -> CanonicalConfig:
        return CanonicalConfig(
            root_dirs=root_dirs,
            primary_config=None,
            config_files=[],
            external_files=[],
            content_hash="bundle-probe-hash",
        )

    def canonicalize(
        self,
        config: CanonicalConfig,
        *,
        run=None,
        tracker=None,
        strict: bool = False,
        plan_only: bool = False,
        options=None,
    ) -> CanonicalizationResult:
        return CanonicalizationResult(
            artifacts=[],
            ingestables=[],
            identity=canonical_identity_from_config(
                adapter_name=self.model_name,
                adapter_version=None,
                config=config,
            ),
        )

    def bundle_artifact(self, config: CanonicalConfig, *, run, tracker):
        self.seen_tracker = tracker
        return None


def test_apply_config_plan_passes_concrete_tracker_to_bundle_hook(
    tracker,
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    adapter = _BundleTrackerProbeAdapter()

    plan = tracker.prepare_config(adapter, [config_dir])

    with tracker.start_run("bundle_probe_run", "bundle_probe"):
        tracker.apply_config_plan(plan, adapter=adapter, ingest=False)

    assert adapter.seen_tracker is tracker
