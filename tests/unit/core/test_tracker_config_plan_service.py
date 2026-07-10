from __future__ import annotations

from pathlib import Path

import pytest

from consist.core.config_canonicalization import (
    CanonicalConfig,
    CanonicalConfigIdentity,
    CanonicalizationReference,
    CanonicalizationResult,
    CanonicalizationSnapshot,
    ConfigReference,
    ArtifactSpec,
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


def test_prepare_config_rejects_references_without_snapshot(
    tracker, tmp_path: Path
) -> None:
    class Adapter(_BundleTrackerProbeAdapter):
        model_name = "snapshot_required"

        def canonicalize(
            self, config: CanonicalConfig, **kwargs
        ) -> CanonicalizationResult:
            del kwargs
            return CanonicalizationResult(
                artifacts=[],
                ingestables=[],
                identity=CanonicalConfigIdentity(
                    adapter_name=self.model_name,
                    adapter_version=None,
                    primary_config=None,
                    identity_hash="identity",
                    references=(
                        ConfigReference(
                            config_key="input",
                            raw_value="input.csv",
                            canonical_value="config:input.csv",
                            status="resolved",
                            required=True,
                        ),
                    ),
                ),
            )

    with pytest.raises(ValueError, match="CanonicalizationSnapshot"):
        tracker.prepare_config(Adapter(), [tmp_path])


def test_canonicalize_config_preserves_the_adapter_snapshot(
    tracker, tmp_path: Path
) -> None:
    source = tmp_path / "input.csv"
    source.write_text("id\n1\n", encoding="utf-8")

    class Adapter(_BundleTrackerProbeAdapter):
        model_name = "snapshot_contribution"

        def canonicalize(
            self, config: CanonicalConfig, **kwargs
        ) -> CanonicalizationResult:
            del config, kwargs
            reference = ConfigReference(
                config_key="input",
                raw_value="input.csv",
                canonical_value="config:input.csv",
                status="resolved",
                required=True,
            )
            identity = CanonicalConfigIdentity(
                adapter_name=self.model_name,
                adapter_version=None,
                primary_config=None,
                identity_hash="identity",
                references=(reference,),
            )
            return CanonicalizationResult(
                artifacts=[ArtifactSpec(source, "config:input.csv", "input", {})],
                ingestables=[],
                identity=identity,
                canonicalization=CanonicalizationSnapshot(
                    adapter_name=self.model_name,
                    adapter_version=None,
                    identity_hash="identity",
                    references=(
                        CanonicalizationReference(
                            reference=reference,
                            resolved_path=source,
                            artifact_keys=("config:input.csv",),
                        ),
                    ),
                ),
            )

    with tracker.start_run("snapshot_contribution", "snapshot_contribution") as active:
        contribution = active.canonicalize_config(Adapter(), [tmp_path], ingest=False)

    assert contribution.canonicalization is not None
    assert contribution.canonicalization.references[0].artifact_keys == (
        "config:input.csv",
    )
