import uuid

from consist.core.container_policy import (
    apply_container_policy_meta,
    normalize_container_policy,
    validate_recovery_registration_policy,
)
from consist.models.artifact import Artifact


def _artifact(
    *,
    driver: str,
    key: str = "artifact",
    meta: dict | None = None,
    parent_artifact_id: uuid.UUID | None = None,
) -> Artifact:
    return Artifact(
        key=key,
        container_uri=f"outputs://{key}",
        driver=driver,
        meta=meta or {},
        parent_artifact_id=parent_artifact_id,
    )


def test_normal_file_artifact_without_container_policy_is_allowed() -> None:
    artifact = _artifact(driver="csv")

    validation = validate_recovery_registration_policy(artifact)

    assert validation.allowed is True
    assert validation.message is None


def test_h5_parent_without_container_policy_is_blocked() -> None:
    artifact = _artifact(driver="h5", key="store")

    validation = validate_recovery_registration_policy(artifact)

    assert validation.allowed is False
    assert validation.message is not None
    assert "container_recovery_unit" in validation.message


def test_h5_parent_file_policy_allows_parent_registration() -> None:
    meta: dict = {}
    apply_container_policy_meta(
        meta,
        container_recovery_unit="parent_file",
        child_recovery_policy="descriptive_only",
    )
    artifact = _artifact(driver="h5", key="store", meta=meta)

    validation = validate_recovery_registration_policy(artifact)

    assert validation.allowed is True
    policy = normalize_container_policy(artifact.meta)
    assert policy.container_recovery_unit == "parent_file"
    assert policy.child_recovery_policy == "descriptive_only"
    assert policy.representation_policy["tabular_parquet"]["role"] == "none"


def test_descriptive_only_parent_policy_blocks_child_registration() -> None:
    parent = _artifact(
        driver="h5",
        key="store",
        meta={
            "container_recovery_unit": "parent_file",
            "child_recovery_policy": "descriptive_only",
        },
    )
    child = _artifact(
        driver="h5_table",
        key="households",
        parent_artifact_id=parent.id,
    )

    validation = validate_recovery_registration_policy(child, parent=parent)

    assert validation.allowed is False
    assert validation.message is not None
    assert "child_recovery_policy='descriptive_only'" in validation.message
    assert "parent H5 artifact" in validation.message


def test_unknown_container_policy_values_fail_closed() -> None:
    artifact = _artifact(
        driver="h5",
        key="store",
        meta={"container_recovery_unit": "surprise"},
    )

    validation = validate_recovery_registration_policy(artifact)

    assert validation.allowed is False
    assert validation.message is not None
    assert "Unknown container_recovery_unit" in validation.message
