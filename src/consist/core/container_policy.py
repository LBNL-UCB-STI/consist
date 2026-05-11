"""Recovery policy helpers for structured container artifacts."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Literal

from consist.models.artifact import Artifact

ContainerRecoveryUnit = Literal["parent_file", "derived_children"]
ChildRecoveryPolicy = Literal["descriptive_only", "independent"]
RepresentationRole = Literal[
    "none",
    "derived_inspection_cache",
    "authoritative_tabular_store",
]

CONTAINER_RECOVERY_UNITS = {"parent_file", "derived_children"}
CHILD_RECOVERY_POLICIES = {"descriptive_only", "independent"}
REPRESENTATION_ROLES = {
    "none",
    "derived_inspection_cache",
    "authoritative_tabular_store",
}

DEFAULT_REPRESENTATION_POLICY: dict[str, dict[str, Any]] = {
    "tabular_parquet": {
        "role": "none",
        "complete_for_rebuild": False,
    }
}


@dataclass(frozen=True, slots=True)
class ContainerPolicy:
    """Normalized view of container recovery metadata."""

    container_recovery_unit: str | None = None
    child_recovery_policy: str | None = None
    representation_policy: Mapping[str, Any] = field(default_factory=dict)
    errors: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.errors


@dataclass(frozen=True, slots=True)
class ContainerRecoveryValidation:
    """Decision for recovery-root registration under container policy."""

    allowed: bool
    message: str | None = None


def _copy_default_representation_policy() -> dict[str, dict[str, Any]]:
    return {
        name: dict(policy) for name, policy in DEFAULT_REPRESENTATION_POLICY.items()
    }


def normalize_container_policy(meta: Mapping[str, Any] | None) -> ContainerPolicy:
    """Return a conservative normalized policy view from artifact metadata."""
    if not meta:
        return ContainerPolicy()

    errors: list[str] = []
    container_recovery_unit = meta.get("container_recovery_unit")
    if container_recovery_unit is not None:
        if (
            not isinstance(container_recovery_unit, str)
            or container_recovery_unit not in CONTAINER_RECOVERY_UNITS
        ):
            errors.append(
                f"Unknown container_recovery_unit {container_recovery_unit!r}."
            )

    child_recovery_policy = meta.get("child_recovery_policy")
    if child_recovery_policy is not None:
        if (
            not isinstance(child_recovery_policy, str)
            or child_recovery_policy not in CHILD_RECOVERY_POLICIES
        ):
            errors.append(f"Unknown child_recovery_policy {child_recovery_policy!r}.")

    representation_policy = meta.get("representation_policy")
    if representation_policy is None:
        representation_policy = {}
    elif not isinstance(representation_policy, Mapping):
        errors.append("representation_policy must be a mapping when provided.")
        representation_policy = {}
    else:
        for name, policy in representation_policy.items():
            if not isinstance(policy, Mapping):
                errors.append(f"representation_policy[{name!r}] must be a mapping.")
                continue
            role = policy.get("role")
            if role is not None and role not in REPRESENTATION_ROLES:
                errors.append(f"Unknown representation_policy[{name!r}].role {role!r}.")

    return ContainerPolicy(
        container_recovery_unit=(
            container_recovery_unit
            if isinstance(container_recovery_unit, str)
            else None
        ),
        child_recovery_policy=(
            child_recovery_policy if isinstance(child_recovery_policy, str) else None
        ),
        representation_policy=representation_policy,
        errors=tuple(errors),
    )


def apply_container_policy_meta(
    meta: MutableMapping[str, Any],
    *,
    container_recovery_unit: ContainerRecoveryUnit | None = None,
    child_recovery_policy: ChildRecoveryPolicy | None = None,
    representation_policy: Mapping[str, Any] | None = None,
) -> None:
    """Apply normalized container recovery policy fields to artifact metadata."""
    if container_recovery_unit is None and child_recovery_policy is None:
        if representation_policy is None:
            return

    if (
        container_recovery_unit is not None
        and container_recovery_unit not in CONTAINER_RECOVERY_UNITS
    ):
        raise ValueError(
            "container_recovery_unit must be one of: "
            f"{', '.join(sorted(CONTAINER_RECOVERY_UNITS))}."
        )
    if (
        child_recovery_policy is not None
        and child_recovery_policy not in CHILD_RECOVERY_POLICIES
    ):
        raise ValueError(
            "child_recovery_policy must be one of: "
            f"{', '.join(sorted(CHILD_RECOVERY_POLICIES))}."
        )

    if container_recovery_unit is not None:
        meta["container_recovery_unit"] = container_recovery_unit
    if child_recovery_policy is not None:
        meta["child_recovery_policy"] = child_recovery_policy

    normalized_representation_policy: dict[str, Any]
    if representation_policy is None:
        normalized_representation_policy = _copy_default_representation_policy()
    else:
        normalized_representation_policy = {
            name: dict(policy) if isinstance(policy, Mapping) else policy
            for name, policy in representation_policy.items()
        }

    policy = normalize_container_policy(
        {"representation_policy": normalized_representation_policy}
    )
    if not policy.valid:
        raise ValueError("; ".join(policy.errors))
    meta["representation_policy"] = normalized_representation_policy


def validate_recovery_registration_policy(
    artifact: Artifact,
    *,
    parent: Artifact | None = None,
) -> ContainerRecoveryValidation:
    """Validate whether an artifact may adopt a verified recovery root."""
    driver = artifact.driver
    meta = artifact.meta if isinstance(artifact.meta, Mapping) else {}

    if driver in {"h5", "hdf5"}:
        policy = normalize_container_policy(meta)
        if not policy.valid:
            return ContainerRecoveryValidation(
                allowed=False,
                message=(
                    f"Artifact {artifact.key!r} has invalid container recovery "
                    f"policy: {'; '.join(policy.errors)}"
                ),
            )
        if policy.container_recovery_unit is None:
            return ContainerRecoveryValidation(
                allowed=False,
                message=(
                    f"Artifact {artifact.key!r} is an HDF5 container without "
                    "container_recovery_unit policy. Declare "
                    "container_recovery_unit='parent_file' before registering "
                    "verified recovery roots."
                ),
            )
        if policy.container_recovery_unit == "parent_file":
            return ContainerRecoveryValidation(allowed=True)
        return ContainerRecoveryValidation(
            allowed=False,
            message=(
                f"Artifact {artifact.key!r} declares "
                f"container_recovery_unit={policy.container_recovery_unit!r}, "
                "but verified recovery-copy adoption only supports parent-file "
                "HDF5 recovery today."
            ),
        )

    if driver == "h5_table":
        if parent is None:
            return ContainerRecoveryValidation(
                allowed=False,
                message=(
                    f"Artifact {artifact.key!r} is an HDF5 child table without "
                    "a resolvable parent container policy. Register recovery "
                    "roots on the parent H5 artifact instead."
                ),
            )
        parent_meta = parent.meta if isinstance(parent.meta, Mapping) else {}
        policy = normalize_container_policy(parent_meta)
        if not policy.valid:
            return ContainerRecoveryValidation(
                allowed=False,
                message=(
                    f"Parent artifact {parent.key!r} has invalid container "
                    f"recovery policy: {'; '.join(policy.errors)}"
                ),
            )
        if policy.child_recovery_policy is None:
            return ContainerRecoveryValidation(
                allowed=False,
                message=(
                    f"Artifact {artifact.key!r} is an HDF5 child table whose "
                    "parent has no child_recovery_policy. Register recovery "
                    "roots on the parent H5 artifact instead."
                ),
            )
        if policy.child_recovery_policy == "descriptive_only":
            return ContainerRecoveryValidation(
                allowed=False,
                message=(
                    f"Artifact {artifact.key!r} is an HDF5 child table whose "
                    "parent declares child_recovery_policy='descriptive_only'. "
                    "Register recovery roots on the parent H5 artifact instead."
                ),
            )
        return ContainerRecoveryValidation(
            allowed=False,
            message=(
                f"Artifact {artifact.key!r} is an HDF5 child table whose "
                "parent declares child_recovery_policy='independent', but "
                "independent child recovery is not supported until a complete "
                "rebuild representation policy exists."
            ),
        )

    return ContainerRecoveryValidation(allowed=True)
