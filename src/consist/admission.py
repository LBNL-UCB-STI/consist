"""
Consist admission control, provenance sidecars, and container inspection.
"""

from __future__ import annotations

from pathlib import Path

from consist.core.admission import (
    AdmissionReference,
    AdmissionReport,
    DeclaredDigestExpectation,
    PriorRunExpectation,
    check_admission_reference,
    check_admission_reference_expected_identity,
    check_admission_reference_prior_run_expectation,
    check_artifact_identity,
    check_expected_identity,
    check_prior_run_expectation,
    compare_path_to_identity,
)
from consist.core.logical_array_identity import (
    ARTIFACT_PROVENANCE_SCHEMA_VERSION,
    inspect_matrix_container,
    read_artifact_provenance_sidecar,
    write_artifact_provenance_sidecar,
)
from consist.digest import Digest, digest_file

FileIdentity = Digest


def admission_file_identity(path: str | Path) -> str:
    """Compute canonical ``sha256:file:<hex>`` admission identity for a file."""
    return digest_file(path)


__all__ = [
    "ARTIFACT_PROVENANCE_SCHEMA_VERSION",
    "AdmissionReference",
    "AdmissionReport",
    "DeclaredDigestExpectation",
    "Digest",
    "FileIdentity",
    "PriorRunExpectation",
    "admission_file_identity",
    "check_admission_reference",
    "check_admission_reference_expected_identity",
    "check_admission_reference_prior_run_expectation",
    "check_artifact_identity",
    "check_expected_identity",
    "check_prior_run_expectation",
    "compare_path_to_identity",
    "inspect_matrix_container",
    "read_artifact_provenance_sidecar",
    "write_artifact_provenance_sidecar",
]
