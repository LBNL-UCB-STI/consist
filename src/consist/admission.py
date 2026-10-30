"""
Consist admission control, provenance sidecars, and container inspection.
"""

from __future__ import annotations

from consist.core.admission import (
    AdmissionIdentity,
    AdmissionReference,
    AdmissionReport,
    DeclaredDigestExpectation,
    FileIdentity,
    PriorRunExpectation,
    admission_file_identity,
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
    ArtifactProvenanceSidecar,
    admission_directory_identity,
    artifact_provenance_sidecar_path,
    inspect_matrix_container,
    read_artifact_provenance_sidecar,
    write_artifact_provenance_sidecar,
)
from consist.digest import MatrixContainerSummary

__all__ = [
    "ARTIFACT_PROVENANCE_SCHEMA_VERSION",
    "AdmissionIdentity",
    "AdmissionReference",
    "AdmissionReport",
    "ArtifactProvenanceSidecar",
    "DeclaredDigestExpectation",
    "FileIdentity",
    "MatrixContainerSummary",
    "PriorRunExpectation",
    "admission_directory_identity",
    "admission_file_identity",
    "artifact_provenance_sidecar_path",
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
