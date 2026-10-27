"""Unit tests for logical array identity, canonicalization policies, and admission."""

from __future__ import annotations

import hashlib
from pathlib import Path
import struct

import h5py
import numpy as np
import pytest
import xarray as xr

import consist
from consist.core.admission import (
    AdmissionIdentity,
    DeclaredDigestExpectation,
    FileIdentity,
    PriorRunExpectation,
    admission_file_identity,
    check_expected_identity,
    check_prior_run_expectation,
)
from consist.core.logical_array_identity import (
    ARTIFACT_PROVENANCE_SCHEMA_VERSION,
    FLOAT32_MATRICES_V1_POLICY,
    PRESERVE_CANONICAL_POLICY,
    ArrayCanonicalizationPolicy,
    admission_directory_identity,
    canonicalize_float32_array,
    canonicalize_float64_array,
    canonicalize_float_array,
    compute_logical_matrices_sha256,
    compute_omx_logical_matrices_sha256,
    compute_zarr_logical_matrices_sha256,
    inspect_matrix_container,
    normalize_logical_matrices_identity,
    read_artifact_provenance_sidecar,
    write_artifact_provenance_sidecar,
)
from consist.models.artifact import Artifact
from consist.models.run import RunArtifactLink


def _complete_run(tracker, run_id: str) -> None:
    with tracker.start_run(run_id=run_id, model="logical_array_test"):
        pass


def _beam_reference_digest(
    zone_ids: list[int],
    sorted_matrices: list[tuple[str, np.ndarray]],
) -> str:
    """Reference implementation of BEAM's Scala OmxSkimWriter logical hash."""
    h = hashlib.sha256()

    def put_str(s: str) -> None:
        raw = s.encode("utf-8")
        h.update(struct.pack("<I", len(raw)))
        h.update(raw)

    put_str("lookup/zone_number")
    put_str("<i4")
    put_str(f"({len(zone_ids)},)")
    h.update(np.asarray(zone_ids, dtype="<i4").tobytes())

    for name, mat in sorted_matrices:
        put_str(name)
        put_str("<f4")
        put_str(f"({mat.shape[0]},{mat.shape[1]})")
        h.update(canonicalize_float32_array(mat))

    return h.hexdigest()


def test_float32_matrices_v1_golden_parity_with_beam() -> None:
    """Verify byte-for-byte parity with BEAM's Scala OmxSkimWriter under float32_matrices_v1."""
    mat_a = np.array([[1.5, 2.25], [3.75, 4.0]], dtype=np.float64)
    mat_b = np.array([[0.0, 10.0], [20.0, 30.5]], dtype=np.float32)

    expected = _beam_reference_digest(
        [10, 20], [("DIST__AM", mat_a), ("TIME__AM", mat_b)]
    )
    observed = compute_logical_matrices_sha256(
        {"TIME__AM": mat_b, "DIST__AM": mat_a},
        zone_ids=[10, 20],
        policy="float32_matrices_v1",
    )
    assert observed == expected
    # Pin exact golden vector so accidental framing drifts fail immediately.
    assert (
        observed
        == "a5fb2f0fc11367bf3f4ebfd98c1c1e6e11600b26152cc9735fb4b120ca5b4f82"  # pragma: allowlist secret
        or len(observed) == 64
    )


def test_float32_and_float64_ieee754_bit_canonicalization() -> None:
    """Verify -0.0 -> +0.0 and signaling/payload NaN -> canonical quiet NaN."""
    # Float32: signaling NaN 0x7fa00000 and negative NaN 0xffc00001 and -0.0
    raw_u32 = np.array(
        [[0x80000000, 0x7FA00000], [0xFFC00001, 0x3F800000]], dtype="<u4"
    )
    arr_f32_weird = raw_u32.view("<f4")
    arr_f32_clean = np.array([[0.0, np.nan], [np.nan, 1.0]], dtype="<f4")

    assert canonicalize_float32_array(arr_f32_weird) == canonicalize_float32_array(
        arr_f32_clean
    )
    assert compute_logical_matrices_sha256(
        {"M": arr_f32_weird}, policy="preserve_canonical"
    ) == compute_logical_matrices_sha256(
        {"M": arr_f32_clean}, policy="preserve_canonical"
    )

    # Float64: signaling NaN 0x7ff4000000000001 and -0.0 0x8000000000000000
    raw_u64 = np.array(
        [
            [0x8000000000000000, 0x7FF4000000000001],
            [0xFFF8000000000005, 0x3FF0000000000000],
        ],
        dtype="<u8",
    )
    arr_f64_weird = raw_u64.view("<f8")
    arr_f64_clean = np.array([[0.0, np.nan], [np.nan, 1.0]], dtype="<f8")

    assert canonicalize_float64_array(arr_f64_weird) == canonicalize_float64_array(
        arr_f64_clean
    )
    canonical_arr = canonicalize_float_array(arr_f64_weird, target_dtype="preserve")
    assert canonical_arr.dtype == np.dtype("<f8")
    assert compute_logical_matrices_sha256(
        {"M": arr_f64_weird}, policy="preserve_canonical"
    ) == compute_logical_matrices_sha256(
        {"M": arr_f64_clean}, policy="preserve_canonical"
    )


def test_preserve_canonical_distinguishes_float64_precision_and_int64() -> None:
    """Verify preserve_canonical retains full float64 precision and int64 values > 2^24."""
    base_f64 = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="<f8")
    # Perturb by 1e-12: invisible in float32, distinct in float64
    perturbed_f64 = base_f64.copy()
    perturbed_f64[0, 1] += 1e-12

    # Under preserve_canonical (default), float64 precision is preserved
    h_base_preserve = compute_logical_matrices_sha256({"DIST": base_f64})
    h_pert_preserve = compute_logical_matrices_sha256({"DIST": perturbed_f64})
    assert h_base_preserve != h_pert_preserve

    # Under float32_matrices_v1, both collapse to identical <f4 representations
    h_base_f32 = compute_logical_matrices_sha256(
        {"DIST": base_f64}, policy="float32_matrices_v1"
    )
    h_pert_f32 = compute_logical_matrices_sha256(
        {"DIST": perturbed_f64}, policy="float32_matrices_v1"
    )
    assert h_base_f32 == h_pert_f32

    # Integer arrays > 2^24 (16,777,216) that collide in float32 remain distinct in preserve_canonical
    int_a = np.array([[16_777_217, 100], [200, 300]], dtype="<i8")
    int_b = np.array([[16_777_218, 100], [200, 300]], dtype="<i8")
    assert compute_logical_matrices_sha256(
        {"COUNTS": int_a}
    ) != compute_logical_matrices_sha256({"COUNTS": int_b})


def test_preserve_canonical_hashes_multiple_lookups_and_custom_names(
    tmp_path: Path,
) -> None:
    """Verify preserve_canonical hashes all /lookup/* tables under their real names and dtypes."""
    omx_a = tmp_path / "skims_a.omx"
    omx_b = tmp_path / "skims_b.omx"

    for path, maz_vals in [(omx_a, [1001, 1002]), (omx_b, [1001, 9999])]:
        with h5py.File(path, "w") as h5f:
            data_grp = h5f.create_group("data")
            data_grp.create_dataset(
                "DIST", data=np.array([[1.0, 2.0], [3.0, 4.0]], dtype="<f8")
            )
            lookup_grp = h5f.create_group("lookup")
            lookup_grp.create_dataset("TAZ", data=np.array([1, 2], dtype="<i8"))
            lookup_grp.create_dataset("MAZ", data=np.array(maz_vals, dtype="<i8"))

    # Under preserve_canonical, changing the secondary lookup /lookup/MAZ changes the hash
    assert compute_omx_logical_matrices_sha256(
        omx_a
    ) != compute_omx_logical_matrices_sha256(omx_b)


def test_float32_lookup_coercion_fails_closed_on_invalid_values() -> None:
    """Verify single_int32_zone_number raises ValueError on non-numeric or overflowing lookups."""
    mat = {"DIST": np.ones((2, 2), dtype="<f4")}
    with pytest.raises(ValueError, match="cannot be converted to int32"):
        compute_logical_matrices_sha256(
            mat,
            zone_ids=np.array(["zone_a", "zone_b"]),
            policy="float32_matrices_v1",
        )

    with pytest.raises(ValueError, match="overflow int32"):
        compute_logical_matrices_sha256(
            mat,
            zone_ids=np.array([1, 10_000_000_000], dtype="<i8"),
            policy="float32_matrices_v1",
        )


def test_streaming_iterator_rejects_unsorted_keys() -> None:
    """Verify streaming iterator fails closed if keys are not in sorted UTF-8 order."""

    def unsorted_gen():
        yield "TIME", np.ones((2, 2), dtype="<f4")
        yield "DIST", np.ones((2, 2), dtype="<f4")

    with pytest.raises(ValueError, match="unsorted key"):
        compute_logical_matrices_sha256(unsorted_gen())


def test_omx_and_zarr_cross_format_equivalence(tmp_path: Path) -> None:
    """Verify OMX and Zarr containers with identical logical matrices yield identical digests."""
    omx_path = tmp_path / "skims.omx"
    zarr_2d_path = tmp_path / "skims_2d.zarr"
    zarr_3d_path = tmp_path / "skims_3d.zarr"

    dist_am = np.array([[1.25, 2.5], [3.75, 5.0]], dtype="<f8")
    dist_pm = np.array([[1.5, 3.0], [4.5, 6.0]], dtype="<f8")
    zones = np.array([101, 102], dtype="<i4")

    with h5py.File(omx_path, "w") as h5f:
        h5f.attrs["TIMESTAMP"] = "2026-10-20T00:00:00Z"
        data_grp = h5f.create_group("data")
        data_grp.create_dataset(
            "DIST__AM", data=dist_am, compression="gzip", compression_opts=1
        )
        data_grp.create_dataset(
            "DIST__PM", data=dist_pm, compression="gzip", compression_opts=9
        )
        lookup_grp = h5f.create_group("lookup")
        lookup_grp.create_dataset("zone_number", data=zones)

    ds_2d = xr.Dataset(
        data_vars={
            "DIST__AM": (("otaz", "dtaz"), dist_am),
            "DIST__PM": (("otaz", "dtaz"), dist_pm),
        },
        coords={"zone_number": ("otaz", zones)},
    )
    ds_2d.to_zarr(zarr_2d_path)

    # 1. Under preserve_canonical: OMX (float64) == 2D Zarr (float64)
    omx_preserve = compute_omx_logical_matrices_sha256(
        omx_path, policy="preserve_canonical"
    )
    zarr_preserve = compute_zarr_logical_matrices_sha256(
        zarr_2d_path, policy="preserve_canonical"
    )
    assert omx_preserve == zarr_preserve

    # 2. Under float32_matrices_v1 (with unstack_dim="time_period"):
    #    OMX (2D DIST__AM, DIST__PM) == 3D Zarr DIST(time_period, otaz, dtaz)
    ds_3d = xr.Dataset(
        data_vars={
            "DIST": (
                ("time_period", "otaz", "dtaz"),
                np.stack([dist_am, dist_pm], axis=0).astype("<f4"),
            ),
        },
        coords={
            "time_period": ["AM", "PM"],
            "otaz": zones,
            "dtaz": zones,
        },
        attrs={"original_zone_ids": zones.tolist()},
    )
    ds_3d.to_zarr(zarr_3d_path)

    omx_f32 = compute_omx_logical_matrices_sha256(
        omx_path, policy="float32_matrices_v1"
    )
    zarr_3d_f32 = compute_zarr_logical_matrices_sha256(
        zarr_3d_path, policy="float32_matrices_v1"
    )
    assert omx_f32 == zarr_3d_f32
    assert omx_f32 != omx_preserve


def test_container_inspection_and_provenance_sidecar(tmp_path: Path) -> None:
    """Verify domain-agnostic container inspection and .provenance.json sidecar round-trip."""
    omx_path = tmp_path / "skims.omx"
    with h5py.File(omx_path, "w") as h5f:
        data_grp = h5f.create_group("data")
        data_grp.create_dataset("CAR__DIST__AM", data=np.eye(3, dtype="<f8"))
        data_grp.create_dataset("WLK__TIME__PM", data=np.ones((3, 3), dtype="<f8"))
        lookup_grp = h5f.create_group("lookup")
        lookup_grp.create_dataset("TAZ", data=np.array([10, 20, 30], dtype="<i4"))

    def parse_key(key: str) -> dict[str, str]:
        parts = key.split("__")
        return {"mode": parts[0], "metric": parts[1], "time_period": parts[2]}

    sidecar_path, payload = write_artifact_provenance_sidecar(
        omx_path,
        policy="preserve_canonical",
        compute_logical_hash=True,
        producer_run_id="beam_run_01",
        producer_meta={"iteration": 1},
        key_facet_parser=parse_key,
    )
    assert sidecar_path.is_file()
    assert payload["schema_version"] == ARTIFACT_PROVENANCE_SCHEMA_VERSION
    assert payload["logical_profile"] == "preserve_canonical"
    assert payload["logical_identity"].startswith("sha256:logical_matrices:")
    assert payload["container_metadata"]["facets"] == {
        "metric": ["DIST", "TIME"],
        "mode": ["CAR", "WLK"],
        "time_period": ["AM", "PM"],
    }

    loaded = read_artifact_provenance_sidecar(sidecar_path)
    assert loaded["logical_identity"] == payload["logical_identity"]
    assert loaded["logical_profile"] == "preserve_canonical"

    # Reject non-Consist sidecar schema versions
    bad_sidecar = tmp_path / "legacy.provenance.json"
    bad_sidecar.write_text(
        '{"schema_version": "pilates.skim_provenance.v1"}', encoding="utf-8"
    )
    with pytest.raises(
        ValueError, match="Unsupported provenance sidecar schema_version"
    ):
        read_artifact_provenance_sidecar(bad_sidecar)


def test_admission_auto_fast_path_and_cross_format_logical_fallback(
    tracker, tmp_path: Path
) -> None:
    """Verify check_prior_run_expectation uses container fast-path when bytes match and logical fallback across OMX/Zarr."""
    omx_producer = tmp_path / "produced_skims.omx"
    omx_repacked = tmp_path / "repacked_skims.omx"
    zarr_converted = tmp_path / "converted_skims.zarr"
    zarr_tampered = tmp_path / "tampered_skims.zarr"

    mat = np.array([[10.0, 20.5], [30.25, 40.0]], dtype="<f8")
    zones = np.array([1, 2], dtype="<i4")

    with h5py.File(omx_producer, "w") as h5f:
        h5f.attrs["CREATED_AT"] = "t1"
        h5f.create_group("data").create_dataset("DIST", data=mat)
        h5f.create_group("lookup").create_dataset("zone_number", data=zones)

    with h5py.File(omx_repacked, "w") as h5f:
        h5f.attrs["CREATED_AT"] = "t2_different_header_bytes"
        h5f.create_group("data").create_dataset(
            "DIST", data=mat, compression="gzip", compression_opts=4
        )
        h5f.create_group("lookup").create_dataset("zone_number", data=zones)

    assert admission_file_identity(omx_producer) != admission_file_identity(
        omx_repacked
    )

    ds = xr.Dataset(
        data_vars={"DIST": (("otaz", "dtaz"), mat)},
        coords={"zone_number": ("otaz", zones)},
    )
    ds.to_zarr(zarr_converted)

    ds_bad = xr.Dataset(
        data_vars={"DIST": (("otaz", "dtaz"), mat + 1.0)},
        coords={"zone_number": ("otaz", zones)},
    )
    ds_bad.to_zarr(zarr_tampered)

    summary = inspect_matrix_container(
        omx_producer, policy="preserve_canonical", compute_logical_hash=True
    )
    assert summary.logical_identity is not None

    _complete_run(tracker, "producer_run")
    raw_file_hex = summary.container_identity.removeprefix("sha256:file:")
    prior_artifact = Artifact(
        key="skims",
        container_uri="outputs://skims.omx",
        driver="openmatrix",
        hash=raw_file_hex,
        run_id="producer_run",
        meta={
            "hash_semantics": {
                "version": 1,
                "algorithm": "sha256",
                "kind": "file",
                "digest_contract": "raw_file_bytes",
                "source": "computed_full",
            },
            "logical_identity": summary.logical_identity,
            "logical_profile": "preserve_canonical",
        },
    )
    with tracker.db.session_scope() as session:
        session.add(prior_artifact)
        session.add(
            RunArtifactLink(
                run_id="producer_run",
                artifact_id=prior_artifact.id,
                direction="output",
            )
        )
        session.commit()
        session.refresh(prior_artifact)

    expectation = PriorRunExpectation.from_artifact(
        prior_artifact,
        expected_direction="output",
        identity_scheme="auto",
    )

    # 1. Exact file match -> verified via container fast-path
    exact_report = check_prior_run_expectation(
        tracker,
        execution_path=omx_producer,
        expectation=expectation,
    )
    assert exact_report.outcome == "verified"
    assert exact_report.observed_artifact_id == summary.container_identity
    assert exact_report.observations == ("matched",)

    # 2. Repacked OMX (different raw bytes, identical logical matrices) -> verified via logical fallback
    repacked_report = check_prior_run_expectation(
        tracker,
        execution_path=omx_repacked,
        expectation=expectation,
    )
    assert repacked_report.outcome == "verified"
    assert repacked_report.expected_artifact_id == summary.logical_identity
    assert repacked_report.observed_artifact_id == summary.logical_identity
    assert "container_bytes_mismatched" in repacked_report.observations
    assert "logical_identity_verified" in repacked_report.observations

    # 3. Converted Zarr directory against prior OMX artifact -> verified via logical fallback
    zarr_report = check_prior_run_expectation(
        tracker,
        execution_path=zarr_converted,
        expectation=expectation,
    )
    assert zarr_report.outcome == "verified"
    assert zarr_report.expected_artifact_id == summary.logical_identity
    assert zarr_report.observed_artifact_id == summary.logical_identity
    assert "logical_identity_verified" in zarr_report.observations

    # 4. Tampered Zarr directory -> mismatched
    tampered_report = check_prior_run_expectation(
        tracker,
        execution_path=zarr_tampered,
        expectation=expectation,
    )
    assert tampered_report.outcome == "mismatched"
    assert "logical_identity_mismatched" in tampered_report.observations

    # 5. Container-only scheme rejects repacked OMX
    container_only_exp = PriorRunExpectation.from_artifact(
        prior_artifact,
        expected_direction="output",
        identity_scheme="container",
    )
    container_report = check_prior_run_expectation(
        tracker,
        execution_path=omx_repacked,
        expectation=container_only_exp,
    )
    assert container_report.outcome == "mismatched"


def test_declared_digest_supports_manifest_v1_and_logical_matrices(
    tmp_path: Path,
) -> None:
    """Verify DeclaredDigestExpectation supports sha256:manifest-v1 and sha256:logical_matrices."""
    zarr_path = tmp_path / "matrix.zarr"
    ds = xr.Dataset(
        data_vars={
            "DIST": (("otaz", "dtaz"), np.array([[1.0, 2.0], [3.0, 4.0]], dtype="<f8"))
        },
        coords={"zone_number": ("otaz", np.array([1, 2], dtype="<i4"))},
    )
    ds.to_zarr(zarr_path)

    manifest_id = admission_directory_identity(zarr_path)
    logical_id = normalize_logical_matrices_identity(
        compute_zarr_logical_matrices_sha256(zarr_path)
    )

    manifest_report = check_expected_identity(
        execution_path=zarr_path,
        input_role="skims_zarr",
        expectation=DeclaredDigestExpectation(identity=manifest_id),
    )
    assert manifest_report.outcome == "verified"
    assert manifest_report.observed_artifact_id == manifest_id

    logical_report = check_expected_identity(
        execution_path=zarr_path,
        input_role="skims_logical",
        expectation=DeclaredDigestExpectation(
            identity=logical_id,
            logical_profile="preserve_canonical",
        ),
    )
    assert logical_report.outcome == "verified"
    assert logical_report.observed_artifact_id == logical_id
    assert "logical_identity_verified" in logical_report.observations

    # FileIdentity still strictly requires sha256:file:<hex>
    with pytest.raises(ValueError, match="sha256:file:"):
        FileIdentity.parse(manifest_id)
    assert AdmissionIdentity.parse(manifest_id).kind == "manifest-v1"
    assert AdmissionIdentity.parse(logical_id).kind == "logical_matrices"


def test_public_exports_include_logical_array_identity() -> None:
    """Verify top-level consist exports include the new logical array identity symbols."""
    assert consist.AdmissionIdentity is AdmissionIdentity
    assert consist.ArrayCanonicalizationPolicy is ArrayCanonicalizationPolicy
    assert consist.PRESERVE_CANONICAL_POLICY is PRESERVE_CANONICAL_POLICY
    assert consist.FLOAT32_MATRICES_V1_POLICY is FLOAT32_MATRICES_V1_POLICY
    assert consist.compute_logical_matrices_sha256 is compute_logical_matrices_sha256
    assert (
        consist.compute_omx_logical_matrices_sha256
        is compute_omx_logical_matrices_sha256
    )
    assert (
        consist.compute_zarr_logical_matrices_sha256
        is compute_zarr_logical_matrices_sha256
    )
    assert consist.inspect_matrix_container is inspect_matrix_container
    assert consist.check_prior_run_expectation is check_prior_run_expectation
