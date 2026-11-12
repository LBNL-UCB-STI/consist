"""Regression tests for non-ORM value models, unified scheme digests, and layering."""

from __future__ import annotations

import ast
import copy
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import pickle
import subprocess
import sys
import tempfile
import time
from types import MappingProxyType
import uuid

import duckdb
import h5py
import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
import pytest

import consist
import consist.api as consist_api
from consist.core.db_runtime import DB_CALL_PROFILER
from consist.core.maintenance import DatabaseMaintenance
from consist.core.tracker import Tracker
from consist.digest import (
    DIGEST_DRIVERS,
    FLOAT32_MATRICES_V1_POLICY,
    PRESERVE_CANONICAL_POLICY,
    ConcurrentMutationError,
    Digest,
    _run_fd_pinned,
    build_manifest_v1,
    compute_ctd1_digest,
    compute_digest,
    compute_output_set_digest,
    verify_digest,
)
import consist.models as consist_models
from consist.models.artifact import Artifact
from consist.models.run import Run
from consist.protocols import (
    ArtifactLike,
    ArtifactRecordLike,
    ArtifactResolver,
    bind_active_resolver,
    get_active_resolver,
)
from consist.types import FilenamePattern, IntCapture, OutputSet


# ---------------------------------------------------------------------------
# 1. Digest Value Object & Leaf Primitives
# ---------------------------------------------------------------------------


class TestDigestAndLeafPrimitives:
    """Verify Digest parsing, export surfaces, fd-pinned race guard, ctd1, and manifest v1."""

    @pytest.mark.parametrize(
        "scheme",
        [
            "sha256:file",
            "ctd1:sha256",
            "dir_manifest_v1:sha256",
            "h5_fixed_v1:sha256",
            "h5_dataset_v1:sha256",
            "logical_matrices_canonical_v1:sha256",
            "logical_matrices_f32_v1:sha256",
            "output_set_v1:sha256",
        ],
    )
    def test_digest_parse_preserves_multi_colon_schemes(self, scheme: str) -> None:
        hex_val = "0123456789abcdef" * 4
        raw = f"{scheme}:{hex_val}"
        parsed = Digest.parse(raw)
        assert parsed == Digest(scheme=scheme, hex=hex_val)
        assert str(parsed) == raw
        assert Digest.parse(parsed) is parsed
        assert scheme in DIGEST_DRIVERS

    def test_digest_parse_normalizes_legacy_manifest_v1_scheme(self) -> None:
        hex_val = "ab" * 32
        parsed = Digest.parse(f"sha256:manifest-v1:{hex_val}")
        assert parsed == Digest(scheme="dir_manifest_v1:sha256", hex=hex_val)

    @pytest.mark.parametrize(
        "invalid_raw",
        [
            "",
            "abc",
            f":{'a' * 64}",
            f"sha256:file:{'A' * 64}",
            f"sha256:file:{'a' * 63}",
            f"sha256:file:{'g' * 64}",
            f"bad scheme:{'a' * 64}",
        ],
    )
    def test_digest_parse_rejects_malformed_inputs(self, invalid_raw: str) -> None:
        with pytest.raises(ValueError):
            Digest.parse(invalid_raw)

    def test_digest_exported_across_public_namespaces(self) -> None:
        assert consist.Digest is Digest
        assert consist_api.Digest is Digest
        assert consist_models.Digest is Digest

    def test_run_fd_pinned_raises_on_mid_read_replacement(self, tmp_path: Path) -> None:
        target = tmp_path / "mutable.bin"
        target.write_bytes(b"initial-payload")

        def _replace_during_read(fd: int) -> bytes:
            chunk = os.read(fd, 7)
            replacement = tmp_path / "replacement.bin"
            replacement.write_bytes(b"replaced-payload-longer")
            os.replace(replacement, target)
            return chunk

        with pytest.raises(ConcurrentMutationError):
            _run_fd_pinned(target, _replace_during_read)

    def test_compute_ctd1_digest_csv_and_parquet_parity(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            {
                "zone_id": [10, 20, 30],
                "trips": [1.5, 2.25, 0.0],
                "mode": ["walk", "transit", "auto"],
            }
        )
        csv_path = tmp_path / "trips.csv"
        pq_path = tmp_path / "trips.parquet"
        df.to_csv(csv_path, index=False)
        df.to_parquet(pq_path, index=False)

        csv_digest = compute_ctd1_digest(csv_path)
        pq_digest = compute_ctd1_digest(pq_path)
        via_registry = compute_digest(
            pq_path, driver="parquet", options={"scheme": "ctd1:sha256"}
        )

        assert csv_digest.scheme == "ctd1:sha256"
        assert csv_digest == pq_digest
        assert via_registry == pq_digest
        assert verify_digest(csv_path, pq_digest, driver="csv") is True

    def test_build_manifest_v1_deterministic_and_sensitive_to_changes(
        self, tmp_path: Path
    ) -> None:
        dir_a = tmp_path / "dir_a"
        dir_b = tmp_path / "dir_b"
        for root in (dir_a, dir_b):
            (root / "nested").mkdir(parents=True)
            (root / "alpha.txt").write_text("hello", encoding="utf-8")
            (root / "nested" / "beta.bin").write_bytes(b"\x00\x01\x02")

        manifest_a = build_manifest_v1(dir_a)
        manifest_b = build_manifest_v1(dir_b)
        assert manifest_a == manifest_b
        assert compute_digest(dir_a, driver="zarr") == compute_digest(
            dir_b, driver="zarr"
        )

        (dir_b / "nested" / "beta.bin").write_bytes(b"\x00\x01\x03")
        assert compute_digest(dir_a, driver="zarr") != compute_digest(
            dir_b, driver="zarr"
        )


# ---------------------------------------------------------------------------
# 2. Unified Scheme Registry & Format Specifics
# ---------------------------------------------------------------------------


class TestUnifiedSchemeRegistry:
    """Verify creation and verification share exact scheme drivers without collisions."""

    def test_h5_fixed_table_hashes_all_blocks_axes_and_table_format(
        self, tmp_path: Path
    ) -> None:
        base_h5 = tmp_path / "base.h5"
        edited_str_h5 = tmp_path / "edited_str.h5"
        edited_idx_h5 = tmp_path / "edited_idx.h5"
        table_fmt_a = tmp_path / "table_a.h5"
        table_fmt_b = tmp_path / "table_b.h5"

        df_base = pd.DataFrame(
            {
                "count": np.array([1, 2, 3], dtype=np.int64),
                "weight": np.array([0.25, 0.5, 0.75], dtype=np.float64),
                "label": ["alpha", "beta", "gamma"],
            },
            index=pd.Index(["r1", "r2", "r3"], name="row_key"),
        )
        df_str_changed = df_base.copy()
        df_str_changed["label"] = ["alpha", "BETA_CHANGED", "gamma"]

        df_idx_changed = df_base.copy()
        df_idx_changed.index = pd.Index(["r1", "r2", "r999"], name="row_key")

        df_base.to_hdf(base_h5, key="households", mode="w", format="fixed")
        df_str_changed.to_hdf(edited_str_h5, key="households", mode="w", format="fixed")
        df_idx_changed.to_hdf(edited_idx_h5, key="households", mode="w", format="fixed")

        d_base = compute_digest(base_h5, driver="h5_table", member="/households")
        d_str = compute_digest(edited_str_h5, driver="h5_table", member="/households")
        d_idx = compute_digest(edited_idx_h5, driver="h5_table", member="/households")

        assert d_base.scheme == "h5_fixed_v1:sha256"
        assert d_base != d_str
        assert d_base != d_idx
        assert verify_digest(base_h5, d_base, driver="h5_table", member="/households")
        assert not verify_digest(
            edited_str_h5, d_base, driver="h5_table", member="/households"
        )
        assert not verify_digest(
            edited_idx_h5, d_base, driver="h5_table", member="/households"
        )

        # Also verify PyTables 'table' format deterministically hashes rows & columns
        df_base.to_hdf(table_fmt_a, key="households", mode="w", format="table")
        df_str_changed.to_hdf(table_fmt_b, key="households", mode="w", format="table")
        d_tbl_a = compute_digest(table_fmt_a, driver="h5_table", member="/households")
        d_tbl_b = compute_digest(table_fmt_b, driver="h5_table", member="/households")
        assert d_tbl_a.scheme == "h5_fixed_v1:sha256"
        assert d_tbl_a != d_tbl_b
        assert verify_digest(
            table_fmt_a, d_tbl_a, driver="h5_table", member="/households"
        )

    def test_raw_h5_dataset_uses_h5_dataset_v1_and_detects_mutations(
        self, tmp_path: Path
    ) -> None:
        h5_a = tmp_path / "matrix_a.h5"
        h5_dtype = tmp_path / "matrix_dtype.h5"
        h5_attr = tmp_path / "matrix_attr.h5"

        with h5py.File(h5_a, "w") as f:
            ds = f.create_dataset(
                "/skims/time", data=np.arange(6, dtype=np.float32).reshape(2, 3)
            )
            ds.attrs["units"] = "minutes"

        with h5py.File(h5_dtype, "w") as f:
            ds = f.create_dataset(
                "/skims/time", data=np.arange(6, dtype=np.float64).reshape(2, 3)
            )
            ds.attrs["units"] = "minutes"

        with h5py.File(h5_attr, "w") as f:
            ds = f.create_dataset(
                "/skims/time", data=np.arange(6, dtype=np.float32).reshape(2, 3)
            )
            ds.attrs["units"] = "seconds"

        d_a = compute_digest(h5_a, driver="h5", member="/skims/time")
        assert d_a.scheme == "h5_dataset_v1:sha256"
        assert d_a.scheme != "ctd1:sha256"
        assert verify_digest(h5_a, d_a, driver="h5", member="/skims/time") is True
        assert verify_digest(h5_dtype, d_a, driver="h5", member="/skims/time") is False
        assert verify_digest(h5_attr, d_a, driver="h5", member="/skims/time") is False

    def test_output_set_v1_tracker_creation_and_verification(
        self, tmp_path: Path
    ) -> None:
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
        )
        bundle_dir = tmp_path / "bundle_out"

        def _produce_bundle() -> None:
            bundle_dir.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "part_01.csv").write_text("id,val\n1,10\n", encoding="utf-8")
            (bundle_dir / "part_02.csv").write_text("id,val\n2,20\n", encoding="utf-8")

        res = tracker.run(
            fn=_produce_bundle,
            name="produce_bundle",
            output_sets={"parts": OutputSet(root=bundle_dir, include="*.csv")},
        )
        parent_art = res.outputs["parts"]
        assert parent_art.digest is not None
        assert parent_art.digest.scheme == "output_set_v1:sha256"

        member_paths = parent_art.meta["output_set_member_paths"]
        assert verify_digest(
            bundle_dir,
            parent_art.digest,
            driver="artifact_set",
            options={"member_paths": member_paths},
        )

        # Order-insensitive member digest computation
        d1 = compute_output_set_digest(
            [("b", f"sha256:file:{'1' * 64}"), ("a", f"sha256:file:{'2' * 64}")]
        )
        d2 = compute_output_set_digest(
            [("a", f"sha256:file:{'2' * 64}"), ("b", f"sha256:file:{'1' * 64}")]
        )
        assert d1 == d2

        # Mutating one member invalidates verification
        (bundle_dir / "part_02.csv").write_text("id,val\n2,999\n", encoding="utf-8")
        assert not verify_digest(
            bundle_dir,
            parent_art.digest,
            driver="artifact_set",
            options={"member_paths": member_paths},
        )
        tracker.close()

    def test_logical_matrix_schemes_distinct_between_canonical_and_f32(
        self, tmp_path: Path
    ) -> None:
        omx_f32 = tmp_path / "skims_f32.omx"
        omx_f64 = tmp_path / "skims_f64.omx"

        arr_f32 = np.array([[1.0, 2.5], [3.25, 4.0]], dtype=np.float32)
        arr_f64 = arr_f32.astype(np.float64)

        for target, arr in ((omx_f32, arr_f32), (omx_f64, arr_f64)):
            with h5py.File(target, "w") as f:
                f.attrs["OMX_VERSION"] = "0.2"
                f.attrs["SHAPE"] = np.array([2, 2], dtype=np.int32)
                data_grp = f.create_group("data")
                data_grp.create_dataset("time", data=arr)

        canonical_f32 = compute_digest(
            omx_f32,
            driver="openmatrix",
            options={
                "scheme": "logical_matrices_canonical_v1:sha256",
                "policy": PRESERVE_CANONICAL_POLICY,
            },
        )
        canonical_f64 = compute_digest(
            omx_f64,
            driver="openmatrix",
            options={
                "scheme": "logical_matrices_canonical_v1:sha256",
                "policy": PRESERVE_CANONICAL_POLICY,
            },
        )
        logical_f32_from_f32 = compute_digest(
            omx_f32,
            driver="openmatrix",
            options={
                "scheme": "logical_matrices_f32_v1:sha256",
                "policy": FLOAT32_MATRICES_V1_POLICY,
            },
        )
        logical_f32_from_f64 = compute_digest(
            omx_f64,
            driver="openmatrix",
            options={
                "scheme": "logical_matrices_f32_v1:sha256",
                "policy": FLOAT32_MATRICES_V1_POLICY,
            },
        )

        assert canonical_f32.scheme == "logical_matrices_canonical_v1:sha256"
        assert logical_f32_from_f32.scheme == "logical_matrices_f32_v1:sha256"
        assert canonical_f32 != logical_f32_from_f32
        assert canonical_f32 != canonical_f64
        assert logical_f32_from_f32 == logical_f32_from_f64

    def test_shapefile_bundle_digest_tracks_sidecars(self, tmp_path: Path) -> None:
        shp = tmp_path / "zones.shp"
        shx = tmp_path / "zones.shx"
        dbf = tmp_path / "zones.dbf"
        prj = tmp_path / "zones.prj"
        shp.write_bytes(b"shp-bytes")
        shx.write_bytes(b"shx-bytes")
        dbf.write_bytes(b"dbf-bytes-v1")
        prj.write_text("EPSG:4326", encoding="utf-8")

        digest = compute_digest(
            shp, driver="shapefile", options={"shapefile_bundle": True}
        )
        assert digest.scheme == "dir_manifest_v1:sha256"
        assert verify_digest(shp, digest, driver="shapefile") is True

        dbf.write_bytes(b"dbf-bytes-v2")
        assert verify_digest(shp, digest, driver="shapefile") is False


# ---------------------------------------------------------------------------
# 3. Separate Raw File Digest from Post-dlt SQL Table State
# ---------------------------------------------------------------------------


class _HouseholdRow(BaseModel):
    model_config = ConfigDict(json_schema_extra={"tablename": "households_ingested"})

    hh_id: int = Field(json_schema_extra={"primary_key": True})
    persons: int
    incomeSegment: str


class TestDltIngestPreservesArtifactDigest:
    """Verify dlt ingestion never overwrites art.digest or stamps SQL table ctd1."""

    def test_ingest_keeps_file_digest_untouched(self, tmp_path: Path) -> None:
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
            schemas=[_HouseholdRow],
        )
        pq_path = tmp_path / "households.parquet"
        df = pd.DataFrame(
            {
                "hh_id": [1, 2],
                "persons": [3, 1],
                "incomeSegment": ["mid", "high"],
            }
        )
        df.to_parquet(pq_path, index=False)

        with tracker.start_run("ingest_run", model="demo"):
            art = tracker.log_artifact(pq_path, key="households", direction="output")
            original_digest = art.digest
            assert original_digest is not None
            assert original_digest.scheme == "sha256:file"

            tracker.ingest(art, schema=_HouseholdRow)

            refreshed_mem = tracker.current_consist.outputs[0]
            assert refreshed_mem.digest == original_digest
            assert refreshed_mem.digest.scheme == "sha256:file"

        persisted = tracker.db.get_artifact(art.id)
        assert persisted is not None
        assert persisted.digest == original_digest
        assert persisted.meta.get("is_ingested") is True
        tracker.close()


# ---------------------------------------------------------------------------
# 4. Artifact Immutability, Construction Speed, Layering DAG, & Protocols
# ---------------------------------------------------------------------------


class TestArtifactImmutabilityLayeringAndProtocols:
    """Verify Artifact immutability, speed, strict layering, and typing_extensions protocols."""

    def test_artifact_immutability_equality_copy_pickle_and_speed(self) -> None:
        caller_meta = {"nested": 1, "tags": ["a"]}
        digest = Digest("sha256:file", "ab" * 32)
        fixed_id = uuid.uuid4()
        fixed_ts = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)

        art = Artifact(
            id=fixed_id,
            key="trips",
            uri="inputs://trips.parquet",
            driver="parquet",
            digest=digest,
            member=None,
            run_id="run_1",
            parent_artifact_id=None,
            facet_id="facet_1",
            created_at=fixed_ts,
            meta=caller_meta,
        )

        # Defensive copy + MappingProxyType
        caller_meta["nested"] = 999
        assert isinstance(art.meta, MappingProxyType)
        assert art.meta["nested"] == 1
        assert not isinstance(art.meta, dict)
        with pytest.raises(TypeError):
            art.meta["new_key"] = 2  # type: ignore[index]

        # Frozen fields & non-field attributes
        with pytest.raises(FrozenInstanceError):
            art.key = "mutated"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            art.abs_path = "/tmp/override"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            art.nonexistent_attr = 123  # type: ignore[attr-defined]

        # 11-field value equality
        same = Artifact(
            id=fixed_id,
            key="trips",
            uri="inputs://trips.parquet",
            driver="parquet",
            digest=digest,
            member=None,
            run_id="run_1",
            parent_artifact_id=None,
            facet_id="facet_1",
            created_at=fixed_ts,
            meta={"nested": 1, "tags": ["a"]},
        )
        diff_meta = same.with_meta(nested=2)
        assert art == same
        assert art != diff_meta

        # copy / deepcopy / pickle round-trip
        assert copy.copy(art) == art
        assert copy.deepcopy(art) == art
        unpickled = pickle.loads(pickle.dumps(art))
        assert unpickled == art
        assert isinstance(unpickled.meta, MappingProxyType)

        # Construction speed sanity bound (< 25 us even under py3.11 sys.settrace --cov; ~1.3 us uninstrumented)
        iterations = 4000
        best_per_item_us = float("inf")
        for _ in range(3):
            t0 = time.perf_counter()
            for _ in range(iterations):
                Artifact(
                    id=fixed_id,
                    key="trips",
                    uri="inputs://trips.parquet",
                    driver="parquet",
                    digest=digest,
                    created_at=fixed_ts,
                )
            elapsed_us = ((time.perf_counter() - t0) * 1_000_000.0) / iterations
            best_per_item_us = min(best_per_item_us, elapsed_us)
        assert best_per_item_us < 25.0, f"Artifact init took {best_per_item_us:.2f} us"

    def test_strict_layer1_and_layer2_import_dag(self) -> None:
        src_root = Path(consist.__file__).resolve().parent

        layer1_files = [
            src_root / "digest.py",
            src_root / "types.py",
            src_root / "protocols.py",
        ]
        for path in layer1_files:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            imported_modules = _collect_all_imports(tree)
            forbidden = [
                mod
                for mod in imported_modules
                if mod.startswith(("consist.models", "consist.core", "consist.api"))
            ]
            assert forbidden == [], (
                f"Layer-1 {path.name} has upward imports: {forbidden}"
            )

        layer2_files = [
            src_root / "models" / "artifact.py",
            src_root / "models" / "run.py",
            src_root / "models" / "config_facet.py",
            src_root / "models" / "run_config_kv.py",
            src_root / "models" / "artifact_facet.py",
            src_root / "models" / "artifact_kv.py",
            src_root / "models" / "artifact_schema.py",
        ]
        for path in layer2_files:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            runtime_imports = _collect_runtime_imports(tree)
            forbidden = [
                mod
                for mod in runtime_imports
                if mod.startswith(
                    ("consist.core", "consist.api", "sqlalchemy", "sqlmodel")
                )
            ]
            assert forbidden == [], (
                f"Layer-2 {path.name} has forbidden runtime imports: {forbidden}"
            )

    def test_all_runtime_checkable_protocols_use_typing_extensions(self) -> None:
        src_root = Path(consist.__file__).resolve().parent
        violations: list[str] = []

        for py_file in sorted(src_root.rglob("*.py")):
            source = py_file.read_text(encoding="utf-8")
            if "runtime_checkable" not in source:
                continue
            tree = ast.parse(source, filename=str(py_file))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module == "typing":
                    imported_names = {alias.name for alias in node.names}
                    bad = imported_names & {"Protocol", "runtime_checkable"}
                    if bad:
                        violations.append(
                            f"{py_file.relative_to(src_root)}: {sorted(bad)}"
                        )

        assert violations == []

        class _RaisingPathRecord:
            id = uuid.uuid4()
            key = "k"
            digest = None
            member = None
            meta: MappingProxyType[str, object] = MappingProxyType({})

            @property
            def path(self) -> Path:
                raise RuntimeError("isinstance must never evaluate .path")

            @property
            def uri(self) -> str:
                return "workspace://k.parquet"

            @property
            def driver(self) -> str:
                return "parquet"

            @property
            def abs_path(self) -> str | None:
                raise RuntimeError("isinstance must never evaluate .abs_path")

        sample = _RaisingPathRecord()
        assert isinstance(sample, ArtifactLike)
        assert isinstance(sample, ArtifactRecordLike)


def _collect_all_imports(tree: ast.AST) -> list[str]:
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def _collect_runtime_imports(tree: ast.Module) -> list[str]:
    modules: list[str] = []

    def _visit_node(node: ast.AST) -> None:
        if isinstance(node, ast.If) and _is_type_checking_guard(node.test):
            for else_stmt in node.orelse:
                _visit_node(else_stmt)
            return
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
            return
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
            return
        for child in ast.iter_child_nodes(node):
            _visit_node(child)

    _visit_node(tree)
    return modules


def _is_type_checking_guard(test: ast.expr) -> bool:
    if isinstance(test, ast.Name) and test.id == "TYPE_CHECKING":
        return True
    if isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING":
        return True
    return False


# ---------------------------------------------------------------------------
# 5. First-Class pydantic.BaseModel User Row Schemas
# ---------------------------------------------------------------------------


class TripRowBaseModel(BaseModel):
    model_config = ConfigDict(json_schema_extra={"tablename": "trips_bm"})

    trip_id: int = Field(json_schema_extra={"primary_key": True})
    origin_taz: int
    purpose: str
    distance_km: float


class TestPydanticBaseModelUserSchemas:
    """Verify plain pydantic.BaseModel row schemas work across Tracker, dlt, views, and stub export."""

    def test_basemodel_end_to_end_ingest_hybrid_view_grouped_view_and_stub(
        self, tmp_path: Path
    ) -> None:
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
            schemas=[TripRowBaseModel],
        )
        assert isinstance(tracker, ArtifactResolver)

        hot_pq = tmp_path / "trips_hot.parquet"
        cold_pq = tmp_path / "trips_cold.parquet"
        pd.DataFrame(
            {
                "trip_id": [1, 2],
                "origin_taz": [101, 102],
                "purpose": ["work", "shop"],
                "distance_km": [5.5, 2.0],
            }
        ).to_parquet(hot_pq, index=False)
        pd.DataFrame(
            {
                "trip_id": [3],
                "origin_taz": [103],
                "purpose": ["school"],
                "distance_km": [3.25],
            }
        ).to_parquet(cold_pq, index=False)

        with tracker.start_run("hot_trip_run", model="sim", config={"year": 2025}):
            hot_art = tracker.log_artifact(
                hot_pq,
                key="trips_bm",
                direction="output",
                schema=TripRowBaseModel,
                profile_file_schema=True,
                facet={"year": 2025},
                facet_index=True,
            )
            tracker.ingest(hot_art, schema=TripRowBaseModel)

        with tracker.start_run("cold_trip_run", model="sim", config={"year": 2030}):
            cold_art = tracker.log_artifact(
                cold_pq,
                key="trips_bm",
                direction="output",
                schema=TripRowBaseModel,
                profile_file_schema=True,
                facet={"year": 2030},
                facet_index=True,
            )

        # Hybrid view combines hot ingested rows + cold Parquet rows
        tracker.create_view("v_trips_bm", "trips_bm")
        rows = tracker.db.fetchall(
            "SELECT trip_id, purpose FROM v_trips_bm ORDER BY trip_id"
        )
        assert rows == [(1, "work"), (2, "shop"), (3, "school")]

        # Grouped view with plain BaseModel schema
        tracker.create_grouped_view(
            "v_trips_by_year",
            schema=TripRowBaseModel,
            attach_facets=["year"],
        )
        grouped = tracker.db.fetchall(
            "SELECT facet_year, COUNT(*) FROM v_trips_by_year GROUP BY facet_year ORDER BY facet_year"
        )
        assert grouped == [(2025, 2), (2030, 1)]

        # Schema stub export from captured observation
        schema_id = cold_art.meta.get("schema_id") or hot_art.meta.get("schema_id")
        assert isinstance(schema_id, str)
        stub_code = tracker.export_schema_sqlmodel(
            schema_id=schema_id, class_name="TripStub"
        )
        assert "class TripStub" in stub_code
        assert "trip_id" in stub_code
        assert "distance_km" in stub_code
        tracker.close()


# ---------------------------------------------------------------------------
# 6. Zero-SQL consist.json Flush & v0.4 / v0.5 rebuild_from_json Round-Trip
# ---------------------------------------------------------------------------


class TestProvenanceJsonZeroSqlAndRebuildRoundTrip:
    """Verify <1ms 0-SQL JSON flush and v0.4 + v0.5 rebuild_from_json fidelity."""

    def test_flush_json_executes_zero_sql_queries_and_sub_millisecond_serialization(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
        )
        sample_csv = tmp_path / "sample.csv"
        sample_csv.write_text("a,b\n1,2\n", encoding="utf-8")

        with tracker.start_run("json_bench_run", model="bench"):
            for idx in range(5):
                tracker.log_artifact(sample_csv, key=f"out_{idx}", direction="output")
            record = tracker.current_consist
            assert record is not None

            # Enable DB_CALL_PROFILER and forbid any DB method execution during JSON dump/flush
            prev_enabled = DB_CALL_PROFILER.enabled
            DB_CALL_PROFILER.enabled = True
            with DB_CALL_PROFILER._lock:
                before_counts = {k: v.count for k, v in DB_CALL_PROFILER._calls.items()}

            def _forbid_sql(*_args: object, **_kwargs: object) -> object:
                raise AssertionError(
                    "SQL query executed during JSON serialization/flush!"
                )

            monkeypatch.setattr(tracker.db, "execute", _forbid_sql)
            monkeypatch.setattr(tracker.db, "fetchall", _forbid_sql)
            monkeypatch.setattr(tracker.db, "fetchone", _forbid_sql)

            try:
                # Pure serialization latency (< 5 ms even under py3.11 --cov)
                iterations = 200
                t0 = time.perf_counter()
                for _ in range(iterations):
                    payload_str = record.model_dump_json()
                avg_dump_ms = ((time.perf_counter() - t0) * 1000.0) / iterations
                assert avg_dump_ms < 5.0, (
                    f"record.model_dump_json() took {avg_dump_ms:.3f} ms"
                )
                parsed = json.loads(payload_str)
                assert parsed["outputs"][0]["digest"].startswith("sha256:file:")
                assert parsed["outputs"][0]["uri"]

                # Full ProvenanceWriter.flush_json() executes 0 SQL queries and stays fast
                flush_iters = 25
                best_flush_ms = float("inf")
                for _ in range(flush_iters):
                    t_flush = time.perf_counter()
                    tracker.persistence.flush_json()
                    best_flush_ms = min(
                        best_flush_ms, (time.perf_counter() - t_flush) * 1000.0
                    )
                assert best_flush_ms < 10.0, (
                    f"flush_json() best time was {best_flush_ms:.3f} ms"
                )

                with DB_CALL_PROFILER._lock:
                    after_counts = {
                        k: v.count for k, v in DB_CALL_PROFILER._calls.items()
                    }
                assert after_counts == before_counts
            finally:
                DB_CALL_PROFILER.enabled = prev_enabled
                monkeypatch.undo()

        tracker.close()

    def test_rebuild_from_json_round_trips_both_v04_and_v05_snapshots(
        self, tmp_path: Path
    ) -> None:
        json_dir = tmp_path / "snapshots"
        json_dir.mkdir(parents=True)

        v04_art_id = uuid.uuid4()
        v04_zarr_id = uuid.uuid4()
        v05_art_id = uuid.uuid4()
        hex_a = "11" * 32
        hex_b = "22" * 32
        hex_c = "33" * 32

        v04_payload = {
            "run": Run(
                id="legacy_v04_run",
                model_name="legacy_model",
                status="completed",
                config_hash="aa" * 32,
                git_hash="bb" * 32,
            ).model_dump(mode="json"),
            "inputs": [],
            "outputs": [
                {
                    "id": str(v04_art_id),
                    "key": "households",
                    "container_uri": "outputs://legacy.h5",
                    "driver": "h5_table",
                    "hash": hex_a,
                    "table_path": "/households",
                    "array_path": None,
                    "run_id": "legacy_v04_run",
                    "meta": {"source": "v04"},
                    "created_at": "2026-01-15T00:00:00+00:00",
                },
                {
                    "id": str(v04_zarr_id),
                    "key": "skims_zarr",
                    "container_uri": "outputs://skims.zarr",
                    "driver": "zarr",
                    "hash": f"sha256:manifest-v1:{hex_b}",
                    "table_path": None,
                    "array_path": "/skims/am",
                    "run_id": "legacy_v04_run",
                    "meta": {"directory_artifact": True},
                    "created_at": "2026-01-15T00:00:00+00:00",
                },
            ],
            "config": {"seed": 42},
            "facet": {},
        }

        v05_art = Artifact(
            id=v05_art_id,
            key="modern_table",
            uri="outputs://modern.h5",
            driver="h5_table",
            digest=Digest("h5_fixed_v1:sha256", hex_c),
            member="/modern_table",
            run_id="modern_v05_run",
            meta={"source": "v05"},
        )
        v05_payload = {
            "run": Run(
                id="modern_v05_run",
                model_name="modern_model",
                status="completed",
                config_hash="cc" * 32,
                git_hash="dd" * 32,
            ).model_dump(mode="json"),
            "inputs": [],
            "outputs": [v05_art.to_json_dict()],
            "config": {"seed": 99},
            "facet": {},
        }

        (json_dir / "01_v04.json").write_text(
            json.dumps(v04_payload, indent=2), encoding="utf-8"
        )
        (json_dir / "02_v05.json").write_text(
            json.dumps(v05_payload, indent=2), encoding="utf-8"
        )

        rebuilt_db_path = tmp_path / "rebuilt.duckdb"
        tracker = Tracker(
            run_dir=tmp_path / "rebuilt_runs", db_path=str(rebuilt_db_path)
        )
        maint = DatabaseMaintenance(db=tracker.db, run_dir=tmp_path / "rebuilt_runs")
        result = maint.rebuild_from_json(json_dir, dry_run=False)

        assert result.errors == []
        assert result.runs_inserted == 2
        assert result.artifacts_inserted == 3

        art_v04 = tracker.db.get_artifact(v04_art_id)
        assert art_v04 is not None
        assert art_v04.uri == "outputs://legacy.h5"
        assert art_v04.member == "/households"
        assert art_v04.digest == Digest("h5_fixed_v1:sha256", hex_a)
        assert art_v04.meta["source"] == "v04"

        art_v04_zarr = tracker.db.get_artifact(v04_zarr_id)
        assert art_v04_zarr is not None
        assert art_v04_zarr.uri == "outputs://skims.zarr"
        assert art_v04_zarr.member == "/skims/am"
        assert art_v04_zarr.digest == Digest("dir_manifest_v1:sha256", hex_b)

        art_v05 = tracker.db.get_artifact(v05_art_id)
        assert art_v05 is not None
        assert art_v05.uri == "outputs://modern.h5"
        assert art_v05.member == "/modern_table"
        assert art_v05.digest == Digest("h5_fixed_v1:sha256", hex_c)
        assert art_v05.meta["source"] == "v05"
        tracker.close()


# ---------------------------------------------------------------------------
# 7. Edge-Case Hardening, Recovery Roots, Admission, & Import Isolation
# ---------------------------------------------------------------------------


class TestEdgeCaseHardeningAndIsolation:
    """Verify cache error guards, directory/output_set admission, recovery roots, and lazy SQLModel imports."""

    def test_error_input_hash_prevents_false_cache_hits_and_remote_caller_supplied_hashes_deterministically(
        self, tmp_path: Path
    ) -> None:
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
        )
        missing_a = Artifact(
            key="ext_a",
            uri="inputs://missing_a.parquet",
            driver="parquet",
            digest=None,
        )
        missing_b = Artifact(
            key="ext_b",
            uri="inputs://missing_b.parquet",
            driver="parquet",
            digest=None,
        )

        with tracker.start_run(
            "missing_run_1",
            model="model_alpha",
            inputs=[missing_a],
            cache_mode="reuse",
        ) as t1:
            assert not t1.is_cached
            run1 = t1.current_consist.run
            assert run1.input_hash == "error"
            assert run1.signature is None

        persisted_1 = tracker.get_run("missing_run_1")
        assert persisted_1 is not None
        assert persisted_1.signature is None
        assert tracker.db.find_matching_run("error", "error", "error") is None
        assert tracker.db.find_run_by_signature("error") is None

        with tracker.start_run(
            "missing_run_2",
            model="model_beta",
            inputs=[missing_b],
            cache_mode="reuse",
        ) as t2:
            assert not t2.is_cached
            assert t2.current_consist.run.signature is None

        # Remote s3:// external Artifact with caller_supplied_v1:sha256 hashes deterministically
        remote_1 = Artifact(
            key="remote_skims",
            uri="s3://sim-bucket/skims_v1.parquet",
            driver="parquet",
            digest=Digest("caller_supplied_v1:sha256", "ab" * 32),
        )
        remote_2 = Artifact(
            key="remote_skims",
            uri="s3://sim-bucket/skims_v2.parquet",
            driver="parquet",
            digest=Digest("caller_supplied_v1:sha256", "ab" * 32),
        )
        h1 = tracker.identity.compute_input_hash([remote_1])
        h1_repeat = tracker.identity.compute_input_hash([remote_1])
        h2 = tracker.identity.compute_input_hash([remote_2])
        assert h1 != "error"
        assert h1 == h1_repeat
        assert h1 != h2
        tracker.close()

    def test_check_artifact_identity_verifies_zarr_directory_and_output_set_artifacts(
        self, tmp_path: Path
    ) -> None:
        from consist.core.admission import check_artifact_identity

        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
        )
        zarr_dir = tracker.run_dir / "outputs" / "skims.zarr"
        zarr_dir.mkdir(parents=True)
        (zarr_dir / ".zgroup").write_text('{"zarr_format": 2}\n', encoding="utf-8")
        (zarr_dir / "0.0").write_bytes(b"\x01\x02\x03\x04")

        plain_dir = tracker.run_dir / "outputs" / "bundle_dir"
        plain_dir.mkdir(parents=True)
        (plain_dir / "part_0.csv").write_text("id,val\n1,10\n", encoding="utf-8")

        outset_dir = tracker.run_dir / "outputs" / "annual_set"
        outset_dir.mkdir(parents=True)
        (outset_dir / "year_2025.csv").write_text("v\n1\n", encoding="utf-8")
        (outset_dir / "year_2030.csv").write_text("v\n2\n", encoding="utf-8")

        def _produce() -> None:
            return None

        result = tracker.run(
            fn=_produce,
            name="produce_dirs",
            output_paths={"skims": zarr_dir},
            output_sets={
                "annual": OutputSet(
                    root=outset_dir,
                    include=FilenamePattern.glob("year_*.csv").with_captures(
                        IntCapture(name="year", wildcard=1)
                    ),
                )
            },
        )
        with tracker.start_run("produce_plain_dir", model="dir_step"):
            dir_art = tracker.log_output(
                plain_dir, key="bundle_dir", directory_artifact=True
            )

        assert dir_art.key == "bundle_dir"

        assert (
            check_artifact_identity(
                execution_path=zarr_dir,
                expected_run_id=result.run.id,
                artifact_key="skims",
                tracker=tracker,
            ).outcome
            == "verified"
        )
        assert (
            check_artifact_identity(
                execution_path=plain_dir,
                expected_run_id="produce_plain_dir",
                artifact_key="bundle_dir",
                tracker=tracker,
            ).outcome
            == "verified"
        )
        assert (
            check_artifact_identity(
                execution_path=outset_dir,
                expected_run_id=result.run.id,
                artifact_key="annual",
                tracker=tracker,
            ).outcome
            == "verified"
        )

        # Mutate each on disk and confirm mismatch detection
        (zarr_dir / "0.0").write_bytes(b"\x99\x99")
        (plain_dir / "part_0.csv").write_text("id,val\n1,999\n", encoding="utf-8")
        (outset_dir / "year_2030.csv").write_text("v\n999\n", encoding="utf-8")

        assert (
            check_artifact_identity(
                execution_path=zarr_dir,
                expected_run_id=result.run.id,
                artifact_key="skims",
                tracker=tracker,
            ).outcome
            == "mismatched"
        )
        assert (
            check_artifact_identity(
                execution_path=plain_dir,
                expected_run_id="produce_plain_dir",
                artifact_key="bundle_dir",
                tracker=tracker,
            ).outcome
            == "mismatched"
        )
        assert (
            check_artifact_identity(
                execution_path=outset_dir,
                expected_run_id=result.run.id,
                artifact_key="annual",
                tracker=tracker,
            ).outcome
            == "mismatched"
        )
        tracker.close()

    def test_manifest_handles_symlinked_os_ancestor_while_rejecting_target_or_child_symlinks(
        self, tmp_path: Path
    ) -> None:
        from consist.digest import build_shapefile_manifest_v1

        real_mount = tmp_path / "real_scratch"
        real_mount.mkdir()
        linked_mount = tmp_path / "scratch_link"
        try:
            linked_mount.symlink_to(real_mount, target_is_directory=True)
        except OSError as exc:
            pytest.skip(f"symlinks unavailable: {exc}")

        # Directory and shapefile under a symlinked mount parent (like macOS /tmp or HPC /scratch) succeed
        zarr_under_mount = linked_mount / "skims.zarr"
        zarr_under_mount.mkdir()
        (zarr_under_mount / "chunk0").write_bytes(b"payload")
        manifest = build_manifest_v1(zarr_under_mount)
        assert len(manifest["entries"]) == 1

        shp_under_mount = linked_mount / "roads.shp"
        shp_under_mount.write_bytes(b"shp")
        (linked_mount / "roads.shx").write_bytes(b"shx")
        (linked_mount / "roads.dbf").write_bytes(b"dbf")
        shp_manifest = build_shapefile_manifest_v1(shp_under_mount)
        assert len(shp_manifest["entries"]) == 3

        # Also verify directly inside /tmp when writable (macOS /tmp -> private/tmp)
        if Path("/tmp").exists() and os.access("/tmp", os.W_OK):
            with tempfile.TemporaryDirectory(dir="/tmp") as raw_tmp:
                tmp_root = Path(raw_tmp)
                sub_dir = tmp_root / "tmp_store.zarr"
                sub_dir.mkdir()
                (sub_dir / "data").write_bytes(b"ok")
                assert len(build_manifest_v1(sub_dir)["entries"]) == 1

                tmp_shp = tmp_root / "parcels.shp"
                tmp_shp.write_bytes(b"shp")
                (tmp_root / "parcels.shx").write_bytes(b"shx")
                (tmp_root / "parcels.dbf").write_bytes(b"dbf")
                assert len(build_shapefile_manifest_v1(tmp_shp)["entries"]) == 3

        # Symlinked artifact root itself is rejected
        root_link = tmp_path / "linked_target.zarr"
        root_link.symlink_to(real_mount / "skims.zarr", target_is_directory=True)
        with pytest.raises(ValueError, match="symlink"):
            build_manifest_v1(root_link)

        # Child symlink inside tree is rejected
        outside = tmp_path / "outside.txt"
        outside.write_text("secret\n", encoding="utf-8")
        (real_mount / "skims.zarr" / "escape").symlink_to(outside)
        with pytest.raises(ValueError, match="symlink"):
            build_manifest_v1(zarr_under_mount)

    def test_repeated_archive_append_on_immutable_artifact_handle_preserves_roots_and_db_meta(
        self, tmp_path: Path
    ) -> None:
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
            schemas=[TripRowBaseModel],
        )
        pq_path = tracker.run_dir / "outputs" / "trips.parquet"
        pq_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "trip_id": [1],
                "origin_taz": [10],
                "purpose": ["work"],
                "distance_km": [4.0],
            }
        ).to_parquet(pq_path, index=False)

        with tracker.start_run("archive_append_run", model="sim"):
            stale_handle = tracker.log_output(pq_path, key="trips_bm")
            tracker.ingest(stale_handle, schema=TripRowBaseModel)

        arch1 = tmp_path / "archive_1"
        arch2 = tmp_path / "archive_2"
        # Pass the SAME pre-ingest, pre-archive immutable handle to both calls
        tracker.archive_artifact(stale_handle, arch1, mode="copy", append=True)
        tracker.archive_artifact(stale_handle, arch2, mode="copy", append=True)

        reloaded = tracker.db.get_artifact(stale_handle.id)
        assert reloaded is not None
        assert reloaded.meta.get("is_ingested") is True
        roots = reloaded.meta.get("recovery_roots")
        assert isinstance(roots, list)
        assert str(arch1.resolve()) in roots
        assert str(arch2.resolve()) in roots
        tracker.close()

    def test_artifact_hash_eq_invariant_nested_frozen_meta_and_pickle_round_trip(
        self, tmp_path: Path
    ) -> None:
        fixed_id = uuid.uuid4()
        fixed_ts = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)
        digest = Digest("sha256:file", "ab" * 32)

        a_int = Artifact(
            id=fixed_id,
            key="k",
            uri="outputs://k.parquet",
            driver="parquet",
            digest=digest,
            created_at=fixed_ts,
            meta={"x": 1},
        )
        a_float = Artifact(
            id=fixed_id,
            key="k",
            uri="outputs://k.parquet",
            driver="parquet",
            digest=digest,
            created_at=fixed_ts,
            meta={"x": 1.0},
        )
        a_bool = Artifact(
            id=fixed_id,
            key="k",
            uri="outputs://k.parquet",
            driver="parquet",
            digest=digest,
            created_at=fixed_ts,
            meta={"x": True},
        )
        assert a_int == a_float == a_bool
        assert hash(a_int) == hash(a_float) == hash(a_bool)

        a_with_dt = Artifact(
            id=fixed_id,
            key="k",
            uri="outputs://k.parquet",
            driver="parquet",
            digest=digest,
            created_at=fixed_ts,
            meta={"logged_at": fixed_ts, "nested": {"level": {"v": 42}}},
        )
        assert isinstance(hash(a_with_dt), int)
        assert isinstance(a_with_dt.meta["nested"], MappingProxyType)
        assert isinstance(a_with_dt.meta["nested"]["level"], MappingProxyType)
        with pytest.raises(TypeError):
            a_with_dt.meta["nested"]["k"] = 2  # type: ignore[index]

        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
        )
        bound = a_with_dt.with_resolver(tracker)
        assert bound.resolver is tracker
        restored = pickle.loads(pickle.dumps(bound))
        assert restored == bound
        assert restored.resolver is None
        assert isinstance(restored.meta["nested"], MappingProxyType)
        assert restored.meta["nested"]["level"]["v"] == 42
        tracker.close()

    def test_positional_and_named_input_roles_use_disjoint_namespaces(
        self, tmp_path: Path
    ) -> None:
        from consist.core.run_resolution import InputBindingRole

        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
        )
        sample = tmp_path / "in.csv"
        sample.write_text("a\n1\n", encoding="utf-8")
        art = tracker.artifacts.create_artifact(
            sample, None, key="in", direction="input"
        )

        h_pos = tracker.identity.compute_input_hash(
            [art],
            binding_roles=[
                InputBindingRole(kind="positional", role="0", input_index=0)
            ],
            path_resolver=tracker.resolve_uri,
        )
        h_named = tracker.identity.compute_input_hash(
            [art],
            binding_roles=[
                InputBindingRole(kind="named", role="positional:0", input_index=0)
            ],
            path_resolver=tracker.resolve_uri,
        )
        h_dep = tracker.identity.compute_input_hash(
            [art],
            binding_roles=[
                InputBindingRole(kind="dependency", role="0", input_index=0)
            ],
            path_resolver=tracker.resolve_uri,
        )
        h_named_dep = tracker.identity.compute_input_hash(
            [art],
            binding_roles=[
                InputBindingRole(kind="named", role="dependency:0", input_index=0)
            ],
            path_resolver=tracker.resolve_uri,
        )

        assert h_pos != "error"
        assert h_named != "error"
        assert h_pos != h_named
        assert h_dep != h_named_dep
        tracker.close()

    def test_consist_load_falls_back_to_verified_recovery_root_after_archive_move(
        self, tmp_path: Path
    ) -> None:
        db_file = str(tmp_path / "provenance.duckdb")
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=db_file,
        )
        pq_path = tracker.run_dir / "outputs" / "moved.parquet"
        pq_path.parent.mkdir(parents=True, exist_ok=True)
        expected_df = pd.DataFrame({"id": [1, 2], "score": [10.5, 20.5]})
        expected_df.to_parquet(pq_path, index=False)

        with tracker.start_run("move_archive_run", model="sim"):
            art = tracker.log_output(pq_path, key="moved")

        archive_dir = tmp_path / "cold_archive"
        tracker.archive_artifact(art, archive_dir, mode="move")
        assert not pq_path.exists()

        # Works both via original handle and via a fresh Tracker resolving recovery_roots from DB
        rel = consist.load(art, tracker=tracker)
        assert isinstance(rel, duckdb.DuckDBPyRelation)
        pd.testing.assert_frame_equal(rel.df(), expected_df)
        tracker.close()

        fresh_tracker = Tracker(run_dir=tmp_path / "runs", db_path=db_file)
        db_art = fresh_tracker.db.get_artifact(art.id)
        assert db_art is not None
        assert db_art.abs_path is None
        rel_fresh = consist.load(db_art, tracker=fresh_tracker)
        assert isinstance(rel_fresh, duckdb.DuckDBPyRelation)
        pd.testing.assert_frame_equal(rel_fresh.df(), expected_df)

        # Corrupt archived file and confirm digest verification fails closed
        archived_file = archive_dir / "outputs" / "moved.parquet"
        archived_file.write_bytes(b"corrupted parquet bytes")
        with pytest.raises(ValueError, match="failed digest verification"):
            consist.load(db_art, tracker=fresh_tracker)
        fresh_tracker.close()

    def test_input_shapefile_attaches_bundle_manifest_and_archives_cleanly(
        self, tmp_path: Path
    ) -> None:
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
        )
        shp_dir = tracker.run_dir / "inputs"
        shp_dir.mkdir(parents=True, exist_ok=True)
        shp_path = shp_dir / "zones.shp"
        shp_path.write_bytes(b"geometry")
        (shp_dir / "zones.shx").write_bytes(b"index")
        (shp_dir / "zones.dbf").write_bytes(b"attributes")

        with tracker.start_run("shp_input_run", model="gis"):
            in_shp = tracker.log_input(shp_path, key="zones_in")

        assert in_shp.driver == "shapefile"
        assert in_shp.digest is not None
        assert in_shp.digest.scheme == "dir_manifest_v1:sha256"
        assert in_shp.meta.get("file_bundle_artifact") is True
        assert in_shp.meta.get("file_bundle_entry") == "zones.shp"
        assert "file_bundle_manifest" in in_shp.meta

        archive_dir = tmp_path / "shp_archive"
        dest = tracker.archive_artifact(in_shp, archive_dir, mode="copy")
        assert (dest / "zones.shp").is_file()
        assert (dest / "zones.shx").is_file()
        assert (dest / "zones.dbf").is_file()
        tracker.close()

    def test_legacy_coercion_utc_timestamps_coupler_ingest_h5_leaf_driver_and_lazy_models(
        self, tmp_path: Path
    ) -> None:
        from consist.core.coupler import Coupler

        # 1. Legacy v0.4 OutputSet + uppercase sha256:<HEX> coercion
        upper_hex = "AB" * 32
        outset_art = Artifact.from_json_dict(
            {
                "key": "legacy_set",
                "container_uri": "outputs://annual",
                "driver": "artifact_set",
                "hash": upper_hex,
                "meta": {"artifact_set": True},
            }
        )
        assert outset_art.digest == Digest("output_set_v1:sha256", upper_hex.lower())

        prefixed_upper = Artifact.from_json_dict(
            {
                "key": "legacy_file",
                "container_uri": "outputs://data.bin",
                "driver": "binary",
                "hash": f"sha256:{upper_hex}",
            }
        )
        assert prefixed_upper.digest == Digest("sha256:file", upper_hex.lower())

        # 2. Run naive and non-UTC offset timestamps normalize to UTC
        plus_two = timezone(timedelta(hours=2))
        run_obj = Run(
            id="tz_run",
            model_name="m",
            config_hash="00" * 32,
            git_hash="11" * 32,
            started_at=datetime(2026, 3, 1, 14, 0, tzinfo=plus_two),
            ended_at=datetime(2026, 3, 1, 12, 5),
            created_at=datetime(2026, 3, 1, 12, 0),
            updated_at=datetime(2026, 3, 1, 14, 5, tzinfo=plus_two),
        )
        assert run_obj.started_at == datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
        assert run_obj.ended_at == datetime(2026, 3, 1, 12, 5, tzinfo=timezone.utc)
        assert run_obj.created_at.tzinfo == timezone.utc
        assert run_obj.updated_at == datetime(2026, 3, 1, 12, 5, tzinfo=timezone.utc)

        # 3. Logical matrix string policy "preserve_canonical_v1" + .nc container
        xr = pytest.importorskip("xarray")
        nc_path = tmp_path / "skims.nc"
        ds = xr.Dataset(
            {"time": (("orig", "dest"), np.array([[1.0, 2.0], [3.0, 4.0]]))},
            coords={"zone_number": ("orig", np.array([101, 102], dtype=np.int32))},
        )
        ds.to_netcdf(nc_path)
        nc_digest = compute_digest(
            nc_path,
            driver="logical_matrices_canonical",
            options={"policy": "preserve_canonical_v1"},
        )
        assert nc_digest.scheme == "logical_matrices_canonical_v1:sha256"

        # 4. Between-step Coupler sync on ingest() when current_consist is None
        tracker = Tracker(
            run_dir=tmp_path / "runs",
            db_path=str(tmp_path / "provenance.duckdb"),
            schemas=[TripRowBaseModel],
        )
        coupler = Coupler(tracker=tracker)
        pq_path = tracker.run_dir / "outputs" / "trips.parquet"
        pq_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "trip_id": [1],
                "origin_taz": [1],
                "purpose": ["work"],
                "distance_km": [2.5],
            }
        ).to_parquet(pq_path, index=False)
        with tracker.start_run("step_1", model="sim"):
            step_art = tracker.log_output(pq_path, key="trips_bm")
        coupler.set("trips_bm", step_art)
        assert tracker.current_consist is None
        assert coupler["trips_bm"].meta.get("is_ingested") is not True
        tracker.ingest(step_art, schema=TripRowBaseModel)
        assert coupler["trips_bm"].meta.get("is_ingested") is True

        # 5. bind_active_resolver ContextVar token reset
        other_tracker = Tracker(run_dir=tmp_path / "other_runs")
        assert get_active_resolver() is None
        with bind_active_resolver(tracker):
            assert get_active_resolver() is tracker
            with bind_active_resolver(other_tracker):
                assert get_active_resolver() is other_tracker
            assert get_active_resolver() is tracker
        assert get_active_resolver() is None

        # 6. scan_h5_container aligns leaf driver with digest scheme
        h5_path = tracker.run_dir / "outputs" / "mixed.h5"
        pd.DataFrame({"id": [1, 2], "name": ["a", "b"]}).to_hdf(
            h5_path, key="pytables_tbl", mode="w", format="table"
        )
        with h5py.File(h5_path, "a") as h5f:
            h5f.create_dataset("raw_matrix", data=np.arange(6, dtype=np.float64))

        with tracker.start_run("h5_scan_run", model="sim"):
            _, children = tracker.log_h5_container(
                h5_path, key="mixed", hash_tables="always"
            )
        by_member = {c.member: c for c in children}
        raw_leaf = by_member["raw_matrix"]
        assert raw_leaf.driver == "h5"
        assert raw_leaf.digest is not None
        assert raw_leaf.digest.scheme == "h5_dataset_v1:sha256"

        tbl_leaf = by_member["pytables_tbl/table"]
        assert tbl_leaf.driver == "h5_table"
        assert tbl_leaf.digest is not None
        assert tbl_leaf.digest.scheme == "h5_fixed_v1:sha256"

        # 6b. Cached HDF5 container child reuse merges container policy updates cleanly
        policy_h5 = tracker.run_dir / "outputs" / "policy_cached.h5"
        with h5py.File(policy_h5, "w") as h5f:
            h5f.create_dataset("scores", data=np.array([1.0, 2.0, 3.0]))

        with tracker.start_run("h5_policy_r1", model="sim", config={"seed": 7}):
            tracker.log_h5_container(
                policy_h5,
                key="policy_store",
                direction="output",
                custom_note="preserved",
            )

        with tracker.start_run("h5_policy_r2", model="sim", config={"seed": 7}):
            assert tracker.is_cached
            cached_container, cached_children = tracker.log_h5_container(
                policy_h5,
                key="policy_store",
                direction="output",
                container_recovery_unit="parent_file",
                child_recovery_policy="descriptive_only",
                representation_policy={
                    "tabular_parquet": {
                        "role": "derived_inspection_cache",
                        "complete_for_rebuild": False,
                    }
                },
            )
        assert len(cached_children) == 1
        assert cached_container.meta["is_container"] is True
        assert cached_container.meta["container_recovery_unit"] == "parent_file"
        assert cached_container.meta["child_recovery_policy"] == "descriptive_only"
        assert cached_container.meta["representation_policy"] == {
            "tabular_parquet": {
                "role": "derived_inspection_cache",
                "complete_for_rebuild": False,
            }
        }
        assert tracker.db is not None
        persisted_container = tracker.db.get_artifact(cached_container.id)
        assert persisted_container is not None
        assert persisted_container.meta["container_recovery_unit"] == "parent_file"
        assert persisted_container.meta["child_recovery_policy"] == "descriptive_only"
        assert persisted_container.meta["representation_policy"] == {
            "tabular_parquet": {
                "role": "derived_inspection_cache",
                "complete_for_rebuild": False,
            }
        }
        tracker.close()

        # 7. Subprocess import isolation: importing consist.models.artifact does not load SQLAlchemy/SQLModel
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys, consist.models.artifact; "
                    "assert 'sqlalchemy' not in sys.modules, list(sys.modules); "
                    "assert 'sqlmodel' not in sys.modules, list(sys.modules)"
                ),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
