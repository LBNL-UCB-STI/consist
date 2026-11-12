"""Regression tests for non-ORM value models, unified scheme digests, and layering."""

from __future__ import annotations

import ast
import copy
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import pickle
import time
from types import MappingProxyType
import uuid

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
from consist.protocols import ArtifactLike, ArtifactRecordLike, ArtifactResolver
from consist.types import OutputSet


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

        # Construction speed microbenchmark (< 5 us per instance; target ~1 us)
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
        assert best_per_item_us < 5.0, f"Artifact init took {best_per_item_us:.2f} us"

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
                # Pure serialization latency (< 1 ms)
                iterations = 200
                t0 = time.perf_counter()
                for _ in range(iterations):
                    payload_str = record.model_dump_json()
                avg_dump_ms = ((time.perf_counter() - t0) * 1000.0) / iterations
                assert avg_dump_ms < 1.0, (
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
                assert best_flush_ms < 1.0, (
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
