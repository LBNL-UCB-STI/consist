from __future__ import annotations

import json
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
from sqlmodel import Field, SQLModel

from consist.core.directory_artifacts import build_directory_manifest
from consist.core.identity import CodeIdentityDescriptor, IdentityManager
from consist.core.run_resolution import InputBindingRole
from consist.core.tracker import Tracker
from consist.digest import canonical_json_sha256, canonical_json_str, digest_table
from consist.models.artifact import Artifact
from consist.models.run import ConsistRecord


def _sample_golden_step(x: int) -> int:
    return x + 1


def test_golden_v1_identity_manager_signatures() -> None:
    im = IdentityManager(project_root=".", hashing_strategy="full")

    cfg_hash = im.compute_config_hash(
        {
            "alpha": 0.25,
            "modes": ["WALK", "DRIVE"],
            "nested": {"z": 1, "a": True},
            "ignored": "skip",
        },
        exclude_keys=["ignored"],
    )
    assert (
        cfg_hash
        == "e1bf2197990483dc045af9c6f3c142215be5932ca11ee2bfbf3bb38a5d686b6e"  # pragma: allowlist secret
    )

    run_cfg_hash = im.compute_run_config_hash(
        config={"alpha": 0.25},
        model="activitysim",
        year=2025,
        iteration=1,
        cache_epoch=2,
        cache_version=3,
    )
    assert (
        run_cfg_hash
        == "759895b4a0b776c7e66c364f49d9b0047bf0fbdcfecc82819a7ecf643151860d"  # pragma: allowlist secret
    )

    callable_hash = im.compute_callable_hash(_sample_golden_step, strategy="source")
    assert (
        callable_hash
        == "1fc6213a4fc7b4b2f1bc4f46325cc11b6a3626bdb450984160839decc14e466e"  # pragma: allowlist secret
    )

    art_managed = Artifact(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        key="households",
        container_uri="workspace://households.parquet",
        driver="parquet",
        run_id="producer_run_1",
        hash="a" * 64,
        meta={
            "hash_semantics": {
                "version": 1,
                "algorithm": "sha256",
                "kind": "file",
                "digest_contract": "raw_file_bytes",
                "source": "computed_full",
            }
        },
    )
    art_table = Artifact(
        id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        key="persons",
        container_uri="workspace://pipeline.h5",
        table_path="/persons",
        driver="h5_table",
        run_id="producer_run_2",
        hash="b" * 64,
    )

    sig_lookup = {"producer_run_1": "c" * 64, "producer_run_2": None}.get

    # Order-independent legacy input hash
    in_hash_ab = im.compute_input_hash(
        [art_managed, art_table], signature_lookup=sig_lookup
    )
    in_hash_ba = im.compute_input_hash(
        [art_table, art_managed], signature_lookup=sig_lookup
    )
    assert in_hash_ab == in_hash_ba
    assert (
        in_hash_ab
        == "2826e59b8bd86be395357c93922675fa8512ce1a96387a6f4190bf14fda7da1d"  # pragma: allowlist secret
    )

    rb_in_hash = im.compute_resolved_binding_input_hash(
        ordinary_inputs=[art_table],
        strict_binding_identity="d" * 64,
        signature_lookup=sig_lookup,
    )
    assert (
        rb_in_hash
        == "49d652e77c54183dab64dd22b826819a310cb706e087217cb3cff6ed64fb8b39"  # pragma: allowlist secret
    )

    action_id = im.compute_action_input_identity(
        inputs=[art_managed, art_table],
        binding_roles=[
            InputBindingRole(kind="named", role="hh", input_index=0),
            InputBindingRole(kind="named", role="per", input_index=1),
        ],
        code_identity=CodeIdentityDescriptor(
            version=1,
            mode="callable_source",
            digest=callable_hash,
        ),
        signature_lookup=sig_lookup,
    )
    assert (
        action_id.value
        == "sha256:action-v2:ec6b9330c7d4e16cf4e3b4c43d68fec2a2c5e92dd332f6fddf5681177e26a41d"  # pragma: allowlist secret
    )
    assert [b.mode for b in action_id.bindings] == [
        "content-v1",
        "legacy-provenance-v1",
    ]

    run_sig = im.calculate_run_signature(callable_hash, run_cfg_hash, in_hash_ab)
    expected_run_sig = "8791b921f7cfa7ef5baaf477ae11383811c8f447f7c3c7bb568a1bb64204e234"  # pragma: allowlist secret
    assert run_sig == expected_run_sig


def test_golden_v1_consist_json_snapshot_structure(tmp_path: Path) -> None:
    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.duckdb"),
    )
    in_file = tmp_path / "land_use.parquet"
    out_file = tmp_path / "runs" / "final_trips.parquet"
    pd.DataFrame({"zone_id": [1, 2], "households": [100, 200]}).to_parquet(in_file)

    with tracker.start_run(
        "golden_v1_run",
        "activitysim",
        year=2025,
        iteration=1,
        config={"sample_rate": 0.1, "modes": ["WALK", "DRIVE"]},
        facet={"calibration_step": "baseline"},
        tags=["golden", "v1"],
        cache_mode="overwrite",
    ):
        tracker.log_artifact(
            str(in_file),
            key="land_use",
            driver="parquet",
            direction="input",
        )
        out_file.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"trip_id": [10, 20], "mode": ["WALK", "DRIVE"]}).to_parquet(
            out_file
        )
        tracker.log_artifact(
            str(out_file),
            key="final_trips",
            driver="parquet",
            direction="output",
        )

    snapshot_path = tmp_path / "runs" / "consist_runs" / "golden_v1_run.json"
    assert snapshot_path.exists()
    raw = json.loads(snapshot_path.read_text(encoding="utf-8"))

    # Top-level keys required by v1 snapshot readers
    assert {"run", "inputs", "outputs", "config", "facet"}.issubset(raw.keys())
    assert set(raw.keys()) == {
        "run",
        "inputs",
        "outputs",
        "config",
        "facet",
        "cached_run",
    }
    assert raw["run"]["id"] == "golden_v1_run"
    assert raw["run"]["model_name"] == "activitysim"
    assert raw["run"]["status"] == "completed"
    assert raw["run"]["year"] == 2025
    assert raw["run"]["iteration"] == 1
    assert raw["run"]["tags"] == ["golden", "v1"]
    assert isinstance(raw["run"]["config_hash"], str)
    assert isinstance(raw["run"]["input_hash"], str)
    assert isinstance(raw["run"]["signature"], str)

    assert len(raw["inputs"]) == 1
    assert raw["inputs"][0]["key"] == "land_use"
    assert raw["inputs"][0]["driver"] == "parquet"
    assert isinstance(raw["inputs"][0]["id"], str)
    assert isinstance(raw["inputs"][0]["hash"], str)
    assert isinstance(raw["inputs"][0]["container_uri"], str)

    assert len(raw["outputs"]) == 1
    assert raw["outputs"][0]["key"] == "final_trips"
    assert raw["outputs"][0]["driver"] == "parquet"
    assert raw["outputs"][0]["run_id"] == "golden_v1_run"
    assert isinstance(raw["outputs"][0]["hash"], str)

    assert raw["config"]["sample_rate"] == 0.1
    assert raw["config"]["modes"] == ["WALK", "DRIVE"]
    assert raw["facet"] == {"calibration_step": "baseline"}

    record = ConsistRecord.model_validate_json(
        snapshot_path.read_text(encoding="utf-8")
    )
    assert record.run.id == "golden_v1_run"
    assert [a.key for a in record.inputs] == ["land_use"]
    assert [a.key for a in record.outputs] == ["final_trips"]


def test_golden_ctd1_duckdb_vs_parquet_and_arrow_parity(tmp_path: Path) -> None:
    class GoldenTrip(SQLModel, table=True):
        __tablename__ = "golden_trips"
        id: int = Field(primary_key=True)
        mode: str
        weight: float

    df = pd.DataFrame(
        {
            "id": [2, 1],
            "mode": ["WALK", "DRIVE"],
            "weight": [1.5, -0.0],
        }
    )
    parquet_path = tmp_path / "golden_trips.parquet"
    df.to_parquet(parquet_path, index=False)

    expected_ctd1 = (
        "ctd1:sha256:fce9c342e6cee687af7b7fd0c841cd956af246fc5b9cb65bca31c9808e6871ef"
    )
    assert (
        digest_table(df, columns=["id", "mode", "weight"], key=["id"]) == expected_ctd1
    )
    assert (
        digest_table(parquet_path, columns=["id", "mode", "weight"], key=["id"])
        == expected_ctd1
    )
    assert (
        digest_table(
            pa.Table.from_pandas(df, preserve_index=False),
            columns=["id", "mode", "weight"],
            key=["id"],
        )
        == expected_ctd1
    )

    tracker = Tracker(
        run_dir=tmp_path / "runs",
        db_path=str(tmp_path / "provenance.duckdb"),
    )
    with tracker.start_run("hot-ctd1-run", "model", year=2025):
        art = tracker.log_artifact(str(parquet_path), key="golden_trips")
        tracker.ingest(
            artifact=art,
            data=df.to_dict(orient="records"),
            schema=GoldenTrip,
        )

    with duckdb.connect(str(tmp_path / "provenance.duckdb"), read_only=True) as con:
        rel = con.sql(
            "SELECT id, mode, weight FROM global_tables.golden_trips "
            f"WHERE consist_artifact_id = '{art.id}'"
        )
        assert (
            digest_table(rel, columns=["id", "mode", "weight"], key=["id"])
            == expected_ctd1
        )


def test_golden_v1_directory_manifest_canonical_json_and_tree_hash(
    tmp_path: Path,
) -> None:
    root = tmp_path.resolve() / "bundle"
    (root / "sub").mkdir(parents=True)
    (root / "empty_dir").mkdir(parents=True)
    (root / ".hidden").write_bytes(b"dotfile\n")
    (root / "a.txt").write_bytes(b"alpha\n")
    (root / "sub" / "b.txt").write_bytes(b"beta\n")

    manifest = build_directory_manifest(root)
    identity = {"version": manifest["version"], "entries": manifest["entries"]}
    expected_canonical_json = (
        '{"entries":['
        '{"kind":"file","path":".hidden","sha256":"5c9f9cd83cd20bc6b154521d6c687594713c5621fca27706b428aff14f97909d","size":8},'  # pragma: allowlist secret
        '{"kind":"file","path":"a.txt","sha256":"b6a98d9ce9a2d9149288fa3df42d377c3e42737afdcdaf714e33c0a100b51060","size":6},'  # pragma: allowlist secret
        '{"kind":"directory","path":"empty_dir"},'
        '{"kind":"directory","path":"sub"},'
        '{"kind":"file","path":"sub/b.txt","sha256":"f2c82decdd7181cf98945929a62598db7e6b477e11f6e0eb0ae97020eff151ad","size":5}'  # pragma: allowlist secret
        '],"version":1}'
    )
    assert canonical_json_str(identity) == expected_canonical_json
    assert (
        manifest["tree_hash"]
        == "cf052b4219689018f5499b4ff2eb8a34c9f4db946ba74587c0ef633affe33c68"  # pragma: allowlist secret
    )
    assert canonical_json_sha256(identity) == manifest["tree_hash"]
