from pathlib import Path
import uuid

from consist.core.persistence import DatabaseManager
from consist.digest import Digest, Identity
from consist.models.artifact_schema import ArtifactSchema, ArtifactSchemaObservation
from consist.store.schema import init_v2_schema
from tests.helpers.artifact_factory import make_artifact


def test_content_hub_deduplicates_shared_anchor_and_looks_up_by_hash(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "content_identity.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        hex_digest = "a" * 64
        anchor_str = f"sha256:file:{hex_digest}"

        first = make_artifact(
            key="first_parquet",
            container_uri="workspace://first.parquet",
            driver="parquet",
            hash=anchor_str,
            meta={"source": "test"},
        )
        second = make_artifact(
            key="second_parquet",
            container_uri="workspace://second.parquet",
            driver="parquet",
            hash=anchor_str,
        )
        db.upsert_artifact(first)
        db.upsert_artifact(second)

        with db.ledger.exec_cursor() as cur:
            content_count = cur.execute(
                "SELECT COUNT(*) FROM main.content WHERE anchor = ?",
                [anchor_str],
            ).fetchone()[0]
            digest_count = cur.execute(
                "SELECT COUNT(*) FROM main.content_digest WHERE anchor = ?",
                [anchor_str],
            ).fetchone()[0]

        assert content_count == 1
        assert digest_count == 1

        by_full = db.find_latest_artifact_by_hash(anchor_str, driver="parquet")
        assert by_full is not None
        assert by_full.id in {first.id, second.id}

        by_hex = db.find_latest_artifact_by_hash(hex_digest)
        assert by_hex is not None
        assert by_hex.anchor == Digest.parse(anchor_str)

        assert db.find_latest_artifact_by_hash(anchor_str, driver="csv") is None

        historical_anchor = Digest("legacy-v1:unverified", "b" * 64)
        legacy_art = make_artifact(
            key="legacy_parquet",
            container_uri="workspace://legacy.parquet",
            driver="parquet",
            identity=Identity(anchor=historical_anchor),
        )
        db.upsert_artifact(legacy_art)
        db.ledger.record_anchor_promotion(
            historical_anchor,
            Digest.parse(anchor_str),
            basis="caller_supplied_match",
        )

        matched = db.find_artifacts_by_anchor(anchor_str)
        assert {art.id for art in matched} == {first.id, second.id, legacy_art.id}
    finally:
        db.close()


def test_v2_schema_init_recreates_artifact_indexes_idempotently(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "artifact_indexes.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        with db.ledger.exec_cursor() as cur:
            initial_indexes = {
                str(row[0])
                for row in cur.execute(
                    "SELECT index_name FROM duckdb_indexes() "
                    "WHERE table_name = 'artifact'"
                ).fetchall()
            }
        assert {
            "idx_artifact_anchor",
            "idx_artifact_run_id",
            "idx_artifact_parent",
        }.issubset(initial_indexes)

        with db.ledger.exec_cursor() as cur:
            cur.execute("DROP INDEX IF EXISTS idx_artifact_anchor")
            cur.execute("DROP INDEX IF EXISTS idx_artifact_parent")

        init_v2_schema(db.ledger.connection)
        init_v2_schema(db.ledger.connection)

        with db.ledger.exec_cursor() as cur:
            recreated_indexes = {
                str(row[0])
                for row in cur.execute(
                    "SELECT index_name FROM duckdb_indexes() "
                    "WHERE table_name = 'artifact'"
                ).fetchall()
            }
            schema_versions = cur.execute(
                "SELECT version FROM main.consist_schema"
            ).fetchall()

        assert {
            "idx_artifact_anchor",
            "idx_artifact_run_id",
            "idx_artifact_parent",
        }.issubset(recreated_indexes)
        assert schema_versions == [(2,)]
    finally:
        db.close()


def test_historical_vs_verified_identity_persistence(tmp_path: Path) -> None:
    db_path = tmp_path / "identity_kinds.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        historical_digest = Digest("legacy-v1:unverified", "1" * 64)
        verified_digest = Digest("sha256:file", "2" * 64)

        legacy = make_artifact(
            key="legacy_network",
            container_uri="outputs://legacy_network.csv",
            driver="csv",
            identity=Identity(anchor=historical_digest),
            run_id="run_a",
        )
        trusted = make_artifact(
            key="trusted_network",
            container_uri="outputs://trusted_network.csv",
            driver="csv",
            hash=verified_digest,
            run_id="run_b",
        )
        db.upsert_artifact(legacy)
        db.upsert_artifact(trusted)
        legacy_id = legacy.id
        trusted_id = trusted.id
    finally:
        db.close()

    reopened = DatabaseManager(str(db_path))
    try:
        legacy_loaded = reopened.get_artifact(legacy_id)
        trusted_loaded = reopened.get_artifact(trusted_id)

        assert legacy_loaded is not None
        assert legacy_loaded.identity.verifiable is False
        assert legacy_loaded.anchor == historical_digest

        assert trusted_loaded is not None
        assert trusted_loaded.identity.verifiable is True
        assert trusted_loaded.anchor == verified_digest

        with reopened.ledger.exec_cursor() as cur:
            legacy_content = cur.execute(
                "SELECT repr_kind FROM main.content WHERE anchor = ?",
                [str(historical_digest)],
            ).fetchone()
            legacy_claims = cur.execute(
                "SELECT COUNT(*) FROM main.content_digest WHERE anchor = ?",
                [str(historical_digest)],
            ).fetchone()[0]
            trusted_content = cur.execute(
                "SELECT repr_kind FROM main.content WHERE anchor = ?",
                [str(verified_digest)],
            ).fetchone()
            trusted_claims = cur.execute(
                "SELECT scheme, value, basis FROM main.content_digest WHERE anchor = ?",
                [str(verified_digest)],
            ).fetchall()

        assert legacy_content == ("historical",)
        assert legacy_claims == 0
        assert trusted_content == ("file",)
        assert trusted_claims == [("sha256:file", "2" * 64, "computed")]
    finally:
        reopened.close()


def test_find_schema_observation_for_anchor(tmp_path: Path) -> None:
    db_path = tmp_path / "anchor_obs.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        shared_hex = "c" * 64
        a = make_artifact(
            key="a",
            container_uri="inputs://a.csv",
            driver="csv",
            hash=shared_hex,
        )
        b = make_artifact(
            key="b",
            container_uri="inputs://b.csv",
            driver="csv",
            hash=shared_hex,
        )
        db.upsert_artifact(a)
        db.upsert_artifact(b)

        schema = ArtifactSchema(
            id="sid",
            summary_json={"table_name": "a"},
            profile_version=1,
        )
        obs_in = ArtifactSchemaObservation(
            artifact_id=a.id,
            schema_id=schema.id,
            source="file",
        )
        db.persist_artifact_schema(schema, observation=obs_in)

        found = db.find_schema_observation_for_anchor(b.anchor)
        assert found is not None
        obs_out, summary = found
        assert obs_out.schema_id == "sid"
        assert summary == {"table_name": "a"}
    finally:
        db.close()


def test_parent_artifact_queries_support_canonical_and_legacy_rows(
    tmp_path: Path,
) -> None:
    db = DatabaseManager(str(tmp_path / "artifact_parent_lookup.duckdb"))
    try:
        parent_id = uuid.uuid4()
        parent = make_artifact(
            id=parent_id,
            key="container",
            container_uri="outputs://container.h5",
            driver="h5",
            run_id="run_a",
        )
        child = make_artifact(
            key="child",
            container_uri="outputs://container.h5",
            driver="h5_table",
            table_path="/a",
            parent_artifact_id=parent_id,
            run_id="run_a",
            meta={"parent_id": str(parent_id)},
        )
        legacy_child = make_artifact(
            key="legacy_child",
            container_uri="outputs://legacy.h5",
            driver="h5_table",
            table_path="/legacy",
            run_id="run_a",
            meta={"parent_id": str(parent_id)},
        )
        db.upsert_artifact(parent)
        db.upsert_artifact(child)
        db.upsert_artifact(legacy_child)

        children = db.get_child_artifacts(parent_id)
        assert [artifact.key for artifact in children] == ["child", "legacy_child"]

        resolved_parent = db.get_parent_artifact(child.id)
        assert resolved_parent is not None
        assert resolved_parent.id == parent_id

        resolved_legacy_parent = db.get_parent_artifact(legacy_child.id)
        assert resolved_legacy_parent is not None
        assert resolved_legacy_parent.id == parent_id
    finally:
        db.close()
