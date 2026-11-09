from __future__ import annotations

import copy
import os
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import consist
from consist.core.directory_artifacts import build_directory_manifest
from consist.digest import (
    Agreement,
    Contract,
    Digest,
    Expectation,
    Identity,
    IdentityConflict,
    Kind,
    SchemeRegistry,
    SchemeSpec,
    Verdict,
    build_manifest_v1,
    compare,
    compute_ctd1_digest,
    compute_logical_matrix_digest,
    compute_manifest_v1_digest,
    refines,
)
from consist.location import (
    EMPTY_META,
    DbTableChangeToken,
    DirTreeStat,
    FileStat,
    Freshness,
    FrozenList,
    FrozenMeta,
    LedgerRef,
    Location,
    MemberOf,
    Mount,
    ObjectETag,
    Observation,
    SiteContext,
    witness_from_json,
    witness_to_json,
)
from consist.meta import (
    EMPTY_META as META_EMPTY_META,
    FrozenList as MetaFrozenList,
    FrozenMeta as MetaFrozenMeta,
)


def test_scheme_registry_builtins_and_custom_registration() -> None:
    expected_builtins = {
        "sha256:file": Kind.BYTES,
        "blake3:file": Kind.BYTES,
        "sha256:manifest-v1": Kind.CONTAINER,
        "ctd1:sha256": Kind.LOGICAL,
        "sha256:logical_matrix:v1": Kind.LOGICAL,
        "sha256:logical_matrices:v1": Kind.LOGICAL,
        "blosc2:header:v1": Kind.CONTAINER,
        "legacy-v1:unverified": Kind.HISTORICAL,
    }
    for name, expected_kind in expected_builtins.items():
        spec = SchemeRegistry.get(name)
        assert spec.name == name
        assert spec.kind == expected_kind
        assert SchemeRegistry.is_historical(name) is (expected_kind is Kind.HISTORICAL)

    # Aliases resolve to canonical SchemeSpec
    assert SchemeRegistry.get("sha256:logical_matrices").name == (
        "sha256:logical_matrices:v1"
    )
    assert SchemeRegistry.get("sha256:logical_matrix").name == (
        "sha256:logical_matrix:v1"
    )
    assert "member" in SchemeRegistry.get("sha256:logical_matrices:v1").applies_to
    assert "member" in SchemeRegistry.get("sha256:logical_matrix:v1").applies_to

    # Dynamic filtered policy scheme
    policy_scheme = f"sha256:manifest-v1+p:{'a' * 64}"
    policy_spec = SchemeRegistry.get(policy_scheme)
    assert policy_spec.kind is Kind.FILTERED

    # Custom registration (e.g. SEMANTIC scheme)
    semantic_spec = SchemeSpec(
        name="schema:arrow:v1",
        kind=Kind.SEMANTIC,
        applies_to=frozenset({"parquet", "csv", "table", "file"}),
        cost_rank=25,
    )
    SchemeRegistry.register(semantic_spec)
    assert SchemeRegistry.get("schema:arrow:v1") == semantic_spec
    assert not SchemeRegistry.is_historical(semantic_spec)

    with pytest.raises(ValueError, match="Unregistered digest scheme"):
        SchemeRegistry.get("unknown:scheme")


def test_digest_validation_and_parse_str_roundtrip() -> None:
    d = Digest("sha256:file", "0123456789abcdef" * 4)
    assert d.scheme == "sha256:file"
    assert d.hex == "0123456789abcdef" * 4
    assert d.value == d.hex
    assert d.kind is Kind.BYTES
    assert str(d) == f"sha256:file:{'0123456789abcdef' * 4}"
    assert Digest.parse(str(d)) == d

    # Scheme with multiple colons parses cleanly via rpartition(":")
    d_mat = Digest.parse(f"sha256:logical_matrices:v1:{'b' * 64}")
    assert d_mat.scheme == "sha256:logical_matrices:v1"
    assert d_mat.hex == "b" * 64
    assert d_mat.kind is Kind.LOGICAL
    assert str(d_mat) == f"sha256:logical_matrices:v1:{'b' * 64}"

    # Alias normalizes to canonical scheme name
    d_alias = Digest("sha256:logical_matrices", "c" * 64)
    assert d_alias.scheme == "sha256:logical_matrices:v1"

    # Historical digest
    d_hist = Digest("legacy-v1:unverified", "d" * 64)
    assert d_hist.kind is Kind.HISTORICAL
    assert SchemeRegistry.is_historical(d_hist)

    # Validation errors: unregistered scheme, empty hex, wrong hex length, uppercase hex, non-hex, malformed parse
    with pytest.raises(ValueError):
        Digest("not-a-registered-scheme", "a" * 64)
    with pytest.raises(ValueError):
        Digest("sha256:file", "")
    with pytest.raises(ValueError):
        Digest("sha256:file", "deadbeef")
    with pytest.raises(ValueError):
        Digest("sha256:file", "ABCDEF" + "0" * 58)
    with pytest.raises(ValueError):
        Digest("sha256:file", "zzzzzz" + "0" * 58)
    with pytest.raises(ValueError):
        Digest.parse("a" * 64)
    with pytest.raises(ValueError):
        Digest.parse(f"sha256:{'a' * 64}")


def test_identity_invariants_and_refines_monotonicity() -> None:
    anchor = Digest("sha256:file", "a" * 64)
    claim_ctd = Digest("ctd1:sha256", "b" * 64)
    claim_blake = Digest("blake3:file", "c" * 64)
    hist = Digest("legacy-v1:unverified", "9" * 64)

    # Deduplication preserving order
    ident = Identity(anchor=anchor, claims=(claim_ctd, claim_ctd, claim_blake, anchor))
    assert ident.anchor == anchor
    assert ident.claims == (claim_ctd, claim_blake)
    assert ident.all_digests == (anchor, claim_ctd, claim_blake)
    assert ident.by_scheme() == {
        "sha256:file": anchor,
        "ctd1:sha256": claim_ctd,
        "blake3:file": claim_blake,
    }
    assert ident.get("ctd1:sha256") == claim_ctd

    # Equality and hash depend ONLY on anchor
    ident_bare = Identity(anchor=anchor)
    assert ident == ident_bare
    assert hash(ident) == hash(ident_bare)

    # Monotonic refinement via with_claim and Identity.refines
    step1 = ident_bare.with_claim(claim_ctd)
    step2 = step1.with_claim(claim_blake)
    assert step1.with_claim(claim_ctd) is step1
    assert Identity.refines(ident_bare, ident_bare)
    assert Identity.refines(ident_bare, step1)
    assert Identity.refines(step1, step2)
    assert Identity.refines(ident_bare, step2)
    assert not Identity.refines(step1, ident_bare)
    assert not Identity.refines(step2, step1)
    assert refines(ident_bare, step2)

    other_anchor_ident = Identity(anchor=Digest("sha256:file", "f" * 64))
    assert not Identity.refines(ident_bare, other_anchor_ident)

    # Historical anchor invariants
    hist_ident = Identity(anchor=hist)
    assert hist_ident.claims == ()
    assert not hist_ident.verifiable
    with pytest.raises(ValueError, match="HISTORICAL"):
        Identity(anchor=hist, claims=(claim_ctd,))
    with pytest.raises(ValueError, match="HISTORICAL"):
        Identity(anchor=anchor, claims=(hist,))
    with pytest.raises(ValueError, match="HISTORICAL"):
        ident_bare.with_claim(hist)
    with pytest.raises(ValueError, match="HISTORICAL"):
        hist_ident.with_claim(claim_ctd)

    # Conflicting claims rejected
    conflicting_ctd = Digest("ctd1:sha256", "e" * 64)
    conflicting_anchor = Digest("sha256:file", "e" * 64)
    with pytest.raises(IdentityConflict):
        Identity(anchor=anchor, claims=(claim_ctd, conflicting_ctd))
    with pytest.raises(IdentityConflict):
        Identity(anchor=anchor, claims=(conflicting_anchor,))
    with pytest.raises(IdentityConflict):
        step1.with_claim(conflicting_ctd)


def test_compare_contracts_drivers_needed_and_historical() -> None:
    SchemeRegistry.register(
        SchemeSpec(
            name="schema:arrow:v1",
            kind=Kind.SEMANTIC,
            applies_to=frozenset({"parquet", "csv", "table", "file"}),
            cost_rank=25,
        )
    )

    d_sha_1 = Digest("sha256:file", "1" * 64)
    d_sha_2 = Digest("sha256:file", "2" * 64)
    d_ctd_same = Digest("ctd1:sha256", "3" * 64)
    d_ctd_diff = Digest("ctd1:sha256", "4" * 64)
    d_schema_1 = Digest("schema:arrow:v1", "5" * 64)
    d_hist = Digest("legacy-v1:unverified", "9" * 64)

    # 1. Kind.HISTORICAL always returns UNKNOWN with agree_reachable=False
    id_hist = Identity(d_hist)
    id_live = Identity(d_sha_1, claims=(d_ctd_same,))
    v_hist = compare(id_hist, id_live, Contract.ANY_SHARED)
    assert v_hist == Verdict(Agreement.UNKNOWN, None, (), False)
    assert compare(id_live, id_hist, Contract.EXACT_BYTES) == Verdict(
        Agreement.UNKNOWN, None, (), False
    )

    # 2. EXACT_BYTES
    assert compare(
        Identity(d_sha_1), Identity(d_sha_1), Contract.EXACT_BYTES
    ) == Verdict(Agreement.MATCH, "sha256:file", (), True)
    assert compare(
        Identity(d_sha_1), Identity(d_sha_2), Contract.EXACT_BYTES
    ) == Verdict(Agreement.MISMATCH, "sha256:file", (), True)

    # 3. LOGICAL_DATA across different container bytes (e.g. Parquet vs CSV)
    id_parquet = Identity(d_sha_1, claims=(d_ctd_same,))
    id_csv = Identity(d_sha_2, claims=(d_ctd_same,))
    v_logical = compare(
        id_parquet,
        id_csv,
        Contract.LOGICAL_DATA,
        driver_a="parquet",
        driver_b="csv",
    )
    assert v_logical == Verdict(Agreement.MATCH, "ctd1:sha256", (), True)

    id_csv_diff = Identity(d_sha_2, claims=(d_ctd_diff,))
    assert compare(
        id_parquet,
        id_csv_diff,
        Contract.LOGICAL_DATA,
        driver_a="parquet",
        driver_b="csv",
    ) == Verdict(Agreement.MISMATCH, "ctd1:sha256", (), True)

    # 4. Needed scheme discovery when logical digest not yet computed on one or both sides
    id_csv_uncomputed = Identity(d_sha_2)
    v_needed = compare(
        id_parquet,
        id_csv_uncomputed,
        Contract.LOGICAL_DATA,
        driver_a="parquet",
        driver_b="csv",
    )
    assert v_needed.agreement is Agreement.UNKNOWN
    assert v_needed.matched_scheme is None
    assert v_needed.needed == ("ctd1:sha256",)
    assert v_needed.agree_reachable is True

    # 5. Incompatible drivers under LOGICAL_DATA -> agree_reachable=False
    v_incompat = compare(
        Identity(d_sha_1),
        Identity(d_sha_2),
        Contract.LOGICAL_DATA,
        driver_a="parquet",
        driver_b="zarr",
    )
    assert v_incompat == Verdict(Agreement.UNKNOWN, None, (), False)

    # 6. SCHEMA_ONLY
    id_s1 = Identity(d_sha_1, claims=(d_schema_1,))
    id_s2 = Identity(d_sha_2, claims=(d_schema_1,))
    assert compare(id_s1, id_s2, Contract.SCHEMA_ONLY) == Verdict(
        Agreement.MATCH, "schema:arrow:v1", (), True
    )

    # 7. ANY_SHARED picks lowest cost_rank shared matching scheme (blake3:file cost 5 < sha256:file cost 10),
    # and matches on a higher-cost logical scheme when container bytes differ across formats
    d_blake = Digest("blake3:file", "7" * 64)
    id_both_1 = Identity(d_sha_1, claims=(d_blake, d_ctd_same))
    id_both_2 = Identity(d_sha_1, claims=(d_blake, d_ctd_same))
    assert compare(id_both_1, id_both_2, Contract.ANY_SHARED) == Verdict(
        Agreement.MATCH, "blake3:file", (), True
    )
    assert compare(id_parquet, id_csv, Contract.ANY_SHARED) == Verdict(
        Agreement.MATCH, "ctd1:sha256", (), True
    )
    assert compare(id_parquet, id_csv_diff, Contract.ANY_SHARED) == Verdict(
        Agreement.MISMATCH, "sha256:file", (), True
    )

    # 8. Contradictory claims across identical container bytes raise IdentityConflict
    id_corrupt_1 = Identity(d_sha_1, claims=(d_ctd_same,))
    id_corrupt_2 = Identity(d_sha_1, claims=(d_ctd_diff,))
    with pytest.raises(IdentityConflict):
        compare(id_corrupt_1, id_corrupt_2, Contract.LOGICAL_DATA)

    # 9. Custom Expectation with pinned_schemes
    pinned = Expectation(
        accepts_kinds=frozenset({Kind.LOGICAL}),
        pinned_schemes=frozenset({"ctd1:sha256"}),
    )
    assert compare(id_parquet, id_csv, pinned) == Verdict(
        Agreement.MATCH, "ctd1:sha256", (), True
    )
    assert compare(Identity(d_sha_1), Identity(d_sha_2), pinned) == Verdict(
        Agreement.UNKNOWN, None, ("ctd1:sha256",), True
    )


def test_frozen_meta_and_frozen_list_immutability_singleton_and_pickle() -> None:
    # Re-export parity between consist.meta and consist.location
    assert EMPTY_META is META_EMPTY_META
    assert FrozenMeta is MetaFrozenMeta
    assert FrozenList is MetaFrozenList
    assert consist.meta.EMPTY_META is EMPTY_META
    assert consist.location.EMPTY_META is EMPTY_META

    # Singleton behavior for empty FrozenMeta
    assert FrozenMeta() is EMPTY_META
    assert FrozenMeta({}) is EMPTY_META
    assert len(EMPTY_META) == 0

    # Deep freezing in __new__
    raw = {
        "tags": ["baseline", "calibrated", {"nested_key": [1, 2, 3]}],
        "params": {"alpha": 0.5, "flag": True, "count": np.int64(7)},
        "empty": {},
    }
    meta = FrozenMeta(raw)
    assert isinstance(meta, FrozenMeta)
    assert isinstance(meta["tags"], FrozenList)
    assert isinstance(meta["tags"][2], FrozenMeta)
    assert isinstance(meta["tags"][2]["nested_key"], FrozenList)
    assert isinstance(meta["params"], FrozenMeta)
    assert meta["params"]["count"] == 7
    assert type(meta["params"]["count"]) is int
    assert meta["empty"] is EMPTY_META

    # Calling __init__ again on EMPTY_META or existing FrozenMeta cannot mutate it
    EMPTY_META.__init__({" leaked ": 1})  # type: ignore[misc]
    assert len(EMPTY_META) == 0

    # Immutability: any mutation attempt raises TypeError
    with pytest.raises(TypeError):
        meta["new_key"] = "val"
    with pytest.raises(TypeError):
        del meta["tags"]
    with pytest.raises(TypeError):
        meta.update({"a": 1})
    with pytest.raises(TypeError):
        meta.pop("tags")
    with pytest.raises(TypeError):
        meta.clear()
    with pytest.raises(TypeError):
        meta.setdefault("x", 1)

    # Equality with plain dict / list / tuple and hashing
    assert meta == {
        "tags": ["baseline", "calibrated", {"nested_key": [1, 2, 3]}],
        "params": {"alpha": 0.5, "flag": True, "count": 7},
        "empty": {},
    }
    assert meta["tags"] == ["baseline", "calibrated", {"nested_key": [1, 2, 3]}]
    assert meta["tags"] == ("baseline", "calibrated", {"nested_key": (1, 2, 3)})
    meta_reordered = FrozenMeta(
        {
            "empty": {},
            "params": {"count": 7, "flag": True, "alpha": 0.5},
            "tags": ["baseline", "calibrated", {"nested_key": [1, 2, 3]}],
        }
    )
    assert meta == meta_reordered
    assert hash(meta) == hash(meta_reordered)
    assert {meta: "ok"}[meta_reordered] == "ok"

    # Python hash-equality invariant across numerically equal int/float/bool values
    m_int = FrozenMeta({"v": 1, "z": 0})
    m_float = FrozenMeta({"v": 1.0, "z": 0.0})
    assert m_int == m_float
    assert hash(m_int) == hash(m_float)
    assert {m_int: "found"}[m_float] == "found"

    # Recursive conversion to plain dict / list
    plain = meta.to_dict()
    assert type(plain) is dict
    assert type(plain["tags"]) is list
    assert type(plain["tags"][2]) is dict
    assert type(plain["tags"][2]["nested_key"]) is list
    assert type(plain["params"]) is dict

    # Pickle and copy round-trip
    unpickled = pickle.loads(pickle.dumps(meta))
    assert unpickled == meta
    assert isinstance(unpickled, FrozenMeta)
    assert isinstance(unpickled["tags"], FrozenList)
    assert pickle.loads(pickle.dumps(EMPTY_META)) is EMPTY_META
    assert copy.copy(meta) is meta
    assert copy.deepcopy(meta) is meta
    assert "FrozenMeta(" in repr(meta)
    assert "FrozenList(" in repr(meta["tags"])

    # Invalid keys / values rejected with TypeError
    with pytest.raises(TypeError):
        FrozenMeta({123: "bad_key"})  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        FrozenMeta({"nan_val": float("nan")})
    with pytest.raises(TypeError):
        FrozenMeta({"set_val": {1, 2, 3}})


def test_file_stat_freshness_racy_window_and_witness_json_roundtrip(
    tmp_path: Path,
) -> None:
    f = tmp_path / "sample.bin"
    f.write_bytes(b"hello world")
    raw_st = f.stat()

    # Non-racy capture (observed >= 2s after mtime_ns)
    non_racy_obs_ns = raw_st.st_mtime_ns + 3_000_000_000
    fstat = FileStat.capture(f, observed_at_ns=non_racy_obs_ns)
    assert fstat.racy is False
    assert fstat.size == 11
    assert fstat.check(f, now_ns=non_racy_obs_ns + 10_000_000_000) is Freshness.FRESH

    # Size, mtime, ctime, ino mismatches -> STALE
    assert (
        FileStat(
            size=fstat.size + 1,
            mtime_ns=fstat.mtime_ns,
            ctime_ns=fstat.ctime_ns,
            ino=fstat.ino,
        ).check(f)
        is Freshness.STALE
    )
    assert (
        FileStat(
            size=fstat.size,
            mtime_ns=fstat.mtime_ns + 1,
            ctime_ns=fstat.ctime_ns,
            ino=fstat.ino,
        ).check(f)
        is Freshness.STALE
    )
    assert (
        FileStat(
            size=fstat.size,
            mtime_ns=fstat.mtime_ns,
            ctime_ns=(fstat.ctime_ns or 0) + 1,
            ino=fstat.ino,
        ).check(f)
        is Freshness.STALE
    )
    assert (
        FileStat(
            size=fstat.size,
            mtime_ns=fstat.mtime_ns,
            ctime_ns=fstat.ctime_ns,
            ino=(fstat.ino or 0) + 1,
        ).check(f)
        is Freshness.STALE
    )

    # Racy capture (< 2s after mtime_ns) transitions from FRESH inside window to STALE once >= 2s
    racy_obs_ns = raw_st.st_mtime_ns + 500_000_000  # 0.5s after mtime
    racy_stat = FileStat.capture(f, observed_at_ns=racy_obs_ns)
    assert racy_stat.racy is True
    assert (
        racy_stat.check(f, now_ns=raw_st.st_mtime_ns + 1_000_000_000) is Freshness.FRESH
    )
    assert (
        racy_stat.check(f, now_ns=raw_st.st_mtime_ns + 2_000_000_000) is Freshness.STALE
    )

    # Missing file -> MISSING
    missing_path = tmp_path / "does_not_exist.bin"
    assert fstat.check(missing_path) is Freshness.MISSING

    # DirTreeStat capture & check
    tree_dir = tmp_path / "tree_witness"
    (tree_dir / "nested").mkdir(parents=True)
    (tree_dir / "a.txt").write_bytes(b"aaa")
    (tree_dir / "nested" / "b.txt").write_bytes(b"bbbbb")
    far_future_ns = f.stat().st_mtime_ns + 10_000_000_000
    dir_stat = DirTreeStat.capture(tree_dir, observed_at_ns=far_future_ns)
    assert dir_stat.file_count == 2
    assert dir_stat.total_bytes == 8
    assert dir_stat.racy is False
    assert dir_stat.check(tree_dir, now_ns=far_future_ns + 1_000_000) is Freshness.FRESH
    (tree_dir / "extra.txt").write_bytes(b"x")
    assert dir_stat.check(tree_dir, now_ns=far_future_ns + 1_000_000) is Freshness.STALE
    assert dir_stat.check(tmp_path / "no_tree") is Freshness.MISSING

    # MemberOf witness
    member_w = MemberOf.capture(f, member="/households", observed_at_ns=non_racy_obs_ns)
    assert member_w.check(f, now_ns=non_racy_obs_ns + 5_000_000_000) is Freshness.FRESH
    assert member_w.check(missing_path) is Freshness.MISSING

    # Witness JSON round-trip across all 5 witness types + None
    witnesses = [
        None,
        fstat,
        dir_stat,
        member_w,
        ObjectETag(etag='"etag-123456"', size=4096, generation="1"),
        DbTableChangeToken(
            token="load_2026_11_09", row_count=250, load_id="1731180000.1"
        ),
    ]
    for w in witnesses:
        kind, payload = witness_to_json(w)
        restored = witness_from_json(kind, payload)
        assert restored == w


def test_mount_and_site_context_detection_and_lawrencium_scopes(
    tmp_path: Path,
) -> None:
    scratch_root = tmp_path / "global_scratch"
    nvme_root = tmp_path / "local_nvme"
    scratch_root.mkdir()
    nvme_root.mkdir()

    m_site = Mount(name="scratch", root=scratch_root, scope="site")
    m_node = Mount(name="node_cache", root=nvme_root, scope="node")
    m_job = Mount(name="job_flash", root=nvme_root, scope="job")

    # Priority 1: explicit_site overrides everything
    ctx_explicit = SiteContext.detect(
        explicit_site="custom-site",
        mounts={"scratch": m_site},
        env={
            "CONSIST_SITE": "env-site",
            "LMOD_SYSTEM_NAME": "lawrencium",
            "SLURMD_NODENAME": "n0042.lr7",
            "SLURM_JOB_ID": "987654",
        },
    )
    assert ctx_explicit.site == "custom-site"
    assert ctx_explicit.node_name == "n0042.lr7"
    assert ctx_explicit.job_id == "987654"

    # Priority 2: $CONSIST_SITE overrides $LMOD_SYSTEM_NAME and mounts
    ctx_env = SiteContext.detect(
        mounts={"scratch": m_site},
        env={
            "CONSIST_SITE": "perlmutter-override",
            "LMOD_SYSTEM_NAME": "lawrencium",
        },
        node_name="nid001024",
    )
    assert ctx_env.site == "perlmutter-override"

    # Priority 3: $LMOD_SYSTEM_NAME / $NERSC_HOST / $SLURM_CLUSTER_NAME
    ctx_lrc = SiteContext.detect(
        mounts={"scratch": m_site},
        env={
            "LMOD_SYSTEM_NAME": "lawrencium",
            "SLURMD_NODENAME": "n0042.lr7",
            "SLURM_JOB_ID": "12345",
        },
    )
    assert ctx_lrc.site == "lawrencium"
    assert ctx_lrc.scope_key_for(m_site) is None
    assert ctx_lrc.scope_key_for(m_node) == "node:n0042.lr7"
    assert ctx_lrc.scope_key_for(m_job) == "node:n0042.lr7:job:12345"

    # Multi-node Slurm job scoping: two nodes in the same Slurm job get distinct job scope keys
    ctx_lrc_node2 = SiteContext.detect(
        mounts={"scratch": m_site},
        env={
            "LMOD_SYSTEM_NAME": "lawrencium",
            "SLURMD_NODENAME": "n0043.lr7",
            "SLURM_JOB_ID": "12345",
        },
    )
    assert ctx_lrc_node2.site == "lawrencium"
    assert ctx_lrc_node2.scope_key_for(m_site) is None
    assert ctx_lrc_node2.scope_key_for(m_job) == "node:n0043.lr7:job:12345"
    assert ctx_lrc_node2.scope_key_for(m_job) != ctx_lrc.scope_key_for(m_job)

    # Job scope fallback when outside Slurm uses pid-<os.getpid()>
    ctx_no_job = SiteContext(site="lawrencium", node_name="lrc-login-01", job_id=None)
    assert ctx_no_job.scope_key_for(m_job) == f"node:lrc-login-01:job:pid-{os.getpid()}"

    # Priority 4: mount-path hash when no explicit/env/cluster site is set
    ctx_mounts = SiteContext.detect(
        mounts={"scratch": m_site, "job_flash": m_job},
        env={},
        node_name="workstation-01",
    )
    assert ctx_mounts.site.startswith("mounts:")
    assert len(ctx_mounts.site) == len("mounts:") + 12

    # Verify .consist-site-id is NEVER created on disk
    assert not (scratch_root / ".consist-site-id").exists()
    assert not (nvme_root / ".consist-site-id").exists()
    assert not (tmp_path / ".consist-site-id").exists()

    # Location, Observation, and LedgerRef construction
    loc = Location(uri="workspace://runs/pipeline.h5", member="/households")
    ident = Identity(Digest("ctd1:sha256", "a" * 64))
    obs = Observation(
        location=loc,
        identity=ident,
        access_driver="h5_table",
        witness=None,
        basis="computed",
        site=ctx_lrc.site,
        scope_key=ctx_lrc.scope_key_for(m_site),
        mount_instance_id=None,
    )
    assert obs.location == loc
    assert obs.identity == ident
    ref = LedgerRef(db_uid="db-1", instance_key="inst-1", db_uri="file:///tmp/p.duckdb")
    assert ref.db_uid == "db-1"


def test_manifest_v1_byte_for_byte_parity_with_build_directory_manifest(
    tmp_path: Path,
) -> None:
    root = tmp_path / "zarr_like_tree"
    (root / "a" / "b").mkdir(parents=True)
    (root / "empty_subdir").mkdir(parents=True)
    (root / ".zgroup").write_bytes(b'{"zarr_format":2}\n')
    (root / "a" / "0.0").write_bytes(b"\x01\x02\x03\x04")
    (root / "a" / "b" / "meta.json").write_bytes(b'{"shape":[10,10]}')

    v1_manifest = build_directory_manifest(root)
    v2_manifest = build_manifest_v1(root)
    assert v2_manifest == v1_manifest

    digest = compute_manifest_v1_digest(root)
    assert digest == Digest("sha256:manifest-v1", v1_manifest["tree_hash"])
    assert digest.kind is Kind.CONTAINER

    # Also verify single-matrix logical digest helper
    arr = np.array([[1.0, -0.0], [2.5, np.nan]], dtype=np.float32)
    mat_digest = compute_logical_matrix_digest("SOV__AM", arr)
    assert mat_digest.scheme == "sha256:logical_matrix:v1"
    assert mat_digest.kind is Kind.LOGICAL

    # Also verify compute_ctd1_digest helper
    df = pd.DataFrame({"k": [2, 1], "v": ["b", "a"]})
    ctd_digest = compute_ctd1_digest(df)
    assert ctd_digest.scheme == "ctd1:sha256"
    assert ctd_digest.kind is Kind.LOGICAL
