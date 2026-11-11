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
    assert ident_bare.refines(ident_bare)
    assert step1.refines(ident_bare)
    assert step2.refines(step1)
    assert step2.refines(ident_bare)
    assert not ident_bare.refines(step1)
    assert not step1.refines(step2)
    assert refines(step2, ident_bare)
    assert not refines(ident_bare, step2)

    other_anchor_ident = Identity(anchor=Digest("sha256:file", "f" * 64))
    assert not ident_bare.refines(other_anchor_ident)

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

    # 5. Incompatible drivers under LOGICAL_DATA -> Agreement.UNVERIFIABLE, agree_reachable=False
    v_incompat = compare(
        Identity(d_sha_1),
        Identity(d_sha_2),
        Contract.LOGICAL_DATA,
        driver_a="parquet",
        driver_b="zarr",
    )
    assert v_incompat == Verdict(Agreement.UNVERIFIABLE, None, (), False)

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

    # Racy capture (abs(now_ns - mtime_ns) < 2s) returns STALE immediately whenever racy is True
    racy_obs_ns = raw_st.st_mtime_ns + 500_000_000  # 0.5s after mtime
    racy_stat = FileStat.capture(f, observed_at_ns=racy_obs_ns)
    assert racy_stat.racy is True
    assert (
        racy_stat.check(f, now_ns=raw_st.st_mtime_ns + 1_000_000_000) is Freshness.STALE
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


def test_phase1_location_racy_subdirs_symlinks_and_ledger_uid_guard(
    tmp_path: Path,
) -> None:
    from consist.location import (
        lookup_live_ledger,
        register_live_ledger,
        unregister_live_ledger,
    )

    # 1. Symmetric negative clock-skew racy check on FileStat and DirTreeStat
    sample = tmp_path / "skew.bin"
    sample.write_bytes(b"clock-skew")
    st = sample.stat()
    neg_skew_ns = st.st_mtime_ns - 500_000_000  # observed 0.5s BEFORE mtime (NFS skew)
    skew_fstat = FileStat.capture(sample, observed_at_ns=neg_skew_ns)
    assert skew_fstat.racy is True
    assert skew_fstat.check(sample) is Freshness.STALE

    tree = tmp_path / "tree_subdirs"
    (tree / "sub_a").mkdir(parents=True)
    (tree / "file.txt").write_bytes(b"123")
    tree_st = (tree / "file.txt").stat()
    racy_dir = DirTreeStat.capture(
        tree, observed_at_ns=tree_st.st_mtime_ns - 100_000_000
    )
    assert racy_dir.racy is True
    assert racy_dir.check(tree) is Freshness.STALE

    # 2. DirTreeStat tracks empty subdirectories & legacy witness_json defaults subdirs=()
    safe_ns = max(tree.stat().st_mtime_ns, tree_st.st_mtime_ns) + 10_000_000_000
    clean_dir = DirTreeStat.capture(tree, observed_at_ns=safe_ns)
    assert clean_dir.racy is False
    assert [rel for rel, _ in clean_dir.subdirs] == ["sub_a"]
    assert clean_dir.check(tree) is Freshness.FRESH

    # Adding an empty subdirectory changes witness -> STALE
    (tree / "empty_added").mkdir()
    assert clean_dir.check(tree) is Freshness.STALE
    updated_dir = DirTreeStat.capture(
        tree,
        observed_at_ns=max(s.stat().st_mtime_ns for s in [tree, tree / "empty_added"])
        + 10_000_000_000,
    )
    assert [rel for rel, _ in updated_dir.subdirs] == ["empty_added", "sub_a"]
    assert updated_dir.check(tree) is Freshness.FRESH
    # Removing empty subdirectory -> STALE
    (tree / "empty_added").rmdir()
    assert updated_dir.check(tree) is Freshness.STALE

    # Backward-compatible deserialization when legacy witness_json omits 'subdirs'
    import json
    from consist.location import deserialize_witness_json, serialize_witness_json

    raw_clean_dict = json.loads(serialize_witness_json(clean_dir) or "{}")
    raw_clean_dict.pop("subdirs", None)
    legacy_parsed = deserialize_witness_json(json.dumps(raw_clean_dict))
    assert isinstance(legacy_parsed, DirTreeStat)
    assert legacy_parsed.subdirs == ()

    # Symlinked file or directory inside tree (including nested subdirs) causes DirTreeStat.check -> STALE
    outside = tmp_path / "outside_target.txt"
    outside.write_bytes(b"secret")
    sym_tree = tmp_path / "sym_tree"
    (sym_tree / "nested" / "empty_leaf").mkdir(parents=True)
    (sym_tree / "real.txt").write_bytes(b"ok")
    sym_safe_ns = (
        max(
            (sym_tree / "real.txt").stat().st_mtime_ns,
            (sym_tree / "nested" / "empty_leaf").stat().st_mtime_ns,
        )
        + 10_000_000_000
    )
    sym_stat = DirTreeStat.capture(sym_tree, observed_at_ns=sym_safe_ns)
    assert [rel for rel, _ in sym_stat.subdirs] == ["nested", "nested/empty_leaf"]
    assert sym_stat.check(sym_tree) is Freshness.FRESH
    (sym_tree / "nested" / "empty_leaf").rmdir()
    assert sym_stat.check(sym_tree) is Freshness.STALE
    (sym_tree / "nested" / "empty_leaf").mkdir()
    sym_stat = DirTreeStat.capture(
        sym_tree,
        observed_at_ns=(sym_tree / "nested" / "empty_leaf").stat().st_mtime_ns
        + 10_000_000_000,
    )
    assert sym_stat.check(sym_tree) is Freshness.FRESH
    (sym_tree / "nested" / "linked.txt").symlink_to(outside)
    assert sym_stat.check(sym_tree) is Freshness.STALE
    (sym_tree / "nested" / "linked.txt").unlink()
    outside_dir = tmp_path / "outside_dir"
    outside_dir.mkdir()
    (sym_tree / "nested" / "linked_dir").symlink_to(outside_dir)
    assert sym_stat.check(sym_tree) is Freshness.STALE

    # 3. lookup_live_ledger verifies ledger.db_uid == ref.db_uid on instance_key AND db_uri
    class _DummyLedger:
        def __init__(self, db_uid: str) -> None:
            self.db_uid = db_uid

    dummy = _DummyLedger(db_uid="uid-actual")
    inst_key = "test-inst-uid-guard"
    db_file = tmp_path / "test-uid-guard.duckdb"
    db_uri = db_file.resolve().as_uri()
    register_live_ledger(dummy, instance_key=inst_key, db_path=db_file)
    try:
        # Matching db_uid resolves via both instance_key and fallback db_uri
        assert (
            lookup_live_ledger(
                LedgerRef(db_uid="uid-actual", instance_key=inst_key, db_uri=db_uri)
            )
            is dummy
        )
        assert (
            lookup_live_ledger(
                LedgerRef(db_uid="uid-actual", instance_key="stale-inst", db_uri=db_uri)
            )
            is dummy
        )
        # Mismatched db_uid MUST return None on both instance_key and db_uri lookups!
        assert (
            lookup_live_ledger(
                LedgerRef(db_uid="uid-wrong", instance_key=inst_key, db_uri=db_uri)
            )
            is None
        )
        assert (
            lookup_live_ledger(
                LedgerRef(db_uid="uid-wrong", instance_key="stale-inst", db_uri=db_uri)
            )
            is None
        )
    finally:
        unregister_live_ledger(dummy, instance_key=inst_key, db_path=db_file)


def test_phase1_digest_blosc2_logical_bytes_expectation_and_symlink_manifest(
    tmp_path: Path,
) -> None:
    from consist.digest import _reject_manifest_symlink_components

    # 1. blosc2:header:v1 has content_pure=False
    assert SchemeRegistry.get("blosc2:header:v1").content_pure is False

    # 2. compare under Contract.LOGICAL_DATA matches identical whole-file Kind.BYTES without re-parsing
    d_same = Digest("sha256:file", "a" * 64)
    assert compare(
        Identity(d_same),
        Identity(d_same),
        Contract.LOGICAL_DATA,
    ) == Verdict(Agreement.MATCH, "sha256:file", (), True)
    assert compare(
        Identity(d_same),
        Identity(d_same),
        Contract.LOGICAL_DATA,
        driver_a="json",
        driver_b="json",
    ) == Verdict(Agreement.MATCH, "sha256:file", (), True)
    # Member driver does NOT short-circuit on container bytes
    v_member = compare(
        Identity(d_same),
        Identity(d_same),
        Contract.LOGICAL_DATA,
        driver_a="member",
        driver_b="member",
    )
    assert v_member.agreement is Agreement.UNKNOWN

    # 3. Expectation.digests normalized to frozenset, participates in eq/hash, rejects all-HISTORICAL
    d1 = Digest("sha256:file", "1" * 64)
    d2 = Digest("ctd1:sha256", "2" * 64)
    d_hist = Digest("legacy-v1:unverified", "9" * 64)
    exp_a = Expectation(
        accepts_kinds=frozenset({Kind.BYTES, Kind.LOGICAL}),
        digests=[d1, d2],
    )
    exp_b = Expectation(
        accepts_kinds=frozenset({Kind.BYTES, Kind.LOGICAL}),
        digests=[d2, d1],
    )
    exp_c = Expectation(
        accepts_kinds=frozenset({Kind.BYTES, Kind.LOGICAL}),
        digests=[d1],
    )
    assert isinstance(exp_a.digests, frozenset)
    assert exp_a == exp_b
    assert hash(exp_a) == hash(exp_b)
    assert exp_a != exp_c

    with pytest.raises(ValueError, match="HISTORICAL"):
        Expectation(
            accepts_kinds=frozenset({Kind.BYTES}),
            digests=[d_hist],
        )

    # 4. _reject_manifest_symlink_components allows ancestor symlink (like macOS /var -> /private/var)
    # while rejecting symlink at root or any descendant
    real_parent = tmp_path / "real_parent"
    real_dir = real_parent / "manifest_dir"
    (real_dir / "sub").mkdir(parents=True)
    (real_dir / "sub" / "data.bin").write_bytes(b"ok")

    sym_parent = tmp_path / "sym_parent"
    sym_parent.symlink_to(real_parent)
    resolved_via_sym_parent = _reject_manifest_symlink_components(
        sym_parent / "manifest_dir",
        label="root",
    )
    assert resolved_via_sym_parent == real_dir.resolve()
    assert build_manifest_v1(sym_parent / "manifest_dir") == build_manifest_v1(real_dir)

    # Symlink at root itself raises ValueError
    sym_root = tmp_path / "sym_manifest_dir"
    sym_root.symlink_to(real_dir)
    with pytest.raises(ValueError, match="symlink"):
        _reject_manifest_symlink_components(sym_root, label="root")

    # Symlink inside canonical_root raises ValueError
    ext_file = tmp_path / "ext.bin"
    ext_file.write_bytes(b"ext")
    (real_dir / "sub" / "bad_link.bin").symlink_to(ext_file)
    with pytest.raises(ValueError, match="symlink"):
        _reject_manifest_symlink_components(
            real_dir / "sub" / "bad_link.bin", label="entry"
        )


def test_phase1_meta_preserved_keys_and_artifact_surface(tmp_path: Path) -> None:
    from datetime import datetime, timezone
    import sys
    import uuid
    from pydantic import TypeAdapter

    from consist.artifact import (
        ArchivedOutputs,
        Artifact,
        ArtifactAccessibility,
        ArtifactLocationStatus,
        HintLedger,
    )
    from consist.meta import (
        PRESERVED_SYSTEM_META_KEYS,
        SYSTEM_META_KEYS,
        merge_artifact_output_meta,
        preserve_system_artifact_meta,
        strip_system_meta,
    )

    # 1. SYSTEM_META_KEYS vs PRESERVED_SYSTEM_META_KEYS and pure-dict helpers
    assert {"_dlt_table_name", "is_ingested", "schema_id"} <= SYSTEM_META_KEYS
    assert "recovery_roots" not in SYSTEM_META_KEYS
    assert "facet" not in SYSTEM_META_KEYS
    assert "schema_profile" not in SYSTEM_META_KEYS
    assert "output_set_key" not in SYSTEM_META_KEYS
    assert "facet" in PRESERVED_SYSTEM_META_KEYS
    assert "schema_profile" in PRESERVED_SYSTEM_META_KEYS
    assert "output_set_key" in PRESERVED_SYSTEM_META_KEYS
    assert "manifest_artifact_id" in PRESERVED_SYSTEM_META_KEYS
    assert "recovery_roots" in PRESERVED_SYSTEM_META_KEYS
    assert "is_ingested" not in PRESERVED_SYSTEM_META_KEYS
    assert "_dlt_table_name" not in PRESERVED_SYSTEM_META_KEYS
    assert "dlt_table_name" not in PRESERVED_SYSTEM_META_KEYS

    stripped = strip_system_meta(
        {
            "is_ingested": True,
            "_dlt_table_name": "t1",
            "_dlt_load_id": "l1",
            "schema_id": "s1",
            "recovery_roots": ["/archive/root1"],
            "facet": {"zone": 1},
            "user_k": "v",
        }
    )
    assert dict(stripped) == {
        "recovery_roots": ["/archive/root1"],
        "facet": {"zone": 1},
        "user_k": "v",
    }

    existing = {
        "user_old": 1,
        "facet": {"mode": "transit"},
        "output_set_key": "skims",
        "output_set_custom": "grp",
        "file_bundle_part": 2,
        "recovery_roots": ["/arch"],
        "_dlt_table_name": "skims_tbl",
        "_dlt_load_id": "l2",
        "directory_manifest": {"entries": []},
        "is_ingested": True,
    }
    replaced = merge_artifact_output_meta(existing, {"user_new": 2}, merge=False)
    assert replaced == {
        "user_new": 2,
        "facet": {"mode": "transit"},
        "output_set_key": "skims",
        "output_set_custom": "grp",
        "file_bundle_part": 2,
        "recovery_roots": ["/arch"],
    }
    merged = merge_artifact_output_meta(existing, {"user_new": 2}, merge=True)
    assert merged["user_old"] == 1
    assert merged["user_new"] == 2
    assert preserve_system_artifact_meta({"schema_summary": {"cols": 3}}, {"a": 1}) == {
        "a": 1,
        "schema_summary": {"cols": 3},
    }
    assert "consist.artifact" not in consist.meta.__dict__.values()

    # 2. HintLedger returns () when requested anchors do NOT match (no unsafe fallback!)
    d_hint = Digest("sha256:file", "a" * 64)
    d_other = Digest("sha256:file", "b" * 64)
    obs_hint = Observation(
        location=Location(uri="workspace://hint.parquet"),
        identity=Identity(d_hint),
        access_driver="parquet",
        witness=None,
        basis="computed",
        site="local",
    )
    hl = HintLedger((obs_hint,))
    assert hl.memoized_candidates(d_other) == ()
    assert hl.candidates_for((str(d_other),)) == ()
    assert len(hl.memoized_candidates(d_hint)) == 1
    assert len(hl.candidates_for((str(d_hint),))) == 1

    # 3. Artifact Phase 1 surface: parent_artifact_id, created_at, weak tracker binding,
    # compatibility properties, accessibility, as_df, Pydantic TypeAdapter, and pickle
    from consist.digest import file_sha256

    pq_path = tmp_path / "trips.parquet"
    df_expected = pd.DataFrame({"trip_id": [10, 20], "dist": [1.5, 3.0]})
    df_expected.to_parquet(pq_path, index=False)

    parent_id = uuid.uuid4()
    ts = datetime(2026, 11, 10, 12, 0, 0, tzinfo=timezone.utc)
    obs_pq = Observation(
        location=Location(uri=pq_path.resolve().as_uri(), member=None),
        identity=Identity(Digest("sha256:file", file_sha256(pq_path))),
        access_driver="parquet",
        witness=FileStat.capture(
            pq_path,
            observed_at_ns=pq_path.stat().st_mtime_ns + 5_000_000_000,
        ),
        basis="computed",
        site="local",
    )
    obs_hot = Observation(
        location=Location(uri="db://global_tables/trips", member=None),
        identity=obs_pq.identity,
        access_driver="duckdb_table",
        witness=None,
        basis="computed",
        site="local",
    )
    art = Artifact(
        id=uuid.uuid4(),
        key="trips",
        driver="parquet",
        identity=obs_pq.identity,
        parent_artifact_id=parent_id,
        created_at=ts,
        meta=FrozenMeta(
            {"recovery_roots": ["/archive/r1", "/archive/r2", "/archive/r1"]}
        ),
        hints=(obs_pq, obs_hot),
    )
    assert art.hash == str(art.anchor)
    assert art.parent_artifact_id == parent_id
    assert art.created_at == ts
    assert art.created_at_iso == ts.isoformat()
    assert art.primary_location == obs_pq.location
    assert art.recovery_roots == ["/archive/r1", "/archive/r2"]
    with pytest.warns(DeprecationWarning):
        assert art.container_uri == pq_path.resolve().as_uri()
    assert art.member is None
    assert art.is_ingested is True
    assert art.with_hints((obs_pq,)).is_ingested is False

    # Also verify member/table_path/array_path on h5_table & zarr drivers
    obs_member = Observation(
        location=Location(uri="workspace://pipeline.h5", member="/trips"),
        identity=Identity(Digest("ctd1:sha256", "d" * 64)),
        access_driver="h5_table",
        witness=None,
        basis="computed",
        site="local",
    )
    art_h5 = Artifact(
        id=uuid.uuid4(),
        key="trips_h5",
        driver="h5_table",
        identity=obs_member.identity,
        hints=(obs_member,),
    )
    assert art_h5.member == "/trips"
    with pytest.warns(DeprecationWarning):
        assert art_h5.table_path == "/trips"
    with pytest.warns(DeprecationWarning):
        assert art_h5.array_path is None

    # Per-instance weak tracker binding keeps Artifact frozen & picklable without global leaks
    class _DummyTracker:
        pass

    trk = _DummyTracker()
    art.bind_tracker(trk)
    assert art.resolve_bound_tracker() is trk
    unpickled = pickle.loads(pickle.dumps(art))
    assert unpickled == art
    assert unpickled.resolve_bound_tracker() is None

    # with_meta preserves system meta (e.g. recovery_roots) when merge=False
    art2 = art.with_meta({"note": "updated"}, merge=False)
    assert art2.meta["note"] == "updated"
    assert art2.recovery_roots == ["/archive/r1", "/archive/r2"]
    assert art2.is_ingested is True
    art3 = art2.with_schema("schema_xyz")
    assert art3.schema_id == "schema_xyz"

    # accessibility & as_df via hint resolution
    acc = art.accessibility()
    assert isinstance(acc, ArtifactAccessibility)
    assert acc.accessible is True
    assert acc.status == "primary"
    assert acc.best_location is not None
    assert isinstance(acc.locations[0], ArtifactLocationStatus)
    pd.testing.assert_frame_equal(art.as_df(), df_expected)

    archived_info = ArchivedOutputs(paths={"trips": pq_path}, outputs={"trips": art})
    assert archived_info["trips"] == pq_path
    assert archived_info.outputs["trips"] == art
    assert len(archived_info) == 1

    # Strict Pydantic v2 JSON + Python round-trip preserving origin, per-hint identity,
    # migrated_v1 basis, empty hints when origin is present, and recovery_roots
    adapter = TypeAdapter(Artifact)

    def _assert_full_roundtrip(target: Artifact) -> None:
        for restored in (
            adapter.validate_json(adapter.dump_json(target)),
            adapter.validate_python(adapter.dump_python(target, mode="json")),
        ):
            assert restored == target
            assert (
                restored.id,
                restored.key,
                restored.driver,
                restored.identity,
                restored.recorded_anchor,
                restored.origin,
                restored.run_id,
                restored.parent_artifact_id,
                restored.schema_id,
                restored.created_at,
                restored.meta,
                restored.hints,
            ) == (
                target.id,
                target.key,
                target.driver,
                target.identity,
                target.recorded_anchor,
                target.origin,
                target.run_id,
                target.parent_artifact_id,
                target.schema_id,
                target.created_at,
                target.meta,
                target.hints,
            )

    _assert_full_roundtrip(art)

    art_origin_only = Artifact(
        id=uuid.uuid4(),
        key="ledger_backed",
        driver="parquet",
        identity=obs_pq.identity,
        origin=LedgerRef(
            db_uid="uid-123",
            instance_key="inst-456",
            db_uri="file:///tmp/provenance.duckdb",
        ),
        run_id="run_1",
        schema_id="sch_1",
        created_at=ts,
        meta=FrozenMeta(
            {"recovery_roots": ["/recovery/a"], "_dlt_custom": "kept_on_v2"}
        ),
        hints=(),
    )
    _assert_full_roundtrip(art_origin_only)

    hist_id = Identity(Digest("legacy-v1:unverified", "9" * 64))
    promoted_obs_id = Identity(
        Digest("sha256:file", "8" * 64),
        claims=(Digest("ctd1:sha256", "7" * 64),),
    )
    art_asserted_promo = Artifact(
        id=uuid.uuid4(),
        key="promoted_hist",
        driver="parquet",
        identity=hist_id,
        hints=(
            Observation(
                location=Location(uri="workspace://restored.parquet"),
                identity=promoted_obs_id,
                access_driver="parquet",
                witness=None,
                basis="migrated_v1",
                site="cluster_a",
                scope_key="lustre",
                mount_instance_id="mnt-1",
                observed_at_ns=42,
            ),
        ),
    )
    assert art_asserted_promo.hints[0].identity != art_asserted_promo.identity
    _assert_full_roundtrip(art_asserted_promo)
    assert sys.modules.get("consist.artifact") is not None
