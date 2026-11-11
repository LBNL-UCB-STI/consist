"""Unit tests for package imports, zero-weight isolation, and namespace curation."""

from __future__ import annotations

import subprocess
import sys
import warnings

import pytest

import consist
import consist.admission
import consist.digest
import consist.gtfs


def test_import_consist_subprocess_isolation() -> None:
    """Verify that importing consist and consist.store.manifest does not load heavy dependencies."""
    cmd = [
        sys.executable,
        "-c",
        (
            "import sys; "
            "import consist, consist.store.manifest; "
            "heavy = [m for m in sys.modules if any(x in m for x in "
            "('sqlmodel', 'sqlalchemy', 'duckdb', 'dlt', 'ibis', 'numpy', 'pyarrow', 'pandas'))]; "
            "assert not heavy, f'Heavy modules loaded: {heavy}'"
        ),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"


def test_import_digest_subprocess_isolation() -> None:
    """Verify that importing consist.digest in a fresh process does not load heavy dependencies."""
    cmd = [
        sys.executable,
        "-c",
        (
            "import sys; "
            "from consist.digest import digest_table, file_sha256, canonical_json_sha256; "
            "heavy = [m for m in sys.modules if any(x in m for x in "
            "('sqlmodel', 'sqlalchemy', 'duckdb', 'pandas'))]; "
            "assert not heavy, f'Heavy modules loaded: {heavy}'"
        ),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"STDOUT: {res.stdout}\nSTDERR: {res.stderr}"


def test_all_curated_exports_resolve() -> None:
    """Verify every symbol declared in the 58-symbol consist.__all__ resolves."""
    assert len(consist.__all__) == 58
    for name in consist.__all__:
        val = consist.__dict__.get(name)
        if val is None:
            val = consist.__getattr__(name)
        assert val is not None, f"Export {name} failed to resolve"


def test_submodule_all_exports_resolve() -> None:
    """Verify every symbol declared in submodules resolves."""
    for mod in (consist.digest, consist.gtfs, consist.admission):
        assert "__all__" in mod.__dict__, f"{mod.__name__} missing __all__"
        for name in mod.__all__:
            if name not in mod.__dict__:
                loader = mod.__dict__.get("__getattr__")
                assert loader is not None, f"{mod.__name__} missing export {name}"
                assert loader(name) is not None
            else:
                assert mod.__dict__[name] is not None


def test_deprecated_top_level_exports_warn() -> None:
    """Verify deprecated top-level exports emit DeprecationWarning and resolve."""
    deprecated_samples = [
        ("GtfsSchema", "consist.gtfs"),
        ("AdmissionReport", "consist.admission"),
        ("compute_logical_matrices_sha256", "consist.digest"),
        ("Digest", "consist.digest"),
        ("StoreLedger", "consist.store"),
        ("migrate_v1_to_v2", "consist.migrations"),
        ("ViewFactory", "consist.core.views"),
    ]
    for symbol, target_submod in deprecated_samples:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            obj = consist.__getattr__(symbol)
            assert obj is not None
            dep_warnings = [
                w for w in recorded if issubclass(w.category, DeprecationWarning)
            ]
            assert dep_warnings, f"No DeprecationWarning for {symbol}"
            assert target_submod in str(dep_warnings[0].message)

            # Accessing again must warn again (not cached in globals)
            with warnings.catch_warnings(record=True) as recorded2:
                warnings.simplefilter("always")
                obj2 = consist.__getattr__(symbol)
                assert obj2 is obj
                dep_warnings2 = [
                    w for w in recorded2 if issubclass(w.category, DeprecationWarning)
                ]
                assert dep_warnings2, f"Subsequent access of {symbol} did not warn"


def test_removed_top_level_exports_raise_attribute_error() -> None:
    """Verify removed v0.5 top-level symbols raise AttributeError."""
    for removed in (
        "db_session",
        "run_query",
        "manifest",
        "RunManifest",
        "export_run_manifest",
        "import_run_manifest",
        "rebuild_store_from_manifests",
        "non_existent_symbol",
    ):
        with pytest.raises(AttributeError, match=f"has no attribute '{removed}'"):
            _ = consist.__getattr__(removed)


def test_dir_contains_curated_symbols() -> None:
    """Verify dir(consist) contains curated symbols and submodules."""
    dir_symbols = set(dir(consist))
    assert "digest" in dir_symbols
    assert "gtfs" in dir_symbols
    assert "admission" in dir_symbols
    assert "Tracker" in dir_symbols
    assert "Artifact" in dir_symbols
    assert "Run" in dir_symbols
    assert "query_df" in dir_symbols
    assert "query_rows" in dir_symbols
    assert "ParameterizedSQL" in dir_symbols
