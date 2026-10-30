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
    """Verify that importing consist in a fresh process does not load heavy dependencies."""
    cmd = [
        sys.executable,
        "-c",
        (
            "import sys; "
            "import consist; "
            "heavy = [m for m in sys.modules if any(x in m for x in "
            "('sqlmodel', 'sqlalchemy', 'duckdb', 'numpy', 'pyarrow', 'pandas'))]; "
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
    """Verify every symbol declared in consist.__all__ resolves."""
    for name in consist.__all__:
        # Use module dict or getattr
        val = consist.__dict__.get(name)
        if val is None:
            # Trigger lazy loader
            import importlib

            mod = importlib.import_module("consist")
            val = mod.__getattr__(name)
        assert val is not None, f"Export {name} failed to resolve"


def test_submodule_all_exports_resolve() -> None:
    """Verify every symbol declared in submodules resolves."""
    for mod in (consist.digest, consist.gtfs, consist.admission):
        assert hasattr(mod, "__all__"), f"{mod.__name__} missing __all__"
        for name in mod.__all__:
            assert hasattr(mod, name), f"{mod.__name__} missing export {name}"


def test_deprecated_top_level_exports_warn() -> None:
    """Verify deprecated top-level exports emit DeprecationWarning and resolve."""
    deprecated_samples = [
        ("GtfsSchema", "consist.gtfs"),
        ("AdmissionReport", "consist.admission"),
        ("compute_logical_matrices_sha256", "consist.digest"),
    ]
    for symbol, target_submod in deprecated_samples:
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            obj = consist.__getattr__(symbol)
            assert obj is not None
            assert len(recorded) >= 1
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


def test_invalid_attribute_raises() -> None:
    """Verify accessing an unknown attribute raises AttributeError."""
    with pytest.raises(AttributeError, match="has no attribute 'non_existent_symbol'"):
        _ = consist.__getattr__("non_existent_symbol")


def test_dir_contains_curated_symbols() -> None:
    """Verify dir(consist) contains curated symbols and submodules."""
    dir_symbols = set(dir(consist))
    assert "digest" in dir_symbols
    assert "gtfs" in dir_symbols
    assert "admission" in dir_symbols
    assert "Tracker" in dir_symbols
    assert "Run" in dir_symbols
