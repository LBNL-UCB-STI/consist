"""
Consist v2 provenance store (`provenance.duckdb` schema, startup detection,
`StoreLedger`, and disaster-recovery `RunManifest` snapshots).
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from consist.store import manifest
    from consist.store.detect import (
        ConsistError,
        DatabaseMigrationRequired,
        IncompatibleSchemaError,
        SchemaState,
        _migration_workdir,
        _resolve_auto_migrate,
        detect_schema,
        inspect_v1_summary,
    )
    from consist.store.ledger import (
        LOCATION_STATUS_RANKS,
        DigestBasis,
        LocationStatus,
        StoreLedger,
        deserialize_witness_json,
        open_v2_store,
        resolve_status_rank,
        serialize_witness_json,
    )
    from consist.store.manifest import (
        SCHEMA_VERSION,
        ManifestArtifactEntry,
        ManifestLocationEntry,
        RunManifest,
        export_run_manifest,
        import_run_manifest,
        load_run_manifest,
        rebuild_store_from_manifests,
        write_run_manifest,
    )
    from consist.store.schema import (
        V1_ONLY_TABLE_NAMES,
        V2_EXPECTED_TABLE_COLUMNS,
        V2_EXPECTED_VIEW_COLUMNS,
        V2_SCHEMA_VERSION,
        V2_TABLE_NAMES,
        V2_VIEW_NAMES,
        init_v2_schema,
        install_v1_tripwires,
        load_v2_schema_sql,
    )

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ConsistError": ("consist.store.detect", "ConsistError"),
    "DatabaseMigrationRequired": ("consist.store.detect", "DatabaseMigrationRequired"),
    "DigestBasis": ("consist.store.ledger", "DigestBasis"),
    "IncompatibleSchemaError": ("consist.store.detect", "IncompatibleSchemaError"),
    "LOCATION_STATUS_RANKS": ("consist.store.ledger", "LOCATION_STATUS_RANKS"),
    "LocationStatus": ("consist.store.ledger", "LocationStatus"),
    "ManifestArtifactEntry": ("consist.store.manifest", "ManifestArtifactEntry"),
    "ManifestLocationEntry": ("consist.store.manifest", "ManifestLocationEntry"),
    "RunManifest": ("consist.store.manifest", "RunManifest"),
    "SCHEMA_VERSION": ("consist.store.manifest", "SCHEMA_VERSION"),
    "SchemaState": ("consist.store.detect", "SchemaState"),
    "StoreLedger": ("consist.store.ledger", "StoreLedger"),
    "V1_ONLY_TABLE_NAMES": ("consist.store.schema", "V1_ONLY_TABLE_NAMES"),
    "V2_EXPECTED_TABLE_COLUMNS": ("consist.store.schema", "V2_EXPECTED_TABLE_COLUMNS"),
    "V2_EXPECTED_VIEW_COLUMNS": ("consist.store.schema", "V2_EXPECTED_VIEW_COLUMNS"),
    "V2_SCHEMA_VERSION": ("consist.store.schema", "V2_SCHEMA_VERSION"),
    "V2_TABLE_NAMES": ("consist.store.schema", "V2_TABLE_NAMES"),
    "V2_VIEW_NAMES": ("consist.store.schema", "V2_VIEW_NAMES"),
    "_migration_workdir": ("consist.store.detect", "_migration_workdir"),
    "_resolve_auto_migrate": ("consist.store.detect", "_resolve_auto_migrate"),
    "deserialize_witness_json": ("consist.store.ledger", "deserialize_witness_json"),
    "detect_schema": ("consist.store.detect", "detect_schema"),
    "export_run_manifest": ("consist.store.manifest", "export_run_manifest"),
    "import_run_manifest": ("consist.store.manifest", "import_run_manifest"),
    "init_v2_schema": ("consist.store.schema", "init_v2_schema"),
    "inspect_v1_summary": ("consist.store.detect", "inspect_v1_summary"),
    "install_v1_tripwires": ("consist.store.schema", "install_v1_tripwires"),
    "load_run_manifest": ("consist.store.manifest", "load_run_manifest"),
    "load_v2_schema_sql": ("consist.store.schema", "load_v2_schema_sql"),
    "open_v2_store": ("consist.store.ledger", "open_v2_store"),
    "rebuild_store_from_manifests": (
        "consist.store.manifest",
        "rebuild_store_from_manifests",
    ),
    "resolve_status_rank": ("consist.store.ledger", "resolve_status_rank"),
    "serialize_witness_json": ("consist.store.ledger", "serialize_witness_json"),
    "write_run_manifest": ("consist.store.manifest", "write_run_manifest"),
}

_SUBMODULES = ("detect", "ledger", "manifest", "schema")

__all__ = [
    "ConsistError",
    "DatabaseMigrationRequired",
    "DigestBasis",
    "IncompatibleSchemaError",
    "LOCATION_STATUS_RANKS",
    "LocationStatus",
    "ManifestArtifactEntry",
    "ManifestLocationEntry",
    "RunManifest",
    "SCHEMA_VERSION",
    "SchemaState",
    "StoreLedger",
    "V1_ONLY_TABLE_NAMES",
    "V2_EXPECTED_TABLE_COLUMNS",
    "V2_EXPECTED_VIEW_COLUMNS",
    "V2_SCHEMA_VERSION",
    "V2_TABLE_NAMES",
    "V2_VIEW_NAMES",
    "_migration_workdir",
    "_resolve_auto_migrate",
    "deserialize_witness_json",
    "detect_schema",
    "export_run_manifest",
    "import_run_manifest",
    "init_v2_schema",
    "inspect_v1_summary",
    "install_v1_tripwires",
    "load_run_manifest",
    "load_v2_schema_sql",
    "manifest",
    "open_v2_store",
    "rebuild_store_from_manifests",
    "resolve_status_rank",
    "serialize_witness_json",
    "write_run_manifest",
]


def __getattr__(name: str) -> Any:
    if name in _SUBMODULES:
        mod = importlib.import_module(f"consist.store.{name}")
        globals()[name] = mod
        return mod
    if name in _LAZY_EXPORTS:
        module_path, attr_name = _LAZY_EXPORTS[name]
        mod = importlib.import_module(module_path)
        val = mod.__dict__[attr_name]
        globals()[name] = val
        return val
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(_LAZY_EXPORTS.keys()) | set(_SUBMODULES))
