"""
Consist v2 provenance store (`provenance.duckdb` schema, startup detection,
and `StoreLedger`).
"""

from __future__ import annotations

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
    LocationStatus,
    StoreLedger,
    deserialize_witness_json,
    open_v2_store,
    resolve_status_rank,
    serialize_witness_json,
)
from consist.store.schema import (
    V1_ONLY_TABLE_NAMES,
    V2_EXPECTED_TABLE_COLUMNS,
    V2_SCHEMA_VERSION,
    V2_TABLE_NAMES,
    V2_VIEW_NAMES,
    init_v2_schema,
    install_v1_tripwires,
    load_v2_schema_sql,
)

__all__ = [
    "ConsistError",
    "DatabaseMigrationRequired",
    "IncompatibleSchemaError",
    "LOCATION_STATUS_RANKS",
    "LocationStatus",
    "SchemaState",
    "StoreLedger",
    "V1_ONLY_TABLE_NAMES",
    "V2_EXPECTED_TABLE_COLUMNS",
    "V2_SCHEMA_VERSION",
    "V2_TABLE_NAMES",
    "V2_VIEW_NAMES",
    "_migration_workdir",
    "_resolve_auto_migrate",
    "deserialize_witness_json",
    "detect_schema",
    "init_v2_schema",
    "inspect_v1_summary",
    "install_v1_tripwires",
    "load_v2_schema_sql",
    "open_v2_store",
    "resolve_status_rank",
    "serialize_witness_json",
]
