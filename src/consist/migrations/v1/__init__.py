"""
Consist v1 database reader and fast-hash classification helpers for `consist migrate`.
"""

from __future__ import annotations

from consist.migrations.v1.fasthash import (
    RACY_STAT_WINDOW_NS,
    compute_v1_fast_file_hash,
    stat_v1_fast_file_hash,
)
from consist.migrations.v1.migrator import (
    ArtifactClassification,
    ClassificationBasis,
    ClassificationOutcome,
    MigrationPlan,
    MigrationResult,
    MigrationVerificationReport,
    _migration_lock_context,
    activate_migration,
    apply_migration,
    classify_v1_artifact,
    default_migration_workers,
    migrate_v1_to_v2,
    open_v2_store,
    plan_migration,
    preflight_check_migration,
    promote_historical_artifact,
    undo_migration,
    verify_migration,
)
from consist.migrations.v1.reader import (
    V1ArtifactRecord,
    V1DatabaseReader,
    V1DirEntryRecord,
    V1DltTableInfo,
    V1LinkRecord,
    V1RunRecord,
    V1SchemaInventory,
)

__all__ = [
    "ArtifactClassification",
    "ClassificationBasis",
    "ClassificationOutcome",
    "MigrationPlan",
    "MigrationResult",
    "MigrationVerificationReport",
    "RACY_STAT_WINDOW_NS",
    "V1ArtifactRecord",
    "V1DatabaseReader",
    "V1DirEntryRecord",
    "V1DltTableInfo",
    "V1LinkRecord",
    "V1RunRecord",
    "V1SchemaInventory",
    "_migration_lock_context",
    "activate_migration",
    "apply_migration",
    "classify_v1_artifact",
    "compute_v1_fast_file_hash",
    "default_migration_workers",
    "migrate_v1_to_v2",
    "open_v2_store",
    "plan_migration",
    "preflight_check_migration",
    "promote_historical_artifact",
    "stat_v1_fast_file_hash",
    "undo_migration",
    "verify_migration",
]
