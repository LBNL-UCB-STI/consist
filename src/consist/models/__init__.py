"""
The `models` module defines the core domain data structures and adapter models
used by Consist for tracking runs, artifacts, schemas, and their relationships.
"""

from __future__ import annotations

from consist.artifact import ArchivedOutputs, Artifact
from consist.models.activitysim import (
    ActivitySimCoefficientsCache,
    ActivitySimCoefficientTemplateRefsCache,
    ActivitySimConfigIngestRunLink,
    ActivitySimConstantsCache,
    ActivitySimProbabilitiesCache,
    ActivitySimProbabilitiesEntriesCache,
    ActivitySimProbabilitiesMetaEntriesCache,
)
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
    ArtifactSchemaRelation,
)
from consist.models.beam import BeamConfigCache, BeamConfigIngestRunLink
from consist.models.gtfs import (
    GTFS_SCHEMA_BY_TABLE_NAME,
    GTFS_SCHEMAS,
    GTFS_TABLE_NAMES,
    GtfsAgency,
    GtfsCalendar,
    GtfsCalendarDates,
    GtfsFareAttributes,
    GtfsFareRules,
    GtfsFeedInfo,
    GtfsFrequencies,
    GtfsRoutes,
    GtfsSchema,
    GtfsShapes,
    GtfsStopTimes,
    GtfsStops,
    GtfsTransfers,
    GtfsTrips,
)
from consist.models.run import (
    ConsistRecord,
    CouplerSnapshotPhase,
    Run,
    RunArtifactLink,
    RunArtifacts,
    RunBindingInvocation,
    RunCouplerSnapshot,
    RunResult,
)
from consist.models.run_config_kv import RunConfigKV

__all__ = [
    "Artifact",
    "ArchivedOutputs",
    "ArtifactSchema",
    "ArtifactKV",
    "ArtifactSchemaField",
    "ArtifactSchemaObservation",
    "ArtifactSchemaRelation",
    "ActivitySimConstantsCache",
    "ActivitySimCoefficientsCache",
    "ActivitySimCoefficientTemplateRefsCache",
    "ActivitySimProbabilitiesCache",
    "ActivitySimProbabilitiesEntriesCache",
    "ActivitySimProbabilitiesMetaEntriesCache",
    "ActivitySimConfigIngestRunLink",
    "BeamConfigCache",
    "BeamConfigIngestRunLink",
    "ConsistRecord",
    "GtfsSchema",
    "GtfsAgency",
    "GtfsStops",
    "GtfsRoutes",
    "GtfsCalendar",
    "GtfsCalendarDates",
    "GtfsFeedInfo",
    "GtfsShapes",
    "GtfsTrips",
    "GtfsStopTimes",
    "GtfsFrequencies",
    "GtfsTransfers",
    "GtfsFareAttributes",
    "GtfsFareRules",
    "GTFS_SCHEMAS",
    "GTFS_SCHEMA_BY_TABLE_NAME",
    "GTFS_TABLE_NAMES",
    "Run",
    "RunArtifactLink",
    "RunArtifacts",
    "RunBindingInvocation",
    "RunConfigKV",
    "RunCouplerSnapshot",
    "RunResult",
    "CouplerSnapshotPhase",
]
