"""
The `models` module defines the core data structures and database models
used by Consist for tracking runs, artifacts, and their relationships.
"""

from __future__ import annotations

from consist.models.artifact import Artifact
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
    ArtifactSchemaRelation,
)
from consist.models.config_facet import ConfigFacet
from consist.models.run import Run
from consist.models.run_config_kv import RunConfigKV
from consist.models.activitysim import (
    ActivitySimCoefficientsCache,
    ActivitySimCoefficientTemplateRefsCache,
    ActivitySimConfigIngestRunLink,
    ActivitySimConstantsCache,
    ActivitySimProbabilitiesCache,
    ActivitySimProbabilitiesEntriesCache,
    ActivitySimProbabilitiesMetaEntriesCache,
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

__all__ = [
    "Artifact",
    "ArtifactSchema",
    "ArtifactFacet",
    "ArtifactKV",
    "ArtifactSchemaField",
    "ArtifactSchemaObservation",
    "ArtifactSchemaRelation",
    "ConfigFacet",
    "ActivitySimConstantsCache",
    "ActivitySimCoefficientsCache",
    "ActivitySimCoefficientTemplateRefsCache",
    "ActivitySimProbabilitiesCache",
    "ActivitySimProbabilitiesEntriesCache",
    "ActivitySimProbabilitiesMetaEntriesCache",
    "ActivitySimConfigIngestRunLink",
    "BeamConfigCache",
    "BeamConfigIngestRunLink",
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
    "RunConfigKV",
]
