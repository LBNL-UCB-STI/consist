"""
The ``models`` module defines the core value structures and optional user table
schemas used by Consist for tracking runs, artifacts, and their relationships.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from consist.digest import Digest
from consist.models.artifact import Artifact, ArchivedOutputs
from consist.models.artifact_facet import ArtifactFacet
from consist.models.artifact_kv import ArtifactKV
from consist.models.artifact_schema import (
    ArtifactSchema,
    ArtifactSchemaField,
    ArtifactSchemaObservation,
    ArtifactSchemaRelation,
)
from consist.models.config_facet import ConfigFacet
from consist.models.run import CouplerSnapshotPhase, Run, RunCouplerSnapshot
from consist.models.run_config_kv import RunConfigKV

if TYPE_CHECKING:
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

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ActivitySimConstantsCache": (
        "consist.models.activitysim",
        "ActivitySimConstantsCache",
    ),
    "ActivitySimCoefficientsCache": (
        "consist.models.activitysim",
        "ActivitySimCoefficientsCache",
    ),
    "ActivitySimCoefficientTemplateRefsCache": (
        "consist.models.activitysim",
        "ActivitySimCoefficientTemplateRefsCache",
    ),
    "ActivitySimProbabilitiesCache": (
        "consist.models.activitysim",
        "ActivitySimProbabilitiesCache",
    ),
    "ActivitySimProbabilitiesEntriesCache": (
        "consist.models.activitysim",
        "ActivitySimProbabilitiesEntriesCache",
    ),
    "ActivitySimProbabilitiesMetaEntriesCache": (
        "consist.models.activitysim",
        "ActivitySimProbabilitiesMetaEntriesCache",
    ),
    "ActivitySimConfigIngestRunLink": (
        "consist.models.activitysim",
        "ActivitySimConfigIngestRunLink",
    ),
    "BeamConfigCache": ("consist.models.beam", "BeamConfigCache"),
    "BeamConfigIngestRunLink": ("consist.models.beam", "BeamConfigIngestRunLink"),
    "GtfsSchema": ("consist.models.gtfs", "GtfsSchema"),
    "GtfsAgency": ("consist.models.gtfs", "GtfsAgency"),
    "GtfsStops": ("consist.models.gtfs", "GtfsStops"),
    "GtfsRoutes": ("consist.models.gtfs", "GtfsRoutes"),
    "GtfsCalendar": ("consist.models.gtfs", "GtfsCalendar"),
    "GtfsCalendarDates": ("consist.models.gtfs", "GtfsCalendarDates"),
    "GtfsFeedInfo": ("consist.models.gtfs", "GtfsFeedInfo"),
    "GtfsShapes": ("consist.models.gtfs", "GtfsShapes"),
    "GtfsTrips": ("consist.models.gtfs", "GtfsTrips"),
    "GtfsStopTimes": ("consist.models.gtfs", "GtfsStopTimes"),
    "GtfsFrequencies": ("consist.models.gtfs", "GtfsFrequencies"),
    "GtfsTransfers": ("consist.models.gtfs", "GtfsTransfers"),
    "GtfsFareAttributes": ("consist.models.gtfs", "GtfsFareAttributes"),
    "GtfsFareRules": ("consist.models.gtfs", "GtfsFareRules"),
    "GTFS_SCHEMAS": ("consist.models.gtfs", "GTFS_SCHEMAS"),
    "GTFS_SCHEMA_BY_TABLE_NAME": ("consist.models.gtfs", "GTFS_SCHEMA_BY_TABLE_NAME"),
    "GTFS_TABLE_NAMES": ("consist.models.gtfs", "GTFS_TABLE_NAMES"),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    module = importlib.import_module(module_name)
    value = vars(module)[attr_name]
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


__all__ = [
    "Artifact",
    "ArchivedOutputs",
    "ArtifactSchema",
    "ArtifactFacet",
    "ArtifactKV",
    "ArtifactSchemaField",
    "ArtifactSchemaObservation",
    "ArtifactSchemaRelation",
    "ConfigFacet",
    "Digest",
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
    "RunCouplerSnapshot",
    "CouplerSnapshotPhase",
]
