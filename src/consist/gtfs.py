"""
Consist GTFS models, schemas, and canonicalization helpers.
"""

from __future__ import annotations

from consist.api import canonicalize_gtfs
from consist.core.gtfs import (
    GtfsCanonicalizationResult,
    GtfsFeedSnapshot,
    canonicalize_gtfs_bundle,
    discover_gtfs_members,
)
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
    "GTFS_SCHEMA_BY_TABLE_NAME",
    "GTFS_SCHEMAS",
    "GTFS_TABLE_NAMES",
    "GtfsAgency",
    "GtfsCalendar",
    "GtfsCalendarDates",
    "GtfsFareAttributes",
    "GtfsFareRules",
    "GtfsFeedInfo",
    "GtfsFrequencies",
    "GtfsRoutes",
    "GtfsSchema",
    "GtfsShapes",
    "GtfsStopTimes",
    "GtfsStops",
    "GtfsTransfers",
    "GtfsTrips",
    "GtfsCanonicalizationResult",
    "GtfsFeedSnapshot",
    "canonicalize_gtfs",
    "canonicalize_gtfs_bundle",
    "discover_gtfs_members",
]
