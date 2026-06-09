"""
Predefined GTFS schema models.

These models are intentionally additive:
- they cover the common standardized GTFS tables Consist should type out of the box
- they keep the table/column names aligned with GTFS conventions
- they allow extra publisher-specific columns to flow through the raw relation path

The models are plain ``SQLModel`` classes rather than ``table=True`` ORM tables so
they can be registered as typed views, used for ingestion hints, and reused as schema
metadata without forcing a concrete storage shape.
"""

from __future__ import annotations

from typing import Optional

from sqlmodel import Field, SQLModel


class GtfsSchema(SQLModel):
    """Base marker for GTFS schema models."""

    feed_key: str = Field(
        index=True,
        description="Stable namespace for a feed within a GTFS bundle.",
    )

    __consist_additive_schema__ = True


class GtfsAgency(GtfsSchema):
    __tablename__ = "agency"

    agency_id: Optional[str] = Field(default=None, index=True)
    agency_name: str
    agency_url: str
    agency_timezone: str
    agency_lang: Optional[str] = None
    agency_phone: Optional[str] = None
    agency_fare_url: Optional[str] = None
    agency_email: Optional[str] = None


class GtfsStops(GtfsSchema):
    __tablename__ = "stops"

    stop_id: str = Field(index=True)
    stop_code: Optional[str] = None
    stop_name: str
    stop_desc: Optional[str] = None
    stop_lat: float
    stop_lon: float
    zone_id: Optional[str] = None
    stop_url: Optional[str] = None
    location_type: Optional[int] = None
    parent_station: Optional[str] = Field(default=None, foreign_key="stops.stop_id")
    stop_timezone: Optional[str] = None
    wheelchair_boarding: Optional[int] = None
    level_id: Optional[str] = None
    platform_code: Optional[str] = None


class GtfsRoutes(GtfsSchema):
    __tablename__ = "routes"

    route_id: str = Field(index=True)
    agency_id: Optional[str] = Field(
        default=None,
        foreign_key="agency.agency_id",
        index=True,
    )
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    route_desc: Optional[str] = None
    route_type: int
    route_url: Optional[str] = None
    route_color: Optional[str] = None
    route_text_color: Optional[str] = None
    route_sort_order: Optional[int] = None
    continuous_pickup: Optional[int] = None
    continuous_drop_off: Optional[int] = None


class GtfsCalendar(GtfsSchema):
    __tablename__ = "calendar"

    service_id: str = Field(index=True)
    monday: int
    tuesday: int
    wednesday: int
    thursday: int
    friday: int
    saturday: int
    sunday: int
    start_date: str
    end_date: str


class GtfsCalendarDates(GtfsSchema):
    __tablename__ = "calendar_dates"

    service_id: str = Field(foreign_key="calendar.service_id", index=True)
    date: str
    exception_type: int


class GtfsFeedInfo(GtfsSchema):
    __tablename__ = "feed_info"

    feed_publisher_name: str
    feed_publisher_url: str
    feed_lang: str
    default_lang: Optional[str] = None
    feed_start_date: Optional[str] = None
    feed_end_date: Optional[str] = None
    feed_version: Optional[str] = None
    feed_contact_email: Optional[str] = None
    feed_contact_url: Optional[str] = None


class GtfsShapes(GtfsSchema):
    __tablename__ = "shapes"

    shape_id: str = Field(index=True)
    shape_pt_lat: float
    shape_pt_lon: float
    shape_pt_sequence: int
    shape_dist_traveled: Optional[float] = None


class GtfsTrips(GtfsSchema):
    __tablename__ = "trips"

    route_id: str = Field(foreign_key="routes.route_id", index=True)
    service_id: str = Field(foreign_key="calendar.service_id", index=True)
    trip_id: str = Field(index=True)
    trip_headsign: Optional[str] = None
    trip_short_name: Optional[str] = None
    direction_id: Optional[int] = None
    block_id: Optional[str] = None
    shape_id: Optional[str] = Field(default=None, foreign_key="shapes.shape_id")
    wheelchair_accessible: Optional[int] = None
    bikes_allowed: Optional[int] = None


class GtfsStopTimes(GtfsSchema):
    __tablename__ = "stop_times"

    trip_id: str = Field(foreign_key="trips.trip_id", index=True)
    arrival_time: Optional[str] = None
    departure_time: Optional[str] = None
    stop_id: Optional[str] = Field(default=None, foreign_key="stops.stop_id")
    stop_sequence: int
    stop_headsign: Optional[str] = None
    pickup_type: Optional[int] = None
    drop_off_type: Optional[int] = None
    continuous_pickup: Optional[int] = None
    continuous_drop_off: Optional[int] = None
    shape_dist_traveled: Optional[float] = None
    timepoint: Optional[int] = None


class GtfsFrequencies(GtfsSchema):
    __tablename__ = "frequencies"

    trip_id: str = Field(foreign_key="trips.trip_id", index=True)
    start_time: str
    end_time: str
    headway_secs: int
    exact_times: Optional[int] = None


class GtfsTransfers(GtfsSchema):
    __tablename__ = "transfers"

    from_stop_id: Optional[str] = Field(default=None, foreign_key="stops.stop_id")
    to_stop_id: Optional[str] = Field(default=None, foreign_key="stops.stop_id")
    transfer_type: int
    min_transfer_time: Optional[int] = None
    from_route_id: Optional[str] = Field(default=None, foreign_key="routes.route_id")
    to_route_id: Optional[str] = Field(default=None, foreign_key="routes.route_id")
    from_trip_id: Optional[str] = Field(default=None, foreign_key="trips.trip_id")
    to_trip_id: Optional[str] = Field(default=None, foreign_key="trips.trip_id")


class GtfsFareAttributes(GtfsSchema):
    __tablename__ = "fare_attributes"

    fare_id: str = Field(index=True)
    price: float
    currency_type: str
    payment_method: int
    transfers: Optional[int] = None
    agency_id: Optional[str] = Field(default=None, foreign_key="agency.agency_id")
    transfer_duration: Optional[int] = None


class GtfsFareRules(GtfsSchema):
    __tablename__ = "fare_rules"

    fare_id: str = Field(foreign_key="fare_attributes.fare_id", index=True)
    route_id: Optional[str] = Field(default=None, foreign_key="routes.route_id")
    origin_id: Optional[str] = None
    destination_id: Optional[str] = None
    contains_id: Optional[str] = None


GTFS_SCHEMAS: tuple[type[SQLModel], ...] = (
    GtfsAgency,
    GtfsStops,
    GtfsRoutes,
    GtfsCalendar,
    GtfsCalendarDates,
    GtfsFeedInfo,
    GtfsShapes,
    GtfsTrips,
    GtfsStopTimes,
    GtfsFrequencies,
    GtfsTransfers,
    GtfsFareAttributes,
    GtfsFareRules,
)

GTFS_SCHEMA_BY_TABLE_NAME: dict[str, type[SQLModel]] = {
    str(getattr(model, "__tablename__", model.__name__).lower()): model
    for model in GTFS_SCHEMAS
}

GTFS_TABLE_NAMES: tuple[str, ...] = tuple(GTFS_SCHEMA_BY_TABLE_NAME)

__all__ = [
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
]
