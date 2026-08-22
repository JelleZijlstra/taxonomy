import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Self

from taxonomy import coordinates
from taxonomy.apis import nominatim
from taxonomy.db import helpers
from taxonomy.db.models.region import Region, RegionTag

DEGREES_MINUTES_RE = re.compile(
    r"^(?P<degrees>\d+(?:\.\d+)?)\s+(?P<minutes>\d+(?:\.\d+)?)\s*(?P<direction>[NSWE])$"
)

COORDINATE_TOLERANCE_KM = 5
REGION_BOUNDARY_TOLERANCE_KM = 0.5
EARTH_RADIUS_KM = 6371.0088


@dataclass(frozen=True, slots=True)
class CoordinateInterval:
    minimum: float
    maximum: float
    minimum_text: str
    maximum_text: str

    def __repr__(self) -> str:
        return self.standardized_text

    @property
    def standardized_text(self) -> str:
        if self.minimum == self.maximum:
            return self.minimum_text
        return f"{self.minimum_text}-{self.maximum_text}"

    @property
    def midpoint(self) -> float:
        return (self.minimum + self.maximum) / 2

    @property
    def is_point(self) -> bool:
        return self.minimum == self.maximum

    def union(self, other: Self) -> Self:
        if self.minimum <= other.minimum:
            minimum = self.minimum
            minimum_text = self.minimum_text
        else:
            minimum = other.minimum
            minimum_text = other.minimum_text
        if self.maximum >= other.maximum:
            maximum = self.maximum
            maximum_text = self.maximum_text
        else:
            maximum = other.maximum
            maximum_text = other.maximum_text
        return type(self)(minimum, maximum, minimum_text, maximum_text)


@dataclass(frozen=True, slots=True)
class CoordinateExtent:
    latitude: CoordinateInterval
    longitude: CoordinateInterval

    @property
    def point(self) -> coordinates.Point | None:
        if not self.latitude.is_point or not self.longitude.is_point:
            return None
        return coordinates.Point(self.longitude.minimum, self.latitude.minimum)

    @property
    def center(self) -> coordinates.Point:
        return coordinates.Point(self.longitude.midpoint, self.latitude.midpoint)

    @property
    def openstreetmap_url(self) -> str:
        return self.openstreetmap_urls[0]

    @property
    def openstreetmap_urls(self) -> tuple[str, ...]:
        point = self.point
        if point is not None:
            return (point.openstreetmap_url,)
        west = self.longitude.minimum
        east = self.longitude.maximum
        south = self.latitude.minimum
        north = self.latitude.maximum
        bounds = f"minlon={west}&minlat={south}&maxlon={east}&maxlat={north}"
        corners = ((north, west), (south, east))
        return tuple(
            f"https://www.openstreetmap.org/?{bounds}&mlat={latitude}"
            f"&mlon={longitude}"
            for latitude, longitude in corners
        )

    def union(self, other: Self) -> Self:
        return type(self)(
            latitude=self.latitude.union(other.latitude),
            longitude=self.longitude.union(other.longitude),
        )


def standardize_coordinate_pair(
    latitude: str, longitude: str
) -> tuple[str, str, CoordinateExtent] | None:
    try:
        standardized_latitude, parsed_latitude = standardize_coordinate_interval(
            latitude, is_latitude=True
        )
        standardized_longitude, parsed_longitude = standardize_coordinate_interval(
            longitude, is_latitude=False
        )
    except helpers.InvalidCoordinates:
        return None
    return (
        standardized_latitude,
        standardized_longitude,
        CoordinateExtent(latitude=parsed_latitude, longitude=parsed_longitude),
    )


def make_extent(latitude: str, longitude: str) -> CoordinateExtent | None:
    standardized = standardize_coordinate_pair(latitude, longitude)
    if standardized is None:
        return None
    return standardized[2]


def make_point(latitude: str, longitude: str) -> coordinates.Point | None:
    extent = make_extent(latitude, longitude)
    if extent is None:
        return None
    return extent.point


def distance_km(first: coordinates.Point, second: coordinates.Point) -> float:
    """Return the great-circle distance between two points in kilometres."""
    lat1 = math.radians(first.latitude)
    lat2 = math.radians(second.latitude)
    delta_lat = lat2 - lat1
    delta_lon = math.radians(second.longitude - first.longitude)
    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(haversine))


def move_point(
    point: coordinates.Point, distance_km: float, bearing_degrees: float
) -> coordinates.Point:
    """Move a point along a great-circle path at the given compass bearing."""
    angular_distance = distance_km / EARTH_RADIUS_KM
    bearing = math.radians(bearing_degrees)
    latitude = math.radians(point.latitude)
    longitude = math.radians(point.longitude)

    destination_latitude = math.asin(
        math.sin(latitude) * math.cos(angular_distance)
        + math.cos(latitude) * math.sin(angular_distance) * math.cos(bearing)
    )
    destination_longitude = longitude + math.atan2(
        math.sin(bearing) * math.sin(angular_distance) * math.cos(latitude),
        math.cos(angular_distance)
        - math.sin(latitude) * math.sin(destination_latitude),
    )
    normalized_longitude = (math.degrees(destination_longitude) + 180) % 360 - 180
    return coordinates.Point(
        longitude=normalized_longitude, latitude=math.degrees(destination_latitude)
    )


def extent_distance_km(first: CoordinateExtent, second: CoordinateExtent) -> float:
    """Return the minimum great-circle distance between two coordinate extents."""
    first_latitude, second_latitude = _closest_interval_values(
        first.latitude, second.latitude
    )
    first_longitude, second_longitude = _closest_interval_values(
        first.longitude, second.longitude
    )
    return distance_km(
        coordinates.Point(first_longitude, first_latitude),
        coordinates.Point(second_longitude, second_latitude),
    )


def _closest_interval_values(
    first: CoordinateInterval, second: CoordinateInterval
) -> tuple[float, float]:
    if first.maximum < second.minimum:
        return first.maximum, second.minimum
    if second.maximum < first.minimum:
        return first.minimum, second.maximum
    overlap = max(first.minimum, second.minimum)
    return overlap, overlap


def standardize_coordinate_interval(
    text: str, *, is_latitude: bool
) -> tuple[str, CoordinateInterval]:
    try:
        standardized, parsed = standardize_coordinate(text, is_latitude=is_latitude)
    except helpers.InvalidCoordinates:
        pass
    else:
        interval = CoordinateInterval(parsed, parsed, standardized, standardized)
        return standardized, interval

    matches: list[CoordinateInterval] = []
    for start, end in _range_separator_spans(text):
        try:
            first_text, first = standardize_coordinate(
                text[:start].strip(), is_latitude=is_latitude
            )
            second_text, second = standardize_coordinate(
                text[end:].strip(), is_latitude=is_latitude
            )
        except helpers.InvalidCoordinates:
            continue
        if first <= second:
            interval = CoordinateInterval(first, second, first_text, second_text)
        else:
            interval = CoordinateInterval(second, first, second_text, first_text)
        if interval not in matches:
            matches.append(interval)
    if len(matches) != 1:
        raise helpers.InvalidCoordinates(
            f"could not match coordinate interval {text!r}"
        )
    interval = matches[0]
    return interval.standardized_text, interval


def _range_separator_spans(text: str) -> Iterable[tuple[int, int]]:
    for match in re.finditer(r"\bto\b|[-\N{EN DASH}\N{EM DASH}]", text):
        if match.start() == 0 and match.group() == "-":
            continue
        yield match.span()


def standardize_coordinate(text: str, *, is_latitude: bool) -> tuple[str, float]:
    try:
        return helpers.standardize_coordinates(text, is_latitude=is_latitude)
    except helpers.InvalidCoordinates:
        pass

    text = text.strip().replace("·", ".")
    try:
        return _standardize_decimal_degrees(text, is_latitude=is_latitude)
    except ValueError:
        pass
    return _standardize_degrees_minutes(text, is_latitude=is_latitude)


def _standardize_decimal_degrees(text: str, *, is_latitude: bool) -> tuple[str, float]:
    value = float(text)
    _validate_coordinate_value(text, value, is_latitude=is_latitude)
    direction = _get_direction(value, is_latitude=is_latitude)
    unsigned_text = text.removeprefix("-").removeprefix("+")
    standardized, parsed = helpers.standardize_coordinates(
        f"{unsigned_text}°{direction}", is_latitude=is_latitude
    )
    return standardized, parsed


def _standardize_degrees_minutes(text: str, *, is_latitude: bool) -> tuple[str, float]:
    match = DEGREES_MINUTES_RE.match(text)
    if match is None:
        raise helpers.InvalidCoordinates(f"could not match {text!r}")

    degrees = match.group("degrees")
    minutes = match.group("minutes")
    direction = match.group("direction")
    if "." in degrees:
        raise helpers.InvalidCoordinates("fractional degrees when minutes are given")
    if is_latitude and direction not in ("N", "S"):
        raise helpers.InvalidCoordinates(f"invalid latitude {direction}")
    if not is_latitude and direction not in ("W", "E"):
        raise helpers.InvalidCoordinates(f"invalid longitude {direction}")
    if float(minutes) > 60:
        raise helpers.InvalidCoordinates(f"invalid minutes {minutes}")

    return helpers.standardize_coordinates(
        f"{degrees}°{minutes}'{direction}", is_latitude=is_latitude
    )


def _validate_coordinate_value(text: str, value: float, *, is_latitude: bool) -> None:
    limit = 90 if is_latitude else 180
    if abs(value) > limit:
        raise helpers.InvalidCoordinates(f"invalid coordinates {text}")


def _get_direction(value: float, *, is_latitude: bool) -> str:
    if is_latitude:
        return "S" if value < 0 else "N"
    return "W" if value < 0 else "E"


def check_extent_in_region(
    extent: CoordinateExtent,
    region: Region,
    *,
    require_full_containment: bool = False,
    allow_network: bool = False,
) -> Iterable[str]:
    boundary = get_region_boundary(region, allow_network=allow_network)
    if boundary is None:
        return
    boundary_region = boundary.region
    result = boundary.result
    assert result.geometry is not None
    if extent_is_in_geometry(
        extent, result.geometry, require_full_containment=require_full_containment
    ):
        return
    point = extent.point
    if point is not None:
        yield f"coordinates {point} are outside {boundary_region.name}"
        return

    if not require_full_containment:
        yield f"coordinate extent {extent} is outside {boundary_region.name}"
        return
    yield f"coordinate extent {extent} extends outside {boundary_region.name}"


def extent_is_in_geometry(
    extent: CoordinateExtent,
    geometry: coordinates.GeoGeometry,
    *,
    require_full_containment: bool = False,
) -> bool:
    point = extent.point
    if point is not None:
        return _point_is_in_geometry(point, geometry)
    geometry_kwargs = {
        "minimum_longitude": extent.longitude.minimum,
        "minimum_latitude": extent.latitude.minimum,
        "maximum_longitude": extent.longitude.maximum,
        "maximum_latitude": extent.latitude.maximum,
    }
    if not require_full_containment:
        return (
            coordinates.geometry_intersects_box(geometry, **geometry_kwargs)
            or _extent_distance_to_geometry_km(extent, geometry)
            <= REGION_BOUNDARY_TOLERANCE_KM
        )
    if coordinates.geometry_contains_box(geometry, **geometry_kwargs):
        return True
    outside_corners = [
        point
        for point in _get_extent_corners(extent)
        if not coordinates.is_in_geometry(point, geometry)
    ]
    return bool(outside_corners) and all(
        coordinates.distance_to_geometry_km(point, geometry)
        <= REGION_BOUNDARY_TOLERANCE_KM
        for point in outside_corners
    )


@dataclass(frozen=True, slots=True)
class RegionBoundary:
    region: Region
    result: nominatim.BoundaryResult


def get_region_boundary(
    region: Region, *, allow_network: bool = False
) -> RegionBoundary | None:
    """Return the deepest linked Region with usable cached OSM geometry."""
    candidate: Region | None = region
    while candidate is not None:
        if boundary := get_direct_region_boundary(
            candidate, allow_network=allow_network
        ):
            return boundary
        candidate = candidate.parent
    return None


def get_direct_region_boundary(
    region: Region, *, allow_network: bool = False
) -> RegionBoundary | None:
    tag = next(
        (
            tag
            for tag in getattr(region, "tags", ())
            if isinstance(tag, RegionTag.OpenStreetMap)
        ),
        None,
    )
    if tag is None:
        return None
    result = nominatim.lookup_boundary(
        tag.osm_type, tag.osm_id, allow_network=allow_network
    )
    if result is None or result.geometry is None:
        return None
    return RegionBoundary(region, result)


def _extent_distance_to_geometry_km(
    extent: CoordinateExtent, geometry: coordinates.GeoGeometry
) -> float:
    return min(
        coordinates.distance_to_geometry_km(point, geometry)
        for point in (extent.center, *_get_extent_corners(extent))
    )


def extent_radius_km(extent: CoordinateExtent) -> float:
    """Return the maximum center-to-corner distance of an extent."""
    return max(
        distance_km(extent.center, point) for point in _get_extent_corners(extent)
    )


def _get_extent_corners(extent: CoordinateExtent) -> tuple[coordinates.Point, ...]:
    return (
        coordinates.Point(extent.longitude.minimum, extent.latitude.minimum),
        coordinates.Point(extent.longitude.maximum, extent.latitude.minimum),
        coordinates.Point(extent.longitude.maximum, extent.latitude.maximum),
        coordinates.Point(extent.longitude.minimum, extent.latitude.maximum),
    )


def check_point_in_region(
    point: coordinates.Point, region: Region, *, allow_network: bool = False
) -> Iterable[str]:
    boundary = get_region_boundary(region, allow_network=allow_network)
    if boundary is None:
        return
    assert boundary.result.geometry is not None
    yield from _check_point_in_boundary(
        point, boundary.region, boundary.result.geometry
    )


def _check_point_in_boundary(
    point: coordinates.Point, region: Region, geometry: coordinates.GeoGeometry
) -> Iterable[str]:
    if _point_is_in_geometry(point, geometry):
        return
    yield f"coordinates {point} are outside {region.name}"


def _point_is_in_geometry(
    point: coordinates.Point, geometry: coordinates.GeoGeometry
) -> bool:
    return (
        coordinates.is_in_geometry(point, geometry)
        or coordinates.distance_to_geometry_km(point, geometry)
        <= REGION_BOUNDARY_TOLERANCE_KM
    )
