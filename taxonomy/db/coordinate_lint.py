from __future__ import annotations

import math
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Self

from taxonomy import coordinates
from taxonomy.apis import nominatim
from taxonomy.db import helpers
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.region import Region

DEGREES_MINUTES_RE = re.compile(
    r"^(?P<degrees>\d+(?:\.\d+)?)\s+(?P<minutes>\d+(?:\.\d+)?)\s*(?P<direction>[NSWE])$"
)

COORDINATE_TOLERANCE_KM = 5
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
    extent: CoordinateExtent, region: Region, *, require_full_containment: bool = False
) -> Iterable[str]:
    point = extent.point
    if point is not None:
        yield from check_point_in_region(point, region)
        return

    country = region.parent_of_kind(RegionKind.country)
    if country is None:
        return
    detailed_region_and_path = _get_detailed_region_path(region, country)
    if detailed_region_and_path is None:
        yield from check_point_in_region(extent.center, region)
        return
    detailed_region, path = detailed_region_and_path
    if _extent_intersects_path(extent, path):
        if require_full_containment and not _extent_is_contained_in_path(extent, path):
            yield f"coordinate extent {extent} extends outside {detailed_region.name}"
        return
    if detailed_region != country:
        country_path = coordinates.get_path(country.name)
        if country_path is not None and _extent_intersects_path(extent, country_path):
            yield (
                f"coordinate extent {extent} overlaps {country.name}, "
                f"but not {detailed_region.name}"
            )
            return
    yield f"coordinate extent {extent} is outside {detailed_region.name}"


def _extent_intersects_path(extent: CoordinateExtent, path: str) -> bool:
    corners = _get_extent_corners(extent)
    if any(coordinates.is_in_polygon(point, path) for point in corners):
        return True
    for polygon in coordinates.get_polygon(path):
        if any(_extent_contains_point(extent, line.p1) for line in polygon):
            return True
        box_edges = (
            coordinates.LineSegment.from_points(corners[0], corners[1]),
            coordinates.LineSegment.from_points(corners[1], corners[2]),
            coordinates.LineSegment.from_points(corners[2], corners[3]),
            coordinates.LineSegment.from_points(corners[3], corners[0]),
        )
        for polygon_edge in polygon:
            for box_edge in box_edges:
                intersection = coordinates.get_intersection(
                    polygon_edge.line, box_edge.line
                )
                if (
                    intersection is not None
                    and polygon_edge.segment_contains_point_on_line(intersection)
                    and box_edge.segment_contains_point_on_line(intersection)
                ):
                    return True
    return False


def _extent_is_contained_in_path(extent: CoordinateExtent, path: str) -> bool:
    # This is intentionally a conservative bounding-box test. It is used for
    # non-General Locations, whose range expresses uncertainty around a specific
    # locality. General islands, rivers, and other areal features legitimately
    # have bounding-box corners outside their land or river polygon.
    return all(
        coordinates.is_in_polygon(point, path) for point in _get_extent_corners(extent)
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


def _extent_contains_point(extent: CoordinateExtent, point: coordinates.Point) -> bool:
    return (
        extent.latitude.minimum <= point.latitude <= extent.latitude.maximum
        and extent.longitude.minimum <= point.longitude <= extent.longitude.maximum
    )


def check_point_in_region(point: coordinates.Point, region: Region) -> Iterable[str]:
    country = region.parent_of_kind(RegionKind.country)
    if country is None:
        return

    detailed_region_and_path = _get_detailed_region_path(region, country)
    if detailed_region_and_path is not None:
        detailed_region, path = detailed_region_and_path
        if coordinates.is_in_polygon(point, path):
            return
        if detailed_region != country:
            country_path = coordinates.get_path(country.name)
            if country_path is not None and coordinates.is_in_polygon(
                point, country_path
            ):
                actual_region = _get_containing_child_region(point, country)
                if actual_region is not None:
                    yield (
                        f"coordinates {point} are in {actual_region.name}, "
                        f"not {detailed_region.name}"
                    )
                    return
                nearest_region = _get_nearest_child_region(point, country)
                if nearest_region == detailed_region:
                    return
                if nearest_region is not None:
                    yield (
                        f"coordinates {point} are closest to {nearest_region.name}, "
                        f"not {detailed_region.name}"
                    )
                    return
                yield (
                    f"coordinates {point} are outside {detailed_region.name}, "
                    f"{country.name}"
                )
                return

    yield from _check_country(point, country)


def _get_detailed_region_path(
    region: Region, country: Region
) -> tuple[Region, str] | None:
    current: Region | None = region
    while current is not None and current != country:
        path = coordinates.get_region_path(current.name, country.name)
        if path is not None:
            return current, path
        current = current.parent

    path = coordinates.get_path(country.name)
    if path is None:
        return None
    return country, path


def _get_containing_child_region(
    point: coordinates.Point, country: Region
) -> Region | None:
    for child in sorted(country.children, key=lambda region: region.name):
        path = coordinates.get_region_path(child.name, country.name)
        if path is None:
            continue
        if coordinates.is_in_polygon(point, path):
            return child
    return None


def _get_nearest_child_region(
    point: coordinates.Point, country: Region
) -> Region | None:
    nearest: tuple[float, Region] | None = None
    for child in sorted(country.children, key=lambda region: region.name):
        path = coordinates.get_region_path(child.name, country.name)
        if path is None:
            continue
        distance = coordinates.get_distance_to_polygon(point, path)
        if nearest is None or distance < nearest[0]:
            nearest = (distance, child)
    return nearest[1] if nearest is not None else None


def _check_country(point: coordinates.Point, country: Region) -> Iterable[str]:
    polygon_path = coordinates.get_path(country.name)
    if polygon_path is not None and coordinates.is_in_polygon(point, polygon_path):
        return
    osm_country = nominatim.get_openstreetmap_country(point)
    if osm_country is None:
        yield f"cannot place coordinates {point} in any country (expected {country.name})"
        return
    our_country = country.name
    if osm_country == our_country:
        return
    our_country = nominatim.HESP_COUNTRY_TO_OSM_COUNTRY.get(our_country, our_country)
    if our_country == osm_country:
        return
    yield f"coordinates {point} are in {osm_country}, not {country.name}"
