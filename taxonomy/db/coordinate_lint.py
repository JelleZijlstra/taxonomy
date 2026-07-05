from __future__ import annotations

import re
from collections.abc import Iterable

from taxonomy import coordinates
from taxonomy.apis import nominatim
from taxonomy.db import helpers
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.region import Region

DEGREES_MINUTES_RE = re.compile(
    r"^(?P<degrees>\d+(?:\.\d+)?)\s+(?P<minutes>\d+(?:\.\d+)?)\s*(?P<direction>[NSWE])$"
)


def make_point(latitude: str, longitude: str) -> coordinates.Point | None:
    try:
        _, lat = standardize_coordinate(latitude, is_latitude=True)
        _, lon = standardize_coordinate(longitude, is_latitude=False)
    except helpers.InvalidCoordinates:
        return None
    return coordinates.Point(lon, lat)


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
