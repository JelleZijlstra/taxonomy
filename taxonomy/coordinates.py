# static analysis: ignore[attribute_is_never_set]
import functools
import json
from dataclasses import dataclass
from typing import Self

import unidecode

from taxonomy.config import get_options


@dataclass(frozen=True, slots=True)
class Point:
    longitude: float
    latitude: float

    @property
    def openstreetmap_url(self) -> str:
        return f"https://www.openstreetmap.org/?mlat={self.latitude}&mlon={self.longitude}&zoom=12"


_ORIGIN = Point(0, 0)


@dataclass(frozen=True, slots=True)
class Line:
    a: float
    b: float


@dataclass(frozen=True, slots=True)
class VerticalLine:
    x: float


@dataclass(frozen=True, slots=True)
class LineSegment:
    p1: Point
    p2: Point
    line: Line | VerticalLine

    @classmethod
    def from_points(cls, p1: Point, p2: Point) -> Self:
        return cls(p1, p2, make_line(p1, p2))

    def segment_contains_point_on_line(self, p: Point) -> bool:
        min_lat = min(self.p1.latitude, self.p2.latitude)
        max_lat = max(self.p1.latitude, self.p2.latitude)
        if not (min_lat <= p.latitude <= max_lat):
            return False
        min_long = min(self.p1.longitude, self.p2.longitude)
        max_long = max(self.p1.longitude, self.p2.longitude)
        if not (min_long <= p.longitude <= max_long):
            return False
        return True


def make_line(p1: Point, p2: Point) -> Line | VerticalLine:
    if p2.longitude == p1.longitude:
        return VerticalLine(p1.longitude)
    a = (p2.latitude - p1.latitude) / (p2.longitude - p1.longitude)
    b = p1.latitude - a * p1.longitude
    return Line(a, b)


def get_intersection(l1: Line | VerticalLine, l2: Line | VerticalLine) -> Point | None:
    match (l1, l2):
        case (Line() as l1, Line() as l2):
            if l1.a == l2.a:  # static analysis: ignore[undefined_attribute]
                return None
            # static analysis: ignore[undefined_attribute]
            longitude = (l2.b - l1.b) / (l1.a - l2.a)
            # static analysis: ignore[undefined_attribute]
            latitude = l1.a * longitude + l1.b
            return Point(longitude, latitude)
        case (VerticalLine() as l1, Line() as l2):
            # static analysis: ignore[undefined_attribute]
            return Point(l1.x, l2.a * l1.x + l2.b)
        case (Line() as l1, VerticalLine() as l2):
            # static analysis: ignore[undefined_attribute]
            return Point(l2.x, l1.a * l2.x + l1.b)
        case (VerticalLine(), VerticalLine()):
            return None
    assert False, "unreachable"


@functools.lru_cache(maxsize=256)
def get_polygon(path: str) -> list[list[LineSegment]]:
    base_path = get_options().geojson_path
    full_path = base_path / (path + ".json")
    with full_path.open() as f:
        data = json.load(f)
    result: list[list[LineSegment]] = []
    for feature in data["features"]:
        coords_list = feature["geometry"]["coordinates"]
        for coords in coords_list:
            lines = []
            for i in range(len(coords) - 1):
                p1 = _make_point(coords[i])
                p2 = _make_point(coords[i + 1])
                lines.append(LineSegment.from_points(p1, p2))
            lines.append(
                LineSegment.from_points(_make_point(coords[-1]), _make_point(coords[0]))
            )
            result.append(lines)
    return result


def _make_point(coords: list[float]) -> Point:
    longitude, latitude = coords
    # sometimes the geojson data is wrapped around, e.g. for Greenland
    while longitude > 180:
        longitude -= 360
    return Point(longitude, latitude)


def is_in_polygon(p: Point, path: str) -> bool:
    polygons = get_polygon(path)
    for polygon in polygons:
        if is_in_polygon_single(p, polygon):
            return True
    return False


def is_in_polygon_single(p: Point, polygon: list[LineSegment]) -> bool:
    num_intersections = 0
    origin_to_point = LineSegment.from_points(_ORIGIN, p)
    for line in polygon:
        intersection = get_intersection(origin_to_point.line, line.line)
        if (
            intersection is not None
            and line.segment_contains_point_on_line(intersection)
            and origin_to_point.segment_contains_point_on_line(intersection)
        ):
            num_intersections += 1
    return num_intersections % 2 == 1


COUNTRY_RENAMES = {
    "republic_of_the_congo": "congo",
    "democratic_republic_of_the_congo": "democratic_congo",
    "cote_d'ivoire": "ivory_coast",
    "czech_republic": "czech",
    "guinea-bissau": "guinea_bissau",
    "swaziland": "eswatini",
    "united_states": "usa",
    "vatican_city": "vatican",
    "united_states_virgin_islands": "us_virgin_islands",
    "northern_marianas": "northern_mariana_islands",
    "bouvet": "bouvet_island",
    "faroe": "faroe_islands",
    "clipperton": "clipperton_island",
    "south_georgia_and_the_south_sandwich_islands": "south_georgia_and_south_sandwich_islands",
    # Dataset does not separate these
    "kosovo": "serbia",
    "taiwan": "china",
    "saint_helena": "saint_helena_ascension_and_tristan_da_cunha",
    "ascension": "saint_helena_ascension_and_tristan_da_cunha",
    "tristan_da_cunha": "saint_helena_ascension_and_tristan_da_cunha",
}
IGNORED_COUNTRIES = {
    "Antarctica",
    "Atlantic Ocean",
    "Pacific Ocean",
    "Indian Ocean",
    "Arctic Ocean",
    "Southern Ocean",
    "Mediterranean Sea",
}


@functools.lru_cache(maxsize=256)
def get_path(country_name: str) -> str | None:
    if country_name in IGNORED_COUNTRIES:
        return None
    base_path = get_options().geojson_path

    def path_exists(path: str) -> bool:
        return (base_path / (path + ".json")).exists()

    transformed_name = country_name.lower().replace(" ", "_")
    transformed_name = unidecode.unidecode(transformed_name)
    transformed_name = COUNTRY_RENAMES.get(transformed_name, transformed_name)
    if path_exists(f"countries/{transformed_name}"):
        return f"countries/{transformed_name}"

    for directory in (base_path / "areas").iterdir():
        if directory.is_dir():
            if path_exists(f"areas/{directory.name}/{transformed_name}"):
                return f"areas/{directory.name}/{transformed_name}"

    raise ValueError(f"Country {country_name!r} not found")
