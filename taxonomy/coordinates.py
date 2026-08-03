# static analysis: ignore[attribute_is_never_set]
import functools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

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
    full_path = _get_geojson_file(path)
    with full_path.open() as f:
        data = json.load(f)
    result: list[list[LineSegment]] = []
    for feature in data["features"]:
        for coords in _get_coordinate_rings(feature["geometry"]):
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


def _get_coordinate_rings(geometry: dict[str, Any]) -> list[list[list[float]]]:
    match geometry["type"]:
        case "Polygon":
            return geometry["coordinates"]
        case "MultiPolygon":
            return [ring for polygon in geometry["coordinates"] for ring in polygon]
        case _:
            raise ValueError(f"unsupported GeoJSON geometry type {geometry['type']!r}")


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


def is_in_bounding_box(p: Point, path: str) -> bool:
    for polygon in get_polygon(path):
        latitudes = [line.p1.latitude for line in polygon]
        longitudes = [line.p1.longitude for line in polygon]
        if min(latitudes) <= p.latitude <= max(latitudes) and min(
            longitudes
        ) <= p.longitude <= max(longitudes):
            return True
    return False


def get_distance_to_polygon(p: Point, path: str) -> float:
    distances: list[float] = []
    for polygon in get_polygon(path):
        if is_in_polygon_single(p, polygon):
            return 0
        distances.extend(get_distance_to_line_segment(p, line) for line in polygon)
    return min(distances)


def get_distance_to_line_segment(p: Point, line: LineSegment) -> float:
    x = p.longitude
    y = p.latitude
    x1 = line.p1.longitude
    y1 = line.p1.latitude
    x2 = line.p2.longitude
    y2 = line.p2.latitude
    dx = x2 - x1
    dy = y2 - y1
    if dx == 0 and dy == 0:
        return math.hypot(x - x1, y - y1)
    fraction = ((x - x1) * dx + (y - y1) * dy) / (dx * dx + dy * dy)
    fraction = max(0, min(1, fraction))
    projected_x = x1 + fraction * dx
    projected_y = y1 + fraction * dy
    return math.hypot(x - projected_x, y - projected_y)


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
    "south_georgia_and_the_south_sandwich_islands": (
        "south_georgia_and_south_sandwich_islands"
    ),
    # Dataset does not separate these
    "kosovo": "serbia",
    "taiwan": "china",
    "saint_helena": "saint_helena_ascension_and_tristan_da_cunha",
    "ascension": "saint_helena_ascension_and_tristan_da_cunha",
    "tristan_da_cunha": "saint_helena_ascension_and_tristan_da_cunha",
}
REGION_RENAMES = {
    "india": {
        "andaman_and_nicobar_islands": "andaman_nicobar_island",
        "dadra_and_nagar_haveli": "dadara_nagar_havelli",
        "daman_and_diu": "daman_diu",
        "delhi": "nct_of_delhi",
        "jammu_and_kashmir": "jammu_kashmir",
        "orissa": "odisha",
        "pondicherry": "puducherry",
        "punjab_(india)": "punjab",
    },
    "switzerland": {
        "appenzell_ausserrhoden": "appenzell-ausserrhoden",
        "appenzell_innerrhoden": "appenzell-innerrhoden",
        "bern": "berne",
        "graubunden": "grisons",
        "jura_(switzerland)": "jura",
        "luzern": "lucerne",
        "neuchatel": "neuchâtel",
        "st._gallen": "st-gallen",
    },
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


def _path_exists(path: str) -> bool:
    return _get_geojson_file(path).exists()


def _get_geojson_file(path: str) -> Path:
    for base_path in _get_geojson_roots():
        json_path = base_path / (path + ".json")
        if json_path.exists():
            return json_path
        geojson_path = base_path / (path + ".geojson")
        if geojson_path.exists():
            return geojson_path
    return _get_geojson_roots()[-1] / (path + ".geojson")


def _get_geojson_roots() -> list[Path]:
    options = get_options()
    paths = []
    generated_geojson_path = options.generated_geojson_path
    if generated_geojson_path != Path():
        paths.append(generated_geojson_path)
    paths.append(options.geojson_path)
    return paths


def _transform_name(name: str) -> str:
    transformed_name = name.lower().replace(" ", "_")
    transformed_name = unidecode.unidecode(transformed_name)
    return COUNTRY_RENAMES.get(transformed_name, transformed_name)


@functools.lru_cache(maxsize=256)
def get_path(country_name: str) -> str | None:
    if country_name in IGNORED_COUNTRIES:
        return None

    transformed_name = _transform_name(country_name)
    if _path_exists(f"countries/{transformed_name}"):
        return f"countries/{transformed_name}"

    for base_path in _get_geojson_roots():
        areas_path = base_path / "areas"
        if not areas_path.exists():
            continue
        for directory in areas_path.iterdir():
            if directory.is_dir():
                if _path_exists(f"areas/{directory.name}/{transformed_name}"):
                    return f"areas/{directory.name}/{transformed_name}"

    raise ValueError(f"Country {country_name!r} not found")


@functools.lru_cache(maxsize=1024)
def get_region_path(region_name: str, country_name: str) -> str | None:
    """Return a GeoJSON path for a subnational region, if available."""
    if country_name in IGNORED_COUNTRIES:
        return None
    country_name = _transform_name(country_name)
    region_name = _transform_name(region_name)
    region_name = REGION_RENAMES.get(country_name, {}).get(region_name, region_name)
    path = f"states/{country_name}/{region_name}"
    return path if _path_exists(path) else None
