# static analysis: ignore[attribute_is_never_set]
import functools
import itertools
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import unidecode
from shapely import make_valid
from shapely.geometry import MultiPolygon as ShapelyMultiPolygon
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry.base import BaseGeometry

from taxonomy.config import get_options


@dataclass(frozen=True, slots=True)
class Point:
    longitude: float
    latitude: float

    @property
    def openstreetmap_url(self) -> str:
        return f"https://www.openstreetmap.org/?mlat={self.latitude}&mlon={self.longitude}&zoom=12"


@dataclass(frozen=True, slots=True)
class GeoPolygon:
    """A GeoJSON polygon, preserving interior rings as holes."""

    exterior: tuple[Point, ...]
    holes: tuple[tuple[Point, ...], ...] = ()


type GeoGeometry = tuple[GeoPolygon, ...]


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


@functools.lru_cache(maxsize=256)
def get_geojson_geometry(path: str) -> GeoGeometry:
    """Read Polygon/MultiPolygon features without flattening their holes."""
    full_path = _get_geojson_file(path)
    with full_path.open() as f:
        data = json.load(f)
    return tuple(
        polygon
        for feature in data["features"]
        for polygon in parse_geojson_geometry(feature["geometry"])
    )


def parse_geojson_geometry(geometry: dict[str, Any]) -> GeoGeometry:
    """Parse a GeoJSON Polygon or MultiPolygon into a hole-aware geometry."""
    try:
        geometry_type = geometry["type"]
        raw_coordinates = geometry["coordinates"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"invalid GeoJSON geometry {geometry!r}") from exc
    if geometry_type == "Polygon":
        raw_polygons = [raw_coordinates]
    elif geometry_type == "MultiPolygon":
        raw_polygons = raw_coordinates
    else:
        raise ValueError(f"unsupported GeoJSON geometry type {geometry_type!r}")
    try:
        polygons = []
        for raw_polygon in raw_polygons:
            rings = tuple(_parse_geojson_ring(ring) for ring in raw_polygon)
            if not rings:
                raise ValueError("polygon has no exterior ring")
            polygons.append(GeoPolygon(rings[0], rings[1:]))
        if not polygons:
            raise ValueError("geometry has no polygons")
        return tuple(polygons)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid GeoJSON geometry {geometry!r}") from exc


def _parse_geojson_ring(raw_ring: Any) -> tuple[Point, ...]:
    if not isinstance(raw_ring, list) or len(raw_ring) < 4:
        raise ValueError("ring has fewer than four positions")
    points = tuple(_make_point(position) for position in raw_ring)
    if points[0] != points[-1]:
        points += (points[0],)
    if len(set(points[:-1])) < 3:
        raise ValueError("ring has fewer than three distinct positions")
    return points


@functools.lru_cache(maxsize=256)
def get_polygon_bounding_boxes(
    path: str,
) -> tuple[tuple[float, float, float, float], ...]:
    return tuple(
        (
            min(line.p1.longitude for line in polygon),
            min(line.p1.latitude for line in polygon),
            max(line.p1.longitude for line in polygon),
            max(line.p1.latitude for line in polygon),
        )
        for polygon in get_polygon(path)
    )


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
    return is_in_geometry(p, get_geojson_geometry(path))


def is_in_bounding_box(p: Point, path: str) -> bool:
    for (
        minimum_longitude,
        minimum_latitude,
        maximum_longitude,
        maximum_latitude,
    ) in get_polygon_bounding_boxes(path):
        if (
            minimum_longitude <= p.longitude <= maximum_longitude
            and minimum_latitude <= p.latitude <= maximum_latitude
        ):
            return True
    return False


def get_distance_to_polygon(p: Point, path: str) -> float:
    distances: list[float] = []
    for polygon in get_polygon(path):
        if is_in_polygon_single(p, polygon):
            return 0
        distances.extend(get_distance_to_line_segment(p, line) for line in polygon)
    return min(distances)


def is_in_geometry(point: Point, geometry: GeoGeometry) -> bool:
    return any(_is_in_geo_polygon(point, polygon) for polygon in geometry)


def _is_in_geo_polygon(point: Point, polygon: GeoPolygon) -> bool:
    exterior = _point_in_ring(point, polygon.exterior)
    if exterior == 0:
        return False
    if exterior == 2:
        return True
    for hole in polygon.holes:
        in_hole = _point_in_ring(point, hole)
        if in_hole == 2:
            # Polygon boundaries are included in the represented Region.
            return True
        if in_hole == 1:
            return False
    return True


def _point_in_ring(point: Point, ring: tuple[Point, ...]) -> int:
    """Return 0 outside, 1 inside, or 2 on the ring boundary."""
    inside = False
    longitude = point.longitude
    latitude = point.latitude
    adjusted = _ring_near(ring, longitude)
    for first, second in itertools.pairwise(adjusted):
        if _point_on_segment(point, first, second):
            return 2
        if (first.latitude > latitude) == (second.latitude > latitude):
            continue
        crossing_longitude = first.longitude + (
            (latitude - first.latitude)
            * (second.longitude - first.longitude)
            / (second.latitude - first.latitude)
        )
        if longitude < crossing_longitude:
            inside = not inside
    return 1 if inside else 0


def _point_on_segment(point: Point, first: Point, second: Point) -> bool:
    cross = (point.latitude - first.latitude) * (second.longitude - first.longitude) - (
        point.longitude - first.longitude
    ) * (second.latitude - first.latitude)
    scale = max(
        1.0,
        abs(second.longitude - first.longitude),
        abs(second.latitude - first.latitude),
    )
    if abs(cross) > 1e-10 * scale:
        return False
    return (
        min(first.longitude, second.longitude) - 1e-10
        <= point.longitude
        <= max(first.longitude, second.longitude) + 1e-10
        and min(first.latitude, second.latitude) - 1e-10
        <= point.latitude
        <= max(first.latitude, second.latitude) + 1e-10
    )


def _longitude_near(longitude: float, reference: float) -> float:
    while longitude - reference > 180:
        longitude -= 360
    while longitude - reference < -180:
        longitude += 360
    return longitude


def _ring_near(ring: tuple[Point, ...], reference: float) -> tuple[Point, ...]:
    """Unwrap a ring continuously, then place it nearest a query longitude."""
    longitudes = [ring[0].longitude]
    for vertex in ring[1:]:
        longitudes.append(_longitude_near(vertex.longitude, longitudes[-1]))
    center = (min(longitudes) + max(longitudes)) / 2
    shift = 360 * round((reference - center) / 360)
    return tuple(
        Point(longitude + shift, vertex.latitude)
        for longitude, vertex in zip(longitudes, ring, strict=True)
    )


def distance_to_geometry_km(point: Point, geometry: GeoGeometry) -> float:
    """Return an approximate geodesic distance to a polygon boundary."""
    if is_in_geometry(point, geometry):
        return 0.0
    return min(
        _distance_to_ring_km(point, ring)
        for polygon in geometry
        for ring in (polygon.exterior, *polygon.holes)
    )


def _distance_to_ring_km(point: Point, ring: tuple[Point, ...]) -> float:
    adjusted = _ring_near(ring, point.longitude)
    return min(
        _distance_to_segment_km(point, first, second)
        for first, second in itertools.pairwise(adjusted)
    )


def _distance_to_segment_km(point: Point, first: Point, second: Point) -> float:
    # An equirectangular projection centered on the query point is sufficiently
    # accurate for the sub-kilometre Region-boundary tolerance and handles the
    # dateline.
    latitude_radians = math.radians(point.latitude)

    def project(vertex: Point) -> tuple[float, float]:
        return (
            math.radians(vertex.longitude - point.longitude)
            * math.cos(latitude_radians)
            * 6371.0088,
            math.radians(vertex.latitude - point.latitude) * 6371.0088,
        )

    x1, y1 = project(first)
    x2, y2 = project(second)
    dx = x2 - x1
    dy = y2 - y1
    if dx == 0 and dy == 0:
        return math.hypot(x1, y1)
    fraction = -(x1 * dx + y1 * dy) / (dx * dx + dy * dy)
    fraction = max(0.0, min(1.0, fraction))
    return math.hypot(x1 + fraction * dx, y1 + fraction * dy)


def geometry_is_within_geometry(
    inner: GeoGeometry, outer: GeoGeometry, *, tolerance_km: float = 0
) -> bool:
    """Return whether one geometry is covered by another after a metric buffer.

    Both geometries use the same local projection, preserving topology while the
    small boundary tolerance is expressed in kilometres.
    """
    if tolerance_km < 0:
        raise ValueError("tolerance_km must be nonnegative")
    reference = inner[0].exterior[0]
    inner_shape = _project_geometry_km(inner, reference)
    outer_shape = _project_geometry_km(outer, reference)
    if not inner_shape.is_valid:
        inner_shape = make_valid(inner_shape)
    if not outer_shape.is_valid:
        outer_shape = make_valid(outer_shape)
    if tolerance_km:
        outer_shape = outer_shape.buffer(tolerance_km)
    return outer_shape.covers(inner_shape)


def _project_geometry_km(geometry: GeoGeometry, reference: Point) -> BaseGeometry:
    latitude_radians = math.radians(reference.latitude)
    longitude_scale = math.cos(latitude_radians) * 6371.0088

    def project_ring(ring: tuple[Point, ...]) -> list[tuple[float, float]]:
        return [
            (
                math.radians(point.longitude - reference.longitude) * longitude_scale,
                math.radians(point.latitude - reference.latitude) * 6371.0088,
            )
            for point in _ring_near(ring, reference.longitude)
        ]

    polygons = [
        ShapelyPolygon(
            project_ring(polygon.exterior),
            [project_ring(hole) for hole in polygon.holes],
        )
        for polygon in geometry
    ]
    if len(polygons) == 1:
        return polygons[0]
    return ShapelyMultiPolygon(polygons)


def geometry_intersects_box(
    geometry: GeoGeometry,
    *,
    minimum_longitude: float,
    minimum_latitude: float,
    maximum_longitude: float,
    maximum_latitude: float,
) -> bool:
    corners = _box_corners(
        minimum_longitude, minimum_latitude, maximum_longitude, maximum_latitude
    )
    if any(is_in_geometry(corner, geometry) for corner in corners):
        return True
    for polygon in geometry:
        if any(
            _point_in_box(
                vertex,
                minimum_longitude,
                minimum_latitude,
                maximum_longitude,
                maximum_latitude,
            )
            for vertex in polygon.exterior
        ):
            return True
        if _ring_intersects_box(
            polygon.exterior,
            minimum_longitude,
            minimum_latitude,
            maximum_longitude,
            maximum_latitude,
        ):
            return True
    return False


def geometry_contains_box(
    geometry: GeoGeometry,
    *,
    minimum_longitude: float,
    minimum_latitude: float,
    maximum_longitude: float,
    maximum_latitude: float,
) -> bool:
    corners = _box_corners(
        minimum_longitude, minimum_latitude, maximum_longitude, maximum_latitude
    )
    if not all(is_in_geometry(corner, geometry) for corner in corners):
        return False
    if any(
        _ring_properly_intersects_box(
            polygon.exterior,
            minimum_longitude,
            minimum_latitude,
            maximum_longitude,
            maximum_latitude,
        )
        for polygon in geometry
    ):
        return False
    # Four inside corners are insufficient if the box crosses an enclave/hole.
    return not any(
        _ring_intersects_box(
            hole,
            minimum_longitude,
            minimum_latitude,
            maximum_longitude,
            maximum_latitude,
        )
        or any(
            _point_in_box(
                vertex,
                minimum_longitude,
                minimum_latitude,
                maximum_longitude,
                maximum_latitude,
            )
            for vertex in hole
        )
        for polygon in geometry
        for hole in polygon.holes
    )


def _box_corners(
    minimum_longitude: float,
    minimum_latitude: float,
    maximum_longitude: float,
    maximum_latitude: float,
) -> tuple[Point, Point, Point, Point]:
    return (
        Point(minimum_longitude, minimum_latitude),
        Point(maximum_longitude, minimum_latitude),
        Point(maximum_longitude, maximum_latitude),
        Point(minimum_longitude, maximum_latitude),
    )


def _point_in_box(
    point: Point,
    minimum_longitude: float,
    minimum_latitude: float,
    maximum_longitude: float,
    maximum_latitude: float,
) -> bool:
    return (
        minimum_longitude <= point.longitude <= maximum_longitude
        and minimum_latitude <= point.latitude <= maximum_latitude
    )


def _ring_intersects_box(
    ring: tuple[Point, ...],
    minimum_longitude: float,
    minimum_latitude: float,
    maximum_longitude: float,
    maximum_latitude: float,
) -> bool:
    box_edges = tuple(
        zip(
            _box_corners(
                minimum_longitude, minimum_latitude, maximum_longitude, maximum_latitude
            ),
            (
                Point(maximum_longitude, minimum_latitude),
                Point(maximum_longitude, maximum_latitude),
                Point(minimum_longitude, maximum_latitude),
                Point(minimum_longitude, minimum_latitude),
            ),
            strict=True,
        )
    )
    return any(
        _segments_intersect(first, second, box_first, box_second)
        for first, second in itertools.pairwise(ring)
        for box_first, box_second in box_edges
    )


def _ring_properly_intersects_box(
    ring: tuple[Point, ...],
    minimum_longitude: float,
    minimum_latitude: float,
    maximum_longitude: float,
    maximum_latitude: float,
) -> bool:
    corners = _box_corners(
        minimum_longitude, minimum_latitude, maximum_longitude, maximum_latitude
    )
    box_edges = tuple(
        zip(
            (corners[0], corners[1], corners[2], corners[3]),
            corners[1:] + corners[:1],
            strict=True,
        )
    )
    return any(
        _segments_properly_intersect(first, second, box_first, box_second)
        for first, second in itertools.pairwise(ring)
        for box_first, box_second in box_edges
    )


def _segments_properly_intersect(
    first: Point, second: Point, third: Point, fourth: Point
) -> bool:
    def orientation(a: Point, b: Point, c: Point) -> float:
        return (b.longitude - a.longitude) * (c.latitude - a.latitude) - (
            b.latitude - a.latitude
        ) * (c.longitude - a.longitude)

    return (
        orientation(first, second, third) * orientation(first, second, fourth) < 0
        and orientation(third, fourth, first) * orientation(third, fourth, second) < 0
    )


def _segments_intersect(
    first: Point, second: Point, third: Point, fourth: Point
) -> bool:
    def orientation(a: Point, b: Point, c: Point) -> float:
        return (b.longitude - a.longitude) * (c.latitude - a.latitude) - (
            b.latitude - a.latitude
        ) * (c.longitude - a.longitude)

    values = (
        orientation(first, second, third),
        orientation(first, second, fourth),
        orientation(third, fourth, first),
        orientation(third, fourth, second),
    )
    if values[0] * values[1] < 0 and values[2] * values[3] < 0:
        return True
    return (
        (abs(values[0]) <= 1e-10 and _point_on_segment(third, first, second))
        or (abs(values[1]) <= 1e-10 and _point_on_segment(fourth, first, second))
        or (abs(values[2]) <= 1e-10 and _point_on_segment(first, third, fourth))
        or (abs(values[3]) <= 1e-10 and _point_on_segment(second, third, fourth))
    )


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
    origin_line: Line | VerticalLine
    if p.longitude == 0:
        origin_line = VerticalLine(0)
    else:
        origin_line = Line(p.latitude / p.longitude, 0)
    ray_minimum_latitude = min(0, p.latitude)
    ray_maximum_latitude = max(0, p.latitude)
    ray_minimum_longitude = min(0, p.longitude)
    ray_maximum_longitude = max(0, p.longitude)
    for line in polygon:
        match origin_line, line.line:
            case Line() as ray, Line() as edge:
                if ray.a == edge.a:
                    continue
                longitude = (edge.b - ray.b) / (ray.a - edge.a)
                latitude = ray.a * longitude + ray.b
            case VerticalLine() as ray, Line() as edge:
                longitude = ray.x
                latitude = edge.a * ray.x + edge.b
            case Line() as ray, VerticalLine() as edge:
                longitude = edge.x
                latitude = ray.a * edge.x + ray.b
            case VerticalLine(), VerticalLine():
                continue
            case _:
                raise AssertionError("unexpected line types")
        if (
            min(line.p1.latitude, line.p2.latitude)
            <= latitude
            <= max(line.p1.latitude, line.p2.latitude)
            and min(line.p1.longitude, line.p2.longitude)
            <= longitude
            <= max(line.p1.longitude, line.p2.longitude)
            and ray_minimum_latitude <= latitude <= ray_maximum_latitude
            and ray_minimum_longitude <= longitude <= ray_maximum_longitude
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
