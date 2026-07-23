from __future__ import annotations

import json
import math
from collections.abc import Iterable, Iterator, Sequence
from html import escape
from pathlib import Path
from typing import NamedTuple, TypeAlias

from taxonomy.config import get_options

SVG_WIDTH = 1200
SVG_HEIGHT = 720
SVG_MARGIN = 28


class MapPoint(NamedTuple):
    latitude: float
    longitude: float
    label: str


RawCoordinate: TypeAlias = Sequence[float]
RawRing: TypeAlias = Sequence[RawCoordinate]
RawPolygon: TypeAlias = Sequence[RawRing]


class _Projection(NamedTuple):
    center_longitude: float
    center_latitude: float
    longitude_scale: float
    scale: float

    def project(self, latitude: float, longitude: float) -> tuple[float, float]:
        longitude = _unwrap_longitude(longitude, self.center_longitude)
        return self.project_unwrapped(latitude, longitude)

    def project_unwrapped(
        self, latitude: float, longitude: float
    ) -> tuple[float, float]:
        x = SVG_WIDTH / 2 + (
            (longitude - self.center_longitude) * self.longitude_scale * self.scale
        )
        y = SVG_HEIGHT / 2 - (latitude - self.center_latitude) * self.scale
        return x, y


def write_svg_map(
    points: Iterable[MapPoint],
    output_path: Path,
    *,
    title: str = "Coordinate map",
    base_map_paths: Iterable[Path] | None = None,
) -> Path:
    """Write an automatically framed SVG point map and return its path."""
    points = list(points)
    if not points:
        raise ValueError("cannot make a map without coordinates")
    projection = _make_projection(points)
    if base_map_paths is None:
        base_map_paths = get_base_map_paths()

    pieces = [
        '<?xml version="1.0" encoding="UTF-8"?>\n',
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {SVG_WIDTH} {SVG_HEIGHT}" ',
        f'role="img" aria-labelledby="map-title map-description">\n<title id="map-title">{escape(title)}</title>\n',
        '<desc id="map-description">Map of coordinate records. Hover over a point to show its label.</desc>\n',
        "<style>\n",
        ":root{color-scheme:light dark}svg{background:#dcebf2}"
        ".graticule{fill:none;stroke:#9db5c1;stroke-width:.6}"
        ".land{fill:#f4f0df;stroke:#788c91;stroke-width:.7;stroke-linejoin:round}"
        ".point-marker{fill:#c43d36;stroke:#fff;stroke-width:1.5;vector-effect:non-scaling-stroke}"
        "@media(prefers-color-scheme:dark){svg{background:#17242a}.graticule{stroke:#40545d}.land{fill:#354239;stroke:#71848a}.point-marker{stroke:#17242a}}\n",
        "</style>\n",
        f'<defs><clipPath id="map-clip"><rect x="{SVG_MARGIN}" y="{SVG_MARGIN}" width="{SVG_WIDTH - 2 * SVG_MARGIN}" height="{SVG_HEIGHT - 2 * SVG_MARGIN}"/></clipPath></defs>\n',
        '<g clip-path="url(#map-clip)">\n',
        _graticule_svg(projection),
        _base_map_svg(base_map_paths, projection),
        _points_svg(points, projection),
        "</g>\n",
        f'<rect x="{SVG_MARGIN}" y="{SVG_MARGIN}" width="{SVG_WIDTH - 2 * SVG_MARGIN}" height="{SVG_HEIGHT - 2 * SVG_MARGIN}" fill="none" stroke="#788c91" stroke-width="1"/>\n',
        "</svg>\n",
    ]
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("".join(pieces))
    return output_path


def get_base_map_paths() -> list[Path]:
    """Return configured country GeoJSON files, preferring generated overrides."""
    options = get_options()
    roots = [options.geojson_path]
    if options.generated_geojson_path != Path():
        roots.append(options.generated_geojson_path)
    by_name: dict[str, Path] = {}
    for root in roots:
        country_directory = root / "countries"
        if not country_directory.is_dir():
            continue
        for suffix in ("*.json", "*.geojson"):
            for path in country_directory.glob(suffix):
                by_name[path.stem] = path
    if not by_name:
        raise RuntimeError(
            "no country GeoJSON files found in the configured geojson_path"
        )
    return [by_name[name] for name in sorted(by_name)]


def _make_projection(points: Sequence[MapPoint]) -> _Projection:
    center_longitude, longitude_span = _longitude_center_and_span(
        [point.longitude for point in points]
    )
    minimum_latitude = min(point.latitude for point in points)
    maximum_latitude = max(point.latitude for point in points)
    center_latitude = (minimum_latitude + maximum_latitude) / 2
    latitude_span = maximum_latitude - minimum_latitude

    longitude_span = min(360.0, max(longitude_span * 1.24, 2.0))
    latitude_span = min(180.0, max(latitude_span * 1.24, 2.0))
    longitude_scale = max(0.2, math.cos(math.radians(center_latitude)))
    usable_width = SVG_WIDTH - 2 * SVG_MARGIN
    usable_height = SVG_HEIGHT - 2 * SVG_MARGIN
    scale = min(
        usable_width / (longitude_span * longitude_scale), usable_height / latitude_span
    )
    return _Projection(center_longitude, center_latitude, longitude_scale, scale)


def _longitude_center_and_span(longitudes: Sequence[float]) -> tuple[float, float]:
    normalized = sorted(longitude % 360 for longitude in longitudes)
    if len(normalized) == 1:
        center = normalized[0]
        return (center - 360 if center > 180 else center), 0
    gaps = [
        (normalized[(index + 1) % len(normalized)] - value) % 360
        for index, value in enumerate(normalized)
    ]
    gap_index = max(range(len(gaps)), key=gaps.__getitem__)
    start = normalized[(gap_index + 1) % len(normalized)]
    span = 360 - gaps[gap_index]
    center = start + span / 2
    while center > 180:
        center -= 360
    return center, span


def _unwrap_longitude(longitude: float, center: float) -> float:
    while longitude - center > 180:
        longitude -= 360
    while longitude - center < -180:
        longitude += 360
    return longitude


def _graticule_svg(projection: _Projection) -> str:
    pieces = ['<g class="graticule">\n']
    for longitude in range(-180, 181, 10):
        x1, y1 = projection.project(-90, longitude)
        x2, y2 = projection.project(90, longitude)
        if _line_may_be_visible(x1, y1, x2, y2):
            pieces.append(f'<path d="M{x1:.2f},{y1:.2f} L{x2:.2f},{y2:.2f}"/>\n')
    for latitude in range(-80, 81, 10):
        points = [
            projection.project(latitude, longitude) for longitude in range(-180, 181, 5)
        ]
        if _bounds_intersect_viewport(points):
            pieces.append(f'<path d="{_path_data(points)}"/>\n')
    pieces.append("</g>\n")
    return "".join(pieces)


def _line_may_be_visible(x1: float, y1: float, x2: float, y2: float) -> bool:
    return not (
        max(x1, x2) < SVG_MARGIN
        or min(x1, x2) > SVG_WIDTH - SVG_MARGIN
        or max(y1, y2) < SVG_MARGIN
        or min(y1, y2) > SVG_HEIGHT - SVG_MARGIN
    )


def _base_map_svg(paths: Iterable[Path], projection: _Projection) -> str:
    pieces = ['<g class="land">\n']
    for path in paths:
        data = json.loads(path.read_text())
        for polygon in _iter_polygons(data):
            path_data = _polygon_path_data(polygon, projection)
            if path_data:
                pieces.append(f'<path fill-rule="evenodd" d="{path_data}"/>\n')
    pieces.append("</g>\n")
    return "".join(pieces)


def _iter_polygons(data: object) -> Iterator[RawPolygon]:
    if not isinstance(data, dict):
        return
    data_type = data.get("type")
    if data_type == "FeatureCollection":
        for feature in data.get("features", []):
            yield from _iter_polygons(feature)
    elif data_type == "Feature":
        yield from _iter_polygons(data.get("geometry"))
    elif data_type == "Polygon":
        yield data["coordinates"]
    elif data_type == "MultiPolygon":
        yield from data["coordinates"]
    elif data_type == "GeometryCollection":
        for geometry in data.get("geometries", []):
            yield from _iter_polygons(geometry)


def _polygon_path_data(polygon: RawPolygon, projection: _Projection) -> str:
    if not polygon:
        return ""
    projected_rings: list[list[tuple[float, float]]] = []
    for ring in polygon:
        if len(ring) < 3:
            continue
        unwrapped = _unwrap_ring(ring, projection.center_longitude)
        projected = [
            projection.project_unwrapped(latitude, longitude)
            for longitude, latitude in unwrapped
        ]
        projected = _simplify_ring(projected, tolerance=0.55)
        if len(projected) >= 4:
            projected_rings.append(projected)
    if not projected_rings or not _bounds_intersect_viewport(projected_rings[0]):
        return ""
    return " ".join(_path_data(ring, close=True) for ring in projected_rings)


def _unwrap_ring(ring: RawRing, center: float) -> list[tuple[float, float]]:
    unwrapped: list[tuple[float, float]] = []
    previous = center
    for coordinate in ring:
        longitude, latitude = coordinate[:2]
        longitude = _unwrap_longitude(float(longitude), previous)
        unwrapped.append((longitude, float(latitude)))
        previous = longitude
    mean_longitude = sum(longitude for longitude, _ in unwrapped) / len(unwrapped)
    shift = round((center - mean_longitude) / 360) * 360
    return [(longitude + shift, latitude) for longitude, latitude in unwrapped]


def _simplify_ring(
    points: Sequence[tuple[float, float]], tolerance: float
) -> list[tuple[float, float]]:
    if points[0] == points[-1]:
        points = points[:-1]
    if len(points) < 4:
        return [*points, points[0]]
    simplified = _douglas_peucker(points, tolerance)
    return [*simplified, simplified[0]]


def _douglas_peucker(
    points: Sequence[tuple[float, float]], tolerance: float
) -> list[tuple[float, float]]:
    if len(points) <= 2:
        return list(points)
    start, end = points[0], points[-1]
    furthest_index = 0
    furthest_distance = 0.0
    for index, point in enumerate(points[1:-1], start=1):
        distance = _distance_to_segment(point, start, end)
        if distance > furthest_distance:
            furthest_distance = distance
            furthest_index = index
    if furthest_distance <= tolerance:
        return [start, end]
    left = _douglas_peucker(points[: furthest_index + 1], tolerance)
    right = _douglas_peucker(points[furthest_index:], tolerance)
    return [*left[:-1], *right]


def _distance_to_segment(
    point: tuple[float, float], start: tuple[float, float], end: tuple[float, float]
) -> float:
    px, py = point
    x1, y1 = start
    x2, y2 = end
    dx = x2 - x1
    dy = y2 - y1
    if dx == 0 and dy == 0:
        return math.hypot(px - x1, py - y1)
    fraction = max(
        0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy))
    )
    x = x1 + fraction * dx
    y = y1 + fraction * dy
    return math.hypot(px - x, py - y)


def _bounds_intersect_viewport(points: Sequence[tuple[float, float]]) -> bool:
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return not (
        max(xs) < SVG_MARGIN
        or min(xs) > SVG_WIDTH - SVG_MARGIN
        or max(ys) < SVG_MARGIN
        or min(ys) > SVG_HEIGHT - SVG_MARGIN
    )


def _path_data(points: Sequence[tuple[float, float]], *, close: bool = False) -> str:
    pieces = [f"M{points[0][0]:.2f},{points[0][1]:.2f}"]
    pieces.extend(f"L{x:.2f},{y:.2f}" for x, y in points[1:])
    if close:
        pieces.append("Z")
    return " ".join(pieces)


def _points_svg(points: Sequence[MapPoint], projection: _Projection) -> str:
    by_coordinate: dict[tuple[float, float], list[str]] = {}
    for point in points:
        by_coordinate.setdefault((point.latitude, point.longitude), []).append(
            point.label
        )
    pieces = ['<g class="points">\n']
    for (latitude, longitude), labels in sorted(by_coordinate.items()):
        x, y = projection.project(latitude, longitude)
        label = "; ".join(dict.fromkeys(labels))
        full_label = f"{label} ({latitude:.6g}, {longitude:.6g})"
        escaped_label = escape(full_label)
        pieces.extend(
            [
                f'<g class="point" aria-label="{escaped_label}">',
                f"<title>{escaped_label}</title>",
                f'<circle class="point-marker" cx="{x:.2f}" cy="{y:.2f}" r="5"/>',
                "</g>\n",
            ]
        )
    pieces.append("</g>\n")
    return "".join(pieces)
