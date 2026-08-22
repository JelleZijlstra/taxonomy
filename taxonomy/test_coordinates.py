import math
from pathlib import Path
from types import SimpleNamespace
from typing import Self, cast
from unittest.mock import Mock

import pytest

from taxonomy import coordinates
from taxonomy.apis import nominatim
from taxonomy.db import coordinate_lint
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.region import Region, RegionTag


class FakeRegion:
    def __init__(
        self, name: str, parent: Self | None = None, *, tags: tuple[object, ...] = ()
    ) -> None:
        self.name = name
        self.parent = parent
        self.tags = tags
        self.children: list[Self] = []
        if parent is not None:
            parent.children.append(self)

    def parent_of_kind(self, kind: RegionKind) -> Self | None:
        assert kind is RegionKind.country
        region: FakeRegion | None = self
        while region is not None:
            if region.parent is None:
                return cast(Self, region)
            region = region.parent
        return None


def test_get_region_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    geojson_path = tmp_path / "geojson"
    (geojson_path / "states" / "usa").mkdir(parents=True)
    (geojson_path / "states" / "usa" / "new_york.json").write_text("{}")
    (geojson_path / "states" / "india").mkdir(parents=True)
    (geojson_path / "states" / "india" / "odisha.json").write_text("{}")
    (geojson_path / "states" / "india" / "nct_of_delhi.json").write_text("{}")
    (geojson_path / "states" / "switzerland").mkdir(parents=True)
    (geojson_path / "states" / "switzerland" / "grisons.geojson").write_text("{}")
    (geojson_path / "states" / "switzerland" / "neuchâtel.geojson").write_text("{}")
    generated_geojson_path = tmp_path / "generated_geojson"
    (generated_geojson_path / "states" / "germany").mkdir(parents=True)
    (generated_geojson_path / "states" / "germany" / "bavaria.json").write_text("{}")

    coordinates.get_region_path.cache_clear()
    monkeypatch.setattr(
        coordinates,
        "get_options",
        lambda: SimpleNamespace(
            geojson_path=geojson_path, generated_geojson_path=generated_geojson_path
        ),
    )

    assert coordinates.get_region_path("Bavaria", "Germany") == (
        "states/germany/bavaria"
    )
    assert coordinates.get_region_path("New York", "United States") == (
        "states/usa/new_york"
    )
    assert coordinates.get_region_path("Orissa", "India") == "states/india/odisha"
    assert coordinates.get_region_path("Delhi", "India") == "states/india/nct_of_delhi"
    assert coordinates.get_region_path("Graubünden", "Switzerland") == (
        "states/switzerland/grisons"
    )
    assert coordinates.get_region_path("Neuchâtel", "Switzerland") == (
        "states/switzerland/neuchâtel"
    )
    assert coordinates.get_region_path("Ontario", "United States") is None
    coordinates.get_region_path.cache_clear()


def test_make_point() -> None:
    assert coordinate_lint.make_point("40°30'N", "74°15'W") == coordinates.Point(
        -74.25, 40.5
    )
    assert coordinate_lint.make_point("40.5", "-74.25") == coordinates.Point(
        -74.25, 40.5
    )
    assert coordinate_lint.make_point("40 30 N", "74 15 W") == coordinates.Point(
        -74.25, 40.5
    )
    assert coordinate_lint.make_point("91", "-74.25") is None
    assert coordinate_lint.make_point("40°N-41°N", "74°W") is None


def test_standardize_coordinate() -> None:
    assert coordinate_lint.standardize_coordinate("40.5°N", is_latitude=True) == (
        "40.5°N",
        40.5,
    )
    assert coordinate_lint.standardize_coordinate("40.5", is_latitude=True) == (
        "40.5°N",
        40.5,
    )
    assert coordinate_lint.standardize_coordinate("-74.25", is_latitude=False) == (
        "74.25°W",
        -74.25,
    )
    assert coordinate_lint.standardize_coordinate("40 30 N", is_latitude=True) == (
        "40°30'N",
        40.5,
    )


@pytest.mark.parametrize(
    ("text", "axis", "expected_text", "minimum", "maximum"),
    [
        ("1°N-2°N", "latitude", "1°N-2°N", 1, 2),
        ("2°N to 1°N", "latitude", "1°N-2°N", 1, 2),
        ("-10.5 to -12", "latitude", "12°S-10.5°S", -12, -10.5),
        ("10.2167°E–10.2°E", "longitude", "10.2°E-10.2167°E", 10.2, 10.2167),
        ("-12--10.5", "latitude", "12°S-10.5°S", -12, -10.5),
    ],
)
def test_standardize_coordinate_interval(
    text: str, axis: str, expected_text: str, minimum: float, maximum: float
) -> None:
    standardized, interval = coordinate_lint.standardize_coordinate_interval(
        text, is_latitude=axis == "latitude"
    )

    assert standardized == expected_text
    assert interval.minimum == minimum
    assert interval.maximum == maximum
    assert repr(interval) == expected_text


def test_standardize_coordinate_pair_with_ranges() -> None:
    parsed = coordinate_lint.standardize_coordinate_pair("12°S-10.5°S", "40°E-41°E")

    assert parsed is not None
    latitude, longitude, extent = parsed
    assert latitude == "12°S-10.5°S"
    assert longitude == "40°E-41°E"
    assert extent.point is None
    assert extent.center == coordinates.Point(40.5, -11.25)


def test_standardize_coordinate_pair_with_prime_signs() -> None:
    parsed = coordinate_lint.standardize_coordinate_pair(
        "9°06'50.569″N–9°15'39.861″N", "92°42′49.618″E–92°50′10.881″E"
    )

    assert parsed is not None
    latitude, longitude, extent = parsed
    assert latitude == "9°6'50.569\"N-9°15'39.861\"N"
    assert longitude == "92°42'49.618\"E-92°50'10.881\"E"
    assert extent.latitude.minimum == pytest.approx(9.114046944444444)
    assert extent.latitude.maximum == pytest.approx(9.2610725)
    assert extent.longitude.minimum == pytest.approx(92.71378277777778)
    assert extent.longitude.maximum == pytest.approx(92.83635583333333)


def test_coordinate_extent_map_urls_mark_opposite_range_corners() -> None:
    extent = coordinate_lint.make_extent(
        "25.3329597°N-25.7789852°N", "79.3146515°W-79.1791883°W"
    )

    assert extent is not None
    prefix = (
        "https://www.openstreetmap.org/?minlon=-79.3146515&minlat=25.3329597"
        "&maxlon=-79.1791883&maxlat=25.7789852"
    )
    assert extent.openstreetmap_urls == (
        f"{prefix}&mlat=25.7789852&mlon=-79.3146515",
        f"{prefix}&mlat=25.3329597&mlon=-79.1791883",
    )
    assert extent.openstreetmap_url == extent.openstreetmap_urls[0]


def test_coordinate_extent_map_url_keeps_openstreetmap_for_point() -> None:
    extent = coordinate_lint.make_extent("25.5°N", "79.25°W")

    assert extent is not None
    assert (
        extent.openstreetmap_url
        == "https://www.openstreetmap.org/?mlat=25.5&mlon=-79.25&zoom=12"
    )
    assert extent.openstreetmap_urls == (extent.openstreetmap_url,)


def test_distance_km() -> None:
    first = coordinates.Point(-74.25, 40.5)
    nearby = coordinates.Point(-74.25, 40.5167)
    distant = coordinates.Point(-74.25, 41)

    assert coordinate_lint.distance_km(first, nearby) < 5
    assert math.isclose(coordinate_lint.distance_km(first, distant), 55.6, abs_tol=0.1)


def test_move_point() -> None:
    origin = coordinates.Point(0, 0)
    east = coordinate_lint.move_point(origin, 100, 90)
    north = coordinate_lint.move_point(origin, 100, 0)

    assert math.isclose(coordinate_lint.distance_km(origin, east), 100)
    assert math.isclose(east.latitude, 0, abs_tol=1e-10)
    assert east.longitude > 0
    assert math.isclose(coordinate_lint.distance_km(origin, north), 100)
    assert math.isclose(north.longitude, 0, abs_tol=1e-10)
    assert north.latitude > 0


def test_extent_distance_km() -> None:
    first = coordinate_lint.make_extent("40°N-41°N", "74°W-73°W")
    overlapping = coordinate_lint.make_extent("40.5°N", "73.5°W")
    distant = coordinate_lint.make_extent("42°N", "73.5°W")
    assert first is not None
    assert overlapping is not None
    assert distant is not None

    assert coordinate_lint.extent_distance_km(first, overlapping) == 0
    assert math.isclose(
        coordinate_lint.extent_distance_km(first, distant), 111.2, abs_tol=0.2
    )


def test_get_distance_to_line_segment() -> None:
    segment = coordinates.LineSegment.from_points(
        coordinates.Point(0, 0), coordinates.Point(2, 0)
    )
    assert (
        coordinates.get_distance_to_line_segment(coordinates.Point(1, 3), segment) == 3
    )
    assert math.isclose(
        coordinates.get_distance_to_line_segment(coordinates.Point(3, 4), segment),
        math.sqrt(17),
    )


def test_geojson_polygon_holes_and_multipolygons() -> None:
    geometry = coordinates.parse_geojson_geometry(
        {
            "type": "MultiPolygon",
            "coordinates": [
                [
                    [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
                    [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]],
                ],
                [[[20, 20], [22, 20], [22, 22], [20, 22], [20, 20]]],
            ],
        }
    )

    assert coordinates.is_in_geometry(coordinates.Point(2, 2), geometry)
    assert not coordinates.is_in_geometry(coordinates.Point(5, 5), geometry)
    assert coordinates.is_in_geometry(coordinates.Point(21, 21), geometry)
    assert not coordinates.geometry_contains_box(
        geometry,
        minimum_longitude=3,
        minimum_latitude=3,
        maximum_longitude=7,
        maximum_latitude=7,
    )


def test_geojson_polygon_crossing_antimeridian() -> None:
    geometry = coordinates.parse_geojson_geometry(
        {
            "type": "Polygon",
            "coordinates": [[[179, -1], [-179, -1], [-179, 1], [179, 1], [179, -1]]],
        }
    )

    assert coordinates.is_in_geometry(coordinates.Point(179.5, 0), geometry)
    assert coordinates.is_in_geometry(coordinates.Point(-179.5, 0), geometry)
    assert not coordinates.is_in_geometry(coordinates.Point(0, 0), geometry)


def test_geometry_is_within_geometry_uses_metric_tolerance() -> None:
    outer = coordinates.parse_geojson_geometry(
        {"type": "Polygon", "coordinates": [[[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]]]}
    )
    inside = coordinates.parse_geojson_geometry(
        {"type": "Polygon", "coordinates": [[[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]]}
    )
    near_boundary = coordinates.parse_geojson_geometry(
        {
            "type": "Polygon",
            "coordinates": [[[3, 1], [4.004, 1], [4.004, 2], [3, 2], [3, 1]]],
        }
    )
    outside = coordinates.parse_geojson_geometry(
        {"type": "Polygon", "coordinates": [[[5, 1], [6, 1], [6, 2], [5, 2], [5, 1]]]}
    )

    assert coordinates.geometry_is_within_geometry(inside, outer)
    assert not coordinates.geometry_is_within_geometry(near_boundary, outer)
    assert coordinates.geometry_is_within_geometry(
        near_boundary, outer, tolerance_km=0.5
    )
    assert not coordinates.geometry_is_within_geometry(outside, outer, tolerance_km=0.5)


def test_geometry_is_within_geometry_preserves_holes_and_antimeridian() -> None:
    outer_with_hole = coordinates.parse_geojson_geometry(
        {
            "type": "Polygon",
            "coordinates": [
                [[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]],
                [[1, 1], [3, 1], [3, 3], [1, 3], [1, 1]],
            ],
        }
    )
    inside_hole = coordinates.parse_geojson_geometry(
        {
            "type": "Polygon",
            "coordinates": [
                [[1.5, 1.5], [2.5, 1.5], [2.5, 2.5], [1.5, 2.5], [1.5, 1.5]]
            ],
        }
    )
    across_antimeridian = coordinates.parse_geojson_geometry(
        {
            "type": "Polygon",
            "coordinates": [[[179, -2], [-179, -2], [-179, 2], [179, 2], [179, -2]]],
        }
    )
    inside_across_antimeridian = coordinates.parse_geojson_geometry(
        {
            "type": "Polygon",
            "coordinates": [
                [[179.5, -1], [-179.5, -1], [-179.5, 1], [179.5, 1], [179.5, -1]]
            ],
        }
    )

    assert not coordinates.geometry_is_within_geometry(inside_hole, outer_with_hole)
    assert coordinates.geometry_is_within_geometry(
        inside_across_antimeridian, across_antimeridian
    )


def test_geometry_does_not_contain_box_spanning_disconnected_parts() -> None:
    geometry = coordinates.parse_geojson_geometry(
        {
            "type": "MultiPolygon",
            "coordinates": [
                [[[0, 0], [4, 0], [4, 10], [0, 10], [0, 0]]],
                [[[6, 0], [10, 0], [10, 10], [6, 10], [6, 0]]],
            ],
        }
    )

    assert not coordinates.geometry_contains_box(
        geometry,
        minimum_longitude=1,
        minimum_latitude=1,
        maximum_longitude=9,
        maximum_latitude=9,
    )


def _square_geometry() -> coordinates.GeoGeometry:
    return coordinates.parse_geojson_geometry(
        {"type": "Polygon", "coordinates": [[[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]]]}
    )


def _boundary_result(geometry: coordinates.GeoGeometry) -> nominatim.BoundaryResult:
    return nominatim.BoundaryResult(
        name="Expected State",
        display_name="Expected State, Country",
        category="boundary",
        feature_type="administrative",
        osm_type="relation",
        osm_id=123,
        names={},
        geometry=geometry,
    )


def test_check_point_in_linked_osm_region(monkeypatch: pytest.MonkeyPatch) -> None:
    country = FakeRegion("Country")
    expected = cast(
        Region,
        FakeRegion(
            "Expected State",
            country,
            tags=(RegionTag.OpenStreetMap("relation", 123, "boundary"),),
        ),
    )
    lookup = Mock(return_value=_boundary_result(_square_geometry()))
    monkeypatch.setattr(nominatim, "lookup_boundary", lookup)

    assert (
        list(
            coordinate_lint.check_point_in_region(
                coordinates.Point(2, 2), expected, allow_network=True
            )
        )
        == []
    )
    assert list(
        coordinate_lint.check_point_in_region(
            coordinates.Point(6, 2), expected, allow_network=True
        )
    ) == ["coordinates Point(longitude=6, latitude=2) are outside Expected State"]
    lookup.assert_called_with("relation", 123, allow_network=True)


def test_check_point_in_region_uses_cached_geometry_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = cast(
        Region,
        FakeRegion(
            "Expected State",
            tags=(RegionTag.OpenStreetMap("relation", 123, "boundary"),),
        ),
    )
    lookup = Mock(return_value=_boundary_result(_square_geometry()))
    monkeypatch.setattr(nominatim, "lookup_boundary", lookup)

    assert (
        list(
            coordinate_lint.check_point_in_region(
                coordinates.Point(2, 2), expected, allow_network=False
            )
        )
        == []
    )
    lookup.assert_called_once_with("relation", 123, allow_network=False)


def test_check_point_in_region_reports_point_just_outside_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = cast(
        Region,
        FakeRegion(
            "Expected State",
            tags=(RegionTag.OpenStreetMap("relation", 123, "boundary"),),
        ),
    )
    monkeypatch.setattr(
        nominatim,
        "lookup_boundary",
        Mock(return_value=_boundary_result(_square_geometry())),
    )

    assert list(
        coordinate_lint.check_point_in_region(coordinates.Point(4.02, 2), expected)
    ) == ["coordinates Point(longitude=4.02, latitude=2) are outside Expected State"]


def test_check_point_in_region_suppresses_sub_500m_boundary_difference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = cast(
        Region,
        FakeRegion(
            "Expected State",
            tags=(RegionTag.OpenStreetMap("relation", 123, "boundary"),),
        ),
    )
    monkeypatch.setattr(
        nominatim,
        "lookup_boundary",
        Mock(return_value=_boundary_result(_square_geometry())),
    )

    assert (
        list(
            coordinate_lint.check_point_in_region(coordinates.Point(4.004, 2), expected)
        )
        == []
    )


def test_check_extent_in_region_overlap_and_full_containment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = cast(
        Region,
        FakeRegion(
            "Expected State",
            tags=(RegionTag.OpenStreetMap("relation", 123, "boundary"),),
        ),
    )
    monkeypatch.setattr(
        nominatim,
        "lookup_boundary",
        Mock(return_value=_boundary_result(_square_geometry())),
    )
    extent = coordinate_lint.make_extent("3°N-5°N", "3°E-5°E")
    assert extent is not None

    assert list(coordinate_lint.check_extent_in_region(extent, expected)) == []
    assert list(
        coordinate_lint.check_extent_in_region(
            extent, expected, require_full_containment=True
        )
    ) == [
        (
            "coordinate extent CoordinateExtent(latitude=3°N-5°N, "
            "longitude=3°E-5°E) extends outside Expected State"
        )
    ]
