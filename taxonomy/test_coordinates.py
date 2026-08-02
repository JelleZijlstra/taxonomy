import math
from pathlib import Path
from types import SimpleNamespace
from typing import Self, cast

import pytest

from taxonomy import coordinates
from taxonomy.apis import nominatim
from taxonomy.db import coordinate_lint
from taxonomy.db.constants import RegionKind
from taxonomy.db.models.region import Region


class FakeRegion:
    def __init__(self, name: str, parent: Self | None = None) -> None:
        self.name = name
        self.parent = parent
        self.children: list[Self] = []
        if parent is not None:
            parent.children.append(self)

    def parent_of_kind(self, kind: RegionKind) -> Self | None:
        assert kind is RegionKind.country
        region: Self | None = self
        while region is not None:
            if region.parent is None:
                return region
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


def test_check_point_in_region_reports_actual_subnational_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = FakeRegion("Country")
    expected = cast(Region, FakeRegion("Expected State", country))
    FakeRegion("Actual State", country)
    point = coordinates.Point(1, 1)

    def get_region_path(region_name: str, country_name: str) -> str | None:
        assert country_name == "Country"
        return {"Expected State": "expected", "Actual State": "actual"}.get(region_name)

    def is_in_polygon(point: coordinates.Point, path: str) -> bool:
        return path in {"country", "actual"}

    monkeypatch.setattr(coordinates, "get_path", lambda country_name: "country")
    monkeypatch.setattr(coordinates, "get_region_path", get_region_path)
    monkeypatch.setattr(coordinates, "is_in_polygon", is_in_polygon)

    assert list(coordinate_lint.check_point_in_region(point, expected)) == [
        "coordinates Point(longitude=1, latitude=1) are in Actual State, "
        "not Expected State"
    ]


def test_check_point_in_region_allows_french_overseas_country_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = cast(Region, FakeRegion("Guadeloupe"))
    point = coordinates.Point(-61.0, 16.3)
    monkeypatch.setattr(coordinates, "get_path", lambda country_name: None)
    monkeypatch.setattr(nominatim, "get_openstreetmap_country", lambda point: "France")

    assert list(coordinate_lint.check_point_in_region(point, country)) == []


def test_check_extent_in_region_allows_overlapping_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = FakeRegion("Country")
    expected = cast(Region, FakeRegion("Expected State", country))
    extent = coordinate_lint.make_extent("1°N-2°N", "1°E-2°E")
    assert extent is not None

    monkeypatch.setattr(coordinates, "get_path", lambda country_name: "country")
    monkeypatch.setattr(
        coordinates, "get_region_path", lambda region_name, country_name: "expected"
    )
    monkeypatch.setattr(
        coordinates,
        "is_in_polygon",
        lambda point, path: path == "expected" and point == coordinates.Point(2, 2),
    )

    assert list(coordinate_lint.check_extent_in_region(extent, expected)) == []


def test_check_extent_in_region_can_require_full_containment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = FakeRegion("Country")
    expected = cast(Region, FakeRegion("Expected State", country))
    extent = coordinate_lint.make_extent("1°N-2°N", "1°E-2°E")
    assert extent is not None

    monkeypatch.setattr(coordinates, "get_path", lambda country_name: "country")
    monkeypatch.setattr(
        coordinates, "get_region_path", lambda region_name, country_name: "expected"
    )
    monkeypatch.setattr(
        coordinates,
        "is_in_polygon",
        lambda point, path: path == "expected" and point == coordinates.Point(2, 2),
    )

    assert list(
        coordinate_lint.check_extent_in_region(
            extent, expected, require_full_containment=True
        )
    ) == [
        "coordinate extent CoordinateExtent(latitude=1°N-2°N, "
        "longitude=1°E-2°E) extends outside Expected State"
    ]


def test_check_point_in_region_allows_nearest_expected_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = FakeRegion("Country")
    expected = cast(Region, FakeRegion("Expected State", country))
    FakeRegion("Actual State", country)
    point = coordinates.Point(1, 1)

    def get_region_path(region_name: str, country_name: str) -> str | None:
        assert country_name == "Country"
        return {"Expected State": "expected", "Actual State": "actual"}.get(region_name)

    def is_in_polygon(point: coordinates.Point, path: str) -> bool:
        return path == "country"

    monkeypatch.setattr(coordinates, "get_path", lambda country_name: "country")
    monkeypatch.setattr(coordinates, "get_region_path", get_region_path)
    monkeypatch.setattr(coordinates, "is_in_polygon", is_in_polygon)
    monkeypatch.setattr(
        coordinates,
        "get_distance_to_polygon",
        lambda point, path: {"expected": 1.0, "actual": 10.0}[path],
    )

    assert list(coordinate_lint.check_point_in_region(point, expected)) == []


def test_check_point_in_region_reports_nearest_subnational_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = FakeRegion("Country")
    expected = cast(Region, FakeRegion("Expected State", country))
    FakeRegion("Actual State", country)
    point = coordinates.Point(1, 1)

    def get_region_path(region_name: str, country_name: str) -> str | None:
        assert country_name == "Country"
        return {"Expected State": "expected", "Actual State": "actual"}.get(region_name)

    def is_in_polygon(point: coordinates.Point, path: str) -> bool:
        return path == "country"

    monkeypatch.setattr(coordinates, "get_path", lambda country_name: "country")
    monkeypatch.setattr(coordinates, "get_region_path", get_region_path)
    monkeypatch.setattr(coordinates, "is_in_polygon", is_in_polygon)
    monkeypatch.setattr(
        coordinates,
        "get_distance_to_polygon",
        lambda point, path: {"expected": 10.0, "actual": 1.0}[path],
    )

    assert list(coordinate_lint.check_point_in_region(point, expected)) == [
        "coordinates Point(longitude=1, latitude=1) are closest to Actual State, "
        "not Expected State"
    ]
