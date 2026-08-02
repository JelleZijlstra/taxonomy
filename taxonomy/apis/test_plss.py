from __future__ import annotations

import json
from unittest.mock import Mock

import httpx
import pytest

from taxonomy import coordinates
from taxonomy.apis import plss
from taxonomy.db import coordinate_lint


@pytest.mark.parametrize(
    ("source", "canonical"),
    [
        ("Narrows, T27S, R31W, Sec. 3: 2 mi SW", "T27S R31W Sec. 3"),
        ("Township 27 South, Range 31 East, Section 3", "T27S R31E Sec. 3"),
        ("Sec. 4, T. 3 N., R. 4 W.", "T3N R4W Sec. 4"),
        ("T29S R33E Sec. 36 SW 1/4 of NE 1/4", "T29S R33E Sec. 36 SW¼NE¼"),
        ("T29S R33E SW 1/4 NE 1/4 Sec. 36", "T29S R33E Sec. 36 SW¼NE¼"),
        ("NE 1/4 of the W 1/2, Sec. 24, T. 1 S, R. 1 W", "T1S R1W Sec. 24 NE¼W½"),
        ("T32 1/2S R32.75E", "T32½S R32¾E"),
        (
            "T27S R31E Sec. 3, Willamette Meridian",
            "T27S R31E Sec. 3, Willamette Meridian",
        ),
    ],
)
def test_extract_plss(source: str, canonical: str) -> None:
    extracted = plss.extract_plss(source)

    assert len(extracted) == 1
    assert extracted[0].description.canonical_text == canonical


def test_parser_does_not_treat_bare_s_number_as_section() -> None:
    description = plss.extract_plss("High Rock Ranch, T34N R25E, S26")[0].description

    assert description.section is None


def test_parser_does_not_combine_aliquots_from_two_sections() -> None:
    description = plss.extract_plss(
        "S1/2 Sec. 13, N1/2 Sec. 24, T1N, R3W, Ute Principal Meridian"
    )[0].description

    assert description.canonical_text == "T1N R3W Sec. 24 N½, Ute Principal Meridian"


@pytest.mark.parametrize(
    "source",
    [
        "Sections 11 or 13, Township 13 S, Range 36 W",
        "Sections 11, 13, Township 13 S, Range 36 W",
        "T13S R36W Sec. 11 and 13",
        "T13S R36W Sections 11-13",
        "Sec. 14,15,22, and 23, T15N R34E",
    ],
)
def test_parser_marks_alternative_sections(source: str) -> None:
    extracted = plss.extract_plss(source)

    assert len(extracted) == 1
    assert extracted[0].has_alternative_section


@pytest.mark.parametrize(
    "source", ["T32S R33E Sec. 31, 7,300 ft", "T3S R8W Sec. 1, 25"]
)
def test_parser_does_not_treat_unlabeled_number_as_alternative_section(
    source: str,
) -> None:
    extracted = plss.extract_plss(source)

    assert len(extracted) == 1
    assert not extracted[0].has_alternative_section


def test_parse_canonical_requires_exact_spacing_and_punctuation() -> None:
    assert plss.parse_canonical("T27S R31E Sec. 3, Willamette Meridian") is not None
    assert plss.parse_canonical("T27 S, R31 E, Section 3") is None


@pytest.mark.parametrize(
    "plss_id", ["NE060190N0470W0", "OR330270S0310E0", "KS060130S0360W0"]
)
def test_valid_plss_id(plss_id: str) -> None:
    assert plss.is_valid_plss_id(plss_id)


@pytest.mark.parametrize("plss_id", ["bad", "OR33027S0310E0", "OR330270S0310E0-extra"])
def test_invalid_plss_id(plss_id: str) -> None:
    assert not plss.is_valid_plss_id(plss_id)


def test_meridian_comparison_ignores_baseline_wording() -> None:
    assert plss._normalize_meridian(
        "Mount Diablo Base Line and Meridian"
    ) == plss._normalize_meridian("Mount Diablo Meridian")


def test_aliquot_compatibility_compares_outer_subdivision() -> None:
    northeast = plss.PLSSDescription(
        33, "", "S", 28, "", "W", section=21, aliquot=("NE",)
    )
    northwest_of_northeast = plss.PLSSDescription(
        33, "", "S", 28, "", "W", section=21, aliquot=("NW", "NE")
    )
    southwest_of_northeast = plss.PLSSDescription(
        33, "", "S", 28, "", "W", section=21, aliquot=("SW", "NE")
    )
    northwest = plss.PLSSDescription(
        33, "", "S", 28, "", "W", section=21, aliquot=("NW",)
    )

    assert northeast.is_compatible_with(northwest_of_northeast)
    assert northwest_of_northeast.is_compatible_with(northeast)
    assert not northwest.is_compatible_with(northwest_of_northeast)
    assert not northwest_of_northeast.is_compatible_with(southwest_of_northeast)


def test_aliquot_compatibility_handles_halves_and_quarters() -> None:
    north_half_of_southwest = plss.PLSSDescription(
        27, "", "N", 24, "", "W", section=10, aliquot=("N", "SW")
    )
    northeast_of_southwest = plss.PLSSDescription(
        27, "", "N", 24, "", "W", section=10, aliquot=("NE", "SW")
    )
    southeast_of_southwest = plss.PLSSDescription(
        27, "", "N", 24, "", "W", section=10, aliquot=("SE", "SW")
    )

    assert north_half_of_southwest.is_compatible_with(northeast_of_southwest)
    assert northeast_of_southwest.is_compatible_with(north_half_of_southwest)
    assert not north_half_of_southwest.is_compatible_with(southeast_of_southwest)


def _polygon(west: float, south: float, east: float, north: float) -> dict[str, object]:
    return {
        "type": "Polygon",
        "coordinates": [
            [[west, south], [east, south], [east, north], [west, north], [west, south]]
        ],
    }


def test_get_townships_groups_multipart_plssid(monkeypatch: pytest.MonkeyPatch) -> None:
    description = plss.PLSSDescription(1, "", "N", 1, "", "E")
    response = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "STATEABBR": "CA",
                    "PRINMERCD": "21",
                    "PRINMER": "Mount Diablo Meridian",
                    "TWNSHPFRAC": "0",
                    "RANGEFRAC": "0",
                    "PLSSID": "CA210010N0010E0",
                },
                "geometry": _polygon(-122, 37, -121.9, 37.1),
            },
            {
                "type": "Feature",
                "properties": {
                    "STATEABBR": "CA",
                    "PRINMERCD": "21",
                    "PRINMER": "Mount Diablo Meridian",
                    "TWNSHPFRAC": "0",
                    "RANGEFRAC": "0",
                    "PLSSID": "CA210010N0010E0",
                },
                "geometry": _polygon(-121.9, 37, -121.8, 37.1),
            },
        ],
    }
    monkeypatch.setattr(plss, "_get_api_data", Mock(return_value=json.dumps(response)))

    townships = plss.get_townships(description, "CA")

    assert len(townships) == 1
    assert townships[0].plss_id == "CA210010N0010E0"
    assert townships[0].description.meridian == "Mount Diablo Meridian"
    assert len(townships[0].geometries) == 2


def test_get_townships_expands_numeric_meridian_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    description = plss.PLSSDescription(3, "", "S", 8, "", "W")
    response = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "STATEABBR": "MS",
                    "PRINMERCD": "25",
                    "PRINMER": "25",
                    "TWNSHPFRAC": "0",
                    "RANGEFRAC": "0",
                    "PLSSID": "MS250030S0080W0",
                },
                "geometry": _polygon(-88.84, 30.73, -88.73, 30.83),
            }
        ],
    }
    monkeypatch.setattr(plss, "_get_api_data", Mock(return_value=json.dumps(response)))

    townships = plss.get_townships(description, "MS")

    assert townships[0].meridian == "St. Stephens Meridian"


def _township(
    plss_id: str, meridian: str, geometry: dict[str, object]
) -> plss.Township:
    description = plss.PLSSDescription(1, "", "N", 1, "", "E")
    return plss.Township(
        state="CA",
        meridian=meridian,
        meridian_code=plss_id[2:4],
        plss_id=plss_id,
        description=description.with_meridian(meridian),
        geometries=(geometry,),
    )


def test_resolve_township_uses_county_geometry(monkeypatch: pytest.MonkeyPatch) -> None:
    humboldt = _township(
        "CA150010N0010E0", "Humboldt Meridian", _polygon(-124, 40, -123.9, 40.1)
    )
    mount_diablo = _township(
        "CA210010N0010E0", "Mount Diablo Meridian", _polygon(-122, 37, -121.9, 37.1)
    )
    monkeypatch.setattr(
        plss, "get_townships", Mock(return_value=(humboldt, mount_diablo))
    )
    monkeypatch.setattr(
        plss,
        "_get_county_geometries",
        Mock(return_value=(_polygon(-122.1, 36.9, -121.8, 37.2),)),
    )

    resolution = plss.resolve_township(
        mount_diablo.description,
        state_code="CA",
        state_fips="06",
        county_name="Example County, California",
    )

    assert resolution.township == mount_diablo
    assert resolution.problem is None


def test_resolve_township_does_not_assume_county_is_unique(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _township(
        "CA150010N0010E0", "Humboldt Meridian", _polygon(-124, 40, -123.9, 40.1)
    )
    second = _township(
        "CA210010N0010E0", "Mount Diablo Meridian", _polygon(-123.95, 40, -123.85, 40.1)
    )
    monkeypatch.setattr(plss, "get_townships", Mock(return_value=(first, second)))
    monkeypatch.setattr(
        plss,
        "_get_county_geometries",
        Mock(return_value=(_polygon(-124.1, 39.9, -123.8, 40.2),)),
    )

    resolution = plss.resolve_township(
        first.description,
        state_code="CA",
        state_fips="06",
        county_name="Example County",
    )

    assert resolution.township is None
    assert resolution.problem is not None
    assert "ambiguous" in resolution.problem


def test_resolve_township_uses_section_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _township(
        "OR330010N0010EA", "Willamette Meridian", _polygon(-124, 40, -123.9, 40.1)
    )
    second = _township(
        "OR330010N0010EB", "Willamette Meridian", _polygon(-124, 40, -123.9, 40.1)
    )
    description = plss.PLSSDescription(1, "", "N", 1, "", "E", section=3)
    monkeypatch.setattr(plss, "get_townships", Mock(return_value=(first, second)))
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        Mock(
            side_effect=(
                plss.ResolvedGeometry("section", ()),
                plss.ResolvedGeometry("section", (_polygon(-124, 40, -123.99, 40.01),)),
            )
        ),
    )

    resolution = plss.resolve_township(description, state_code="OR")

    assert resolution.township == second


def test_resolve_township_keeps_ambiguity_when_sections_are_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _township(
        "OR330010N0010EA", "Willamette Meridian", _polygon(-124, 40, -123.9, 40.1)
    )
    second = _township(
        "OR330010N0010EB", "Willamette Meridian", _polygon(-124, 40, -123.9, 40.1)
    )
    description = plss.PLSSDescription(1, "", "N", 1, "", "E", section=3)
    monkeypatch.setattr(plss, "get_townships", Mock(return_value=(first, second)))
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        Mock(return_value=plss.ResolvedGeometry("section", ())),
    )

    resolution = plss.resolve_township(description, state_code="OR")

    assert resolution.township is None
    assert resolution.problem is not None
    assert "ambiguous" in resolution.problem


def test_geometry_distance_and_bounds() -> None:
    geometry = _polygon(-122, 37, -121, 38)

    assert plss.geometry_distance_km(coordinates.Point(-121.5, 37.5), (geometry,)) == 0
    assert plss.geometry_distance_km(coordinates.Point(-120, 37.5), (geometry,)) > 80
    assert plss.geometry_bounds((geometry,)) == (37, 38, -122, -121)


def test_coordinate_extent_distance_uses_overlap() -> None:
    geometry = _polygon(-122, 37, -121, 38)
    overlapping = coordinate_lint.make_extent("36.9°N-37.1°N", "122.1°W-121.9°W")
    distant = coordinate_lint.make_extent("40°N-41°N", "122°W-121°W")
    assert overlapping is not None
    assert distant is not None

    assert plss.geometry_extent_distance_km(overlapping, (geometry,)) == 0
    assert plss.geometry_extent_distance_km(distant, (geometry,)) > 100


def test_section_lookup_zero_pads_ordinary_section_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    township = _township(
        "CA210010N0010E0", "Mount Diablo Meridian", _polygon(-122, 37, -121.9, 37.1)
    )
    description = township.description
    description = plss.PLSSDescription(
        description.township,
        description.township_fraction,
        description.township_direction,
        description.range,
        description.range_fraction,
        description.range_direction,
        section=3,
    )
    get_data = Mock(
        return_value=json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {},
                        "geometry": _polygon(-122, 37, -121.99, 37.01),
                    }
                ],
            }
        )
    )
    monkeypatch.setattr(plss, "_get_api_data", get_data)

    resolved = plss.get_description_geometry(township, description)

    assert len(resolved.geometries) == 1
    where = httpx.URL(get_data.call_args.args[0]).params["where"]
    assert where == "PLSSID='CA210010N0010E0' AND FRSTDIVNO IN ('03','3')"
