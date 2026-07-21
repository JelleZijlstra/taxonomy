from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, call

import pytest

from taxonomy import getinput
from taxonomy.apis import nominatim
from taxonomy.db import coordinate_lint
from taxonomy.db.constants import RegionKind
from taxonomy.db.models import lint as model_lint
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location, LocationStatus, LocationTag
from taxonomy.db.models.location import lint as location_lint
from taxonomy.db.models.name import TypeTag
from taxonomy.db.models.occurrence_record import OccurrenceRecordTag
from taxonomy.db.models.period import Period
from taxonomy.db.models.region import Region


def _mock_location_name_lookup(
    monkeypatch: pytest.MonkeyPatch, side_effect: list[object]
) -> Mock:
    get = Mock(side_effect=side_effect)
    filtered = SimpleNamespace(get=get)
    query = SimpleNamespace(filter=Mock(return_value=filtered))
    monkeypatch.setattr(Location, "select", Mock(return_value=query))
    return get


def test_create_interactively_recovers_from_duplicate_name_race(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    region = cast(Region, object())
    period = cast(Period, object())
    created = SimpleNamespace(fill_required_fields=Mock())
    make = Mock(side_effect=[sqlite3.IntegrityError, created])
    get_line = Mock(return_value="Walnut Creek, California")
    monkeypatch.setattr(Location, "make", make)
    monkeypatch.setattr(getinput, "get_line", get_line)
    _mock_location_name_lookup(
        monkeypatch, [Location.DoesNotExist, Location.DoesNotExist]
    )

    result = Location.create_interactively(
        name="Walnut Creek", region=region, period=period
    )

    assert result is created
    assert make.call_args_list == [
        call(name="Walnut Creek", region=region, period=period),
        call(name="Walnut Creek, California", region=region, period=period),
    ]
    assert (
        "Could not create Location 'Walnut Creek' because that name is now in use"
        in capsys.readouterr().out
    )


def test_open_coordinates_is_adt_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    source_callback = Mock()
    source = SimpleNamespace(
        get_shareable_adt_callbacks=Mock(
            return_value={"source_callback": source_callback}
        )
    )
    monkeypatch.setattr(Location, "source", source)
    loc = object.__new__(Location)

    callbacks = loc.get_adt_callbacks()

    assert callbacks["open_coordinates"] == loc.open_coordinates
    assert callbacks["infer_coordinates"] == loc.infer_coordinates
    assert callbacks["coordinate_evidence"] == loc.coordinate_evidence
    assert callbacks["source_callback"] is source_callback


def test_alias_requires_target_without_running_regular_lints() -> None:
    alias = cast(Location, SimpleNamespace(deleted=LocationStatus.alias, parent=None))

    assert list(Location.lint_invalid(alias, LintConfig())) == [
        "alias location has no parent"
    ]

    alias.parent = cast(Location, object())
    assert list(Location.lint_invalid(alias, LintConfig())) == []


def test_likely_synonymous_location_name_comparison() -> None:
    assert location_lint.are_likely_synonymous_names("Kalipoetjang", "Kaliputjang")
    assert location_lint.are_likely_synonymous_names(
        "Waikthlatingwaialwa", "Waikthlatingwayalwa"
    )
    assert location_lint.are_likely_synonymous_names("Ipanema", "Ypanema")
    assert location_lint.are_likely_synonymous_names("St. Louis", "Saint Louis")
    assert location_lint.are_likely_synonymous_names("Ste. Anne", "Sainte Anne")
    assert location_lint.are_likely_synonymous_names("Ft. Dauphin", "Fort Dauphin")
    assert location_lint.are_likely_synonymous_names("Mt. McKinley", "Mount McKinley")
    assert location_lint.are_likely_synonymous_names(
        "Mount McKinley", "McKinley Mountain"
    )
    assert location_lint.are_likely_synonymous_names("N Fork Creek", "North Fork Creek")
    assert location_lint.are_likely_synonymous_names("Smith Cr.", "Smith Creek")
    assert location_lint.are_likely_synonymous_names("Black R.", "Black River")
    assert location_lint.are_likely_synonymous_names("Lk. George", "Lake George")
    assert location_lint.are_likely_synonymous_names("Warm Spgs.", "Warm Springs")
    assert location_lint.are_likely_synonymous_names("Union Stn.", "Union Station")
    assert location_lint.are_likely_synonymous_names("Road Jct.", "Road Junction")
    assert location_lint.are_likely_synonymous_names("Main Rd.", "Main Road")
    assert location_lint.are_likely_synonymous_names("State Rte.", "State Route")
    assert location_lint.are_likely_synonymous_names("Park Ave.", "Park Avenue")
    assert location_lint.are_likely_synonymous_names(
        "Cozumel Island", "Isla de Cozumel"
    )
    assert location_lint.are_likely_synonymous_names("Cozumel", "Cozumel Island")
    assert location_lint.are_likely_synonymous_names("Río Negro", "Negro River")
    assert location_lint.are_likely_synonymous_names("Lago Victoria", "Lake Victoria")
    assert location_lint.are_likely_synonymous_names("Cabo Blanco", "Blanco Cape")
    assert location_lint.are_likely_synonymous_names("Bahía Honda", "Honda Bay")
    assert location_lint.are_likely_synonymous_names("Puerto Bello", "Bello Port")
    assert location_lint.are_likely_synonymous_names("Ciudad Juarez", "Juarez City")
    assert location_lint.are_likely_synonymous_names(
        "Trinidad Valley", "Valle de la Trinidad"
    )
    assert location_lint.are_likely_synonymous_names("Bosque Verde", "Verde Forest")
    assert location_lint.are_likely_synonymous_names("Cueva Negra", "Negra Cave")
    assert location_lint.are_likely_synonymous_names("Laguna Azul", "Azul Lagoon")
    assert location_lint.are_likely_synonymous_names(
        "Peninsula Blanca", "Blanca Peninsula"
    )
    assert location_lint.are_likely_synonymous_names("Catarata Alta", "Alta Falls")
    assert location_lint.are_likely_synonymous_names("Llano Grande", "Grande Plain")
    assert location_lint.are_likely_synonymous_names("Cerro Verde", "Verde Hill")
    assert location_lint.are_likely_synonymous_names("Les Beilleaux", "Beilleaux")
    assert location_lint.are_likely_synonymous_names("El Chico", "Chico")
    assert not location_lint.are_likely_synonymous_names(
        "Cozumel City", "Cozumel Island"
    )
    assert not location_lint.are_likely_synonymous_names("Victoria", "Lake Victoria")
    assert not location_lint.are_likely_synonymous_names("Negro Lake", "Negro River")
    assert not location_lint.are_likely_synonymous_names(
        "Trinidad Valley", "Trinidad Hill"
    )
    assert not location_lint.are_likely_synonymous_names("Trinidad", "Trinidad Valley")
    assert not location_lint.are_likely_synonymous_names("Lima", "Loma")
    assert not location_lint.are_likely_synonymous_names(
        "San Sebastian (27°10'36.16″S)", "San Sebastian (27°11'03.57″S)"
    )
    assert not location_lint.are_likely_synonymous_names(
        "Lossiemouth East Quarry", "Lossiemouth West Quarry"
    )
    assert not location_lint.are_likely_synonymous_names(
        "Fayum Quarry A", "Fayum Quarry B"
    )
    assert not location_lint.are_likely_synonymous_names(
        "Lissieu (Miocene)", "Lissieu (Eocene)"
    )
    assert not location_lint.are_likely_synonymous_names("Coldstream", "Goldstream")
    assert not location_lint.are_likely_synonymous_names(
        "Olduvai Bed I", "Olduvai B.K.II"
    )
    assert location_lint.are_likely_synonymous_names("Katschemak Bay", "Kachemak Bay")


def test_likely_synonym_map_flags_all_but_lowest_id_in_same_region() -> None:
    java = SimpleNamespace(id=1, name="Java")
    other_region = SimpleNamespace(id=2, name="Other")

    def make_location(
        location_id: int,
        name: str,
        *,
        region: SimpleNamespace = java,
        general: bool = False,
        period: object | None = None,
    ) -> Location:
        return cast(
            Location,
            SimpleNamespace(
                id=location_id,
                name=name,
                region=region,
                min_period=period,
                max_period=period,
                stratigraphic_unit=None,
                is_general=lambda: general,
            ),
        )

    lowest = make_location(10, "Kaliputjang")
    middle = make_location(20, "Kalipoetjang City")
    highest = make_location(30, "Kalipoetjang")
    different_region = make_location(40, "Kalipoetjang", region=other_region)
    different_period = make_location(50, "Kalipoetjang", period=object())
    general = make_location(5, "Kaliputjang Island", general=True)

    mapping = location_lint._build_likely_synonym_map(
        [highest, different_region, different_period, general, middle, lowest]
    )

    assert set(mapping) == {20, 30}
    assert mapping[20] == (lowest, (lowest, middle, highest))
    assert mapping[30] == (lowest, (lowest, middle, highest))


def test_coordinate_evidence_prints_all_sources(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9", "-122.1"),), name_tags=True)
    name.id = 101
    record = _tagged_object(
        (
            OccurrenceRecordTag.Coordinates("37.8", "-122.2"),
            OccurrenceRecordTag.VerbatimCoordinates("37.8, -122.2"),
            OccurrenceRecordTag.CoordinateUncertaintyFromSource("within 1 km"),
        ),
        name_tags=False,
    )
    record.id = 202
    loc = _location_without_coordinates(names=(name,), records=(record,))
    loc.latitude = "37.95°N"
    loc.longitude = "122.15°W"
    monkeypatch.setattr(
        nominatim,
        "search",
        Mock(
            return_value=[
                nominatim.SearchResult(
                    latitude="37.9",
                    longitude="-122.1",
                    name="Walnut Creek",
                    display_name="Walnut Creek, California, United States",
                    category="place",
                    feature_type="city",
                    address={
                        "city": "Walnut Creek",
                        "state": "California",
                        "country": "United States",
                        "country_code": "us",
                    },
                ),
                nominatim.SearchResult(
                    latitude="40",
                    longitude="-75",
                    name="Other place",
                    display_name="Other place, California, United States",
                    category="place",
                    feature_type="village",
                    address={
                        "village": "Other place",
                        "state": "California",
                        "country": "United States",
                        "country_code": "us",
                    },
                ),
            ]
        ),
    )

    Location.coordinate_evidence(loc)

    output = capsys.readouterr().out
    assert "Coordinate evidence for" in output
    assert "Location coordinates: 37.95°N, 122.15°W" in output
    assert "Nominatim candidates:" in output
    assert "Query: Walnut Creek, California" in output
    assert "[accepted] place/city: Walnut Creek" in output
    assert "[rejected by locality/region checks] place/village: Other place" in output
    assert "Type-locality Names:" in output
    assert "Coordinates tag: 37.9°N, 122.1°W" in output
    assert "OccurrenceRecords:" in output
    assert "Coordinates tag: 37.8°N, 122.2°W" in output
    assert "Verbatim coordinates: '37.8, -122.2' -> 37.8°N, 122.2°W" in output
    assert "Coordinate uncertainty: within 1 km" in output


def _tagged_object(tags: tuple[object, ...], *, name_tags: bool) -> SimpleNamespace:
    field = "type_tags" if name_tags else "tags"
    obj = SimpleNamespace(**{field: tags})
    obj.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    return obj


def _location_without_coordinates(
    *,
    name: str = "Walnut Creek",
    region_name: str = "California",
    region: Region | None = None,
    names: tuple[SimpleNamespace, ...] = (),
    records: tuple[SimpleNamespace, ...] = (),
) -> Location:
    if region is None:
        region = _make_region(region_name, RegionKind.state)
    loc = SimpleNamespace(
        name=name,
        region=region,
        latitude=None,
        longitude=None,
        stratigraphic_unit=None,
        type_localities=names,
        occurrence_records=records,
        tags=(),
        get_tags=lambda tags, tag_type: (
            tag for tag in tags if isinstance(tag, tag_type)
        ),
        is_invalid=lambda: False,
        is_general=lambda: name == region.name,
    )
    return cast(Location, loc)


def _make_region(name: str, kind: RegionKind, parent: Region | None = None) -> Region:
    region = SimpleNamespace(name=name, kind=kind, parent=parent)

    def all_parents() -> tuple[Region, ...]:
        parents = []
        current = parent
        while current is not None:
            parents.append(current)
            current = current.parent
        return tuple(parents)

    region.all_parents = all_parents
    return cast(Region, region)


def _marin_county() -> Region:
    country = _make_region("United States", RegionKind.country)
    state = _make_region("California", RegionKind.state, country)
    return _make_region("Marin County, California", RegionKind.county, state)


def _nominatim_result(
    *,
    latitude: str,
    longitude: str,
    category: str = "place",
    feature_type: str = "hamlet",
    county: str = "Marin County",
    display_name: str = "Nicasio, Marin County, California, United States",
) -> nominatim.SearchResult:
    return nominatim.SearchResult(
        latitude=latitude,
        longitude=longitude,
        name="Nicasio",
        display_name=display_name,
        category=category,
        feature_type=feature_type,
        address={
            feature_type: "Nicasio",
            "county": county,
            "state": "California",
            "country": "United States",
            "country_code": "us",
        },
    )


def test_location_infers_coordinates_from_name() -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9", "-122.1"),), name_tags=True)
    loc = _location_without_coordinates(names=(name,))

    assert (
        list(location_lint.check_linked_coordinates(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.latitude == "37.9°N"
    assert loc.longitude == "122.1°W"


def test_location_infers_coordinates_from_occurrence_record() -> None:
    record = _tagged_object(
        (OccurrenceRecordTag.Coordinates("37.9", "-122.1"),), name_tags=False
    )
    loc = _location_without_coordinates(records=(record,))

    messages = list(
        location_lint.check_linked_coordinates(loc, LintConfig(autofix=False))
    )

    assert len(messages) == 1
    assert "coordinates should be 37.9°N, 122.1°W" in messages[0]


def test_location_infers_coordinate_ranges_from_linked_evidence() -> None:
    name = _tagged_object(
        (TypeTag.Coordinates("37.8°N-38°N", "122.2°W-122°W"),), name_tags=True
    )
    loc = _location_without_coordinates(names=(name,))

    assert (
        list(location_lint.check_linked_coordinates(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.latitude == "37.8°N-38°N"
    assert loc.longitude == "122.2°W-122°W"


def test_location_inference_envelopes_compatible_evidence() -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True)
    record = _tagged_object(
        (OccurrenceRecordTag.Coordinates("37.91°N", "122.09°W"),), name_tags=False
    )
    loc = _location_without_coordinates(names=(name,), records=(record,))

    assert (
        list(location_lint.check_linked_coordinates(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.latitude == "37.9°N-37.91°N"
    assert loc.longitude == "122.1°W-122.09°W"


def test_location_does_not_infer_conflicting_coordinates() -> None:
    name = _tagged_object((TypeTag.Coordinates("40°N", "74°W"),), name_tags=True)
    record = _tagged_object(
        (OccurrenceRecordTag.Coordinates("41°N", "74°W"),), name_tags=False
    )
    loc = _location_without_coordinates(names=(name,), records=(record,))

    messages = list(
        location_lint.check_linked_coordinates(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "cannot infer coordinates" in messages[0]
    assert loc.latitude is None
    assert loc.longitude is None


def test_general_location_does_not_load_linked_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="California")
    search = Mock()
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert list(location_lint.check_linked_coordinates(loc, LintConfig())) == []
    assert list(location_lint.check_nominatim_coordinates(loc, LintConfig())) == []
    loc.latitude = "38°N"
    loc.longitude = "122°W"
    assert (
        list(location_lint.check_nominatim_coordinate_consistency(loc, LintConfig()))
        == []
    )
    search.assert_not_called()


def test_location_infers_coordinates_from_nominatim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    county = _marin_county()
    loc = _location_without_coordinates(name="Nicasio", region=county)
    boundary = _nominatim_result(
        latitude="38.0601015",
        longitude="-122.6984323",
        category="boundary",
        feature_type="census",
    )
    settlement = _nominatim_result(latitude="38.0615885", longitude="-122.6985975")
    search = Mock(return_value=[boundary, settlement])
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    messages = list(
        location_lint.check_nominatim_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude == "38.0615885°N"
    assert loc.longitude == "122.6985975°W"
    search.assert_called_once_with("Nicasio, Marin County, California, United States")


def test_location_prefers_place_over_administrative_boundaries(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    place = _nominatim_result(
        latitude="38.0615885",
        longitude="-122.6985975",
        feature_type="village",
        display_name="Nicasio village",
    )
    boundaries = [
        _nominatim_result(
            latitude="38.5",
            longitude="-122.7",
            category="boundary",
            feature_type="administrative",
            display_name="Nicasio administrative boundary 1",
        ),
        _nominatim_result(
            latitude="38.7",
            longitude="-122.9",
            category="boundary",
            feature_type="administrative",
            display_name="Nicasio administrative boundary 2",
        ),
    ]
    monkeypatch.setattr(nominatim, "search", Mock(return_value=[*boundaries, place]))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    messages = list(
        location_lint.check_nominatim_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude == "38.0615885°N"
    assert loc.longitude == "122.6985975°W"
    assert "preferred over boundary/administrative matches" in capsys.readouterr().out


def test_location_does_not_choose_between_multiple_place_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    results = [
        _nominatim_result(
            latitude="38.0615885",
            longitude="-122.6985975",
            feature_type="village",
            display_name="Nicasio village",
        ),
        _nominatim_result(
            latitude="38.5",
            longitude="-122.7",
            feature_type="city",
            display_name="Nicasio city",
        ),
        _nominatim_result(
            latitude="38.7",
            longitude="-122.9",
            category="boundary",
            feature_type="administrative",
            display_name="Nicasio administrative boundary",
        ),
    ]
    monkeypatch.setattr(nominatim, "search", Mock(return_value=results))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_nominatim_coordinates(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "returned 3 conflicting exact matches" in messages[0]
    assert all(result.display_name in messages[0] for result in results)
    assert loc.latitude is None
    assert loc.longitude is None


def test_location_coordinates_match_nearby_nominatim_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    loc.latitude = "38.0616°N"
    loc.longitude = "122.6986°W"
    nearby = _nominatim_result(
        latitude="38.0615885",
        longitude="-122.6985975",
        display_name="Nicasio settlement",
    )
    distant = _nominatim_result(
        latitude="38.5",
        longitude="-122.7",
        category="natural",
        feature_type="peak",
        display_name="Nicasio peak",
    )
    search = Mock(return_value=[distant, nearby])
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_nominatim_coordinate_consistency(loc, LintConfig())
    )

    assert messages == []
    search.assert_called_once_with("Nicasio, Marin County, California, United States")


def test_location_coordinates_are_far_from_all_nominatim_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    loc.latitude = "37.9°N"
    loc.longitude = "122.5°W"
    results = [
        _nominatim_result(
            latitude="38.0615885",
            longitude="-122.6985975",
            display_name="Nicasio settlement",
        ),
        _nominatim_result(
            latitude="38.0601015",
            longitude="-122.6984323",
            category="boundary",
            feature_type="census",
            display_name="Nicasio census area",
        ),
    ]
    monkeypatch.setattr(nominatim, "search", Mock(return_value=results))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_nominatim_coordinate_consistency(loc, LintConfig())
    )

    assert len(messages) == 1
    assert "more than 5 km from all 2 exact Nominatim matches" in messages[0]
    assert all(result.display_name in messages[0] for result in results)
    assert messages[0].count("km away") == 2


def test_nominatim_coordinate_consistency_is_a_normal_network_lint() -> None:
    wrapper = next(
        wrapper
        for wrapper in location_lint.LINT.linters
        if wrapper.label == "nominatim_coordinate_consistency"
    )

    assert wrapper.requires_network
    assert wrapper not in location_lint.LINT.disabled_linters


def test_location_rejects_nominatim_result_from_wrong_county(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    monkeypatch.setattr(
        nominatim,
        "search",
        Mock(
            return_value=[
                _nominatim_result(
                    latitude="38.0615885",
                    longitude="-122.6985975",
                    county="Sonoma County",
                )
            ]
        ),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_nominatim_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude is None
    assert loc.longitude is None


def test_location_lists_all_conflicting_nominatim_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    results = [
        _nominatim_result(
            latitude="38.0615885",
            longitude="-122.6985975",
            display_name="Nicasio settlement",
        ),
        _nominatim_result(
            latitude="38.0601015",
            longitude="-122.6984323",
            category="boundary",
            feature_type="census",
            display_name="Nicasio census area",
        ),
        _nominatim_result(
            latitude="38.5",
            longitude="-122.7",
            category="natural",
            feature_type="peak",
            display_name="Nicasio peak",
        ),
    ]
    monkeypatch.setattr(nominatim, "search", Mock(return_value=results))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_nominatim_coordinates(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "returned 3 conflicting exact matches" in messages[0]
    assert all(result.display_name in messages[0] for result in results)
    assert loc.latitude is None
    assert loc.longitude is None


def test_location_does_not_query_nominatim_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    search = Mock()
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: False)

    assert list(location_lint.check_nominatim_coordinates(loc, LintConfig())) == []
    loc.latitude = "38.0616°N"
    loc.longitude = "122.6986°W"
    assert (
        list(location_lint.check_nominatim_coordinate_consistency(loc, LintConfig()))
        == []
    )
    search.assert_not_called()


def test_location_lint_can_be_ignored() -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9", "-122.1"),), name_tags=True)
    loc = _location_without_coordinates(names=(name,))
    loc.tags = (LocationTag.IgnoreLintLocation("linked_coordinates"),)  # type: ignore[assignment]

    assert list(location_lint.check_linked_coordinates(loc, LintConfig())) == []
    assert loc.latitude is None
    assert loc.longitude is None


def test_remove_unused_location_ignore() -> None:
    loc = _location_without_coordinates()
    loc.tags = (LocationTag.IgnoreLintLocation("period"),)  # type: ignore[assignment]

    location_lint.remove_unused_ignores(loc, {"period"})

    assert loc.tags == []
