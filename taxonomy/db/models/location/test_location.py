from __future__ import annotations

import math
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


def test_coordinate_collision_map_normalizes_equivalent_coordinates() -> None:
    recent = SimpleNamespace(name="Recent")
    california = SimpleNamespace(id=1, name="California")
    nevada = SimpleNamespace(id=2, name="Nevada")

    def make_location(
        location_id: int,
        name: str,
        latitude: str,
        longitude: str,
        region: SimpleNamespace,
    ) -> Location:
        return cast(
            Location,
            SimpleNamespace(
                id=location_id,
                name=name,
                latitude=latitude,
                longitude=longitude,
                region=region,
                min_period=recent,
                max_period=recent,
            ),
        )

    decimal = make_location(10, "Decimal", "40.5°N", "74.25°W", california)
    degrees_minutes = make_location(20, "Degrees minutes", "40°30'N", "74°15'W", nevada)
    different = make_location(30, "Different", "40.6°N", "74.25°W", california)

    mapping = location_lint._build_coordinate_collision_map(
        [decimal, different, degrees_minutes]
    )

    assert mapping == {10: (decimal, degrees_minutes), 20: (decimal, degrees_minutes)}


def test_coordinate_collision_reports_different_regions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    california = SimpleNamespace(id=1, name="California")
    nevada = SimpleNamespace(id=2, name="Nevada")
    first = cast(
        Location,
        SimpleNamespace(
            id=10,
            name="First",
            latitude="40.5°N",
            longitude="74.25°W",
            region=california,
            min_period=recent,
            max_period=recent,
            tags=(),
            get_tags=lambda tags, tag_type: (),
            reload=lambda: first,  # type: ignore[has-type]
            is_invalid=lambda: False,
        ),
    )
    second = cast(
        Location,
        SimpleNamespace(
            id=20,
            name="Second",
            latitude="40°30'N",
            longitude="74°15'W",
            region=nevada,
            min_period=recent,
            max_period=recent,
            tags=(),
            get_tags=lambda tags, tag_type: (),
            reload=lambda: second,  # type: ignore[has-type]
            is_invalid=lambda: False,
        ),
    )
    monkeypatch.setattr(
        location_lint,
        "_get_coordinate_collision_map",
        lambda: {10: (first, second), 20: (first, second)},
    )

    messages = list(location_lint.check_coordinate_collision(first, LintConfig()))

    assert len(messages) == 1
    assert "shared with Location(s) in different Regions" in messages[0]
    assert "20: 'Second' (Nevada)" in messages[0]


def test_coordinate_collision_rechecks_cached_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    region = SimpleNamespace(id=1, name="Solomon Islands")
    current = cast(
        Location,
        SimpleNamespace(
            id=10,
            name="Santa Cruz Islands",
            latitude="12.3°S-9.7°S",
            longitude="165.7°E-170.2°E",
            region=region,
            min_period=recent,
            max_period=recent,
            reload=lambda: current,  # type: ignore[has-type]
            is_invalid=lambda: False,
        ),
    )
    stale_current = cast(
        Location,
        SimpleNamespace(
            id=10,
            name="Santa Cruz Islands",
            latitude="10.7°S",
            longitude="165.8°E",
            region=region,
            min_period=recent,
            max_period=recent,
        ),
    )
    nendo = cast(
        Location,
        SimpleNamespace(
            id=20,
            name="Nendö",
            latitude="10.7°S",
            longitude="165.8°E",
            region=region,
            min_period=recent,
            max_period=recent,
            reload=lambda: nendo,  # type: ignore[has-type]
            is_invalid=lambda: False,
        ),
    )
    monkeypatch.setattr(
        location_lint,
        "_get_coordinate_collision_map",
        lambda: {10: (stale_current, nendo), 20: (stale_current, nendo)},
    )

    assert list(location_lint.check_coordinate_collision(current, LintConfig())) == []


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
    id: int = 1,
    name: str = "Walnut Creek",
    region_name: str = "California",
    region: Region | None = None,
    min_period: object | None = None,
    max_period: object | None = None,
    min_age: int | None = None,
    max_age: int | None = None,
    stratigraphic_unit: object | None = None,
    names: tuple[SimpleNamespace, ...] = (),
    records: tuple[SimpleNamespace, ...] = (),
    general: bool | None = None,
) -> Location:
    if region is None:
        region = _make_region(region_name, RegionKind.state)
    loc = SimpleNamespace(
        id=id,
        name=name,
        region=region,
        latitude=None,
        longitude=None,
        min_period=min_period,
        max_period=max_period,
        min_age=min_age,
        max_age=max_age,
        stratigraphic_unit=stratigraphic_unit,
        type_localities=names,
        occurrence_records=records,
        tags=(),
        get_tags=lambda tags, tag_type: (
            tag for tag in tags if isinstance(tag, tag_type)
        ),
        is_invalid=lambda: False,
        is_general=lambda: name == region.name if general is None else general,
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
    name: str = "Nicasio",
    category: str = "place",
    feature_type: str = "hamlet",
    county: str = "Marin County",
    state: str = "California",
    country: str = "United States",
    display_name: str = "Nicasio, Marin County, California, United States",
    osm_type: str | None = None,
    bounding_box: tuple[str, str, str, str] | None = None,
) -> nominatim.SearchResult:
    return nominatim.SearchResult(
        latitude=latitude,
        longitude=longitude,
        name=name,
        display_name=display_name,
        category=category,
        feature_type=feature_type,
        osm_type=osm_type,
        bounding_box=bounding_box,
        address={
            feature_type: name,
            "county": county,
            "state": state,
            "country": country,
            "country_code": "us",
        },
    )


def _nominatim_reverse_result(state: str) -> nominatim.ReverseResult:
    return nominatim.ReverseResult(
        display_name=f"Nearest feature, {state}, United States",
        address={"state": state, "country": "United States", "country_code": "us"},
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
    assert (
        list(location_lint.check_nominatim_general_coordinates(loc, LintConfig())) == []
    )
    loc.latitude = "38°N"
    loc.longitude = "122°W"
    assert (
        list(location_lint.check_nominatim_coordinate_consistency(loc, LintConfig()))
        == []
    )
    search.assert_not_called()


def test_general_location_rejects_point_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Island group", general=True)
    loc.latitude = "8°N"
    loc.longitude = "94°E"
    check_region = Mock(return_value=())
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", check_region)

    messages = list(location_lint.check_coordinates(loc, LintConfig()))

    assert len(messages) == 1
    assert "should use a coordinate range, not point coordinates" in messages[0]
    check_region.assert_not_called()


def test_general_location_allows_coordinate_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Island group", general=True)
    loc.latitude = "6.75°N-9.25°N"
    loc.longitude = "92.7°E-94°E"
    check_region = Mock(return_value=())
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", check_region)

    assert list(location_lint.check_coordinates(loc, LintConfig())) == []
    check_region.assert_called_once()


def test_general_location_infers_nominatim_bounding_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _make_region("India", RegionKind.country)
    state = _make_region("Andaman and Nicobar Islands", RegionKind.state, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Nicobar Islands",
        region=state,
        min_period=recent,
        max_period=recent,
        general=True,
    )
    result = _nominatim_result(
        latitude="7.0000167",
        longitude="93.8110528",
        name="Nicobar Islands",
        feature_type="archipelago",
        county="Great Nicobar",
        state="Andaman and Nicobar Islands",
        country="India",
        display_name="Nicobar Islands, Andaman and Nicobar Islands, India",
        osm_type="relation",
        bounding_box=("6.7562674", "9.2562168", "92.7186468", "93.9468357"),
    )
    search = Mock(return_value=[result])
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    messages = list(
        location_lint.check_nominatim_general_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude == "6.7562674°N-9.2562168°N"
    assert loc.longitude == "92.7186468°E-93.9468357°E"
    search.assert_called_once_with(
        "Nicobar Islands, Andaman and Nicobar Islands, India"
    )


def test_general_location_proposes_replacing_point_with_nominatim_bounding_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _make_region("India", RegionKind.country)
    state = _make_region("Andaman and Nicobar Islands", RegionKind.state, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Nicobar Islands",
        region=state,
        min_period=recent,
        max_period=recent,
        general=True,
    )
    loc.latitude = "7.0000167°N"
    loc.longitude = "93.8110528°E"
    result = _nominatim_result(
        latitude="7.0000167",
        longitude="93.8110528",
        name="Nicobar Islands",
        feature_type="archipelago",
        state="Andaman and Nicobar Islands",
        country="India",
        osm_type="relation",
        bounding_box=("6.7562674", "9.2562168", "92.7186468", "93.9468357"),
    )
    monkeypatch.setattr(nominatim, "search", Mock(return_value=[result]))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    messages = list(
        location_lint.check_nominatim_general_coordinates(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "coordinate range should be 6.7562674°N-9.2562168°N" in messages[0]
    assert "replace point coordinates 7.0000167°N, 93.8110528°E manually" in messages[0]
    assert loc.latitude == "7.0000167°N"
    assert loc.longitude == "93.8110528°E"


def test_general_location_does_not_use_nominatim_node_bounding_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _make_region("India", RegionKind.country)
    state = _make_region("Andaman and Nicobar Islands", RegionKind.state, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Nicobar Islands",
        region=state,
        min_period=recent,
        max_period=recent,
        general=True,
    )
    result = _nominatim_result(
        latitude="7",
        longitude="94",
        name="Nicobar Islands",
        feature_type="archipelago",
        state="Andaman and Nicobar Islands",
        country="India",
        osm_type="node",
        bounding_box=("6.75", "9.25", "92.7", "94"),
    )
    search = Mock(return_value=[result])
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert (
        list(location_lint.check_nominatim_general_coordinates(loc, LintConfig())) == []
    )
    assert loc.latitude is None
    assert loc.longitude is None


def test_nominatim_bounding_box_rejects_antimeridian_fallback() -> None:
    result = _nominatim_result(
        latitude="0",
        longitude="180",
        osm_type="relation",
        bounding_box=("-10", "10", "-180", "180"),
    )

    assert location_lint.get_nominatim_result_bounding_box(result) is None


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


def test_nominatim_search_removes_location_disambiguator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(
        name="Nicasio (Marin County, California)", region=_marin_county()
    )
    search = Mock(
        return_value=[
            _nominatim_result(latitude="38.0615885", longitude="-122.6985975")
        ]
    )
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


@pytest.mark.parametrize(
    ("locality_name", "region_name", "region_kind", "country_name", "query"),
    [
        (
            "Valdivia",
            "Antioquia Department",
            RegionKind.department,
            "Colombia",
            "Valdivia, Antioquia, Colombia",
        ),
        (
            "Chachapoyas",
            "Amazonas Department (Peru)",
            RegionKind.subnational,
            "Peru",
            "Chachapoyas, Amazonas, Peru",
        ),
        (
            "Lahore",
            "Punjab (Pakistan)",
            RegionKind.province,
            "Pakistan",
            "Lahore, Punjab, Pakistan",
        ),
        (
            "Tlalpan",
            "Distrito Federal (Mexico)",
            RegionKind.subnational,
            "Mexico",
            "Tlalpan, Mexico City, Mexico",
        ),
        (
            "Brasília",
            "Distrito Federal (Brazil)",
            RegionKind.subnational,
            "Brazil",
            "Brasília, Federal District, Brazil",
        ),
        (
            "El Pardo",
            "Madrid",
            RegionKind.subnational,
            "Spain",
            "El Pardo, Community of Madrid, Spain",
        ),
        (
            "Buin",
            "Bougainville Region",
            RegionKind.region,
            "Papua New Guinea",
            "Buin, Autonomous Region of Bougainville, Papua New Guinea",
        ),
    ],
)
def test_nominatim_query_uses_osm_region_names(
    locality_name: str,
    region_name: str,
    region_kind: RegionKind,
    country_name: str,
    query: str,
) -> None:
    country = _make_region(country_name, RegionKind.country)
    region = _make_region(region_name, region_kind, country)
    location = _location_without_coordinates(name=locality_name, region=region)

    assert location_lint.get_nominatim_query(location) == query


@pytest.mark.parametrize(
    ("region_name", "region_kind", "country_name", "osm_region_name"),
    [
        ("Madrid", RegionKind.subnational, "Spain", "Community of Madrid"),
        (
            "Bougainville Region",
            RegionKind.region,
            "Papua New Guinea",
            "Autonomous Region of Bougainville",
        ),
        ("Castellón", RegionKind.province, "Spain", "Castelló / Castellón"),
    ],
)
def test_forward_geocoding_accepts_osm_region_name_variants(
    region_name: str, region_kind: RegionKind, country_name: str, osm_region_name: str
) -> None:
    country = _make_region(country_name, RegionKind.country)
    region = _make_region(region_name, region_kind, country)
    location = _location_without_coordinates(name="Site", region=region)
    result = nominatim.SearchResult(
        latitude="10",
        longitude="10",
        name="Site",
        display_name=f"Site, {osm_region_name}, {country_name}",
        category="place",
        feature_type="village",
        address={"village": "Site", "state": osm_region_name, "country": country_name},
    )

    assert location_lint.is_sane_nominatim_result(location, result)


@pytest.mark.parametrize(
    ("name", "base_name", "offsets"),
    [
        ("8 mi E Monterey", "Monterey", ((8 * 1.609344, 90),)),
        ("Monterey: 8 mi E", "Monterey", ((8 * 1.609344, 90),)),
        ("1.5 km NW Monterey", "Monterey", ((1.5, 315),)),
        ("1 mi W 2 mi S Monterey", "Monterey", ((2 * 1.609344, 180), (1.609344, 270))),
        ("3 mi E of Boise", "Boise", ((3 * 1.609344, 90),)),
        ("10 miles south of Coy Inlet", "Coy Inlet", ((10 * 1.609344, 180),)),
        (
            "25 miles northeast of Gur Tung Khara Usu",
            "Gur Tung Khara Usu",
            ((25 * 1.609344, 45),),
        ),
        ("Castle Brace (2 mi. SW)", "Castle Brace", ((2 * 1.609344, 225),)),
        (
            "Azua (8.9 mi N, 1.8 mi W)",
            "Azua",
            ((8.9 * 1.609344, 0), (1.8 * 1.609344, 270)),
        ),
        ("Point Lookout (1/4 mi. NE)", "Point Lookout", ((0.25 * 1.609344, 45),)),
    ],
)
def test_parse_nominatim_locality_offsets(
    name: str, base_name: str, offsets: tuple[tuple[float, float], ...]
) -> None:
    loc = _location_without_coordinates(name=name)

    plan = location_lint.get_nominatim_search_plan(loc)

    assert plan.locality_name == base_name
    assert (
        tuple((offset.distance_km, offset.bearing_degrees) for offset in plan.offsets)
        == offsets
    )


@pytest.mark.parametrize("name", ["15 Mile Creek", "3.7 km from Sina"])
def test_nominatim_offset_parser_leaves_unsupported_names_alone(name: str) -> None:
    loc = _location_without_coordinates(name=name)

    plan = location_lint.get_nominatim_search_plan(loc)

    assert plan.locality_name == name
    assert plan.offsets == ()


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("3 mi E of Boise", "Boise: 3 mi E"),
        ("10 miles south of Coy Inlet", "Coy Inlet: 10 mi S"),
        ("1 kilometer NW Monterey", "Monterey: 1 km NW"),
        ("1 mi W 2 mi S Monterey", "Monterey: 2 mi S 1 mi W"),
        ("1 mi W, 2 km S of Monterey", "Monterey: 2 km S 1 mi W"),
        (
            "8 miles east of Monterey (Monterey County, California)",
            "Monterey (Monterey County, California): 8 mi E",
        ),
        ("Castle Brace (2 mi. SW)", "Castle Brace: 2 mi SW"),
        ("Azua (8.9 mi N, 1.8 mi W)", "Azua: 8.9 mi N 1.8 mi W"),
        ("Point Lookout (1/4 mi. NE)", "Point Lookout: 1/4 mi NE"),
    ],
)
def test_location_offset_name_lint(name: str, expected: str) -> None:
    loc = _location_without_coordinates(name=name)

    messages = list(location_lint.check_offset_name(loc, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert f"distance-offset name should be {expected!r}" in messages[0]


def test_location_offset_name_lint_accepts_canonical_name() -> None:
    loc = _location_without_coordinates(name="Monterey: 2 mi S 1 mi W")

    assert list(location_lint.check_offset_name(loc, LintConfig())) == []


def test_location_name_lint_proposes_canonical_spacing() -> None:
    loc = _location_without_coordinates(name="Foo River (California) : mouth")

    messages = list(location_lint.check_location_name(loc, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "location name should be 'Foo River (California): mouth'" in messages[0]


def test_location_name_lint_does_not_autofix_canonical_spacing() -> None:
    loc = _location_without_coordinates(name="Foo River (California) : mouth")

    messages = list(location_lint.check_location_name(loc, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "location name should be 'Foo River (California): mouth'" in messages[0]
    assert loc.name == "Foo River (California) : mouth"


def test_location_name_lint_rejects_multiple_disambiguators() -> None:
    loc = _location_without_coordinates(name="Site (Recent) (Milne Bay Province)")

    messages = list(location_lint.check_location_name(loc, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "may contain only one parenthetical disambiguator" in messages[0]


def test_location_offset_name_lint_autofixes_available_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Castle Brace (2 mi. SW)")
    monkeypatch.setattr(location_lint, "_is_location_name_taken", lambda name: False)

    messages = list(location_lint.check_offset_name(loc, LintConfig(autofix=True)))

    assert messages == []
    assert loc.name == "Castle Brace: 2 mi SW"


def test_location_offset_name_lint_does_not_autofix_name_collision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Castle Brace (2 mi. SW)")
    monkeypatch.setattr(location_lint, "_is_location_name_taken", lambda name: True)

    messages = list(location_lint.check_offset_name(loc, LintConfig(autofix=True)))

    assert loc.name == "Castle Brace (2 mi. SW)"
    assert len(messages) == 1
    assert (
        "distance-offset name should be 'Castle Brace: 2 mi SW'; "
        "cannot autofix because that name is already in use" in messages[0]
    )


def test_location_offset_name_lint_respects_ignore_lint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Castle Brace (2 mi. SW)")
    name_taken = Mock()
    monkeypatch.setattr(location_lint, "_is_location_name_taken", name_taken)
    monkeypatch.setattr(
        location_lint.LINT, "is_ignoring_lint", lambda location, label: True
    )

    messages = list(location_lint.check_offset_name(loc, LintConfig(autofix=True)))

    assert loc.name == "Castle Brace (2 mi. SW)"
    assert len(messages) == 1
    name_taken.assert_not_called()


def _period(
    name: str,
    *,
    oldest_age: int = 10_000_000,
    youngest_age: int = 1_000_000,
    parent: object | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        parent=parent,
        get_max_age=lambda: oldest_age,
        get_min_age=lambda: youngest_age,
    )


@pytest.mark.parametrize(
    "name",
    [
        "Site",
        "Site (Milne Bay Province)",
        "Site (Papua New Guinea)",
        "Site (Recent)",
        "Saint Martin (island)",
        "Guinea (region)",
        "Guinea (Region)",
        "HGSP 81-07(a)",
    ],
)
def test_location_disambiguator_lint_accepts_sane_names(name: str) -> None:
    country = _make_region("Papua New Guinea", RegionKind.country)
    province = _make_region("Milne Bay Province", RegionKind.province, country)
    recent = _period("Recent")
    loc = _location_without_coordinates(
        name=name, region=province, min_period=recent, max_period=recent
    )

    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


def test_location_disambiguator_lint_accepts_nested_region_name() -> None:
    country = _make_region("Netherlands", RegionKind.country)
    province = _make_region("Limburg (Netherlands)", RegionKind.province, country)
    for name in [
        "Maastricht Formation (Limburg (Netherlands))",
        "Maastricht Formation (Limburg, Netherlands)",
        "Maastricht Formation (Limburg)",
    ]:
        loc = _location_without_coordinates(name=name, region=province)

        assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


def test_location_disambiguator_lint_accepts_comma_qualified_region_name() -> None:
    country = _make_region("United States", RegionKind.country)
    state = _make_region("South Carolina", RegionKind.state, country)
    county = _make_region("Charleston County, South Carolina", RegionKind.county, state)
    loc = _location_without_coordinates(
        name="North Charleston (Charleston County)", region=county
    )

    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


def test_location_disambiguator_lint_accepts_containing_period(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assigned = _period("MN7-8", oldest_age=13_820_000, youngest_age=11_630_000)
    miocene = _period("Miocene", oldest_age=23_030_000, youngest_age=5_333_000)
    monkeypatch.setattr(
        location_lint, "_get_periods_by_name", lambda: {"Miocene": (miocene,)}
    )
    loc = _location_without_coordinates(
        name="Steinheim (Miocene)", min_period=assigned, max_period=assigned
    )

    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


def test_location_disambiguator_lint_accepts_parent_period() -> None:
    barstovian = _period("Barstovian", oldest_age=15_900_000, youngest_age=11_500_000)
    ba1 = _period(
        "Ba1", oldest_age=15_970_000, youngest_age=13_820_000, parent=barstovian
    )
    loc = _location_without_coordinates(
        name="Eastgate (Barstovian)", min_period=ba1, max_period=ba1
    )

    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


@pytest.mark.parametrize(
    "name",
    [
        "Site (Las Flores Formation (Chubut))",
        "Site (Las Flores Formation, Chubut)",
        "Site (Las Flores Formation)",
    ],
)
def test_location_disambiguator_lint_accepts_assigned_stratigraphic_unit(
    name: str,
) -> None:
    formation = SimpleNamespace(name="Las Flores Formation (Chubut)", parent=None)
    loc = _location_without_coordinates(name=name, stratigraphic_unit=formation)

    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


@pytest.mark.parametrize(
    ("name", "disambiguator"), [("Top Camp (Goodenough Island)", "Goodenough Island")]
)
def test_location_disambiguator_lint_rejects_other_qualifiers(
    name: str, disambiguator: str
) -> None:
    loc = _location_without_coordinates(name=name)

    messages = list(location_lint.check_disambiguator(loc, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert (
        f"disambiguator {disambiguator!r} is not an enclosing Region, "
        "an assigned Period, or an assigned StratigraphicUnit" in messages[0]
    )
    assert f"location name should be {f'Top Camp: {disambiguator}'!r}" in messages[0]


def test_location_disambiguator_lint_accepts_modifier() -> None:
    loc = _location_without_coordinates(name="Lomas Cantadas: upper")

    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


def test_location_disambiguator_lint_does_not_autofix_modifier() -> None:
    loc = _location_without_coordinates(name="Lomas Cantadas (upper)")

    messages = list(location_lint.check_disambiguator(loc, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "location name should be 'Lomas Cantadas: upper'" in messages[0]
    assert loc.name == "Lomas Cantadas (upper)"


def test_coordinate_modifier_lint_normalizes_parenthetical_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(
        name="Vilacota, Tacna, Peru (-17.145759, -70.054278)"
    )
    loc.latitude = "17.145759°S"
    loc.longitude = "70.054278°W"
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )
    monkeypatch.setattr(location_lint, "_is_location_name_taken", lambda name: False)

    messages = list(
        location_lint.check_coordinate_modifier(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.name == "Vilacota, Tacna, Peru: 17.145759°S 70.054278°W"


def test_coordinate_modifier_lint_preserves_geographic_disambiguator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Vilacota (Peru) (-17.145759, -70.054278)")
    loc.latitude = "-17.145759"
    loc.longitude = "-70.054278"
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )
    monkeypatch.setattr(location_lint, "_is_location_name_taken", lambda name: False)

    messages = list(
        location_lint.check_coordinate_modifier(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.name == "Vilacota (Peru): 17.145759°S 70.054278°W"
    assert list(location_lint.check_location_name(loc, LintConfig())) == []


def test_coordinate_modifier_lint_detects_location_coordinate_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(
        name="Vilacota, Tacna, Peru: 17.145759°S 70.054278°W"
    )
    loc.latitude = "17.122149°S"
    loc.longitude = "70.063451°W"
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    messages = list(location_lint.check_coordinate_modifier(loc, LintConfig()))

    assert len(messages) == 1
    assert "does not match Location coordinates" in messages[0]
    assert "km apart" in messages[0]


def test_coordinate_modifier_lint_allows_equivalent_coordinate_notation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Site: 17°30'S 70°W")
    loc.latitude = "17.5°S"
    loc.longitude = "-70"
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    assert list(location_lint.check_coordinate_modifier(loc, LintConfig())) == []


def test_coordinate_modifier_lint_requires_location_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(
        name="Vilacota, Tacna, Peru: 17.145759°S 70.054278°W"
    )
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    messages = list(location_lint.check_coordinate_modifier(loc, LintConfig()))

    assert len(messages) == 1
    assert "Location coordinates are missing or incomplete" in messages[0]


def test_coordinate_modifier_lint_rejects_impossible_coordinates() -> None:
    loc = _location_without_coordinates(name="Site (-117.1, -270.2)")

    messages = list(location_lint.check_coordinate_modifier(loc, LintConfig()))

    assert len(messages) == 1
    assert "invalid coordinate modifier '-117.1, -270.2'" in messages[0]
    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


def test_coordinate_modifier_lint_checks_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Site: 17°S 70°W")
    loc.latitude = "17°S"
    loc.longitude = "70°W"
    monkeypatch.setattr(
        coordinate_lint,
        "check_extent_in_region",
        lambda extent, region: ("coordinate extent is outside Test Region",),
    )

    messages = list(location_lint.check_coordinate_modifier(loc, LintConfig()))

    assert len(messages) == 1
    assert (
        "coordinate modifier 17°S 70°W: "
        "coordinate extent is outside Test Region" in messages[0]
    )


def test_location_modifier_does_not_trigger_coordinate_inference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Foo River (California): mouth")
    search = Mock()
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    plan = location_lint.get_nominatim_search_plan(loc)

    assert plan.locality_name == "Foo River"
    assert plan.modifier == "mouth"
    assert not plan.coordinates_can_be_inferred
    assert list(location_lint.check_nominatim_coordinates(loc, LintConfig())) == []
    search.assert_not_called()


def test_coordinate_evidence_skips_free_form_modifier(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    loc = _location_without_coordinates(name="Foo River (California): mouth")
    search = Mock()
    monkeypatch.setattr(nominatim, "search", search)

    Location.coordinate_evidence(loc)

    assert (
        "Lookup skipped: modifier is not a fully parsed distance offset"
        in capsys.readouterr().out
    )
    search.assert_not_called()


def test_coordinate_evidence_skips_explicit_coordinate_modifier(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    loc = _location_without_coordinates(name="Site: 17°S 70°W")
    search = Mock()
    monkeypatch.setattr(nominatim, "search", search)

    Location.coordinate_evidence(loc)

    assert (
        "Lookup skipped: modifier supplies explicit locality coordinates"
        in capsys.readouterr().out
    )
    search.assert_not_called()


def test_parenthetical_offset_does_not_trigger_coordinate_inference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(
        name="Castle Brace (2 mi. SW)", region=_marin_county()
    )
    search = Mock()
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    plan = location_lint.get_nominatim_search_plan(loc)

    assert plan.standardized_name == "Castle Brace: 2 mi SW"
    assert not plan.coordinates_can_be_inferred
    assert list(location_lint.check_nominatim_coordinates(loc, LintConfig())) == []
    search.assert_not_called()


def test_canonicalized_parenthetical_offset_can_infer_coordinates() -> None:
    loc = _location_without_coordinates(name="Castle Brace: 2 mi SW")

    plan = location_lint.get_nominatim_search_plan(loc)

    assert plan.standardized_name == loc.name
    assert plan.coordinates_can_be_inferred


def test_location_modifier_keeps_disambiguator_when_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = _make_region("Dominica", RegionKind.country)
    loc = _location_without_coordinates(
        id=1, name="Castle Brace: 2 mi SW", region=region
    )
    other = _location_without_coordinates(
        id=2, name="Castle Brace (Dominica)", region=region
    )
    monkeypatch.setattr(
        location_lint,
        "_get_base_name_to_locations",
        lambda: {"Castle Brace": (loc, other)},
    )

    messages = list(
        location_lint.check_should_have_disambiguator(loc, LintConfig(autofix=False))
    )

    assert len(messages) == 1
    assert "Castle Brace (Dominica): 2 mi SW" in messages[0]


@pytest.mark.parametrize(
    "name",
    [
        "10 mi above lower powerhouse, Big Cottonwood Canyon",
        "15 Mile Creek",
        "18 Mile District",
        "3.7 km from Sina",
    ],
)
def test_location_offset_name_lint_ignores_unsupported_names(name: str) -> None:
    loc = _location_without_coordinates(name=name)

    assert list(location_lint.check_offset_name(loc, LintConfig())) == []


def test_location_applies_nominatim_locality_offset(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    country = _make_region("United States", RegionKind.country)
    state = _make_region("California", RegionKind.state, country)
    county = _make_region("Monterey County, California", RegionKind.county, state)
    loc = _location_without_coordinates(name="Monterey: 8 mi E", region=county)
    base_latitude = "36.600238"
    base_longitude = "-121.894676"
    search = Mock(
        return_value=[
            _nominatim_result(
                name="Monterey",
                latitude=base_latitude,
                longitude=base_longitude,
                county="Monterey County",
                feature_type="city",
                display_name="Monterey, Monterey County, California, United States",
            )
        ]
    )
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    messages = list(
        location_lint.check_nominatim_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude is not None
    assert loc.longitude is not None
    inferred = coordinate_lint.make_point(loc.latitude, loc.longitude)
    base = coordinate_lint.make_point(base_latitude, base_longitude)
    assert inferred is not None
    assert base is not None
    assert math.isclose(
        coordinate_lint.distance_km(base, inferred), 8 * 1.609344, abs_tol=0.001
    )
    assert inferred.longitude > base.longitude
    assert "applying 8 mi E" in capsys.readouterr().out
    search.assert_called_once_with(
        "Monterey, Monterey County, California, United States"
    )


def test_coordinate_evidence_shows_nominatim_locality_offset(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    loc = _location_without_coordinates(name="Nicasio: 8 mi E", region=_marin_county())
    monkeypatch.setattr(
        nominatim,
        "search",
        Mock(
            return_value=[
                _nominatim_result(latitude="38.0615885", longitude="-122.6985975")
            ]
        ),
    )

    Location.coordinate_evidence(loc)

    output = capsys.readouterr().out
    assert "Query: Nicasio, Marin County, California, United States" in output
    assert "[accepted] place/hamlet" in output
    assert "After 8 mi E:" in output


def test_nominatim_consistency_uses_offset_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio: 8 mi E", region=_marin_county())
    result = _nominatim_result(latitude="38.0615885", longitude="-122.6985975")
    plan = location_lint.get_nominatim_search_plan(loc)
    inferred = location_lint.get_nominatim_result_coordinates(
        result, offsets=plan.offsets
    )
    assert inferred is not None
    loc.latitude, loc.longitude = inferred[:2]
    search = Mock(return_value=[result])
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert (
        list(location_lint.check_nominatim_coordinate_consistency(loc, LintConfig()))
        == []
    )
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


def test_reverse_geocoding_reports_stable_region_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _make_region("United States", RegionKind.country)
    state = _make_region("California", RegionKind.state, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Boundary site", region=state, min_period=recent, max_period=recent
    )
    loc.latitude = "39.16°N"
    loc.longitude = "119.77°W"
    reverse = Mock(return_value=_nominatim_reverse_result("Nevada"))
    monkeypatch.setattr(nominatim, "reverse", reverse)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(location_lint.check_nominatim_region_consistency(loc, LintConfig()))

    assert len(messages) == 1
    assert "consistently reverse-geocode to 'Nevada'" in messages[0]
    assert "not assigned Region 'California'" in messages[0]
    assert reverse.call_count == 5
    assert all(call.kwargs == {"zoom": 8} for call in reverse.call_args_list)


def test_reverse_geocoding_suppresses_boundary_ambiguity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _make_region("United States", RegionKind.country)
    state = _make_region("California", RegionKind.state, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Boundary site", region=state, min_period=recent, max_period=recent
    )
    loc.latitude = "39.16°N"
    loc.longitude = "119.77°W"
    reverse = Mock(
        side_effect=[
            _nominatim_reverse_result("Nevada"),
            _nominatim_reverse_result("California"),
        ]
    )
    monkeypatch.setattr(nominatim, "reverse", reverse)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert (
        list(location_lint.check_nominatim_region_consistency(loc, LintConfig())) == []
    )
    assert reverse.call_count == 2


def test_reverse_geocoding_accepts_unqualified_region_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _make_region("Netherlands", RegionKind.country)
    province = _make_region("Limburg (Netherlands)", RegionKind.province, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Site", region=province, min_period=recent, max_period=recent
    )
    loc.latitude = "51°N"
    loc.longitude = "5.8°E"
    reverse = Mock(
        return_value=nominatim.ReverseResult(
            display_name="Site, Limburg, Netherlands",
            address={"province": "Limburg", "country": "Netherlands"},
        )
    )
    monkeypatch.setattr(nominatim, "reverse", reverse)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert (
        list(location_lint.check_nominatim_region_consistency(loc, LintConfig())) == []
    )
    reverse.assert_called_once()


@pytest.mark.parametrize(
    ("region_name", "region_kind", "country_name", "address"),
    [
        (
            "Var",
            RegionKind.department,
            "France",
            {
                "county": "Var",
                "state": "Provence-Alpes-Côte d'Azur",
                "country": "France",
            },
        ),
        (
            "Thessaly",
            RegionKind.region,
            "Greece",
            {
                "county": "Larisa Regional Unit",
                "state": "Thessaly",
                "country": "Greece",
            },
        ),
        (
            "Alicante",
            RegionKind.province,
            "Spain",
            {
                "region": "l'Alacantí",
                "province": "Alacant / Alicante",
                "state": "Valencian Community",
                "country": "Spain",
            },
        ),
    ],
)
def test_reverse_geocoding_matches_full_administrative_hierarchy(
    monkeypatch: pytest.MonkeyPatch,
    region_name: str,
    region_kind: RegionKind,
    country_name: str,
    address: dict[str, str],
) -> None:
    country = _make_region(country_name, RegionKind.country)
    region = _make_region(region_name, region_kind, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Site", region=region, min_period=recent, max_period=recent
    )
    loc.latitude = "10°N"
    loc.longitude = "10°E"
    reverse = Mock(
        return_value=nominatim.ReverseResult(
            display_name=", ".join(address.values()), address=address
        )
    )
    monkeypatch.setattr(nominatim, "reverse", reverse)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert (
        list(location_lint.check_nominatim_region_consistency(loc, LintConfig())) == []
    )
    reverse.assert_called_once()
    assert reverse.call_args.kwargs == {"zoom": 8}


@pytest.mark.parametrize(
    ("region_name", "region_kind", "country_name", "osm_region_name"),
    [
        ("Bolívar", RegionKind.state, "Venezuela", "Bolivar State"),
        ("La Paz Department", RegionKind.subnational, "Bolivia", "La Paz"),
        ("La Paz Department", RegionKind.department, "Bolivia", "La Paz"),
        ("Mexico State", RegionKind.state, "Mexico", "State of Mexico"),
    ],
)
def test_reverse_geocoding_accepts_administrative_designator_aliases(
    monkeypatch: pytest.MonkeyPatch,
    region_name: str,
    region_kind: RegionKind,
    country_name: str,
    osm_region_name: str,
) -> None:
    country = _make_region(country_name, RegionKind.country)
    region = _make_region(region_name, region_kind, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Site", region=region, min_period=recent, max_period=recent
    )
    loc.latitude = "10°N"
    loc.longitude = "60°W"
    reverse = Mock(
        return_value=nominatim.ReverseResult(
            display_name=f"{osm_region_name}, {country_name}",
            address={"state": osm_region_name, "country": country_name},
        )
    )
    monkeypatch.setattr(nominatim, "reverse", reverse)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert (
        list(location_lint.check_nominatim_region_consistency(loc, LintConfig())) == []
    )
    reverse.assert_called_once()


@pytest.mark.parametrize(
    ("region_name", "region_kind", "country_name", "osm_region_name"),
    [
        ("Alpes-Maritimes", RegionKind.department, "France", "Maritime Alps"),
        (
            "Basque Country",
            RegionKind.subnational,
            "Spain",
            "Autonomous Community of the Basque Country",
        ),
        (
            "Bougainville Region",
            RegionKind.region,
            "Papua New Guinea",
            "Autonomous Region of Bougainville",
        ),
        ("Castilla-La Mancha", RegionKind.subnational, "Spain", "Castile-La Mancha"),
        (
            "Distrito Federal (Brazil)",
            RegionKind.subnational,
            "Brazil",
            "Federal District",
        ),
        ("Distrito Federal (Mexico)", RegionKind.subnational, "Mexico", "Mexico City"),
        ("Graubünden", RegionKind.canton, "Switzerland", "Grisons"),
        ("Haute-Corse", RegionKind.department, "France", "Upper Corsica"),
        ("Haute-Savoie", RegionKind.department, "France", "Upper Savoy"),
        ("La Guaira", RegionKind.state, "Venezuela", "Vargas State"),
        ("Madrid", RegionKind.subnational, "Spain", "Community of Madrid"),
        ("North Aegean", RegionKind.region, "Greece", "Northern Aegean"),
        (
            "North Ossetia",
            RegionKind.subnational,
            "Russia",
            "Republic of North Ossetia – Alania",
        ),
        ("Orissa", RegionKind.state, "India", "Odisha"),
        ("Tibet", RegionKind.subnational, "China", "Xizang"),
        ("Khyber-Pakhtunkhwa", RegionKind.province, "Pakistan", "Khyber Pakhtunkhwa"),
        ("Sakha", RegionKind.subnational, "Russia", "Sakha Republic"),
        (
            "Trentino-Alto Adige",
            RegionKind.region,
            "Italy",
            "Trentino – Alto Adige/Südtirol",
        ),
        ("Castellón", RegionKind.province, "Spain", "Castelló / Castellón"),
    ],
)
def test_reverse_geocoding_accepts_osm_region_name_variants(
    monkeypatch: pytest.MonkeyPatch,
    region_name: str,
    region_kind: RegionKind,
    country_name: str,
    osm_region_name: str,
) -> None:
    country = _make_region(country_name, RegionKind.country)
    region = _make_region(region_name, region_kind, country)
    recent = SimpleNamespace(name="Recent")
    location = _location_without_coordinates(
        name="Site", region=region, min_period=recent, max_period=recent
    )
    location.latitude = "10°N"
    location.longitude = "10°E"
    reverse = Mock(
        return_value=nominatim.ReverseResult(
            display_name=f"{osm_region_name}, {country_name}",
            address={"state": osm_region_name, "country": country_name},
        )
    )
    monkeypatch.setattr(nominatim, "reverse", reverse)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert (
        list(location_lint.check_nominatim_region_consistency(location, LintConfig()))
        == []
    )
    reverse.assert_called_once()


def test_reverse_geocoding_only_matches_administrative_address_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _make_region("United States", RegionKind.country)
    state = _make_region("California", RegionKind.state, country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Site", region=state, min_period=recent, max_period=recent
    )
    loc.latitude = "39°N"
    loc.longitude = "119°W"
    reverse = Mock(
        return_value=nominatim.ReverseResult(
            display_name="California settlement, Nevada, United States",
            address={
                "city": "California",
                "state": "Nevada",
                "country": "United States",
            },
        )
    )
    monkeypatch.setattr(nominatim, "reverse", reverse)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(location_lint.check_nominatim_region_consistency(loc, LintConfig()))

    assert len(messages) == 1
    assert "reverse-geocode to 'Nevada'" in messages[0]
    assert reverse.call_count == 5


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
