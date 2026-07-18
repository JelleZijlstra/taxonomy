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
from taxonomy.db.models.location import Location, LocationTag
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
    assert callbacks["source_callback"] is source_callback


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
        coordinate_lint, "check_point_in_region", lambda point, region: ()
    )

    messages = list(
        location_lint.check_nominatim_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude == "38.0615885°N"
    assert loc.longitude == "122.6985975°W"
    search.assert_called_once_with("Nicasio, Marin County, California, United States")


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
