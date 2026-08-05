import dataclasses
import math
import sqlite3
from collections.abc import Callable
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, call

import pytest

from taxonomy import getinput
from taxonomy.apis import geonames, nominatim, plss
from taxonomy.db import coordinate_lint, models
from taxonomy.db.constants import RegionKind, SpeciesGroupType
from taxonomy.db.models import lint as model_lint
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location, LocationStatus
from taxonomy.db.models.location import lint as location_lint
from taxonomy.db.models.location import model as location_model
from taxonomy.db.models.name import TypeTag
from taxonomy.db.models.occurrence_record import OccurrenceRecordTag
from taxonomy.db.models.period import Period
from taxonomy.db.models.region import Region, RegionTag
from taxonomy.db.models.stratigraphic_unit import StratigraphicUnit
from taxonomy.db.models.tags import LocationTag

_REAL_GET_GEONAMES_COORDINATE_MATCHES = location_lint._get_geonames_coordinate_matches


@pytest.fixture(autouse=True)
def _disable_live_geonames_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        location_lint, "_get_geonames_coordinate_matches", Mock(return_value=[])
    )
    monkeypatch.setattr(geonames, "get_administrative_hierarchy", Mock(return_value=[]))


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
    created = cast(Location, SimpleNamespace(fill_required_fields=Mock()))
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
    assert callbacks["generalize"] == loc.generalize
    assert callbacks["coordinate_evidence"] == loc.coordinate_evidence
    assert callbacks["pick_coordinates"] == loc.pick_coordinates
    assert callbacks["source_callback"] is source_callback


def test_open_coordinates_opens_opposite_range_corner_markers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = cast(
        Location,
        SimpleNamespace(
            latitude="25.3329597°N-25.7789852°N", longitude="79.3146515°W-79.1791883°W"
        ),
    )
    open_ = Mock()
    monkeypatch.setattr(
        "taxonomy.db.models.location.model.subprocess.check_call", open_
    )

    Location.open_coordinates(loc)

    assert loc.latitude is not None
    assert loc.longitude is not None
    extent = coordinate_lint.make_extent(loc.latitude, loc.longitude)
    assert extent is not None
    open_.assert_called_once_with(["open", *extent.openstreetmap_urls])


def _coordinate_choice(
    latitude: str, longitude: str, source: str
) -> location_model._CoordinateChoice:
    parsed = coordinate_lint.standardize_coordinate_pair(latitude, longitude)
    assert parsed is not None
    standardized_latitude, standardized_longitude, extent = parsed
    return location_model._CoordinateChoice(
        standardized_latitude, standardized_longitude, extent, [source]
    )


def test_pick_coordinates_uses_getinput_menu_and_prints_existing_coordinates(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    loc = _location_without_coordinates(name="Nicasio")
    loc.latitude = "38°N"
    loc.longitude = "122°W"
    choices = [
        _coordinate_choice("38.1°N", "122.1°W", "linked Name"),
        _coordinate_choice("38.2°N", "122.2°W", "GeoNames P/PPL"),
    ]
    loc._get_coordinate_choices = lambda: choices  # type: ignore[method-assign]
    get_with_completion = Mock(return_value="1")
    monkeypatch.setattr(getinput, "get_with_completion", get_with_completion)

    Location.pick_coordinates(loc)

    assert loc.latitude == "38.2°N"
    assert loc.longitude == "122.2°W"
    output = capsys.readouterr().out
    assert "Existing Location coordinates: 38°N, 122°W" in output
    assert "0: 38.1°N, 122.1°W — linked Name" in output
    assert "1: 38.2°N, 122.2°W — GeoNames P/PPL" in output
    assert "Selected coordinates: 38.2°N, 122.2°W" in output
    assert get_with_completion.call_args.kwargs["callbacks"].keys() == {"combine"}


def test_pick_coordinates_empty_choice_leaves_coordinates_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio")
    loc._get_coordinate_choices = lambda: [  # type: ignore[method-assign]
        _coordinate_choice("38.1°N", "122.1°W", "linked Name")
    ]
    monkeypatch.setattr(getinput, "get_with_completion", Mock(return_value=None))

    Location.pick_coordinates(loc)

    assert loc.latitude is None
    assert loc.longitude is None


def test_pick_coordinates_combine_unions_all_options(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    loc = _location_without_coordinates(name="Nicasio")
    loc._get_coordinate_choices = lambda: [  # type: ignore[method-assign]
        _coordinate_choice("37.9°N-38°N", "122.2°W-122.1°W", "linked Name"),
        _coordinate_choice("38.2°N", "121.9°W", "Nominatim"),
    ]

    def choose_combine(*args: object, **kwargs: object) -> None:
        callbacks = cast(dict[str, Callable[[], None]], kwargs["callbacks"])
        callbacks["combine"]()

    monkeypatch.setattr(getinput, "get_with_completion", choose_combine)

    Location.pick_coordinates(loc)

    assert loc.latitude == "37.9°N-38.2°N"
    assert loc.longitude == "122.2°W-121.9°W"
    assert (
        "Combined coordinates: 37.9°N-38.2°N, 122.2°W-121.9°W"
        in capsys.readouterr().out
    )


def test_coordinate_choices_include_linked_geonames_and_nominatim_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, SimpleNamespace(id=99))
    name = _tagged_object(
        (
            TypeTag.Coordinates("10°N", "20°E"),
            TypeTag.LocationDetail("reported at 11°N, 21°E", source),
        ),
        name_tags=True,
    )
    name.id = 101
    record = _tagged_object(
        (
            OccurrenceRecordTag.Coordinates("10°N", "20°E"),
            OccurrenceRecordTag.VerbatimCoordinates("12°N, 22°E"),
        ),
        name_tags=False,
    )
    record.id = 202
    loc = _location_without_coordinates(names=(name,), records=(record,))
    monkeypatch.setattr(
        location_lint,
        "_get_accepted_geonames_coordinate_candidates",
        Mock(return_value=[_geonames_candidate(latitude=13, longitude=23)]),
    )
    parsed_nominatim = coordinate_lint.standardize_coordinate_pair("14", "24")
    assert parsed_nominatim is not None
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_coordinate_candidates",
        Mock(
            return_value=[
                (_nominatim_result(latitude="14", longitude="24"), parsed_nominatim)
            ]
        ),
    )

    choices = Location._get_coordinate_choices(loc)

    assert [(choice.latitude, choice.longitude) for choice in choices] == [
        ("10°N", "20°E"),
        ("11°N", "21°E"),
        ("12°N", "22°E"),
        ("13°N", "23°E"),
        ("14°N", "24°E"),
    ]
    assert len(choices[0].sources) == 2
    assert "LocationDetail" in choices[1].sources[0]
    assert "verbatim coordinates" in choices[2].sources[0]
    assert "GeoNames P/PPL" in choices[3].sources[0]
    assert "Nominatim place/hamlet coordinates" in choices[4].sources[0]


def test_neotype_coordinate_choices_ignore_original_location_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_source = cast(Article, SimpleNamespace(id=98))
    neotype_source = cast(Article, SimpleNamespace(id=99))
    name = _tagged_object(
        (
            TypeTag.NeotypeDesignation(
                optional_source=neotype_source, neotype="USNM 1", valid=True
            ),
            TypeTag.LocationDetail("original locality at 10°N, 20°E", original_source),
            TypeTag.LocationDetail("neotype locality at 11°N, 21°E", neotype_source),
        ),
        name_tags=True,
    )
    name.id = 101
    name.species_type_kind = SpeciesGroupType.neotype
    loc = _location_without_coordinates(names=(name,))
    monkeypatch.setattr(
        location_lint,
        "_get_accepted_geonames_coordinate_candidates",
        Mock(return_value=[]),
    )
    monkeypatch.setattr(
        location_lint, "_get_nominatim_coordinate_candidates", Mock(return_value=[])
    )

    choices = Location._get_coordinate_choices(loc)

    assert [(choice.latitude, choice.longitude) for choice in choices] == [
        ("11°N", "21°E")
    ]


def test_neotype_name_provenance_uses_only_designation_source() -> None:
    original_source = cast(Article, SimpleNamespace(id=98))
    neotype_source = cast(Article, SimpleNamespace(id=99))
    name = _tagged_object(
        (
            TypeTag.NeotypeDesignation(
                optional_source=neotype_source, neotype="USNM 1", valid=True
            ),
            TypeTag.LocationDetail("original locality at 10°N, 20°E", original_source),
            TypeTag.LocationDetail("neotype locality at 11°N, 21°E", neotype_source),
        ),
        name_tags=True,
    )
    name.id = 101
    name.species_type_kind = SpeciesGroupType.neotype
    loc = _location_without_coordinates(names=(name,))

    extents, issue = location_lint._get_coordinate_provenance_extents(
        loc, LocationTag.CoordinatesFromName(cast(models.Name, name))
    )

    assert issue is None
    assert [
        (extent.latitude.standardized_text, extent.longitude.standardized_text)
        for extent in extents
    ] == [("11°N", "21°E")]


def test_quoted_name_provenance_accepts_unparsed_location_detail() -> None:
    source = cast(Article, SimpleNamespace(id=99))
    quote = "38o 10' 52.92\" S–57o 39' 11.66\" W"
    name = _tagged_object(
        (TypeTag.LocationDetail(f"Punta San Andrés ({quote})", source),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "38°10'52.92\"S"
    loc.longitude = "57°39'11.66\"W"

    extents, issue = location_lint._get_coordinate_provenance_extents(
        loc, LocationTag.CoordinatesFromName(cast(models.Name, name), text=quote)
    )

    assert issue is None
    assert [
        (extent.latitude.standardized_text, extent.longitude.standardized_text)
        for extent in extents
    ] == [("38°10'52.92\"S", "57°39'11.66\"W")]


@pytest.mark.parametrize(
    ("quote", "latitude", "longitude"),
    [
        ("Kampungu, 2.37 S – 26.47 E", "2°37'S", "26°47'E"),
        ("lat. I2°32'S, long. 132°23'E", "12°32'S", "132°23'E"),
        ("36° östl. Länge und 5° 10' südl. Breite", "5°10'S", "36°E"),
        ("lat. 32° 27' north, long. 115* 53' west", "32°27'N", "115°53'W"),
    ],
)
def test_quoted_name_provenance_safely_normalizes_reviewed_source_formats(
    quote: str, latitude: str, longitude: str
) -> None:
    source = cast(Article, SimpleNamespace(id=99))
    name = _tagged_object(
        (TypeTag.LocationDetail(f"Type locality: {quote}", source),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = latitude
    loc.longitude = longitude

    extents, issue = location_lint._get_coordinate_provenance_extents(
        loc, LocationTag.CoordinatesFromName(cast(models.Name, name), text=quote)
    )

    assert issue is None
    assert len(extents) == 1
    assert extents[0] == coordinate_lint.make_extent(latitude, longitude)


def test_quoted_name_provenance_rejects_stale_text() -> None:
    source = cast(Article, SimpleNamespace(id=99))
    name = _tagged_object(
        (TypeTag.LocationDetail("Punta San Andrés, coastal cliffs", source),),
        name_tags=True,
    )
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "38°S"
    loc.longitude = "57°W"

    extents, issue = location_lint._get_coordinate_provenance_extents(
        loc,
        LocationTag.CoordinatesFromName(
            cast(models.Name, name), text="38o 10' 52.92\" S"
        ),
    )

    assert extents == []
    assert issue is not None
    assert "is not present in an applicable LocationDetail tag" in issue


def test_quoted_neotype_name_provenance_rejects_original_locality() -> None:
    original_source = cast(Article, SimpleNamespace(id=98))
    neotype_source = cast(Article, SimpleNamespace(id=99))
    name = _tagged_object(
        (
            TypeTag.NeotypeDesignation(
                optional_source=neotype_source, neotype="USNM 1", valid=True
            ),
            TypeTag.LocationDetail(
                "original locality at unusual 10o N by 20o E", original_source
            ),
            TypeTag.LocationDetail(
                "neotype locality at unusual 11o N by 21o E", neotype_source
            ),
        ),
        name_tags=True,
    )
    name.id = 101
    name.species_type_kind = SpeciesGroupType.neotype
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "10°N"
    loc.longitude = "20°E"

    extents, issue = location_lint._get_coordinate_provenance_extents(
        loc,
        LocationTag.CoordinatesFromName(
            cast(models.Name, name), text="unusual 10o N by 20o E"
        ),
    )

    assert extents == []
    assert issue is not None
    assert "is not present in an applicable LocationDetail tag" in issue


def test_neotype_plss_evidence_uses_only_designation_source() -> None:
    original_source = cast(Article, SimpleNamespace(id=98))
    neotype_source = cast(Article, SimpleNamespace(id=99))
    name = _tagged_object(
        (
            TypeTag.NeotypeDesignation(
                optional_source=neotype_source, neotype="USNM 1", valid=True
            ),
            TypeTag.LocationDetail("original site T1N R1E Sec. 1", original_source),
            TypeTag.LocationDetail("neotype site T2N R2E Sec. 2", neotype_source),
        ),
        name_tags=True,
    )
    name.id = 101
    name.species_type_kind = SpeciesGroupType.neotype
    loc = _location_without_coordinates(names=(name,))

    evidence = location_lint._get_linked_plss_evidence(loc)

    assert [item.description.canonical_text for item in evidence] == ["T2N R2E Sec. 2"]


@pytest.mark.parametrize(
    ("latitude", "longitude", "expected_latitude", "expected_longitude"),
    [
        ("38°N", "122°W", None, None),
        ("37°N-39°N", "123°W-121°W", "37°N-39°N", "123°W-121°W"),
        ("invalid", "122°W", "invalid", "122°W"),
    ],
)
def test_generalize_adds_tag_removes_only_point_coordinates_and_formats(
    latitude: str,
    longitude: str,
    expected_latitude: str | None,
    expected_longitude: str | None,
) -> None:
    tags: list[object] = []
    format_location = Mock()

    def add_tag(tag: object) -> None:
        tags.append(tag)

    loc = cast(
        Location,
        SimpleNamespace(
            latitude=latitude,
            longitude=longitude,
            tags=(),
            has_tag=lambda tag_cls: bool(tags),
            add_tag=add_tag,
            format=format_location,
        ),
    )

    Location.generalize(loc)

    assert len(tags) == 1
    assert tags[0] is LocationTag.General
    assert loc.latitude == expected_latitude
    assert loc.longitude == expected_longitude
    format_location.assert_called_once_with()


def test_generalize_does_not_duplicate_general_tag() -> None:
    add_tag = Mock()
    format_location = Mock()
    loc = cast(
        Location,
        SimpleNamespace(
            latitude=None,
            longitude=None,
            has_tag=lambda tag_cls: True,
            add_tag=add_tag,
            format=format_location,
        ),
    )

    Location.generalize(loc)

    add_tag.assert_not_called()
    format_location.assert_called_once_with()


def test_generalize_removes_provenance_with_point_coordinates() -> None:
    loc = _location_without_coordinates()
    loc.latitude = "38°N"
    loc.longitude = "122°W"
    loc.tags = (LocationTag.CoordinatesFromGeoNames(1),)  # type: ignore[assignment]
    loc.format = Mock()  # type: ignore[method-assign]

    Location.generalize(loc)

    assert loc.latitude is None
    assert loc.longitude is None
    assert loc.tags == (LocationTag.General,)


def test_alias_requires_target_without_running_regular_lints() -> None:
    alias = cast(Location, SimpleNamespace(deleted=LocationStatus.alias, parent=None))

    assert list(Location.lint_invalid(alias, LintConfig())) == [
        "alias location has no parent"
    ]

    alias.parent = cast(Location, object())
    assert list(Location.lint_invalid(alias, LintConfig())) == []


def _mergeable_location(**kwargs: object) -> Location:
    defaults: dict[str, object] = {
        "id": 1,
        "name": "Source",
        "region": "Example Region",
        "min_period": None,
        "max_period": None,
        "stratigraphic_unit": None,
        "min_age": None,
        "max_age": None,
        "source": None,
        "latitude": None,
        "longitude": None,
        "comment": None,
        "location_detail": "",
        "age_detail": "",
        "tags": (),
        "deleted": LocationStatus.valid,
        "parent": None,
        "reassign_references": Mock(),
    }
    defaults.update(kwargs)
    return cast(Location, SimpleNamespace(**defaults))


def test_merge_preserves_compatible_metadata() -> None:
    article = cast(Article, object())
    min_period = cast(Period, object())
    max_period = cast(Period, object())
    stratigraphic_unit = cast(StratigraphicUnit, object())
    source = _mergeable_location(
        id=1,
        name="Old spelling",
        min_age=100,
        max_age=200,
        min_period=min_period,
        max_period=max_period,
        stratigraphic_unit=stratigraphic_unit,
        source=article,
        latitude="10°N",
        longitude="20°E",
        comment="Source comment.",
        location_detail="Source locality evidence.",
        age_detail="Source age evidence.",
        tags=(LocationTag.General,),
    )
    target = _mergeable_location(id=2, name="Canonical spelling")

    Location.merge(source, target)

    assert target.min_age == 100
    assert target.max_age == 200
    assert target.min_period is min_period
    assert target.max_period is max_period
    assert target.stratigraphic_unit is stratigraphic_unit
    assert target.source is article
    assert (target.latitude, target.longitude) == ("10°N", "20°E")
    assert target.comment == "Source comment."
    assert target.location_detail == "Source locality evidence."
    assert target.age_detail == "Source age evidence."
    assert target.tags == (LocationTag.General,)
    source.reassign_references.assert_called_once_with(target)  # type: ignore[attr-defined]
    assert source.deleted is LocationStatus.alias
    assert source.parent is target


def test_merge_combines_text_tags_and_compatible_partial_coordinates() -> None:
    source = _mergeable_location(
        id=1,
        name="Old spelling",
        latitude="10°N",
        longitude="20°E",
        comment="Source comment.",
        location_detail="Source evidence.",
        tags=(LocationTag.General, LocationTag.Unplaced()),
    )
    target = _mergeable_location(
        id=2,
        name="Canonical spelling",
        latitude="10°N",
        comment="Target comment.",
        location_detail="Target evidence.",
        tags=(LocationTag.General,),
    )

    Location.merge(source, target)

    assert (target.latitude, target.longitude) == ("10°N", "20°E")
    assert (
        target.comment
        == "Target comment.\n\nMerged from Old spelling (L#1):\nSource comment."
    )
    assert (
        target.location_detail
        == "Target evidence.\n\nMerged from Old spelling (L#1):\nSource evidence."
    )
    assert target.tags == (LocationTag.General, LocationTag.Unplaced())


def test_merge_sorts_and_deduplicates_tags() -> None:
    source = _mergeable_location(
        id=1, name="Old spelling", tags=(LocationTag.Unplaced(), LocationTag.General)
    )
    target = _mergeable_location(
        id=2, name="Canonical spelling", tags=(LocationTag.Unplaced(),)
    )

    Location.merge(source, target)

    assert target.tags == tuple(sorted((LocationTag.General, LocationTag.Unplaced())))


def test_merge_keeps_conflicting_scalar_and_coordinate_metadata(
    capsys: pytest.CaptureFixture[str],
) -> None:
    source_article = cast(Article, object())
    target_article = cast(Article, object())
    source = _mergeable_location(
        id=1,
        name="Old spelling",
        min_age=100,
        source=source_article,
        latitude="10°N",
        longitude="20°E",
    )
    target = _mergeable_location(
        id=2,
        name="Canonical spelling",
        min_age=110,
        source=target_article,
        latitude="11°N",
        longitude="21°E",
    )

    Location.merge(source, target)

    assert target.min_age == 110
    assert target.source is target_article
    assert (target.latitude, target.longitude) == ("11°N", "21°E")
    output = capsys.readouterr().out
    assert "warning: keeping min_age" in output
    assert "warning: keeping source" in output
    assert "warning: keeping coordinates" in output


def test_merge_rejects_self_merge() -> None:
    location = _mergeable_location()

    with pytest.raises(ValueError, match="cannot merge a Location into itself"):
        Location.merge(location, location)


def test_merge_copies_coordinate_provenance_with_coordinates() -> None:
    source = _mergeable_location(
        id=1,
        name="Source",
        latitude="10°N",
        longitude="20°E",
        tags=(LocationTag.CoordinatesManual("Reviewed source coordinates."),),
    )
    target = _mergeable_location(id=2, name="Target")

    Location.merge(source, target)

    assert (target.latitude, target.longitude) == ("10°N", "20°E")
    assert target.tags == (
        LocationTag.CoordinatesManual("Reviewed source coordinates."),
    )


def test_merge_does_not_copy_provenance_for_conflicting_coordinates() -> None:
    source = _mergeable_location(
        id=1,
        name="Source",
        latitude="10°N",
        longitude="20°E",
        tags=(LocationTag.CoordinatesManual("Reviewed source coordinates."),),
    )
    target = _mergeable_location(id=2, name="Target", latitude="11°N", longitude="21°E")

    Location.merge(source, target)

    assert (target.latitude, target.longitude) == ("11°N", "21°E")
    assert target.tags == ()


def test_fully_divided_region_lint_reports_direct_location() -> None:
    region = SimpleNamespace(
        name="Example Region",
        has_children=Mock(return_value=True),
        has_tag=Mock(return_value=False),
    )
    location = SimpleNamespace(
        region=region,
        is_general=Mock(return_value=False),
        has_tag=Mock(return_value=False),
    )

    assert list(
        location_lint.check_fully_divided_region.linter(
            cast(Location, location), LintConfig()
        )
    ) == [
        "is directly assigned to fully divided Region 'Example Region'; move it to "
        "a child Region or add the General or Unplaced tag"
    ]
    region.has_tag.assert_called_once_with(RegionTag.IncompletelyDivided)


def test_fully_divided_region_lint_allows_general_location() -> None:
    region = SimpleNamespace(
        name="Example Region",
        has_children=Mock(return_value=True),
        has_tag=Mock(return_value=False),
    )
    location = SimpleNamespace(
        region=region,
        is_general=Mock(return_value=True),
        has_tag=Mock(return_value=False),
    )

    assert (
        list(
            location_lint.check_fully_divided_region.linter(
                cast(Location, location), LintConfig()
            )
        )
        == []
    )
    region.has_children.assert_not_called()


def test_fully_divided_region_lint_allows_unplaced_location() -> None:
    region = SimpleNamespace(
        name="Example Region",
        has_children=Mock(return_value=True),
        has_tag=Mock(return_value=False),
    )
    location = SimpleNamespace(
        region=region,
        is_general=Mock(return_value=False),
        has_tag=Mock(return_value=True),
    )

    assert (
        list(
            location_lint.check_fully_divided_region.linter(
                cast(Location, location), LintConfig()
            )
        )
        == []
    )
    region.has_children.assert_not_called()


def test_fully_divided_region_lint_allows_incomplete_division() -> None:
    region = SimpleNamespace(
        name="Example Region",
        has_children=Mock(return_value=True),
        has_tag=Mock(return_value=True),
    )
    location = SimpleNamespace(
        region=region,
        is_general=Mock(return_value=False),
        has_tag=Mock(return_value=False),
    )

    assert (
        list(
            location_lint.check_fully_divided_region.linter(
                cast(Location, location), LintConfig()
            )
        )
        == []
    )
    region.has_tag.assert_called_once_with(RegionTag.IncompletelyDivided)


def test_fully_divided_region_lint_allows_undivided_region() -> None:
    region = SimpleNamespace(
        name="Example Region",
        has_children=Mock(return_value=False),
        has_tag=Mock(return_value=False),
    )
    location = SimpleNamespace(
        region=region,
        is_general=Mock(return_value=False),
        has_tag=Mock(return_value=False),
    )

    assert (
        list(
            location_lint.check_fully_divided_region.linter(
                cast(Location, location), LintConfig()
            )
        )
        == []
    )
    region.has_tag.assert_not_called()


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


def test_extract_bracketed_location_equivalences() -> None:
    assert location_lint.extract_bracketed_location_equivalences(
        "Toeare [= Tuare]; Umpata (= Humpata); Tlalpam [Tlalpan]; "
        "Umpata (Humpata); [map](https://example.com)"
    ) == ("Tuare", "Humpata", "Tlalpan")


def test_explicit_location_equivalence_lint_accepts_parenthetical_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = SimpleNamespace(id=1, name="Angola")
    detail = TypeTag.LocationDetail(
        "Umpata (= Humpata)", cast(models.Article, object())
    )
    name = _tagged_object((detail,), name_tags=True)
    name.id = 100
    current = _location_without_coordinates(
        id=10, name="Umpata", region=cast(Region, region), names=(name,)
    )
    matching = _location_without_coordinates(
        id=20, name="Humpata", region=cast(Region, region)
    )
    matching.reload = lambda: matching  # type: ignore[method-assign]
    monkeypatch.setattr(
        location_lint,
        "_get_locations_by_region",
        lambda: {region.id: (current, matching)},
    )

    messages = list(
        location_lint.check_explicit_location_equivalence(current, LintConfig())
    )

    assert len(messages) == 1
    assert "bracketed equivalent 'Humpata'" in messages[0]
    assert "valid Location 20: 'Humpata'" in messages[0]
    assert "Region 'Angola'" in messages[0]


def test_explicit_location_equivalence_lint_rechecks_same_region_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = SimpleNamespace(id=1, name="Central Sulawesi")
    other_region = SimpleNamespace(id=2, name="Elsewhere")
    source = cast(models.Article, object())
    detail = TypeTag.LocationDetail("Toeare [= Tuare], Bada", source)
    contextual_detail = TypeTag.LocationDetail(
        "Toeare, near Mount Kenia [= Mount Kenya]", source
    )
    administrative_detail = TypeTag.LocationDetail(
        "Toeare, [Toware], Central Sulawesi", source
    )
    name = _tagged_object(
        (detail, contextual_detail, administrative_detail), name_tags=True
    )
    name.id = 101

    current = _location_without_coordinates(
        id=10, name="Toeare", region=cast(Region, region), names=(name,)
    )
    matching = _location_without_coordinates(
        id=20, name="Toware", region=cast(Region, region)
    )
    matching.reload = lambda: matching  # type: ignore[method-assign]
    wrong_region = _location_without_coordinates(
        id=30, name="Tuare", region=cast(Region, other_region)
    )
    wrong_region.reload = lambda: wrong_region  # type: ignore[method-assign]
    contextual_match = _location_without_coordinates(
        id=40, name="Mount Kenya", region=cast(Region, region)
    )
    contextual_match.reload = lambda: contextual_match  # type: ignore[method-assign]
    monkeypatch.setattr(
        location_lint,
        "_get_locations_by_region",
        lambda: {region.id: (current, matching, wrong_region, contextual_match)},
    )

    messages = list(
        location_lint.check_explicit_location_equivalence(current, LintConfig())
    )

    assert len(messages) == 1
    assert "bracketed equivalent 'Tuare'" in messages[0]
    assert "valid Location 20: 'Toware'" in messages[0]
    assert "Region 'Central Sulawesi'" in messages[0]
    assert "Location 30" not in messages[0]
    assert "Location 40" not in messages[0]


def test_explicit_location_equivalence_lint_rechecks_cached_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = SimpleNamespace(id=1, name="Distrito Federal (Mexico)")
    detail = TypeTag.LocationDetail("Tlalpam [Tlalpan]", cast(models.Article, object()))
    name = _tagged_object((detail,), name_tags=True)
    name.id = 102
    current = _location_without_coordinates(
        id=10, name="Tlalpam", region=cast(Region, region), names=(name,)
    )
    matching = _location_without_coordinates(
        id=20, name="Tlalpan", region=cast(Region, region)
    )
    matching.reload = lambda: matching  # type: ignore[method-assign]
    monkeypatch.setattr(
        location_lint,
        "_get_locations_by_region",
        lambda: {region.id: (current, matching)},
    )
    messages = list(
        location_lint.check_explicit_location_equivalence(current, LintConfig())
    )
    assert len(messages) == 1
    assert "bracketed equivalent 'Tlalpan'" in messages[0]
    assert "valid Location 20: 'Tlalpan'" in messages[0]

    stale = _location_without_coordinates(
        id=20, name="Tlalpan", region=cast(Region, region)
    )
    refreshed = _location_without_coordinates(
        id=20, name="Different place", region=cast(Region, region)
    )
    stale.reload = lambda: refreshed  # type: ignore[method-assign]
    monkeypatch.setattr(
        location_lint, "_get_locations_by_region", lambda: {region.id: (current, stale)}
    )

    assert (
        list(location_lint.check_explicit_location_equivalence(current, LintConfig()))
        == []
    )


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
            tags=(),
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
            tags=(),
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
            tags=(),
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


def test_coordinate_collision_ignores_same_plss_polygon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    region = SimpleNamespace(id=1, name="Harney County")
    tags = (
        LocationTag.PLSS("T27S R31E Sec. 3, Willamette Meridian", "OR330270S0310E0"),
        LocationTag.CoordinatesFromPLSS("OR330270S0310E0"),
    )

    def make_location(location_id: int, name: str) -> Location:
        location = cast(
            Location,
            SimpleNamespace(
                id=location_id,
                name=name,
                latitude="43.1°N-43.2°N",
                longitude="118.5°W-118.4°W",
                region=region,
                min_period=recent,
                max_period=recent,
                tags=tags,
                get_tags=lambda values, tag_type: tuple(
                    tag for tag in values if isinstance(tag, tag_type)
                ),
                is_invalid=lambda: False,
            ),
        )
        location.reload = lambda: location  # type: ignore[method-assign]
        return location

    first = make_location(10, "Narrows: 2 mi SW")
    second = make_location(20, "Horse Pasture Field")
    monkeypatch.setattr(
        location_lint,
        "_get_coordinate_collision_map",
        lambda: {10: (first, second), 20: (first, second)},
    )

    assert list(location_lint.check_coordinate_collision(first, LintConfig())) == []


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
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(
            return_value=[
                _geonames_candidate(
                    geoname_id=5376890,
                    name="Nicasio",
                    matched_name="Walnut Creek",
                    match_kind="alternate_name",
                    latitude=37.91,
                    longitude=-122.11,
                ),
                _geonames_candidate(
                    geoname_id=2,
                    name="Other place",
                    latitude=40,
                    longitude=-75,
                    feature_class="T",
                    feature_code="MT",
                    region_issues=("coordinates are outside California",),
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
    assert "[rejected] place/village: Other place" in output
    assert "Rejection reasons:" in output
    assert (
        "locality name mismatch: expected 'Walnut Creek', got 'Other place'" in output
    )
    assert "GeoNames candidates:" in output
    assert "Exact-name query: 'Walnut Creek'; country=(unresolved)" in output
    assert (
        "[accepted for point-coordinate checks] P/PPL: Nicasio "
        "(GeoNames ID 5376890" in output
    )
    assert "matched alternate name 'Walnut Creek'" in output
    assert "[rejected by region checks] T/MT: Other place" in output
    assert "Region issues: coordinates are outside California" in output
    assert "Type-locality Names:" in output
    assert "Coordinates tag: 37.9°N, 122.1°W" in output
    assert "OccurrenceRecords:" in output
    assert "Coordinates tag: 37.8°N, 122.2°W" in output
    assert "Verbatim coordinates: '37.8, -122.2' -> 37.8°N, 122.2°W" in output
    assert "Coordinate uncertainty: within 1 km" in output


def test_coordinate_evidence_prints_reviewed_and_linked_plss(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = cast(Article, object())
    name = _tagged_object(
        (TypeTag.LocationDetail("T27S R30E Sec. 3", source),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(region=_plss_county(), names=(name,))
    loc.latitude = "42.9°N-43°N"
    loc.longitude = "119.1°W-119°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.PLSS("T27S R31E Sec. 3, Willamette Meridian", "OR330270S0310E0"),
    )
    monkeypatch.setattr(
        location_lint,
        "_get_plss_provenance_extent",
        Mock(
            return_value=(
                coordinate_lint.make_extent("42.9°N-43°N", "119.1°W-119°W"),
                None,
            )
        ),
    )
    monkeypatch.setattr(nominatim, "search", Mock(return_value=[]))

    Location.coordinate_evidence(loc)

    output = capsys.readouterr().out
    assert "PLSS evidence:" in output
    assert (
        "Location tag: PLSS('T27S R31E Sec. 3, Willamette Meridian', "
        "'OR330270S0310E0')" in output
    )
    assert "Resolved coordinates: 42.9°N-43°N, 119.1°W-119°W" in output
    assert "[conflicts with Location PLSS] T27S R30E Sec. 3" in output
    assert "Name 101 LocationDetail 'T27S R30E Sec. 3'" in output


def test_coordinate_evidence_explains_accepted_spelling_variant(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    country = _make_region("Philippines", RegionKind.country)
    loc = _location_without_coordinates(name="Isabella", region=country)
    monkeypatch.setattr(
        nominatim,
        "search",
        Mock(
            return_value=[
                nominatim.SearchResult(
                    latitude="6.7053597",
                    longitude="121.9710604",
                    name="Isabela",
                    display_name="Isabela, Basilan, Philippines",
                    category="place",
                    feature_type="city",
                    address={
                        "city": "Isabela",
                        "state": "Basilan",
                        "country": "Philippines",
                    },
                )
            ]
        ),
    )
    monkeypatch.setattr(
        location_lint, "_get_geonames_coordinate_matches", Mock(return_value=[])
    )

    Location.coordinate_evidence(loc)

    output = capsys.readouterr().out
    assert "[accepted] place/city: Isabela, Basilan, Philippines" in output
    assert "Match notes:" in output
    assert (
        "locality spelling differs but is a likely variant: expected 'Isabella', "
        "got 'Isabela'" in output
    )


class _TaggedObject:
    id: int
    species_type_kind: object
    is_virtual = False


def _tagged_object(tags: tuple[object, ...], *, name_tags: bool) -> _TaggedObject:
    field = "type_tags" if name_tags else "tags"
    obj = _TaggedObject()
    obj.id = 1
    obj.species_type_kind = None
    setattr(obj, field, tags)
    obj.get_tags = lambda values, tag_type: (  # type: ignore[attr-defined]
        tag for tag in values if isinstance(tag, tag_type)
    )
    obj.has_lint_ignore = lambda label: False  # type: ignore[attr-defined]
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
    names: tuple[_TaggedObject, ...] = (),
    records: tuple[_TaggedObject, ...] = (),
    general: bool | None = None,
    virtual_origin_id: int | None = None,
) -> Location:
    if region is None:
        region = _make_region(region_name, RegionKind.state)
    loc = SimpleNamespace(
        id=id,
        virtual_origin_id=virtual_origin_id,
        name=name,
        region=region,
        latitude=None,
        longitude=None,
        min_period=min_period,
        max_period=max_period,
        min_age=min_age,
        max_age=max_age,
        stratigraphic_unit=stratigraphic_unit,
        location_detail="",
        type_localities=names,
        occurrence_records=records,
        tags=(),
        get_tags=lambda tags, tag_type: (
            tag for tag in tags if isinstance(tag, tag_type)
        ),
        has_tag=lambda tag_type: any(
            tag is tag_type
            or (isinstance(tag_type, type) and isinstance(tag, tag_type))
            for tag in loc.tags  # type: ignore[has-type]
        ),
        add_tag=lambda tag: setattr(loc, "tags", (*loc.tags, tag)),  # type: ignore[has-type]
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


def _plss_county() -> Region:
    country = _make_region("United States", RegionKind.country)
    state = _make_region("Oregon", RegionKind.state, country)
    return _make_region("Harney County, Oregon", RegionKind.county, state)


def _plss_polygon() -> dict[str, object]:
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [-119.1, 42.9],
                [-119.0, 42.9],
                [-119.0, 43.0],
                [-119.1, 43.0],
                [-119.1, 42.9],
            ]
        ],
    }


def _resolved_plss(description: plss.PLSSDescription) -> plss.TownshipResolution:
    township = plss.Township(
        state="OR",
        meridian="Willamette Meridian",
        meridian_code="33",
        plss_id="OR330270S0310E0",
        description=description.with_meridian("Willamette Meridian"),
        geometries=(_plss_polygon(),),
    )
    return plss.TownshipResolution(township, (township,))


def test_plss_tag_requires_canonical_text_and_meridian() -> None:
    loc = _location_without_coordinates(region=_plss_county())
    loc.tags = (LocationTag.PLSS("T27 S, R31 E, Section 3", "bad"),)  # type: ignore[assignment]

    messages = list(location_lint.check_plss_tag(loc, LintConfig(autofix=False)))

    assert any("not canonical" in message for message in messages)
    assert any("not a valid CadNSDI PLSSID" in message for message in messages)


def test_plss_tag_autofixes_noncanonical_text() -> None:
    loc = _location_without_coordinates(region=_plss_county())
    loc.tags = (  # type: ignore[assignment]
        LocationTag.PLSS(
            "T27 S, R31 E, Section 3, Willamette Meridian", "OR330270S0310E0"
        ),
    )

    messages = list(location_lint.check_plss_tag(loc, LintConfig(autofix=True)))

    assert messages == []
    assert loc.tags == (
        LocationTag.PLSS("T27S R31E Sec. 3, Willamette Meridian", "OR330270S0310E0"),
    )


def test_plss_tag_serialization() -> None:
    tag = LocationTag.PLSS(
        "T27S R31E Sec. 3, Willamette Meridian",
        "OR330270S0310E0",
        comment="Source range is probably a typo.",
    )

    assert tag.serialize() == [
        8,
        "T27S R31E Sec. 3, Willamette Meridian",
        "OR330270S0310E0",
        "Source range is probably a typo.",
    ]
    assert LocationTag.unserialize(tag.serialize()) == tag


@pytest.mark.parametrize(
    "tag",
    [
        LocationTag.CoordinatesFromPLSS("OR330270S0310E0"),
        LocationTag.CoordinatesFromGeoNames(5376890),
        LocationTag.CoordinatesFromNominatim(
            "relation", 1234, "place", use_bounding_box=False
        ),
        LocationTag.CoordinatesFromName(models.Name(101)),
        LocationTag.CoordinatesFromOccurrenceRecord(models.OccurrenceRecord(202)),
        LocationTag.CoordinatesFromLocationName,
        LocationTag.CoordinatesManual("Reviewed against the specimen label."),
    ],
)
def test_coordinate_provenance_tag_serialization(tag: object) -> None:
    serialized = tag.serialize()  # type: ignore[attr-defined]

    assert LocationTag.unserialize(serialized) == tag


def test_coordinates_from_name_uses_name_object_with_compatible_serialization() -> None:
    name = models.Name(101)
    tag = LocationTag.CoordinatesFromName(name)

    assert tag.name is name
    assert tag.serialize() == [12, 101]
    restored = LocationTag.unserialize([12, 101])
    assert isinstance(restored, LocationTag.CoordinatesFromName)
    assert isinstance(restored.name, models.Name)
    assert restored.name.id == 101
    assert (
        LocationTag.CoordinatesFromName,
        "name",
    ) in Location.get_completers_for_adt_field("tags")


def test_coordinates_from_occurrence_record_uses_model_reference() -> None:
    record = models.OccurrenceRecord(202)
    tag = LocationTag.CoordinatesFromOccurrenceRecord(record)

    assert tag.occurrence_record is record
    assert tag.serialize() == [13, 202]
    restored = LocationTag.unserialize([13, 202])
    assert isinstance(restored, LocationTag.CoordinatesFromOccurrenceRecord)
    assert isinstance(restored.occurrence_record, models.OccurrenceRecord)
    assert restored.occurrence_record.id == 202
    assert (
        LocationTag.CoordinatesFromOccurrenceRecord,
        "occurrence_record",
    ) in Location.get_completers_for_adt_field("tags")


def test_coordinates_from_name_serializes_optional_source_text() -> None:
    name = models.Name(101)
    text = "38o 10' 52.92\" S–57o 39' 11.66\" W"
    tag = LocationTag.CoordinatesFromName(name, text=text)

    assert tag.serialize() == [12, 101, text]
    assert LocationTag.unserialize(tag.serialize()) == tag


def test_location_plss_lint_suggests_tag_from_linked_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, object())
    name = _tagged_object(
        (TypeTag.LocationDetail("T27S R31E Sec. 3", source),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(region=_plss_county(), names=(name,))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "add PLSS" in messages[0]
    assert "T27S R31E Sec. 3, Willamette Meridian" in messages[0]
    assert "OR330270S0310E0" in messages[0]


def test_location_plss_lint_uses_common_township_for_distinct_sections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, object())
    first = _tagged_object(
        (TypeTag.LocationDetail("T27S R31E Sec. 3", source),), name_tags=True
    )
    first.id = 101
    second = _tagged_object(
        (TypeTag.LocationDetail("T27S R31E Sec. 8", source),), name_tags=True
    )
    second.id = 102
    loc = _location_without_coordinates(region=_plss_county(), names=(first, second))
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "add PLSS('T27S R31E, Willamette Meridian'" in messages[0]


def test_location_plss_lint_uses_common_section_for_distinct_quarters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, object())
    first = _tagged_object(
        (TypeTag.LocationDetail("T27S R31E Sec. 3 NE1/4", source),), name_tags=True
    )
    first.id = 101
    second = _tagged_object(
        (TypeTag.LocationDetail("T27S R31E Sec. 3 SW1/4", source),), name_tags=True
    )
    second.id = 102
    loc = _location_without_coordinates(region=_plss_county(), names=(first, second))
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "add PLSS('T27S R31E Sec. 3, Willamette Meridian'" in messages[0]


def test_location_plss_lint_extracts_location_detail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(region=_plss_county())
    loc.location_detail = 'Reported as "T27S, R31E, Sec. 3".'  # type: ignore[assignment]
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=False)))

    assert len(messages) == 1
    assert "add PLSS" in messages[0]
    assert "Location location_detail" in messages[0]


def test_location_plss_lint_infers_coordinates_before_gazetteers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, object())
    name = _tagged_object(
        (TypeTag.LocationDetail("T27S R31E Sec. 3", source),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(region=_plss_county(), names=(name,))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        lambda *_: plss.ResolvedGeometry("section", (_plss_polygon(),)),
    )
    geonames_candidates = Mock(side_effect=AssertionError("GeoNames should not run"))
    nominatim_candidates = Mock(side_effect=AssertionError("Nominatim should not run"))
    monkeypatch.setattr(
        location_lint, "_get_usable_geonames_coordinate_candidates", geonames_candidates
    )
    monkeypatch.setattr(
        location_lint, "_get_nominatim_coordinate_candidates", nominatim_candidates
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=True)))
    geonames_messages = list(
        location_lint.check_geonames_coordinates(loc, LintConfig(autofix=True))
    )
    nominatim_messages = list(
        location_lint.check_nominatim_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert geonames_messages == []
    assert nominatim_messages == []
    assert loc.latitude == "42.9°N-43°N"
    assert loc.longitude == "119.1°W-119°W"
    assert loc.tags == (
        LocationTag.PLSS("T27S R31E Sec. 3, Willamette Meridian", "OR330270S0310E0"),
        LocationTag.CoordinatesFromPLSS("OR330270S0310E0"),
    )
    geonames_candidates.assert_not_called()
    nominatim_candidates.assert_not_called()


def test_plss_lint_runs_before_gazetteer_coordinate_lints() -> None:
    labels = [linter.label for linter in location_lint.LINT.linters]

    assert labels.index("plss") < labels.index("geonames_coordinates")
    assert labels.index("plss") < labels.index("nominatim_coordinates")


def test_location_plss_lint_does_not_add_tag_to_general_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, object())
    name = _tagged_object(
        (TypeTag.LocationDetail("T27S R31E Sec. 3", source),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(
        region=_plss_county(), names=(name,), general=True
    )
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "general location has precise PLSS evidence" in messages[0]
    assert loc.tags == ()


def test_location_plss_lint_rejects_tag_on_general_location() -> None:
    loc = _location_without_coordinates(region=_plss_county(), general=True)
    loc.tags = (  # type: ignore[assignment]
        LocationTag.PLSS("T27S R31E Sec. 3, Willamette Meridian", "OR330270S0310E0"),
    )

    messages = list(location_lint.check_general_plss(loc, LintConfig()))

    assert len(messages) == 1
    assert "general location should not have a precise PLSS tag" in messages[0]


def test_location_plss_lint_infers_township_from_alternative_sections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, object())
    name = _tagged_object(
        (
            TypeTag.LocationDetail(
                "Bardack gave the provenance as Section 11 or 13, Township 13 S, "
                "Range 36 W.",
                source,
            ),
        ),
        name_tags=True,
    )
    name.id = 69895
    loc = _location_without_coordinates(
        region=_plss_county(), names=(name,), general=True
    )
    resolver = Mock(side_effect=lambda description, **_: _resolved_plss(description))
    monkeypatch.setattr(plss, "resolve_township", resolver)

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "general location has precise PLSS evidence" in messages[0]
    assert "T13S R36W, Willamette Meridian" in messages[0]
    assert loc.tags == ()
    resolver.assert_called_once()


def test_location_plss_ignore_suppresses_autofix_and_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, object())
    name = _tagged_object(
        (TypeTag.LocationDetail("T27S R31E Sec. 3", source),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(
        region=_plss_county(), names=(name,), general=True
    )
    loc.tags = (  # type: ignore[assignment]
        LocationTag.IgnoreLintLocation(
            "plss", comment="The source explicitly questions this provenance."
        ),
    )
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=True)))

    assert messages == []
    assert loc.tags == (
        LocationTag.IgnoreLintLocation(
            "plss", comment="The source explicitly questions this provenance."
        ),
    )


def test_location_plss_lint_checks_coordinates_against_polygon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = "T27S R31E Sec. 3, Willamette Meridian"
    loc = _location_without_coordinates(region=_plss_county())
    loc.tags = (LocationTag.PLSS(text, "OR330270S0310E0"),)  # type: ignore[assignment]
    loc.latitude = "45°N"
    loc.longitude = "119°W"
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        lambda *_: plss.ResolvedGeometry("section", (_plss_polygon(),)),
    )

    messages = list(location_lint.check_plss(loc, LintConfig()))

    assert len(messages) == 1
    assert "from the section polygon" in messages[0]


def test_location_plss_lint_adds_coordinate_bounds_in_autofix_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = "T27S R31E Sec. 3, Willamette Meridian"
    plss_tag = LocationTag.PLSS(text, "OR330270S0310E0")
    loc = _location_without_coordinates(region=_plss_county())
    loc.tags = (plss_tag,)  # type: ignore[assignment]
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        lambda *_: plss.ResolvedGeometry("section", (_plss_polygon(),)),
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=True)))

    assert messages == []
    assert loc.latitude == "42.9°N-43°N"
    assert loc.longitude == "119.1°W-119°W"
    assert loc.tags == (plss_tag, LocationTag.CoordinatesFromPLSS("OR330270S0310E0"))
    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []


def test_location_plss_lint_does_not_overwrite_partial_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = "T27S R31E Sec. 3, Willamette Meridian"
    plss_tag = LocationTag.PLSS(text, "OR330270S0310E0")
    loc = _location_without_coordinates(region=_plss_county())
    loc.latitude = "42.95°N"
    loc.tags = (plss_tag,)  # type: ignore[assignment]
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        lambda *_: plss.ResolvedGeometry("section", (_plss_polygon(),)),
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "coordinate bounds could be" in messages[0]
    assert loc.latitude == "42.95°N"
    assert loc.longitude is None
    assert loc.tags == (plss_tag,)


def test_location_plss_lint_autofixes_resolved_tag_fields_and_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(region=_plss_county())
    loc.tags = (  # type: ignore[assignment]
        LocationTag.PLSS(
            "T27S R31E Sec. 3, Willamette principal meridian",
            "OR330270S0300E0",
            comment="Reviewed source.",
        ),
        LocationTag.CoordinatesFromPLSS("OR330270S0300E0"),
    )
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        lambda *_: plss.ResolvedGeometry("section", ()),
    )

    messages = list(location_lint.check_plss(loc, LintConfig(autofix=True)))

    assert messages == []
    assert loc.tags == (
        LocationTag.PLSS(
            "T27S R31E Sec. 3, Willamette Meridian",
            "OR330270S0310E0",
            comment="Reviewed source.",
        ),
        LocationTag.CoordinatesFromPLSS("OR330270S0310E0"),
    )


def test_location_plss_lint_leaves_linked_conflicts_to_name_lint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(Article, object())
    name = _tagged_object(
        (TypeTag.LocationDetail("T27S R30E Sec. 3", source),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(region=_plss_county(), names=(name,))
    loc.tags = (  # type: ignore[assignment]
        LocationTag.PLSS("T27S R31E Sec. 3, Willamette Meridian", "OR330270S0310E0"),
    )
    loc.latitude = "42.9°N-43°N"
    loc.longitude = "119.1°W-119°W"
    resolver = Mock(side_effect=lambda description, **_: _resolved_plss(description))
    monkeypatch.setattr(plss, "resolve_township", resolver)
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        lambda *_: plss.ResolvedGeometry("section", (_plss_polygon(),)),
    )

    assert list(location_lint.check_plss(loc, LintConfig())) == []
    resolver.assert_called_once()


def test_location_plss_lint_only_reports_missing_polygon_when_verbose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = "T27S R31E Sec. 3, Willamette Meridian"
    loc = _location_without_coordinates(region=_plss_county())
    loc.tags = (LocationTag.PLSS(text, "OR330270S0310E0"),)  # type: ignore[assignment]
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        lambda *_: plss.ResolvedGeometry("section", ()),
    )

    assert list(location_lint.check_plss(loc, LintConfig())) == []
    messages = list(location_lint.check_plss(loc, LintConfig(verbose=True)))
    assert len(messages) == 1
    assert "BLM has no section polygon" in messages[0]


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
    osm_type: str | None = "node",
    osm_id: int | None = 1,
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
        osm_id=osm_id,
        bounding_box=bounding_box,
        address={
            feature_type: name,
            "county": county,
            "state": state,
            "country": country,
            "country_code": "us",
        },
    )


def _geonames_candidate(
    *,
    geoname_id: int = 1,
    name: str = "Nicasio",
    latitude: float = 38.0615885,
    longitude: float = -122.6985975,
    matched_name: str | None = None,
    match_kind: geonames.MatchKind = "name",
    feature_class: str = "P",
    feature_code: str = "PPL",
    region_issues: tuple[str, ...] = (),
) -> location_lint.GeoNamesCoordinateCandidate:
    record = geonames.GeoNamesRecord(
        geoname_id=geoname_id,
        name=name,
        ascii_name=name,
        alternate_names="",
        latitude=latitude,
        longitude=longitude,
        feature_class=feature_class,
        feature_code=feature_code,
        country_code="US",
        alternate_country_codes="",
        admin1_code="CA",
        admin2_code="041",
        admin3_code="",
        admin4_code="",
        population=0,
        elevation=None,
        dem=None,
        timezone="America/Los_Angeles",
        modification_date="2026-01-01",
    )
    match = geonames.GeoNamesMatch(
        record, matched_name if matched_name is not None else name, match_kind
    )
    parsed = coordinate_lint.standardize_coordinate_pair(str(latitude), str(longitude))
    assert parsed is not None
    standardized_latitude, standardized_longitude, extent = parsed
    return location_lint.GeoNamesCoordinateCandidate(
        match, standardized_latitude, standardized_longitude, extent, region_issues
    )


def test_geonames_alternate_name_lint_preserves_name_structure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Calcutta (India): 2 km N")
    loc.tags = (LocationTag.CoordinatesFromGeoNames(1275004),)  # type: ignore[assignment]
    record = dataclasses.replace(
        _geonames_candidate(geoname_id=1275004, name="Kolkata").match.record,
        alternate_names="Calcutta,Kolkata",
    )
    monkeypatch.setattr(geonames, "get_by_id", Mock(return_value=record))

    messages = list(
        location_lint.check_geonames_alternate_name.linter(
            loc, LintConfig(autofix=True)
        )
    )

    assert messages == [
        (
            "base name 'Calcutta' is a GeoNames alternate name rather than the "
            "primary name 'Kolkata' (ID 1275004); review possible modern name "
            "'Kolkata (India): 2 km N'"
        )
    ]
    assert loc.name == "Calcutta (India): 2 km N"


def test_geonames_alternate_name_lint_flags_ascii_spelling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Valparaiso")
    loc.tags = (LocationTag.CoordinatesFromGeoNames(3868626),)  # type: ignore[assignment]
    record = dataclasses.replace(
        _geonames_candidate(geoname_id=3868626, name="Valparaíso").match.record,
        ascii_name="Valparaiso",
        alternate_names="Valparaiso,Valparaíso",
    )
    monkeypatch.setattr(geonames, "get_by_id", Mock(return_value=record))

    messages = list(
        location_lint.check_geonames_alternate_name.linter(loc, LintConfig())
    )

    assert messages == [
        (
            "base name 'Valparaiso' is a GeoNames ASCII name rather than the primary "
            "name 'Valparaíso' (ID 3868626); review possible modern name 'Valparaíso'"
        )
    ]


@pytest.mark.parametrize(
    ("location_name", "record_values"),
    [
        ("Kolkata", ("Kolkata", "Kolkata", "Calcutta,Kolkata", "PPL")),
        ("Unrelated", ("Kolkata", "Kolkata", "Calcutta,Kolkata", "PPL")),
        (
            "Reese River",
            (
                "Jacobsville (historical)",
                "Jacobsville (historical)",
                "Reese River",
                "PPLQ",
            ),
        ),
    ],
)
def test_geonames_alternate_name_lint_skips_non_candidates(
    monkeypatch: pytest.MonkeyPatch,
    location_name: str,
    record_values: tuple[str, str, str, str],
) -> None:
    record_name, ascii_name, alternate_names, feature_code = record_values
    loc = _location_without_coordinates(name=location_name)
    loc.tags = (LocationTag.CoordinatesFromGeoNames(1),)  # type: ignore[assignment]
    record = dataclasses.replace(
        _geonames_candidate(name=record_name, feature_code=feature_code).match.record,
        ascii_name=ascii_name,
        alternate_names=alternate_names,
    )
    monkeypatch.setattr(geonames, "get_by_id", Mock(return_value=record))

    assert (
        list(location_lint.check_geonames_alternate_name.linter(loc, LintConfig()))
        == []
    )


def test_geonames_alternate_name_lint_skips_unavailable_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Calcutta")
    loc.tags = (LocationTag.CoordinatesFromGeoNames(1275004),)  # type: ignore[assignment]
    monkeypatch.setattr(
        geonames, "get_by_id", Mock(side_effect=RuntimeError("not configured"))
    )

    assert (
        list(location_lint.check_geonames_alternate_name.linter(loc, LintConfig()))
        == []
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
    assert loc.tags == (LocationTag.CoordinatesFromName(cast(models.Name, name)),)


def test_unplaced_location_does_not_infer_point_coordinates_from_name() -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9", "-122.1"),), name_tags=True)
    loc = _location_without_coordinates(names=(name,))
    loc.tags = (LocationTag.Unplaced(),)  # type: ignore[assignment]

    assert (
        list(location_lint.check_linked_coordinates(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.latitude is None
    assert loc.longitude is None
    assert loc.tags == (LocationTag.Unplaced(),)


def test_location_infers_coordinates_from_name_location_detail() -> None:
    source = cast(models.Article, object())
    name = _tagged_object(
        (TypeTag.LocationDetail("Collected at 37.9°N, 122.1°W", source),),
        name_tags=True,
    )
    loc = _location_without_coordinates(names=(name,))

    assert (
        list(location_lint.check_linked_coordinates(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.latitude == "37.9°N"
    assert loc.longitude == "122.1°W"
    assert loc.tags == (LocationTag.CoordinatesFromName(cast(models.Name, name)),)


def test_location_coordinate_evidence_honors_name_coordinate_ignore() -> None:
    source = cast(models.Article, object())
    name = _tagged_object(
        (
            TypeTag.LocationDetail("Referred specimen at 37.9°N, 122.1°W", source),
            TypeTag.IgnoreLintName(
                "location_detail_coordinates",
                comment="The coordinate belongs to a referred specimen.",
            ),
        ),
        name_tags=True,
    )
    name.has_lint_ignore = lambda label: label == "location_detail_coordinates"  # type: ignore[attr-defined]
    loc = _location_without_coordinates(names=(name,))

    assert location_lint._get_linked_coordinate_evidence(loc) == []


def test_location_infers_coordinates_from_name_specimen_detail() -> None:
    source = cast(models.Article, object())
    name = _tagged_object(
        (
            TypeTag.LocationDetail("Mount Somoro, Papua New Guinea", source),
            TypeTag.SpecimenDetail(
                "Holotype from Mount Somoro (3°22'S, 142°9'E)", source
            ),
        ),
        name_tags=True,
    )
    loc = _location_without_coordinates(names=(name,))

    assert (
        list(location_lint.check_linked_coordinates(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.latitude == "3°22'S"
    assert loc.longitude == "142°9'E"
    assert loc.tags == (LocationTag.CoordinatesFromName(cast(models.Name, name)),)


def test_location_does_not_infer_multiple_specimen_detail_coordinates() -> None:
    source = cast(models.Article, object())
    name = _tagged_object(
        (
            TypeTag.SpecimenDetail(
                "Holotype from Bogamanda (2.08°S, 28.49°E); "
                "paratype from Tschamola (2.01°S, 28.53°E)",
                source,
            ),
        ),
        name_tags=True,
    )
    loc = _location_without_coordinates(names=(name,))

    assert list(location_lint.check_linked_coordinates(loc, LintConfig())) == []
    assert loc.latitude is None
    assert loc.longitude is None


def test_location_does_not_infer_coordinates_from_proposed_neotype() -> None:
    source = cast(models.Article, object())
    name = _tagged_object(
        (
            TypeTag.SpecimenDetail(
                "We hereby propose to designate MVZ 232023, collected at "
                "34.98466N, 118.30149W",
                source,
            ),
        ),
        name_tags=True,
    )
    name.species_type_kind = SpeciesGroupType.holotype
    loc = _location_without_coordinates(names=(name,))

    assert list(location_lint.check_linked_coordinates(loc, LintConfig())) == []
    assert loc.latitude is None
    assert loc.longitude is None


def test_location_detail_coordinates_do_not_duplicate_structured_tag() -> None:
    source = cast(models.Article, object())
    name = _tagged_object(
        (
            TypeTag.Coordinates("37.9°N", "122.1°W"),
            TypeTag.LocationDetail("Collected at 37.9°N, 122.1°W", source),
        ),
        name_tags=True,
    )
    loc = _location_without_coordinates(names=(name,))

    evidence = location_lint._get_linked_coordinate_evidence(loc)

    assert len(evidence) == 1
    assert evidence[0].source == f"Name {name}"


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
    assert loc.tags == (
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
        LocationTag.CoordinatesFromOccurrenceRecord(
            cast(models.OccurrenceRecord, record)
        ),
    )


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


def test_coordinate_provenance_lint_backfills_linked_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True)
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.tags == (LocationTag.CoordinatesFromName(cast(models.Name, name)),)


def test_coordinate_provenance_lint_suggests_backfill_without_autofix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True)
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=False))
    )

    assert len(messages) == 1
    assert "add coordinate provenance tags" in messages[0]
    assert loc.tags == ()


def test_coordinate_provenance_lint_does_not_backfill_nearby_name_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True)
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "37.9001°N"
    loc.longitude = "122.1°W"
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        location_lint, "_get_usable_geonames_coordinate_candidates", lambda _: []
    )
    monkeypatch.setattr(
        location_lint, "_get_nominatim_coordinate_candidates", lambda _: []
    )

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "not supported by a coordinate provenance tag" in messages[0]
    assert loc.tags == ()


def test_coordinate_provenance_lint_reports_changed_name_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object((TypeTag.Coordinates("40°N", "74°W"),), name_tags=True)
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(location_lint.check_coordinate_provenance(loc, LintConfig()))

    assert len(messages) == 1
    assert "do not exactly match the numeric extent" in messages[0]


def test_coordinate_provenance_lint_rejects_nearby_name_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True)
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "37.9001°N"
    loc.longitude = "122.1°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(location_lint.check_coordinate_provenance(loc, LintConfig()))

    assert len(messages) == 1
    assert "do not exactly match the numeric extent" in messages[0]
    assert "expected 37.9°N, 122.1°W" in messages[0]


def test_coordinate_provenance_lint_accepts_equivalent_formatting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object(
        (TypeTag.Coordinates("37.9000", "-122.1000"),), name_tags=True
    )
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []


def test_coordinate_provenance_lint_accepts_one_exact_location_detail_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(models.Article, object())
    name = _tagged_object(
        (
            TypeTag.LocationDetail(
                "Nandesen Forest at 20°50'S, 47°10'E; an alternative account gives "
                "20°27'S, 47°9'E",
                source,
            ),
        ),
        name_tags=True,
    )
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "20°50'S"
    loc.longitude = "47°10'E"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []


def test_coordinate_provenance_lint_rejects_nonmatching_location_detail_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(models.Article, object())
    name = _tagged_object(
        (TypeTag.LocationDetail("sites at 37.9°N, 122.1°W and 38°N, 122°W", source),),
        name_tags=True,
    )
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "37.95°N"
    loc.longitude = "122.05°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(location_lint.check_coordinate_provenance(loc, LintConfig()))

    assert len(messages) == 1
    assert "expected 37.9°N-38°N, 122.1°W-122°W" in messages[0]


def test_coordinate_provenance_lint_backfills_one_exact_name_coordinate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object(
        (
            TypeTag.Coordinates("37.9°N", "122.1°W"),
            TypeTag.Coordinates("38°N", "122°W"),
        ),
        name_tags=True,
    )
    name.id = 101
    loc = _location_without_coordinates(names=(name,))
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.tags == (LocationTag.CoordinatesFromName(cast(models.Name, name)),)


def test_coordinate_provenance_lint_accepts_exact_multi_source_union(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True)
    name.id = 101
    record = _tagged_object(
        (OccurrenceRecordTag.Coordinates("37.91°N", "122.09°W"),), name_tags=False
    )
    record.id = 202
    loc = _location_without_coordinates(names=(name,), records=(record,))
    loc.latitude = "37.9°N-37.91°N"
    loc.longitude = "122.1°W-122.09°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
        LocationTag.CoordinatesFromOccurrenceRecord(
            cast(models.OccurrenceRecord, record)
        ),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []


def test_coordinate_provenance_lint_accepts_virtual_linked_name() -> None:
    loc = Location.virtual(
        name="Precise site", latitude="37.9°N", longitude="122.1°W", tags=()
    )
    name = models.Name.virtual(
        type_locality=loc, type_tags=(TypeTag.Coordinates("37.9°N", "122.1°W"),)
    )
    loc.tags = (LocationTag.CoordinatesFromName(name),)

    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []


def test_coordinate_provenance_lint_accepts_virtual_linked_occurrence() -> None:
    loc = Location.virtual(
        name="Precise site", latitude="37.9°N", longitude="122.1°W", tags=()
    )
    record = models.OccurrenceRecord.virtual(
        location=loc,
        locality_text="Precise site",
        tags=(OccurrenceRecordTag.Coordinates("37.9°N", "122.1°W"),),
    )
    loc.tags = (LocationTag.CoordinatesFromOccurrenceRecord(record),)

    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []


def test_coordinate_provenance_lint_accepts_alternative_when_each_source_is_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = cast(models.Article, object())
    first_name = _tagged_object(
        (TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True
    )
    first_name.id = 101
    second_name = _tagged_object(
        (
            TypeTag.LocationDetail(
                "Most material at 37.9°N, 122.1°W; some from 38°N, 122°W", source
            ),
        ),
        name_tags=True,
    )
    second_name.id = 102
    loc = _location_without_coordinates(names=(first_name, second_name))
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromName(cast(models.Name, first_name)),
        LocationTag.CoordinatesFromName(cast(models.Name, second_name)),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []


def test_coordinate_provenance_lint_rejects_stale_extra_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True)
    name.id = 101
    record = _tagged_object(
        (OccurrenceRecordTag.Coordinates("38°N", "122°W"),), name_tags=False
    )
    record.id = 202
    loc = _location_without_coordinates(names=(name,), records=(record,))
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
        LocationTag.CoordinatesFromOccurrenceRecord(
            cast(models.OccurrenceRecord, record)
        ),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(location_lint.check_coordinate_provenance(loc, LintConfig()))

    assert len(messages) == 1
    assert "expected 37.9°N-38°N, 122.1°W-122°W" in messages[0]


def test_coordinate_provenance_lint_backfills_exact_multi_source_union(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _tagged_object((TypeTag.Coordinates("37.9°N", "122.1°W"),), name_tags=True)
    name.id = 101
    record = _tagged_object(
        (OccurrenceRecordTag.Coordinates("37.91°N", "122.09°W"),), name_tags=False
    )
    record.id = 202
    loc = _location_without_coordinates(names=(name,), records=(record,))
    loc.latitude = "37.9°N-37.91°N"
    loc.longitude = "122.1°W-122.09°W"
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.tags == (
        LocationTag.CoordinatesFromName(cast(models.Name, name)),
        LocationTag.CoordinatesFromOccurrenceRecord(
            cast(models.OccurrenceRecord, record)
        ),
    )


def test_coordinate_provenance_lint_accepts_manual_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates()
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesManual("Selected from the specimen label map."),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []


def test_coordinate_provenance_lint_reports_unsupported_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates()
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        location_lint, "_get_usable_geonames_coordinate_candidates", lambda _: []
    )
    monkeypatch.setattr(
        location_lint, "_get_nominatim_coordinate_candidates", lambda _: []
    )

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "not supported by a coordinate provenance tag" in messages[0]


def test_coordinate_provenance_lint_backfills_nominatim_for_non_recent_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oligocene = SimpleNamespace(name="Oligocene")
    loc = _location_without_coordinates(
        name="White Bluffs", min_period=oligocene, max_period=oligocene
    )
    loc.latitude = "46.6737483°N"
    loc.longitude = "119.4366902°W"
    result = _nominatim_result(
        latitude="46.6737483",
        longitude="-119.4366902",
        name="White Bluffs",
        category="natural",
        feature_type="cliff",
        display_name="White Bluffs, Franklin County, Washington, United States",
        osm_id=356541547,
    )
    parsed = location_lint.get_nominatim_result_coordinates(result)
    assert parsed is not None
    monkeypatch.setattr(
        location_lint, "_get_usable_geonames_coordinate_candidates", lambda _: []
    )
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_coordinate_candidates",
        lambda _: [(result, parsed)],
    )

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.tags == (
        LocationTag.CoordinatesFromNominatim(
            "node", 356541547, "natural", use_bounding_box=False
        ),
    )


def test_coordinate_provenance_lint_backfills_plss_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(region=_plss_county())
    loc.latitude = "42.9°N-43°N"
    loc.longitude = "119.1°W-119°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.PLSS("T27S R31E Sec. 3, Willamette Meridian", "OR330270S0310E0"),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        plss, "resolve_township", lambda description, **_: _resolved_plss(description)
    )
    monkeypatch.setattr(
        plss,
        "get_description_geometry",
        lambda *_: plss.ResolvedGeometry("section", (_plss_polygon(),)),
    )

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert LocationTag.CoordinatesFromPLSS("OR330270S0310E0") in loc.tags


def test_coordinate_provenance_lint_rechecks_nominatim_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates()
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromNominatim(
            "node", 1234, "place", use_bounding_box=False
        ),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    lookup = Mock(
        return_value=_nominatim_result(
            latitude="37.9", longitude="-122.1", osm_type="node", osm_id=1234
        )
    )
    monkeypatch.setattr(nominatim, "lookup", lookup)

    assert list(location_lint.check_coordinate_provenance(loc, LintConfig())) == []
    lookup.assert_called_once_with("node", 1234)


def test_coordinate_provenance_lint_updates_nominatim_category(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates()
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    old_tag = LocationTag.CoordinatesFromNominatim(
        "node", 1234, "place", use_bounding_box=False
    )
    loc.tags = (old_tag,)  # type: ignore[assignment]
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    result = _nominatim_result(
        latitude="37.9",
        longitude="-122.1",
        osm_type="node",
        osm_id=1234,
        category="natural",
    )
    monkeypatch.setattr(nominatim, "lookup", Mock(return_value=result))

    assert (
        list(location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.tags == (
        LocationTag.CoordinatesFromNominatim(
            "node", 1234, "natural", use_bounding_box=False
        ),
    )


def test_coordinate_provenance_lint_updates_categories_for_bbox_union(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates()
    loc.latitude = "37°N-39°N"
    loc.longitude = "123°W-121°W"
    loc.tags = (  # type: ignore[assignment]
        LocationTag.CoordinatesFromNominatim(
            "way", 1, "natural", use_bounding_box=True
        ),
        LocationTag.CoordinatesFromNominatim(
            "way", 2, "natural", use_bounding_box=True
        ),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    results = {
        ("way", 1): _nominatim_result(
            latitude="37.5",
            longitude="-122.5",
            osm_type="way",
            osm_id=1,
            category="leisure",
            bounding_box=("37", "38", "-123", "-122"),
        ),
        ("way", 2): _nominatim_result(
            latitude="38.5",
            longitude="-121.5",
            osm_type="way",
            osm_id=2,
            category="leisure",
            bounding_box=("38", "39", "-122", "-121"),
        ),
    }
    monkeypatch.setattr(
        nominatim,
        "lookup",
        Mock(side_effect=lambda osm_type, osm_id: results[osm_type, osm_id]),
    )

    assert (
        list(location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.tags == (
        LocationTag.CoordinatesFromNominatim(
            "way", 1, "leisure", use_bounding_box=True
        ),
        LocationTag.CoordinatesFromNominatim(
            "way", 2, "leisure", use_bounding_box=True
        ),
    )


def test_coordinate_provenance_lint_updates_category_but_reports_changed_extent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates()
    loc.latitude = "37.9°N"
    loc.longitude = "122.1°W"
    old_tag = LocationTag.CoordinatesFromNominatim(
        "node", 1234, "place", use_bounding_box=False
    )
    loc.tags = (old_tag,)  # type: ignore[assignment]
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    result = _nominatim_result(
        latitude="38",
        longitude="-122",
        osm_type="node",
        osm_id=1234,
        category="natural",
    )
    monkeypatch.setattr(nominatim, "lookup", Mock(return_value=result))

    messages = list(
        location_lint.check_coordinate_provenance(loc, LintConfig(autofix=True))
    )

    assert any("do not exactly match" in message for message in messages)
    assert loc.tags == (
        LocationTag.CoordinatesFromNominatim(
            "node", 1234, "natural", use_bounding_box=False
        ),
    )


def test_location_infers_coordinates_from_geonames(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    candidate = _geonames_candidate(geoname_id=5376890)
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[candidate]),
    )
    monkeypatch.setattr(nominatim, "search", Mock(return_value=[]))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_geonames_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude == "38.0615885°N"
    assert loc.longitude == "122.6985975°W"
    assert loc.tags == (LocationTag.CoordinatesFromGeoNames(5376890),)


def test_location_does_not_infer_conflicting_geonames_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Springfield", region=_marin_county())
    candidates = [
        _geonames_candidate(
            geoname_id=1, name="Springfield", latitude=38, longitude=-122
        ),
        _geonames_candidate(
            geoname_id=2, name="Springfield", latitude=39, longitude=-122
        ),
    ]
    monkeypatch.setattr(
        location_lint, "_get_geonames_coordinate_matches", Mock(return_value=candidates)
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_geonames_coordinates(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "2 conflicting usable exact matches in the assigned Region" in messages[0]
    assert "ID 1" in messages[0]
    assert "ID 2" in messages[0]
    assert loc.latitude is None
    assert loc.longitude is None


def test_geonames_inference_rejects_candidate_outside_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    candidate = _geonames_candidate(
        region_issues=("coordinates are outside Marin County",)
    )
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[candidate]),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert (
        list(location_lint.check_geonames_coordinates(loc, LintConfig(autofix=True)))
        == []
    )
    assert loc.latitude is None
    assert loc.longitude is None


@pytest.mark.parametrize(
    ("feature_class", "feature_code", "expected"),
    [
        ("P", "PPL", 1),
        ("S", "CAVE", 1),
        ("H", "SPNG", 1),
        ("H", "STM", 0),
        ("T", "MT", 1),
        ("T", "ISL", 0),
        ("R", "RDJCT", 1),
        ("R", "RD", 0),
        ("A", "ADM2", 0),
        ("L", "PRK", 0),
    ],
)
def test_geonames_point_feature_restriction(
    feature_class: str, feature_code: str, expected: int
) -> None:
    candidate = _geonames_candidate(
        feature_class=feature_class, feature_code=feature_code
    )

    assert location_lint._is_geonames_point_candidate(candidate) is bool(expected)


def test_nonpoint_geonames_feature_does_not_infer_or_check_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    loc.latitude = "37.9°N"
    loc.longitude = "122.5°W"
    stream = _geonames_candidate(feature_class="H", feature_code="STM")
    monkeypatch.setattr(
        location_lint, "_get_geonames_coordinate_matches", Mock(return_value=[stream])
    )

    assert (
        list(location_lint.check_geonames_coordinate_consistency(loc, LintConfig()))
        == []
    )


def test_coordinate_evidence_marks_nonpoint_geonames_feature_as_evidence_only(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    monkeypatch.setattr(nominatim, "search", Mock(return_value=[]))
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[_geonames_candidate(feature_class="H", feature_code="STM")]),
    )

    Location.coordinate_evidence(loc)

    assert "[evidence only; feature is not point-like] H/STM" in capsys.readouterr().out


def test_geonames_and_nominatim_compatible_coordinates_are_inferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[_geonames_candidate(latitude=38.061, longitude=-122.699)]),
    )
    monkeypatch.setattr(
        nominatim,
        "search",
        Mock(
            return_value=[
                _nominatim_result(latitude="38.0615885", longitude="-122.6985975")
            ]
        ),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda extent, region: ()
    )

    messages = list(
        location_lint.check_geonames_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude == "38.0615885°N"
    assert loc.longitude == "122.6985975°W"
    assert loc.tags == (
        LocationTag.CoordinatesFromNominatim(
            "node", 1, "place", use_bounding_box=False
        ),
    )


def test_geonames_does_not_override_ambiguous_nominatim_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[_geonames_candidate()]),
    )
    results = [
        _nominatim_result(latitude="38.0615885", longitude="-122.6985975"),
        _nominatim_result(
            latitude="38.5",
            longitude="-122.7",
            feature_type="village",
            display_name="Other Nicasio",
        ),
    ]
    monkeypatch.setattr(nominatim, "search", Mock(return_value=results))
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_geonames_coordinates(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "Nominatim returned 2 conflicting exact matches" in messages[0]
    assert loc.latitude is None
    assert loc.longitude is None


def test_geonames_and_nominatim_incompatible_coordinates_are_not_inferred(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[_geonames_candidate()]),
    )
    monkeypatch.setattr(
        nominatim,
        "search",
        Mock(return_value=[_nominatim_result(latitude="38.5", longitude="-122.7")]),
    )
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    messages = list(
        location_lint.check_geonames_coordinates(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "resolve to incompatible coordinate clusters" in messages[0]
    assert loc.latitude is None
    assert loc.longitude is None


def test_nonpoint_geonames_feature_does_not_block_nominatim_inference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[_geonames_candidate(feature_class="H", feature_code="STM")]),
    )
    monkeypatch.setattr(
        nominatim,
        "search",
        Mock(
            return_value=[
                _nominatim_result(latitude="38.0615885", longitude="-122.6985975")
            ]
        ),
    )
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


def test_geonames_administrative_codes_validate_assigned_region() -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    extent = _geonames_candidate().extent
    record = _geonames_candidate().match.record
    expected_codes = frozenset({("ADM2", "CA", "041")})

    assert (
        location_lint._get_geonames_region_issues(
            loc, extent, record, loc.region, expected_codes
        )
        == ()
    )


def test_geonames_administrative_codes_reject_wrong_region() -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    extent = _geonames_candidate().extent
    record = dataclasses.replace(_geonames_candidate().match.record, admin2_code="097")
    expected_codes = frozenset({("ADM2", "CA", "041")})

    issues = location_lint._get_geonames_region_issues(
        loc, extent, record, loc.region, expected_codes
    )

    assert len(issues) == 1
    assert "ADM2.CA.097" in issues[0]
    assert "assigned Region 'Marin County, California' (ADM2.CA.041)" in issues[0]


def test_geonames_candidate_is_rejected_without_subnational_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    country = _make_region("Exampleland", RegionKind.country)
    state = _make_region("Example State", RegionKind.state, country)
    loc = _location_without_coordinates(name="Example", region=state)
    candidate = _geonames_candidate()
    check_extent = Mock()
    monkeypatch.setattr(
        coordinate_lint,
        "_get_detailed_region_path",
        lambda region, found_country: (country, "country.json"),
    )
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", check_extent)

    issues = location_lint._get_geonames_region_issues(
        loc, candidate.extent, candidate.match.record, state, frozenset()
    )

    assert len(issues) == 1
    assert (
        "no matching GeoNames administrative unit or detailed region polygon"
        in issues[0]
    )
    check_extent.assert_not_called()


def test_location_coordinates_match_geonames_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    loc.latitude = "38.0616°N"
    loc.longitude = "122.6986°W"
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[_geonames_candidate(geoname_id=5376890)]),
    )

    assert (
        list(location_lint.check_geonames_coordinate_consistency(loc, LintConfig()))
        == []
    )


def test_location_coordinates_are_far_from_all_geonames_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio", region=_marin_county())
    loc.latitude = "37.9°N"
    loc.longitude = "122.5°W"
    candidates = [
        _geonames_candidate(geoname_id=5376890),
        _geonames_candidate(
            geoname_id=2,
            latitude=38.0601015,
            longitude=-122.6984323,
            feature_class="P",
            feature_code="PPLX",
        ),
    ]
    monkeypatch.setattr(
        location_lint, "_get_geonames_coordinate_matches", Mock(return_value=candidates)
    )

    messages = list(
        location_lint.check_geonames_coordinate_consistency(loc, LintConfig())
    )

    assert len(messages) == 1
    assert "more than 5 km from all 2 exact GeoNames matches" in messages[0]
    assert "ID 5376890" in messages[0]
    assert "ID 2" in messages[0]
    assert messages[0].count("km away") == 2


def test_geonames_consistency_reports_conflicting_points_masked_by_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Springfield", region=_marin_county())
    loc.latitude = "37°N-39°N"
    loc.longitude = "123°W-121°W"
    candidates = [
        _geonames_candidate(
            geoname_id=1, name="Springfield", latitude=38, longitude=-122.7
        ),
        _geonames_candidate(
            geoname_id=2, name="Springfield", latitude=38.8, longitude=-121.2
        ),
    ]
    monkeypatch.setattr(
        location_lint, "_get_geonames_coordinate_matches", Mock(return_value=candidates)
    )

    messages = list(
        location_lint.check_geonames_coordinate_consistency(loc, LintConfig())
    )

    assert len(messages) == 1
    assert "may mask conflated homonyms" in messages[0]
    assert "2 conflicting point-like GeoNames exact matches" in messages[0]
    assert "ID 1" in messages[0]
    assert "ID 2" in messages[0]


def test_geonames_search_uses_country_and_applies_locality_offset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Nicasio: 2 km N", region=_marin_county())
    match = _geonames_candidate(geoname_id=5376890).match
    search = Mock(return_value=[match])
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        _REAL_GET_GEONAMES_COORDINATE_MATCHES,
    )
    monkeypatch.setattr(location_lint, "_get_geonames_country_code", lambda _: "US")
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_region_code_prefixes",
        lambda country_code, region_names, parent_code_prefixes=(): frozenset(
            {("ADM2", "CA", "041")}
        ),
    )
    monkeypatch.setattr(geonames, "search_name", search)
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", Mock(return_value=())
    )

    candidates = location_lint._get_geonames_coordinate_matches(loc)

    assert len(candidates) == 1
    assert candidates[0].is_accepted
    assert candidates[0].latitude != "38.0615885°N"
    assert coordinate_lint.extent_distance_km(
        candidates[0].extent, _geonames_candidate().extent
    ) == pytest.approx(2, rel=0.01)
    search.assert_called_once_with("Nicasio", country_code="US", limit=100)


def test_geonames_country_code_prefers_direct_country_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    match = _geonames_candidate(
        name="Martinique", feature_class="A", feature_code="PCLD"
    ).match
    match = geonames.GeoNamesMatch(
        dataclasses.replace(match.record, country_code="MQ"),
        match.matched_name,
        match.match_kind,
    )
    search = Mock(return_value=[match])
    monkeypatch.setattr(geonames, "search_name", search)
    location_lint._get_geonames_country_code.cache_clear()

    assert location_lint._get_geonames_country_code("Martinique") == "MQ"
    search.assert_called_once_with(
        "Martinique",
        feature_classes=("A",),
        feature_codes=location_lint._GEONAMES_COUNTRY_FEATURE_CODES,
        limit=10,
    )
    location_lint._get_geonames_country_code.cache_clear()


def test_geonames_region_codes_prefer_qualified_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    match = _geonames_candidate(
        name="Marin County", feature_class="A", feature_code="ADM2"
    ).match
    search = Mock(
        side_effect=lambda name, **kwargs: [match] if name == "Marin County" else []
    )
    monkeypatch.setattr(geonames, "search_name", search)
    location_lint._get_geonames_region_code_prefixes.cache_clear()

    codes = location_lint._get_geonames_region_code_prefixes(
        "US", ("Marin", "Marin County")
    )

    assert codes == frozenset({("ADM2", "CA", "041")})
    assert search.call_args_list == [
        call(
            "Marin County",
            country_code="US",
            feature_classes=("A",),
            feature_codes=location_lint._GEONAMES_ADMIN_FEATURE_CODES,
            limit=100,
        )
    ]
    location_lint._get_geonames_region_code_prefixes.cache_clear()


def test_geonames_region_codes_use_parent_administrative_unit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def administrative_match(
        name: str, feature_code: str, admin1_code: str, admin2_code: str = ""
    ) -> geonames.GeoNamesMatch:
        match = _geonames_candidate(
            name=name, feature_class="A", feature_code=feature_code
        ).match
        return geonames.GeoNamesMatch(
            dataclasses.replace(
                match.record, admin1_code=admin1_code, admin2_code=admin2_code
            ),
            match.matched_name,
            match.match_kind,
        )

    oklahoma = administrative_match("Oklahoma", "ADM1", "OK")
    mississippi_county = administrative_match("Leflore County", "ADM2", "MS", "083")
    oklahoma_county = administrative_match("LeFlore County", "ADM2", "OK", "079")
    search = Mock(
        side_effect=lambda name, **kwargs: {
            "Oklahoma": [oklahoma],
            "Le Flore County": [mississippi_county],
            "Le Flore": [oklahoma_county, mississippi_county],
        }.get(name, [])
    )
    monkeypatch.setattr(geonames, "search_name", search)
    location_lint._get_geonames_region_code_prefixes.cache_clear()
    country = _make_region("United States", RegionKind.country)
    state = _make_region("Oklahoma", RegionKind.state, country)
    county = _make_region("Le Flore County, Oklahoma", RegionKind.county, state)

    codes = location_lint._get_geonames_region_code_prefixes_for_region("US", county)

    assert codes == frozenset({("ADM2", "OK", "079")})
    assert any(call.args[0] == "Le Flore County" for call in search.call_args_list)
    assert any(call.args[0] == "Le Flore" for call in search.call_args_list)
    location_lint._get_geonames_region_code_prefixes.cache_clear()


def test_general_location_does_not_load_linked_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="California")
    search = Mock()
    monkeypatch.setattr(nominatim, "search", search)
    monkeypatch.setattr(model_lint, "is_network_available", lambda: True)

    assert list(location_lint.check_linked_coordinates(loc, LintConfig())) == []
    assert list(location_lint.check_geonames_coordinates(loc, LintConfig())) == []
    assert list(location_lint.check_nominatim_coordinates(loc, LintConfig())) == []
    assert (
        list(location_lint.check_nominatim_general_coordinates(loc, LintConfig())) == []
    )
    loc.latitude = "38°N"
    loc.longitude = "122°W"
    assert (
        list(location_lint.check_geonames_coordinate_consistency(loc, LintConfig()))
        == []
    )
    assert (
        list(location_lint.check_nominatim_coordinate_consistency(loc, LintConfig()))
        == []
    )
    search.assert_not_called()


def test_non_recent_location_infers_geonames_coordinates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oligocene = SimpleNamespace(name="Oligocene")
    loc = _location_without_coordinates(
        name="Nicasio",
        region=_marin_county(),
        min_period=oligocene,
        max_period=oligocene,
    )
    candidate = _geonames_candidate(geoname_id=5376890)
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[candidate]),
    )
    monkeypatch.setattr(nominatim, "search", Mock(return_value=[]))

    messages = list(
        location_lint.check_geonames_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude == "38.0615885°N"
    assert loc.longitude == "122.6985975°W"
    assert loc.tags == (LocationTag.CoordinatesFromGeoNames(5376890),)


def test_non_recent_location_checks_geonames_coordinate_consistency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oligocene = SimpleNamespace(name="Oligocene")
    loc = _location_without_coordinates(
        name="Nicasio",
        region=_marin_county(),
        min_period=oligocene,
        max_period=oligocene,
    )
    loc.latitude = "40°N"
    loc.longitude = "100°W"
    monkeypatch.setattr(
        location_lint,
        "_get_geonames_coordinate_matches",
        Mock(return_value=[_geonames_candidate()]),
    )

    messages = list(
        location_lint.check_geonames_coordinate_consistency(loc, LintConfig())
    )

    assert len(messages) == 1
    assert "more than 5 km from all" in messages[0]


def test_non_general_significant_coordinate_range_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    linked_name = _tagged_object(
        (TypeTag.Coordinates("0.5°N", "0.5°E"),), name_tags=True
    )
    loc = _location_without_coordinates(
        name="Broad site", min_period=recent, max_period=recent, names=(linked_name,)
    )
    loc.latitude = "0°N-1°N"
    loc.longitude = "0°E-1°E"
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", lambda *args, **kwargs: ()
    )

    messages = list(location_lint.check_coordinates(loc, LintConfig()))

    assert len(messages) == 1
    assert (
        "non-General location has a coordinate range with 78.6 km radius" in messages[0]
    )
    assert "contains more precise linked evidence" in messages[0]
    assert "0.5°N, 0.5°E" in messages[0]


def test_unplaced_significant_coordinate_range_is_not_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Uncertain site", min_period=recent, max_period=recent
    )
    loc.latitude = "0°N-1°N"
    loc.longitude = "0°E-1°E"
    loc.tags = (LocationTag.Unplaced(comment="uncertain"),)  # type: ignore[assignment]
    check_region = Mock(return_value=())
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", check_region)

    assert list(location_lint.check_coordinates(loc, LintConfig())) == []
    assert check_region.call_args.kwargs == {"require_full_containment": False}


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
    assert loc.tags == (
        LocationTag.CoordinatesFromNominatim(
            "relation", 1, "place", use_bounding_box=True
        ),
    )
    search.assert_called_once_with(
        "Nicobar Islands, Andaman and Nicobar Islands, India"
    )


def test_general_location_infers_nominatim_water_lake_bounding_box(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kenya = _make_region("Kenya", RegionKind.country)
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Lake Naivasha",
        region=kenya,
        min_period=recent,
        max_period=recent,
        general=True,
    )
    result = _nominatim_result(
        latitude="-0.7593017",
        longitude="36.3631459",
        name="Lake Naivasha",
        category="water",
        feature_type="lake",
        state="Nakuru",
        country="Kenya",
        display_name="Lake Naivasha, Naivasha, Nakuru, 20117, Kenya",
        osm_type="relation",
        bounding_box=("-0.8293959", "-0.6880452", "36.2836586", "36.4283279"),
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
    assert loc.latitude == "0.8293959°S-0.6880452°S"
    assert loc.longitude == "36.2836586°E-36.4283279°E"
    search.assert_called_once_with("Lake Naivasha, Kenya")


def test_general_location_prefers_bbox_containing_all_exact_matches(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Example River", min_period=recent, max_period=recent, general=True
    )
    broad_river = _nominatim_result(
        latitude="0.5",
        longitude="0.5",
        name="Example River",
        category="waterway",
        feature_type="river",
        display_name="Example River relation",
        osm_type="relation",
        bounding_box=("0", "1", "0", "1"),
    )
    small_river = _nominatim_result(
        latitude="0.35",
        longitude="0.35",
        name="Example River",
        category="waterway",
        feature_type="river",
        display_name="Example River way",
        osm_type="way",
        bounding_box=("0.2", "0.5", "0.2", "0.5"),
    )
    small_islet = _nominatim_result(
        latitude="0.305",
        longitude="0.305",
        name="Example River",
        category="place",
        feature_type="islet",
        display_name="Example River islet",
        osm_type="way",
        bounding_box=("0.3", "0.31", "0.3", "0.31"),
    )
    monkeypatch.setattr(
        nominatim, "search", Mock(return_value=[small_islet, broad_river, small_river])
    )
    monkeypatch.setattr(
        coordinate_lint, "check_extent_in_region", Mock(return_value=())
    )

    messages = list(
        location_lint.check_nominatim_general_coordinates(loc, LintConfig(autofix=True))
    )

    assert messages == []
    assert loc.latitude == "0°N-1°N"
    assert loc.longitude == "0°E-1°E"
    assert "contains all other exact-match bounds" in capsys.readouterr().out


def test_general_location_retains_conflicting_broad_river_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Example River", min_period=recent, max_period=recent, general=True
    )
    results = [
        _nominatim_result(
            latitude=str(offset + 0.5),
            longitude=str(offset + 0.5),
            name="Example River",
            category="waterway",
            feature_type="river",
            display_name=f"Example River relation {offset}",
            osm_type="relation",
            bounding_box=(str(offset), str(offset + 1), str(offset), str(offset + 1)),
        )
        for offset in (0, 2)
    ]
    monkeypatch.setattr(nominatim, "search", Mock(return_value=results))

    messages = list(
        location_lint.check_nominatim_general_coordinates(loc, LintConfig())
    )

    assert len(messages) == 1
    assert "2 conflicting exact-match bounding boxes" in messages[0]


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


def test_possible_general_location_reports_broad_nominatim_feature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Example River", min_period=recent, max_period=recent
    )
    result = _nominatim_result(
        latitude="0.5",
        longitude="0.5",
        name="Example River",
        category="waterway",
        feature_type="river",
        display_name="Example River, California, United States",
        osm_type="relation",
        bounding_box=("0", "1", "0", "1"),
    )
    parsed = location_lint.get_nominatim_result_bounding_box(result)
    assert parsed is not None
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_bounding_box_candidates",
        Mock(return_value=[(result, parsed)]),
    )

    messages = list(
        location_lint.check_possible_general_location(loc, LintConfig(autofix=True))
    )

    assert len(messages) == 1
    assert "should be reviewed for the General tag" in messages[0]
    assert "relation waterway/river" in messages[0]
    assert "78.6 km bounding-box radius" in messages[0]
    assert loc.tags == ()


@pytest.mark.parametrize(
    ("latitude", "longitude", "expected_count"),
    [("0.5°N", "0.5°E", 1), ("10°N", "10°E", 0)],
)
def test_possible_general_location_matches_existing_coordinates(
    monkeypatch: pytest.MonkeyPatch, latitude: str, longitude: str, expected_count: int
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Example River", min_period=recent, max_period=recent
    )
    loc.latitude = latitude
    loc.longitude = longitude
    result = _nominatim_result(
        latitude="0.5",
        longitude="0.5",
        name="Example River",
        category="waterway",
        feature_type="river",
        display_name="Example River, California, United States",
        osm_type="relation",
        bounding_box=("0", "1", "0", "1"),
    )
    parsed = location_lint.get_nominatim_result_bounding_box(result)
    assert parsed is not None
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_bounding_box_candidates",
        Mock(return_value=[(result, parsed)]),
    )

    messages = list(location_lint.check_possible_general_location(loc, LintConfig()))

    assert len(messages) == expected_count


def test_possible_general_location_accepts_named_administrative_area(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Example State", min_period=recent, max_period=recent
    )
    result = _nominatim_result(
        latitude="0.5",
        longitude="0.5",
        name="Example State",
        category="boundary",
        feature_type="administrative",
        display_name="Example State, United States",
        osm_type="relation",
        bounding_box=("0", "1", "0", "1"),
    )
    parsed = location_lint.get_nominatim_result_bounding_box(result)
    assert parsed is not None
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_bounding_box_candidates",
        Mock(return_value=[(result, parsed)]),
    )
    monkeypatch.setattr(
        location_lint, "_get_nominatim_coordinate_candidates", Mock(return_value=[])
    )

    messages = list(location_lint.check_possible_general_location(loc, LintConfig()))

    assert len(messages) == 1
    assert "boundary/administrative" in messages[0]


def test_possible_general_location_rejects_settlement_administrative_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Example State", min_period=recent, max_period=recent
    )
    boundary = _nominatim_result(
        latitude="0.5",
        longitude="0.5",
        name="Example State",
        category="boundary",
        feature_type="administrative",
        display_name="Example State boundary, United States",
        osm_type="relation",
        bounding_box=("0", "1", "0", "1"),
    )
    settlement = _nominatim_result(
        latitude="0.5",
        longitude="0.5",
        name="Example State",
        category="place",
        feature_type="town",
        display_name="Example State town, United States",
        osm_type="node",
    )
    parsed = location_lint.get_nominatim_result_bounding_box(boundary)
    assert parsed is not None
    settlement_coordinates = location_lint.get_nominatim_result_coordinates(settlement)
    assert settlement_coordinates is not None
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_bounding_box_candidates",
        Mock(return_value=[(boundary, parsed)]),
    )
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_coordinate_candidates",
        Mock(return_value=[(settlement, settlement_coordinates)]),
    )

    assert list(location_lint.check_possible_general_location(loc, LintConfig())) == []


@pytest.mark.parametrize(
    ("name", "category", "feature_type", "bounding_box"),
    [
        ("Nicasio", "boundary", "administrative", ("0", "1", "0", "1")),
        ("Tiny Island", "place", "island", ("0", "0.01", "0", "0.01")),
        ("Small Island", "place", "island", ("0", "0.12", "0", "0.12")),
    ],
)
def test_possible_general_location_rejects_weak_nominatim_candidates(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    category: str,
    feature_type: str,
    bounding_box: tuple[str, str, str, str],
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(name=name, min_period=recent, max_period=recent)
    result = _nominatim_result(
        latitude="0",
        longitude="0",
        name=name,
        category=category,
        feature_type=feature_type,
        display_name=f"{name}, California, United States",
        osm_type="relation",
        bounding_box=bounding_box,
    )
    parsed = location_lint.get_nominatim_result_bounding_box(result)
    assert parsed is not None
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_bounding_box_candidates",
        Mock(return_value=[(result, parsed)]),
    )

    assert list(location_lint.check_possible_general_location(loc, LintConfig())) == []


def test_possible_general_location_skips_offset_locality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    loc = _location_without_coordinates(
        name="Example River: 2 km N", min_period=recent, max_period=recent
    )
    search = Mock()
    monkeypatch.setattr(location_lint, "_get_nominatim_bounding_box_candidates", search)

    assert list(location_lint.check_possible_general_location(loc, LintConfig())) == []
    search.assert_not_called()


def test_possible_general_location_reports_linked_coordinate_spread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = SimpleNamespace(name="Recent")
    names = (
        _tagged_object((TypeTag.Coordinates("0°N", "0°E"),), name_tags=True),
        _tagged_object((TypeTag.Coordinates("1°N", "0°E"),), name_tags=True),
    )
    loc = _location_without_coordinates(
        name="Example Island", min_period=recent, max_period=recent, names=names
    )
    result = _nominatim_result(
        latitude="0.5",
        longitude="0.5",
        name="Example Island",
        feature_type="island",
        display_name="Example Island, California, United States",
        osm_type="relation",
        bounding_box=("0", "1", "0", "1"),
    )
    parsed = location_lint.get_nominatim_result_bounding_box(result)
    assert parsed is not None
    monkeypatch.setattr(
        location_lint,
        "_get_nominatim_bounding_box_candidates",
        Mock(return_value=[(result, parsed)]),
    )

    messages = list(location_lint.check_possible_general_location(loc, LintConfig()))

    assert len(messages) == 1
    assert "Linked coordinate evidence also spans 111.2 km" in messages[0]


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


def test_forward_geocoding_accepts_likely_spelling_variant() -> None:
    country = _make_region("Philippines", RegionKind.country)
    location = _location_without_coordinates(name="Isabella", region=country)
    result = nominatim.SearchResult(
        latitude="6.7053597",
        longitude="121.9710604",
        name="Isabela",
        display_name="Isabela, Basilan, Philippines",
        category="place",
        feature_type="city",
        address={"city": "Isabela", "state": "Basilan", "country": "Philippines"},
    )

    assessment = location_lint.assess_nominatim_result(location, result)

    assert assessment.is_accepted
    assert assessment.issues == ()
    assert assessment.notes == (
        "locality spelling differs but is a likely variant: expected 'Isabella', "
        "got 'Isabela'",
    )


def test_forward_geocoding_rejects_unrelated_name_with_specific_reason() -> None:
    country = _make_region("Philippines", RegionKind.country)
    location = _location_without_coordinates(name="Isabella", region=country)
    result = nominatim.SearchResult(
        latitude="10.2048",
        longitude="122.9888",
        name="Other place",
        display_name="Other place, Philippines",
        category="place",
        feature_type="town",
        address={"town": "Other place", "country": "Philippines"},
    )

    assessment = location_lint.assess_nominatim_result(location, result)

    assert not assessment.is_accepted
    assert assessment.issues == (
        "locality name mismatch: expected 'Isabella', got 'Other place'",
    )
    assert assessment.notes == ()


def test_forward_geocoding_does_not_treat_feature_suffix_as_spelling_variant() -> None:
    country = _make_region("Mexico", RegionKind.country)
    location = _location_without_coordinates(name="Cozumel", region=country)
    result = nominatim.SearchResult(
        latitude="20.43",
        longitude="-86.92",
        name="Cozumel Island",
        display_name="Cozumel Island, Quintana Roo, Mexico",
        category="place",
        feature_type="island",
        address={"state": "Quintana Roo", "country": "Mexico"},
    )

    assessment = location_lint.assess_nominatim_result(location, result)

    assert not assessment.is_accepted
    assert assessment.issues == (
        "locality name mismatch: expected 'Cozumel', got 'Cozumel Island'",
    )


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


def test_location_disambiguator_lint_accepts_nearby_region_for_modifier() -> None:
    netherlands = _make_region("Netherlands", RegionKind.country)
    belgium = _make_region("Belgium", RegionKind.country)
    loc = _location_without_coordinates(
        name="Maastricht (Netherlands): 20 km W", region=belgium
    )
    loc.tags = (LocationTag.NearbyRegion(netherlands),)  # type: ignore[assignment]

    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


def test_location_disambiguator_lint_rejects_nearby_region_without_modifier() -> None:
    netherlands = _make_region("Netherlands", RegionKind.country)
    belgium = _make_region("Belgium", RegionKind.country)
    loc = _location_without_coordinates(name="Maastricht (Netherlands)", region=belgium)
    loc.tags = (LocationTag.NearbyRegion(netherlands),)  # type: ignore[assignment]

    messages = list(location_lint.check_disambiguator(loc, LintConfig()))

    assert len(messages) == 1
    assert "disambiguator 'Netherlands' is not an enclosing Region" in messages[0]


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
    assert "location name should be" not in messages[0]


def test_location_disambiguator_lint_accepts_modifier() -> None:
    loc = _location_without_coordinates(name="Lomas Cantadas: upper")

    assert list(location_lint.check_disambiguator(loc, LintConfig())) == []


def test_location_disambiguator_lint_does_not_propose_or_autofix_modifier() -> None:
    loc = _location_without_coordinates(name="Lomas Cantadas (upper)")

    messages = list(location_lint.check_disambiguator(loc, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "location name should be" not in messages[0]
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


def test_virtual_copy_does_not_collide_with_its_persisted_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    region = _make_region("Sabah", RegionKind.state)
    original = _location_without_coordinates(
        id=10, name="Mount Kinabalu", region=region
    )
    virtual = _location_without_coordinates(
        id=-1, name="Mount Kinabalu", region=region, virtual_origin_id=10
    )
    monkeypatch.setattr(
        location_lint,
        "_get_base_name_to_locations",
        lambda: {"Mount Kinabalu": (original, virtual)},
    )

    assert (
        list(location_lint.check_should_have_disambiguator(virtual, LintConfig())) == []
    )


def test_nearby_region_modifier_does_not_require_disambiguators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    netherlands = _make_region("Netherlands", RegionKind.country)
    limburg = _make_region("Limburg", RegionKind.province, netherlands)
    belgium = _make_region("Belgium", RegionKind.country)
    base = _location_without_coordinates(id=1, name="Maastricht", region=limburg)
    modified = _location_without_coordinates(
        id=2, name="Maastricht: 20 km W", region=belgium
    )
    modified.tags = (LocationTag.NearbyRegion(netherlands),)  # type: ignore[assignment]
    monkeypatch.setattr(
        location_lint,
        "_get_base_name_to_locations",
        lambda: {"Maastricht": (base, modified)},
    )

    assert list(location_lint.check_should_have_disambiguator(base, LintConfig())) == []
    assert (
        list(location_lint.check_should_have_disambiguator(modified, LintConfig()))
        == []
    )


def test_nearby_region_name_is_primary_despite_other_disambiguated_locations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    united_states = _make_region("United States", RegionKind.country)
    florida = _make_region("Florida", RegionKind.state, united_states)
    atlantic_ocean = _make_region("Atlantic Ocean", RegionKind.sea)
    coastal_waters = _location_without_coordinates(
        id=1,
        name="Florida: eastern coastal waters",
        region=atlantic_ocean,
        general=True,
    )
    coastal_waters.tags = (  # type: ignore[assignment]
        LocationTag.General,
        LocationTag.NearbyRegion(florida),
    )
    solomon_islands = _location_without_coordinates(
        id=2,
        name="Florida (Solomon Islands)",
        region=_make_region("Solomon Islands", RegionKind.country),
    )
    colorado = _location_without_coordinates(
        id=3,
        name="Florida (La Plata County, Colorado)",
        region=_make_region("La Plata County, Colorado", RegionKind.county),
    )
    monkeypatch.setattr(
        location_lint,
        "_get_base_name_to_locations",
        lambda: {"Florida": (coastal_waters, solomon_islands, colorado)},
    )

    assert (
        list(
            location_lint.check_should_have_disambiguator(coastal_waters, LintConfig())
        )
        == []
    )


def test_untagged_cross_region_modifier_still_requires_disambiguators(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    netherlands = _make_region("Netherlands", RegionKind.country)
    belgium = _make_region("Belgium", RegionKind.country)
    base = _location_without_coordinates(id=1, name="Maastricht", region=netherlands)
    modified = _location_without_coordinates(
        id=2, name="Maastricht: 20 km W", region=belgium
    )
    monkeypatch.setattr(
        location_lint,
        "_get_base_name_to_locations",
        lambda: {"Maastricht": (base, modified)},
    )

    base_messages = list(
        location_lint.check_should_have_disambiguator(base, LintConfig())
    )
    modified_messages = list(
        location_lint.check_should_have_disambiguator(modified, LintConfig())
    )

    assert len(base_messages) == 1
    assert "Maastricht (Netherlands)" in base_messages[0]
    assert len(modified_messages) == 1
    assert "Maastricht (Belgium): 20 km W" in modified_messages[0]


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


def test_location_does_not_choose_place_over_distant_administrative_boundaries(
    monkeypatch: pytest.MonkeyPatch,
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

    assert len(messages) == 1
    assert "returned 3 conflicting exact matches" in messages[0]
    assert loc.latitude is None
    assert loc.longitude is None


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


def test_nominatim_consistency_reports_conflicting_places_masked_by_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Springfield", region=_marin_county())
    loc.latitude = "37°N-39°N"
    loc.longitude = "123°W-121°W"
    results = [
        _nominatim_result(
            latitude="38",
            longitude="-122.7",
            name="Springfield",
            feature_type="village",
            display_name="Springfield village 1",
        ),
        _nominatim_result(
            latitude="38.8",
            longitude="-121.2",
            name="Springfield",
            feature_type="town",
            display_name="Springfield town 2",
        ),
    ]
    monkeypatch.setattr(nominatim, "search", Mock(return_value=results))

    messages = list(
        location_lint.check_nominatim_coordinate_consistency(loc, LintConfig())
    )

    assert len(messages) == 1
    assert "may mask conflated homonyms" in messages[0]
    assert "2 conflicting non-linear Nominatim exact matches" in messages[0]
    assert all(result.display_name in messages[0] for result in results)


def test_nominatim_masked_homonym_check_ignores_river_segments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loc = _location_without_coordinates(name="Example River", region=_marin_county())
    loc.latitude = "37°N-39°N"
    loc.longitude = "123°W-121°W"
    results = [
        _nominatim_result(
            latitude="38",
            longitude="-122.7",
            name="Example River",
            category="waterway",
            feature_type="river",
            display_name="Example River segment 1",
        ),
        _nominatim_result(
            latitude="38.8",
            longitude="-121.2",
            name="Example River",
            category="waterway",
            feature_type="river",
            display_name="Example River segment 2",
        ),
    ]
    monkeypatch.setattr(nominatim, "search", Mock(return_value=results))

    assert (
        list(location_lint.check_nominatim_coordinate_consistency(loc, LintConfig()))
        == []
    )


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


@pytest.mark.parametrize(
    "label",
    [
        "geonames_coordinates",
        "nominatim_coordinates",
        "nominatim_coordinate_consistency",
        "possible_general_location",
    ],
)
def test_coordinate_lints_are_normal_network_lints(label: str) -> None:
    wrapper = next(
        wrapper for wrapper in location_lint.LINT.linters if wrapper.label == label
    )

    assert wrapper.requires_network
    assert wrapper not in location_lint.LINT.disabled_linters


def test_geonames_consistency_lint_is_a_normal_offline_lint() -> None:
    wrapper = next(
        wrapper
        for wrapper in location_lint.LINT.linters
        if wrapper.label == "geonames_coordinate_consistency"
    )

    assert not wrapper.requires_network
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

    assert loc.tags == ()
