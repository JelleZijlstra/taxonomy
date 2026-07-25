from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from taxonomy.db import coordinate_lint, models
from taxonomy.db.constants import AgeClass, OccurrenceValidity, SpecimenOrgan
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.location import Location

from .lint import (
    check_coordinates,
    check_location_detail_coordinates,
    check_type_locality_age,
    check_type_locality_distribution_rules,
    check_type_locality_validity,
    parse_date,
)
from .name import Name, TypeTag


def test_parse_date() -> None:
    assert parse_date("Feb 2013") == "2013-02"
    assert parse_date("1 Feb 2013") == "2013-02-01"
    assert parse_date("23 Feb 2013") == "2013-02-23"
    assert parse_date("July 2013") == "2013-07"
    assert parse_date("7 July 2013") == "2013-07-07"


def _name_with_coordinates(
    name_coordinates: tuple[str, str], location_coordinates: tuple[str, str]
) -> Name:
    tag = TypeTag.Coordinates(*name_coordinates)
    location = SimpleNamespace(
        latitude=location_coordinates[0],
        longitude=location_coordinates[1],
        region=object(),
    )
    name = SimpleNamespace(
        type_locality=location,
        type_tags=(tag,),
        get_tags=lambda tags, tag_type: (
            candidate for candidate in tags if isinstance(candidate, tag_type)
        ),
    )
    return cast(Name, name)


def test_name_coordinates_allow_five_kilometres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _name_with_coordinates(("40°30'N", "74°15'W"), ("40°31'N", "74°15'W"))
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", lambda *_: ())

    assert list(check_coordinates(name, LintConfig())) == []


def test_name_coordinates_must_match_location(monkeypatch: pytest.MonkeyPatch) -> None:
    name = _name_with_coordinates(("40°30'N", "74°15'W"), ("41°N", "74°15'W"))
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", lambda *_: ())

    messages = list(check_coordinates(name, LintConfig()))

    assert len(messages) == 1
    assert "55.6 km from Location" in messages[0]


def test_name_coordinate_range_matches_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = _name_with_coordinates(("40°N-41°N", "75°W-74°W"), ("40.5°N", "74.5°W"))
    monkeypatch.setattr(coordinate_lint, "check_extent_in_region", lambda *_: ())

    assert list(check_coordinates(name, LintConfig())) == []


def _name_with_location_details(
    texts: tuple[str, ...],
    *,
    location_coordinates: tuple[str | None, str | None] | None,
) -> Name:
    source = cast(models.Article, object())
    tags = tuple(TypeTag.LocationDetail(text, source) for text in texts)
    if location_coordinates is None:
        location = None
    else:
        location = SimpleNamespace(
            latitude=location_coordinates[0], longitude=location_coordinates[1]
        )
    return cast(
        Name,
        SimpleNamespace(
            type_locality=location,
            type_tags=tags,
            get_tags=lambda values, tag_type: (
                tag for tag in values if isinstance(tag, tag_type)
            ),
        ),
    )


def test_location_detail_does_not_duplicate_location_coordinates() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.",), location_coordinates=("40.5°N", "74.25°W")
    )

    assert list(check_location_detail_coordinates(name, LintConfig(autofix=True))) == []
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_location_detail_coordinates_must_match_location() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.",), location_coordinates=("41°N", "74.25°W")
    )

    messages = list(check_location_detail_coordinates(name, LintConfig()))

    assert len(messages) == 1
    assert "55.6 km from Location" in messages[0]


def test_location_detail_infers_coordinates_when_location_has_none() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.",), location_coordinates=(None, None)
    )

    assert list(check_location_detail_coordinates(name, LintConfig(autofix=True))) == []
    assert TypeTag.Coordinates("40°30'N", "74°15'W") in name.type_tags


def test_conflicting_location_detail_coordinates_are_not_inferred() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.", "Locality reported as 41°N, 74°15'W."),
        location_coordinates=(None, None),
    )

    messages = list(check_location_detail_coordinates(name, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "cannot infer Coordinates tag" in messages[0]
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_distinct_nearby_location_detail_coordinates_are_not_combined() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.", "Locality reported as 40°31'N, 74°15'W."),
        location_coordinates=(None, None),
    )

    messages = list(check_location_detail_coordinates(name, LintConfig(autofix=True)))

    assert len(messages) == 1
    assert "multiple coordinate pairs within 5 km" in messages[0]
    assert not any(isinstance(tag, TypeTag.Coordinates) for tag in name.type_tags)


def test_location_detail_coordinates_must_match_existing_tag() -> None:
    name = _name_with_location_details(
        ("Collected at 40°30'N, 74°15'W.",), location_coordinates=(None, None)
    )
    name.type_tags = [*name.type_tags, TypeTag.Coordinates("41°N", "74°15'W")]  # type: ignore[assignment]

    messages = list(check_location_detail_coordinates(name, LintConfig()))

    assert len(messages) == 1
    assert "conflict with all Coordinates tags" in messages[0]


def _location_with_age(period_name: str, youngest_age: int) -> Location:
    period = SimpleNamespace(name=period_name, get_min_age=lambda: youngest_age)
    return cast(
        Location, SimpleNamespace(min_period=period, max_period=period, min_age=None)
    )


def _name_with_age(
    taxon_age: AgeClass, location: Location, *, organs: tuple[SpecimenOrgan, ...] = ()
) -> Name:
    tags = tuple(TypeTag.Organ(organ) for organ in organs)
    name = SimpleNamespace(
        type_locality=location,
        taxon=SimpleNamespace(age=taxon_age),
        type_tags=tags,
        get_tags=lambda tags, tag_type: (
            tag for tag in tags if isinstance(tag, tag_type)
        ),
    )
    return cast(Name, name)


def test_name_recent_type_locality_requires_recent_taxon() -> None:
    location = _location_with_age("Recent", 0)

    assert (
        list(
            check_type_locality_age(
                _name_with_age(AgeClass.extant, location), LintConfig()
            )
        )
        == []
    )
    messages = list(
        check_type_locality_age(_name_with_age(AgeClass.fossil, location), LintConfig())
    )

    assert len(messages) == 1
    assert "is Recent" in messages[0]
    assert "has age fossil" in messages[0]


@pytest.mark.parametrize("organ", [SpecimenOrgan.skin, SpecimenOrgan.in_alcohol])
def test_name_recent_organ_conflicts_with_fossil_locality(organ: SpecimenOrgan) -> None:
    name = _name_with_age(
        AgeClass.extant, _location_with_age("Pleistocene", 11_700), organs=(organ,)
    )

    messages = list(check_type_locality_age(name, LintConfig()))

    assert len(messages) == 1
    assert organ.name.replace("_", " ") in messages[0]
    assert "indicate a Recent type specimen" in messages[0]


def test_name_extant_taxon_may_have_pleistocene_type_locality() -> None:
    name = _name_with_age(AgeClass.extant, _location_with_age("Pleistocene", 11_700))

    assert list(check_type_locality_age(name, LintConfig())) == []


def test_name_mixed_recent_fossil_locality_is_left_alone() -> None:
    recent = SimpleNamespace(name="Recent", get_min_age=lambda: 0)
    pleistocene = SimpleNamespace(name="Pleistocene", get_min_age=lambda: 11_700)
    location = cast(
        Location,
        SimpleNamespace(min_period=recent, max_period=pleistocene, min_age=None),
    )
    name = _name_with_age(AgeClass.extant, location, organs=(SpecimenOrgan.skin,))

    assert list(check_type_locality_age(name, LintConfig())) == []


def test_name_extant_taxon_conflicts_with_pre_pleistocene_type_locality() -> None:
    name = _name_with_age(
        AgeClass.recently_extinct, _location_with_age("Pliocene", 2_590_000)
    )

    messages = list(check_type_locality_age(name, LintConfig()))

    assert len(messages) == 1
    assert "is pre-Pleistocene" in messages[0]
    assert "recently extinct taxon" in messages[0]


def test_name_fossil_taxon_allows_pre_pleistocene_type_locality() -> None:
    name = _name_with_age(AgeClass.fossil, _location_with_age("Pliocene", 2_590_000))

    assert list(check_type_locality_age(name, LintConfig())) == []


def _tagged_name(tags: tuple[object, ...]) -> Name:
    name = SimpleNamespace(type_tags=tags)
    name.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    return cast(Name, name)


def test_type_locality_validity_rejects_valid_tag() -> None:
    name = _tagged_name((TypeTag.TypeLocalityValidity(OccurrenceValidity.valid),))

    messages = list(check_type_locality_validity(name, LintConfig()))

    assert len(messages) == 1
    assert "only allows occurrence_dubious or classification_dubious" in messages[0]


def test_redirect_rule_flags_name_type_locality() -> None:
    region = SimpleNamespace()
    region.all_parents = lambda: iter(())
    target = SimpleNamespace()
    source = SimpleNamespace()
    rule = models.tags.TaxonTag.RedirectOccurrences(region, target, source)
    taxon = SimpleNamespace(tags=(rule,))
    taxon.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )
    name = SimpleNamespace(
        type_locality=SimpleNamespace(region=region), taxon=taxon, type_tags=()
    )
    name.get_tags = lambda values, tag_type: (
        tag for tag in values if isinstance(tag, tag_type)
    )

    messages = list(
        check_type_locality_distribution_rules(cast(Name, name), LintConfig())
    )

    assert len(messages) == 1
    assert "where RedirectOccurrences says" in messages[0]
