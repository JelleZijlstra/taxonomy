from types import SimpleNamespace
from typing import cast

from scripts import mdd_diff
from scripts.mdd_names import _mdd_coordinates_match, get_type_locality_coordinates
from taxonomy.db.constants import NamingConvention, PersonType
from taxonomy.db.models import Person
from taxonomy.db.models.location import Location
from taxonomy.db.models.name import Name, TypeTag


def _name(
    *,
    coordinates: tuple[str, str] | None = None,
    location_coordinates: tuple[str | None, str | None] | None = None,
    partial_location_coordinates: tuple[tuple[int, str | None, str | None], ...] = (),
) -> Name:
    tags: tuple[TypeTag, ...] = (
        () if coordinates is None else (TypeTag.Coordinates(*coordinates),)
    )
    tags += tuple(
        TypeTag.PartialTypeLocality(
            cast(
                Location,
                SimpleNamespace(id=id_, latitude=latitude, longitude=longitude),
            )
        )
        for id_, latitude, longitude in partial_location_coordinates
    )
    if location_coordinates is None:
        type_locality = None
    else:
        type_locality = SimpleNamespace(
            latitude=location_coordinates[0], longitude=location_coordinates[1]
        )
    return cast(Name, SimpleNamespace(type_tags=tags, type_locality=type_locality))


def test_type_locality_coordinates_fall_back_to_location() -> None:
    name = _name(location_coordinates=("37.9", "-122.1"))

    assert get_type_locality_coordinates([name], name) == ("37.9", "-122.1")


def test_name_coordinates_take_precedence_over_location() -> None:
    name = _name(coordinates=("38°N", "123°W"), location_coordinates=("37.9", "-122.1"))

    assert get_type_locality_coordinates([name], name) == ("38", "-123")


def test_invalid_name_coordinates_fall_back_to_location() -> None:
    name = _name(
        coordinates=("not a latitude", "not a longitude"),
        location_coordinates=("37.9°N", "122.1°W"),
    )

    assert get_type_locality_coordinates([name], name) == ("37.9", "-122.1")


def test_type_locality_coordinate_ranges_are_exported_for_mdd() -> None:
    name = _name(coordinates=("12°S-10.5°S", "99°10'E-99°40'E"))

    assert get_type_locality_coordinates([name], name) == (
        "(-12 to -10.5)",
        "(99.166667 to 99.666667)",
    )


def test_partial_type_locality_coordinates_are_unioned_for_mdd() -> None:
    name = _name(
        partial_location_coordinates=(
            (1, "5.2586951°N-5.3699421°N", "162.9007493°E-163.0356302°E"),
            (2, "6.784067°N-6.9898922°N", "158.1112084°E-158.3379619°E"),
        )
    )

    assert get_type_locality_coordinates([name], name) == (
        "(5.258695 to 6.989892)",
        "(158.111208 to 163.03563)",
    )


def test_partial_type_locality_union_requires_every_component() -> None:
    name = _name(
        location_coordinates=("1", "2"),
        partial_location_coordinates=((1, "5°N", "6°E"), (2, None, None)),
    )

    assert get_type_locality_coordinates([name], name) == ("", "")


def test_name_coordinates_take_precedence_over_partial_type_localities() -> None:
    name = _name(
        coordinates=("3°N", "4°E"),
        partial_location_coordinates=((1, "5°N", "6°E"), (2, "7°N", "8°E")),
    )

    assert get_type_locality_coordinates([name], name) == ("3", "4")


def test_mdd_coordinate_ranges_compare_semantically() -> None:
    assert _mdd_coordinates_match(
        "(10.2 to 10.2167)", "(10.2167 to 10.2)", is_latitude=False
    )
    assert _mdd_coordinates_match(
        "(99.166667 to 99.666667)", "(99.1667 to 99.6667)", is_latitude=False
    )
    assert not _mdd_coordinates_match(
        "(-12 to -10.5)", "(-10.5 to 12)", is_latitude=True
    )


def _person(
    family_name: str,
    *,
    given_names: str | None = None,
    tussenvoegsel: str | None = None,
    naming_convention: NamingConvention = NamingConvention.unspecified,
) -> Person:
    return Person.virtual(
        family_name=family_name,
        given_names=given_names,
        tussenvoegsel=tussenvoegsel,
        naming_convention=naming_convention,
        type=PersonType.checked,
        tags=(),
    )


def test_mdd_author_uses_native_order_for_vietnamese_names() -> None:
    vuong = _person(
        "Vuong",
        given_names="Tu",
        tussenvoegsel="Tan",
        naming_convention=NamingConvention.vietnamese,
    )
    hassanin = _person("Hassanin")
    article_authors = [vuong, _person("Cornette"), _person("Utge"), hassanin]
    name = cast(
        Name,
        SimpleNamespace(
            get_authors=lambda: [vuong, hassanin],
            original_citation=SimpleNamespace(get_authors=lambda: article_authors),
        ),
    )

    assert (
        mdd_diff.get_mdd_style_authority(name, set())
        == "Vuong Tan Tu & Hassanin in Vuong Tan Tu, Cornette, Utge, & Hassanin"
    )
    assert list(mdd_diff.possible_mdd_authors(vuong)) == ["Vuong Tan Tu"]
