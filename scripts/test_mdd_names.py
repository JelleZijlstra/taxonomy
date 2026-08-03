from types import SimpleNamespace
from typing import cast

from scripts.mdd_names import _mdd_coordinates_match, get_type_locality_coordinates
from taxonomy.db.models.name import Name, TypeTag


def _name(
    *,
    coordinates: tuple[str, str] | None = None,
    location_coordinates: tuple[str | None, str | None] | None = None,
) -> Name:
    tags = () if coordinates is None else (TypeTag.Coordinates(*coordinates),)
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
