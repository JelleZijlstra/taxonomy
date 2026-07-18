from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from taxonomy.db import coordinate_lint
from taxonomy.db.models.base import LintConfig

from .lint import check_coordinates, parse_date
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
    monkeypatch.setattr(coordinate_lint, "check_point_in_region", lambda *_: ())

    assert list(check_coordinates(name, LintConfig())) == []


def test_name_coordinates_must_match_location(monkeypatch: pytest.MonkeyPatch) -> None:
    name = _name_with_coordinates(("40°30'N", "74°15'W"), ("41°N", "74°15'W"))
    monkeypatch.setattr(coordinate_lint, "check_point_in_region", lambda *_: ())

    messages = list(check_coordinates(name, LintConfig()))

    assert len(messages) == 1
    assert "55.6 km from Location" in messages[0]
