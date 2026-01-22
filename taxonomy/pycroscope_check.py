"""Test that certain types are resolved correctly by pycroscope."""

from typing import assert_type

from taxonomy.db.constants import Group
from taxonomy.db.models.name import Name, TypeTag


def test_fields(name: Name) -> None:
    assert_type(name.root_name, str)
    assert_type(name.group, Group)


def test_adts(ld: TypeTag.LocationDetail) -> None:
    assert_type(ld, TypeTag.LocationDetail)
    assert_type(ld.text, str)
    assert_type(ld.comment, str | None)
