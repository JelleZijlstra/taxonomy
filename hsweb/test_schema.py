from unittest.mock import patch

import pytest

from hsweb.schema import (
    get_adt_member_graphql_name,
    get_openstreetmap_url,
    get_schema_string,
    schema,
)
from taxonomy.db.constants import Calendar
from taxonomy.db.models import Article, Book, Name
from taxonomy.db.models.issue_date import IssueDate, IssueDateTag
from taxonomy.db.models.name import TypeTag
from taxonomy.db.models.person import AuthorTag


def test_adt_member_graphql_names_use_model_call_signs() -> None:
    assert get_adt_member_graphql_name(Name, TypeTag.IgnoreLint) == "IgnoreLintN"
    assert get_adt_member_graphql_name(Article, AuthorTag.Author) == "AuthorA"
    assert get_adt_member_graphql_name(Name, AuthorTag.Author) == "AuthorN"
    assert get_adt_member_graphql_name(Book, AuthorTag.Author) == "AuthorB"


def test_schema_contains_model_scoped_tag_types() -> None:
    sdl = get_schema_string(schema)

    for graphql_type in (
        "AuthorA",
        "AuthorB",
        "AuthorN",
        "IgnoreLintA",
        "IgnoreLintL",
        "IgnoreLintN",
        "LSIDA",
        "LSIDCE",
        "LSIDN",
        "PublicationDateA",
    ):
        assert f"type {graphql_type} " in sdl

    assert "type TypeTagIgnoreLint " not in sdl


def test_schema_exposes_location_context() -> None:
    sdl = get_schema_string(schema)

    assert "regionPath: [Region!]!" in sdl
    assert "openstreetmapUrl: String" in sdl
    assert "type CoordinatesN implements TypeTag" in sdl
    assert "type CoordinatesOR implements OccurrenceRecordTag" in sdl


def test_openstreetmap_url_uses_parsed_coordinate_context() -> None:
    url = get_openstreetmap_url("13°51'S", "69°41'W")

    assert url is not None
    assert url.startswith("https://www.openstreetmap.org/")
    assert "mlat=-13.85" in url
    assert "mlon=-69.68333333333334" in url


def test_openstreetmap_url_rejects_incomplete_or_invalid_coordinates() -> None:
    assert get_openstreetmap_url(None, "69°41'W") is None
    assert get_openstreetmap_url("not a latitude", "69°41'W") is None


@pytest.mark.parametrize(
    ("date", "calendar", "expected"),
    [
        ("1900-02", Calendar.gregorian, "1900-02"),
        ("1900-02-29", Calendar.julian, "1900-03-13"),
        ("12-Brumaire-1", Calendar.french_republican, "1803-10-24"),
        ("undated", Calendar.gregorian, None),
    ],
)
def test_issue_date_exposes_gregorian_date_without_replacing_source(
    date: str, calendar: Calendar, expected: str | None
) -> None:
    issue_date = IssueDate.virtual(date=date, tags=(IssueDateTag.Calendar(calendar),))
    with patch("hsweb.schema.get_model", return_value=issue_date):
        result = schema.execute("{ issueDate(oid: 1) { date gregorianDate } }")

    assert not result.errors
    assert result.data == {"issueDate": {"date": date, "gregorianDate": expected}}
