from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from hsweb.schema import (
    get_adt_member_graphql_name,
    get_openstreetmap_url,
    get_schema_string,
    schema,
)
from taxonomy.db.constants import Calendar
from taxonomy.db.models import Article, Book, CitationGroup, Location, Name, Region
from taxonomy.db.models.issue_date import IssueDate, IssueDateTag
from taxonomy.db.models.location.model import LocationStatus
from taxonomy.db.models.name import TypeTag
from taxonomy.db.models.person import AuthorTag
from taxonomy.db.models.region import RegionTag
from taxonomy.db.models.tags import LocationTag


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
    ("latitude", "longitude", "expected_view"),
    [
        (
            "29.669289°N-29.683838°N",
            "82.573847°W-82.557073°W",
            {"extent": ["-82.573847,29.669289,-82.557073,29.683838"]},
        ),
        ("29.67°N", "82.57°W", {"center": ["-82.57,29.67"], "level": ["15"]}),
        (None, "82.57°W", None),
        ("29.67°N", None, None),
        ("not a latitude", "82.57°W", None),
    ],
)
def test_location_plss_map_uses_coordinate_extent(
    latitude: str | None,
    longitude: str | None,
    expected_view: dict[str, list[str]] | None,
) -> None:
    location = Location.virtual(latitude=latitude, longitude=longitude)
    with patch("hsweb.schema.get_model", return_value=location):
        result = schema.execute("{ location(oid: 1) { plssMapUrl } }")

    assert not result.errors
    url = result.data["location"]["plssMapUrl"]
    if expected_view is None:
        assert url is None
    else:
        parsed = urlparse(url)
        assert parsed.scheme == "https"
        assert parsed.netloc == "www.arcgis.com"
        assert parsed.path == "/apps/mapviewer/index.html"
        assert parse_qs(parsed.query) == {
            "url": [
                (
                    "https://gis.blm.gov/arcgis/rest/services/Cadastral/"
                    "BLM_Natl_PLSS_CadNSDI/MapServer"
                )
            ],
            "mapOnly": ["true"],
            **expected_view,
        }


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


def test_issue_dates_paginate_in_gregorian_order_without_truncation() -> None:
    # Deliberately put the latest record first in storage order and include a
    # Julian date that sorts differently before conversion.
    dates = [
        IssueDate.virtual(date="1900-03-14"),
        IssueDate.virtual(
            date="1900-02-29", tags=(IssueDateTag.Calendar(Calendar.julian),)
        ),
        *[IssueDate.virtual(date="1900-03-01") for _ in range(1001)],
    ]
    citation_group = MagicMock(spec=CitationGroup)
    citation_group.issue_date_set = dates
    query = """
        query($cursor: String) {
            citationGroup(oid: 1) {
                issueDateSet(first: 400, after: $cursor) {
                    edges { node { oid } }
                    pageInfo { endCursor hasNextPage }
                }
            }
        }
    """
    cursor = None
    seen = []
    with patch("hsweb.schema.get_model", return_value=citation_group):
        while True:
            result = schema.execute(
                query, variable_values={"cursor": cursor}, context_value={"request": {}}
            )
            assert not result.errors
            connection = result.data["citationGroup"]["issueDateSet"]
            ids = [edge["node"]["oid"] for edge in connection["edges"]]
            assert ids
            seen.extend(ids)
            if not connection["pageInfo"]["hasNextPage"]:
                break
            cursor = connection["pageInfo"]["endCursor"]

    assert seen == [*sorted(date.id for date in dates[2:]), dates[1].id, dates[0].id]


@pytest.mark.parametrize("osm_id", [2**31 - 1, 5482962866, 2**63 - 1])
@pytest.mark.parametrize("is_region", [False, True])
def test_osm_identifiers_preserve_large_values(osm_id: int, *, is_region: bool) -> None:
    if is_region:
        obj = Region.virtual(tags=(RegionTag.OpenStreetMap("node", osm_id, "place"),))
        field, tag_type = "region", "OpenStreetMapR"
    else:
        obj = Location.virtual(
            tags=(
                LocationTag.CoordinatesFromNominatim(
                    "node", osm_id, "place", use_bounding_box=False
                ),
            )
        )
        field, tag_type = "location", "CoordinatesFromNominatimL"
    with patch("hsweb.schema.get_model", return_value=obj):
        result = schema.execute(
            f"{{ {field}(oid: 1) {{ tags {{ ... on {tag_type} {{ osmId }} }} }} }}"
        )

    assert not result.errors
    assert result.data == {field: {"tags": [{"osmId": str(osm_id)}]}}


@pytest.mark.parametrize(
    ("call_sign", "oid"), [("unknown", "1"), ("l", "1"), ("l", str(2**63))]
)
def test_missing_model_returns_no_results(call_sign: str, oid: str) -> None:
    result = schema.execute(
        "query($callSign: String!, $oid: String!) {"
        "byCallSign(callSign: $callSign, oid: $oid) { oid pageTitle }}",
        variable_values={"callSign": call_sign, "oid": oid},
        context_value={"request": {}},
    )

    assert not result.errors
    assert result.data == {"byCallSign": []}


@pytest.mark.parametrize("status", list(LocationStatus))
def test_model_lookup_handles_valid_deleted_and_redirected_locations(
    status: LocationStatus,
) -> None:
    location = Location.virtual(name="Sibube", deleted=status, parent=Location(2))
    with patch.object(Location, "get", return_value=location):
        result = schema.execute(
            '{ byCallSign(callSign: "l", oid: "1") { oid pageTitle redirectUrl } }',
            context_value={"request": {}},
        )

    assert not result.errors
    if status is LocationStatus.deleted:
        assert result.data == {"byCallSign": []}
    else:
        assert result.data == {
            "byCallSign": [
                {
                    "oid": 1,
                    "pageTitle": "Sibube",
                    "redirectUrl": "/l/2" if status is LocationStatus.alias else None,
                }
            ]
        }
