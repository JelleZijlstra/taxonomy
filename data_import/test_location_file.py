from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

from data_import import lib, location_file
from taxonomy.db.constants import OccurrenceBasis, Rank
from taxonomy.db.models import Article, Period, Region
from taxonomy.db.models.location import Location, LocationStatus


def test_companion_path() -> None:
    assert location_file.companion_path(Path("source.ce.jsonl")) == Path(
        "source.locations.jsonl"
    )


def test_serialize_location() -> None:
    proposal: location_file.LocationDict = {
        "name": "Port locality, Ecuador",
        "region": cast(Region, type("Region", (), {"name": "Ecuador"})()),
        "period": cast(Period, type("Period", (), {"name": "Recent"})()),
        "source": cast(Article, type("Article", (), {"name": "gazetteer.pdf"})()),
        "latitude": "1°S",
        "longitude": "80°W",
    }

    assert location_file.serialize_location(proposal) == {
        "name": "Port locality, Ecuador",
        "region": "Ecuador",
        "period": "Recent",
        "source": "gazetteer.pdf",
        "latitude": "1°S",
        "longitude": "80°W",
    }


def test_build_plan_reports_missing_proposal(monkeypatch: pytest.MonkeyPatch) -> None:
    article = cast(Article, object())
    entries: list[lib.CEDict] = [
        {
            "article": article,
            "page": "1",
            "name": "Rattus rattus",
            "rank": Rank.species,
            "occurrences": [
                {
                    "locality": "near the port",
                    "mapped_location": "Port locality, Ecuador",
                    "basis": OccurrenceBasis.listing,
                }
            ],
        }
    ]
    monkeypatch.setattr(location_file, "_existing_location", lambda name: None)

    plan = location_file.build_plan(entries, [])

    assert plan.statuses == {"Port locality, Ecuador": "unresolved"}
    assert plan.is_clean
    assert len(plan.warnings) == 1


def test_existing_location_allows_formatted_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    location = cast(
        Location, SimpleNamespace(deleted=LocationStatus.valid, parent=None)
    )
    calls = []

    def locations_with_name(name: str) -> list[Location]:
        calls.append(name)
        return [location] if name == "Mary's Fancy, Sint Maarten" else []

    monkeypatch.setattr(location_file, "_locations_with_name", locations_with_name)

    assert location_file._existing_location("Mary’s Fancy, Sint Maarten") is location
    assert calls == ["Mary’s Fancy, Sint Maarten", "Mary's Fancy, Sint Maarten"]


def test_build_plan_restores_deleted_general_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    article = cast(Article, object())
    region = cast(Region, SimpleNamespace(name="Carbon County, Montana"))
    period = cast(Period, SimpleNamespace(name="Recent"))
    deleted = cast(
        Location,
        SimpleNamespace(
            name="Carbon County, Montana",
            deleted=LocationStatus.deleted,
            region=region,
            min_period=period,
            max_period=period,
            latitude=None,
            longitude=None,
        ),
    )
    entries: list[lib.CEDict] = [
        {
            "article": article,
            "page": "1",
            "name": "Rattus rattus",
            "rank": Rank.species,
            "occurrences": [
                {
                    "locality": "Carbon County, Montana",
                    "mapped_location": "Carbon County, Montana",
                    "basis": OccurrenceBasis.voucher,
                }
            ],
        }
    ]
    proposal: location_file.LocationDict = {
        "name": "Carbon County, Montana",
        "region": region,
        "period": period,
    }
    monkeypatch.setattr(location_file, "_existing_location", lambda name: None)
    monkeypatch.setattr(
        location_file, "_restorable_deleted_location", lambda name, candidate: deleted
    )

    plan = location_file.build_plan(entries, [proposal])

    assert plan.statuses == {"Carbon County, Montana": "restore"}
    assert plan.locations == {"Carbon County, Montana": deleted}
    assert plan.is_clean


def test_apply_plan_restores_deleted_location() -> None:
    article = cast(Article, object())
    region = cast(Region, SimpleNamespace(name="Carbon County, Montana"))
    period = cast(Period, SimpleNamespace(name="Recent"))
    format_location = MagicMock()
    edit_until_clean = MagicMock()
    deleted = cast(
        Location,
        SimpleNamespace(
            deleted=LocationStatus.deleted,
            region=region,
            min_period=period,
            max_period=period,
            latitude=None,
            longitude=None,
            comment=None,
            source=None,
            location_detail=None,
            tags=(),
            format=format_location,
            edit_until_clean=edit_until_clean,
        ),
    )
    proposal: location_file.LocationDict = {
        "name": "Carbon County, Montana",
        "region": region,
        "period": period,
        "source": article,
        "location_detail": "Source locality detail.",
    }
    plan = location_file.LocationPlan(
        used_names={"Carbon County, Montana"},
        locations={"Carbon County, Montana": deleted},
        proposals={"Carbon County, Montana": proposal},
        statuses={"Carbon County, Montana": "restore"},
        errors=[],
        warnings=[],
    )

    locations = location_file.apply_plan(plan)

    assert deleted.deleted is LocationStatus.valid
    assert deleted.source is article
    assert deleted.location_detail == "Source locality detail."
    assert locations == {"Carbon County, Montana": deleted}
    assert plan.statuses == {"Carbon County, Montana": "existing"}
    format_location.assert_called_once_with(quiet=True)
    edit_until_clean.assert_called_once_with()


def test_print_plan_lists_each_location(capsys: pytest.CaptureFixture[str]) -> None:
    region = cast(Region, SimpleNamespace(name="Ecuador"))
    period = cast(Period, SimpleNamespace(name="Recent"))
    existing = cast(
        Location,
        SimpleNamespace(
            id=42,
            name="Port locality",
            region=region,
            min_period=period,
            max_period=period,
        ),
    )
    plan = location_file.LocationPlan(
        used_names={"Old port name", "New site", "Unknown site"},
        locations={"Old port name": existing, "New site": None, "Unknown site": None},
        proposals={
            "New site": {
                "name": "New site",
                "region": region,
                "period": period,
                "latitude": "1.0°S",
                "longitude": "80.0°W",
            }
        },
        statuses={
            "Old port name": "existing",
            "New site": "create",
            "Unknown site": "unresolved",
        },
        errors=[],
        warnings=[],
    )

    location_file.print_plan(plan)

    assert capsys.readouterr().out.splitlines() == [
        "Occurrence locations: existing=1, restore=0, create=1, unresolved=1",
        "  [create] New site; Ecuador; Recent; 1.0°S 80.0°W",
        "  [existing] Old port name -> Port locality (#42); Ecuador; Recent",
        "  [unresolved] Unknown site",
    ]


def test_location_file_report_collects_and_prints_all_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "source.locations.jsonl"
    path.write_text(
        '{"name":"first","region":"Wrong one","period":"Recent"}\n'
        '{"name":"second","region":"Wrong two","period":"Recent"}\n'
    )

    def fail_region(data: object, *, line_number: int) -> location_file.LocationDict:
        assert isinstance(data, dict)
        raise location_file.LocationFileError(
            f"line {line_number}: no Region named {data['region']!r}"
        )

    monkeypatch.setattr(location_file, "deserialize_location", fail_region)

    report = location_file.read_location_file_report(path)
    plan = location_file.build_plan([], report.locations, proposal_errors=report.errors)
    location_file.print_plan(plan)

    assert not plan.is_clean
    assert report.errors == [
        "line 1: no Region named 'Wrong one'",
        "line 2: no Region named 'Wrong two'",
    ]
    assert capsys.readouterr().out.splitlines()[-2:] == [
        "[error] line 1: no Region named 'Wrong one'",
        "[error] line 2: no Region named 'Wrong two'",
    ]
