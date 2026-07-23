from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import cast

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
