from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import pytest

from taxonomy import shell
from taxonomy.db import models
from taxonomy.db.constants import AgeClass, Group
from taxonomy.db.models import Name, Taxon, TypeTag


@dataclass
class _FakePeriod:
    name: str


@dataclass
class _FakeRegion:
    name: str


@dataclass
class _FakeLocation:
    name: str
    region: _FakeRegion
    min_period: _FakePeriod
    max_period: _FakePeriod
    tags: frozenset[Any] = frozenset()
    latitude: str | None = None
    longitude: str | None = None

    def has_tag(self, tag: Any) -> bool:
        return tag in self.tags


@dataclass
class _FakeName:
    type_locality: _FakeLocation | None
    age: AgeClass
    locality_required: bool = True
    imprecise: bool = False
    group: Group = Group.species

    @property
    def taxon(self) -> SimpleNamespace:
        return SimpleNamespace(age=self.age)

    def get_required_fields(self) -> tuple[str, ...]:
        if self.locality_required:
            return ("type_locality",)
        return ()

    def has_type_tag(self, tag: Any) -> bool:
        return tag is TypeTag.ImpreciseLocality and self.imprecise


def _location(
    name: str,
    *,
    region_name: str = "Country",
    recent: bool = True,
    tags: frozenset[Any] = frozenset(),
    coordinates: bool = False,
) -> _FakeLocation:
    period = _FakePeriod("Recent" if recent else "Jurassic")
    return _FakeLocation(
        name=name,
        region=_FakeRegion(region_name),
        min_period=period,
        max_period=period,
        tags=tags,
        latitude="1°N" if coordinates else None,
        longitude="2°E" if coordinates else None,
    )


def test_type_locality_summary_lines() -> None:
    names = cast(
        list[Name],
        [
            _FakeName(None, AgeClass.extant, locality_required=False),
            _FakeName(None, AgeClass.fossil, locality_required=False),
            _FakeName(None, AgeClass.extant),
            _FakeName(_location("Country"), AgeClass.extant, imprecise=True),
            _FakeName(
                _location(
                    "Broad area", tags=frozenset({models.tags.LocationTag.General})
                ),
                AgeClass.extant,
            ),
            _FakeName(
                _location(
                    "Unknown site", tags=frozenset({models.tags.LocationTag.Unplaced})
                ),
                AgeClass.extant,
            ),
            _FakeName(_location("Exact site", coordinates=True), AgeClass.extant),
            _FakeName(
                _location(
                    "Country",
                    recent=False,
                    tags=frozenset({models.tags.LocationTag.General}),
                ),
                AgeClass.fossil,
            ),
            _FakeName(
                _location(
                    "Unknown bed",
                    recent=False,
                    tags=frozenset({models.tags.LocationTag.Unplaced}),
                ),
                AgeClass.fossil,
            ),
            _FakeName(_location("Country", recent=False), AgeClass.fossil),
        ],
    )

    assert shell._type_locality_summary_lines(names) == [
        "Of 10 names (100.0% of total):",
        "- 3 names (30.0% of total): type locality not set",
        "  - 2 names (20.0% of total): type locality not required",
        "    - 1 name (10.0% of total): fossil or ichnotaxon",
        "    - 1 name (10.0% of total): extant or recently extinct",
        "  - 1 name (10.0% of total): type locality required",
        "- 7 names (70.0% of total): type locality set",
        "  - 4 names (40.0% of total): Recent location",
        "    - 1 name (10.0% of total): regionwide location",
        "      - 1 name (10.0% of total): ImpreciseLocality tag set",
        "      - 0 names (0.0% of total): no ImpreciseLocality tag",
        "    - 1 name (10.0% of total): General location",
        "      - 0 names (0.0% of total): ImpreciseLocality tag set",
        "      - 1 name (10.0% of total): no ImpreciseLocality tag",
        "    - 1 name (10.0% of total): Unplaced location",
        "    - 1 name (10.0% of total): precise location",
        "      - 1 name (10.0% of total): coordinates set",
        "      - 0 names (0.0% of total): coordinates not set",
        "  - 3 names (30.0% of total): fossil location (all non-Recent locations)",
        "    - 1 name (10.0% of total): General location",
        "    - 1 name (10.0% of total): Unplaced location",
        "    - 1 name (10.0% of total): other location",
    ]


def test_type_locality_summary_lines_empty() -> None:
    lines = shell._type_locality_summary_lines([])

    assert lines[0] == "Of 0 names (0.0% of total):"
    assert all("(0.0% of total)" in line for line in lines)


def test_type_locality_summary_filters_taxon_names_to_species_group(
    capsys: pytest.CaptureFixture[str],
) -> None:
    species_name = _FakeName(None, AgeClass.extant, locality_required=False)
    genus_name = _FakeName(
        None, AgeClass.extant, locality_required=False, group=Group.genus
    )
    taxon = cast(Taxon, SimpleNamespace(all_names=lambda: [species_name, genus_name]))

    shell.type_locality_summary(taxon)

    assert capsys.readouterr().out.startswith("Of 1 name (100.0% of total):\n")
