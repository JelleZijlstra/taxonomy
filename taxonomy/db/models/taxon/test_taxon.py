from dataclasses import dataclass
from io import StringIO
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

from taxonomy.db.constants import RegionKind
from taxonomy.db.models import Name
from taxonomy.db.models.taxon.taxon import Taxon


@dataclass(frozen=True)
class _Period:
    name: str
    max_age: int | None = 0
    parent: None = None
    max_period: None = None

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class _Region:
    name: str
    kind: RegionKind
    parent: _Region | None = None

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class _Location:
    name: str
    region: _Region
    min_period: _Period
    max_period: _Period

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class _Name:
    label: str
    type_locality: _Location

    def __str__(self) -> str:
        return self.label


def test_display_type_localities_groups_and_sorts_by_region() -> None:
    recent = _Period("Recent")
    canada = _Region("Canada", RegionKind.country)
    alberta = _Region("Alberta", RegionKind.province, canada)
    ontario = _Region("Ontario", RegionKind.province, canada)
    atlantic = _Region("Atlantic Ocean", RegionKind.sea)
    zulu = _Location("Zulu", alberta, recent, recent)
    alpha = _Location("Alpha", ontario, recent, recent)
    coastal_waters = _Location("Newfoundland: coastal waters", atlantic, recent, recent)
    taxon = cast(
        Taxon,
        SimpleNamespace(
            all_names=Mock(
                return_value=[
                    cast(Name, _Name("Aus bus Smith, 1993", alpha)),
                    cast(Name, _Name("Cus dus Smith, 1994", coastal_waters)),
                    cast(Name, _Name("Dus eus Smith, 1995", zulu)),
                ]
            )
        ),
    )
    output = StringIO()

    Taxon.display_type_localities(taxon, file=output)

    assert output.getvalue() == (
        "Recent\n"
        "    Atlantic Ocean\n"
        "        Newfoundland: coastal waters\n"
        "            Cus dus Smith, 1994\n"
        "    Canada\n"
        "        Alberta\n"
        "            Zulu\n"
        "                Dus eus Smith, 1995\n"
        "        Ontario\n"
        "            Alpha\n"
        "                Aus bus Smith, 1993\n"
    )


def test_display_type_localities_stops_region_context_at_country() -> None:
    recent = _Period("Recent")
    north_america = _Region("North America", RegionKind.continent)
    canada = _Region("Canada", RegionKind.country, north_america)
    ontario = _Region("Ontario", RegionKind.province, canada)
    toronto = _Location("Toronto", ontario, recent, recent)
    taxon = cast(
        Taxon,
        SimpleNamespace(
            all_names=Mock(
                return_value=[cast(Name, _Name("Aus bus Smith, 1993", toronto))]
            )
        ),
    )
    output = StringIO()

    Taxon.display_type_localities(taxon, file=output)

    assert output.getvalue() == (
        "Recent\n"
        "    Canada\n"
        "        Ontario\n"
        "            Toronto\n"
        "                Aus bus Smith, 1993\n"
    )
