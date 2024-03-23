from __future__ import annotations

import sys
from collections.abc import Iterable
from functools import lru_cache
from typing import IO, Any, Self, TypeVar

from clorm import Field

from taxonomy.apis.cloud_search import SearchField, SearchFieldType

from ... import events, getinput
from .. import models
from ..constants import RequirednessLevel, StratigraphicUnitRank
from ..derived_data import DerivedField
from .base import BaseModel
from .period import Period
from .region import Region

T = TypeVar("T")


class StratigraphicUnit(BaseModel):
    creation_event = events.Event["StratigraphicUnit"]()
    save_event = events.Event["StratigraphicUnit"]()
    label_field = "name"
    call_sign = "S"
    clorm_table_name = "stratigraphic_unit"

    name = Field[str]()
    parent = Field[Self | None]("parent_id", related_name="children")
    prev = Field[Self | None]("prev_id", related_name="next")
    min_period = Field[Period | None](
        "min_period_id", related_name="stratigraphic_units_min"
    )
    max_period = Field[Period | None](
        "max_period_id", related_name="stratigraphic_units_max"
    )
    rank = Field[StratigraphicUnitRank]()
    comment = Field[str]()
    region = Field[Region | None]("region_id", related_name="stratigraphic_units")
    deleted = Field[bool](default=False)

    derived_fields = [
        DerivedField("has_locations", bool, lambda unit: unit.has_locations())
    ]
    search_fields = [
        SearchField(SearchFieldType.text, "name"),
        SearchField(SearchFieldType.literal, "rank"),
        SearchField(SearchFieldType.text, "comment", highlight_enabled=True),
    ]

    def get_search_dicts(self) -> list[dict[str, Any]]:
        data = {"name": self.name, "rank": self.rank.name, "comment": self.comment}
        return [data]

    def has_locations(self) -> bool:
        for _ in self.locations:
            return True
        return any(child.has_locations() for child in self.children)

    def __repr__(self) -> str:
        parts = [self.rank.name]
        if self.parent is not None:
            parts.append(f"part of {self.parent.name}")
        if self.region is not None:
            parts.append(f"located in {self.region.name}")
        if self.max_period is not None and self.min_period is not None:
            if self.min_period == self.max_period:
                parts.append(f"correlated to {self.min_period.name}")
            else:
                parts.append(
                    f"correlated to {self.max_period.name}—{self.min_period.name}"
                )
        if self.prev is not None:
            parts.append(f"overlies {self.prev.name}")
        if self.deleted:
            parts.append("DELETED")
        return "{} ({})".format(self.name, ", ".join(parts))

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(StratigraphicUnit.deleted != True)

    def is_invalid(self) -> bool:
        return self.deleted

    def should_skip(self) -> bool:
        return self.deleted

    def merge(self, other: StratigraphicUnit | None = None) -> None:
        if other is None:
            other = StratigraphicUnit.getter(None).get_one("merge into> ")
            if other is None:
                return
        for loc in self.locations:
            loc.stratigraphic_unit = other
        new_comment = f"Merged into {other} (P#{other.id})"
        if not self.comment:
            self.comment = new_comment
        else:
            self.comment = f"{self.comment} – {new_comment}"
        self.parent = other
        self.deleted = True

    @staticmethod
    def _filter_none(seq: Iterable[T | None]) -> Iterable[T]:
        return (elt for elt in seq if elt is not None)

    def sort_key(self) -> tuple[int, int, str]:
        return unit_sort_key(self)

    def get_min_age(self) -> int | None:
        if self.min_age is not None:
            return self.min_age
        return min(
            self._filter_none(child.get_min_age() for child in self.children),
            default=None,
        )

    def get_max_age(self) -> int | None:
        if self.max_age is not None:
            return self.max_age
        return max(
            self._filter_none(child.get_max_age() for child in self.children),
            default=None,
        )

    @classmethod
    def create_interactively(
        cls,
        name: str | None = None,
        rank: StratigraphicUnitRank | None = None,
        **kwargs: Any,
    ) -> StratigraphicUnit:
        if name is None:
            name = getinput.get_line("name> ")
        assert name is not None
        if rank is None:
            rank = getinput.get_enum_member(
                StratigraphicUnitRank, "rank> ", allow_empty=False
            )
        result = cls.make(name, rank)
        result.fill_required_fields()
        return result

    @classmethod
    def make(
        cls,
        name: str,
        rank: StratigraphicUnitRank,
        period: models.Period | None = None,
        parent: StratigraphicUnit | None = None,
        **kwargs: Any,
    ) -> StratigraphicUnit:
        if period is not None:
            kwargs["max_period"] = kwargs["min_period"] = period
        period = cls.create(
            name=name, rank=rank.value, parent=parent, deleted=False, **kwargs
        )
        if "next" in kwargs:
            next_period = kwargs["next"]
            next_period.prev = period
        return period

    def display(
        self,
        full: bool = False,
        depth: int = 0,
        file: IO[str] = sys.stdout,
        locations: bool = True,
        children: bool = True,
    ) -> None:
        file.write("{}{}\n".format(" " * (depth + 4), repr(self)))
        if locations:
            for location in self.locations:
                location.display(full=full, depth=depth + 4, file=file)
        if children:
            for period in sorted(self.children, key=unit_sort_key):
                period.display(
                    full=full, depth=depth + 2, file=file, locations=locations
                )

    def all_localities(self) -> Iterable[models.Location]:
        yield from self.locations
        for child in self.children:
            yield from child.all_localities()

    def all_type_localities(self, include_children: bool = True) -> list[models.Name]:
        if include_children:
            locs = self.all_localities()
        else:
            locs = self.locations
        return [nam for loc in locs for nam in loc.type_localities]

    def display_type_localities(self, include_children: bool = True) -> None:
        models.name.name.write_names(
            self.all_type_localities(include_children=include_children), organized=True
        )

    def all_regions(self) -> set[Region]:
        return {loc.region for loc in self.all_localities()}

    def autoset_region(self) -> bool:
        if self.region is not None:
            return True
        regions = self.all_regions()
        if len(regions) == 1:
            region = next(iter(regions))
            print(f"{self}: setting region to {region}")
            self.region = region
            return True
        else:
            return False

    def set_period(self, period: models.Period | None) -> None:
        self.min_period = self.max_period = period

    def fill_field(self, field: str) -> None:
        if field == "period":
            period = self.get_value_for_foreign_class(
                "period",
                models.Period,
                default_obj=self.min_period,
                callbacks=self.get_adt_callbacks(),
            )
            self.set_period(period)
        else:
            super().fill_field(field)

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        return {**super().get_adt_callbacks(), "merge": self.merge}

    def get_required_fields(self) -> Iterable[str]:
        yield "name"
        yield "rank"
        if self.requires_parent() is not RequirednessLevel.disallowed:
            yield "parent"

    def requires_parent(self) -> RequirednessLevel:
        if self.rank is StratigraphicUnitRank.supergroup:
            return RequirednessLevel.disallowed
        else:
            return RequirednessLevel.optional


@lru_cache(maxsize=1024)
def unit_sort_key(unit: StratigraphicUnit) -> tuple[int, int, str]:
    """The sort key consists of three parts.

    - The number of recursive parents.
    - The number of siblings that are younger and are otherwise the same, as a
      negative number.
    - The name of the unit.

    """
    if unit.parent is not None:
        parents, _, _ = unit_sort_key(unit.parent)
        for next_period in unit.next:
            next_parents, next_siblings, _ = unit_sort_key(next_period)
            if next_parents == parents + 1:
                return (parents + 1, next_siblings - 1, unit.name)
        return (parents + 1, 0, unit.name)
    return (0, 0, unit.name)
