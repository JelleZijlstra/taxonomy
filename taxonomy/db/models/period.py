from collections import defaultdict
from functools import lru_cache
import sys
from typing import IO, Any, Dict, Iterable, List, Optional, Set, Tuple, TypeVar

from peewee import BooleanField, CharField, ForeignKeyField, IntegerField

from .. import models
from ..constants import PeriodRank, PeriodSystem, RequirednessLevel
from ..derived_data import DerivedField
from ... import events, getinput

from .base import BaseModel, EnumField
from .region import Region


T = TypeVar("T")


class Period(BaseModel):
    creation_event = events.Event["Period"]()
    save_event = events.Event["Period"]()
    label_field = "name"
    call_sign = "P"

    name = CharField()
    parent = ForeignKeyField(
        "self", related_name="children", db_column="parent_id", null=True
    )
    prev = ForeignKeyField(
        "self", related_name="next_foreign", db_column="prev_id", null=True
    )
    next = ForeignKeyField(
        "self", related_name="prev_foreign", db_column="next_id", null=True
    )
    min_age = IntegerField(null=True)
    max_age = IntegerField(null=True)
    min_period = ForeignKeyField(
        "self", related_name="children_min", db_column="min_period_id", null=True
    )
    max_period = ForeignKeyField(
        "self", related_name="children_max", db_column="max_period_id", null=True
    )
    system = EnumField(PeriodSystem)
    rank = EnumField(PeriodRank)
    comment = CharField()
    region = ForeignKeyField(
        Region, related_name="periods", db_column="region_id", null=True
    )
    deleted = BooleanField(default=False)

    derived_fields = [
        DerivedField("has_locations", bool, lambda period: period.has_locations())
    ]

    def has_locations(self) -> bool:
        for _ in self.locations_min:
            return True
        for _ in self.locations_max:
            return True
        return any(child.has_locations() for child in self.children)

    def __repr__(self) -> str:
        parts = [f"{self.rank.name} in {self.system.name}"]
        if self.parent is not None:
            parts.append(f"part of {self.parent.name}")
        if self.region is not None:
            parts.append(f"located in {self.region.name}")
        if self.max_period is not None:
            if self.min_period == self.max_period:
                parts.append(f"correlated to {self.min_period.name}")
            else:
                parts.append(
                    f"correlated to {self.max_period.name}—{self.min_period.name}"
                )
        if self.max_age is not None:
            parts.append(
                f"dated to {display_age(self.max_age)}—{display_age(self.min_age)}"
            )
        if self.prev is not None:
            parts.append(f"overlies {self.prev.name}")
        if self.next is not None:
            parts.append(f"underlies {self.next.name}")
        if self.deleted:
            parts.append("DELETED")
        return "{} ({})".format(self.name, ", ".join(parts))

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(Period.deleted != True)

    def should_skip(self) -> bool:
        return self.deleted

    def merge(self, other: "Period") -> None:
        for loc in self.locations_min:
            loc.min_period = other
        for loc in self.locations_max:
            loc.max_period = other
        new_comment = f"Merged into {other} (P#{other.id})"
        if not self.comment:
            self.comment = new_comment
        else:
            self.comment = f"{self.comment} – {new_comment}"
        self.deleted = True
        self.save()

    @staticmethod
    def _filter_none(seq: Iterable[Optional[T]]) -> Iterable[T]:
        return (elt for elt in seq if elt is not None)

    def sort_key(self) -> Tuple[int, int, int, str]:
        return period_sort_key(self)

    def get_min_age(self) -> Optional[int]:
        if self.min_age is not None:
            return self.min_age
        return min(
            self._filter_none(child.get_min_age() for child in self.children),
            default=None,
        )

    def get_max_age(self) -> Optional[int]:
        if self.max_age is not None:
            return self.max_age
        return max(
            self._filter_none(child.get_max_age() for child in self.children),
            default=None,
        )

    @classmethod
    def make(
        cls,
        name: str,
        system: PeriodSystem,
        rank: PeriodRank,
        *,
        parent: Optional["Period"] = None,
        next: Optional["Period"] = None,
        min_age: Optional[int] = None,
        max_age: Optional[int] = None,
        **kwargs: Any,
    ) -> "Period":
        if max_age is None and next is not None:
            max_age = next.min_age
        period = cls.create(
            name=name,
            system=system,
            rank=rank,
            parent=parent,
            next=next,
            min_age=min_age,
            max_age=max_age,
            deleted=False,
            **kwargs,
        )
        if next is not None:
            next.prev = period
            next.save()
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
            for location in self.period_localities():
                location.display(full=full, depth=depth + 4, file=file)
            partial_locations = list(self.max_only_localities())
            if partial_locations:
                file.write(f"{' ' * (depth + 6)}Partially within this interval:\n")
                for location in partial_locations:
                    location.display(full=full, depth=depth + 4, file=file)
        if children:
            for period in sorted(self.children, key=period_sort_key):
                period.display(
                    full=full, depth=depth + 2, file=file, locations=locations
                )
            for period in sorted(
                Period.select_valid().filter(
                    Period.max_period == self, Period.min_period == self
                ),
                key=period_sort_key,
            ):
                period.display(
                    full=full, depth=depth + 2, file=file, locations=locations
                )

    def max_only_localities(self) -> Iterable["models.Location"]:
        return models.Location.select_valid().filter(
            models.Location.max_period == self, models.Location.min_period != self
        )

    def period_localities(self) -> Iterable["models.Location"]:
        return models.Location.select_valid().filter(
            models.Location.max_period == self, models.Location.min_period == self
        )

    def make_locality(self, region: "Region") -> "models.Location":
        return models.Location.make(self.name, region, self)

    def all_localities(
        self, include_children: bool = True, include_partial: bool = False
    ) -> Set["models.Location"]:
        if include_partial:
            locations = {*self.locations_min, *self.locations_max}
        else:
            locations = set(
                self.locations_min.filter(models.Location.max_period == self)
            )
        if include_children:
            if include_partial:
                children = {*self.children, *self.children_min, *self.children_max}
            else:
                children = {
                    *self.children,
                    *self.children_min.filter(Period.max_period == self),
                }
            for child in children:
                locations |= child.all_localities(
                    include_children=include_children, include_partial=include_partial
                )
        return {loc for loc in locations if loc.deleted is not True}

    def all_type_localities(
        self, include_children: bool = True, include_partial: bool = False
    ) -> List["models.Name"]:
        return [
            nam
            for loc in self.all_localities(
                include_children=include_children, include_partial=include_partial
            )
            for nam in loc.type_localities
        ]

    def display_type_localities(
        self, include_children: bool = True, include_partial: bool = False
    ) -> None:
        models.name.write_names(
            self.all_type_localities(
                include_children=include_children, include_partial=include_partial
            ),
            organized=True,
        )

    def all_regions(self) -> Set[Region]:
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

    def set_period(self, period: Optional["Period"]) -> None:
        self.min_period = self.max_period = period

    def fill_field(self, field: str) -> None:
        if field == "period":
            period = self.get_value_for_foreign_class(
                "period",
                Period,
                default_obj=self.min_period,
                callbacks=self.get_adt_callbacks(),
            )
            self.set_period(period)
        else:
            super().fill_field(field)

    def get_required_fields(self) -> Iterable[str]:
        yield "name"
        yield "parent"
        yield "system"

    def requires_parent(self) -> RequirednessLevel:
        if self.system is PeriodSystem.gts:
            if self.rank is PeriodRank.eon:
                return RequirednessLevel.disallowed
            else:
                return RequirednessLevel.required
        elif self.system in (
            PeriodSystem.nalma,
            PeriodSystem.elma,
            PeriodSystem.alma,
            PeriodSystem.salma,
            PeriodSystem.aulma,
        ):
            if self.rank is PeriodRank.age:
                return RequirednessLevel.disallowed
            else:
                if self.rank is PeriodRank.biozone and self.system is PeriodSystem.elma:
                    return RequirednessLevel.optional
                return RequirednessLevel.required
        elif self.system is PeriodSystem.local_biostratigraphy:
            if self.rank is PeriodRank.zonation:
                return RequirednessLevel.disallowed
            else:
                return RequirednessLevel.required
        else:
            assert False, f"unrecognized system {self.system!r}"


def display_age(age: int) -> str:
    if age < 1000:
        return str(age)
    elif age < 1_000_000:
        return f"{age / 1000}k"
    else:
        return f"{age / 1_000_000}m"


@lru_cache(maxsize=1024)
def period_sort_key(period: Period) -> Tuple[int, int, int, str]:
    """The sort key consists of four parts.

    - The maximum age of the period, or of its first parent that has a minimum
      age. This is a negative number.
    - The number of recursive parents that have the same age, or no age.
    - The number of siblings that are younger and are otherwise the same, as a
      negative number.
    - The name of the period.

    """
    if period.max_age is not None:
        if period.parent is not None and period.parent.max_age == period.max_age:
            return _get_from_parent(period, period.parent)
        if (
            period.max_period is not None
            and period.max_period.max_age == period.max_age
        ):
            return _get_from_parent(period, period.max_period)
        return (-period.max_age, 0, 0, period.name)
    if period.parent is not None:
        return _get_from_parent(period, period.parent)
    if period.max_period is not None:
        return _get_from_parent(period, period.max_period)
    return (0, 0, 0, period.name)


def _get_from_parent(period: Period, parent: Period) -> Tuple[int, int, int, str]:
    age, parents, _, _ = period_sort_key(parent)
    return _apply_next_correction(period, age, parents)


def _apply_next_correction(
    period: Period, age: int, parents: int
) -> Tuple[int, int, int, str]:
    if period.next is not None:
        next_age, next_parents, next_siblings, _ = period_sort_key(period.next)
        if (next_age, next_parents) == (age, parents + 1):
            return (age, parents + 1, next_siblings - 1, period.name)
    return (age, parents + 1, 0, period.name)


def display_period_tree(
    min_count: int = 0,
    system: Optional[PeriodSystem] = None,
    full: bool = False,
    include_taxa: bool = False,
) -> None:
    max_parent_to_periods: Dict[Period, int] = defaultdict(int)
    parent_to_periods: Dict[Period, List[Period]] = defaultdict(list)
    period_to_max_parent: Dict[Period, Period] = {}

    def add_period(period: Period) -> Period:
        if period in period_to_max_parent:
            return period_to_max_parent[period]
        if period.parent is None:
            period_to_max_parent[period] = period
            max_parent_to_periods[period] += 1
            return period
        parent_to_periods[period.parent].append(period)
        max_parent = add_period(period.parent)
        max_parent_to_periods[max_parent] += 1
        period_to_max_parent[period] = max_parent
        return max_parent

    periods = Period.select_valid()
    if system is not None:
        periods = periods.filter(Period.system == system)

    for period in periods:
        add_period(period)

    def display_period(period: Period, depth: int) -> None:
        spacing = " " * (depth * 4)
        if include_taxa:
            period.display(depth=depth * 4)
            return
        if full:
            print(f"{spacing}{period!r}")
        else:
            print(f"{spacing}{period}")
        for child in sorted(parent_to_periods[period], key=period_sort_key):
            display_period(child, depth + 1)

    if system is None:
        key = lambda item: -item[1]
    else:
        key = lambda item: period_sort_key(item[0])

    for max_parent, count in sorted(max_parent_to_periods.items(), key=key):
        if count >= min_count:
            getinput.print_header(f"{max_parent.name} ({count})")
            display_period(max_parent, 0)
