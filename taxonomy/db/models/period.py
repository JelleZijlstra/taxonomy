import sys
from typing import IO, Any, Iterable, Optional, Set, TypeVar

from peewee import CharField, ForeignKeyField, IntegerField

from .. import constants, models
from ... import events, getinput

from .base import BaseModel, EnumField
from .region import Region


T = TypeVar("T")


class Period(BaseModel):
    creation_event = events.Event["Period"]()
    save_event = events.Event["Period"]()
    label_field = "name"

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
    system = EnumField(constants.PeriodSystem)
    comment = CharField()
    region = ForeignKeyField(
        Region, related_name="periods", db_column="region_id", null=True
    )

    @staticmethod
    def _filter_none(seq: Iterable[Optional[T]]) -> Iterable[T]:
        return (elt for elt in seq if elt is not None)

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
        system: constants.PeriodSystem,
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
            system=system.value,
            parent=parent,
            next=next,
            min_age=min_age,
            max_age=max_age,
            **kwargs,
        )
        if next is not None:
            next.prev = period
            next.save()
        return period

    @classmethod
    def create_interactively(cls) -> "Period":
        print("creating Periods interactively only allows stratigraphic units")
        name = getinput.get_line("name> ")
        assert name is not None
        kind = getinput.get_enum_member(
            constants.PeriodSystem, "kind> ", allow_empty=False
        )
        result = cls.make_stratigraphy(name, kind)
        result.fill_required_fields()
        return result

    @classmethod
    def make_stratigraphy(
        cls,
        name: str,
        kind: constants.PeriodSystem,
        period: Optional["Period"] = None,
        parent: Optional["Period"] = None,
        **kwargs: Any,
    ) -> "Period":
        if period is not None:
            kwargs["max_period"] = kwargs["min_period"] = period
        period = cls.create(name=name, system=kind.value, parent=parent, **kwargs)
        if "next" in kwargs:
            next_period = kwargs["next"]
            next_period.prev = period
            next_period.save()
        return period

    def display(
        self, full: bool = False, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        file.write("{}{}\n".format(" " * (depth + 4), repr(self)))
        for location in models.Location.filter(
            models.Location.max_period == self,
            models.Location.min_period == self,
            models.Location.deleted == False,
        ):
            location.display(full=full, depth=depth + 2, file=file)
        for location in self.locations_stratigraphy:
            location.display(full=full, depth=depth + 2, file=file)
        for period in self.children:
            period.display(full=full, depth=depth + 1, file=file)
        for period in Period.filter(
            Period.max_period == self, Period.min_period == self
        ):
            period.display(full=full, depth=depth + 1, file=file)

    def make_locality(self, region: "Region") -> "models.Location":
        return models.Location.make(self.name, region, self)

    def all_localities(self) -> Iterable["models.Location"]:
        yield from self.locations_stratigraphy
        for child in self.children:
            yield from child.all_localities()

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

    def __repr__(self) -> str:
        properties = {}
        for field in self.fields():
            if field == "name":
                continue
            value = getattr(self, field)
            if value is None:
                continue
            if isinstance(value, Period):
                value = value.name
            properties[field] = value
        return "{} ({})".format(
            self.name, ", ".join("%s=%s" % item for item in properties.items())
        )
