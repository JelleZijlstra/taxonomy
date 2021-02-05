import enum
import sys
from typing import Any, Callable, Dict, IO, Iterable, Optional, Type, Union

from peewee import CharField, ForeignKeyField, IntegerField, TextField

from .. import models
from ... import adt, events, getinput

from .base import BaseModel, ADTField, EnumField
from .article import Article
from .period import Period, period_sort_key
from .region import Region
from .stratigraphic_unit import StratigraphicUnit


class LocationStatus(enum.IntEnum):
    valid = 0
    deleted = 1
    alias = 2


class Location(BaseModel):
    creation_event = events.Event["Location"]()
    save_event = events.Event["Location"]()
    label_field = "name"
    grouping_field = "min_period"
    call_sign = "L"

    name = CharField()
    min_period = ForeignKeyField(
        Period, related_name="locations_min", db_column="min_period_id", null=True
    )
    max_period = ForeignKeyField(
        Period, related_name="locations_max", db_column="max_period_id", null=True
    )
    min_age = IntegerField(null=True)
    max_age = IntegerField(null=True)
    stratigraphic_unit = ForeignKeyField(
        StratigraphicUnit, related_name="locations", null=True
    )
    region = ForeignKeyField(Region, related_name="locations", db_column="region_id")
    comment = CharField()
    latitude = CharField()
    longitude = CharField()
    location_detail = TextField()
    age_detail = TextField()
    source = ForeignKeyField(Article, related_name="locations", null=True)
    deleted = EnumField(LocationStatus)
    tags = ADTField(lambda: LocationTag, null=True)
    parent = ForeignKeyField(
        "self", related_name="aliases", null=True, db_column="parent_id"
    )

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(Location.deleted != True)

    def should_skip(self) -> bool:
        return self.deleted

    @classmethod
    def make(
        cls,
        name: str,
        region: Region,
        period: Period,
        comment: Optional[str] = None,
        stratigraphic_unit: Optional[StratigraphicUnit] = None,
    ) -> "Location":
        return cls.create(
            name=name,
            min_period=period,
            max_period=period,
            region=region,
            comment=comment,
            stratigraphic_unit=stratigraphic_unit,
        )

    @classmethod
    def create_interactively(
        cls,
        name: Optional[str] = None,
        region: Optional[Region] = None,
        period: Optional[Period] = None,
        comment: Optional[str] = None,
        **kwargs: Any,
    ) -> "Location":
        if name is None:
            name = getinput.get_line("name> ")
        assert name is not None
        if region is None:
            region = cls.get_value_for_foreign_key_field_on_class(
                "region", allow_none=False
            )
        if period is None:
            period = cls.get_value_for_foreign_key_field_on_class("min_period")
        result = cls.make(
            name=name, region=region, period=period, comment=comment, **kwargs
        )
        result.fill_required_fields()
        return result

    def __repr__(self) -> str:
        age_str = ""
        if self.stratigraphic_unit is not None:
            age_str += self.stratigraphic_unit.name
        if self.max_period is not None:
            if self.stratigraphic_unit is not None:
                age_str += "; "
            age_str += self.max_period.name
            if self.min_period is not None and self.min_period != self.max_period:
                age_str += "–%s" % self.min_period.name
        if self.min_age is not None and self.max_age is not None:
            age_str += f"; {self.max_age}–{self.min_age}"
        if self.tags:
            age_str += "; " + ", ".join(repr(tag) for tag in self.tags)
        return f"{self.name} ({age_str}), {self.region.name}"

    def sort_key(self) -> Any:
        return (
            period_sort_key(self.min_period),
            period_sort_key(self.max_period),
            self.name,
        )

    def display(
        self,
        full: bool = False,
        organized: bool = False,
        *,
        include_occurrences: bool = False,
        depth: int = 0,
        file: IO[str] = sys.stdout,
    ) -> None:
        file.write("{}{}\n".format(" " * (depth + 4), repr(self)))
        if self.comment:
            space = " " * (depth + 12)
            file.write(f"{space}Comment: {self.comment}\n")
        type_locs = list(self.type_localities)
        models.name.write_type_localities(
            type_locs, depth=depth, full=full, organized=organized, file=file
        )
        if include_occurrences:
            taxa = list(self.taxa)
            if not taxa:
                return
            file.write("{}Occurrences:\n".format(" " * (depth + 8)))
            if organized:
                models.taxon.display_organized(
                    [(str(occ), occ.taxon) for occ in taxa], depth=depth, file=file
                )
            else:
                for occurrence in sorted(taxa, key=lambda occ: occ.taxon.valid_name):
                    file.write("{}{}\n".format(" " * (depth + 12), occurrence))

    def merge(self, other: "Location") -> None:
        self.reassign_references(other)
        self.deleted = LocationStatus.alias  # type: ignore
        self.parent = other

    def reassign_references(self, other: "Location") -> None:
        print(f"{self}: reassign references to {other}")
        for taxon in self.type_localities:
            taxon.type_locality = other
        for occ in self.taxa:
            occ.location = other

    def set_period(self, period: Optional[Period]) -> None:
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
        yield "max_period"
        yield "min_period"
        yield "stratigraphic_unit"
        yield "region"

    def has_tag(self, tag_cls: Union[adt.ADT, Type[adt.ADT]]) -> bool:
        tag_id = tag_cls._tag
        for tag in self.get_raw_tags_field("tags"):
            if tag[0] == tag_id:
                return True
        return False

    def add_tag(self, tag: adt.ADT) -> None:
        if self.tags is None:
            self.tags = (tag,)
        else:
            self.tags = self.tags + (tag,)

    def is_in_region(self, query: Region) -> bool:
        region = self.region
        while region is not None:
            if region == query:
                return True
            region = region.parent
        return False

    def is_empty(self) -> bool:
        if self.taxa.count():
            return False
        if self.type_localities.count():
            return False
        return True

    def lint(self) -> bool:
        if self.status == LocationStatus.alias and not self.parent:
            print(f"{self}: alias location has no parent")
            return False
        if self.status != LocationStatus.valid and not self.is_empty():
            print(f"{self}: deleted location has references")
            return False
        return True

    @classmethod
    def fix_references(cls) -> None:
        for alias in cls.select_valid().filter(cls.status == LocationStatus.alias):
            if not alias.is_empty() and alias.parent:
                alias.reassign_references(alias.parent)

    @classmethod
    def get_or_create_general(cls, region: Region, period: Period) -> "Location":
        if period.name == "Recent":
            name = region.name
        elif period.name == "Phanerozoic":
            name = f"{region.name} fossil"
        elif period.name == "Pleistocene":
            name = f"{region.name} Pleistocene"
        else:
            name = f"{period.name} ({region.name})"
        objs = list(Location.select_valid().filter(Location.name == name))
        if objs:
            return objs[0]  # should only be one

        objs = list(
            Location.select().filter(Location.name == name, Location.deleted == True)
        )
        if objs:
            obj = objs[0]
            obj.deleted = False
            print(f"Resurrected {obj}")
            return obj

        else:
            obj = cls.make(name=name, region=region, period=period)
            if not (period.name == "Recent" and region.children.count() == 0):
                obj.tags = [LocationTag.General]
            print(f"Created {obj}")
            return obj

    @classmethod
    def autodelete(cls, dry_run: bool = False) -> None:
        for loc in cls.select_valid():
            loc.maybe_autodelete(dry_run=dry_run)

    def maybe_autodelete(self, dry_run: bool = True) -> None:
        if not self.is_empty():
            return
        print(f"Autodeleting {self!r}")
        if not dry_run:
            self.deleted = LocationStatus.deleted  # type: ignore
            self.save()

    @classmethod
    def get_interactive_creators(cls) -> Dict[str, Callable[[], Any]]:
        def callback() -> Optional[Location]:
            region = models.Region.getter(None).get_one("region> ")
            if region is None:
                return None
            period = models.Period.getter(None).get_one("period> ")
            if period is None:
                return None
            return cls.get_or_create_general(region, period)

        return {**super().get_interactive_creators(), "u": callback}


class LocationTag(adt.ADT):
    # General locality; should be simplified if possible.
    General(tag=1)  # type: ignore

    # Locality identifiers in other databases

    # Paleobiology Database
    PBDB(id=str, tag=2)  # type: ignore

    # {North America Tertiary-localities.pdf}, appendix to
    # Evolution of Tertiary Mammals of North America
    ETMNA(id=str, tag=3)  # type: ignore

    # Neogene of the Old World database
    NOW(id=str, tag=4)  # type: ignore
