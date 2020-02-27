import sys
from typing import Any, IO, Iterable, Optional, Type, Union

from peewee import BooleanField, CharField, ForeignKeyField, IntegerField, TextField

from .. import constants, models
from ... import adt, events, getinput

from .base import BaseModel, ADTField
from .article import Article
from .period import Period, period_sort_key
from .region import Region


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
        Period,
        related_name="locations_stratigraphy",
        db_column="stratigraphic_unit_id",
        null=True,
    )
    region = ForeignKeyField(Region, related_name="locations", db_column="region_id")
    comment = CharField()
    latitude = CharField()
    longitude = CharField()
    location_detail = TextField()
    age_detail = TextField()
    source = ForeignKeyField(Article, related_name="locations", null=True)
    deleted = BooleanField(default=False)
    tags = ADTField(lambda: LocationTag, null=True)

    @classmethod
    def select_valid(cls, *args: Any) -> Any:
        return cls.select(*args).filter(Location.deleted != True)

    @classmethod
    def make(
        cls,
        name: str,
        region: Region,
        period: Period,
        comment: Optional[str] = None,
        stratigraphic_unit: Optional[Period] = None,
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
            region = cls.get_value_for_foreign_key_field_on_class("region")
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

    def make_local_unit(
        self, name: Optional[str] = None, parent: Optional[Period] = None
    ) -> Period:
        if name is None:
            name = self.name
        period = Period.make(
            name,
            constants.PeriodRank.local_unit,
            parent=parent,
            min_age=self.min_age,
            max_age=self.max_age,
            min_period=self.min_period,
            max_period=self.max_period,
        )
        self.min_period = self.max_period = period
        self.save()
        return period

    def merge(self, other: "Location") -> None:
        for taxon in self.type_localities:
            taxon.type_locality = other
        for occ in self.taxa:
            occ.location = other
        new_comment = f"Merged into {other} (L#{other.id})"
        if not self.comment:
            self.comment = new_comment
        else:
            self.comment = f"{self.comment} – {new_comment}"
        self.deleted = True

    def set_period(self, period: Period) -> None:
        self.min_period = self.max_period = period

    def fill_field(self, field: str) -> None:
        if field == "period":
            period = self.get_value_for_foreign_class(
                "period", Period, self.min_period, self.get_adt_callbacks()
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

    def has_tag(self, tag: Union[adt.ADT, Type[adt.ADT]]) -> bool:
        if self.tags is None:
            return False
        if isinstance(tag, type):
            return any(isinstance(my_tag, tag) for my_tag in self.tags)
        else:
            return any(my_tag is tag for my_tag in self.tags)

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
