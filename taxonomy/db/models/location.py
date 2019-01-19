import sys
from typing import Any, IO, Optional

from peewee import BooleanField, CharField, ForeignKeyField, IntegerField, TextField

from .. import constants
from ... import events, getinput

from .base import BaseModel
from .article import Article
from .period import Period
from .region import Region


class Location(BaseModel):
    creation_event = events.Event["Location"]()
    save_event = events.Event["Location"]()
    label_field = "name"

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
        if comment is None:
            comment = getinput.get_line("comment> ")
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
        return f"{self.name} ({age_str}), {self.region.name}"

    def display(
        self, full: bool = False, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        file.write("{}{}\n".format(" " * (depth + 4), repr(self)))
        space = " " * (depth + 12)
        if self.comment:
            file.write(f"{space}Comment: {self.comment}\n")
        type_locs = list(self.type_localities)
        if type_locs:
            file.write("{}Type localities:\n".format(" " * (depth + 8)))
            for nam in type_locs:
                file.write(f"{space}{nam}\n")
        if full:
            self.display_organized(depth=depth, file=file)
        else:
            for occurrence in sorted(self.taxa, key=lambda occ: occ.taxon.valid_name):
                file.write("{}{}\n".format(" " * (depth + 8), occurrence))

    def display_organized(self, depth: int = 0, file: IO[str] = sys.stdout) -> None:
        taxa = sorted(
            ((occ, occ.taxon.ranked_parents()) for occ in self.taxa),
            key=lambda pair: (
                "" if pair[1][0] is None else pair[1][0].valid_name,
                "" if pair[1][1] is None else pair[1][1].valid_name,
                pair[0].taxon.valid_name,
            ),
        )
        current_order = None
        current_family = None
        for occ, (order, family) in taxa:
            if order != current_order:
                current_order = order
                if order is not None:
                    file.write("{}{}\n".format(" " * (depth + 8), order))
            if family != current_family:
                current_family = family
                if family is not None:
                    file.write("{}{}\n".format(" " * (depth + 12), family))
            file.write("{}{}\n".format(" " * (depth + 16), occ))

    def make_local_unit(
        self, name: Optional[str] = None, parent: Optional[Period] = None
    ) -> Period:
        if name is None:
            name = self.name
        period = Period.make(
            name,
            constants.PeriodSystem.local_unit,
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
