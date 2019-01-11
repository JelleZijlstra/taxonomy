import collections
import sys
from typing import IO, Dict, Iterable, List, Optional

from peewee import CharField, ForeignKeyField

from .. import constants, models
from ... import getinput

from .base import BaseModel, EnumField


class Region(BaseModel):
    label_field = "name"

    name = CharField()
    comment = CharField(null=True)
    parent = ForeignKeyField(
        "self", related_name="children", db_column="parent_id", null=True
    )
    kind = EnumField(constants.RegionKind)

    @classmethod
    def make(
        cls, name: str, kind: constants.RegionKind, parent: Optional["Region"] = None
    ) -> "Region":
        region = cls.create(name=name, kind=kind, parent=parent)
        models.Location.make(
            name=name,
            period=models.Period.filter(models.Period.name == "Recent").get(),
            region=region,
        )
        return region

    def __repr__(self) -> str:
        out = self.name
        if self.parent:
            out += ", %s" % self.parent.name
        out += " (%s)" % self.kind
        return out

    def display(
        self, full: bool = False, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        getinput.flush()
        file.write("{}{}\n".format(" " * (depth + 4), repr(self)))
        if self.comment:
            file.write("{}Comment: {}\n".format(" " * (depth + 12), self.comment))
        for location in self.locations:
            location.display(full=full, depth=depth + 4, file=file)
        for child in self.children:
            child.display(full=full, depth=depth + 4, file=file)

    def get_location(self) -> "models.Location":
        """Returns the corresponding Recent Location."""
        return models.Location.get(region=self, name=self.name, deleted=False)

    def all_parents(self) -> Iterable["Region"]:
        """Returns all parent regions of this region."""
        if self.parent is not None:
            yield self.parent
            yield from self.parent.all_parents()

    def has_collections(self) -> bool:
        for _ in self.collections:
            return True
        return any(child.has_collections() for child in self.children)

    def display_collections(
        self, full: bool = False, only_nonempty: bool = True, depth: int = 0
    ) -> None:
        if only_nonempty and not self.has_collections():
            return
        print(" " * depth + self.name)
        by_city: Dict[str, List["models.Collection"]] = collections.defaultdict(list)
        cities = set()
        for collection in sorted(self.collections, key=lambda c: c.label):
            by_city[collection.city or ""].append(collection)
            cities.add(collection.city)
        if cities == {None}:
            for collection in by_city[""]:
                collection.display(full=full, depth=depth + 4)
        else:
            for city, colls in sorted(by_city.items()):
                print(" " * (depth + 4) + city)
                for collection in colls:
                    collection.display(full=full, depth=depth + 8)
        for child in sorted(self.children, key=lambda c: c.name):
            child.display_collections(
                full=full, only_nonempty=only_nonempty, depth=depth + 4
            )

    def has_periods(self) -> bool:
        for _ in self.periods:
            return True
        return any(child.has_periods() for child in self.children)

    def display_periods(self, full: bool = False, depth: int = 0) -> None:
        if not self.has_periods():
            return
        print(" " * depth + self.name)
        for period in sorted(self.periods, key=lambda p: p.name):
            if full:
                period.display(depth=depth + 4)
            else:
                print(" " * (depth + 4) + period.name)
        for child in sorted(self.children, key=lambda c: c.name):
            child.display_periods(full=full, depth=depth + 4)

    def add_cities(self) -> None:
        for collection in self.collections.filter(models.Collection.city == None):
            collection.display()
            collection.fill_field("city")
        for child in self.children:
            child.add_cities()