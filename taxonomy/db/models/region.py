import collections
from functools import lru_cache
import sys
from typing import IO, Dict, Iterable, List, Optional

from peewee import CharField, ForeignKeyField

from .. import constants, models
from ..derived_data import DerivedField
from ... import events, getinput

from .base import BaseModel, EnumField, get_tag_based_derived_field


class Region(BaseModel):
    creation_event = events.Event["Region"]()
    save_event = events.Event["Region"]()
    label_field = "name"
    call_sign = "R"

    name = CharField()
    comment = CharField(null=True)
    parent = ForeignKeyField(
        "self", related_name="children", db_column="parent_id", null=True
    )
    kind = EnumField(constants.RegionKind)

    derived_fields = [
        DerivedField("has_collections", bool, lambda region: region.has_collections()),
        DerivedField(
            "has_citation_groups", bool, lambda region: region.has_citation_groups()
        ),
        DerivedField("has_locations", bool, lambda region: region.has_locations()),
        DerivedField("has_periods", bool, lambda region: region.has_periods()),
        DerivedField("has_type_localities", bool, lambda region: not region.is_empty()),
        DerivedField(
            "has_associated_people", bool, lambda region: region.has_associated_people()
        ),
        get_tag_based_derived_field(
            "associated_people",
            lambda: models.Person,
            "tags",
            lambda: models.tags.PersonTag.ActiveRegion,
            1,
        ),
    ]

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

    def get_general_localities(self) -> List["models.Location"]:
        name_field = models.Location.name
        my_name = self.name
        return models.Location.bfind(
            (name_field == my_name)
            | (name_field == f"{my_name} Pleistocene")
            | (name_field == f"{my_name} fossil")
            | (name_field.endswith(f"({my_name})"))
        )

    def rename(self, new_name: Optional[str] = None) -> None:
        old_name = self.name
        if new_name is None:
            new_name = self.getter("name").get_one_key(
                default=old_name, allow_empty=False
            )

        for loc in self.get_general_localities():
            if loc.name.endswith(f"({old_name})"):
                loc_name = loc.name.replace(f"({old_name})", f"({new_name})")
            elif loc.name == old_name:
                loc_name = new_name
            elif loc.name == f"{old_name} fossil":
                loc_name = f"{new_name} fossil"
            elif loc.name == f"{old_name} Pleistocene":
                loc_name = f"{new_name} Pleistocene"
            else:
                print("Skipping unrecognized name", loc.name)
                continue
            print(f"Renaming {loc.name!r} -> {loc_name!r}")
            loc.name = loc_name

        self.name = new_name

    def display(
        self,
        full: bool = False,
        depth: int = 0,
        file: IO[str] = sys.stdout,
        children: bool = True,
        skip_empty: bool = True,
        locations: bool = True,
    ) -> None:
        if skip_empty and self.is_empty():
            return
        getinput.flush()
        file.write("{}{}\n".format(" " * (depth + 4), repr(self)))
        if self.comment:
            file.write("{}Comment: {}\n".format(" " * (depth + 12), self.comment))
        if locations:
            for location in self.sorted_locations():
                if skip_empty and location.type_localities.count() == 0:
                    continue
                location.display(full=full, depth=depth + 4, file=file)
        if children:
            for child in self.sorted_children():
                child.display(
                    full=full,
                    depth=depth + 4,
                    file=file,
                    skip_empty=skip_empty,
                    locations=locations,
                )

    def display_without_stratigraphy(
        self,
        full: bool = False,
        depth: int = 0,
        file: IO[str] = sys.stdout,
        skip_empty: bool = False,
    ) -> None:
        for location in self.sorted_locations():
            if skip_empty and location.type_localities.count() == 0:
                continue
            if location.stratigraphic_unit is not None:
                continue
            if location.has_tag(models.location.LocationTag.General):
                continue
            location.display(full=full, depth=depth + 4, file=file)

    def is_empty(self) -> bool:
        for loc in self.locations.filter(models.Location.deleted != True):
            if loc.type_localities.count() > 0:
                return False
        for child in self.children:
            if not child.is_empty():
                return False
        return True

    def sorted_children(self) -> List["Region"]:
        return sorted(self.children, key=lambda c: c.name)

    def sorted_locations(self) -> List["models.Location"]:
        return sorted(
            self.locations.filter(models.Location.deleted != True),
            key=models.Location.sort_key,
        )

    def get_location(self) -> "models.Location":
        """Returns the corresponding Recent Location."""
        return models.Location.get(region=self, name=self.name, deleted=False)

    def all_parents(self) -> Iterable["Region"]:
        """Returns all parent regions of this region."""
        if self.parent is not None:
            yield self.parent
            yield from self.parent.all_parents()

    def all_citation_groups(self) -> Iterable["models.CitationGroup"]:
        yield from self.citation_groups
        for child in self.children:
            yield from child.all_citation_groups()

    def has_citation_groups(self) -> bool:
        for _ in self.citation_groups:
            return True
        return any(child.has_citation_groups() for child in self.children)

    def display_citation_groups(
        self, full: bool = False, only_nonempty: bool = True, depth: int = 0
    ) -> None:
        if only_nonempty and not self.has_citation_groups():
            return
        print(" " * depth + self.name)
        by_type: Dict[
            constants.ArticleType, List["models.CitationGroup"]
        ] = collections.defaultdict(list)
        for group in sorted(self.citation_groups, key=lambda cg: cg.name):
            by_type[group.type].append(group)
        for typ, groups in sorted(by_type.items(), key=lambda pair: pair[0].name):
            print(f"{' ' * (depth + 4)}{typ.name}")
            for group in groups:
                if not group.deleted:
                    group.display(full=full, include_articles=full, depth=depth + 8)
        for child in self.sorted_children():
            child.display_citation_groups(
                full=full, only_nonempty=only_nonempty, depth=depth + 4
            )

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
        for child in self.sorted_children():
            child.display_collections(
                full=full, only_nonempty=only_nonempty, depth=depth + 4
            )

    def has_locations(self) -> bool:
        for _ in self.locations:
            return True
        return any(child.has_locations() for child in self.children)

    def has_associated_people(self) -> bool:
        if self.get_raw_derived_field("associated_people"):
            return True
        return any(child.has_associated_people() for child in self.children)

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
        for child in self.sorted_children():
            child.display_periods(full=full, depth=depth + 4)

    def add_cities(self) -> None:
        for collection in self.collections.filter(models.Collection.city == None):
            collection.display()
            collection.fill_field("city")
        for child in self.children:
            child.add_cities()

    @lru_cache(maxsize=2048)
    def has_parent(self, parent: "Region") -> bool:
        if self == parent:
            return True
        elif self.parent is None:
            return False
        else:
            return self.parent.has_parent(parent)
