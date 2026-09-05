import collections
import sys
from collections.abc import Iterable
from typing import IO, Any, ClassVar, NotRequired, Self, cast

from clirm import Field

from taxonomy import adt, events, getinput
from taxonomy.apis.cloud_search import SearchField, SearchFieldType
from taxonomy.db import constants, models
from taxonomy.db.constants import Managed, Markdown
from taxonomy.db.derived_data import DerivedField

from .base import ADTField, BaseModel, LintConfig, get_tag_based_derived_field
from .lint_types import LintResult


class Region(BaseModel):
    creation_event = events.Event["Region"]()
    save_event = events.Event["Region"]()
    label_field = "name"
    call_sign = "R"
    clirm_table_name = "region"

    name = Field[str]()
    comment = Field[str | None]()
    parent = Field[Self | None]("parent_id", related_name="children")
    kind = Field[constants.RegionKind]()
    tags = ADTField["RegionTag"](is_ordered=False)

    derived_fields: ClassVar[list[DerivedField[Any]]] = [
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
        DerivedField(
            "has_stratigraphic_units",
            bool,
            lambda region: region.has_stratigraphic_units(),
        ),
        get_tag_based_derived_field(
            "associated_people",
            lambda: models.Person,
            "tags",
            lambda: models.tags.PersonTag.ActiveRegion,
            1,
        ),
    ]
    search_fields: ClassVar[list[SearchField]] = [
        SearchField(SearchFieldType.text, "name"),
        SearchField(SearchFieldType.literal, "kind"),
    ]

    def get_search_dicts(self) -> list[dict[str, Any]]:
        return [{"name": self.name, "kind": self.kind.name}]

    def __repr__(self) -> str:
        if self.kind is constants.RegionKind.redirect:
            return f"{self.name} (redirect to {self.parent})"
        if self.kind is constants.RegionKind.deleted:
            return f"{self.name} (deleted)"
        out = self.name
        if self.parent:
            out += f", {self.parent.name}"
        out += f" ({self.kind.name})"
        return out

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(
            cls.kind != constants.RegionKind.redirect,
            cls.kind != constants.RegionKind.deleted,
        )

    def get_redirect_target(self) -> Self | None:
        if self.kind is constants.RegionKind.redirect:
            return self.parent
        return None

    def is_invalid(self) -> bool:
        return self.kind in {
            constants.RegionKind.redirect,
            constants.RegionKind.deleted,
        }

    def should_skip(self) -> bool:
        return self.kind in {
            constants.RegionKind.redirect,
            constants.RegionKind.deleted,
        }

    def lint_invalid(self, cfg: LintConfig) -> Iterable[LintResult]:
        if self.kind is constants.RegionKind.redirect and self.parent is None:
            yield "redirect Region has no target"
        if self.tags:
            yield "invalid Region has tags"

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        return {
            **super().get_adt_callbacks(),
            "merge": self.merge,
            "remove": self.remove,
            "display_collections": self.display_collections,
            "display_citation_groups": self.display_citation_groups,
            "display_periods": self.display_periods,
            "display_type_localities": self.display_type_localities,
            "display_children": self.display_children,
        }

    def get_general_localities(self) -> list[models.Location]:
        name_field = models.Location.name
        my_name = self.name
        return models.Location.bfind(
            models.Location.region == self,
            (name_field == my_name)
            | (name_field == f"{my_name} Pleistocene")
            | (name_field == f"{my_name} fossil")
            | (name_field.endswith(f"({my_name})")),
        )

    def edit(self) -> None:
        self.fill_field("tags")

    def rename(self, new_name: str | None = None) -> None:
        old_name = self.name
        if new_name is None:
            new_name = self.getter("name").get_one_key(
                default=old_name, allow_empty=False
            )
        if new_name is None:
            return

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
        *,
        full: bool = False,
        depth: int = 0,
        file: IO[str] = sys.stdout,
        children: bool = False,
        skip_empty: bool = True,
        locations: bool = False,
    ) -> None:
        if skip_empty and self.is_empty():
            return
        getinput.flush()
        file.write("{}{}\n".format(" " * (depth + 4), repr(self)))
        if self.comment:
            file.write("{}Comment: {}\n".format(" " * (depth + 12), self.comment))
        if locations or full:
            for location in self.sorted_locations():
                if skip_empty and location.type_localities.count() == 0:
                    continue
                location.display(full=full, depth=depth + 4, file=file)
        if children or full:
            for child in self.sorted_children():
                child.display(
                    full=full,
                    depth=depth + 4,
                    file=file,
                    children=children,
                    skip_empty=skip_empty,
                    locations=locations,
                )

    def display_children(
        self, *, full: bool = False, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        self.display(full=full, depth=depth, file=file, children=True, skip_empty=False)

    def display_type_localities(
        self, *, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        self.display(full=False, depth=depth, file=file, children=True, locations=True)

    def display_without_stratigraphy(
        self,
        *,
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
            if location.has_tag(models.tags.LocationTag.General):
                continue
            location.display(full=full, depth=depth + 4, file=file)

    def is_empty(self) -> bool:
        for loc in self.locations.filter(
            models.Location.deleted != models.location.LocationStatus.deleted
        ):
            if loc.type_localities.count() > 0:
                return False
        return all(
            child.is_empty() for child in Region.add_validity_check(self.children)
        )

    def has_children(self) -> bool:
        for _ in Region.add_validity_check(self.children):
            return True
        return False

    def has_tag(self, tag_cls: adt.ADT | type[adt.ADT]) -> bool:
        tag_id = tag_cls._tag
        return any(tag._tag == tag_id for tag in self.tags)

    def add_tag(self, tag: adt.ADT) -> None:
        self.tags = (*self.tags, tag)  # type: ignore[assignment]

    def sorted_children(self) -> list[Region]:
        return sorted(Region.add_validity_check(self.children), key=lambda c: c.name)

    def sorted_locations(self) -> list[models.Location]:
        return sorted(
            self.locations.filter(
                models.Location.deleted != models.location.LocationStatus.deleted
            ),
            key=models.Location.sort_key,
        )

    def get_location(self) -> models.Location:
        """Returns the corresponding Recent Location."""
        return models.Location.get(
            region=self, name=self.name, deleted=models.location.LocationStatus.valid
        )

    def all_parents(self) -> Iterable[Region]:
        """Returns all parent regions of this region."""
        if self.parent is not None:
            yield self.parent
            yield from self.parent.all_parents()

    def parent_of_kind(self, kind: constants.RegionKind) -> Region | None:
        if self.kind is kind:
            return self
        for parent in self.all_parents():
            if parent.kind is kind:
                return parent
        return None

    def all_citation_groups(self) -> Iterable[models.CitationGroup]:
        yield from self.citation_groups
        for child in Region.add_validity_check(self.children):
            yield from child.all_citation_groups()

    def has_citation_groups(self, type: constants.ArticleType | None = None) -> bool:
        for cg in self.citation_groups:
            if type is None or cg.type is type:
                return True
        return any(
            child.has_citation_groups(type)
            for child in Region.add_validity_check(self.children)
        )

    def display_citation_groups(
        self,
        *,
        full: bool = False,
        only_nonempty: bool = True,
        depth: int = 0,
        type: constants.ArticleType | None = None,
    ) -> None:
        if only_nonempty and not self.has_citation_groups(type=type):
            return
        print(" " * depth + self.name)
        by_type: dict[constants.ArticleType, list[models.CitationGroup]] = (
            collections.defaultdict(list)
        )
        for group in sorted(self.citation_groups, key=lambda cg: cg.name):
            if type is not None and group.type is not type:
                continue
            by_type[group.type].append(group)
        for typ, groups in sorted(by_type.items(), key=lambda pair: pair[0].name):
            if type is None:
                print(f"{' ' * (depth + 4)}{typ.name}")
            for group in groups:
                if not group.is_invalid():
                    group.display(full=full, include_articles=full, depth=depth + 8)
        for child in self.sorted_children():
            child.display_citation_groups(
                full=full, only_nonempty=only_nonempty, depth=depth + 4, type=type
            )

    def has_collections(self) -> bool:
        for _ in self.collections:
            return True
        return any(
            child.has_collections()
            for child in Region.add_validity_check(self.children)
        )

    def has_stratigraphic_units(self) -> bool:
        for _ in self.stratigraphic_units:
            return True
        return any(
            child.has_stratigraphic_units()
            for child in Region.add_validity_check(self.children)
        )

    def display_collections(
        self, *, full: bool = False, only_nonempty: bool = True, depth: int = 0
    ) -> None:
        if only_nonempty and not self.has_collections():
            return
        print(" " * depth + self.name)
        by_city: dict[str, list[models.Collection]] = collections.defaultdict(list)
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
        return any(
            child.has_locations() for child in Region.add_validity_check(self.children)
        )

    def has_associated_people(self) -> bool:
        if self.get_raw_derived_field("associated_people"):
            return True
        return any(
            child.has_associated_people()
            for child in Region.add_validity_check(self.children)
        )

    def has_periods(self) -> bool:
        for _ in self.periods:
            return True
        return any(
            child.has_periods() for child in Region.add_validity_check(self.children)
        )

    def display_periods(self, *, full: bool = False, depth: int = 0) -> None:
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
        for child in Region.add_validity_check(self.children):
            child.add_cities()

    def merge(self, other: Self | None = None) -> None:
        """Redirect this Region to ``other`` and move ordinary references to it."""
        if other is None:
            selected_other = Region.getter(None).get_one("merge target> ")
            if selected_other is None:
                return
            other = cast(Self, selected_other)
        if other == self:
            raise ValueError("cannot merge a Region into itself")
        if self.is_invalid():
            raise ValueError("cannot merge an invalid Region")
        if other.is_invalid():
            raise ValueError("cannot merge into an invalid Region")
        if other.has_parent(self):
            raise ValueError("cannot merge a Region into one of its descendants")

        # Include invalid objects so existing aliases do not become redirect chains.
        for field, obj in tuple(self.get_direct_backrefs(include_invalid=True)):
            setattr(obj, field.attribute_name, other)
        self.parent = other
        self.kind = constants.RegionKind.redirect

    def remove(self) -> None:
        """Mark an unreferenced Region as deleted without removing its DB row."""
        if self.is_invalid():
            raise ValueError("cannot remove an invalid Region")
        references = list(self.get_direct_backrefs())
        if references:
            descriptions = ", ".join(
                f"{type(obj).__name__} {obj.id}.{field.attribute_name}"
                for field, obj in references[:10]
            )
            suffix = " ..." if len(references) > 10 else ""
            raise ValueError(
                "cannot remove a Region with valid references: "
                f"{descriptions}{suffix}"
            )
        self.kind = constants.RegionKind.deleted

    def has_parent(self, parent: Region) -> bool:
        if self == parent:
            return True
        elif self.parent is None:
            return False
        else:
            return self.parent.has_parent(parent)

    def lint(self, cfg: LintConfig) -> Iterable[LintResult]:
        from . import region_lint

        yield from region_lint.LINT.run(self, cfg)

    @classmethod
    def clear_lint_caches(cls) -> None:
        from . import region_lint

        region_lint.LINT.clear_caches()


class RegionTag(adt.ADT):
    # The Region's children cover only part of its geographic extent.
    IncompletelyDivided(tag=1)  # type: ignore[name-defined]
    # Names with a general type locality in this Region or one of its descendants
    # should be migrated to a more precise Location where possible.
    MustHavePreciseTypeLocality(tag=2)  # type: ignore[name-defined]
    # Stable OpenStreetMap identity used through Nominatim. Do not store
    # installation-specific Nominatim place_id values here.
    OpenStreetMap(  # type: ignore[name-defined]
        osm_type=str, osm_id=int, category=str, tag=3
    )
    # Suppress a reviewed Region lint while retaining the reason.
    IgnoreLint(  # type: ignore[name-defined]
        label=Managed, comment=NotRequired[Markdown], tag=4
    )
