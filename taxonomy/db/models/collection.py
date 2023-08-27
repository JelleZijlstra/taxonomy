from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any, NotRequired

from peewee import BooleanField, CharField, ForeignKeyField
from typing_extensions import Self

from taxonomy.apis.cloud_search import SearchField, SearchFieldType

from ... import adt, events, getinput
from .. import constants, models
from .article import Article
from .base import ADTField, BaseModel, LintConfig, get_tag_based_derived_field
from .region import Region
from .taxon import Taxon


class Collection(BaseModel):
    creation_event = events.Event["Collection"]()
    save_event = events.Event["Collection"]()
    label_field = "label"
    grouping_field = "city"
    call_sign = "C"

    label = CharField()
    name = CharField()
    location = ForeignKeyField(
        Region, related_name="collections", db_column="location_id"
    )
    comment = CharField(null=True)
    city = CharField(null=True)
    removed = BooleanField(default=False)
    tags = ADTField(lambda: CollectionTag, null=True)
    parent = ForeignKeyField("self", related_name="children", null=True)

    derived_fields = [
        get_tag_based_derived_field(
            "associated_people",
            lambda: models.Person,
            "tags",
            lambda: models.tags.PersonTag.Institution,
            1,
        ),
        get_tag_based_derived_field(
            "probable_specimens",
            lambda: models.Name,
            "type_tags",
            lambda: models.name.TypeTag.ProbableRepository,
            1,
        ),
        get_tag_based_derived_field(
            "shared_specimens",
            lambda: models.Name,
            "type_tags",
            lambda: models.name.TypeTag.Repository,
            1,
        ),
        get_tag_based_derived_field(
            "former_specimens",
            lambda: models.Name,
            "type_tags",
            lambda: models.name.TypeTag.FormerRepository,
            1,
        ),
        get_tag_based_derived_field(
            "future_specimens",
            lambda: models.Name,
            "type_tags",
            lambda: models.name.TypeTag.FutureRepository,
            1,
        ),
        get_tag_based_derived_field(
            "extra_specimens",
            lambda: models.Name,
            "type_tags",
            lambda: models.name.TypeTag.ExtraRepository,
            1,
        ),
    ]
    search_fields = [
        SearchField(SearchFieldType.text, "name"),
        SearchField(SearchFieldType.literal, "label"),
        SearchField(SearchFieldType.text, "comment", highlight_enabled=True),
        SearchField(SearchFieldType.text, "city"),
    ]

    def get_search_dicts(self) -> list[dict[str, Any]]:
        data = {
            "name": self.name,
            "label": self.label,
            "comment": self.comment,
            "city": self.city,
        }
        return [data]

    def __repr__(self) -> str:
        city = f", {self.city}" if self.city else ""
        return f"{self.name}{city} ({self.label})"

    def edit(self) -> None:
        self.fill_field("tags")

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        for tag in self.tags:
            if isinstance(tag, CollectionTag.SpecimenRegex):
                try:
                    re.compile(tag.regex)
                except re.error:
                    yield f"{self}: invalid specimen regex {tag.regex!r}"
        if CollectionTag.MustHaveSpecimenLinks in self.tags or any(
            isinstance(tag, CollectionTag.ConditionalMustHaveSpecimenLinks)
            for tag in self.tags
        ):
            if not any(
                isinstance(tag, CollectionTag.SpecimenLinkPrefix) for tag in self.tags
            ):
                yield f"{self}: must have SpecimenLinkPrefix tag"

    @classmethod
    def by_label(cls, label: str) -> Collection:
        colls = list(cls.filter(cls.label == label))
        if len(colls) == 1:
            return colls[0]
        else:
            raise ValueError(f"found {colls} with label {label}")

    @classmethod
    def get_or_create(
        cls, label: str, name: str, location: Region, comment: str | None = None
    ) -> Collection:
        try:
            return cls.by_label(label)
        except ValueError:
            return cls.create(
                label=label, name=name, location=location, comment=comment
            )

    def get_required_fields(self) -> list[str]:
        return ["label", "name", "location", "city"]

    @classmethod
    def create_interactively(
        cls,
        label: str | None = None,
        name: str | None = None,
        location: Region | None = None,
        parent: Self | None = None,
        **kwargs: Any,
    ) -> Self:
        if label is None:
            label = getinput.get_line(
                "label> ", default=parent.label if parent is not None else ""
            )
        if name is None:
            name = getinput.get_line(
                "name> ", default=parent.name if parent is not None else ""
            )
        if location is None:
            location = cls.get_value_for_foreign_key_field_on_class(
                "location", allow_none=False
            )
        obj = cls.create(
            label=label, name=name, location=location, parent=parent, **kwargs
        )
        obj.fill_required_fields()
        return obj

    def display(
        self, full: bool = False, depth: int = 0, organized: bool = False
    ) -> None:
        city = f", {self.city}" if self.city else ""
        print(" " * depth + f"{self!r}{city}, {self.location}")
        if self.comment:
            print(" " * (depth + 4) + f"Comment: {self.comment}")
        if full:
            if organized:
                models.taxon.display_organized(
                    [
                        (str(f"{nam} (type: {nam.type_specimen})"), nam.taxon)
                        for nam in self.type_specimens
                    ],
                    depth=depth,
                )
            else:
                for nam in sorted(
                    self.type_specimens, key=lambda nam: nam.taxon.valid_name
                ):
                    print(" " * (depth + 4) + f"{nam} (type: {nam.type_specimen})")
                    for tag in nam.type_tags or ():
                        if isinstance(tag, models.name.TypeTag.CollectionDetail):
                            print(" " * (depth + 8) + str(tag))

    def get_partial(
        self, display: bool = False
    ) -> tuple[list[models.name.Name], list[models.name.Name]]:
        multiple = []
        probable_repo = []
        for nam in models.Name.with_tag_of_type(models.name.TypeTag.Repository):
            for tag in nam.get_tags(nam.type_tags, models.name.TypeTag.Repository):
                if tag.repository == self:
                    multiple.append(nam)
                    if display:
                        print(tag)
                        nam.display()
                    break

        for nam in models.Name.with_tag_of_type(models.name.TypeTag.ProbableRepository):
            for tag in nam.get_tags(
                nam.type_tags, models.name.TypeTag.ProbableRepository
            ):
                if tag.repository == self:
                    probable_repo.append(nam)
                    if display:
                        print(tag)
                        nam.display()
                    break
        return multiple, probable_repo

    def merge(self, other: Collection | None = None) -> None:
        if other is None:
            other = Collection.getter(None).get_one("merge target> ")
            if other is None:
                return
        for nam in self.type_specimens:
            nam.collection = other
        self.parent = other
        self.removed = True

    def get_redirect_target(self) -> Collection | None:
        if self.removed:
            return self.parent
        return None

    def is_invalid(self) -> bool:
        return self.removed

    def must_use_children(self) -> bool:
        return any(tag is CollectionTag.MustUseChildrenCollection for tag in self.tags)

    def must_have_specimen_links(self, nam: models.Name) -> bool:
        for tag in self.tags:
            if tag is CollectionTag.MustHaveSpecimenLinks:
                return True
            if isinstance(tag, CollectionTag.ConditionalMustHaveSpecimenLinks):
                if tag.regex and nam.type_specimen:
                    if not re.fullmatch(tag.regex, nam.type_specimen):
                        continue
                if tag.taxon:
                    if not nam.taxon.is_child_of(tag.taxon):
                        continue
                if tag.age:
                    if nam.taxon.age is not tag.age:
                        continue
                return True
        return False

    def is_valid_specimen_link(self, link: str) -> bool:
        prefixes = tuple(
            tag.prefix
            for tag in self.tags
            if isinstance(tag, CollectionTag.SpecimenLinkPrefix)
        )
        if not prefixes:
            return True
        return link.startswith(prefixes)

    def print_specimen_links(self) -> None:
        for nam in self.type_specimens:
            for tag in nam.type_tags:
                if isinstance(tag, models.name.TypeTag.TypeSpecimenLink):
                    print(tag.url)

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        return {
            **super().get_adt_callbacks(),
            "add_child": lambda: Collection.create_interactively(
                parent=self, city=self.city, location=self.location
            ),
            "lint_names": lambda: models.Name.lint_all(query=self.type_specimens),
            "print_specimen_links": self.print_specimen_links,
        }


class CollectionTag(adt.ADT):
    CollectionDatabase(citation=Article, comment=str, tag=1)  # type: ignore
    TypeCatalog(citation=Article, coverage=str, tag=2)  # type: ignore
    SpecimenRegex(regex=str, tag=3)  # type: ignore
    MustUseChildrenCollection(tag=4)  # type: ignore
    ChildRule(collection=Collection, regex=str, taxon=NotRequired[Taxon], age=NotRequired[constants.AgeClass], tag=5)  # type: ignore
    MustHaveSpecimenLinks(tag=6)  # type: ignore
    ConditionalMustHaveSpecimenLinks(regex=str, taxon=NotRequired[Taxon], age=NotRequired[constants.AgeClass], tag=7)  # type: ignore
    # To be counted as a specimen link for this collection, a link must have this prefix.
    # Multiple copies of this tag may be present.
    SpecimenLinkPrefix(prefix=str, tag=8)  # type: ignore
