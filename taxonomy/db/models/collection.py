from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any, ClassVar, NotRequired, Self

from clirm import Field

from taxonomy import adt, events, getinput, parsing
from taxonomy.apis.cloud_search import SearchField, SearchFieldType
from taxonomy.db import constants, helpers, models
from taxonomy.db.derived_data import DerivedField

from .article import Article
from .base import ADTField, BaseModel, LintConfig, get_tag_based_derived_field
from .region import Region
from .taxon import Taxon

# Special collection IDs
LOST_COLLECTION = 75
UNTRACED_COLLECTION = 381
MULTIPLE_COLLECTION = 366
IN_SITU_COLLECTION = 471
SPECIAL_COLLECTION_IDS = {
    LOST_COLLECTION,
    UNTRACED_COLLECTION,
    MULTIPLE_COLLECTION,
    IN_SITU_COLLECTION,
}
BMNH_COLLECTION = 5

DEFAULT_TRIPLET_REGEX = re.compile(r"\d+[a-z]?")


class Collection(BaseModel):
    creation_event = events.Event["Collection"]()
    save_event = events.Event["Collection"]()
    label_field = "label"
    grouping_field = "city"
    call_sign = "C"
    clirm_table_name = "collection"

    label = Field[str]()
    name = Field[str]()
    location = Field[Region]("location_id", related_name="collections")
    comment = Field[str | None]()
    city = Field[str | None]()
    removed = Field[bool](default=False)
    tags = ADTField["CollectionTag"](is_ordered=False)
    parent = Field[Self | None]("parent_id", related_name="children")

    derived_fields: ClassVar[list[DerivedField[Any]]] = [
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
            "guessed_specimens",
            lambda: models.Name,
            "type_tags",
            lambda: models.name.TypeTag.GuessedRepository,
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
    search_fields: ClassVar[list[SearchField]] = [
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

    def add_tag_interactively(self, tag_cls: type[CollectionTag]) -> None:
        try:
            tag = getinput.get_adt_member(
                tag_cls, completers=self.get_completers_for_adt_field("tags")
            )
        except getinput.StopException:
            return
        self.tags = (*self.tags, tag)  # type: ignore[assignment]

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        for tag in self.tags:
            if isinstance(tag, CollectionTag.SpecimenRegex):
                try:
                    re.compile(tag.regex)
                except re.error:
                    yield f"{self}: invalid specimen regex {tag}"
            if isinstance(tag, CollectionTag.CollectionCode):
                if tag.specimen_regex is not None:
                    try:
                        re.compile(tag.specimen_regex)
                    except re.error:
                        yield f"{self}: invalid specimen regex {tag}"
                if not parsing.matches_grammar(
                    tag.label, parsing.collection_code_pattern
                ):
                    yield f"{self}: invalid collection code {tag.label}"
            if tag is CollectionTag.MustUseChildrenCollection or isinstance(
                tag, CollectionTag.ChildRule
            ):
                yield f"{self}: uses deprecated tag {tag}"
        if CollectionTag.MustHaveSpecimenLinks in self.tags or any(
            isinstance(tag, CollectionTag.ConditionalMustHaveSpecimenLinks)
            for tag in self.tags
        ):
            if not any(
                isinstance(tag, CollectionTag.SpecimenLinkPrefix) for tag in self.tags
            ):
                yield f"{self}: must have SpecimenLinkPrefix tag"
        if not parsing.matches_grammar(self.label, parsing.collection_pattern):
            yield f"{self}: invalid label"

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
        self, *, full: bool = False, depth: int = 0, organized: bool = False
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
        self, *, display: bool = False
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

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(cls.removed == False)

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
                if isinstance(
                    tag,
                    (
                        models.name.TypeTag.TypeSpecimenLink,
                        models.name.TypeTag.TypeSpecimenLinkFor,
                    ),
                ):
                    print(tag.url)

    def validate_specimen(
        self, spec: models.name.type_specimen.BaseSpecimen
    ) -> str | None:
        if error := self._validate_specimen_label(spec):
            return error
        if isinstance(spec, models.name.type_specimen.SimpleSpecimen):
            if self.must_use_triplets():
                return f"collection {self} requires the use of triplets"
            return self._validate_specimen_text(spec.text)
        elif isinstance(spec, models.name.type_specimen.TripletSpecimen):
            code_tag = self._get_applicable_collection_code(spec.collection_code)
            if code_tag is None:
                return f"collection code {spec.collection_code!r} not allowed"
            if code_tag.specimen_regex:
                if not re.fullmatch(code_tag.specimen_regex, spec.catalog_number):
                    return (
                        f"catalog number {spec.catalog_number!r} does not match regex"
                        f" {code_tag.specimen_regex}"
                    )
            elif not DEFAULT_TRIPLET_REGEX.fullmatch(spec.catalog_number):
                return (
                    f"catalog number {spec.catalog_number!r} does not match default"
                    " regex"
                )
            if self.id == BMNH_COLLECTION:
                return _validate_bmnh(spec.collection_code, spec.catalog_number)
        return None

    def must_use_triplets(self) -> bool:
        return CollectionTag.MustUseTriplets in self.tags

    def _get_applicable_collection_code(
        self, code: str
    ) -> CollectionTag.CollectionCode | None:  # type: ignore[name-defined]
        for tag in self.tags:
            if isinstance(tag, CollectionTag.CollectionCode) and tag.label == code:
                return tag
        return None

    def _validate_specimen_text(self, text: str) -> str | None:
        for tag in self.tags:
            if isinstance(tag, CollectionTag.SpecimenRegex):
                if not re.fullmatch(tag.regex, text):
                    return f"does not match regex {tag.regex}"
        return None

    def _validate_specimen_label(
        self, spec: models.name.type_specimen.BaseSpecimen
    ) -> str | None:
        expected_label = self.get_expected_label()
        if expected_label is None:
            return None
        actual_label = spec.institution_code
        if actual_label != expected_label:
            return f"expected label {expected_label!r}, got {actual_label!r}"
        return None

    def get_expected_label(self) -> str | None:
        if self.id in SPECIAL_COLLECTION_IDS:
            return None
        if self.label.endswith(" collection"):
            return self.label.removesuffix(" collection")
        return self.label

    def rename_type_specimens(self, *, full: bool = False) -> None:
        if full:
            age = getinput.get_enum_member(
                constants.AgeClass, prompt="age> ", allow_empty=True
            )
            parent_taxon = Taxon.getter(None).get_one("taxon> ")
            include_regex = getinput.get_line(
                "include regex (full match)> ", allow_none=True
            )
        else:
            age = parent_taxon = include_regex = None
        to_replace = getinput.get_line("replace (regex)> ", allow_none=False)
        replace_with = getinput.get_line("replace with> ", allow_none=False)
        assert to_replace is not None
        assert replace_with is not None
        dry_run = getinput.yes_no("dry run? ")
        result = self._do_rename_type_specimens(
            to_replace=to_replace,
            replace_with=replace_with,
            dry_run=dry_run,
            age=age,
            parent_taxon=parent_taxon,
            include_regex=include_regex,
        )
        if dry_run and result > 0:
            if getinput.yes_no("Continue with actual rename? "):
                self._do_rename_type_specimens(
                    to_replace=to_replace,
                    replace_with=replace_with,
                    dry_run=False,
                    age=age,
                    parent_taxon=parent_taxon,
                    include_regex=include_regex,
                )

    def _do_rename_type_specimens(
        self,
        *,
        to_replace: str,
        replace_with: str,
        dry_run: bool,
        age: constants.AgeClass | None,
        parent_taxon: Taxon | None,
        include_regex: str | None,
    ) -> int:
        replacements = 0
        for nam in self.type_specimens.filter(models.Name.type_specimen != None):
            if age is not None and nam.taxon.age is not age:
                continue
            if parent_taxon is not None and not nam.taxon.is_child_of(parent_taxon):
                continue
            if include_regex and not re.fullmatch(include_regex, nam.type_specimen):
                continue
            new_type_specimen = re.sub(to_replace, replace_with, nam.type_specimen)
            if nam.type_specimen == new_type_specimen:
                continue
            print(f"{nam.type_specimen!r} -> {new_type_specimen!r} ({nam})")
            replacements += 1
            if not dry_run:
                old_type_specimen = nam.type_specimen
                nam.type_specimen = new_type_specimen

                def mapper(
                    tag: models.name.TypeTag,
                    old_type_specimen: str = old_type_specimen,
                    new_type_specimen: str = new_type_specimen,
                ) -> models.name.TypeTag | None:
                    if (
                        isinstance(tag, models.name.TypeTag.TypeSpecimenLinkFor)
                        and tag.specimen == old_type_specimen
                    ):
                        return models.name.TypeTag.TypeSpecimenLinkFor(
                            tag.url, new_type_specimen
                        )
                    return tag

                nam.map_type_tags(mapper)
                nam.edit_until_clean()
        print(f"{replacements} replacements made")
        return replacements

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        return {
            **super().get_adt_callbacks(),
            "add_child": lambda: Collection.create_interactively(
                parent=self, city=self.city, location=self.location
            ),
            "lint_names": lambda: models.Name.lint_all(query=self.type_specimens),
            "print_specimen_links": self.print_specimen_links,
            "merge": self.merge,
            "rename_type_specimens": self.rename_type_specimens,
            "rename_type_specimens_full": lambda: self.rename_type_specimens(full=True),
            "add_collection_code": self.add_collection_code,
        }

    def add_collection_code(self) -> None:
        self.add_tag_interactively(CollectionTag.CollectionCode)


FOSSIL_CATALOGS = [
    ("M", "mammal"),
    ("R", "reptile"),
    ("A", "bird"),
    ("E", "?paleoanthropology"),
    ("OR", "old collection"),
]


def _validate_bmnh(collection_code: str, catalog_number: str) -> str | None:
    """We allow the following formats:

    - "BMNH:Rept:1901.2.3.4"
    - "BMNH:Mamm:1901.2.3.4"
    - "BMNH:Mamm:123a"
    - "BMNH:Amph:1901.2.3.4"
    - "BMNH:Minor:1901.2.3.4"
    - "BMNH:PV:R 1234"
    - "BMNH:PV:M 1234"
    - "BMNH:PV:A 1234"
    - "BMNH:PV:OR 1234"
    - "BMNH:PV:E 1234"

    """
    if collection_code == "PV":
        # Fossil numbers: BMNH M 1234 for fossil mammals
        for catalog, label in FOSSIL_CATALOGS:
            if catalog_number.startswith(catalog):
                if not re.fullmatch(catalog + r" \d+[a-z]?", catalog_number):
                    return (
                        f"invalid fossil {label} number (should be of form"
                        f" {catalog} <number>)"
                    )
                return None
        return f"invalid fossil catalog {catalog_number!r}"

    periods = catalog_number.count(".")

    # Date-based catalog numbers: BMNH 1901.2.24.3 was cataloged on February 24, 1901
    if periods == 3:
        year, month, day, number = catalog_number.split(".")
        if not year.isdigit():
            return f"invalid year {year}"
        num_year = int(year)
        if not (1830 <= num_year <= 2100):
            return f"year {num_year} out of range"
        try:
            helpers.parse_date(year, month, day)
        except ValueError as e:
            return f"invalid date in catalog number: {e}"
        if not re.fullmatch(r"\d+([a-z]|bis)?", number):
            return f"invalid number {number}"
        return None
    # Year based catalog numbers: BMNH 1992.123 was cataloged in 1992
    elif periods == 1:
        year, number = catalog_number.split(".")
        if not year.isdigit():
            return f"invalid year {year}"
        num_year = int(year)
        if not (1830 <= num_year <= 2100):
            return f"year {num_year} out of range"
        if not re.fullmatch(r"\d+[a-z]?", number):
            return f"invalid number {number}"
        return None

    # Old mammal catalog
    if re.fullmatch(r"\d+[a-z]", catalog_number):
        return None
    return "invalid BMNH specimen"


class CollectionTag(adt.ADT):
    CollectionDatabase(citation=Article, comment=NotRequired[str], tag=1)  # type: ignore[name-defined]
    TypeCatalog(citation=Article, coverage=str, tag=2)  # type: ignore[name-defined]
    SpecimenRegex(regex=str, tag=3)  # type: ignore[name-defined]
    MustUseChildrenCollection(tag=4)  # type: ignore[name-defined]  # deprecated
    ChildRule(collection=Collection, regex=str, taxon=NotRequired[Taxon], age=NotRequired[constants.AgeClass], tag=5)  # type: ignore[name-defined]
    MustHaveSpecimenLinks(tag=6)  # type: ignore[name-defined]
    ConditionalMustHaveSpecimenLinks(regex=str, taxon=NotRequired[Taxon], age=NotRequired[constants.AgeClass], tag=7)  # type: ignore[name-defined]
    # To be counted as a specimen link for this collection, a link must have this prefix.
    # Multiple copies of this tag may be present.
    SpecimenLinkPrefix(prefix=str, tag=8)  # type: ignore[name-defined]
    MustUseTriplets(tag=9)  # type: ignore[name-defined]
    CollectionCode(label=str, comment=str, specimen_regex=NotRequired[str], tag=10)  # type: ignore[name-defined]
