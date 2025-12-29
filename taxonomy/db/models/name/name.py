from __future__ import annotations

import builtins
import datetime
import enum
import json
import pprint
import re
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    ClassVar,
    NotRequired,
    Self,
    TypeAlias,
    assert_never,
)

from clirm import DoesNotExist, Field, Query

from taxonomy import adt, events, getinput, parsing
from taxonomy.apis import bhl
from taxonomy.apis.cloud_search import SearchField, SearchFieldType
from taxonomy.apis.zoobank import get_zoobank_data, get_zoobank_data_for_act
from taxonomy.db import constants, helpers, models
from taxonomy.db.constants import (
    URL,
    AgeClass,
    ArticleType,
    EmendationJustification,
    FillDataLevel,
    Group,
    Managed,
    Markdown,
    NameDataLevel,
    NomenclatureStatus,
    OriginalCitationDataLevel,
    PhylogeneticDefinitionType,
    Rank,
    RegionKind,
    SpeciesBasis,
    Status,
    TypeSpecimenKind,
)
from taxonomy.db.derived_data import DerivedField
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import (
    ADTField,
    BaseModel,
    LintConfig,
    TextField,
    TextOrNullField,
    get_str_completer,
    get_tag_based_derived_field,
)
from taxonomy.db.models.citation_group import CitationGroup
from taxonomy.db.models.classification_entry.ce import ClassificationEntry
from taxonomy.db.models.collection import (
    LOST_COLLECTION,
    MULTIPLE_COLLECTION,
    UNTRACED_COLLECTION,
    Collection,
)
from taxonomy.db.models.location import Location
from taxonomy.db.models.name_complex import NameComplex, SpeciesNameComplex
from taxonomy.db.models.person import AuthorTag, Person, get_new_authors_list
from taxonomy.db.models.taxon import Taxon, display_organized

from .type_specimen import parse_type_specimen

_CRUCIAL_MISSING_FIELDS_ALL_GROUPS = {
    "original_name",
    "corrected_original_name",
    "author_tags",
    "year",
    "page_described",
    "original_rank",
    # Note fields not applicable to the name are filtered out
    "original_parent",
    "name_complex",
    "species_name_complex",
}
_CRUCIAL_MISSING_FIELDS: dict[Group, set[str]] = {
    Group.species: _CRUCIAL_MISSING_FIELDS_ALL_GROUPS,
    Group.genus: _CRUCIAL_MISSING_FIELDS_ALL_GROUPS,
    Group.family: {"type", *_CRUCIAL_MISSING_FIELDS_ALL_GROUPS},
    Group.high: _CRUCIAL_MISSING_FIELDS_ALL_GROUPS,
}
_ETYMOLOGY_CUTOFF = 1990
_DATA_CUTOFF = 1900


class Name(BaseModel):
    creation_event = events.Event["Name"]()
    save_event = events.Event["Name"]()
    label_field = "corrected_original_name"
    grouping_field = "status"
    call_sign = "N"
    field_defaults: ClassVar[dict[str, Any]] = {
        "nomenclature_status": NomenclatureStatus.available,
        "status": Status.valid,
    }
    excluded_fields: ClassVar[set[str]] = {"data"}
    fields_without_completers: ClassVar[set[str]] = {"data"}
    markdown_fields: ClassVar[set[str]] = {"verbatim_citation"}
    clirm_table_name = "name"

    # Basic data
    group = Field[Group]()
    root_name = Field[str]()
    status = Field[Status]()
    taxon = Field[Taxon]("taxon_id", related_name="names")
    original_name = Field[str | None]()
    # Original name, with corrections for issues like capitalization and diacritics. Should not correct incorrect original spellings
    # for other reasons (e.g., prevailing usage). Consider a case where Gray (1825) names _Mus Somebodyi_, then Gray (1827) spells it
    # _Mus Somebodii_ and all subsequent authors follow this usage, rendering it a justified emendation. In this case, the 1825 name
    # should have original_name _Mus Somebodyi_, corrected original name _Mus somebodyi_, and root name _somebodii_. The 1827 name
    # should be listed as a justified emendation.
    corrected_original_name = Field[str | None]()
    nomenclature_status = Field[NomenclatureStatus](
        default=NomenclatureStatus.available
    )
    # for redirects
    target = Field[Self | None]("target", related_name="redirects")

    # Citation and authority
    author_tags = ADTField["AuthorTag"]()
    original_citation = Field[Article | None](
        "original_citation_id", related_name="new_names"
    )
    page_described = Field[str | None]()
    verbatim_citation = Field[str | None]()
    citation_group = Field[CitationGroup | None]("citation_group", related_name="names")
    year = Field[str | None]()  # redundant with data for the publication itself

    # Gender and stem
    name_complex = Field[NameComplex | None]("name_complex_id", related_name="names")
    species_name_complex = Field[SpeciesNameComplex | None](
        "species_name_complex_id", related_name="names"
    )

    # Types
    type = Field[Self | None](
        "type_id", related_name="typified_names"
    )  # for family and genus group
    type_locality = Field[Location | None](
        "type_locality_id", related_name="type_localities"
    )
    type_specimen = Field[str | None]()
    collection = Field[Collection | None](
        "collection_id", related_name="type_specimens"
    )
    genus_type_kind = Field[constants.TypeSpeciesDesignation | None]()
    species_type_kind = Field[constants.SpeciesGroupType | None]()
    type_tags = ADTField["TypeTag"](is_ordered=False)
    original_rank = Field[constants.Rank | None]()

    # Miscellaneous data
    original_parent = Field[Self | None](  # for species-group names
        related_name="original_children"
    )
    data = TextOrNullField()
    tags = ADTField["NameTag"](is_ordered=False)

    derived_fields: ClassVar[list[DerivedField[Any]]] = [
        DerivedField(
            "fill_data_level", FillDataLevel, lambda nam: nam.fill_data_level()[0]
        ),
        get_tag_based_derived_field(
            "preoccupied_names", lambda: Name, "tags", lambda: NameTag.PreoccupiedBy, 1
        ),
        get_tag_based_derived_field(
            "junior_primary_homonyms",
            lambda: Name,
            "tags",
            lambda: NameTag.PrimaryHomonymOf,
            1,
        ),
        get_tag_based_derived_field(
            "junior_secondary_homonyms",
            lambda: Name,
            "tags",
            lambda: NameTag.SecondaryHomonymOf,
            1,
        ),
        get_tag_based_derived_field(
            "unjustified_emendations",
            lambda: Name,
            "tags",
            lambda: NameTag.UnjustifiedEmendationOf,
            1,
        ),
        get_tag_based_derived_field(
            "incorrect_subsequent_spellings",
            lambda: Name,
            "tags",
            lambda: NameTag.IncorrectSubsequentSpellingOf,
            1,
        ),
        get_tag_based_derived_field(
            "nomina_nova", lambda: Name, "tags", lambda: NameTag.NomenNovumFor, 1
        ),
        get_tag_based_derived_field(
            "variants", lambda: Name, "tags", lambda: NameTag.VariantOf, 1
        ),
        get_tag_based_derived_field(
            "taking_priority", lambda: Name, "tags", lambda: NameTag.TakesPriorityOf, 1
        ),
        get_tag_based_derived_field(
            "nomina_oblita", lambda: Name, "tags", lambda: NameTag.NomenOblitum, 1
        ),
        get_tag_based_derived_field(
            "mandatory_changes",
            lambda: Name,
            "tags",
            lambda: NameTag.MandatoryChangeOf,
            1,
        ),
        get_tag_based_derived_field(
            "incorrect_original_spellings",
            lambda: Name,
            "tags",
            lambda: NameTag.IncorrectOriginalSpellingOf,
            1,
        ),
        get_tag_based_derived_field(
            "subsequent_usages",
            lambda: Name,
            "tags",
            lambda: NameTag.SubsequentUsageOf,
            1,
        ),
        get_tag_based_derived_field(
            "misidentifications",
            lambda: Name,
            "tags",
            lambda: NameTag.MisidentificationOf,
            1,
        ),
        get_tag_based_derived_field(
            "name_combinations",
            lambda: Name,
            "tags",
            lambda: NameTag.NameCombinationOf,
            1,
        ),
        get_tag_based_derived_field(
            "rerankings", lambda: Name, "tags", lambda: NameTag.RerankingOf, 1
        ),
        get_tag_based_derived_field(
            "selections_of_priority",
            lambda: Name,
            "tags",
            lambda: NameTag.SelectionOfPriority,
            1,
        ),
        get_tag_based_derived_field(
            "selections_of_spelling",
            lambda: Name,
            "tags",
            lambda: NameTag.SelectionOfSpelling,
            1,
        ),
        get_tag_based_derived_field(
            "reversals_of_priority",
            lambda: Name,
            "tags",
            lambda: NameTag.ReversalOfPriority,
            1,
        ),
        get_tag_based_derived_field(
            "justified_emendations",
            lambda: Name,
            "tags",
            lambda: NameTag.JustifiedEmendationOf,
            1,
        ),
        get_tag_based_derived_field(
            "designated_as_type",
            lambda: Name,
            "type_tags",
            lambda: TypeTag.TypeDesignation,
            2,
        ),
        get_tag_based_derived_field(
            "commission_designated_as_type",
            lambda: Name,
            "type_tags",
            lambda: TypeTag.CommissionTypeDesignation,
            2,
        ),
    ]

    search_fields: ClassVar[Sequence[SearchField]] = [
        SearchField(SearchFieldType.literal, "group"),
        SearchField(SearchFieldType.literal, "root_name"),
        SearchField(SearchFieldType.literal, "status"),
        SearchField(SearchFieldType.text, "original_name"),
        SearchField(SearchFieldType.text, "corrected_original_name"),
        SearchField(SearchFieldType.literal, "nomenclature_status"),
        SearchField(SearchFieldType.text_array, "authors"),
        SearchField(SearchFieldType.text, "page_described"),
        SearchField(SearchFieldType.text, "verbatim_citation", highlight_enabled=True),
        SearchField(SearchFieldType.text, "year"),
        SearchField(SearchFieldType.text, "type_specimen"),
        SearchField(SearchFieldType.literal, "genus_type_kind"),
        SearchField(SearchFieldType.literal, "species_type_kind"),
        SearchField(SearchFieldType.text_array, "tags", highlight_enabled=True),
    ]

    def get_search_dicts(self) -> list[dict[str, Any]]:
        data = {
            "group": self.group.name,
            "root_name": self.root_name,
            "status": self.status.name,
            "original_name": self.original_name,
            "corrected_original_name": self.corrected_original_name,
            "nomenclature_status": self.nomenclature_status.name,
            "page_described": self.page_described,
            "verbatim_citation": self.verbatim_citation,
            "year": self.year,
            "type_specimen": self.type_specimen,
            "genus_type_kind": (
                self.genus_type_kind.name if self.genus_type_kind else None
            ),
            "species_type_kind": (
                self.species_type_kind.name if self.species_type_kind else None
            ),
            "authors": [person.get_full_name() for person in self.get_authors()],
        }
        tags = []
        for tag in self.tags or ():
            if isinstance(tag, NameTag.HMW):
                continue
            tags.append(_stringify_tag(tag))
        for tag in self.type_tags or ():
            if isinstance(
                tag,
                (
                    TypeTag._RawCollector,
                    TypeTag.TypeLocality,
                    TypeTag.StratigraphyDetail,
                    TypeTag.Habitat,
                    TypeTag.Host,
                    TypeTag.ImpreciseLocality,
                    TypeTag.IncorrectGrammar,
                ),
            ) or tag in (
                TypeTag.NoEtymology,
                TypeTag.NoLocation,
                TypeTag.NoSpecimen,
                TypeTag.NoDate,
                TypeTag.NoCollector,
                TypeTag.NoOrgan,
                TypeTag.NoGender,
                TypeTag.NoAge,
            ):
                continue
            tags.append(_stringify_tag(tag))
        data["tags"] = tags
        return [data]

    @classmethod
    def with_tag_of_type(cls, tag_cls: builtins.type[adt.ADT]) -> list[Name]:
        names = cls.select_valid().filter(Name.type_tags.contains(f"[{tag_cls._tag},"))
        return [
            name
            for name in names
            if any(
                tag[0] == tag_cls._tag for tag in name.get_raw_tags_field("type_tags")
            )
        ]

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(
            Name.status != Status.removed, Name.status != Status.redirect
        )

    def get_redirect_target(self) -> Name | None:
        return self.target

    def is_invalid(self) -> bool:
        return self.status in (Status.removed, Status.redirect)

    def should_skip(self) -> bool:
        return self.status in (Status.removed, Status.redirect)

    def get_family_group_stem(self) -> str:
        if self.group is not Group.family:
            raise ValueError(f"not a family name: {self}")
        if self.original_rank is None:
            return self.root_name
        expected_suffix = helpers.SUFFIXES.get(self.original_rank)
        if expected_suffix is None:
            return self.root_name
        else:
            return self.root_name.removesuffix(expected_suffix)

    def get_grouped_rank(self) -> Rank:
        match self.group:
            case Group.family:
                return helpers.get_grouped_family_group_rank(
                    self.original_rank, self.corrected_original_name
                )
            case Group.high:
                return Rank.unranked
            case Group.genus:
                return Rank.genus
            case Group.species:
                return Rank.species
            case _:
                assert_never(self.group)

    def get_stem(self) -> str | None:
        if self.group != Group.genus or self.name_complex is None:
            return None
        return self.name_complex.get_stem_from_name(self.root_name)

    def safe_get_stem(self) -> str | None:
        try:
            return self.get_stem()
        except ValueError:
            return None

    def infer_original_rank(self) -> constants.Rank | None:
        if self.corrected_original_name is None or self.original_name is None:
            return None
        handcleaned_name = (
            self.original_name.lower()
            .replace("?", "")
            .replace("cf.", "")
            .replace("aff.", "")
            .replace("()", "")
            .replace("æ", "ae")
            .replace('"', "")
            .replace("'", "")
            .strip()
        )
        if self.group is Group.species:
            handcleaned_name = re.sub(r"\([A-Za-z]+\)", "", handcleaned_name)
        handcleaned_name = re.sub(r"\s+", " ", handcleaned_name)
        handcleaned_name = handcleaned_name[0].upper() + handcleaned_name[1:]
        if self.corrected_original_name != handcleaned_name:
            if self.group is Group.species:
                if " var. " in handcleaned_name:
                    return Rank.variety
                if (
                    self.corrected_original_name.count(" ") == 1
                    and self.original_name.count(" ") >= 1
                ):
                    return Rank.species
                if (
                    self.corrected_original_name.count(" ") == 3
                    and self.nomenclature_status is NomenclatureStatus.infrasubspecific
                ):
                    return Rank.infrasubspecific
            elif self.group is Group.genus:
                if re.search(
                    rf"^[A-Z][a-z]+ \({self.corrected_original_name}\)$",
                    self.original_name,
                ):
                    return Rank.subgenus
            return None
        if self.group is Group.species:
            spaces = self.corrected_original_name.count(" ")
            if spaces == 2:
                return Rank.subspecies
            elif spaces == 1:
                return Rank.species
        elif self.group is Group.family:
            if self.original_name.endswith("idae"):
                return Rank.family
            elif self.original_name.endswith("inae"):
                return Rank.subfamily
            elif self.original_name.endswith("oidea"):
                return Rank.superfamily
        elif self.group is Group.genus:
            if self.type is not None:
                type_name = self.type.corrected_original_name
                if (
                    type_name is not None
                    and type_name.split()[0] == self.corrected_original_name
                ):
                    return Rank.genus
        return None

    def autoset_original_rank(
        self, *, interactive: bool = False, quiet: bool = True, dry_run: bool = False
    ) -> bool:
        if self.original_rank is not None:
            return False
        inferred = self.infer_original_rank()
        if inferred is not None:
            print(
                f"{self}: inferred original_rank to be {inferred!r} from"
                f" {self.original_name!r}"
            )
            if not dry_run:
                self.original_rank = inferred
            return True
        else:
            if not quiet:
                print(
                    f"{self}: could not infer original rank from {self.original_name!r}"
                )
            if interactive:
                self.display()
                self.open_description()
                self.fill_field("original_rank")
            return False

    def infer_corrected_original_name(self, *, aggressive: bool = False) -> str | None:
        if not self.original_name:
            return None
        if self.nomenclature_status.permissive_corrected_original_name():
            return None
        if self.group is Group.family:
            original_name = clean_original_name(self.original_name)
            if (
                self.nomenclature_status
                is NomenclatureStatus.not_based_on_a_generic_name
            ):
                if re.match(r"^[A-Z][a-z]+$", original_name):
                    return original_name
            if self.type is not None:
                stem = self.type.get_stem()
                if stem is not None:
                    for suffix in helpers.VALID_SUFFIXES:
                        if original_name == f"{stem}{suffix}":
                            return original_name
                    if aggressive and not any(
                        original_name.endswith(suffix)
                        for suffix in helpers.VALID_SUFFIXES
                    ):
                        return f"{stem}idae"
                if aggressive:
                    stem = self.type.get_stem()
                    if stem is not None:
                        for suffix in helpers.VALID_SUFFIXES:
                            if original_name == f"{stem}{suffix}":
                                return original_name
                        if aggressive and not any(
                            original_name.endswith(suffix)
                            for suffix in helpers.VALID_SUFFIXES
                        ):
                            return f"{stem}idae"
        else:
            return infer_corrected_original_name(self.original_name, self.group)
        return None

    def get_value_for_field(self, field: str, default: str | None = None) -> Any:
        if (
            field == "collection"
            and self.collection is None
            and self.type_specimen is not None
        ):
            coll_name = parsing.extract_collection_from_type_specimen(
                self.type_specimen
            )
            if coll_name is not None:
                getter = list(
                    Collection.select_valid().filter(Collection.label == coll_name)
                )
                if len(getter) == 1:
                    coll = getter[0]
                    print(f"inferred collection to be {coll} from {self.type_specimen}")
                    return coll
            return super().get_value_for_field(field, default=default)
        elif field == "original_name":
            if self.original_name is None and self.group in (Group.genus, Group.high):
                return self.root_name
            else:
                return super().get_value_for_field(field, default=default)
        elif field == "corrected_original_name":
            if self.corrected_original_name is None:
                inferred = self.infer_corrected_original_name()
                if inferred is not None:
                    print(
                        f"inferred corrected_original_name to be {inferred!r} from"
                        f" {self.original_name!r}"
                    )
                    return inferred
            if self.corrected_original_name is not None:
                default = self.corrected_original_name
            else:
                default = self.original_name
            return super().get_value_for_field(field, default=default)
        elif field == "original_rank":
            if self.original_rank is None:
                inferred_rank = self.infer_original_rank()
                if inferred_rank is not None:
                    print(
                        f"inferred original_rank to be {inferred_rank!r} from"
                        f" {self.original_name!r}"
                    )
                    return inferred_rank
            if self.group is Group.species:
                rank_default = Rank.species
            elif self.group is Group.genus:
                rank_default = Rank.genus
            else:
                rank_default = None
            return super().get_value_for_field(field, default=rank_default)
        elif field == "type_tags":
            if self.type_locality is not None:
                print(repr(self.type_locality))
            if self.collection is not None:
                print(repr(self.collection))
            return super().get_value_for_field(field, default=default)
        elif field == "type":
            typ = super().get_value_for_field(field, default=default)
            print(f"type: {typ}")
            if typ is None:
                return None
            elif self.group is Group.genus or getinput.yes_no("Is this correct? "):
                return typ
            else:
                return None
        elif field == "citation_group":
            existing = self.citation_group
            value = super().get_value_for_field(field, default=default)
            if (
                existing is None
                and value is not None
                and self.verbatim_citation is not None
            ):
                value.apply_to_patterns()
            return value
        elif field == "author_tags" and not self.author_tags:
            return get_new_authors_list()
        else:
            return super().get_value_for_field(field, default=default)

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = super().get_adt_callbacks()
        article_callbacks = (
            self.original_citation.get_shareable_adt_callbacks()
            if self.original_citation is not None
            else {}
        )
        return {
            **callbacks,
            **article_callbacks,
            "add_comment": self.add_comment,
            "d": self._display_plus,
            "o": self.open_description,
            "open_url": self.open_url,
            "add_type_identical": self._add_type_identical_callback,
            "from_paper": self._from_paper_callback,
            "add_child": self._add_child_callback,
            "syn_from_paper": self._syn_from_paper_callback,
            "variant_from_paper": self.variant_from_paper,
            "combination_from_paper": self.combination_from_paper,
            "add_combination": self.add_combination,
            "add_syn": self._add_syn_callback,
            "make_variant": self.make_variant,
            "add_variant": self.add_variant,
            "add_nomen_nudum": lambda: self.add_nomen_nudum(interactive=True),
            "preoccupied_by": self.preoccupied_by,
            "not_preoccupied_by": self.not_preoccupied_by,
            "add_condition": self.add_condition,
            "display_type_locality": (
                lambda: self.type_locality and self.type_locality.display()
            ),
            "fill_required_fields": lambda: self.fill_required_fields(
                skip_fields={"type_tags"}
            ),
            "copy_year": self.copy_year,
            "copy_authors": self.copy_authors,
            "check_authors": self.check_authors,
            "level": self.print_fill_data_level,
            "set_nos": self.set_nos,
            "validate": self.validate,
            "validate_as_child": self.validate_as_child_command,
            "add_nominate": lambda: self.taxon.add_nominate(),
            "merge": self._merge,
            "redirect": self._redirect,
            "remove_duplicate": self._remove_duplicate,
            "edit_comments": self.edit_comments,
            "replace_original_citation": self.replace_original_citation,
            "open_zoobank": self.open_zoobank,
            "print_zoobank_data": self.print_zoobank_data,
            "open_type_specimen_link": self.open_type_specimen_link,
            "replace_type": self.replace_type,
            "print_type_specimen": self.print_type_specimen,
            "add_collection_code": (
                lambda: self.collection is not None
                and self.collection.add_collection_code()
            ),
            "add_authority_page_link": self.add_authority_page_link,
            "try_to_find_bhl_links": self.try_to_find_bhl_links,
            "clear_bhl_caches": self.clear_bhl_caches,
            "open_coordinates": self.open_coordinates,
            "edit_mapped_ce": self._edit_mapped_ce,
            "display_classification_entries": self.display_classification_entries,
            "display_usage_list": lambda: print(self.make_usage_list()),
            "set_page_described": self.set_page_described,
            "update_type_designations": self.update_type_designations,
            "find_older_usages": self.find_older_usages,
            "find_older_usages_literal": lambda: self.find_older_usages(literal=True),
        }

    def find_older_usages(self, *, literal: bool = False) -> None:
        if self.corrected_original_name is not None:
            query = self.corrected_original_name
        else:
            query = self.root_name
        if literal:
            query = f'"{query}"'
        from taxonomy.shell import interactive_search

        interactive_search(query, max_year=self.valid_numeric_year())

    def find_older_usages_auto(self) -> None:
        if self.corrected_original_name is not None:
            query = f'"{self.corrected_original_name}"'
        else:
            query = f'"{self.root_name}"'
        max_year = self.numeric_year() - 1
        from taxonomy.search import search

        results = search(query, year_max=max_year)
        if results:
            ignored = {
                tag.article.id
                for tag in self.get_tags(
                    self.type_tags, TypeTag.IgnorePotentialCitationFrom
                )
            }
            results = [hit for hit in results if hit.article_id not in ignored]
        if not results:
            print(f"{self}: no older usages found")
            return
        earliest = min(results, key=lambda hit: hit.year or 100000)
        article = Article(earliest.article_id)
        getinput.print_header(self)
        print(f"{self}: found older usage in {article}: {earliest}")
        article.display_classification_entries()
        article.edit()
        if self.original_citation != article and getinput.yes_no(
            "Add IgnorePotentialCitationFrom? "
        ):
            self.add_type_tag(TypeTag.IgnorePotentialCitationFrom(article=article))

    def get_classification_entries(self) -> Query[ClassificationEntry]:
        return ClassificationEntry.select_valid().filter(
            ClassificationEntry.mapped_name == self
        )

    def display_classification_entries(self) -> None:
        for ce in sorted(
            self.get_classification_entries(), key=lambda ce: ce.article.numeric_year()
        ):
            ce.display()

    def _edit_mapped_ce(self) -> None:
        ce = self.get_mapped_classification_entry()
        if ce is not None:
            ce.display()
            ce.edit()

    def open_coordinates(self) -> None:
        for tag in self.get_tags(self.type_tags, TypeTag.Coordinates):
            point = models.name.lint.make_point(tag)
            if point is not None:
                subprocess.check_call(["open", point.openstreetmap_url])

    def clear_bhl_caches(self) -> None:
        for tag in self.type_tags:
            if isinstance(tag, TypeTag.AuthorityPageLink):
                bhl.clear_caches_related_to_url(tag.url)

    def add_authority_page_link(self) -> None:
        if self.page_described is None:
            print("Page described is missing; add AuthorityPageLink tag directly")
            return
        link = getinput.get_line("link> ")
        if not link:
            return
        if link.isnumeric():
            link = f"https://www.biodiversitylibrary.org/page/{link}"
        self.add_type_tag(
            TypeTag.AuthorityPageLink(
                url=link, confirmed=True, page=self.page_described
            )
        )

    def try_to_find_bhl_links(self) -> None:
        cfg = LintConfig(verbose=True)
        if self.has_type_tag(models.name.TypeTag.AuthorityPageLink):
            return
        for _ in models.name.lint.infer_bhl_page(self, cfg):
            pass
        for _ in models.name.lint.infer_bhl_page_from_other_names(self, cfg):
            pass
        for _ in models.name.lint.infer_bhl_page_from_article(self, cfg):
            pass

    def print_type_specimen(self) -> None:
        if self.type_specimen is None:
            print("No type specimen")
            return
        for spec in parse_type_specimen(self.type_specimen):
            print(spec)

    def open_type_specimen_link(self) -> None:
        for tag in self.type_tags:
            if isinstance(tag, (TypeTag.TypeSpecimenLink, TypeTag.TypeSpecimenLinkFor)):
                subprocess.check_call(["open", tag.url])

    def replace_type(self) -> None:
        new_type = Name.getter("type_specimen").get_one_key("new type> ")
        if not new_type:
            return
        coll = Collection.getter(None).get_one("collection> ")
        if coll is None:
            return
        if self.type_specimen is None:
            new_text = new_type
        else:
            new_text = f"{new_type} (= {self.type_specimen})"
        self.type_specimen = new_text
        if self.collection is not None:
            self.add_type_tag(TypeTag.FormerRepository(self.collection))
        self.collection = coll

    def _merge(self) -> None:
        other = Name.getter(None).get_one("name to merge into> ")
        if other is None:
            return
        self.merge(other)

    def _redirect(self) -> None:
        other = Name.getter(None).get_one("name to redirect to> ")
        if other is None:
            return
        self.redirect(other)

    def _remove_duplicate(self) -> None:
        other = Name.getter(None).get_one("name to remove> ")
        if other is None:
            return
        other.merge(self)

    def print_fill_data_level(self) -> None:
        for label, (level, reason) in (
            ("name", self.name_data_level()),
            ("original citation", self.original_citation_data_level()),
            ("fill data", self.fill_data_level()),
        ):
            if reason:
                print(f"{label}: {level.name}: {reason}")
            else:
                print(f"{label}: {level.name}")

    def edit(self) -> None:
        self.fill_field("type_tags")

    def _add_type_identical_callback(self) -> None:
        root_name = self.getter("root_name").get_one_key("root_name> ")
        if root_name is None:
            return
        self.add_type_identical(root_name)

    def _from_paper_callback(self) -> None:
        self.taxon.from_paper()

    def _add_child_callback(self) -> None:
        self.taxon.add()

    def _syn_from_paper_callback(self) -> None:
        self.taxon.syn_from_paper()

    def _add_syn_callback(self) -> None:
        self.taxon.add_syn()

    def add_type_identical(
        self,
        name: str,
        page_described: None | int | str = None,
        locality: Location | None = None,
        **kwargs: Any,
    ) -> Taxon:
        """Convenience method to add a type species described in the same paper as the genus."""
        assert self.taxon.rank == Rank.genus
        assert self.type is None
        full_name = f"{self.corrected_original_name} {name}"
        if isinstance(page_described, int):
            page_described = str(page_described)
        if self.genus_type_kind is None:
            self.fill_field("genus_type_kind")
        result = self.add_child_taxon(
            Rank.species,
            full_name,
            author_tags=self.author_tags,
            year=self.year,
            original_citation=self.original_citation,
            verbatim_citation=self.verbatim_citation,
            citation_group=self.citation_group,
            original_name=full_name,
            page_described=page_described,
            status=self.status,
        )
        self.type = result.base_name
        if locality is not None:
            result.add_occurrence(locality)
        result.base_name.s(**kwargs)
        result.base_name.edit()
        return result

    @classmethod
    def get_completers_for_adt_field(cls, field: str) -> getinput.CompleterMap:
        completers = dict(super().get_completers_for_adt_field(field))
        for field_name, tag_cls in [
            ("type_tags", TypeTag),
            ("tags", NameTag),
            ("author_tags", AuthorTag),
        ]:
            if field == field_name:
                for tag in tag_cls._tag_to_member.values():
                    for attribute, typ in tag._attributes.items():
                        if typ is str and attribute in ("lectotype", "neotype"):
                            completer = get_str_completer(Name, "type_specimen")
                            completers[(tag, attribute)] = completer
        return completers

    def get_empty_required_fields(self) -> Iterable[str]:
        fields = []
        for field in super().get_empty_required_fields():
            fields.append(field)
            yield field
        if (
            fields
            and fields[-1] != "type_tags"
            and "type_tags" in self.get_required_fields()
        ):
            # Always make the user edit type_tags if some other field was unfilled,
            # even if we edited type_tags previously in this run.
            yield "type_tags"

    def fill_field_if_empty(self, field: str) -> None:
        if field in self.get_empty_required_fields():
            if field == "verbatim_citation":
                getinput.print_header(self)
                self.possible_citation_groups()
            self.display()
            self.fill_field(field)

    def add_additional_data(self, new_data: str) -> None:
        """Add data to the "additional" field within the "data" field"""
        data = self._load_data()
        if "additional" not in data:
            data["additional"] = []
        data["additional"].append(new_data)
        self.data = json.dumps(data)  # type: ignore[assignment]

    def add_data(
        self, field: str, value: Any, *, concat_duplicate: bool = False
    ) -> None:
        data = self._load_data()
        if field in data:
            if concat_duplicate:
                existing = data[field]
                if isinstance(existing, list):
                    value = [*existing, value]
                else:
                    value = [existing, value]
            else:
                raise ValueError(f"{field} is already in {data}")
        data[field] = value
        self.data = json.dumps(data)  # type: ignore[assignment]

    def get_data(self, field: str) -> Any:
        data = self._load_data()
        return data[field]

    def _load_data(self) -> dict[str, Any]:
        if self.data is None or self.data == "":
            return {}
        else:
            return json.loads(self.data)

    def get_type_tag(self, tag_cls: TypeTag._Constructor) -> Any | None:  # type: ignore[name-defined]
        for tag in self.type_tags:
            if isinstance(tag, tag_cls):
                return tag
        return None

    def get_tag_target(self, tag_cls: Tag._Constructor) -> Name | None:  # type: ignore[name-defined]
        tags = self.tags
        if tags:
            for tag in tags:
                if isinstance(tag, tag_cls) and hasattr(tag, "name"):
                    return tag.name
        return None

    def get_tag_targets(self, tag_cls: Tag._Constructor) -> Iterable[Name]:  # type: ignore[name-defined]
        tags = self.tags
        if tags:
            for tag in tags:
                if isinstance(tag, tag_cls) and hasattr(tag, "name"):
                    yield tag.name

    def get_names_taking_priority(self) -> Iterable[models.Name]:
        tags = self.tags
        if tags:
            for tag in tags:
                if (
                    isinstance(tag, models.name.NameTag.TakesPriorityOf)
                    and tag.is_in_prevailing_usage
                ):
                    yield tag.name

    def add_tag(self, tag: NameTag) -> None:
        tags = self.tags
        if tags is None:
            self.tags = [tag]
        else:
            self.tags = (*tags, tag)  # type: ignore[assignment]

    def add_type_tag(self, tag: TypeTag) -> None:
        type_tags = self.type_tags
        if type_tags is None:
            self.type_tags = [tag]
        elif tag not in type_tags:
            self.type_tags = (*type_tags, tag)  # type: ignore[assignment]

    def remove_type_tag(self, tag: TypeTag) -> None:
        type_tags = self.type_tags
        if type_tags is None:
            return
        self.type_tags = tuple(t for t in type_tags if t != tag)  # type: ignore[assignment]

    @classmethod
    def with_tag(cls, tag_cls: NameTagCons) -> Query[Name]:
        return cls.select_valid().filter(Name.tags.contains(f"[{tag_cls._tag},"))

    @classmethod
    def with_type_tag(cls, tag_cls: TypeTagCons) -> Query[Name]:
        return cls.select_valid().filter(Name.type_tags.contains(f"[{tag_cls._tag},"))

    def has_type_tag(self, tag_cls: TypeTagCons) -> bool:
        tag_id = tag_cls._tag
        return any(tag[0] == tag_id for tag in self.get_raw_tags_field("type_tags"))

    def num_type_tags(self, tag_cls: TypeTagCons) -> int:
        tag_id = tag_cls._tag
        return sum(tag[0] == tag_id for tag in self.get_raw_tags_field("type_tags"))

    def has_name_tag(self, tag_cls: NameTagCons) -> bool:
        tag_id = tag_cls._tag
        return any(tag[0] == tag_id for tag in self.get_raw_tags_field("tags"))

    def map_type_tags(
        self, fn: Callable[[Any], Any | None], *, dry_run: bool = False
    ) -> None:
        self.map_tags_field(Name.type_tags, fn, dry_run=dry_run)

    def map_type_tags_by_type(
        self,
        typ: builtins.type[Any],
        fn: Callable[[Any], Any],
        *,
        dry_run: bool = False,
    ) -> None:
        self.map_tags_by_type(Name.type_tags, typ, fn, dry_run=dry_run)

    def replace_original_citation(self, new_citation: Article | None = None) -> None:
        if new_citation is None:
            new_citation = Article.get_one_by("name", allow_empty=False)
        if new_citation is None:
            return
        existing = self.original_citation

        def map_fn(tag: TypeTag) -> TypeTag:
            if (
                hasattr(tag, "text")
                and hasattr(tag, "source")
                and set(tag.__dict__) == {"text", "source"}
            ):
                if tag.source == existing:
                    return type(tag)(text=tag.text, source=new_citation)
            return tag

        self.map_type_tags(map_fn)
        self.original_citation = new_citation

    def add_included(self, species: Name, comment: str | None = None) -> None:
        assert isinstance(species, Name)
        self.add_type_tag(TypeTag.IncludedSpecies(species, comment=comment))

    def edit_comments(self) -> None:
        for comment in self.comments:
            comment.display()
            comment.edit()

    def add_static_comment(
        self,
        kind: constants.CommentKind,
        text: str,
        source: Article | None = None,
        page: str | None = None,
    ) -> NameComment:
        return NameComment.make(
            name=self, kind=kind, text=text, source=source, page=page
        )

    def add_comment(
        self,
        kind: constants.CommentKind | None = None,
        text: str | None = None,
        source: Article | None = None,
        page: str | None = None,
    ) -> NameComment | None:
        return NameComment.create_interactively(
            name=self, kind=kind, text=text, source=source, page=page
        )

    def add_child_taxon(
        self, rank: Rank, name: str, age: AgeClass | None = None, **kwargs: Any
    ) -> Taxon:
        return self.taxon.add_static(rank, name, age=age, **kwargs)

    def add_nomen_nudum(self, *, interactive: bool = True) -> Name | None:
        """Adds a nomen nudum similar to this name."""
        tags = [NameTag.Condition(NomenclatureStatus.nomen_nudum, comment="")]
        if interactive:
            paper = self.get_value_for_foreign_class("paper", Article)
            if paper is not None:
                return self.taxon.syn_from_paper(
                    paper=paper,
                    root_name=self.root_name,
                    original_name=self.original_name,
                    author_tags=self.author_tags,
                    tags=tags,
                    nomenclature_status=NomenclatureStatus.nomen_nudum,
                )
        return self.taxon.add_syn(
            root_name=self.root_name,
            original_name=self.original_name,
            author_tags=self.author_tags,
            tags=tags,
            nomenclature_status=NomenclatureStatus.nomen_nudum,
        )

    def description(self, *, include_parentheses: bool = True) -> str:
        if self.original_name:
            out = self.original_name
        elif self.root_name:
            out = self.root_name
        else:
            out = "<no name>"
        if (
            not self.nomenclature_status.can_preoccupy()
            and not self.nomenclature_status.is_variant()
        ):
            out = f'"{out}"'
        if self.author_tags:
            if self.nomenclature_status in (
                NomenclatureStatus.name_combination,
                NomenclatureStatus.subsequent_usage,
                NomenclatureStatus.reranking,
                NomenclatureStatus.misidentification,
            ):
                out += ":"
            out += f" {self.taxonomic_authority()}"
        if self.year:
            out += f", {self.year}"
        if self.page_described:
            out += f":{self.page_described}"
        if include_parentheses:
            parenthesized_bits = []
            try:
                taxon = self.taxon
            except DoesNotExist:
                parenthesized_bits.append("= <invalid taxon>")
            else:
                if taxon.is_nominate_subgenus():
                    valid_name = taxon.base_name.root_name
                else:
                    valid_name = taxon.valid_name
                if valid_name != self.original_name:
                    parenthesized_bits.append(f"= {valid_name}")
            if self.nomenclature_status is None:
                parenthesized_bits.append("<no nomenclature status>")
            elif self.nomenclature_status != NomenclatureStatus.available:
                parenthesized_bits.append(self.nomenclature_status.name)
            if self.status is None:
                parenthesized_bits.append("<no status>")
            elif self.status != Status.valid:
                parenthesized_bits.append(self.status.name)
            if parenthesized_bits:
                out += f" ({', '.join(parenthesized_bits)})"
        return out

    def get_default_valid_name(self) -> str:
        if self.corrected_original_name is not None:
            return self.corrected_original_name
        return self.root_name

    def is_unavailable(self) -> bool:
        return not self.nomenclature_status.can_preoccupy()

    def can_preoccupy(self, *, depth: int = 0) -> bool:
        if depth > 10:
            raise ValueError(f"reached recursion limit on {self}")
        if not self.nomenclature_status.can_preoccupy():
            return False
        if nam := self.get_variant_base_name():
            return nam.can_preoccupy(depth=depth + 1)
        return True

    def has_priority_over(self, nam: Name) -> bool:
        my_date = self.get_date_object()
        their_date = nam.get_date_object()

        # First, check the date
        if my_date < their_date:
            return True
        if their_date < my_date:
            return False

        # Names with a higher original rank have priority
        if (
            self.original_rank is not None
            and nam.original_rank is not None
            and not self.original_rank.is_uncomparable
            and not nam.original_rank.is_uncomparable
        ):
            if self.original_rank.comparison_value > nam.original_rank.comparison_value:
                return True
            if nam.original_rank.comparison_value > self.original_rank.comparison_value:
                return False

        # Check for explicit priority selection
        for tag in self.tags:
            if isinstance(tag, NameTag.SelectionOfPriority) and tag.over_name == nam:
                return True
        return False

    def can_be_valid_base_name(self, *, allow_preoccupied: bool = False) -> bool:
        if self.nomenclature_status is NomenclatureStatus.nomen_novum:
            nam = self.get_tag_target(NameTag.NomenNovumFor)
            if nam is None:
                return False
            return nam.can_be_valid_base_name(allow_preoccupied=True)
        if self.has_name_tag(NameTag.PendingRejection):
            return False
        if self.nomenclature_status in (
            NomenclatureStatus.available,
            NomenclatureStatus.as_emended,
            NomenclatureStatus.collective_group,
            NomenclatureStatus.informal,
            NomenclatureStatus.unpublished_pending,
        ):
            return True
        if allow_preoccupied and self.nomenclature_status in (
            NomenclatureStatus.preoccupied,
            NomenclatureStatus.partially_suppressed,
        ):
            return True
        if (
            self.nomenclature_status
            is NomenclatureStatus.not_intended_as_a_scientific_name
            and self.taxon.rank == Rank.division
        ):
            return True
        return False

    def get_variant_base_name(
        self, tags: tuple[builtins.type[NameTag], ...] | None = None
    ) -> Name | None:
        if tags is None:
            tags = VARIANT_TAGS
        for tag in self.tags:
            if isinstance(tag, tags):
                return tag.name
        return None

    def resolve_variant(
        self,
        tags: tuple[builtins.type[NameTag], ...] | None = None,
        *,
        misidentification: bool = False,
    ) -> Name:
        if tags is None:
            tags = VARIANT_TAGS
        if misidentification:
            tags = (*tags, NameTag.MisidentificationOf)
        return self._resolve_variant(10, tags)

    def _resolve_variant(
        self, max_depth: int, tags: tuple[builtins.type[NameTag], ...]
    ) -> Name:
        if max_depth == 0:
            raise ValueError(f"too deep for {self}")
        base_name = self.get_variant_base_name(tags)
        if base_name is None:
            return self
        return base_name._resolve_variant(max_depth - 1, tags)

    def is_high_mammal(self) -> bool:
        return (
            self.group is not Group.species
            and self.taxon.get_derived_field("class_").valid_name == "Mammalia"
        )

    def numeric_page_described(self) -> int:
        return helpers.to_int(self.page_described)

    def extract_page_described(self) -> int | None:
        """Attempts to extract a page that appears in the original description, if at all possible."""
        page_described = self.numeric_page_described()
        if page_described != 0:
            return page_described
        if self.verbatim_citation is not None:
            match = re.search(
                r"[):]\s*(\d+)\s*([\-–]\s*\d+)?\.?\s*$", self.verbatim_citation
            )
            if match:
                return int(match.group(1))
        return None

    def set_page_described(self, page: str | None = None) -> None:
        existing = self.page_described
        if page is None:
            page = self.getter("page_described").get_one_key(
                "page_described> ", default=existing or ""
            )
            if page is None:
                return
        if existing is not None:

            def adjust_tag(tag: TypeTag) -> TypeTag:
                if isinstance(tag, TypeTag.AuthorityPageLink) and tag.page == existing:
                    print(f"Update page on {tag}")
                    return TypeTag.AuthorityPageLink(
                        url=tag.url, confirmed=tag.confirmed, page=page
                    )
                else:
                    return tag

            self.map_type_tags(adjust_tag)
        self.page_described = page
        print(f"{self}: change page {existing} -> {page}")
        ce = self.get_mapped_classification_entry()
        if ce is not None:
            ce.set_page(page)

    def update_type_designations(self) -> None:
        if self.type_specimen is None:
            print(f"{self} has no type specimen")
            return
        new_tags: list[TypeTag] = []
        for tag in self.type_tags:
            if isinstance(tag, TypeTag.LectotypeDesignation):
                if tag.lectotype != self.type_specimen:
                    print(tag)
                    if getinput.yes_no(
                        f"Change type specimen from {tag.lectotype!r} to {self.type_specimen!r}? "
                    ):
                        tag = tag.replace(lectotype=self.type_specimen)
                        print(f"Updated {tag}")
            elif isinstance(tag, TypeTag.NeotypeDesignation):
                if tag.neotype != self.type_specimen:
                    print(tag)
                    if getinput.yes_no(
                        f"Change neotype from {tag.neotype!r} to {self.type_specimen!r}? "
                    ):
                        tag = tag.replace(neotype=self.type_specimen)
                        print(f"Changed {tag}")
            new_tags.append(tag)
        self.type_tags = new_tags  # type: ignore[assignment]

    def get_repositories(self) -> list[Collection]:
        if self.collection is None:
            return []
        if self.collection.name == "multiple":
            return [
                tag.repository
                for tag in self.type_tags
                if isinstance(tag, TypeTag.Repository)
            ]
        else:
            return [self.collection]

    def get_date_object(self) -> datetime.date:
        return helpers.get_date_object(self.year)

    def numeric_year(self) -> int:
        return self.get_date_object().year

    def valid_numeric_year(self) -> int | None:
        if self.year is not None and helpers.is_valid_date(self.year):
            return self.numeric_year()
        else:
            return None

    def get_citation_group(self) -> models.CitationGroup | None:
        if self.citation_group is not None:
            return self.citation_group
        if self.original_citation is not None:
            return self.original_citation.get_citation_group()
        return None

    def get_type_locality_country(self) -> models.Region | None:
        tl = self.type_locality
        if tl is None:
            return None
        region = tl.region
        country = region.parent_of_kind(RegionKind.country)
        if country is None:
            return region
        return country

    def sort_key(self) -> tuple[object, ...]:
        return (
            self.get_date_object(),
            self.numeric_page_described(),
            self.corrected_original_name or "",
            self.root_name,
        )

    def make_variant(
        self,
        status: NomenclatureStatus | None = None,
        of_name: Name | None = None,
        comment: str | None = None,
    ) -> None:
        if self.nomenclature_status not in (
            NomenclatureStatus.available,
            NomenclatureStatus.unpublished_pending,
        ):
            raise ValueError(f"{self} is {self.nomenclature_status.name}")
        if status is None:
            status = getinput.get_enum_member(
                NomenclatureStatus, prompt="nomenclature_status> "
            )
        if status is None:
            return
        if of_name is None:
            of_name = Name.getter("corrected_original_name").get_one(prompt="of_name> ")
        if of_name is None:
            return
        self.add_tag(
            CONSTRUCTABLE_STATUS_TO_TAG[status](name=of_name, comment=comment or "")
        )
        self.nomenclature_status = status

    def combination_from_paper(self) -> Name | None:
        return self.add_combination(from_paper=True)

    def variant_from_paper(self) -> Name | None:
        root_name = Name.getter("root_name").get_one_key(prompt="root_name> ")
        if root_name is None:
            return None
        paper = self.get_value_for_foreign_class("paper", Article)
        if paper is None:
            return None
        return self.add_variant(root_name, paper=paper)

    def add_combination(self, *, from_paper: bool = False) -> Name | None:
        original_name = Name.getter("original_name").get_one_key(
            prompt="original_name> "
        )
        if not original_name:
            return None
        existing = list(Name.select_valid().filter(Name.original_name == original_name))
        if existing:
            print("Existing similar names:")
            for nam in existing:
                nam.display()
            if not getinput.yes_no("continue? "):
                return None
        paper = None
        if from_paper:
            paper = self.get_value_for_foreign_class("paper", Article)
            if paper is None:
                return None
        return self.add_variant(
            root_name=self.root_name,
            status=NomenclatureStatus.name_combination,
            interactive=True,
            original_name=original_name,
            paper=paper,
        )

    def add_variant(
        self,
        root_name: str | None = None,
        status: NomenclatureStatus | None = None,
        paper: Article | None = None,
        page_described: str | None = None,
        original_name: str | None = None,
        *,
        interactive: bool = True,
    ) -> Name | None:
        if root_name is None:
            root_name = Name.getter("root_name").get_one_key(prompt="root_name> ")
        if root_name is None:
            return None
        if status is None:
            status = getinput.get_enum_member(
                NomenclatureStatus, prompt="nomenclature_status> "
            )
        if status is None:
            return None

        if paper is not None:
            nam = self.taxon.syn_from_paper(
                root_name=root_name, paper=paper, interactive=False
            )
            if nam is None:
                return None
            nam.original_name = original_name
            nam.nomenclature_status = status
        else:
            nam = self.taxon.add_syn(
                root_name=root_name,
                nomenclature_status=status,
                original_name=original_name,
                interactive=False,
            )
            if nam is None:
                return None
        tag_cls = CONSTRUCTABLE_STATUS_TO_TAG[status]
        if page_described is not None:
            nam.page_described = page_described
        nam.add_tag(tag_cls(self, comment=""))
        if interactive:
            nam.fill_required_fields()
        return nam

    def preoccupied_by(
        self,
        name: Name | None = None,
        comment: str | None = None,
        *,
        tag: builtins.type[NameTag] | None = None,
    ) -> None:
        if name is None:
            name = Name.getter("corrected_original_name").get_one(prompt="name> ")
        if name is None:
            return
        if tag is None:
            tag = NameTag.PreoccupiedBy
        self.add_tag(tag(name, comment=comment or ""))
        if self.nomenclature_status == NomenclatureStatus.available:
            self.nomenclature_status = NomenclatureStatus.preoccupied
        else:
            print(f"not changing status because it is {self.nomenclature_status!r}")

    def not_preoccupied_by(
        self, name: Name | None = None, comment: str | None = None
    ) -> None:
        if name is None:
            getter = Name.getter("corrected_original_name")
            for tag in self.tags:
                if (
                    isinstance(tag, PREOCCUPIED_TAGS)
                    and tag.name.corrected_original_name is not None
                ):
                    getinput.append_history(getter, tag.name.corrected_original_name)
            name = getter.get_one(prompt="name> ")
        if name is None:
            return
        tags = list(self.tags)
        tags = [
            tag
            for tag in tags
            if not (isinstance(tag, PREOCCUPIED_TAGS) and tag.name == name)
        ]
        tags.append(NameTag.NotPreoccupiedBy(name, comment=comment or ""))
        getinput.print_diff(self.tags, tags)
        self.tags = tags  # type: ignore[assignment]

    def add_condition(self, status: NomenclatureStatus | None = None) -> None:
        if status is None:
            status = getinput.get_enum_member(
                NomenclatureStatus, prompt="nomenclature_status> "
            )
        if status is None:
            return
        self.add_tag(NameTag.Condition(status, comment=""))

    def conserve(self, opinion: Article, comment: str | None = None) -> None:
        self.add_tag(NameTag.Conserved(opinion, comment=comment or ""))

    @classmethod
    def infer_author_tags(cls, authority: str) -> list[AuthorTag] | None:
        params_by_name: list[dict[str, str] | None]
        if "et al." in authority:
            params_by_name = [None]
        elif authority == "H.E. Wood, 2nd":
            params_by_name = [
                {"family_name": "Wood", "given_names": "Horace Elmer", "suffix": "2nd"}
            ]
        else:
            authors = re.split(r", | & ", re.sub(r"et al\.$", "", authority))
            params_by_name = [cls._author_to_person(author) for author in authors]
        tags = []
        for params in params_by_name:
            if params is None:
                return None
            tags.append(
                AuthorTag.Author(person=Person.get_or_create_unchecked(**params))
            )
        print(f"Authors: {authority!r} -> {params_by_name}")
        return tags

    @staticmethod
    def _author_to_person(author: str) -> dict[str, str] | None:
        match = re.match(
            (
                r"^((?P<initials>([A-ZÉ]\.)+) )?((?P<tussenvoegsel>de|von|van|van"
                r" der|van den|van de) )?(?P<family_name>(d'|de|de la |zur |du |dos"
                r" |del |di |ul-|von der |da |vander|dal |delle |ul )?[ÄÉÜÁÖŞA-Z].*)(,"
                r" (?P<suffix>2nd))?$"
            ),
            author,
        )
        if match is not None:
            return match.groupdict()
        return None

    def author_set(self) -> set[int]:
        return {pair[1] for pair in self.get_raw_tags_field("author_tags")}

    def get_authors(self) -> list[Person]:
        if self.author_tags is None:
            return []
        return [author.person for author in self.author_tags]

    def taxonomic_authority(self) -> str:
        return Person.join_authors(self.get_authors())

    def should_parenthesize_authority(self) -> bool | None:
        if self.group is not Group.species:
            return False
        if self.original_parent is None:
            return None  # unknown
        genus = self.taxon.get_current_genus()
        if genus is None:
            return False  # not in any genus, so don't parenthesize
        return genus.resolve_variant(
            misidentification=True
        ) != self.original_parent.resolve_variant(misidentification=True)

    def get_full_authority(self) -> str:
        authority = self.taxonomic_authority()
        if self.year is not None:
            authority += f", {self.numeric_year()}"
        if self.should_parenthesize_authority():
            authority = f"({authority})"
        return authority

    def copy_year(self, *, quiet: bool = False) -> None:
        citation = self.original_citation
        if citation is None:
            print("No original citation; cannot copy year")
            return
        if self.year == citation.year:
            if not quiet:
                print("Year already matches")
            return
        print(f"Setting year: {self.year!r} -> {citation.year!r}")
        self.year = citation.year

    def copy_authors(self) -> None:
        citation = self.original_citation
        if citation is None:
            print("No original citation; cannot copy authors")
            return
        if citation.issupplement() and citation.parent is not None:
            authors = citation.parent.author_tags
        else:
            authors = citation.author_tags
        assert authors is not None, f"missing authors for {citation}"
        if self.author_tags:
            getinput.print_diff(self.author_tags, authors)
        else:
            print(f"Setting authors: {authors}")
        self.author_tags = authors

    @classmethod
    def check_all_authors(
        cls, *, autofix: bool = True, quiet: bool = True
    ) -> list[Name]:
        bad = [
            nam
            for nam in cls.select_valid().filter(cls.author_tags != None)
            if not nam.check_authors(autofix=autofix, quiet=quiet)
        ]
        print(f"{len(bad)} discrepancies")
        return bad

    def check_authors(self, *, autofix: bool = True, quiet: bool = False) -> bool:
        if self.has_type_tag(TypeTag.DifferentAuthority):
            return True
        citation = self.original_citation
        if not citation:
            return True
        if self.get_raw_tags_field("author_tags") == citation.get_raw_tags_field(
            "author_tags"
        ):
            return True
        maybe_print = (
            (lambda message: None)
            if quiet
            else lambda message: print(f"{self}: {message}")
        )
        name_authors = self.get_authors()
        article_authors = citation.get_authors()
        if name_authors == article_authors:
            return True  # can happen with supplements
        if name_authors:
            if len(name_authors) != len(article_authors):
                maybe_print(
                    f"length mismatch {len(name_authors)} vs. {len(article_authors)}"
                )
                return False
            new_authors = list(self.author_tags)
            for i, (name_author, article_author) in enumerate(
                zip(name_authors, article_authors, strict=True)
            ):
                if name_author == article_author:
                    continue

                if article_author.is_more_specific_than(name_author):
                    maybe_print(
                        f"author {i}: {article_author} is more specific than {name_author}"
                    )
                    new_authors[i] = AuthorTag.Author(person=article_author)
                    if autofix:
                        name_author.move_reference(article_author, "names", self)
                else:
                    maybe_print(
                        f"author {i}: {article_author} (article) does not match"
                        f" {name_author} (name)"
                    )
        else:
            new_authors = []
        getinput.print_diff(self.author_tags, new_authors)
        if autofix:
            self.author_tags = new_authors  # type: ignore[assignment]
        return False

    def get_description(
        self,
        *,
        full: bool = False,
        depth: int = 0,
        include_data: bool = False,
        include_taxon: bool = False,
        skip_lint: bool = False,
    ) -> str:
        if self.original_name is None:
            out = self.root_name
        else:
            out = self.original_name
        if self.author_tags is not None:
            out += f" {self.taxonomic_authority()}"
        if self.year is not None:
            out += f", {self.year}"
        takes_prio = list(self.get_names_taking_priority())
        if takes_prio:
            oldest = min(takes_prio, key=lambda n: n.get_date_object())
            out += f" ({oldest.year})"
        if self.page_described is not None:
            out += f":{self.page_described}"
        if self.original_citation is not None:
            out += f" {{{self.original_citation.name}}}"
        if self.type is not None:
            kind = f"; {self.genus_type_kind.name}" if self.genus_type_kind else ""
            out += f" (type: {self.type}{kind})"
        statuses = []
        if self.status != Status.valid:
            statuses.append(self.status)
        if self.nomenclature_status != NomenclatureStatus.available:
            statuses.append(self.nomenclature_status)
        if statuses:
            out += f' ({", ".join(status.name for status in statuses)})'
        if full:
            parts = []
            if self.original_name is not None:
                parts.append(f"root: {self.root_name}")
            if (
                self.corrected_original_name is not None
                and self.corrected_original_name != self.original_name
            ):
                parts.append(f"corrected: {self.corrected_original_name}")
            if self.name_complex is not None:
                parts.append(f"name complex: {self.name_complex}")
            elif self.species_name_complex is not None:
                parts.append(f"name complex: {self.species_name_complex}")
            parts.append(f"#{self.id}")
            out += " ({})".format("; ".join(parts))
        if include_taxon:
            out += f" (={self.taxon})"
        knowledge_level = self.knowledge_level()
        if knowledge_level == 0:
            intro_line = getinput.red(out)
        elif knowledge_level == 1:
            intro_line = getinput.yellow(out)
        else:
            intro_line = getinput.green(out)
        result = " " * ((depth + 1) * 4) + intro_line + "\n"
        if full:
            data: dict[str, Any] = {}
            if not skip_lint:
                lints = "; ".join(self.lint())
                if lints:
                    data["lint"] = lints
            level_strings = []
            ocdl, ocdl_reason = self.original_citation_data_level()
            if ocdl not in (
                OriginalCitationDataLevel.all_required_data,
                OriginalCitationDataLevel.no_citation,
            ):
                level_strings.append(f"citation: {ocdl.name.upper()} ({ocdl_reason})")
            ndl, ndl_reason = self.name_data_level()
            if ndl is not NameDataLevel.nothing_needed:
                level_strings.append(f"name: {ndl.name.upper()} ({ndl_reason})")
            if level_strings:
                data["level"] = "; ".join(level_strings)
            if self.type_locality is not None:
                data["locality"] = repr(self.type_locality)
            type_info = []
            if self.species_type_kind is not None:
                type_info.append(self.species_type_kind.name)
            if self.type_specimen is not None:
                type_info.append(self.type_specimen)
            if self.collection is not None:
                type_info.append(f"in {self.collection!r}")
            if type_info:
                data["type"] = "; ".join(type_info)
            if self.citation_group is not None:
                data["citation_group"] = self.citation_group.name
            data["verbatim_citation"] = self.verbatim_citation
            if include_data:
                data["data"] = self.data

            spacing = " " * ((depth + 2) * 4)
            result = "".join(
                [result]
                + [f"{spacing}{key}: {value}\n" for key, value in data.items() if value]
                + list(getinput.display_tags(spacing, self.tags))
                + list(getinput.display_tags(spacing, self.type_tags))
                + [
                    f"{spacing}{comment.get_description()}\n"
                    for comment in self.comments
                    if include_data
                    or comment.kind
                    not in (
                        constants.CommentKind.structured_quote,
                        constants.CommentKind.automatic_change,
                    )
                ]
            )
        return result

    def display(self, *, full: bool = True, include_data: bool = False) -> None:
        print(
            self.get_description(
                full=full, include_data=include_data, include_taxon=True
            ),
            end="",
        )

    def knowledge_level(self, *, verbose: bool = False) -> int:
        """Returns whether all necessary attributes of the name have been filled in."""
        required_fields = set(self.get_required_fields())
        if "original_citation" in required_fields and self.original_citation is None:
            if verbose:
                print("0 because no original citation")
            return 0
        deprecated_fields = set(self.get_deprecated_fields())
        for field in required_fields:
            if field in deprecated_fields:
                if getattr(self, field) is not None:
                    if verbose:
                        print(f"1 because {field} is set")
                    return 1
            elif getattr(self, field) is None:
                if verbose:
                    print(f"1 because {field} is missing")
                return 1
        if verbose:
            print("2 because all fields are set")
        return 2

    def is_patronym(self) -> bool:
        snc = self.species_name_complex
        if snc is None:
            return False
        return snc.kind.is_patronym()

    def is_fossil(self) -> bool:
        return self.taxon.age is not AgeClass.extant

    def is_ichno(self) -> bool:
        return self.taxon.age.is_ichno()

    def set_nos(self) -> None:
        if self.original_citation is None:
            print("No original citation; cannot set nos")
            return
        required_derived_tags = [group for group, _ in self.get_required_derived_tags()]
        for group in self.get_missing_tags(required_derived_tags):
            if len(group) >= 2:
                tag_cls = group[1]
                # static analysis: ignore
                new_tag = tag_cls(source=self.original_citation)
                print(f"Adding tag: {new_tag!r}")
                assert isinstance(new_tag, TypeTag)
                self.add_type_tag(new_tag)

    def original_citation_data_level(self) -> tuple[OriginalCitationDataLevel, str]:
        if self.original_citation is None:
            return (OriginalCitationDataLevel.no_citation, "missing original_citation")
        if self.original_citation.has_tag(models.article.ArticleTag.NeedsTranslation):
            return (
                OriginalCitationDataLevel.no_citation,
                "original citation needs translation",
            )
        if self.original_citation.lacks_full_text():
            return (OriginalCitationDataLevel.no_citation, "non-original citation")
        source_tags = list(get_tags_from_original_citation(self))
        if not source_tags:
            return (OriginalCitationDataLevel.no_data, "no tags from original citation")
        missing = []
        for group in self.get_required_details_tags():
            if not any(tag in source_tags for tag in group):
                missing.append(group)
        if missing:
            return (OriginalCitationDataLevel.some_data, tag_list(missing))
        return (OriginalCitationDataLevel.all_required_data, "")

    def name_data_level(self) -> tuple[NameDataLevel, str]:
        missing_fields = set(self.get_empty_required_fields())
        crucial_missing = _CRUCIAL_MISSING_FIELDS[self.group] & missing_fields
        if crucial_missing:
            return (
                NameDataLevel.missing_crucial_fields,
                f"missing {', '.join(sorted(crucial_missing))}",
            )
        if missing_fields:
            return (
                NameDataLevel.missing_required_fields,
                f"missing {', '.join(sorted(missing_fields))}",
            )

        required_details_tags = list(self.get_required_details_tags())
        missing_details_tags = list(self.get_missing_tags(required_details_tags))
        if missing_details_tags:
            return (NameDataLevel.missing_details_tags, tag_list(missing_details_tags))

        required_derived_tags = [tags for tags, _ in self.get_required_derived_tags()]
        missing_derived_tags = list(self.get_missing_tags(required_derived_tags))
        if missing_derived_tags:
            return (NameDataLevel.missing_derived_tags, tag_list(missing_derived_tags))

        return (NameDataLevel.nothing_needed, "")

    def fill_data_level(self) -> tuple[FillDataLevel, str]:
        missing_fields = set(self.get_empty_required_fields())
        required_details_tags = list(self.get_required_details_tags())
        missing_details_tags = list(self.get_missing_tags(required_details_tags))
        required_derived_tags = [tags for tags, _ in self.get_required_derived_tags()]
        missing_derived_tags = list(self.get_missing_tags(required_derived_tags))

        if has_data_from_original(self):
            crucial_missing = _CRUCIAL_MISSING_FIELDS[self.group] & missing_fields
            if crucial_missing:
                return (
                    FillDataLevel.missing_required_fields,
                    f"missing {', '.join(sorted(crucial_missing))}",
                )
            orig = self.original_citation
            if orig is None:
                return (FillDataLevel.needs_basic_data, "missing original_citation")
            missing_sourced_tags = [
                (tag, no_tag)
                for tag, no_tag in required_details_tags
                if not (
                    self.has_tag_from_source(tag, orig) or self.has_type_tag(no_tag)
                )
            ]
            if missing_sourced_tags:
                return (FillDataLevel.incomplete_detail, tag_list(missing_sourced_tags))
            if missing_derived_tags:
                if self.group is Group.genus and self.has_type_tag(
                    TypeTag.TypeSpeciesDetail
                ):
                    return (FillDataLevel.nothing_needed, "")
                return (
                    FillDataLevel.incomplete_derived_tags,
                    tag_list(missing_derived_tags),
                )
            return (FillDataLevel.nothing_needed, "")
        elif missing_fields:
            return (
                FillDataLevel.needs_basic_data,
                f"missing {', '.join(sorted(missing_fields))}",
            )
        elif missing_details_tags:
            return (FillDataLevel.missing_detail, tag_list(missing_details_tags))
        elif missing_derived_tags:
            return (FillDataLevel.missing_detail, tag_list(missing_derived_tags))
        elif required_details_tags:
            return (
                FillDataLevel.no_data_from_original,
                "has all required tags, but no data from original",
            )
        else:
            return (FillDataLevel.nothing_needed, "")

    def has_tag_from_source(self, tag_cls: TypeTagCons, source: Article) -> bool:
        tag_id = tag_cls._tag
        return any(
            tag[0] == tag_id and tag[2] == source.id
            for tag in self.get_raw_tags_field("type_tags")
        )

    def requires_etymology(self) -> bool:
        if self.group is Group.genus:
            return (
                self.nomenclature_status.requires_name_complex()
                and self.numeric_year() >= _ETYMOLOGY_CUTOFF
            )
        elif self.group is Group.species:
            return (
                self.nomenclature_status.requires_name_complex()
                and not self.nomenclature_status.is_variant()
                and (self.numeric_year() >= _ETYMOLOGY_CUTOFF or self.is_patronym())
            )
        else:
            return False

    def get_required_details_tags(self) -> Iterable[tuple[TypeTagCons, TypeTagCons]]:
        if self.requires_etymology():
            yield ETYMOLOGY_TAGS
        if (
            self.group is Group.species
            and self.numeric_year() >= _DATA_CUTOFF
            and self.nomenclature_status.requires_type()
        ):
            yield LOCATION_TAGS
            yield SPECIMEN_TAGS

    def get_required_derived_tags(
        self,
    ) -> Iterable[tuple[tuple[TypeTagCons, ...], ExperimentalLintCondition]]:
        if self.group is Group.species:
            if self.collection and self.collection.id == MULTIPLE_COLLECTION:
                yield (TypeTag.Repository,), ExperimentalLintCondition(always=True)
            if (
                self.type_specimen
                and self.species_type_kind is not constants.SpeciesGroupType.syntypes
            ):
                if not self.is_ichno():
                    yield (TypeTag.Organ, TypeTag.NoOrgan), ExperimentalLintCondition(
                        only_with_tags_from_original=SPECIMEN_TAGS
                    )
                if not self.is_fossil():
                    yield (TypeTag.Date, TypeTag.NoDate), ExperimentalLintCondition(
                        only_with_tags_from_original=SPECIMEN_TAGS
                    )
                    yield (
                        TypeTag.CollectedBy,
                        TypeTag.NoCollector,
                        TypeTag.Involved,
                    ), ExperimentalLintCondition(
                        only_with_tags_from_original=SPECIMEN_TAGS
                    )
                    yield (TypeTag.Age, TypeTag.NoAge), ExperimentalLintCondition(
                        only_with_tags_from_original=SPECIMEN_TAGS
                    )
                    yield (TypeTag.Gender, TypeTag.NoGender), ExperimentalLintCondition(
                        only_with_tags_from_original=SPECIMEN_TAGS
                    )
            if self.is_patronym() and not self.nomenclature_status.is_variant():
                yield (TypeTag.NamedAfter,), ExperimentalLintCondition(
                    only_with_tags_from_original=ETYMOLOGY_TAGS
                )
            if self.type_specimen and self.species_type_kind and not self.collection:
                yield (TypeTag.ProbableRepository,), ExperimentalLintCondition(
                    only_with_tags_from_original=SPECIMEN_TAGS
                )
        if self.group is Group.genus:
            if self.nomenclature_status.requires_type():
                if self.genus_type_kind is None or self.genus_type_kind.requires_tag():
                    yield (
                        TypeTag.IncludedSpecies,
                        TypeTag.GenusCoelebs,
                    ), ExperimentalLintCondition(always=True)
        if self.original_rank is not None and self.original_rank.needs_textual_rank:
            yield (TypeTag.TextualOriginalRank,), ExperimentalLintCondition(always=True)

    def get_missing_tags(
        self, required_tags: Iterable[tuple[TypeTagCons, ...]]
    ) -> Iterable[tuple[TypeTagCons, ...]]:
        for group in required_tags:
            if not any(self.has_type_tag(tag) for tag in group):
                yield group

    def get_required_fields(self) -> Iterable[str]:
        if (
            self.status is Status.spurious
            or self.nomenclature_status is NomenclatureStatus.informal
        ):
            return
        yield "original_name"
        if self.original_name is not None:
            yield "corrected_original_name"
        if self.corrected_original_name is not None:
            yield "original_rank"

        yield "author_tags"
        yield "year"
        if not (
            self.original_citation is not None
            and self.original_citation.type is ArticleType.WEB
        ):
            yield "page_described"
        yield "original_citation"
        if self.original_citation is None:
            yield "verbatim_citation"
            if self.verbatim_citation is not None:
                yield "citation_group"

        if self.nomenclature_status.requires_type():
            # Yield this early because it's often easier to first get all the *Detail
            # tags and then fill in the required fields.
            if self.group is Group.species:
                yield "type_tags"
            if self.group is Group.genus:
                if (
                    (self.type is None and self.genus_type_kind is None)
                    or (
                        self.genus_type_kind is None
                        and self.original_citation is not None
                    )
                    or (
                        self.genus_type_kind is not None
                        and self.genus_type_kind.requires_tag()
                    )
                    or self.requires_etymology()
                ):
                    yield "type_tags"

        if (
            self.group is Group.genus
            and self.nomenclature_status.requires_name_complex()
        ):
            yield "name_complex"
        if (
            self.group is Group.species
            and self.nomenclature_status.requires_name_complex()
        ):
            yield "species_name_complex"

        if self.nomenclature_status.requires_type():
            if self.group is Group.family:
                yield "type"
            elif self.group is Group.species:
                yield "type_locality"
                # These are special Collections that indicate there is no preserved specimen.
                if self.collection is None or (
                    self.collection.id not in (LOST_COLLECTION, UNTRACED_COLLECTION)
                ):
                    yield "type_specimen"
                yield "collection"
                if self.type_specimen is not None or self.collection is not None:
                    yield "species_type_kind"
            elif self.group is Group.genus:
                if (
                    self.genus_type_kind
                    is not constants.TypeSpeciesDesignation.undesignated
                ):
                    yield "type"
                if self.type is not None:
                    yield "genus_type_kind"
        if (
            (self.group is Group.species or self.original_rank is Rank.subgenus)
            and self.corrected_original_name is not None
            and self.nomenclature_status.requires_original_parent()
            and not self.has_type_tag(TypeTag.NoOriginalParent)
            and models.name.lint.should_require_subgenus_original_parent(self)
        ):
            yield "original_parent"

    def lint(
        self, cfg: LintConfig = LintConfig(autofix=False, interactive=False)
    ) -> Iterable[str]:
        try:
            self.get_description(full=True, include_taxon=True, skip_lint=True)
        except Exception as e:
            yield f"{self.id}: cannot display due to {e}"
            return
        yield from models.name.lint.LINT.run(self, cfg)
        if not self.check_authors():
            yield f"{self}: discrepancy in authors"

    @classmethod
    def clear_lint_caches(cls) -> None:
        models.name.lint.LINT.clear_caches()

    def should_exempt_from_string_cleaning(self, field: str) -> bool:
        return field == "data"

    def validate_as_child_command(self) -> None:
        try:
            self.validate_as_child()
        except ValueError:
            print(f"Validation failed for {self}")

    def validate_as_child(self, status: Status = Status.valid) -> Taxon:
        if self.taxon.rank is Rank.species:
            new_rank = Rank.subspecies
        elif self.taxon.rank is Rank.genus:
            new_rank = Rank.subgenus
        elif self.taxon.rank is Rank.tribe:
            new_rank = Rank.subtribe
        elif self.taxon.rank is Rank.subfamily:
            new_rank = Rank.tribe
        elif self.taxon.rank is Rank.family:
            new_rank = Rank.subfamily
        elif self.taxon.rank is Rank.unranked:
            new_rank = Rank.unranked
        else:
            raise ValueError(f"cannot validate child with rank {self.taxon.rank}")
        return self.validate(parent=self.taxon, rank=new_rank, status=status)

    def validate(
        self,
        status: Status = Status.valid,
        parent: Taxon | None = None,
        rank: Rank | None = None,
    ) -> Taxon:
        assert not self.status.is_base_name()
        old_taxon = self.taxon
        parent_group = helpers.group_of_rank(old_taxon.rank)
        if self.group == Group.species and parent_group != Group.species:
            if rank is None:
                rank = Rank.species
            if parent is None:
                parent = old_taxon
        elif self.group == Group.genus and parent_group != Group.genus:
            if rank is None:
                rank = Rank.genus
            if parent is None:
                parent = old_taxon
        elif self.group == Group.family and parent_group != Group.family:
            if rank is None:
                rank = Rank.family
            if parent is None:
                parent = old_taxon
        else:
            if rank is None:
                rank = old_taxon.rank
            if parent is None:
                parent = old_taxon.parent
        assert parent is not None, f"found no parent for {self}"
        new_taxon = Taxon.make_or_revalidate(rank, self, old_taxon.age, parent)
        self.taxon = new_taxon
        self.status = status
        new_taxon.recompute_name()
        return new_taxon

    def merge(
        self, into: Name, *, allow_valid: bool = False, copy_fields: bool = True
    ) -> None:
        if not allow_valid:
            assert self.status in (
                Status.synonym,
                Status.dubious,
            ), f"Can only merge synonymous names (not {self})"
        if copy_fields:
            if self.type_tags and into.type_tags:
                into.type_tags = [  # type: ignore[assignment]
                    *into.type_tags,
                    *[
                        tag
                        for tag in self.type_tags
                        if not isinstance(tag, TypeTag.AuthorityPageLink)
                    ],
                ]
            self._merge_fields(into, exclude={"id", "tags"})
        self.redirect(into)

    def redirect(self, into: Name) -> None:
        assert self != into, "cannot redirect to self"
        self.status = Status.redirect
        self.target = into

    def open_zoobank(self) -> None:
        lsids = {tag.text for tag in self.get_tags(self.type_tags, TypeTag.LSIDName)}
        if self.corrected_original_name:
            lsids |= {
                zoobank_data.name_lsid
                for zoobank_data in get_zoobank_data(self.corrected_original_name)
            }
        for lsid in lsids:
            url = f"https://zoobank.org/NomenclaturalActs/{lsid}"
            subprocess.check_call(["open", url])

    def print_zoobank_data(self) -> None:
        lsids = {tag.text for tag in self.get_tags(self.type_tags, TypeTag.LSIDName)}
        for lsid in lsids:
            for data in get_zoobank_data_for_act(lsid):
                pprint.pprint(data, sort_dicts=False)

    def open_description(self) -> bool:
        if self.original_citation is None:
            print(f"{self.description()}: original citation unknown")
        else:
            self.original_citation.openf()
        return True

    def open_url(self) -> bool:
        if self.original_citation is None:
            print(f"{self.description()}: original citation unknown")
        else:
            self.original_citation.openurl()
        for tag in self.get_tags(self.type_tags, TypeTag.AuthorityPageLink):
            subprocess.call(["open", tag.url])
        return True

    def _display_plus(self) -> None:
        self.format()
        self.display()

    def remove(self, reason: str | None = None) -> None:
        print("Deleting name: " + self.description())
        self.status = Status.removed
        if reason:
            self.add_comment(constants.CommentKind.removal, reason, None, "")

    def original_valid(self) -> None:
        assert self.original_name is None
        assert self.status == Status.valid
        self.original_name = self.taxon.valid_name

    def get_root_name_forms(self) -> Iterable[str]:
        if self.species_name_complex is None:
            return [self.root_name]
        return self.species_name_complex.get_forms(self.root_name)

    def get_normalized_root_name(self) -> str:
        if self.species_name_complex is None:
            return self.root_name
        try:
            return self.species_name_complex.get_form(
                self.root_name, constants.GrammaticalGender.masculine
            )
        except ValueError:
            return self.root_name

    def get_normalized_root_name_for_homonymy(self) -> str:
        match self.group:
            case Group.species:
                root_name = self.get_normalized_root_name()
                return models.name_complex.normalize_root_name_for_homonymy(
                    root_name, self.species_name_complex
                )
            case Group.family:
                if self.type is not None:
                    try:
                        stem = self.type.get_stem()
                    except ValueError:
                        pass
                    else:
                        if stem is not None:
                            return stem
        return self.root_name

    def short_description(self) -> str:
        return self.root_name

    def concise_markdown_link(self) -> str:
        if self.corrected_original_name:
            name = self.corrected_original_name
        else:
            name = self.root_name
        if self.group in (Group.genus, Group.species):
            name = f"_{name}_"
        parts = [name]
        if self.author_tags:
            parts.append(" " + self.taxonomic_authority())
        if self.year:
            parts.append(f", {self.numeric_year()}")
        return f"[{''.join(parts)}](/n/{self.id})"

    def __str__(self) -> str:
        return self.description()

    def __repr__(self) -> str:
        return self.description()

    def set_paper(
        self,
        *,
        paper: Article | None = None,
        page_described: None | int | str = None,
        original_name: int | None = None,
        force: bool = False,
        **kwargs: Any,
    ) -> None:
        if paper is None:
            paper = self.get_value_for_foreign_class(
                "original_citation", Article, allow_none=False
            )
        if original_name is None and self.status == Status.valid:
            original_name = self.taxon.valid_name
        attributes = [
            ("author_tags", paper.author_tags),
            ("year", paper.year),
            ("original_citation", paper),
            ("page_described", page_described),
            ("original_name", original_name),
        ]
        for label, value in attributes:
            if value is None:
                continue
            current_value = getattr(self, label)
            if current_value is not None:
                if current_value != value and current_value != str(value):
                    print(
                        f"Warning: {label} does not match (given as {current_value},"
                        f" paper has {value})"
                    )
                    if force:
                        setattr(self, label, value)
            else:
                setattr(self, label, value)
        self.s(**kwargs)
        self.fill_required_fields()

    def has_lint_ignore(self, label: str) -> bool:
        return any(
            isinstance(tag, TypeTag.IgnoreLintName) and tag.label == label
            for tag in self.type_tags
        )

    def possible_citation_groups(self) -> int:
        if self.verbatim_citation is not None:
            same_citation = list(
                # pyanalyze doesn't understand the .id field properly
                self.select_valid().filter(  # static analysis: ignore[incompatible_argument]
                    Name.verbatim_citation == self.verbatim_citation, Name.id != self.id
                )
            )
            if same_citation:
                print("=== same verbatim_citation:")
                for nam in same_citation:
                    nam.display()

        similar = [
            self.get_similar_names_and_papers_for_author(author.family_name)
            for author in self.get_authors()
        ]
        if not similar:
            return 0
        similar_art_sets, similar_nam_sets = zip(*similar, strict=True)
        similar_arts = set.intersection(*similar_art_sets)
        if similar_arts:
            print(f"=== {len(similar_arts)} similar articles")
            for art in similar_arts:
                print(repr(art))
        similar_names = set.intersection(*similar_nam_sets)
        if not similar_names:
            return 0

        print(f"=== {len(similar_names)} similar names")
        for nam in sorted(similar_names, key=lambda nam: nam.numeric_page_described()):
            nam.display()

        citations = {
            nam.original_citation
            for nam in similar_names
            if nam.original_citation is not None
        }
        if citations:
            print("=== citations for names with same author")
            cgs = Counter(art.citation_group for art in citations)
            for cg, count in cgs.most_common():
                print(count, cg)

        similar_name_cgs = [
            nam.citation_group
            for nam in similar_names
            if nam.citation_group is not None
        ]
        if similar_name_cgs:
            print("=== citation_group for names with same author")
            for cg, count in Counter(similar_name_cgs).most_common():
                print(count, cg)
        return len(similar_names)

    def get_similar_names_and_papers_for_author(
        self, author_name: str
    ) -> tuple[set[Article], set[Name]]:
        authors = Person.select_valid().filter(Person.family_name == author_name)
        nams = set()
        arts = set()
        year = self.numeric_year()
        for author in authors:
            for art in author.get_sorted_derived_field("articles"):
                if art.numeric_year() == year:
                    arts.add(art)
            for nam in author.get_sorted_derived_field("names"):
                if (
                    nam.id != self.id
                    and nam.numeric_year() == year
                    and nam.citation_group != self.citation_group
                ):
                    nams.add(nam)
        return arts, nams

    def get_possible_mapped_classification_entries(
        self,
    ) -> Iterable[models.ClassificationEntry]:
        if self.original_citation is None:
            return []
        return models.ClassificationEntry.select_valid().filter(
            models.ClassificationEntry.mapped_name == self,
            models.ClassificationEntry.article == self.original_citation,
        )

    def get_mapped_classification_entry(self) -> models.ClassificationEntry | None:
        if self.original_citation is None:
            return None
        candidates = list(self.get_possible_mapped_classification_entries())
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        candidates = [ce for ce in candidates if ce.name == self.original_name]
        if len(candidates) == 1:
            return candidates[0]
        candidates = [ce for ce in candidates if not ce.rank.is_synonym]
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) == 2 and {ce.rank for ce in candidates} == {
            Rank.genus,
            Rank.subgenus,
        }:
            return next(ce for ce in candidates if ce.rank is Rank.genus)
        return None

    def highest_taxon(self) -> Taxon:
        taxa = list(
            Taxon.select_valid().filter(
                Taxon.base_name == self, Taxon.rank != Rank.species_group
            )
        )
        if not taxa:
            return self.taxon
        return max(taxa, key=lambda taxon: taxon.rank)

    def make_usage_list(self, style: str = "paper") -> str:
        usages: dict[Article, str | None] = {}
        for related_nam in self.taxon.get_names():
            if related_nam.resolve_variant() == self:
                for ce in related_nam.get_classification_entries():
                    comment_pieces = []
                    if ce.page is not None:
                        if ce.page.isnumeric():
                            comment_pieces.append(f"p. {ce.page}")
                        else:
                            comment_pieces.append(ce.page)
                    usages[ce.article] = (
                        "; ".join(comment_pieces) if comment_pieces else None
                    )
        for tag in self.tags:
            if isinstance(tag, NameTag.ValidUse):
                if usages.get(tag.source) is not None:
                    continue
                if not tag.comment:
                    comment = None
                elif tag.comment[0].isnumeric():
                    comment = f"p. {tag.comment}"
                else:
                    comment = tag.comment
                usages[tag.source] = comment
        authority = self.taxonomic_authority()
        lines = [
            f"### Usages of _{self.original_name}_ {authority} (currently _{self.highest_taxon().valid_name}_)\n\n"
        ]
        i = 1
        for source, comment in sorted(
            usages.items(), key=lambda pair: pair[0].get_date_object()
        ):
            if source.is_unpublished() or source.numeric_year() < 1980:
                continue
            if comment is not None:
                comment_str = f" ({comment})"
            else:
                comment_str = ""
            lines.append(f"{i}. {source.cite(style)}{comment_str}\n")
            i += 1
        lines.append("\n")
        return "".join(lines)

    @classmethod
    def add_hmw_tags(cls, family: str) -> None:
        while True:
            nam = Name.getter("corrected_original_name").get_one()
            if nam is None:
                break
            taxon = nam.taxon
            if taxon.rank is Rank.subspecies:
                taxon = taxon.parent_of_rank(Rank.species)
            taxon.display()
            number = getinput.get_line("number> ")
            if not number:
                break
            default = taxon.valid_name
            hmw_name = getinput.get_line("name> ", default=default)
            if not hmw_name:
                hmw_name = default
            tag = NameTag.HMW(number=f"{family}{number}", name=hmw_name)
            nam.add_tag(tag)
            print(f"Added tag {tag} to {nam}")


class NameComment(BaseModel):
    call_sign = "NCO"
    grouping_field = "kind"
    fields_may_be_invalid: ClassVar[set[str]] = {"name"}
    clirm_table_name = "name_comment"
    fields_without_completers: ClassVar[set[str]] = {"text"}

    name = Field[Name]("name_id", related_name="comments")
    kind = Field[constants.CommentKind]()
    date = Field[int]()
    text = TextField()
    source = Field[Article | None]("source_id", related_name="name_comments")
    page = Field[str | None]()

    search_fields: ClassVar[list[SearchField]] = [
        SearchField(SearchFieldType.literal, "kind"),
        SearchField(SearchFieldType.text, "text", highlight_enabled=True),
    ]

    def get_search_dicts(self) -> list[dict[str, Any]]:
        if self.should_skip():
            return []
        return [{"kind": self.kind.name, "text": self.text}]

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(NameComment.kind != constants.CommentKind.removed)

    def is_invalid(self) -> bool:
        return self.kind is constants.CommentKind.removed

    def should_skip(self) -> bool:
        return self.kind in (
            constants.CommentKind.removed,
            constants.CommentKind.structured_quote,
            constants.CommentKind.automatic_change,
        )

    def should_exempt_from_string_cleaning(self, field: str) -> bool:
        return field == "text" and self.kind is constants.CommentKind.structured_quote

    def get_page_title(self) -> str:
        return f"Comment on {self.name}"

    @classmethod
    def make(
        cls,
        name: Name,
        kind: constants.CommentKind,
        text: str,
        source: Article | None = None,
        page: str | None = None,
    ) -> NameComment:
        return cls.create(
            name=name,
            kind=kind,
            text=text,
            date=int(time.time()),
            source=source,
            page=page,
        )

    @classmethod
    def create_interactively(
        cls,
        name: Name | None = None,
        kind: constants.CommentKind | None = None,
        text: str | None = None,
        source: Article | None = None,
        page: str | None = None,
        **kwargs: Any,
    ) -> NameComment | None:
        if name is None:
            name = cls.get_value_for_foreign_key_field_on_class(
                "name", allow_none=False
            )
            if name is None:
                return None
        if kind is None:
            kind = getinput.get_enum_member(constants.CommentKind, prompt="kind> ")
            if kind is None:
                return None
        if text is None:
            text = getinput.get_line(prompt="text> ")
            if text is None:
                return None
        if source is None:
            source = cls.get_value_for_foreign_class("source", Article)
        if page is None:
            page = getinput.get_line(prompt="page> ")
        return cls.make(name=name, kind=kind, text=text, source=source, page=page)

    def get_description(self) -> str:
        components = [
            self.kind.name,
            datetime.datetime.fromtimestamp(self.date, tz=datetime.UTC).strftime(
                "%b %d, %Y %H:%M:%S"
            ),
        ]
        if self.source:
            components.append(
                f"{{{self.source.name}}}:{self.page}"
                if self.page
                else f"{{{self.source.name}}}"
            )
        return f'{self.text} ({"; ".join(components)})'


def citation_sort_key(nam: Name) -> tuple[object, ...]:
    return (
        nam.numeric_year(),
        nam.numeric_page_described(),
        nam.corrected_original_name or "",
        nam.root_name or "",
    )


def get_ordered_names(nams: Iterable[Name] | None) -> list[Name]:
    if nams is None:
        return []
    return sorted(nams, key=citation_sort_key)


def has_data_from_original(nam: Name) -> bool:
    if not nam.original_citation or not nam.get_raw_tags_field("type_tags"):
        return False
    if not nam.type_tags:
        return False
    for tag in nam.type_tags:
        if isinstance(tag, SOURCE_TAGS) and tag.source == nam.original_citation:
            return True
        if isinstance(tag, SOURCE_DATA_TAGS):
            return True
        if (
            isinstance(tag, NO_DATA_FROM_SOURCE_TAGS)
            and tag.source == nam.original_citation
        ):
            return True
    return False


def get_tags_from_original_citation(nam: Name) -> Iterable[TypeTagCons]:
    if not nam.original_citation or not nam.get_raw_tags_field("type_tags"):
        return
    if not nam.type_tags:
        return
    for tag in nam.type_tags:
        if isinstance(tag, SOURCE_TAGS) and tag.source == nam.original_citation:
            yield type(tag)  # static analysis: ignore[incompatible_yield]
        elif isinstance(
            tag,
            (
                TypeTag.IncludedSpecies,
                TypeTag.GenusCoelebs,
                TypeTag.TextualOriginalRank,
            ),
        ):
            yield type(tag)  # static analysis: ignore[incompatible_yield]
        elif (
            isinstance(tag, NO_DATA_FROM_SOURCE_TAGS)
            and tag.source == nam.original_citation
        ):
            yield type(tag)  # static analysis: ignore[incompatible_yield]


def tag_list(tags: Iterable[tuple[TypeTagCons, ...]]) -> str:
    # display only the first one in the group; no need to mention NoEtymology c.s.
    firsts = [group[0] for group in tags]
    return (
        "missing"
        f" {', '.join(first.__name__ if hasattr(first, '__name__') else repr(first) for first in firsts)}"
    )


def clean_original_name(original_name: str) -> str:
    if " " in original_name and original_name[0].islower():
        original_name = original_name[0].upper() + original_name[1:]
    original_name = (
        original_name.replace("(?)", "")
        .replace("?", "")
        .replace("æ", "ae")
        .replace("œ", "oe")
        .replace("Œ", "Oe")
        .replace("Æ", "Ae")
        .replace("ë", "e")
        .replace("í", "i")
        .replace("ï", "i")
        .replace("á", "a")
        .replace('"', "")
        .replace("'", "")
        .replace("ř", "r")
        .replace("é", "e")
        .replace("š", "s")
        .replace("á", "a")
        .replace("ć", "c")
        .replace(" cf. ", " ")
        .replace(" aff. ", " ")
    )
    original_name = re.sub(r"\s+", " ", original_name).strip()
    original_name = re.sub(r", ", " ", original_name)
    original_name = re.sub(r" [1-9α-ω]\. ", " ", original_name)
    return re.sub(r"([a-z]{2})-([a-z]{2})", r"\1\2", original_name)


def infer_corrected_original_name(original_name: str, group: Group) -> str | None:
    original_name = clean_original_name(original_name)
    if group is Group.high:
        if re.match(r"^(Pan-)?[A-Z][a-z]+?$", original_name):
            return original_name
    elif group in (Group.genus, Group.family):
        if re.match(r"^[A-Z][a-z]+$", original_name):
            return original_name
    elif group is Group.species:
        if re.match(r"^[A-Z][a-z]+( [a-z]+){1,2}$", original_name):
            return original_name
        if re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+$", original_name):
            genus, species = original_name.split()
            return f"{genus} {species.lower()}"
        if match := re.fullmatch(
            r"^\[(?P<genus>[A-Z][a-z]+)\] (?P<species>[a-z]+)$", original_name
        ):
            return f"{match.group('genus')} {match.group('species')}"
        match = re.match(
            (
                r"^(?P<genus>[A-Z][a-z]+)( \([A-Z][a-z]+\))?"
                r" (?P<species>[A-Z]?[a-z]+)"
                r"((,? [Vv]ar\.)? (?P<subspecies>[A-Z]?[a-z]+))?$"
                # We can't support infrasubspecific names here
                # because then names like "Buffelus indicus Varietas sondaica"
                # get inferred as infrasubspecific names instead of subspecific names
            ),
            original_name,
        )
        if match:
            name = f'{match.group("genus")} {match.group("species").lower()}'
            if match.group("subspecies"):
                name += " " + match.group("subspecies").lower()
            return name
    return None


def _stringify_tag(tag: adt.ADT) -> str:
    name = type(tag).__name__
    name = re.sub(r"(?=[A-Z])", " ", name).lower().strip()
    args = [str(value) for value in tag.__dict__.values() if value]
    if args:
        return f"{name}: {'; '.join(args)}"
    else:
        return name


class SelectionReason(enum.IntEnum):
    primary_homonymy = 1
    secondary_homonymy = 2
    synonymy = 3
    mixed_homonymy = 4
    reverse_mixed_homonymy = 5


class NameTag(adt.ADT):
    PreoccupiedBy(name=Name, comment=NotRequired[Markdown], tag=1)  # type: ignore[name-defined]
    UnjustifiedEmendationOf(name=Name, comment=NotRequired[Markdown], tag=2)  # type: ignore[name-defined]
    IncorrectSubsequentSpellingOf(name=Name, comment=NotRequired[Markdown], tag=4)  # type: ignore[name-defined]
    NomenNovumFor(name=Name, comment=NotRequired[Markdown], tag=5)  # type: ignore[name-defined]
    # If we don't know which of 2-4 to use
    VariantOf(name=Name, comment=NotRequired[Markdown], tag=6)  # type: ignore[name-defined]
    # "opinion" is a reference to an Article containing an ICZN Opinion
    PartiallySuppressedBy(opinion=Article, comment=NotRequired[Markdown], tag=7)  # type: ignore[name-defined]
    FullySuppressedBy(opinion=Article, comment=NotRequired[Markdown], tag=8)  # type: ignore[name-defined]
    # ICZN Art. 40.2. Family-group name replaced another name before 1961 because of synonymy of the type genus.
    TakesPriorityOf(  # type: ignore[name-defined]
        name=Name,
        comment=NotRequired[Markdown],
        optional_source=NotRequired[Article],
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        is_in_prevailing_usage=NotRequired[bool],
        tag=9,
    )
    # ICZN Art. 23.9. The reference is to the nomen protectum relative to which precedence is reversed.
    NomenOblitum(  # type: ignore[name-defined]
        name=Name,
        comment=NotRequired[Markdown],
        optional_source=NotRequired[Article],
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        tag=10,
    )
    MandatoryChangeOf(name=Name, comment=NotRequired[Markdown], tag=11)  # type: ignore[name-defined]
    # Conserved by placement on the Official List.
    Conserved(opinion=Article, comment=NotRequired[Markdown], tag=12)  # type: ignore[name-defined]
    IncorrectOriginalSpellingOf(name=Name, comment=NotRequired[Markdown], tag=13)  # type: ignore[name-defined]
    # selection as the correct original spelling
    SelectionOfSpelling(  # type: ignore[name-defined]
        optional_source=NotRequired[Article],
        comment=NotRequired[Markdown],
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        tag=14,
    )
    SubsequentUsageOf(name=Name, comment=NotRequired[Markdown], tag=15)  # type: ignore[name-defined]
    SelectionOfPriority(  # type: ignore[name-defined]
        over_name=NotRequired[Name],
        optional_source=NotRequired[Article],
        comment=NotRequired[Markdown],
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        over_ce=NotRequired[ClassificationEntry],
        tag=16,
    )
    # Priority reversed by ICZN opinion
    ReversalOfPriority(over=Name, opinion=Article, comment=NotRequired[Markdown], tag=17)  # type: ignore[name-defined]
    # Placed on the Official Index, but without being suppressed.
    Rejected(opinion=Article, comment=NotRequired[Markdown], tag=18)  # type: ignore[name-defined]
    # See discussion in docs/name.md
    JustifiedEmendationOf(  # type: ignore[name-defined]
        name=Name,
        justification=EmendationJustification,
        comment=NotRequired[Markdown],
        tag=19,
    )
    HMW(number=Managed, name=Managed, tag=20)  # type: ignore[name-defined]
    # Not required, used when the name can't have the "as_emended" nomenclature status
    AsEmendedBy(name=Name, comment=NotRequired[Markdown], tag=21)  # type: ignore[name-defined]
    NameCombinationOf(name=Name, comment=NotRequired[Markdown], tag=22)  # type: ignore[name-defined]

    # These replace PreoccupiedBy for species-group names
    PrimaryHomonymOf(name=Name, comment=NotRequired[Markdown], tag=23)  # type: ignore[name-defined]
    SecondaryHomonymOf(name=Name, comment=NotRequired[Markdown], tag=24)  # type: ignore[name-defined]

    # Used if another name does not preoccupy a name (e.g., because it is unavailable
    # or spelled differently), but there are suggestions in the literature that it is.
    NotPreoccupiedBy(name=Name, comment=NotRequired[Markdown], tag=25)  # type: ignore[name-defined]

    # An arbitrary nomenclature status that is applicable to this name.
    Condition(status=NomenclatureStatus, comment=NotRequired[Markdown], tag=26)  # type: ignore[name-defined]

    # A use of this name as valid, for the purposes of ICZN Art. 23.9 (reversal of precedence).
    ValidUse(source=Article, comment=NotRequired[Markdown], tag=27)  # type: ignore[name-defined]

    # Like Condition(variety_or_form), but separate because of special conditions in the Code.
    VarietyOrForm(comment=NotRequired[Markdown], tag=28)  # type: ignore[name-defined]
    # Same for not_used_as_valid
    NotUsedAsValid(comment=NotRequired[Markdown], tag=29)  # type: ignore[name-defined]

    NeedsPrioritySelection(over=Name, reason=SelectionReason, tag=30)  # type: ignore[name-defined]

    # ICZN Art. 59.3
    PermanentlyReplacedSecondaryHomonymOf(  # type: ignore[name-defined]
        name=Name,
        optional_source=NotRequired[Article],
        is_in_use=bool,
        comment=NotRequired[Markdown],
        replacement_name=NotRequired[Name],  # The name replacing the homonym
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        tag=31,
    )
    IgnorePreoccupationBy(name=Name, comment=Markdown, tag=32)  # type: ignore[name-defined]

    # Deprecated (not necessary) and obsolete.
    MappedClassificationEntry(ce=ClassificationEntry, tag=33)  # type: ignore[name-defined]

    MisidentificationOf(name=Name, comment=NotRequired[Markdown], tag=34)  # type: ignore[name-defined]

    UnavailableVersionOf(name=Name, comment=NotRequired[Markdown], tag=35)  # type: ignore[name-defined]

    # The name should be rejected (e.g., suppressed by the Commission or marked as a nomen oblitum)
    # but this has not happened yet.
    PendingRejection(comment=NotRequired[Markdown], tag=36)  # type: ignore[name-defined]

    RerankingOf(name=Name, comment=NotRequired[Markdown], tag=37)  # type: ignore[name-defined]


VARIANT_TAGS = (
    NameTag.VariantOf,
    NameTag.UnjustifiedEmendationOf,
    NameTag.JustifiedEmendationOf,
    NameTag.IncorrectOriginalSpellingOf,
    NameTag.SubsequentUsageOf,
    NameTag.MandatoryChangeOf,
    NameTag.IncorrectSubsequentSpellingOf,
    NameTag.NameCombinationOf,
    NameTag.RerankingOf,
    NameTag.UnavailableVersionOf,
)

CONSTRUCTABLE_STATUS_TO_TAG = {
    NomenclatureStatus.unjustified_emendation: NameTag.UnjustifiedEmendationOf,
    NomenclatureStatus.incorrect_subsequent_spelling: (
        NameTag.IncorrectSubsequentSpellingOf
    ),
    NomenclatureStatus.variant: NameTag.VariantOf,
    NomenclatureStatus.mandatory_change: NameTag.MandatoryChangeOf,
    NomenclatureStatus.nomen_novum: NameTag.NomenNovumFor,
    NomenclatureStatus.incorrect_original_spelling: NameTag.IncorrectOriginalSpellingOf,
    NomenclatureStatus.subsequent_usage: NameTag.SubsequentUsageOf,
    NomenclatureStatus.misidentification: NameTag.MisidentificationOf,
    NomenclatureStatus.name_combination: NameTag.NameCombinationOf,
    NomenclatureStatus.preoccupied: NameTag.PreoccupiedBy,
    NomenclatureStatus.reranking: NameTag.RerankingOf,
}
STATUS_TO_TAG = {
    **CONSTRUCTABLE_STATUS_TO_TAG,
    NomenclatureStatus.justified_emendation: NameTag.JustifiedEmendationOf,
}
PREOCCUPIED_TAGS = (
    NameTag.PreoccupiedBy,
    NameTag.PrimaryHomonymOf,
    NameTag.SecondaryHomonymOf,
    NameTag.PermanentlyReplacedSecondaryHomonymOf,
)


class LectotypeDesignationTerm(enum.IntEnum):
    lectotype = 1
    holotype = 2
    the_type = 3
    other = 4


class TypeTag(adt.ADT):
    # 1 used to be Collector, kept for compatibility with some deleted names
    _RawCollector(text=Managed, tag=1)  # type: ignore[name-defined]
    Date(date=Managed, tag=2)  # type: ignore[name-defined]
    Gender(gender=constants.SpecimenGender, tag=3)  # type: ignore[name-defined]
    Age(age=constants.SpecimenAge, tag=4)  # type: ignore[name-defined]
    Organ(  # type: ignore[name-defined]
        organ=constants.SpecimenOrgan,
        detail=NotRequired[Managed],
        condition=NotRequired[Markdown],
        tag=5,
    )
    Altitude(altitude=Managed, unit=constants.AltitudeUnit, tag=6)  # type: ignore[name-defined]
    Coordinates(latitude=Managed, longitude=Managed, tag=7)  # type: ignore[name-defined]
    # Authoritative description for a disputed type locality. Should be rarely used.
    TypeLocality(text=Markdown, tag=8)  # type: ignore[name-defined]
    StratigraphyDetail(text=Markdown, tag=9)  # type: ignore[name-defined]
    Habitat(text=Markdown, tag=10)  # type: ignore[name-defined]
    Host(name=Markdown, tag=11)  # type: ignore[name-defined]
    # 12 is unused
    # subsequent designation of the type (for a genus)
    TypeDesignation(  # type: ignore[name-defined]
        optional_source=NotRequired[Article],
        type=Name,
        comment=NotRequired[Markdown],
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        tag=13,
    )
    # like the above, but by the Commission (and therefore trumping everything else)
    CommissionTypeDesignation(  # type: ignore[name-defined]
        opinion=Article,
        type=Name,
        comment=NotRequired[Markdown],
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        tag=14,
    )
    LectotypeDesignation(  # type: ignore[name-defined]
        optional_source=NotRequired[Article],
        lectotype=Managed,
        valid=bool,
        comment=NotRequired[Markdown],
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        year=NotRequired[Managed],
        term=NotRequired[LectotypeDesignationTerm],
        # Whether the author explicitly chose a type from the type series
        # Relevant in Art. 74.5
        is_explicit_choice=NotRequired[bool],
        # Whether the author assumed there was a single type specimen
        # Relevant in Art. 74.6
        is_assumption_of_monotypy=NotRequired[bool],
        tag=15,
    )
    NeotypeDesignation(  # type: ignore[name-defined]
        optional_source=NotRequired[Article],
        neotype=Managed,
        valid=bool,
        comment=NotRequired[Markdown],
        page=NotRequired[Managed],
        verbatim_citation=NotRequired[Markdown],
        citation_group=NotRequired[CitationGroup],
        page_link=NotRequired[URL],
        tag=16,
    )
    # more information on the specimen
    SpecimenDetail(text=Markdown, source=Article, tag=17)  # type: ignore[name-defined]
    # phrasing of the type locality in a particular source
    LocationDetail(  # type: ignore[name-defined]
        text=Markdown,
        source=Article,
        comment=NotRequired[Markdown],
        page=NotRequired[Managed],
        translation=NotRequired[Markdown],
        page_link=NotRequired[URL],
        tag=18,
    )
    # an originally included species in a genus without an original type designation
    IncludedSpecies(  # type: ignore[name-defined]
        name=Name,
        comment=NotRequired[Markdown],
        page=NotRequired[Managed],
        page_link=NotRequired[URL],
        classification_entry=NotRequired[ClassificationEntry],
        tag=19,
    )
    # repository that holds some of the type specimens
    Repository(repository=Collection, tag=20)  # type: ignore[name-defined]
    # indicates that it was originally a genus coelebs
    GenusCoelebs(comments=NotRequired[Markdown], tag=21)  # type: ignore[name-defined]
    # quotation with information about a type species
    TypeSpeciesDetail(text=Markdown, source=Article, tag=22)  # type: ignore[name-defined]
    # Likely location of the type specimen.
    ProbableRepository(repository=Collection, reasoning=NotRequired[Markdown], tag=23)  # type: ignore[name-defined]
    # Data on the repository of the type material.
    CollectionDetail(text=Markdown, source=Article, tag=24)  # type: ignore[name-defined]
    # Quotes about the original citation.
    CitationDetail(text=Markdown, source=Article, tag=25)  # type: ignore[name-defined]
    DefinitionDetail(text=Markdown, source=Article, tag=26)  # type: ignore[name-defined]
    EtymologyDetail(text=Markdown, source=Article, tag=27)  # type: ignore[name-defined]
    NamedAfter(person=Person, tag=28)  # type: ignore[name-defined]
    CollectedBy(person=Person, tag=29)  # type: ignore[name-defined]

    DifferentAuthority(comment=NotRequired[Markdown], tag=30)  # type: ignore[name-defined]
    NoEtymology(source=Article, tag=31)  # type: ignore[name-defined]
    NoLocation(source=Article, tag=32)  # type: ignore[name-defined]
    NoSpecimen(source=Article, tag=33)  # type: ignore[name-defined]
    NoDate(source=Article, tag=34)  # type: ignore[name-defined]
    NoCollector(source=Article, tag=35)  # type: ignore[name-defined]
    NoOrgan(source=Article, tag=36)  # type: ignore[name-defined]
    NoGender(source=Article, tag=37)  # type: ignore[name-defined]
    NoAge(source=Article, tag=38)  # type: ignore[name-defined]
    # Person who is involved in the type specimen's history
    Involved(person=Person, comment=NotRequired[Markdown], tag=39)  # type: ignore[name-defined]
    # Indicates that a General type locality cannot be fixed
    ImpreciseLocality(comment=NotRequired[Markdown], tag=40)  # type: ignore[name-defined]
    # Arbitrary text about nomenclature
    NomenclatureDetail(text=Markdown, source=Article, tag=41)  # type: ignore[name-defined]
    TextualOriginalRank(text=Managed, tag=42)  # type: ignore[name-defined]
    # Denotes that this name does something grammatically incorrect. A published
    # paper should correct it.
    IncorrectGrammar(text=Markdown, tag=43)  # type: ignore[name-defined]
    LSIDName(text=Managed, tag=44)  # type: ignore[name-defined]
    TypeSpecimenLink(url=URL, tag=45)  # type: ignore[name-defined]
    # Ignore lints with a specific label
    IgnoreLintName(label=Managed, comment=NotRequired[Markdown], tag=46)  # type: ignore[name-defined]
    RejectedLSIDName(text=Managed, tag=47)  # type: ignore[name-defined]
    # For hybrids and composites
    PartialTaxon(taxon=Taxon, tag=48)  # type: ignore[name-defined]
    FormerRepository(repository=Collection, tag=49)  # type: ignore[name-defined]
    ExtraRepository(repository=Collection, tag=50)  # type: ignore[name-defined]
    FutureRepository(repository=Collection, tag=51)  # type: ignore[name-defined]
    TypeSpecimenLinkFor(url=URL, specimen=Managed, suffix=NotRequired[Managed], tag=52)  # type: ignore[name-defined]
    PhyloCodeNumber(number=int, tag=53)  # type: ignore[name-defined]
    AuthorityPageLink(url=URL, confirmed=bool, page=Managed, tag=54)  # type: ignore[name-defined]
    GuessedRepository(repository=Collection, score=float, tag=55)  # type: ignore[name-defined]

    # Used for subgenera proposed without an associated genus
    NoOriginalParent(tag=56)  # type: ignore[name-defined]
    # Sources for old names
    SourceDetail(text=Markdown, source=Article, tag=57)  # type: ignore[name-defined]

    # Can be used optionally to hold the fully verbatim original name, including abbreviations.
    # Not mandatory.
    VerbatimName(text=Markdown, tag=58)  # type: ignore[name-defined]

    IgnorePotentialCitationFrom(article=Article, comment=NotRequired[Markdown], tag=59)  # type: ignore[name-defined]

    # Description of the taxon
    DescriptionDetail(text=Markdown, source=Article, tag=60)  # type: ignore[name-defined]

    InterpretedTypeLocality(text=Markdown, tag=61)  # type: ignore[name-defined]
    InterpretedTypeSpecimen(text=Markdown, tag=62)  # type: ignore[name-defined]
    InterpretedTypeTaxon(text=Markdown, tag=63)  # type: ignore[name-defined]
    NomenclatureComments(text=Markdown, record=Managed, tag=64)  # type: ignore[name-defined]

    AdditionalTypeSpecimen(  # type: ignore[name-defined]
        text=Managed, kind=TypeSpecimenKind, comment=NotRequired[Markdown], tag=65
    )
    OriginalTypification(  # type: ignore[name-defined]
        basis=SpeciesBasis, source=Article, comment=NotRequired[Markdown], tag=66
    )

    PhylogeneticDefinition(type=PhylogeneticDefinitionType, source=Article, comment=NotRequired[Markdown], tag=67)  # type: ignore[name-defined]
    InternalSpecifier(name=Name, comment=NotRequired[Markdown], tag=68)  # type: ignore[name-defined]
    ExternalSpecifier(name=Name, comment=NotRequired[Markdown], tag=69)  # type: ignore[name-defined]
    TreatAsEquivalentTo(name=Name, tag=70)  # type: ignore[name-defined]
    MustNotInclude(name=Name, comment=NotRequired[Markdown], tag=71)  # type: ignore[name-defined]
    MustBePartOf(name=Name, comment=NotRequired[Markdown], tag=72)  # type: ignore[name-defined]
    MustNotBePartOf(name=Name, comment=NotRequired[Markdown], tag=73)  # type: ignore[name-defined]
    MustBeExtinct(comment=NotRequired[Markdown], tag=74)  # type: ignore[name-defined]

    StructuredVerbatimCitation(  # type: ignore[name-defined]
        volume=NotRequired[Managed],
        issue=NotRequired[Managed],
        start_page=NotRequired[Managed],
        end_page=NotRequired[Managed],
        series=NotRequired[Managed],
        tag=75,
    )


SOURCE_TAGS = (
    TypeTag.SourceDetail,
    TypeTag.LocationDetail,
    TypeTag.SpecimenDetail,
    TypeTag.CitationDetail,
    TypeTag.EtymologyDetail,
    TypeTag.CollectionDetail,
    TypeTag.DefinitionDetail,
    TypeTag.TypeSpeciesDetail,
    TypeTag.NomenclatureDetail,
    TypeTag.DescriptionDetail,
)
NO_DATA_FROM_SOURCE_TAGS = (TypeTag.NoEtymology, TypeTag.NoLocation, TypeTag.NoSpecimen)
SOURCE_DATA_TAGS = (
    TypeTag.IncludedSpecies,
    TypeTag.GenusCoelebs,
    TypeTag.TextualOriginalRank,
)
ETYMOLOGY_TAGS = (TypeTag.EtymologyDetail, TypeTag.NoEtymology)
LOCATION_TAGS = (TypeTag.LocationDetail, TypeTag.NoLocation)
SPECIMEN_TAGS = (TypeTag.SpecimenDetail, TypeTag.NoSpecimen)

if TYPE_CHECKING:
    TypeTagCons: TypeAlias = Any
    NameTagCons: TypeAlias = Any
else:
    TypeTagCons = TypeTag._Constructors
    NameTagCons = NameTag._Constructors


def write_names(
    nams: Sequence[Name],
    *,
    depth: int = 0,
    full: bool = False,
    organized: bool = False,
    file: IO[str] = sys.stdout,
    tag_classes: tuple[type[TypeTag]] = (TypeTag.LocationDetail,),
) -> None:
    if not nams:
        return

    def write_nam(nam: Name) -> str:
        lines = [f"{nam}\n"]
        if full and nam.type_tags:
            for tag in nam.type_tags:
                if isinstance(tag, tag_classes):
                    lines.append(f"    {tag}\n")
        return "".join(lines)

    if organized:
        display_organized([(write_nam(nam), nam.taxon) for nam in nams], depth=depth)
    else:
        for nam in nams:
            file.write(getinput.indent(write_nam(nam), depth + 8))


@dataclass
class ExperimentalLintCondition:
    """When to apply an experimental lind check.

    Conditions are positive: they apply e.g. before year 1760 if before_year=1760 is set.
    """

    always: bool = False
    before_year: int | None = 1760
    after_year: int | None = 2020
    above_name_id: int | None = 100_000
    above_article_id: int | None = 70_000
    require_in_groups: Sequence[Group] = ()
    only_with_tags_from_original: Sequence[TypeTagCons] = ()

    def should_apply(self, nam: Name, cfg: LintConfig) -> bool:
        if self.always:
            return True
        if (
            self.only_with_tags_from_original
            and nam.original_citation is not None
            and not any(
                type(tag) in self.only_with_tags_from_original
                and hasattr(tag, "source")
                and tag.source == nam.original_citation
                for tag in nam.type_tags
            )
        ):
            return False
        if cfg.experimental:
            return True
        if self.before_year is not None and nam.numeric_year() <= self.before_year:
            return True
        if self.after_year is not None and nam.numeric_year() >= self.after_year:
            return True
        if self.above_name_id is not None and nam.id >= self.above_name_id:
            return True
        if (
            self.above_article_id is not None
            and nam.original_citation is not None
            and nam.original_citation.id >= self.above_article_id
        ):
            return True
        if nam.group in self.require_in_groups:
            return True
        return False
