from __future__ import annotations

import builtins
import datetime
import json
import re
import sys
import time
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from typing import IO, TYPE_CHECKING, Any, TypeAlias

from peewee import CharField, ForeignKeyField, IntegerField, TextField

from ... import adt, events, getinput
from .. import constants, helpers, models
from ..constants import (
    AgeClass,
    EmendationJustification,
    FillDataLevel,
    Group,
    NomenclatureStatus,
    Rank,
    SpeciesNameKind,
    Status,
)
from ..definition import Definition
from ..derived_data import DerivedField
from .article import Article
from .base import (
    ADTField,
    BaseModel,
    EnumField,
    LintConfig,
    get_str_completer,
    get_tag_based_derived_field,
)
from .citation_group import CitationGroup
from .collection import Collection
from .location import Location
from .name_complex import NameComplex, SpeciesNameComplex
from .person import AuthorTag, Person, get_new_authors_list
from .taxon import Taxon, display_organized

_CRUCIAL_MISSING_FIELDS: dict[Group, set[str]] = {
    Group.species: {
        "species_name_complex",
        "original_name",
        "corrected_original_name",
        "original_rank",
    },
    Group.genus: {"name_complex", "original_rank"},
    Group.family: {"type", "original_rank"},
    Group.high: {"original_rank"},
}
_ETYMOLOGY_CUTOFF = 1990
_DATA_CUTOFF = 1900


class Name(BaseModel):
    creation_event = events.Event["Name"]()
    save_event = events.Event["Name"]()
    label_field = "corrected_original_name"
    grouping_field = "status"
    call_sign = "N"
    field_defaults = {
        "nomenclature_status": NomenclatureStatus.available,
        "status": Status.valid,
    }
    excluded_fields = {"data"}
    markdown_fields = {"verbatim_citation"}

    # Basic data
    group = EnumField(Group)
    root_name = CharField()
    status = EnumField(Status)
    taxon = ForeignKeyField(Taxon, related_name="names", db_column="taxon_id")
    original_name = CharField(null=True)
    # Original name, with corrections for issues like capitalization and diacritics. Should not correct incorrect original spellings
    # for other reasons (e.g., prevailing usage). Consider a case where Gray (1825) names _Mus Somebodyi_, then Gray (1827) spells it
    # _Mus Somebodii_ and all subsequent authors follow this usage, rendering it a justified emendation. In this case, the 1825 name
    # should have original_name _Mus Somebodyi_, corrected original name _Mus somebodyi_, and root name _somebodii_. The 1827 name
    # should be listed as a justified emendation.
    corrected_original_name = CharField(null=True)
    nomenclature_status = EnumField(
        NomenclatureStatus, default=NomenclatureStatus.available
    )
    # for redirects
    target = ForeignKeyField(
        "self", null=True, db_column="target", related_name="redirects"
    )

    # Citation and authority
    author_tags = ADTField(lambda: AuthorTag, null=True)
    original_citation = ForeignKeyField(
        Article, null=True, db_column="original_citation_id", related_name="new_names"
    )
    page_described = CharField(null=True)
    verbatim_citation = CharField(null=True)
    citation_group = ForeignKeyField(
        CitationGroup, null=True, db_column="citation_group", related_name="names"
    )
    year = CharField(null=True)  # redundant with data for the publication itself

    # Gender and stem
    name_complex = ForeignKeyField(NameComplex, null=True, related_name="names")
    species_name_complex = ForeignKeyField(
        SpeciesNameComplex, null=True, related_name="names"
    )

    # Types
    type = ForeignKeyField(
        "self", null=True, db_column="type_id", related_name="typified_names"
    )  # for family and genus group
    verbatim_type = CharField(null=True)  # deprecated
    type_locality = ForeignKeyField(
        Location,
        related_name="type_localities",
        db_column="type_locality_id",
        null=True,
    )
    type_specimen = CharField(null=True)
    collection = ForeignKeyField(
        Collection, null=True, db_column="collection_id", related_name="type_specimens"
    )
    genus_type_kind = EnumField(constants.TypeSpeciesDesignation, null=True)
    species_type_kind = EnumField(constants.SpeciesGroupType, null=True)
    type_tags = ADTField(lambda: TypeTag, null=True)
    original_rank = EnumField(constants.Rank, null=True)

    # Miscellaneous data
    data = TextField(null=True)
    _definition = CharField(null=True, db_column="definition")
    tags = ADTField(lambda: NameTag, null=True)

    class Meta:
        db_table = "name"

    derived_fields = [
        DerivedField(
            "fill_data_level", FillDataLevel, lambda nam: nam.fill_data_level()[0]
        ),
        get_tag_based_derived_field(
            "preoccupied_names", lambda: Name, "tags", lambda: NameTag.PreoccupiedBy, 1
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

    def get_stem(self) -> str | None:
        if self.group != Group.genus or self.name_complex is None:
            return None
        return self.name_complex.get_stem_from_name(self.root_name)

    @property
    def definition(self) -> Definition | None:
        data = self._definition
        if data is None:
            return None
        else:
            return Definition.unserialize(data)

    @definition.setter
    def definition(self, defn: Definition) -> None:
        if defn is None:
            self._definition = None
        else:
            self._definition = defn.serialize()

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
        self, interactive: bool = False, quiet: bool = True, dry_run: bool = False
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

    def infer_corrected_original_name(self, aggressive: bool = False) -> str | None:
        if not self.original_name:
            return None
        original_name = clean_original_name(self.original_name)
        if self.nomenclature_status.permissive_corrected_original_name():
            return None
        if self.group in (Group.genus, Group.high):
            if re.match(r"^[A-Z][a-z]+$", original_name):
                return original_name
            if self.group is Group.genus:
                match = re.match(r"^[A-Z][a-z]+ \(([A-Z][a-z]+)\)$", original_name)
                if match:
                    return match.group(1)
        elif self.group is Group.family:
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
                if aggressive and self.type.stem is not None:
                    stem = self.type.stem
                    for suffix in helpers.VALID_SUFFIXES:
                        if original_name == f"{stem}{suffix}":
                            return original_name
                    if aggressive and not any(
                        original_name.endswith(suffix)
                        for suffix in helpers.VALID_SUFFIXES
                    ):
                        return f"{stem}idae"
        elif self.group is Group.species:
            if re.match(r"^[A-Z][a-z]+( [a-z]+){1,2}$", original_name):
                if self.root_name == original_name.split(" ")[-1]:
                    return original_name
            if re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+$", original_name):
                genus, species = original_name.split()
                return f"{genus} {species.lower()}"
            match = re.match(
                (
                    r"^(?P<genus>[A-Z][a-z]+)( \([A-Z][a-z]+\))?"
                    r" (?P<species>[A-Z]?[a-z]+)((,? var\.)?"
                    r" (?P<subspecies>[A-Z]?[a-z]+))?$"
                ),
                original_name,
            )
            if match:
                name = f'{match.group("genus")} {match.group("species").lower()}'
                if match.group("subspecies"):
                    name += " " + match.group("subspecies").lower()
                if self.root_name == name.split(" ")[-1]:
                    return name
        return None

    def get_value_for_field(self, field: str, default: str | None = None) -> Any:
        if (
            field == "collection"
            and self.collection is None
            and self.type_specimen is not None
        ):
            coll_name = self.type_specimen.split()[0]
            getter = Collection.getter("label")
            if coll_name in getter:
                coll = getter(coll_name)
                print(f"inferred collection to be {coll} from {self.type_specimen}")
                return coll
            return super().get_value_for_field(field, default=default)
        elif field == "original_name":
            if self.original_name is None and self.group in (Group.genus, Group.high):
                return self.root_name
            else:
                return super().get_value_for_field(field, default=default)
        elif field == "corrected_original_name":
            inferred = self.infer_corrected_original_name()
            if inferred is not None:
                print(
                    f"inferred corrected_original_name to be {inferred!r} from"
                    f" {self.original_name!r}"
                )
                return inferred
            else:
                if self.corrected_original_name is not None:
                    default = self.corrected_original_name
                else:
                    default = self.original_name
                return super().get_value_for_field(field, default=default)
        elif field == "original_rank":
            if self.original_rank is None:
                inferred = self.infer_original_rank()
                if inferred is not None:
                    print(
                        f"inferred original_rank to be {inferred!r} from"
                        f" {self.original_name!r}"
                    )
                    return inferred
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
        return {
            **callbacks,
            "add_comment": self.add_comment,
            "o": self.open_description,
            "add_type_identical": self._add_type_identical_callback,
            "from_paper": self._from_paper_callback,
            "add_child": self._add_child_callback,
            "syn_from_paper": self._syn_from_paper_callback,
            "add_syn": self._add_syn_callback,
            "make_variant": self.make_variant,
            "add_variant": self.add_variant,
            "add_nomen_nudum": lambda: self.add_nomen_nudum(interactive=True),
            "preoccupied_by": self.preoccupied_by,
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
            "validate_as_child": self.validate_as_child,
            "add_nominate": lambda: self.taxon.add_nominate(),
            "merge": self._merge,
            "remove_duplicate": self._remove_duplicate,
            "edit_comments": self.edit_comments,
            "replace_original_citation": self.replace_original_citation,
        }

    def _merge(self) -> None:
        other = Name.getter(None).get_one("name to merge into> ")
        if other is None:
            return
        self.merge(other)

    def _remove_duplicate(self) -> None:
        other = Name.getter(None).get_one("name to remove> ")
        if other is None:
            return
        other.merge(self)

    def print_fill_data_level(self) -> None:
        level, reason = self.fill_data_level()
        if reason:
            print(f"{level.name}: {reason}")
        else:
            print(level.name)

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
            and "type_tags" != fields[-1]
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
        self.data = json.dumps(data)

    def add_data(self, field: str, value: Any, concat_duplicate: bool = False) -> None:
        data = self._load_data()
        if field in data:
            if concat_duplicate:
                existing = data[field]
                if isinstance(existing, list):
                    value = existing + [value]
                else:
                    value = [existing, value]
            else:
                raise ValueError(f"{field} is already in {data}")
        data[field] = value
        self.data = json.dumps(data)

    def get_data(self, field: str) -> Any:
        data = self._load_data()
        return data[field]

    def _load_data(self) -> dict[str, Any]:
        if self.data is None or self.data == "":
            return {}
        else:
            return json.loads(self.data)

    def get_tag_target(self, tag_cls: Tag._Constructor) -> Name | None:  # type: ignore
        tags = self.tags
        if tags:
            for tag in tags:
                if isinstance(tag, tag_cls) and hasattr(tag, "name"):
                    return tag.name
        return None

    def add_tag(self, tag: NameTag) -> None:
        tags = self.tags
        if tags is None:
            self.tags = [tag]
        else:
            self.tags = tags + (tag,)

    def add_type_tag(self, tag: TypeTag) -> None:
        type_tags = self.type_tags
        if type_tags is None:
            self.type_tags = [tag]
        else:
            self.type_tags = type_tags + (tag,)

    def has_type_tag(self, tag_cls: TypeTagCons) -> bool:
        tag_id = tag_cls._tag
        for tag in self.get_raw_tags_field("type_tags"):
            if tag[0] == tag_id:
                return True
        return False

    def map_type_tags(self, fn: Callable[[Any], Any | None]) -> None:
        self.map_tags_field(Name.type_tags, fn)

    def map_type_tags_by_type(
        self, typ: builtins.type[Any], fn: Callable[[Any], Any]
    ) -> None:
        self.map_tags_by_type(Name.type_tags, typ, fn)

    def replace_original_citation(self, new_citation: Article | None = None) -> None:
        if new_citation is None:
            new_citation = Article.get_one_by("name", allow_empty=False)
        if new_citation is None:
            return
        existing = self.original_citation

        def map_fn(tag: TypeTag) -> TypeTag:
            for tag_cls in (
                TypeTag.LocationDetail,
                TypeTag.SpecimenDetail,
                TypeTag.CitationDetail,
                TypeTag.EtymologyDetail,
            ):
                if isinstance(tag, tag_cls) and tag.source == existing:
                    return tag_cls(tag.text, new_citation)
            return tag

        self.map_type_tags(map_fn)
        self.original_citation = new_citation

    def add_included(self, species: Name, comment: str = "") -> None:
        assert isinstance(species, Name)
        self.add_type_tag(TypeTag.IncludedSpecies(species, comment))

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
        interactive: bool = True,
    ) -> NameComment | None:
        return NameComment.create_interactively(
            name=self, kind=kind, text=text, source=source, page=page
        )

    def add_child_taxon(
        self, rank: Rank, name: str, age: AgeClass | None = None, **kwargs: Any
    ) -> Taxon:
        return self.taxon.add_static(rank, name, age=age, **kwargs)

    def add_nomen_nudum(self, interactive: bool = True) -> Name | None:
        """Adds a nomen nudum similar to this name."""
        if interactive:
            paper = self.get_value_for_foreign_class("paper", Article)
            if paper is not None:
                return self.taxon.syn_from_paper(
                    paper=paper,
                    root_name=self.root_name,
                    original_name=self.original_name,
                    author_tags=self.author_tags,
                    nomenclature_status=NomenclatureStatus.nomen_nudum,
                )
        return self.taxon.add_syn(
            root_name=self.root_name,
            original_name=self.original_name,
            author_tags=self.author_tags,
            nomenclature_status=NomenclatureStatus.nomen_nudum,
        )

    def description(self) -> str:
        if self.original_name:
            out = self.original_name
        elif self.root_name:
            out = self.root_name
        else:
            out = "<no name>"
        if self.author_tags:
            out += " %s" % self.taxonomic_authority()
        if self.year:
            out += f", {self.year}"
        if self.page_described:
            out += f":{self.page_described}"
        parenthesized_bits = []
        try:
            taxon = self.taxon
        except Taxon.DoesNotExist:
            parenthesized_bits.append("= <invalid taxon>")
        else:
            if taxon.valid_name != self.original_name:
                parenthesized_bits.append(f"= {taxon.valid_name}")
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

    def get_date_object(self) -> datetime.date:
        return helpers.get_date_object(self.year)

    def numeric_year(self) -> int:
        return self.get_date_object().year

    def valid_numeric_year(self) -> int | None:
        if self.year is not None and helpers.is_valid_date(self.year):
            return self.numeric_year()
        else:
            return None

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
        if self.nomenclature_status != NomenclatureStatus.available:
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
        self.nomenclature_status = status  # type: ignore

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
            nam = self.taxon.syn_from_paper(root_name, paper, interactive=False)
            if nam is None:
                return None
            nam.original_name = original_name
            nam.nomenclature_status = status
        else:
            nam = self.taxon.add_syn(
                root_name,
                nomenclature_status=status,
                original_name=original_name,
                interactive=False,
            )
            if nam is None:
                return None
        tag_cls = CONSTRUCTABLE_STATUS_TO_TAG[status]
        nam.page_described = page_described
        nam.add_tag(tag_cls(self, ""))
        if interactive:
            nam.fill_required_fields()
        return nam

    def preoccupied_by(
        self, name: Name | None = None, comment: str | None = None
    ) -> None:
        if name is None:
            name = Name.getter("corrected_original_name").get_one(prompt="name> ")
        if name is None:
            return
        self.add_tag(NameTag.PreoccupiedBy(name, comment or ""))
        if self.nomenclature_status == NomenclatureStatus.available:
            self.nomenclature_status = NomenclatureStatus.preoccupied  # type: ignore
        else:
            print(f"not changing status because it is {self.nomenclature_status}")

    def conserve(self, opinion: Article, comment: str | None = None) -> None:
        self.add_tag(NameTag.Conserved(opinion, comment or ""))

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

    def copy_year(self, quiet: bool = False) -> None:
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
    def check_all_authors(cls, autofix: bool = True, quiet: bool = True) -> list[Name]:
        bad = []
        for nam in cls.select_valid().filter(cls.author_tags != None):
            if not nam.check_authors(autofix=autofix, quiet=quiet):
                bad.append(nam)
        print(f"{len(bad)} discrepancies")
        return bad

    def check_authors(self, autofix: bool = True, quiet: bool = False) -> bool:
        if self.author_tags is None:
            return True
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
        getinput.print_diff(self.author_tags, new_authors)
        if autofix:
            self.author_tags = new_authors  # type: ignore
        return False

    def get_description(
        self,
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
            out += " %s" % self.taxonomic_authority()
        if self.year is not None:
            out += ", %s" % self.year
        if self.page_described is not None:
            out += ":%s" % self.page_described
        if self.original_citation is not None:
            out += " {%s}" % self.original_citation.name
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
        if full and (self.original_name is not None or self.definition is not None):
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
            if self.definition is not None:
                parts.append(str(self.definition))
            out += " (%s)" % "; ".join(parts)
        if include_taxon:
            out += f" (={self.taxon})"
        knowledge_level = self.knowledge_level()
        if knowledge_level == 0:
            intro_line = getinput.red(out)
        elif knowledge_level == 1:
            intro_line = getinput.blue(out)
        else:
            intro_line = getinput.green(out)
        result = " " * ((depth + 1) * 4) + intro_line + "\n"
        if full:
            data: dict[str, Any] = {}
            if not skip_lint:
                lints = "; ".join(self.lint())
                if lints:
                    data["lint"] = lints
            level, reason = self.fill_data_level()
            if level is not FillDataLevel.nothing_needed:
                data["level"] = f"{level.name.upper()} ({reason})"
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
            data["verbatim_type"] = self.verbatim_type
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

    def display(self, full: bool = True, include_data: bool = False) -> None:
        print(
            self.get_description(
                full=full, include_data=include_data, include_taxon=True
            ),
            end="",
        )

    def knowledge_level(self, verbose: bool = False) -> int:
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
            else:
                if getattr(self, field) is None:
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
        required_derived_tags = list(self.get_required_derived_tags())
        for group in self.get_missing_tags(required_derived_tags):
            if len(group) >= 2:
                new_tag = group[1]
                print(f"Adding tag: {new_tag!r}")
                assert isinstance(new_tag, TypeTag)
                self.add_type_tag(new_tag)

    def fill_data_level(self) -> tuple[FillDataLevel, str]:
        if (
            self.name_complex is not None
            and self.original_citation is not None
            and self.name_complex.code_article is constants.GenderArticle.assumed
        ):
            return (
                FillDataLevel.needs_basic_data,
                "'assumed' name_complex for name with original_citation",
            )
        missing_fields = set(self.get_empty_required_fields())
        required_details_tags = list(self.get_required_details_tags())
        missing_details_tags = list(self.get_missing_tags(required_details_tags))
        required_derived_tags = list(self.get_required_derived_tags())
        missing_derived_tags = list(self.get_missing_tags(required_derived_tags))

        def tag_list(tags: Iterable[tuple[TypeTagCons, ...]]) -> str:
            # display only the first one in the group; no need to mention NoEtymology c.s.
            firsts = [group[0] for group in tags]
            return (
                "missing"
                f" {', '.join(first.__name__ if hasattr(first, '__name__') else repr(first) for first in firsts)}"
            )

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
        else:
            if missing_fields:
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
        for tag in self.get_raw_tags_field("type_tags"):
            if tag[0] == tag_id and tag[2] == source.id:
                return True
        return False

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
            yield (TypeTag.EtymologyDetail, TypeTag.NoEtymology)
        if (
            self.group is Group.species
            and self.numeric_year() >= _DATA_CUTOFF
            and self.nomenclature_status.requires_type()
        ):
            yield (TypeTag.LocationDetail, TypeTag.NoLocation)
            yield (TypeTag.SpecimenDetail, TypeTag.NoSpecimen)

    def get_required_derived_tags(self) -> Iterable[tuple[TypeTagCons, ...]]:
        if self.group is Group.species:
            if self.collection and self.collection.id == 366:  # multiple
                yield (TypeTag.Repository,)
            if (
                self.type_specimen
                and self.species_type_kind is not constants.SpeciesGroupType.syntypes
            ):
                if not self.is_ichno():
                    yield (TypeTag.Organ, TypeTag.NoOrgan)
                if not self.is_fossil():
                    yield (TypeTag.Date, TypeTag.NoDate)
                    yield (TypeTag.CollectedBy, TypeTag.NoCollector, TypeTag.Involved)
                    yield (TypeTag.Age, TypeTag.NoAge)
                    yield (TypeTag.Gender, TypeTag.NoGender)
            if self.is_patronym() and not self.nomenclature_status.is_variant():
                yield (TypeTag.NamedAfter,)
            if self.type_specimen and self.species_type_kind and not self.collection:
                yield (TypeTag.ProbableRepository,)
        if self.group is Group.genus:
            if self.nomenclature_status.requires_type():
                if self.genus_type_kind is None or self.genus_type_kind.requires_tag():
                    yield (TypeTag.IncludedSpecies, TypeTag.GenusCoelebs)
        if self.original_rank is Rank.other:
            yield (TypeTag.TextualOriginalRank,)

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
                # 75 (lost) and 381 (untraced) are special Collections that
                # indicate there is no preserved specimen.
                if self.collection is None or (self.collection.id not in (75, 381)):
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

    def lint(
        self, cfg: LintConfig = LintConfig(autofix=False, interactive=False)
    ) -> Iterable[str]:
        try:
            self.get_description(full=True, include_taxon=True, skip_lint=True)
        except Exception as e:
            yield f"{self.id}: cannot display due to {e}"
            return
        if self.status is Status.removed:
            return
        for linter in models.name_lint.LINTERS:
            yield from linter(self, cfg)
        if not self.check_authors():
            yield f"{self}: discrepancy in authors"

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
        else:
            raise ValueError(f"cannot validate child with rank {self.taxon.rank}")
        return self.validate(parent=self.taxon, rank=new_rank, status=status)

    def validate(
        self,
        status: Status = Status.valid,
        parent: Taxon | None = None,
        rank: Rank | None = None,
    ) -> Taxon:
        assert self.status not in (
            Status.valid,
            Status.nomen_dubium,
            Status.species_inquirenda,
        )
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
        self.status = status  # type: ignore
        new_taxon.recompute_name()
        return new_taxon

    def merge(self, into: Name, allow_valid: bool = False) -> None:
        if not allow_valid:
            assert self.status in (
                Status.synonym,
                Status.dubious,
            ), f"Can only merge synonymous names (not {self})"
        if self.type_tags and into.type_tags:
            into.type_tags += self.type_tags
        self._merge_fields(into, exclude={"id"})
        self.status = Status.redirect  # type: ignore
        self.target = into

    def open_description(self) -> bool:
        if self.original_citation is None:
            print("%s: original citation unknown" % self.description())
        else:
            self.original_citation.openf()
        return True

    def remove(self, reason: str | None = None) -> None:
        print("Deleting name: " + self.description())
        self.status = Status.removed  # type: ignore
        if reason:
            self.add_comment(constants.CommentKind.removal, reason, None, "")

    def original_valid(self) -> None:
        assert self.original_name is None
        assert self.status == Status.valid
        self.original_name = self.taxon.valid_name

    def compute_gender(self, dry_run: bool = True) -> bool:
        if (
            self.group != Group.species
            or self.species_name_complex is None
            or self.species_name_complex.kind != SpeciesNameKind.adjective
        ):
            return True
        try:
            genus = self.taxon.parent_of_rank(Rank.genus)
        except ValueError:
            return True
        if genus.base_name.name_complex is None:
            return True

        gender = genus.base_name.name_complex.gender
        try:
            computed = self.species_name_complex.get_form(self.root_name, gender)
        except ValueError:
            print(f"Invalid root_name for {self} (complex {self.species_name_complex})")
            return False
        if computed != self.root_name:
            print(f"Modifying root_name for {self}: {self.root_name} -> {computed}")
            if not dry_run:
                self.root_name = computed
            return False
        return True

    def short_description(self) -> str:
        return self.root_name

    def __str__(self) -> str:
        return self.description()

    def __repr__(self) -> str:
        return self.description()

    def set_paper(
        self,
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
                        "Warning: %s does not match (given as %s, paper has %s)"
                        % (label, current_value, value)
                    )
                    if force:
                        setattr(self, label, value)
            else:
                setattr(self, label, value)
        self.s(**kwargs)
        self.fill_required_fields()

    def detect_and_set_type(
        self, verbatim_type: str | None = None, verbose: bool = False
    ) -> bool:
        if verbatim_type is None:
            verbatim_type = self.verbatim_type
        if verbose:
            print(f"=== Detecting type for {self} from {verbatim_type}")
        candidates = self.detect_type(verbatim_type=verbatim_type, verbose=verbose)
        if candidates is None or not candidates:
            print(
                "Verbatim type %s for name %s could not be recognized"
                % (verbatim_type, self)
            )
            return False
        elif len(candidates) == 1:
            if verbose:
                print("Detected type: %s" % candidates[0])
            self.type = candidates[0]
            return True
        else:
            print(
                "Verbatim type %s for name %s yielded multiple possible names: %s"
                % (verbatim_type, self, candidates)
            )
            return False

    def detect_type(
        self, verbatim_type: str | None = None, verbose: bool = False
    ) -> list[Name]:
        def cleanup(name: str) -> str:
            return re.sub(
                r"\s+",
                " ",
                name.strip().rstrip(".").replace("<i>", "").replace("</i>", ""),
            )

        steps = [
            lambda verbatim: verbatim,
            lambda verbatim: re.sub(r"\([^)]+\)", "", verbatim),
            lambda verbatim: re.sub(r"=.*$", "", verbatim),
            lambda verbatim: re.sub(r"\(.*$", "", verbatim),
            lambda verbatim: re.sub(r"\[.*$", "", verbatim),
            lambda verbatim: re.sub(r",.*$", "", verbatim),
            lambda verbatim: self._split_authority(verbatim)[0],
            lambda verbatim: verbatim.split()[1] if " " in verbatim else verbatim,
            lambda verbatim: helpers.convert_gender(
                verbatim, constants.GrammaticalGender.masculine
            ),
            lambda verbatim: helpers.convert_gender(
                verbatim, constants.GrammaticalGender.feminine
            ),
            lambda verbatim: helpers.convert_gender(
                verbatim, constants.GrammaticalGender.neuter
            ),
        ]
        if verbatim_type is None:
            verbatim_type = self.verbatim_type
        if not verbatim_type:
            return []
        candidates = None
        for step in steps:
            new_verbatim = cleanup(step(verbatim_type))
            if verbatim_type != new_verbatim or candidates is None:
                if verbose:
                    print("Trying verbatim type: %s" % new_verbatim)
                verbatim_type = new_verbatim
                candidates = self.detect_type_from_verbatim_type(verbatim_type)
                if candidates:
                    return candidates
        return []

    @staticmethod
    def _split_authority(verbatim_type: str) -> tuple[str, str | None]:
        # if there is an uppercase letter following an all-lowercase word (the species name),
        # the authority is included
        find_authority = re.match(r"^(.* [a-z]+) ([A-Z+].+)$", verbatim_type)
        if find_authority:
            return find_authority.group(1), find_authority.group(2)
        else:
            return verbatim_type, None

    def detect_type_from_verbatim_type(self, verbatim_type: str) -> list[Name]:
        def _filter_by_authority(
            candidates: list[Name], authority: str | None
        ) -> list[Name]:
            if authority is None:
                return candidates
            split = re.split(r", (?=\d)", authority, maxsplit=1)
            if len(split) == 1:
                author, year = authority, None
            else:
                author, year = split
            result = []
            for candidate in candidates:
                if candidate.taxonomic_authority() != author:
                    continue
                if year is not None and candidate.year != year:
                    continue
                result.append(candidate)
            return result

        parent = self.taxon
        if self.group == Group.family:
            verbatim = verbatim_type.split(maxsplit=1)
            if len(verbatim) == 1:
                type_name, authority = verbatim[0], None
            else:
                type_name, authority = verbatim
            return _filter_by_authority(
                parent.find_names(verbatim[0], group=Group.genus), authority
            )
        else:
            type_name, authority = self._split_authority(verbatim_type)
            if " " not in type_name:
                root_name = type_name
                candidates = Name.filter(
                    Name.root_name == root_name, Name.group == Group.species
                )
                find_abbrev = False
            else:
                match = re.match(r"^[A-Z]\. ([a-z]+)$", type_name)
                find_abbrev = bool(match)
                if match:
                    root_name = match.group(1)
                    candidates = Name.filter(
                        Name.root_name == root_name, Name.group == Group.species
                    )
                else:
                    candidates = Name.filter(
                        Name.original_name == type_name, Name.group == Group.species
                    )
            # filter by authority first because it's cheaper
            candidates = _filter_by_authority(candidates, authority)
            candidates = [
                candidate
                for candidate in candidates
                if candidate.taxon.is_child_of(parent)
            ]
            # if we failed to find using the original_name, try the valid_name
            if not candidates and not find_abbrev:
                candidates = (
                    Name.filter(Name.status == Status.valid)
                    .join(Taxon)
                    .where(Taxon.valid_name == type_name)
                )
                candidates = _filter_by_authority(candidates, authority)
                candidates = [
                    candidate
                    for candidate in candidates
                    if candidate.taxon.is_child_of(parent)
                ]
            return candidates

    def possible_citation_groups(self) -> int:
        if self.verbatim_citation is not None:
            same_citation = list(
                self.select_valid().filter(
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
    fields_may_be_invalid = {"name"}

    name = ForeignKeyField(Name, related_name="comments", db_column="name_id")
    kind = EnumField(constants.CommentKind)
    date = IntegerField()
    text = TextField()
    source = ForeignKeyField(
        Article, related_name="name_comments", null=True, db_column="source_id"
    )
    page = TextField()

    class Meta:
        db_table = "name_comment"

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(NameComment.kind != constants.CommentKind.removed)

    def is_invalid(self) -> bool:
        return self.kind is constants.CommentKind.removed

    def should_skip(self) -> bool:
        return self.kind in (
            constants.CommentKind.removed,
            constants.CommentKind.structured_quote,
        )

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
            datetime.datetime.fromtimestamp(self.date).strftime("%b %d, %Y %H:%M:%S"),
        ]
        if self.source:
            components.append(
                f"{{{self.source.name}}}:{self.page}"
                if self.page
                else f"{{{self.source.name}}}"
            )
        return f'{self.text} ({"; ".join(components)})'


def has_data_from_original(nam: Name) -> bool:
    if not nam.original_citation or not nam.get_raw_tags_field("type_tags"):
        return False
    if not nam.type_tags:
        return False
    for tag in nam.type_tags:
        if isinstance(tag, SOURCE_TAGS) and tag.source == nam.original_citation:
            return True
        if isinstance(
            tag,
            (
                TypeTag.IncludedSpecies,
                TypeTag.GenusCoelebs,
                TypeTag.TextualOriginalRank,
            ),
        ) or tag in (TypeTag.NoEtymology, TypeTag.NoLocation, TypeTag.NoSpecimen):
            return True
    return False


def is_valid_page_described(page_described: str) -> bool:
    parts = re.split(r", |-", page_described)
    return all(is_valid_page_described_single(part) for part in parts)


def is_valid_page_described_single(page_described: str) -> bool:
    pattern = r" \(footnote( \d+)?\)$"
    if re.search(pattern, page_described):
        return is_valid_page_described_single(re.sub(pattern, "", page_described))
    if page_described.isnumeric():
        return True
    # Roman numerals
    if set(page_described) <= set("ixvcl"):
        return True
    for prefix in ("pl. ", "fig. ", "figs. ", "pls. "):
        if page_described.startswith(prefix):
            return is_valid_page_described_single(page_described[len(prefix) :])
    return False


def clean_original_name(original_name: str) -> str:
    original_name = (
        original_name.replace("(?)", "")
        .replace("?", "")
        .replace("æ", "ae")
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
    )
    original_name = re.sub(r"\s+", " ", original_name).strip()
    return re.sub(r"([a-z]{2})-([a-z]{2})", r"\1\2", original_name)


class NameTag(adt.ADT):
    PreoccupiedBy(name=Name, comment=str, tag=1)  # type: ignore
    UnjustifiedEmendationOf(name=Name, comment=str, tag=2)  # type: ignore
    IncorrectSubsequentSpellingOf(name=Name, comment=str, tag=4)  # type: ignore
    NomenNovumFor(name=Name, comment=str, tag=5)  # type: ignore
    # If we don't know which of 2-4 to use
    VariantOf(name=Name, comment=str, tag=6)  # type: ignore
    # "opinion" is a reference to an Article containing an ICZN Opinion
    PartiallySuppressedBy(opinion=Article, comment=str, tag=7)  # type: ignore
    FullySuppressedBy(opinion=Article, comment=str, tag=8)  # type: ignore
    TakesPriorityOf(name=Name, comment=str, tag=9)  # type: ignore
    # ICZN Art. 23.9. The reference is to the nomen protectum relative to which precedence is reversed.
    NomenOblitum(name=Name, comment=str, tag=10)  # type: ignore
    MandatoryChangeOf(name=Name, comment=str, tag=11)  # type: ignore
    # Conserved by placement on the Official List.
    Conserved(opinion=Article, comment=str, tag=12)  # type: ignore
    IncorrectOriginalSpellingOf(name=Name, comment=str, tag=13)  # type: ignore
    # selection as the correct original spelling
    SelectionOfSpelling(source=Article, comment=str, tag=14)  # type: ignore
    SubsequentUsageOf(name=Name, comment=str, tag=15)  # type: ignore
    SelectionOfPriority(over=Name, source=Article, comment=str, tag=16)  # type: ignore
    # Priority reversed by ICZN opinion
    ReversalOfPriority(over=Name, opinion=Article, comment=str, tag=17)  # type: ignore
    # Placed on the Official Index, but without being suppressed.
    Rejected(opinion=Article, comment=str, tag=18)  # type: ignore
    # See discussion in docs/name.md
    JustifiedEmendationOf(  # type: ignore
        name=Name, justification=EmendationJustification, comment=str, tag=19
    )
    HMW(number=str, name=str, tag=20)  # type: ignore
    # Not required, used when the name can't have the "as_emended" nomenclature status
    AsEmendedBy(name=Name, comment=str, tag=21)  # type: ignore


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
    NomenclatureStatus.preoccupied: NameTag.PreoccupiedBy,
}
STATUS_TO_TAG = {
    **CONSTRUCTABLE_STATUS_TO_TAG,
    NomenclatureStatus.justified_emendation: NameTag.JustifiedEmendationOf,
}


class TypeTag(adt.ADT):
    # 1 used to be Collector, kept for compatibility with some deleted names
    _RawCollector(text=str, tag=1)  # type: ignore
    Date(date=str, tag=2)  # type: ignore
    Gender(gender=constants.SpecimenGender, tag=3)  # type: ignore
    Age(age=constants.SpecimenAge, tag=4)  # type: ignore
    Organ(  # type: ignore
        organ=constants.SpecimenOrgan, detail=str, condition=str, tag=5
    )
    Altitude(altitude=str, unit=constants.AltitudeUnit, tag=6)  # type: ignore
    Coordinates(latitude=str, longitude=str, tag=7)  # type: ignore
    # Authoritative description for a disputed type locality. Should be rarely used.
    TypeLocality(text=str, tag=8)  # type: ignore
    StratigraphyDetail(text=str, tag=9)  # type: ignore
    Habitat(text=str, tag=10)  # type: ignore
    Host(name=str, tag=11)  # type: ignore
    # 12 is unused
    # subsequent designation of the type (for a genus)
    TypeDesignation(source=Article, type=Name, comment=str, tag=13)  # type: ignore
    # like the above, but by the Commission (and therefore trumping everything else)
    CommissionTypeDesignation(opinion=Article, type=Name, tag=14)  # type: ignore
    LectotypeDesignation(  # type: ignore
        source=Article, lectotype=str, valid=bool, comment=str, tag=15
    )
    NeotypeDesignation(  # type: ignore
        source=Article, neotype=str, valid=bool, comment=str, tag=16
    )
    # more information on the specimen
    SpecimenDetail(text=str, source=Article, tag=17)  # type: ignore
    # phrasing of the type locality in a particular source
    LocationDetail(text=str, source=Article, tag=18)  # type: ignore
    # an originally included species in a genus without an original type designation
    IncludedSpecies(name=Name, comment=str, tag=19)  # type: ignore
    # repository that holds some of the type specimens
    Repository(repository=Collection, tag=20)  # type: ignore
    # indicates that it was originally a genus coelebs
    GenusCoelebs(comments=str, tag=21)  # type: ignore
    # quotation with information about a type species
    TypeSpeciesDetail(text=str, source=Article, tag=22)  # type: ignore
    # Likely location of the type specimen.
    ProbableRepository(repository=Collection, reasoning=str, tag=23)  # type: ignore
    # Data on the repository of the type material.
    CollectionDetail(text=str, source=Article, tag=24)  # type: ignore
    # Quotes about the original citation.
    CitationDetail(text=str, source=Article, tag=25)  # type: ignore
    DefinitionDetail(text=str, source=Article, tag=26)  # type: ignore
    EtymologyDetail(text=str, source=Article, tag=27)  # type: ignore
    NamedAfter(person=Person, tag=28)  # type: ignore
    CollectedBy(person=Person, tag=29)  # type: ignore

    DifferentAuthority(comment=str, tag=30)  # type: ignore
    NoEtymology(tag=31)  # type: ignore
    NoLocation(tag=32)  # type: ignore
    NoSpecimen(tag=33)  # type: ignore
    NoDate(tag=34)  # type: ignore
    NoCollector(tag=35)  # type: ignore
    NoOrgan(tag=36)  # type: ignore
    NoGender(tag=37)  # type: ignore
    NoAge(tag=38)  # type: ignore
    # Person who is involved in the type specimen's history
    Involved(person=Person, comment=str, tag=39)  # type: ignore
    # Indicates that a General type locality cannot be fixed
    ImpreciseLocality(comment=str, tag=40)  # type: ignore
    # Arbitrary text about nomenclature
    NomenclatureDetail(text=str, source=Article, tag=41)  # type: ignore
    TextualOriginalRank(text=str, tag=42)  # type: ignore
    # Denotes that this name does something grammatically incorrect. A published
    # paper should correct it.
    IncorrectGrammar(text=str, tag=43)  # type: ignore
    LSIDName(text=str, tag=44)  # type: ignore


SOURCE_TAGS = (
    TypeTag.LocationDetail,
    TypeTag.SpecimenDetail,
    TypeTag.CitationDetail,
    TypeTag.EtymologyDetail,
    TypeTag.CollectionDetail,
    TypeTag.DefinitionDetail,
    TypeTag.TypeSpeciesDetail,
)

if TYPE_CHECKING:
    TypeTagCons: TypeAlias = Any
else:
    TypeTagCons = TypeTag._Constructors


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
