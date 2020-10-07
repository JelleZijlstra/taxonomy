from collections import Counter
import datetime
import json
import re
import sys
import time
from typing import (
    Any,
    Callable,
    Dict,
    IO,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

from peewee import CharField, ForeignKeyField, IntegerField, TextField

from .. import constants, helpers
from ... import adt, events, getinput
from ..constants import (
    EmendationJustification,
    Group,
    NomenclatureStatus,
    Rank,
    SpeciesNameKind,
    Status,
    AgeClass,
)
from ..definition import Definition

from .base import BaseModel, EnumField, ADTField, get_completer, get_str_completer
from .article import Article
from .citation_group import CitationGroup
from .collection import Collection
from .taxon import Taxon, display_organized
from .location import Location
from .name_complex import NameComplex, SpeciesNameComplex
from .person import Person, AuthorTag


class Name(BaseModel):
    creation_event = events.Event["Name"]()
    save_event = events.Event["Name"]()
    label_field = "corrected_original_name"
    grouping_field = "status"
    call_sign = "N"
    field_defaults = {
        "species_type_kind": constants.SpeciesGroupType.holotype,
        "nomenclature_status": NomenclatureStatus.available,
        "status": Status.valid,
    }
    excluded_fields = {"data"}

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

    # Citation and authority
    authority = CharField(null=True)
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
    stem = CharField(null=True)  # redundant with name complex?
    gender = EnumField(
        constants.GrammaticalGender, null=True
    )  # for genus group; redundant with name complex
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
    type_specimen_source = ForeignKeyField(
        Article,
        null=True,
        db_column="type_specimen_source_id",
        related_name="type_source_names",
    )
    genus_type_kind = EnumField(constants.TypeSpeciesDesignation, null=True)
    species_type_kind = EnumField(constants.SpeciesGroupType, null=True)
    type_tags = ADTField(lambda: TypeTag, null=True)

    # Miscellaneous data
    data = TextField(null=True)
    _definition = CharField(null=True, db_column="definition")
    tags = ADTField(lambda: NameTag, null=True)

    class Meta(object):
        db_table = "name"

    @classmethod
    def with_tag_of_type(cls, tag_cls: Type[adt.ADT]) -> List["Name"]:
        names = cls.select_valid().filter(
            Name._raw_type_tags.contains(f"[{tag_cls._tag}, ")
        )
        return [
            name
            for name in names
            if any(isinstance(tag, tag_cls) for tag in name.type_tags)
        ]

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(Name.status != Status.removed)

    def should_skip(self) -> bool:
        return self.status is Status.removed

    def get_stem(self) -> Optional[str]:
        if self.group != Group.genus or self.name_complex is None:
            return None
        return self.name_complex.get_stem_from_name(self.root_name)

    @property
    def definition(self) -> Optional[Definition]:
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

    def infer_corrected_original_name(self, aggressive: bool = False) -> Optional[str]:
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
                r"^(?P<genus>[A-Z][a-z]+)( \([A-Z][a-z]+\))? (?P<species>[A-Z]?[a-z]+)((,? var\.)? (?P<subspecies>[A-Z]?[a-z]+))?$",
                original_name,
            )
            if match:
                name = f'{match.group("genus")} {match.group("species").lower()}'
                if match.group("subspecies"):
                    name += " " + match.group("subspecies").lower()
                if self.root_name == name.split(" ")[-1]:
                    return name
        return None

    def get_value_for_field(self, field: str, default: Optional[str] = None) -> Any:
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
                    f"inferred corrected_original_name to be {inferred!r} from {self.original_name!r}"
                )
                return inferred
            else:
                if self.corrected_original_name is not None:
                    default = self.corrected_original_name
                else:
                    default = self.original_name
                return super().get_value_for_field(field, default=default)
        elif field == "type_tags":
            if self.type_locality is not None:
                print(repr(self.type_locality))
            if self.collection is not None:
                print(repr(self.collection))
            return super().get_value_for_field(field, default=default)
        elif field == "type_specimen_source":
            return self.get_value_for_foreign_key_field(
                field, default_obj=None, callbacks=self.get_adt_callbacks()
            )
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
            "preoccupied_by": self.preoccupied_by,
            "display_type_locality": lambda: self.type_locality.display(),
            "fill_required_fields": lambda: self.fill_required_fields(
                skip_fields={"type_tags"}
            ),
        }

    def edit(self) -> None:
        self.fill_field("type_tags")

    def _add_type_identical_callback(self) -> None:
        root_name = self.getter("root_name").get_one_key(
            "root_name> ", allow_empty=False
        )
        assert root_name is not None
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
        page_described: Union[None, int, str] = None,
        locality: Optional[Location] = None,
        **kwargs: Any,
    ) -> "Taxon":
        """Convenience method to add a type species described in the same paper as the genus."""
        assert self.taxon.rank == Rank.genus
        assert self.type is None
        full_name = f"{self.corrected_original_name} {name}"
        if isinstance(page_described, int):
            page_described = str(page_described)
        result = self.add_child_taxon(
            Rank.species,
            full_name,
            authority=self.authority,
            year=self.year,
            original_citation=self.original_citation,
            verbatim_citation=self.verbatim_citation,
            citation_group=self.citation_group,
            original_name=full_name,
            page_described=page_described,
            status=self.status,
        )
        self.type = result.base_name
        self.save()
        if locality is not None:
            result.add_occurrence(locality)
        result.base_name.s(**kwargs)
        if self.original_citation is not None:
            self.fill_required_fields()
            result.base_name.fill_required_fields()
        return result

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:
        for field_name, tag_cls in [("type_tags", TypeTag), ("tags", NameTag), ("author_tags", AuthorTag)]:
            if field == field_name:
                completers: Dict[
                    Tuple[Type[adt.ADT], str], getinput.Completer[Any]
                ] = {}
                for tag in tag_cls._tag_to_member.values():  # type: ignore
                    for attribute, typ in tag._attributes.items():
                        completer: Optional[getinput.Completer[Any]]
                        if typ is Name:
                            completer = get_completer(Name, "corrected_original_name")
                        elif typ is Collection:
                            completer = get_completer(Collection, "label")
                        elif typ is Article:
                            completer = get_completer(Article, "name")
                        elif typ is Person:
                            completer = get_completer(Person, None)
                        elif typ is str and attribute in ("lectotype", "neotype"):
                            completer = get_str_completer(Name, "type_specimen")
                        else:
                            completer = None
                        if completer is not None:
                            completers[(tag, attribute)] = completer
                return completers
        return {}

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
        self.save()

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

    def _load_data(self) -> Dict[str, Any]:
        if self.data is None or self.data == "":
            return {}
        else:
            return json.loads(self.data)

    def get_tag_target(self, tag_cls: Type[adt.ADT]) -> Optional["Name"]:
        if self.tags:
            for tag in self.tags:
                if isinstance(tag, tag_cls):
                    return tag.name
        return None

    def add_tag(self, tag: adt.ADT) -> None:
        if self.tags is None:
            self.tags = [tag]
        else:
            self.tags = self.tags + (tag,)

    def add_type_tag(self, tag: adt.ADT) -> None:
        if self.type_tags is None:
            self.type_tags = [tag]
        else:
            self.type_tags = self.type_tags + (tag,)

    def has_type_tag(self, tag_cls: Type[adt.ADT]) -> bool:
        for _ in self.get_tags(self.type_tags, tag_cls):
            return True
        return False

    def map_type_tags(self, fn: Callable[["TypeTag"], Optional["TypeTag"]]) -> None:
        type_tags = self.type_tags
        if type_tags is None:
            return
        new_tags = []
        for tag in type_tags:
            new_tag = fn(tag)
            if new_tag is not None:
                new_tags.append(new_tag)
        self.type_tags = tuple(new_tags)  # type: ignore

    def map_type_tags_by_type(self, typ: Type[Any], fn: Callable[[Any], Any]) -> None:
        def map_fn(tag: TypeTag) -> TypeTag:
            new_args = []
            tag_type = type(tag)
            for arg_name, arg_type in tag_type._attributes.items():
                val = getattr(tag, arg_name)
                if arg_type is typ:
                    new_args.append(fn(val))
                else:
                    new_args.append(val)
            return tag_type(*new_args)

        self.map_type_tags(map_fn)

    def replace_original_citation(self, new_citation: Optional[Article] = None) -> None:
        if new_citation is None:
            new_citation = Article.get_one_by("name", allow_empty=False)
        existing = self.original_citation

        def map_fn(tag: TypeTag) -> TypeTag:
            if isinstance(tag, TypeTag.LocationDetail) and tag.source == existing:
                return TypeTag.LocationDetail(tag.text, new_citation)
            elif isinstance(tag, TypeTag.SpecimenDetail) and tag.source == existing:
                return TypeTag.SpecimenDetail(tag.text, new_citation)
            else:
                return tag

        self.map_type_tags(map_fn)
        self.original_citation = new_citation
        self.save()

    def add_included(self, species: "Name", comment: str = "") -> None:
        assert isinstance(species, Name)
        self.add_type_tag(TypeTag.IncludedSpecies(species, comment))

    def add_static_comment(
        self,
        kind: constants.CommentKind,
        text: str,
        source: Optional[Article] = None,
        page: Optional[str] = None,
    ) -> "NameComment":
        return NameComment.make(
            name=self, kind=kind, text=text, source=source, page=page
        )

    def add_comment(
        self,
        kind: Optional[constants.CommentKind] = None,
        text: Optional[str] = None,
        source: Optional[Article] = None,
        page: Optional[str] = None,
        interactive: bool = True,
    ) -> "NameComment":
        return NameComment.create_interactively(
            name=self, kind=kind, text=text, source=source, page=page
        )

    def add_child_taxon(
        self,
        rank: Rank,
        name: str,
        authority: Optional[str] = None,
        year: Union[None, str, int] = None,
        age: Optional[AgeClass] = None,
        **kwargs: Any,
    ) -> "Taxon":
        if age is None:
            age = self.taxon.age
        taxon = Taxon.create(valid_name=name, age=age, rank=rank, parent=self.taxon)
        kwargs["group"] = helpers.group_of_rank(rank)
        kwargs["root_name"] = helpers.root_name_of_name(name, rank)
        if "status" not in kwargs:
            kwargs["status"] = Status.valid
        name_obj = Name.create(taxon=taxon, **kwargs)
        if authority is not None:
            name_obj.authority = authority
        if year is not None:
            name_obj.year = year
        name_obj.save()
        taxon.base_name = name_obj
        taxon.save()
        return taxon

    def add_nomen_nudum(self) -> "Name":
        """Adds a nomen nudum similar to this name."""
        return self.taxon.add_syn(
            root_name=self.root_name,
            original_name=self.original_name,
            authority=self.authority,
            nomenclature_status=NomenclatureStatus.nomen_nudum,
        )

    def description(self) -> str:
        if self.original_name:
            out = self.original_name
        else:
            out = self.root_name
        if self.authority:
            out += " %s" % self.authority
        if self.year:
            out += f", {self.year}"
        if self.page_described:
            out += f":{self.page_described}"
        parenthesized_bits = []
        if self.taxon.valid_name != self.original_name:
            parenthesized_bits.append(f"= {self.taxon.valid_name}")
        if self.nomenclature_status != NomenclatureStatus.available:
            parenthesized_bits.append(self.nomenclature_status.name)
        if self.status != Status.valid:
            parenthesized_bits.append(self.status.name)
        if parenthesized_bits:
            out += f" ({', '.join(parenthesized_bits)})"
        return out

    def is_unavailable(self) -> bool:
        return not self.nomenclature_status.can_preoccupy()

    def numeric_page_described(self) -> int:
        return helpers.to_int(self.page_described)

    def extract_page_described(self) -> Optional[int]:
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

    def numeric_year(self) -> int:
        if self.year is None:
            return 0
        elif "-" in self.year:
            try:
                return int(self.year.split("-")[-1])
            except ValueError:
                return 0
        else:
            try:
                return int(self.year)
            except ValueError:
                return 0

    def sort_key(self) -> Tuple[Any, ...]:
        return (
            self.numeric_year(),
            self.numeric_page_described(),
            self.corrected_original_name or "",
            self.root_name,
        )

    def make_variant(
        self,
        status: Optional[NomenclatureStatus] = None,
        of_name: Optional["Name"] = None,
        comment: Optional[str] = None,
    ) -> None:
        if self.nomenclature_status != NomenclatureStatus.available:
            raise ValueError(f"{self} is {self.nomenclature_status.name}")
        if status is None:
            status = getinput.get_enum_member(
                NomenclatureStatus, prompt="nomenclature_status> ", allow_empty=False
            )
        if of_name is None:
            of_name = Name.getter("corrected_original_name").get_one(
                prompt="of_name> ", allow_empty=False
            )
        if of_name is None:
            raise ValueError("of_name is None")
        self.add_tag(STATUS_TO_TAG[status](name=of_name, comment=comment))
        self.nomenclature_status = status  # type: ignore
        self.save()

    def add_variant(
        self,
        root_name: Optional[str] = None,
        status: Optional[NomenclatureStatus] = None,
        paper: Optional[str] = None,
        page_described: Optional[str] = None,
        original_name: Optional[str] = None,
        *,
        interactive: bool = True,
    ) -> "Name":
        if root_name is None:
            root_name = Name.getter("root_name").get_one_key(
                prompt="root_name> ", allow_empty=False
            )
        if root_name is None:
            raise ValueError("root_name is None")
        if status is None:
            status = getinput.get_enum_member(
                NomenclatureStatus, prompt="nomenclature_status> ", allow_empty=False
            )

        if paper is not None:
            nam = self.taxon.syn_from_paper(root_name, paper, interactive=False)
            nam.original_name = original_name
            nam.nomenclature_status = status
        else:
            nam = self.taxon.add_syn(
                root_name,
                nomenclature_status=status,
                original_name=original_name,
                interactive=False,
            )
        tag_cls = STATUS_TO_TAG[status]
        nam.page_described = page_described
        nam.add_tag(tag_cls(self, ""))
        if interactive:
            nam.fill_required_fields()
        return nam

    def preoccupied_by(
        self, name: Optional["Name"] = None, comment: Optional[str] = None
    ) -> None:
        if name is None:
            name = Name.getter("corrected_original_name").get_one(
                prompt="name> ", allow_empty=False
            )
        if name is None:
            raise ValueError("name is None")
        self.add_tag(NameTag.PreoccupiedBy(name, comment))
        if self.nomenclature_status == NomenclatureStatus.available:
            self.nomenclature_status = NomenclatureStatus.preoccupied  # type: ignore
        else:
            print(f"not changing status because it is {self.nomenclature_status}")
        self.save()

    def conserve(self, opinion: str, comment: Optional[str] = None) -> None:
        self.add_tag(NameTag.Conserved(opinion, comment))

    @classmethod
    def infer_author_tags_for_all(
        cls, dry_run: bool = True, limit: Optional[int] = None
    ) -> None:
        nams = (
            cls.select_valid()
            .filter(cls.authority != None, cls.author_tags == None)
            .limit(limit)
        )
        for nam in nams:
            nam.display(full=False)
            nam.infer_author_tags(dry_run=dry_run)

    def infer_author_tags(self, dry_run: bool = True) -> None:
        if self.authority is None or self.author_tags is not None:
            return
        if (
            self.original_citation is not None
            and self.original_citation.author_tags is not None
            and self.original_citation.taxonomicAuthority()[0] == self.authority
        ):
            author_tags = self.original_citation.author_tags
            print(f"Inferred: {self.authority!r} -> {author_tags}")
            if not dry_run:
                self.author_tags = author_tags
        else:
            if "et al." in self.authority:
                params_by_name = [None]
            elif self.authority == "H.E. Wood, 2nd":
                params_by_name = {
                    "family_name": "Wood",
                    "given_names": "Horace Elmer",
                    "suffix": "2nd",
                }
            else:
                authors = self.get_authors()
                params_by_name = [self._author_to_person(author) for author in authors]
            if all(params_by_name):
                print(f"Authors: {self.authority!r} -> {params_by_name}")
                if not dry_run:
                    tags = [
                        AuthorTag.Author(
                            person=Person.get_or_create_unchecked(**params)
                        )
                        for params in params_by_name
                    ]
                    self.author_tags = tags
            else:
                print("Failed to match", self.authority)

    def _author_to_person(self, author: str) -> Dict[str, Optional[str]]:
        match = re.match(
            r"^((?P<initials>([A-ZÉ]\.)+) )?((?P<tussenvoegsel>de|von|van|van der|van den|van de) )?(?P<family_name>(d'|de|de la |zur |du |dos |del |di |ul-|von der |da |vander|dal |delle |ul )?[ÄÉÜÁÖŞA-Z].*)(, (?P<suffix>2nd))?$",
            author,
        )
        if match is not None:
            return match.groupdict()
        return None

    def author_set(self) -> Set[str]:
        return {
            author.rsplit(". ", 1)[-1].split(", ", 1)[0]
            for author in self.get_authors()
        }

    def get_authors(self) -> List[str]:
        return re.split(r", | & ", re.sub(r"et al\.$", "", self.authority))

    def set_authors(self, authors: List[str]) -> None:
        self.authority = helpers.unsplit_authors(authors)

    def effective_year(self) -> int:
        """Returns the effective year of validity for this name.

        Defaults to the year after the current year if the year is unknown or invalid.

        """
        if self.year is None:
            return datetime.datetime.now().year + 1
        if self.year == "in press":
            return datetime.datetime.now().year
        else:
            year_str = self.year[-4:]
            try:
                return int(year_str)
            except ValueError:
                # invalid year
                return datetime.datetime.now().year + 1

    def get_description(
        self,
        full: bool = False,
        depth: int = 0,
        include_data: bool = False,
        include_taxon: bool = False,
    ) -> str:
        if self.original_name is None:
            out = self.root_name
        else:
            out = self.original_name
        if self.authority is not None:
            out += " %s" % self.authority
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
        if full and (
            self.original_name is not None
            or self.stem is not None
            or self.gender is not None
            or self.definition is not None
        ):
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
            else:
                if self.stem is not None:
                    parts.append("stem: %s" % self.stem)
                if self.gender is not None:
                    parts.append(constants.GrammaticalGender(self.gender).name)
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
            data: Dict[str, Any] = {}
            if self.type_locality is not None:
                data["locality"] = repr(self.type_locality)
            type_info = []
            if self.species_type_kind is not None:
                type_info.append(self.species_type_kind.name)
            if self.type_specimen is not None:
                type_info.append(self.type_specimen)
            if self.collection is not None:
                type_info.append(f"in {self.collection!r}")
            if self.type_specimen_source is not None:
                type_info.append(f"{{{self.type_specimen_source.name}}}")
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

    def get_required_fields(self) -> Iterable[str]:
        if (
            self.status is Status.spurious
            or self.nomenclature_status is NomenclatureStatus.informal
        ):
            return
        yield "original_name"
        if self.original_name is not None:
            yield "corrected_original_name"

        yield "authority"
        yield "year"
        if self.nomenclature_status != NomenclatureStatus.as_emended:
            yield "page_described"
        yield "original_citation"
        if self.original_citation is None:
            yield "verbatim_citation"
            if self.verbatim_citation is not None:
                yield "citation_group"

        if self.nomenclature_status.requires_type() and self.group is Group.species:
            # Yield this early because it's often easier to first get all the *Detail
            # tags and then fill in the required fields.
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
                if self.genus_type_kind is None:
                    if self.original_citation is not None:
                        yield "type_tags"
                elif self.genus_type_kind.requires_tag():
                    yield "type_tags"

    def get_deprecated_fields(self) -> Iterable[str]:
        yield "type_specimen_source"
        yield "stem"
        yield "gender"

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
        parent: Optional[Taxon] = None,
        rank: Optional[Rank] = None,
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
        new_taxon = Taxon.create(
            rank=rank, parent=parent, age=old_taxon.age, valid_name=""
        )
        new_taxon.base_name = self
        new_taxon.valid_name = new_taxon.compute_valid_name()
        new_taxon.save()
        self.taxon = new_taxon
        self.status = status  # type: ignore
        self.save()
        return new_taxon

    def merge(self, into: "Name", allow_valid: bool = False) -> None:
        if not allow_valid:
            assert self.status in (
                Status.synonym,
                Status.dubious,
            ), f"Can only merge synonymous names (not {self})"
        if self.type_tags and into.type_tags:
            into.type_tags += self.type_tags
        self._merge_fields(into, exclude={"id"})
        self.remove(reason=f"Removed because it was merged into {into} (N#{into.id})")

    def open_description(self) -> bool:
        if self.original_citation is None:
            print("%s: original citation unknown" % self.description())
        else:
            self.original_citation.openf()
        return True

    def remove(self, reason: Optional[str] = None) -> None:
        print("Deleting name: " + self.description())
        self.status = Status.removed  # type: ignore
        self.save()
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
                self.save()
            return False
        return True

    def __str__(self) -> str:
        return self.description()

    def __repr__(self) -> str:
        return self.description()

    def set_paper(
        self,
        paper: Optional[Article] = None,
        page_described: Union[None, int, str] = None,
        original_name: Optional[int] = None,
        force: bool = False,
        **kwargs: Any,
    ) -> None:
        if paper is None:
            paper = self.get_value_for_foreign_class(
                "original_citation", Article, allow_none=False
            )
        authority, year = paper.taxonomicAuthority()
        if original_name is None and self.status == Status.valid:
            original_name = self.taxon.valid_name
        attributes = [
            ("authority", authority),
            ("year", year),
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
        self.save()

    def detect_and_set_type(
        self, verbatim_type: Optional[str] = None, verbose: bool = False
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
            self.save()
            return True
        else:
            print(
                "Verbatim type %s for name %s yielded multiple possible names: %s"
                % (verbatim_type, self, candidates)
            )
            return False

    def detect_type(
        self, verbatim_type: Optional[str] = None, verbose: bool = False
    ) -> List["Name"]:
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
    def _split_authority(verbatim_type: str) -> Tuple[str, Optional[str]]:
        # if there is an uppercase letter following an all-lowercase word (the species name),
        # the authority is included
        find_authority = re.match(r"^(.* [a-z]+) ([A-Z+].+)$", verbatim_type)
        if find_authority:
            return find_authority.group(1), find_authority.group(2)
        else:
            return verbatim_type, None

    def detect_type_from_verbatim_type(self, verbatim_type: str) -> List["Name"]:
        def _filter_by_authority(
            candidates: List["Name"], authority: Optional[str]
        ) -> List["Name"]:
            if authority is None:
                return candidates
            split = re.split(r", (?=\d)", authority, maxsplit=1)
            if len(split) == 1:
                author, year = authority, None
            else:
                author, year = split
            result = []
            for candidate in candidates:
                if candidate.authority != author:
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
        similar_names = list(
            self.select_valid().filter(
                Name.authority == self.authority,
                Name.year == self.year,
                Name.id != self.id,
                Name.citation_group != self.citation_group,
            )
        )
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

    @classmethod
    def find_name(
        cls,
        name: str,
        rank: Optional[Rank] = None,
        authority: Optional[str] = None,
        year: Union[None, int, str] = None,
    ) -> "Name":
        """Find a Name object corresponding to the given information."""
        if rank is None:
            group = None
            initial_lst = cls.select().where(cls.root_name == name)
        else:
            group = helpers.group_of_rank(rank)
            if group == Group.family:
                root_name = helpers.strip_rank(name, rank, quiet=True)
            else:
                root_name = name
            initial_lst = cls.select().where(
                cls.root_name == root_name, cls.group == group
            )
        for nm in initial_lst:
            if authority is not None and nm.authority and nm.authority != authority:
                continue
            if year is not None and nm.year and nm.year != year:
                continue
            if group == Group.family:
                if (
                    nm.original_name
                    and nm.original_name != name
                    and initial_lst.count() > 1
                ):
                    continue
            return nm
        raise cls.DoesNotExist


class NameComment(BaseModel):
    call_sign = "NCO"
    grouping_field = "kind"

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

    def should_skip(self) -> bool:
        return self.kind in (
            constants.CommentKind.removed,
            constants.CommentKind.structured_quote,
        )

    @classmethod
    def make(
        cls,
        name: Name,
        kind: constants.CommentKind,
        text: str,
        source: Optional[Article] = None,
        page: Optional[str] = None,
    ) -> "NameComment":
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
        name: Optional[Name] = None,
        kind: Optional[constants.CommentKind] = None,
        text: Optional[str] = None,
        source: Optional[Article] = None,
        page: Optional[str] = None,
        **kwargs: Any,
    ) -> "NameComment":
        if name is None:
            name = cls.get_value_for_foreign_key_field_on_class(
                "name", allow_none=False
            )
        assert name is not None
        if kind is None:
            kind = getinput.get_enum_member(
                constants.CommentKind, prompt="kind> ", allow_empty=False
            )
        if text is None:
            text = getinput.get_line(prompt="text> ")
        assert text is not None
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
    if not nam.original_citation or not nam.type_tags:
        return False
    for tag in nam.type_tags:
        if isinstance(tag, SOURCE_TAGS) and tag.source == nam.original_citation:
            return True
        if isinstance(tag, (TypeTag.IncludedSpecies, TypeTag.GenusCoelebs)):
            return True
    return False


def write_type_localities(
    type_locs: Sequence[Name],
    *,
    depth: int = 0,
    full: bool = False,
    organized: bool = False,
    file: IO[str] = sys.stdout,
) -> None:
    if not type_locs:
        return

    def write_type_loc(nam: Name) -> str:
        lines = [f"{nam}\n"]
        if full and nam.type_tags:
            for tag in nam.type_tags:
                if isinstance(tag, TypeTag.LocationDetail):
                    lines.append(f"    {tag}\n")
        return "".join(lines)

    if organized:
        display_organized(
            [(write_type_loc(nam), nam.taxon) for nam in type_locs], depth=depth
        )
    else:
        for nam in type_locs:
            file.write(getinput.indent(write_type_loc(nam), depth + 8))


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
    # See discussion in docs/name.rst
    JustifiedEmendationOf(  # type: ignore
        name=Name, justification=EmendationJustification, comment=str, tag=19
    )


STATUS_TO_TAG = {
    NomenclatureStatus.unjustified_emendation: NameTag.UnjustifiedEmendationOf,
    NomenclatureStatus.justified_emendation: NameTag.JustifiedEmendationOf,
    NomenclatureStatus.incorrect_subsequent_spelling: NameTag.IncorrectSubsequentSpellingOf,
    NomenclatureStatus.variant: NameTag.VariantOf,
    NomenclatureStatus.mandatory_change: NameTag.MandatoryChangeOf,
    NomenclatureStatus.nomen_novum: NameTag.NomenNovumFor,
    NomenclatureStatus.incorrect_original_spelling: NameTag.IncorrectOriginalSpellingOf,
    NomenclatureStatus.subsequent_usage: NameTag.SubsequentUsageOf,
    NomenclatureStatus.preoccupied: NameTag.PreoccupiedBy,
}


class TypeTag(adt.ADT):
    Collector(name=str, tag=1)  # type: ignore
    Date(date=str, tag=2)  # type: ignore
    Gender(gender=constants.SpecimenGender, tag=3)  # type: ignore
    Age(age=constants.SpecimenAge, tag=4)  # type: ignore
    Organ(
        organ=constants.SpecimenOrgan, detail=str, condition=str, tag=5
    )  # type: ignore
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


SOURCE_TAGS = (
    TypeTag.LocationDetail,
    TypeTag.SpecimenDetail,
    TypeTag.CitationDetail,
    TypeTag.EtymologyDetail,
    TypeTag.CollectionDetail,
    TypeTag.DefinitionDetail,
    TypeTag.TypeSpeciesDetail,
)
