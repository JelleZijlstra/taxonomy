import datetime
import json
import re
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from peewee import CharField, ForeignKeyField, IntegerField, Model, TextField

from .. import constants, ehphp, helpers
from ... import adt, events, getinput
from ..constants import Group, NomenclatureStatus, Rank, SpeciesNameKind, Status
from ..definition import Definition

from .base import BaseModel, EnumField, ADTField
from .article import Article
from .collection import Collection
from .taxon import Taxon
from .location import Location
from .name_complex import NameComplex, SpeciesNameComplex

ModelT = TypeVar("ModelT", bound="BaseModel")


class Name(BaseModel):
    creation_event = events.Event["Name"]()
    save_event = events.Event["Name"]()
    label_field = "corrected_original_name"
    call_sign = "N"
    field_defaults = {
        "genus_type_kind": constants.TypeSpeciesDesignation.original_designation,
        "species_type_kind": constants.SpeciesGroupType.holotype,
        "nomenclature_status": NomenclatureStatus.available,
        "status": Status.valid,
    }

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
    original_citation = ForeignKeyField(
        Article, null=True, db_column="original_citation_id", related_name="new_names"
    )
    page_described = CharField(null=True)
    verbatim_citation = CharField(null=True)
    year = CharField(null=True)  # redundant with data for the publication itself

    # Gender and stem
    stem = CharField(null=True)  # redundant with name complex?
    gender = EnumField(constants.Gender)  # for genus group; redundant with name complex
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
    type_locality_description = TextField(null=True)
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
    nomenclature_comments = TextField(null=True)
    other_comments = TextField(null=True)  # deprecated
    taxonomy_comments = TextField(null=True)
    _definition = CharField(null=True, db_column="definition")
    tags = ADTField(lambda: Tag, null=True)

    class Meta(object):
        db_table = "name"

    @classmethod
    def select_valid(cls, *args: Any) -> Any:
        return cls.select(*args).filter(Name.status != Status.removed)

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

    def infer_corrected_original_name(self) -> Optional[str]:
        if not self.original_name or self.group not in (Group.genus, Group.species):
            return None
        original_name = (
            self.original_name.replace("(?)", "")
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
        original_name = re.sub(r"([a-z]{2})-([a-z]{2})", r"\1\2", original_name)
        if self.group == Group.genus:
            if re.match(r"^[A-Z][a-z]+$", original_name):
                return original_name
            match = re.match(r"^[A-Z][a-z]+ \(([A-Z][a-z]+)\)$", original_name)
            if match:
                return match.group(1)
        elif self.group == Group.species:
            if re.match(r"^[A-Z][a-z]+( [a-z]+){1,2}$", original_name):
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
                return name
        return None

    def get_value_for_field(self, field: str) -> Any:
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
            return super().get_value_for_field(field)
        elif field == "original_name":
            if self.original_name is None and self.group in (Group.genus, Group.high):
                return self.root_name
            else:
                return super().get_value_for_field(field)
        elif field == "corrected_original_name":
            inferred = self.infer_corrected_original_name()
            if inferred is not None:
                print(
                    f"inferred corrected_original_name to be {inferred!r} from {self.original_name!r}"
                )
                return inferred
            else:
                return super().get_value_for_field(field)
        elif field == "type_tags":
            if self.type_locality_description is not None:
                print(self.type_locality_description)
            if self.type_locality is not None:
                print(self.type_locality)
            return super().get_value_for_field(field)
        elif field == "type_specimen_source":
            return self.get_value_for_foreign_key_field(
                field,
                default=self.original_citation
                if self.type_specimen_source is None
                else None,
            )
        elif field == "type":
            typ = self.get_value_for_foreign_key_field("type")
            print(f"type: {typ}")
            if typ is None:
                return None
            elif getinput.yes_no("Is this correct? "):
                return typ
            else:
                raise EOFError
        elif field == "species_name_complex":
            value = super().get_value_for_field(field)
            if value is not None and value.kind.is_single_complex():
                value.apply_to_ending(self.root_name, interactive=True)
            return value
        else:
            return super().get_value_for_field(field)

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = super().get_adt_callbacks()
        return {**callbacks, "add_comment": self.add_comment}

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:
        for field_name, tag_cls in [("type_tags", TypeTag), ("tags", Tag)]:
            if field == field_name:
                completers: Dict[
                    Tuple[Type[adt.ADT], str], getinput.Completer[Any]
                ] = {}
                for tag in tag_cls._tag_to_member.values():  # type: ignore
                    for attribute, typ in tag._attributes.items():
                        completer: Optional[getinput.Completer[Any]]
                        if typ is Name:
                            completer = get_completer(Name, "original_name")
                        elif typ is Collection:
                            completer = get_completer(Collection, "label")
                        elif typ is Article:
                            completer = get_completer(Article, "name")
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
            and "type_tags" not in fields
            and "type_tags" in self.get_required_fields()
        ):
            # Always make the user edit type_tags if some other field was unfilled.
            yield "type_tags"

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
        for _ in self.get_tag(self.type_tags, tag_cls):
            return True
        return False

    def add_included(self, species: "Name", comment: str = "") -> None:
        assert isinstance(species, Name)
        self.add_type_tag(TypeTag.IncludedSpecies(species, comment))

    def add_comment(
        self,
        kind: Optional[constants.CommentKind] = None,
        text: Optional[str] = None,
        source: Optional[Article] = None,
        page: Optional[str] = None,
    ) -> "NameComment":
        return NameComment.create_interactively(
            name=self, kind=kind, text=text, source=source, page=page
        )

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
            out += ", %s" % self.year
        out += " (= %s)" % self.taxon.valid_name
        return out

    def is_unavailable(self) -> bool:
        return not self.nomenclature_status.can_preoccupy()

    def numeric_page_described(self) -> int:
        if self.page_described is None:
            return 0
        match = re.match(r"^(\d+)", self.page_described)
        if match:
            return int(match.group(1))
        else:
            return 0

    def numeric_year(self) -> int:
        if self.year is None:
            return 0
        elif "-" in self.year:
            return int(self.year.split("-")[-1])
        else:
            return int(self.year)

    def make_variant(
        self, status: NomenclatureStatus, of_name: "Name", comment: Optional[str] = None
    ) -> None:
        if self.nomenclature_status != NomenclatureStatus.available:
            raise ValueError(f"{self} is {self.nomenclature_status.name}")
        self.add_tag(STATUS_TO_TAG[status](name=of_name, comment=comment))
        self.nomenclature_status = status  # type: ignore
        self.save()

    def add_variant(
        self,
        root_name: str,
        status: NomenclatureStatus = NomenclatureStatus.variant,
        paper: Optional[str] = None,
        page_described: Optional[str] = None,
        original_name: Optional[str] = None,
        *,
        interactive: bool = True,
    ) -> "Name":
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

    def preoccupied_by(self, name: "Name", comment: Optional[str] = None) -> None:
        self.add_tag(Tag.PreoccupiedBy(name, comment))
        if self.nomenclature_status == NomenclatureStatus.available:
            self.nomenclature_status = NomenclatureStatus.preoccupied  # type: ignore
        else:
            print(f"not changing status because it is {self.nomenclature_status}")
        self.save()

    def conserve(self, opinion: str, comment: Optional[str] = None) -> None:
        self.add_tag(Tag.Conserved(opinion, comment))

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
                    parts.append(constants.Gender(self.gender).name)
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
            data = {
                "nomenclature_comments": self.nomenclature_comments,
                "other_comments": self.other_comments,
                "taxonomy_comments": self.taxonomy_comments,
                "verbatim_type": self.verbatim_type,
                "verbatim_citation": self.verbatim_citation,
                "type_locality_description": self.type_locality_description,
                "tags": sorted(self.tags) if self.tags else None,
            }
            if include_data:
                data["data"] = self.data
            type_info = []
            if self.species_type_kind is not None:
                type_info.append(self.species_type_kind.name)
            if self.type_specimen is not None:
                type_info.append(self.type_specimen)
            if self.collection is not None:
                type_info.append(f"in {self.collection}")
            if self.type_specimen_source is not None:
                type_info.append(f"{{{self.type_specimen_source.name}}}")
            if self.type_locality is not None:
                type_info.append(f"from {self.type_locality.name}")
            if type_info:
                data["type"] = "; ".join(type_info)
            result = "".join(
                [result]
                + [
                    " " * ((depth + 2) * 4) + f"{key}: {value}\n"
                    for key, value in data.items()
                    if value
                ]
                + [
                    " " * ((depth + 2) * 4) + str(tag) + "\n"
                    for tag in (self.type_tags or [])
                ]
                + [
                    " " * ((depth + 2) * 4) + comment.get_description() + "\n"
                    for comment in self.comments
                    if include_data
                    or comment.kind != constants.CommentKind.structured_quote
                ]
            )
        return result

    def display(self, full: bool = True, include_data: bool = False) -> None:
        print(
            self.get_description(
                full=full, include_data=include_data, include_taxon=True
            )
        )

    def knowledge_level(self, verbose: bool = False) -> int:
        """Returns whether all necessary attributes of the name have been filled in."""
        required_fields = set(self.get_required_fields())
        if "original_citation" in required_fields and self.original_citation is None:
            if verbose:
                print("0 because no original citation")
            return 0
        for field in required_fields:
            if getattr(self, field) is None:
                if verbose:
                    print(f"1 because {field} is missing")
                return 1
        if verbose:
            print("2 because all fields are set")
        return 2

    def get_required_fields(self) -> Iterable[str]:
        if (
            self.status == Status.spurious
            or self.nomenclature_status == NomenclatureStatus.informal
        ):
            return
        yield "original_name"
        if (
            self.group in (Group.genus, Group.species)
            and self.original_name is not None
            and self.nomenclature_status.requires_corrected_original_name()
        ):
            yield "corrected_original_name"

        yield "authority"
        yield "year"
        yield "page_described"
        yield "original_citation"
        if self.original_citation is None:
            yield "verbatim_citation"

        if (
            self.group == Group.genus
            and self.nomenclature_status.requires_name_complex()
        ):
            yield "name_complex"
        if (
            self.group == Group.species
            and self.nomenclature_status.requires_name_complex()
        ):
            yield "species_name_complex"

        if self.nomenclature_status.requires_type():
            if self.group == Group.family:
                yield "type"
            if self.group == Group.species:
                yield "type_locality"
                # 75 (lost) and 381 (untraced) are special Collections that indicate there is no preserved specimen.
                if self.collection is None or (self.collection.id not in (75, 381)):
                    yield "type_specimen"
                yield "collection"
                if self.type_specimen is not None or self.collection is not None:
                    yield "type_specimen_source"
                    yield "species_type_kind"
                yield "type_tags"
            if self.group == Group.genus:
                if (
                    self.genus_type_kind
                    != constants.TypeSpeciesDesignation.undesignated
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
        yield "type_locality_description"
        yield "taxonomy_comments"
        yield "nomenclature_comments"
        yield "other_comments"
        # maybe stem and gender? should add something automated to get rid of those if there's a name complex

    def validate_as_child(self, status: Status = Status.valid) -> Taxon:
        if self.taxon.rank == Rank.species:
            new_rank = Rank.subspecies
        elif self.taxon.rank == Rank.genus:
            new_rank = Rank.subgenus
        elif self.taxon.rank == Rank.tribe:
            new_rank = Rank.subtribe
        elif self.taxon.rank == Rank.subfamily:
            new_rank = Rank.tribe
        elif self.taxon.rank == Rank.family:
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
        self._merge_fields(into, exclude={"id"})
        self.remove(reason=f"Removed because it was merged into {into} (N#{into.id})")

    def open_description(self) -> bool:
        if self.original_citation is None:
            print("%s: original citation unknown" % self.description())
        else:
            try:
                ehphp.call_ehphp("openf", [self.original_citation.name])
            except ehphp.EHPHPError:
                pass
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
            paper = self.get_value_for_foreign_class("original_citation", Article)
        authority, year = ehphp.call_ehphp("taxonomicAuthority", [paper])[0]
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
                verbatim, constants.Gender.masculine
            ),
            lambda verbatim: helpers.convert_gender(
                verbatim, constants.Gender.feminine
            ),
            lambda verbatim: helpers.convert_gender(verbatim, constants.Gender.neuter),
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
            name = cls.get_value_for_foreign_key_field_on_class("name")
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


def has_data_from_original(nam: "Name") -> bool:
    if not nam.original_citation or not nam.type_tags:
        return False
    for tag in nam.type_tags:
        if nam.group == Group.species:
            if (
                isinstance(tag, TypeTag.LocationDetail)
                and tag.source == nam.original_citation
            ):
                return True
        elif nam.group == Group.genus:
            if isinstance(tag, (TypeTag.IncludedSpecies, TypeTag.GenusCoelebs)):
                return True
    return False


def get_completer(
    cls: Type[ModelT], field: str
) -> Callable[[str, Optional[str]], Optional[ModelT]]:
    def completer(prompt: str, default: Any) -> Any:
        if isinstance(default, BaseModel):
            default = str(default.id)
        elif default is None:
            default = ""
        elif not isinstance(default, str):
            raise TypeError(f"default must be str or Model, not {default!r}")
        return cls.getter(field).get_one(prompt, default=default)

    return completer


def get_str_completer(
    cls: Type[Model], field: str
) -> Callable[[str, Optional[str]], Optional[str]]:
    def completer(prompt: str, default: Optional[str]) -> Any:
        return cls.getter(field).get_one_key(prompt, default=default or "")

    return completer


class Tag(adt.ADT):
    PreoccupiedBy(name=Name, comment=str, tag=1)  # type: ignore
    UnjustifiedEmendationOf(name=Name, comment=str, tag=2)  # type: ignore
    JustifiedEmendationOf(name=Name, comment=str, tag=3)  # type: ignore
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


STATUS_TO_TAG = {
    NomenclatureStatus.unjustified_emendation: Tag.UnjustifiedEmendationOf,
    NomenclatureStatus.justified_emendation: Tag.JustifiedEmendationOf,
    NomenclatureStatus.incorrect_subsequent_spelling: Tag.IncorrectSubsequentSpellingOf,
    NomenclatureStatus.variant: Tag.VariantOf,
    NomenclatureStatus.mandatory_change: Tag.MandatoryChangeOf,
    NomenclatureStatus.nomen_novum: Tag.NomenNovumFor,
    NomenclatureStatus.incorrect_original_spelling: Tag.IncorrectOriginalSpellingOf,
    NomenclatureStatus.subsequent_usage: Tag.SubsequentUsageOf,
    NomenclatureStatus.preoccupied: Tag.PreoccupiedBy,
}


class TypeTag(adt.ADT):
    Collector(name=str, tag=1)  # type: ignore
    Date(date=str, tag=2)  # type: ignore
    Gender(gender=constants.SpecimenGender, tag=3)  # type: ignore
    Age(age=constants.SpecimenAge, tag=4)  # type: ignore
    Organ(organ=constants.Organ, detail=str, condition=str, tag=5)  # type: ignore
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
