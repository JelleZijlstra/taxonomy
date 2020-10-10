import sys
from typing import IO, Any, Dict, Optional, Sequence, Tuple, Type

from peewee import CharField, DeferredForeignKey, TextField

from .collection import Collection
from .region import Region
from ..constants import NamingConvention, PersonType
from .. import models
from ... import adt, events, getinput

from .base import (
    BaseModel,
    EnumField,
    ADTField,
    get_completer,
    get_tag_based_derived_field,
)


class Person(BaseModel):
    creation_event = events.Event["Person"]()
    save_event = events.Event["Person"]()
    label_field = "family_name"
    call_sign = "H"  # for human, P is taken for Period

    family_name = CharField()
    given_names = CharField(null=True)
    initials = CharField(null=True)
    suffix = CharField(null=True)
    tussenvoegsel = CharField(null=True)
    birth = CharField(null=True)
    death = CharField(null=True)
    tags = ADTField(lambda: models.tags.PersonTag, null=True)
    naming_convention = EnumField(NamingConvention)
    type = EnumField(PersonType)
    target = DeferredForeignKey("Person", null=True)
    bio = TextField(null=True)

    derived_fields = [
        get_tag_based_derived_field(
            "patronyms",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.NamedAfter,
            1,
        ),
        get_tag_based_derived_field(
            "collected",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.CollectedBy,
            1,
        ),
        get_tag_based_derived_field(
            "articles",
            lambda: models.Article,
            "author_tags",
            lambda: AuthorTag.Author,
            1,
            skip_filter=True,
        ),
        get_tag_based_derived_field(
            "names",
            lambda: models.Name,
            "author_tags",
            lambda: AuthorTag.Author,
            1,
            skip_filter=True,
        ),
    ]

    def __str__(self) -> str:
        return self.get_description()

    def get_description(self, family_first: bool = False, url: bool = False) -> str:
        parts = [self.get_full_name(family_first)]
        parens = []
        if self.birth or self.death:
            parens.append(f"{_display_year(self.birth)}–{_display_year(self.death)}")
        if self.bio is not None:
            parens.append(self.bio)
        parens.append(self.type.name)
        parens.append(self.naming_convention.name)
        if url:
            parens.append(self.get_url())
        parts.append(f" ({'; '.join(parens)})")
        return "".join(parts)

    def get_full_name(self, family_first: bool = False) -> str:
        parts = []
        if family_first:
            parts.append(self.family_name)
            if self.given_names or self.initials or self.tussenvoegsel:
                parts.append(", ")
                if self.given_names:
                    parts.append(self.given_names)
                elif self.initials:
                    parts.append(self.initials)
                if self.tussenvoegsel and (self.given_names or self.initials):
                    parts.append(" ")
                if self.tussenvoegsel:
                    parts.append(self.tussenvoegsel)
        else:
            if self.given_names:
                parts.append(self.given_names)
            elif self.initials:
                parts.append(self.initials)
            if self.given_names or self.initials:
                parts.append(" ")
            if self.tussenvoegsel:
                parts.append(self.tussenvoegsel + " ")
            parts.append(self.family_name)
        if self.suffix:
            if self.naming_convention is NamingConvention.ancient:
                parts.append(" " + self.suffix)
            else:
                parts.append(", " + self.suffix)
        return "".join(parts)

    def get_initials(self) -> Optional[str]:
        if self.initials:
            return self.initials
        if not self.given_names:
            return None
        names = self.given_names.split(" ")
        return "".join(
            name[0] + "." if name[0].isupper() else f" {name} " for name in names
        )

    @classmethod
    def join_authors(cls, authors: Sequence["Person"]) -> str:
        if len(authors) <= 2:
            return " & ".join(author.taxonomic_authority() for author in authors)
        return (
            ", ".join(author.taxonomic_authority() for author in authors[:-1])
            + " & "
            + authors[-1].taxonomic_authority()
        )

    def taxonomic_authority(self) -> str:
        if self.tussenvoegsel is not None and self.naming_convention is NamingConvention.dutch:
            return f"{self.tussenvoegsel[0].upper()}{self.tussenvoegsel[1:]} {self.family_name}"
        else:
            return self.family_name

    def get_value_to_show_for_field(self, field: Optional[str]) -> str:
        if field is None:
            return self.get_description(family_first=True, url=True)
        return getattr(self, field)

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(Person.type != PersonType.deleted)

    def should_skip(self) -> bool:
        return self.type is not PersonType.deleted

    @classmethod
    def create_interactively(
        cls, family_name: Optional[str] = None, **kwargs: Any
    ) -> "Person":
        if family_name is None:
            family_name = cls.getter("family_name").get_one_key("family_name> ")
        assert family_name is not None
        kwargs.setdefault("type", PersonType.unchecked)
        kwargs.setdefault("naming_convention", NamingConvention.western)
        result = cls.create(family_name=family_name, **kwargs)
        result.fill_field("tags")
        return result

    def display(
        self, full: bool = True, depth: int = 0, file: IO[str] = sys.stdout
    ) -> None:
        onset = " " * depth
        indented_onset = onset + " " * 4
        double_indented_onset = onset + " " * 8
        file.write(onset + str(self) + "\n")
        if self.tags:
            for tag in self.tags:
                file.write(indented_onset + repr(tag) + "\n")
        if full:
            for field in self.derived_fields:
                refs = self.get_derived_field(field.name)
                if refs is not None:
                    file.write(f"{indented_onset}{field.name.title()} ({len(refs)})\n")
                    for nam in sorted(refs, key=lambda nam: nam.sort_key()):
                        file.write(f"{double_indented_onset}{nam}\n")

    def find_tag(
        self, tags: Optional[Sequence[adt.ADT]], tag_cls: Type[adt.ADT]
    ) -> Optional[adt.ADT]:
        if tags is None:
            return None
        for tag in tags:
            if isinstance(tag, tag_cls) and tag.person == self:
                return tag
        return None

    def add_to_derived_field(self, field_name: str, obj: BaseModel) -> None:
        current = self.get_raw_derived_field(field_name) or []
        self.set_derived_field(field_name, [*current, obj.id])

    def remove_from_derived_field(self, field_name: str, obj: BaseModel) -> None:
        current = self.get_raw_derived_field(field_name) or []
        self.set_derived_field(field_name, [o for o in current if o != obj.id])

    def edit_tag_sequence(
        self,
        obj: BaseModel,
        tags: Optional[Sequence[adt.ADT]],
        tag_cls: Type[adt.ADT],
        target: Optional["Person"] = None,
    ) -> Tuple[Optional[Sequence[adt.ADT]], Optional["Person"]]:
        if tags is None:
            return None, None
        matching_tag = self.find_tag(tags, tag_cls)
        if matching_tag is None:
            return None, None
        print(matching_tag)
        if target is None:
            new_person = self.getter(None).get_one(callbacks=obj.get_adt_callbacks())
            if new_person is None:
                return None, None
        else:
            new_person = target
        new_tag = tag_cls(person=new_person)
        return [new_tag if tag == matching_tag else tag for tag in tags], new_person

    def edit_tag_sequence_on_object(
        self,
        obj: BaseModel,
        field_name: str,
        tag_cls: Type[adt.ADT],
        derived_field_name: str,
        target: Optional["Person"] = None,
    ) -> None:
        tags = getattr(obj, field_name)
        tag = self.find_tag(tags, tag_cls)
        if tag is None:
            return
        obj.display()
        new_tags, new_person = self.edit_tag_sequence(obj, tags, tag_cls, target)
        if new_tags is not None:
            setattr(obj, field_name, new_tags)
            obj.save()
            if new_person is not None:
                self.remove_from_derived_field(derived_field_name, obj)
                new_person.add_to_derived_field(derived_field_name, obj)

    def edit(self) -> None:
        self.fill_field("tags")

    def num_references(self) -> Dict[str, int]:
        num_refs = {}
        for field in self.derived_fields:
            refs = self.get_raw_derived_field(field.name)
            if refs is not None:
                num_refs[field.name] = len(refs)
        return num_refs

    def total_references(self) -> int:
        return sum(self.num_references().values())

    def reassign_references(self, target: Optional["Person"] = None) -> None:
        for field_name, tag_name, tag_cls in [
            ("patronyms", "type_tags", models.TypeTag.NamedAfter),
            ("collected", "type_tags", models.TypeTag.CollectedBy),
            ("names", "author_tags", AuthorTag.Author),
            ("articles", "author_tags", AuthorTag.Author),
        ]:
            objs = self.get_derived_field(field_name)
            if objs:
                for obj in objs:
                    self.edit_tag_sequence_on_object(
                        obj, tag_name, tag_cls, field_name, target=target
                    )

    def make_soft_redirect(self, target: "Person") -> None:
        self.type = PersonType.soft_redirect  # type: ignore
        self.target = target
        self.reassign_references(target=target)

    def maybe_autodelete(self, dry_run: bool = True) -> None:
        if self.type is not PersonType.unchecked:
            return
        num_refs = sum(self.num_references().values())
        if num_refs > 0:
            return
        print(f"Autodeleting {self!r}")
        if not dry_run:
            self.type = PersonType.deleted  # type: ignore
            self.save()

    @classmethod
    def autodelete(cls, dry_run: bool = True) -> None:
        cls.compute_all_derived_fields()
        for person in cls.select_valid().filter(cls.type == PersonType.unchecked):
            person.maybe_autodelete(dry_run=dry_run)

    @classmethod
    def get_or_create_unchecked(
        cls,
        family_name: str,
        *,
        initials: Optional[str] = None,
        given_names: Optional[str] = None,
        suffix: Optional[str] = None,
        tussenvoegsel: Optional[str] = None,
    ) -> None:
        objs = list(
            Person.select_valid().filter(
                Person.family_name == family_name,
                Person.given_names == given_names,
                Person.initials == initials,
                Person.suffix == suffix,
                Person.tussenvoegsel == tussenvoegsel,
                Person.type
                << (
                    PersonType.unchecked,
                    PersonType.soft_redirect,
                    PersonType.hard_redirect,
                ),
            )
        )
        if objs:
            return objs[0]  # should only be one

        objs = list(
            Person.select().filter(
                Person.family_name == family_name,
                Person.given_names == given_names,
                Person.initials == initials,
                Person.suffix == suffix,
                Person.tussenvoegsel == tussenvoegsel,
                Person.type == PersonType.deleted,
            )
        )
        if objs:
            obj = objs[0]
            obj.type = PersonType.unchecked
            print(f"Resurrected {obj}")
            return obj

        else:
            obj = cls.create(
                family_name=family_name,
                given_names=given_names,
                initials=initials,
                suffix=suffix,
                tussenvoegsel=tussenvoegsel,
                type=PersonType.unchecked,
                naming_convention=NamingConvention.western,
            )
            print(f"Created {obj}")
            return obj

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:
        for field_name, tag_cls in [("tags", models.tags.PersonTag)]:
            if field == field_name:
                completers: Dict[
                    Tuple[Type[adt.ADT], str], getinput.Completer[Any]
                ] = {}
                for tag in tag_cls._tag_to_member.values():
                    for attribute, typ in tag._attributes.items():
                        completer: Optional[getinput.Completer[Any]]
                        if typ is Collection:
                            completer = get_completer(Collection, None)
                        elif typ is Region:
                            completer = get_completer(Region, None)
                        elif typ is models.Article:
                            completer = get_completer(models.Article, None)
                        else:
                            completer = None
                        if completer is not None:
                            completers[(tag, attribute)] = completer
                return completers
        return {}


# Reused in Article and Name
class AuthorTag(adt.ADT):
    Author(person=Person, tag=2)  # type: ignore


def _display_year(year: Optional[str]) -> str:
    if year is None:
        return ""
    try:
        if int(year) < 0:
            return f"{-int(year)} BC"
    except ValueError:
        pass
    return year
