from __future__ import annotations
import builtins
from collections import defaultdict
import enum
import sys
from typing import IO, Any
from collections.abc import Callable, Iterable, Mapping, Sequence

from peewee import CharField, DeferredForeignKey, TextField

from .collection import Collection
from .region import Region
from ..constants import NamingConvention, PersonType
from .. import models, helpers
from ..openlibrary import get_author
from ... import adt, events, getinput, parsing

from .base import (
    BaseModel,
    EnumField,
    ADTField,
    get_completer,
    get_tag_based_derived_field,
)

ALLOWED_TUSSENVOEGSELS = {
    NamingConvention.dutch: {
        "de",
        "den",
        "van",
        "van den",
        "van der",
        "van de",
        "ten",
        "ter",
        "in den",
        "in 't",
        "'t",
        "von der",
    },
    NamingConvention.german: {"von", "von den", "von der", "zu"},
    # French, Italian, Portuguese
    NamingConvention.western: {"de", "du", "dos", "da", "de la", "del", "do", "von"},
    NamingConvention.spanish: {"de", "del", "de la", "de los", "de las"},
}
ALLOWED_TUSSENVOEGSELS[NamingConvention.unspecified] = set.union(
    *ALLOWED_TUSSENVOEGSELS.values()
) | {"v.d."}
UNCHECKED_TYPES = (
    PersonType.unchecked,
    PersonType.soft_redirect,
    PersonType.hard_redirect,
)


class PersonLevel(enum.IntEnum):
    unused = 1
    family_name_only = 2
    initials_only = 3
    has_given_name = 4
    has_convention = 5
    redirect = 6
    checked = 7


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
    ol_id = CharField(null=True)

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
            "involved",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.Involved,
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
            "books",
            lambda: models.Book,
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
        if self.target:
            parens.append(f"target: {self.target!r}")
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

    def get_initials(self) -> str | None:
        if self.initials:
            return self.initials
        if not self.given_names:
            return None
        names = self.given_names.split(" ")

        def name_to_initial(name: str) -> str:
            if not name:
                return ""
            elif "." in name:
                return name
            elif "-" in name:
                return "-".join(name_to_initial(part) for part in name.split("-"))
            elif name[0].isupper() or self.naming_convention is NamingConvention.pinyin:
                return name[0] + "."
            else:
                return f" {name}"

        return "".join(name_to_initial(name) for name in names)

    @classmethod
    def join_authors(cls, authors: Sequence[Person]) -> str:
        if len(authors) <= 2:
            return " & ".join(author.taxonomic_authority() for author in authors)
        return (
            ", ".join(author.taxonomic_authority() for author in authors[:-1])
            + " & "
            + authors[-1].taxonomic_authority()
        )

    def taxonomic_authority(self) -> str:
        if (
            self.tussenvoegsel is not None
            and self.naming_convention is NamingConvention.dutch
        ):
            return (
                f"{self.tussenvoegsel[0].upper()}{self.tussenvoegsel[1:]} {self.family_name}"
            )
        else:
            return self.family_name

    def get_value_to_show_for_field(self, field: str | None) -> str:
        if field is None:
            return self.get_description(family_first=True, url=True)
        return getattr(self, field)

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(Person.type != PersonType.deleted)

    def get_redirect_target(self) -> Person | None:
        return self.target

    def is_invalid(self) -> bool:
        return self.type in (
            PersonType.deleted,
            PersonType.hard_redirect,
            PersonType.soft_redirect,
        )

    def should_skip(self) -> bool:
        return self.type is not PersonType.deleted

    def display(
        self,
        full: bool = False,
        depth: int = 0,
        file: IO[str] = sys.stdout,
        include_detail: bool = False,
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
                refs = self.get_sorted_derived_field(field.name)
                if refs:
                    file.write(f"{indented_onset}{field.name.title()} ({len(refs)})\n")
                    for nam in refs:
                        if include_detail and isinstance(nam, models.Name):
                            file.write(nam.get_description(full=True, depth=depth + 4))
                        else:
                            file.write(f"{double_indented_onset}{nam!r}\n")

    def find_tag(
        self, tags: Sequence[adt.ADT] | None, tag_cls: builtins.type[adt.ADT]
    ) -> tuple[int, adt.ADT | None]:
        if tags is None:
            return 0, None
        for i, tag in enumerate(tags):
            if (
                isinstance(tag, tag_cls)
                and hasattr(tag, "person")
                and tag.person == self
            ):
                return i, tag
        return 0, None

    def add_to_derived_field(self, field_name: str, obj: BaseModel) -> None:
        current = self.get_raw_derived_field(field_name) or []
        self.set_derived_field(field_name, [*current, obj.id])

    def remove_from_derived_field(self, field_name: str, obj: BaseModel) -> None:
        current = self.get_raw_derived_field(field_name) or []
        self.set_derived_field(field_name, [o for o in current if o != obj.id])

    def edit_tag_sequence(
        self,
        obj: BaseModel,
        tags: Sequence[adt.ADT] | None,
        tag_cls: builtins.type[adt.ADT],
        target: Person | None = None,
    ) -> tuple[Sequence[adt.ADT] | None, Person | None]:
        if tags is None:
            return None, None
        matching_idx, matching_tag = self.find_tag(tags, tag_cls)
        if matching_tag is None:
            return None, None

        if target is None:

            def who() -> None:
                print(f"{matching_tag} at index {matching_idx}")

            who()
            new_person = self.getter(None).get_one(
                callbacks={**obj.get_adt_callbacks(), "who": who}
            )
            if new_person is None:
                return None, None
        else:
            new_person = target
        new_tag = matching_tag.replace(person=new_person)
        return (
            [
                new_tag if i == matching_idx and tag == matching_tag else tag
                for i, tag in enumerate(tags)
            ],
            new_person,
        )

    def edit_tag_sequence_on_object(
        self,
        obj: BaseModel,
        field_name: str,
        tag_cls: builtins.type[adt.ADT],
        derived_field_name: str,
        target: Person | None = None,
    ) -> Person | None:
        tags = getattr(obj, field_name)
        matching_idx, tag = self.find_tag(tags, tag_cls)
        if tag is None:
            return None
        obj.display()
        new_tags, new_person = self.edit_tag_sequence(obj, tags, tag_cls, target)
        if new_tags is not None:
            setattr(obj, field_name, new_tags)
            if new_person is not None:
                self.move_reference(new_person, derived_field_name, obj)
            return new_person
        return None

    def move_reference(
        self, new_person: Person, derived_field_name: str, obj: BaseModel
    ) -> None:
        self.remove_from_derived_field(derived_field_name, obj)
        new_person.add_to_derived_field(derived_field_name, obj)

    def edit(self) -> None:
        self.fill_field("tags")

    def sort_key(self) -> tuple[str, ...]:
        return (
            self.family_name,
            self.get_initials() or "",
            self.given_names or "",
            self.initials or "",
            self.tussenvoegsel or "",
            self.suffix or "",
            self.type.name,
        )

    def lint_invalid(self, autofix: bool = True) -> Iterable[str]:
        if self.type in (PersonType.hard_redirect, PersonType.soft_redirect):
            if not self.target:
                yield f"{self}: redirect has no target"
                if autofix:
                    print(f"{self}: resetting type to unchecked")
                    self.type = PersonType.unchecked  # type: ignore

    def lint(self, autofix: bool = True) -> Iterable[str]:
        for field_name, field_obj in self._meta.fields.items():
            if isinstance(field_obj, CharField):
                value = getattr(self, field_name)
                if value is not None and not helpers.is_clean_string(value):
                    cleaned = helpers.clean_string(value)
                    print(f"{self}: clean {field_name} from {value!r} to {cleaned!r}")
                    if autofix:
                        setattr(self, field_name, cleaned)

        if self.type in (
            PersonType.deleted,
            PersonType.hard_redirect,
            PersonType.soft_redirect,
        ):
            if self.total_references() > 0:
                yield f"{self}: deleted person has references"
            return
        if (
            self.type is PersonType.checked
            and self.naming_convention is NamingConvention.unspecified
        ):
            yield f"{self}: checked but naming convention not set"
        if self.type is PersonType.unchecked:
            if self.bio:
                yield f"{self}: unchecked but bio set"
            if self.tags:
                yield f"{self}: unchecked but tags set"
            if self.birth:
                yield f"{self}: unchecked but year of birth set"
            if self.death:
                yield f"{self}: unchecked but year of death set"
        if self.naming_convention is NamingConvention.other:
            return
        if self.tussenvoegsel:
            allowed = ALLOWED_TUSSENVOEGSELS.get(self.naming_convention, set())
            if self.tussenvoegsel not in allowed:
                yield f"{self}: disallowed tussenvoegsel {self.tussenvoegsel!r}"
        if self.initials and not self.given_names:
            if self.naming_convention is NamingConvention.russian:
                grammar = parsing.russian_initials_pattern
            elif self.naming_convention is NamingConvention.ukrainian:
                grammar = parsing.ukrainian_initials_pattern
            else:
                grammar = parsing.initials_pattern
            if not parsing.matches_grammar(self.initials, grammar):
                yield f"{self}: invalid initials: {self.initials!r}"
        if self.naming_convention is NamingConvention.organization:
            if self.given_names:
                yield f"{self}: given_names set for organization"
            if self.initials:
                yield f"{self}: initials set for organization"
            if self.suffix:
                yield f"{self}: suffix set for organization"
        elif self.naming_convention is NamingConvention.pinyin:
            if self.given_names:
                if not parsing.matches_grammar(
                    self.given_names, parsing.pinyin_given_names_pattern
                ):
                    yield (
                        f"{self}: invalid pinyin in given names: {self.given_names!r}"
                    )
                if not parsing.matches_grammar(
                    self.given_names.lower(),
                    parsing.pinyin_given_names_lowercased_pattern,
                ):
                    yield (
                        f"{self}: invalid pinyin in given names: {self.given_names!r}"
                    )
            if not parsing.matches_grammar(
                self.family_name, parsing.chinese_family_name_pattern
            ):
                yield f"{self}: invalid pinyin in family name: {self.family_name!r}"
            if not parsing.matches_grammar(
                self.family_name.lower(), parsing.pinyin_family_name_lowercased_pattern
            ):
                yield f"{self}: invalid pinyin in family name: {self.family_name!r}"

        else:
            if self.given_names:
                if self.naming_convention is NamingConvention.russian:
                    grammar = parsing.russian_given_names_pattern
                elif self.naming_convention is NamingConvention.ukrainian:
                    grammar = parsing.ukrainian_given_names_pattern
                elif self.naming_convention is NamingConvention.chinese:
                    grammar = parsing.chinese_given_names_pattern
                else:
                    grammar = parsing.given_names_pattern
                if not parsing.matches_grammar(self.given_names, grammar):
                    yield f"{self}: invalid given names: {self.given_names!r}"
            if self.naming_convention is NamingConvention.chinese:
                grammar = parsing.chinese_family_name_pattern
            elif self.naming_convention is NamingConvention.russian:
                grammar = parsing.russian_family_name_pattern
            elif self.naming_convention is NamingConvention.ukrainian:
                grammar = parsing.ukrainian_family_name_pattern
            elif self.naming_convention is NamingConvention.burmese:
                grammar = parsing.burmese_names_pattern
            else:
                grammar = parsing.family_name_pattern
            if not parsing.matches_grammar(self.family_name, grammar):
                yield f"{self}: invalid family name: {self.family_name!r}"

    @classmethod
    def fix_bad_suffixes(cls) -> None:
        for person in (
            cls.select_valid().filter(cls.suffix != None).order_by(cls.family_name)
        ):
            if person.suffix != "Jr." and "." in person.suffix:
                print(person)
                person.display(full=True)
                person.maybe_reassign_references()

    @classmethod
    def find_duplicates(cls, autofix: bool = False) -> list[list[Person]]:
        by_key: dict[tuple[str | None, ...], list[Person]] = defaultdict(list)
        for person in cls.select_valid().filter(cls.type << UNCHECKED_TYPES):
            key = (
                person.family_name,
                None if person.given_names is not None else person.initials,
                person.given_names,
                person.suffix,
                person.tussenvoegsel,
            )
            by_key[key].append(person)
        return cls.display_duplicates(by_key, autofix=autofix)

    @classmethod
    def find_near_duplicates(cls, min_count: int = 20) -> list[list[Person]]:
        by_key: dict[str, list[Person]] = defaultdict(list)
        for person in cls.select_valid():
            key = helpers.simplify_string(
                person.get_full_name().replace("-", ""), clean_words=False
            )
            by_key[key].append(person)
        by_key = {
            key: persons
            for key, persons in by_key.items()
            if sum(person.total_references() > 0 for person in persons) > 1
        }
        return cls.display_duplicates(by_key, min_count=min_count)

    @classmethod
    def display_duplicates(
        cls,
        by_key: Mapping[Any, list[Person]],
        autofix: bool = False,
        min_count: int = 0,
    ) -> list[list[Person]]:
        out = []
        for key, group in sorted(
            by_key.items(), key=lambda pair: sum(p.total_references() for p in pair[1])
        ):
            if len(group) == 1:
                continue
            if (
                min_count > 0
                and sum(person.total_references() for person in group) < min_count
            ):
                continue
            getinput.print_header(key)
            for person in group:
                print(person.total_references(), repr(person))
            if autofix:
                cls.maybe_merge_group(group)
            out.append(group)
        return out

    @classmethod
    def maybe_merge_group(cls, group: list[Person]) -> None:
        group = sorted(group, key=lambda person: (-person.get_level().value, person.id))

        def all_the_same(group: Iterable[Person], field: str) -> bool:
            return len({getattr(person, field) for person in group}) == 1

        if (
            (
                all(person.type is PersonType.unchecked for person in group)
                and all_the_same(group, "naming_convention")
            )
            or (
                group[0].get_level() is PersonLevel.has_convention
                and all(
                    person.get_level() < PersonLevel.has_convention
                    for person in group[1:]
                )
            )
            or (
                all(person.type is PersonType.soft_redirect for person in group)
                and all(
                    all_the_same(group, field)
                    for field in [
                        "family_name",
                        "given_names",
                        "initials",
                        "suffix",
                        "tussenvoegsel",
                    ]
                )
            )
        ):
            for person in group[1:]:
                print(f"Reassign {person} -> {group[0]}")
                person.reassign_references(target=group[0])
                person.type = PersonType.deleted  # type: ignore

    def num_references(self) -> dict[str, int]:
        num_refs = {}
        for field in self.derived_fields:
            refs = self.get_raw_derived_field(field.name)
            if refs is not None:
                num_refs[field.name] = len(refs)
        return num_refs

    def total_references(self) -> int:
        return sum(self.num_references().values())

    def reassign_initials_only(self) -> None:
        arts = self.get_sorted_derived_field("articles")
        for art in arts:
            art = art.reload()
            art.specify_authors()

    def reassign_references(self, target: Person | None = None) -> None:
        for field_name, tag_name, tag_cls in [
            ("books", "author_tags", AuthorTag.Author),
            ("articles", "author_tags", AuthorTag.Author),
            ("names", "author_tags", AuthorTag.Author),
            ("patronyms", "type_tags", models.TypeTag.NamedAfter),
            ("collected", "type_tags", models.TypeTag.CollectedBy),
            ("involved", "type_tags", models.TypeTag.Involved),
        ]:
            for obj in self.get_sorted_derived_field(field_name):
                if field_name == "names":
                    obj.check_authors(autofix=True)
                self.edit_tag_sequence_on_object(
                    obj, tag_name, tag_cls, field_name, target=target
                )

    def get_sorted_derived_field(self, field_name: str) -> list[Any]:
        objs = self.get_derived_field(field_name)
        if objs is None:
            return []
        return sorted(objs, key=_display_sort_key)

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = super().get_adt_callbacks()
        return {
            **callbacks,
            "v": lambda: self.reassign_names_with_verbatim(filter_for_name=True),
            "i": lambda: self.display(full=True, include_detail=True),
            "r": self.reassign_references,
            "reassign_references": self.reassign_references,
            "rio": self.reassign_initials_only,
            "reassign_initials_only": self.reassign_initials_only,
            "move": self.move_all_references,
            "soft": self.make_soft_redirect,
            "hard": self.make_hard_redirect,
            "print_character_names": self.print_character_names,
        }

    def print_character_names(self) -> None:
        print("=== family name ===")
        helpers.print_character_names(self.family_name)
        if self.given_names:
            print("=== given names ===")
            helpers.print_character_names(self.given_names)
        if self.initials:
            print("=== initials ===")
            helpers.print_character_names(self.initials)
        if self.tussenvoegsel:
            print("=== tussenvoegsel ===")
            helpers.print_character_names(self.tussenvoegsel)

    def maybe_reassign_references(self) -> None:
        num_refs = self.total_references()
        if num_refs == 0:
            return
        print(f"======= {self} ({num_refs}) =======")
        while True:
            command = getinput.get_line(
                "command> ",
                validate=lambda command: command
                in (
                    "s",
                    "skip",
                    "soft",
                    "move",
                    "soft_redirect",
                    "",
                    "h",
                    "hard_redirect",
                    "",
                ),
                allow_none=True,
                mouse_support=False,
                history_key="reassign_references",
                callbacks={
                    **self.get_adt_callbacks(),
                    "p": lambda: print("s = skip, r = soft redirect, d = display"),
                    "e": self.edit,
                },
                should_stop=lambda _: self.total_references() == 0,
            )
            if command in ("s", "skip"):
                return
            else:
                self.reassign_references()
                return

    def reassign_names_with_verbatim(self, filter_for_name: bool = False) -> None:
        nams = self.get_derived_field("names")
        if not nams:
            return
        nams = [nam for nam in nams if nam.verbatim_citation is not None]
        if filter_for_name:
            query = self.family_name.lower()
            nams = [nam for nam in nams if query in nam.verbatim_citation.lower()]
        nams = sorted(nams, key=lambda nam: (nam.numeric_year(), nam.verbatim_citation))
        verbatim_to_target: dict[str, Person] = {}
        for nam in nams:
            new_target = self.edit_tag_sequence_on_object(
                nam,
                "author_tags",
                AuthorTag.Author,
                "names",
                verbatim_to_target.get(nam.verbatim_citation),
            )
            if new_target is not None:
                verbatim_to_target[nam.verbatim_citation] = new_target

    def move_all_references(self, target: Person | None = None) -> None:
        if target is None:
            target = Person.getter(None).get_one("target > ")
        if target is None:
            return
        if target == self:
            print(f"Cannot move references of {self} to itself")
            return
        self.reassign_references(target=target)

    def make_soft_redirect(self, target: Person | None = None) -> None:
        if target is None:
            target = Person.getter(None).get_one("target > ")
        if target is None:
            return
        if target == self:
            print(f"Cannot redirect {self} to itself")
            return
        self.type = PersonType.soft_redirect  # type: ignore
        self.target = target
        self.reassign_references(target=target)

    def make_hard_redirect(self, target: Person | None = None) -> None:
        if target is None:
            target = Person.getter(None).get_one("target > ")
        if target is None:
            return
        if target == self:
            print(f"Cannot redirect {self} to itself")
            return
        self.type = PersonType.hard_redirect  # type: ignore
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

    def is_more_specific_than(self, other: Person) -> bool:
        if other.type in (PersonType.hard_redirect, PersonType.soft_redirect):
            return other.target == self
        if self.family_name != other.family_name:
            return False
        if self.given_names:
            if other.given_names:
                return self.given_names.startswith(other.given_names + " ")
            elif other.initials:
                return self._has_more_specific_initials(other)
            else:
                return True
        elif self.initials:
            if other.given_names:
                return False
            elif other.initials:
                return self._has_more_specific_initials(other)
            else:
                return True
        return False

    def _has_more_specific_initials(self, other: Person) -> bool:
        my_initials = self.get_initials()
        other_initials = other.get_initials()
        return (
            my_initials is not None
            and other_initials is not None
            and my_initials.replace("-", "")
            .lower()
            .startswith(other_initials.replace("-", "").lower())
        )

    @classmethod
    def resolve_redirects(cls) -> None:
        for person in cls.select_valid().filter(
            cls.type << (PersonType.soft_redirect, PersonType.hard_redirect)
        ):
            refs = person.total_references()
            if refs > 0:
                getinput.print_header(f"{person!r}: {refs} references")
                if person.type is PersonType.hard_redirect:
                    person.reassign_references(person.target)
                else:
                    person.display(full=True)
                    if getinput.yes_no("Reassign references? "):
                        target = person.target
                    else:
                        target = None
                    person.reassign_references(target)

    @classmethod
    def autodelete(cls, dry_run: bool = False) -> None:
        cls.compute_all_derived_fields()
        for person in cls.select_valid().filter(cls.type == PersonType.unchecked):
            person.maybe_autodelete(dry_run=dry_run)

    @classmethod
    def create_interactively(
        cls, family_name: str | None = None, **kwargs: Any
    ) -> Person | None:
        if family_name is None:
            family_name = cls.getter("family_name").get_one_key("family_name> ")
        if family_name is None:
            return None
        # Always make a checked person, since unchecked persons should be created
        # through get_or_create_unchecked
        kwargs.setdefault("type", PersonType.checked)
        kwargs.setdefault("naming_convention", NamingConvention.unspecified)
        person = cls.create(family_name=family_name, **kwargs)
        person.edit_until_clean(initial_edit=True)
        return person

    @classmethod
    def get_interactive_creators(cls) -> dict[str, Callable[[], Any]]:
        return {
            **super().get_interactive_creators(),
            "u": cls.make_unchecked,
            "f": cls.make_family_name,
        }

    @classmethod
    def make_unchecked(cls) -> Person | None:
        family_name = cls.getter("family_name").get_one_key("family_name> ")
        if family_name is None:
            return None
        kwargs = {}
        for field in ("initials", "given_names", "suffix", "tussenvoegsel"):
            kwargs[field] = cls.getter(field).get_one_key(f"{field}> ")
        person = cls.get_or_create_unchecked(family_name, **kwargs)
        person.edit_until_clean()
        return person

    @classmethod
    def make_family_name(cls) -> Person | None:
        family_name = cls.getter("family_name").get_one_key("family_name> ")
        if family_name is None:
            return None
        person = cls.get_or_create_unchecked(family_name)
        person.edit_until_clean()
        return person

    @classmethod
    def get_or_create_from_ol_id(cls, ol_id: str) -> Person:
        person = cls.select_one(ol_id=ol_id)
        if person is None:
            data = get_author(ol_id)
            name = data["name"]
            if " " in name:
                given_names, family_name = name.rsplit(" ", maxsplit=1)
            else:
                family_name = name
                given_names = None
            person = cls.get_or_create_unchecked(
                family_name=family_name, given_names=given_names
            )
            if person.ol_id is None:
                person.ol_id = ol_id
            else:
                cls.create(
                    family_name=family_name,
                    given_names=given_names,
                    ol_id=ol_id,
                    type=PersonType.soft_redirect,
                    target=person,
                )
        return person

    @classmethod
    def get_or_create_unchecked(
        cls,
        family_name: str,
        *,
        initials: str | None = None,
        given_names: str | None = None,
        suffix: str | None = None,
        tussenvoegsel: str | None = None,
    ) -> Person:
        family_name = helpers.clean_string(family_name)
        if initials is not None:
            initials = helpers.clean_string(initials)
        if given_names is not None:
            given_names = helpers.clean_string(given_names)
        if suffix is not None:
            suffix = helpers.clean_string(suffix)
        if tussenvoegsel is not None:
            tussenvoegsel = helpers.clean_string(tussenvoegsel)
        objs = list(
            Person.select_valid().filter(
                Person.family_name == family_name,
                Person.given_names == given_names,
                Person.initials == initials,
                Person.suffix == suffix,
                Person.tussenvoegsel == tussenvoegsel,
                Person.type << UNCHECKED_TYPES,
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
                naming_convention=NamingConvention.unspecified,
            )
            print(f"Created {obj}")
            return obj

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:
        for field_name, tag_cls in [("tags", models.tags.PersonTag)]:
            if field == field_name:
                completers: dict[
                    tuple[type[adt.ADT], str], getinput.Completer[Any]
                ] = {}
                for tag in tag_cls._tag_to_member.values():
                    for attribute, typ in tag._attributes.items():
                        completer: getinput.Completer[Any] | None
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

    def get_level(self) -> PersonLevel:
        if self.type is PersonType.checked:
            return PersonLevel.checked
        elif self.type in (PersonType.soft_redirect, PersonType.hard_redirect):
            return PersonLevel.redirect
        elif self.type is PersonType.unchecked:
            if self.given_names:
                if self.naming_convention is NamingConvention.unspecified:
                    return PersonLevel.has_given_name
                else:
                    return PersonLevel.has_convention
            elif self.initials:
                return PersonLevel.initials_only
            elif self.naming_convention is NamingConvention.burmese:
                return PersonLevel.has_convention
            else:
                return PersonLevel.family_name_only
        else:
            return PersonLevel.unused


# Reused in Article and Name
class AuthorTag(adt.ADT):
    Author(person=Person, tag=2)  # type: ignore


def get_new_authors_list() -> list[AuthorTag]:
    authors = []
    while True:
        author = Person.getter(None).get_one("author> ")
        if author is None:
            break
        authors.append(AuthorTag.Author(person=author))
    return authors


def _display_year(year: str | None) -> str:
    if year is None:
        return ""
    try:
        if int(year) < 0:
            return f"{-int(year)} BC"
    except ValueError:
        pass
    return year


def _display_sort_key(obj: BaseModel) -> Any:
    if isinstance(obj, (models.Name, models.Article)):
        return (obj.numeric_year(), obj.sort_key())
    else:
        return obj.sort_key()
