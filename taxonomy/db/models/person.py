import sys
from collections import defaultdict
from typing import IO, Any, Dict, List, Optional, Type

from peewee import CharField, DeferredForeignKey

from ..constants import NamingConvention, PersonType
from ..derived_data import DerivedField, LazyType
from .. import models
from ... import adt, events

from .base import BaseModel, EnumField, ADTField


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
    tags = ADTField(lambda: PersonTag, null=True)
    naming_convention = EnumField(NamingConvention)
    type = EnumField(PersonType)
    target = DeferredForeignKey("Person", null=True)

    derived_fields = [
        DerivedField(
            "patronyms",
            LazyType(lambda: List[models.Name]),
            compute_all=lambda: _compute_from_type_tag(models.TypeTag.NamedAfter),
            pull_on_miss=False,
        ),
        DerivedField(
            "collected",
            LazyType(lambda: List[models.Name]),
            compute_all=lambda: _compute_from_type_tag(models.TypeTag.CollectedBy),
            pull_on_miss=False,
        ),
    ]

    def __str__(self) -> str:
        parts = []
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
        if self.birth or self.death:
            parts.append(f" ({_display_year(self.birth)}–{_display_year(self.death)})")
        parts.append(f" ({self.type.name}; {self.naming_convention.name})")
        return "".join(parts)

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
        kwargs.setdefault("type", PersonType.checked)
        kwargs.setdefault("naming_convention", NamingConvention.western)
        result = cls.create(family_name=family_name, **kwargs)
        result.fill_field("tags")
        return result

    def display(self, depth: int = 0, file: IO[str] = sys.stdout) -> None:
        onset = " " * depth
        file.write(onset + str(self) + "\n")
        if self.tags:
            for tag in self.tags:
                file.write(onset + " " * 4 + repr(tag) + "\n")


class PersonTag(adt.ADT):
    Wiki(text=str, tag=1)


def _display_year(year: Optional[str]) -> str:
    if year is None:
        return ""
    try:
        if int(year) < 0:
            return f"{-int(year)} BC"
    except ValueError:
        pass
    return year


def _compute_from_type_tag(
    tag_cls: "Type[models.TypeTag]"
) -> Dict[int, "List[models.Name]"]:
    out = defaultdict(list)
    for nam in models.Name.select_valid().filter(
        models.Name.type_tags.contains(f"[{tag_cls._tag},")
    ):
        for tag in nam.type_tags:
            if isinstance(tag, tag_cls):
                out[tag.person.id].append(nam)
    return out
