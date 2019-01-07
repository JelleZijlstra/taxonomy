import collections
import datetime
import enum
import json
import operator
import re
import sys
import time
import traceback
from typing import (
    cast,
    IO,
    Any,
    Callable,
    Container,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import peewee
from peewee import (
    BooleanField,
    CharField,
    ForeignKeyField,
    IntegerField,
    Model,
    MySQLDatabase,
    SqliteDatabase,
    TextField,
)

from .. import constants, definition, ehphp, helpers, settings
from ... import adt, events, getinput
from ..constants import (
    GenderArticle,
    Group,
    NomenclatureStatus,
    OccurrenceStatus,
    Rank,
    SourceLanguage,
    SpeciesNameKind,
    Status,
)
from ..definition import Definition

from .base import BaseModel, ModelT
from .region import Region

class Collection(BaseModel):
    creation_event = events.Event["Collection"]()
    save_event = events.Event["Collection"]()
    label_field = "label"

    label = CharField()
    name = CharField()
    location = ForeignKeyField(
        Region, related_name="collections", db_column="location_id"
    )
    comment = CharField(null=True)
    city = CharField(null=True)
    removed = BooleanField(default=False)

    def __repr__(self) -> str:
        city = f", {self.city}" if self.city else ""
        return f"{self.name}{city} ({self.label})"

    @classmethod
    def by_label(cls, label: str) -> "Collection":
        colls = list(cls.filter(cls.label == label))
        if len(colls) == 1:
            return colls[0]
        else:
            raise ValueError(f"found {colls} with label {label}")

    @classmethod
    def get_or_create(
        cls, label: str, name: str, location: Region, comment: Optional[str] = None
    ) -> "Collection":
        try:
            return cls.by_label(label)
        except ValueError:
            return cls.create(
                label=label, name=name, location=location, comment=comment
            )

    @classmethod
    def create_interactively(cls: Type[ModelT]) -> ModelT:
        label = getinput.get_line("label> ")
        name = getinput.get_line("name> ")
        location = cls.get_value_for_foreign_key_field_on_class("location")
        obj = cls.create(label=label, name=name, location=location)
        obj.fill_required_fields()
        return obj

    def display(self, full: bool = True, depth: int = 0) -> None:
        city = f", {self.city}" if self.city else ""
        print(" " * depth + f"{self!r}{city}, {self.location}")
        if self.comment:
            print(" " * (depth + 4) + f"Comment: {self.comment}")
        if full:
            for nam in sorted(self.type_specimens, key=lambda nam: nam.root_name):
                print(" " * (depth + 4) + f"{nam} (type: {nam.type_specimen})")

    def merge(self, other: "Collection") -> None:
        for nam in self.type_specimens:
            nam.collection = other
            nam.save()
        self.delete_instance()
