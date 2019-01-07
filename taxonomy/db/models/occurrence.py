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

from .base import BaseModel, EnumField
from .taxon import Taxon
from .location import Location



class Occurrence(BaseModel):
    taxon = ForeignKeyField(Taxon, related_name="occurrences", db_column="taxon_id")
    location = ForeignKeyField(Location, related_name="taxa", db_column="location_id")
    comment = CharField()
    status = EnumField(OccurrenceStatus, default=OccurrenceStatus.valid)
    source = CharField()

    def add_comment(self, new_comment: str) -> None:
        if self.comment is None:
            self.comment = new_comment
        else:
            self.comment += " " + new_comment
        self.save()

    def __repr__(self) -> str:
        out = "{} in {} ({}{})".format(
            self.taxon,
            self.location,
            self.source,
            "; " + self.comment if self.comment else "",
        )
        if self.status != OccurrenceStatus.valid:
            out = "[{}] {}".format(self.status.name.upper(), out)
        return out

