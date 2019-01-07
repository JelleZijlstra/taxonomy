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

from .base import BaseModel

class Article(BaseModel):
    addmonth = CharField()
    addday = CharField()
    addyear = CharField()
    path = CharField()
    name = CharField()
    authors = CharField()
    year = CharField()
    title = CharField()
    journal = CharField()
    series = CharField()
    volume = CharField()
    issue = CharField()
    start_page = CharField()
    end_page = CharField()
    url = CharField()
    doi = CharField()
    typ = IntegerField(db_column="type")
    publisher = CharField()
    location = CharField()
    pages = CharField()
    ids = TextField()
    bools = TextField()
    parent = CharField()
    misc_data = TextField()

    class Meta:
        db_table = "article"
