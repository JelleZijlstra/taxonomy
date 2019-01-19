from ..constants import ArticleType

from peewee import CharField, ForeignKeyField, TextField
from typing import Iterable, List, NamedTuple

from .base import BaseModel, EnumField

_TYPE_TO_FIELDS = {
    ArticleType.JOURNAL: [
        "authors",
        "year",
        "title",
        "journal",
        "volume",
        "issue",
        "start_page",
        "end_page",
        "url",
    ],
    ArticleType.CHAPTER: [
        "authors",
        "year",
        "title",
        "start_page",
        "end_page",
        "parent",
        "url",
    ],
    ArticleType.BOOK: ["authors", "year", "title", "pages", "publisher", "isbn"],
    ArticleType.THESIS: ["authors", "year", "title", "pages", "publisher", "series"],
    ArticleType.SUPPLEMENT: ["title", "parent"],
    ArticleType.WEB: ["authors", "year", "title", "url"],
    ArticleType.MISCELLANEOUS: ["authors", "year", "title", "url"],
}


class LsFile(NamedTuple):
    name: str
    path: List[str]


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
    typ = EnumField(ArticleType, db_column="type")
    publisher = CharField()
    location = CharField()
    pages = CharField()
    ids = TextField()
    bools = TextField()
    parent = ForeignKeyField("self", related_name="children", null=True)
    misc_data = TextField()

    label_field = "name"

    class Meta:
        db_table = "article"

    def get_required_fields(self) -> Iterable[str]:
        yield "type"
        yield "addmonth"
        yield "addday"
        yield "addyear"
        yield "path"
        yield "name"
        yield from _TYPE_TO_FIELDS[self.type]

    def __repr__(self) -> str:
        return f"{{{self.name}}}"
