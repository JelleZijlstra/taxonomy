from ..constants import ArticleKind, ArticleType

from peewee import CharField, ForeignKeyField, TextField
from typing import Iterable, List, NamedTuple

from .base import ADTField, BaseModel, EnumField
from ... import events, adt

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
    creation_event = events.Event["Article"]()
    save_event = events.Event["Article"]()
    label_field = "name"
    call_sign = "A"

    # Properties that have a one-to-one correspondence with the database.
    addmonth = CharField()  # month added to catalog
    addday = CharField()  # day added to catalog
    addyear = CharField()  # year added to catalog
    name = CharField()  # name of file (or handle of citation)
    authors = CharField()
    year = CharField()  # year published
    # title (chapter title for book chapter; book title for full book or thesis)
    title = CharField()
    journal = CharField()  # journal published in
    series = CharField()  # journal series
    volume = CharField()  # journal volume
    issue = CharField()  # journal issue
    start_page = CharField()  # start page
    end_page = CharField()  # end page
    url = CharField()  # url where available
    doi = CharField()  # DOI
    type = EnumField(ArticleType)  # type of file
    publisher = CharField()  # publisher
    location = CharField()  # geographical location published
    pages = CharField()  # number of pages in book
    misc_data = CharField()  # miscellaneous data

    path = CharField()  # path to file (contains NOFILE if "file" is not a file)
    ids = CharField()  # array of properties for various less-common identifiers
    bools = CharField()  # array of boolean flags
    kind = EnumField(ArticleKind)
    parent = ForeignKeyField("self", related_name="children", null=True)
    tags = ADTField(lambda: Tag, null=True)

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


class Tag(adt.ADT):
    # identifiers
    ISBN(text=str, tag=1)  # type: ignore
    Eurobats(text=str, tag=2)  # type: ignore
    HDL(text=str, tag=3)  # type: ignore
    JStor(text=str, tag=4)  # type: ignore
    PMID(text=str, tag=5)  # type: ignore
    ISSN(text=str, tag=6)  # type: ignore
    PMC(text=str, tag=7)  # type: ignore

    # other
    Edition(text=str, tag=8)  # type: ignore
    FullIssue(comment=str, tag=9)  # type: ignore
    PartLocation(  # type: ignore
        parent=Article, start_page=int, end_page=int, comment=str, tag=10
    )
