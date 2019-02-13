from ..constants import ArticleKind, ArticleType

from pathlib import Path
from peewee import CharField, ForeignKeyField, TextField
import subprocess
from typing import Iterable, List, NamedTuple

from .base import ADTField, BaseModel, EnumField
from ... import config, events, adt

_options = config.get_options()
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
    parent = ForeignKeyField("self", null=True)
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

    def trymanual(self) -> bool:
        fields = _TYPE_TO_FIELDS[self.type]
        for field in fields:
            if getattr(self, field, None):
                continue
            self.fill_field(field)
        return True

    def get_path(self, *, folder: bool = False, fullpath: bool = True) -> Path:
        # returns path to file
        # 'type' => 'Type of path: shell, url, or none',
        # 'folder' => 'Whether we want the folder only',
        # 'fullpath' => 'Whether the full path should be returned',
        # 'print' => 'Whether the result should be printed',
        if not self.isfile():
            raise ValueError("path() called on a non-file")
        out = self._path()
        if fullpath:
            out = _options.library_path / out
        if not folder:
            out = out / self.name
        return out

    def path_string(self) -> str:
        return self.path or ""

    def path_list(self) -> List[str]:
        if self.path is None:
            return []
        else:
            return self.path.split("/")

    def _path(self) -> Path:
        path = self.path_list()
        out = Path(path[0])
        for part in path[1:]:
            out = out / part
        return out

    def openf(self, place: str = "catalog") -> bool:
        if not self.isfile():
            print("openf: error: not a file, cannot open")
            return False
        if place == "catalog":
            path = self.get_path(fullpath=True)
        elif place == "temp":
            path = _options.new_path / self.name
        subprocess.check_call(["open", str(path)])
        return True

    def isfile(self) -> bool:
        # returns whether this 'file' is a file
        return self.kind == ArticleKind.electronic

    def isnofile(self) -> bool:
        return not self.isfile()

    def issupplement(self) -> bool:
        return self.type == ArticleType.SUPPLEMENT

    def isredirect(self) -> bool:
        return self.type == ArticleType.REDIRECT

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
