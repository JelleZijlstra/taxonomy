from bs4 import BeautifulSoup
import datetime
from pathlib import Path
from peewee import (
    CharField,
    DeferredForeignKey,
    ForeignKeyField,
    IntegerField,
    TextField,
)
import re
import requests
import subprocess
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from .base import ADTField, BaseModel, EnumField
from ..constants import ArticleCommentKind, ArticleKind, ArticleType
from ..helpers import to_int
from ... import config, events, adt, getinput

from .citation_group import CitationGroup

T = TypeVar("T", bound="Article")

_options = config.get_options()
_TYPE_TO_FIELDS = {
    ArticleType.JOURNAL: [
        "authors",
        "year",
        "title",
        "citation_group",
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
    ArticleType.BOOK: [
        "authors",
        "year",
        "title",
        "pages",
        "publisher",
        "citation_group",
    ],
    ArticleType.THESIS: [
        "authors",
        "year",
        "title",
        "pages",
        "citation_group",
        "series",
    ],
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
    excluded_fields = {"path", "addmonth", "addday", "addyear", "kind", "tags"}

    # Properties that have a one-to-one correspondence with the database.
    addmonth = CharField()  # month added to catalog
    addday = CharField()  # day added to catalog
    addyear = CharField()  # year added to catalog
    name = CharField()  # name of file (or handle of citation)
    authors = CharField(null=True)
    year = CharField(null=True)  # year published
    # title (chapter title for book chapter; book title for full book or thesis)
    title = CharField(null=True)
    _journal = CharField(
        db_column="journal"
    )  # journal published in (deprecated; use citation_group)
    series = CharField(null=True)  # journal series
    volume = CharField(null=True)  # journal volume
    issue = CharField(null=True)  # journal issue
    start_page = CharField(null=True)
    end_page = CharField(null=True)
    url = CharField(null=True)
    doi = CharField(null=True)
    type = EnumField(ArticleType)  # type of file
    publisher = CharField(null=True)
    _location = CharField(
        db_column="location"
    )  # geographical location published (deprecated; use citation_group)
    pages = CharField(null=True)  # number of pages in book
    misc_data = CharField()  # miscellaneous data

    path = CharField(
        null=True
    )  # path to file (contains NOFILE if "file" is not a file)
    ids = CharField(
        null=True
    )  # array of properties for various less-common identifiers
    bools = CharField(null=True)  # array of boolean flags
    kind = EnumField(ArticleKind)
    parent = DeferredForeignKey("Article", null=True)
    tags = ADTField(lambda: ArticleTag, null=True)
    citation_group = ForeignKeyField(CitationGroup, null=True)

    @property
    def journal(self) -> Optional[str]:
        if self.type != ArticleType.JOURNAL:
            return None
        if self.citation_group is not None:
            return self.citation_group.name
        else:
            return None

    @journal.setter
    def journal(self, value: str) -> None:
        if value is None:
            self.citation_group = None
        else:
            self.citation_group = CitationGroup.get_or_create(value)

    @property
    def place_of_publication(self) -> Optional[str]:
        if self.type not in (ArticleType.BOOK, ArticleType.WEB):
            return None
        if self.citation_group is not None:
            return self.citation_group.name
        else:
            return None

    @property
    def institution(self) -> Optional[str]:
        if self.type != ArticleType.THESIS:
            return None
        if self.citation_group is not None:
            return self.citation_group.name
        else:
            return None

    class Meta:
        db_table = "article"

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(cls.kind != ArticleKind.redirect)

    def should_skip(self) -> bool:
        return self.kind is ArticleKind.redirect

    @classmethod
    def bfind(
        cls,
        *args: Any,
        quiet: bool = False,
        sort_key: Optional[Callable[["Article"], Any]] = None,
        journal: Optional[str] = None,
        **kwargs: Any,
    ) -> List["Article"]:
        if journal is not None:
            args = (*args, cls.citation_group == CitationGroup.get(name=journal))
        return super().bfind(*args, quiet=quiet, sort_key=sort_key, **kwargs)

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        callbacks = super().get_adt_callbacks()
        return {
            **callbacks,
            "o": self.openf,
            "openf": self.openf,
            "reverse": self.reverse_authors,
        }

    def get_required_fields(self) -> Iterable[str]:
        yield "kind"
        yield "type"
        yield "addmonth"
        yield "addday"
        yield "addyear"
        if self.kind == ArticleKind.electronic:
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

    def get_path(self) -> Path:
        """Returns the full path to this file."""
        if not self.isfile():
            raise ValueError("path() called on a non-file")
        out = self.relative_path()
        return _options.library_path / out / self.name

    def path_list(self) -> List[str]:
        if self.path is None:
            return []
        else:
            return self.path.split("/")

    def relative_path(self) -> Path:
        """Returns the path relative to the library root."""
        path = self.path_list()
        if not path:
            return Path()
        out = Path(path[0])
        for part in path[1:]:
            out = out / part
        return out

    def openf(self, place: str = "catalog") -> None:
        if not self.isfile():
            if self.parent is not None:
                self.parent.openf()
            else:
                print("openf: error: not a file, cannot open")
            return
        if place == "temp" or not self.path:
            path = _options.new_path / self.name
        elif place == "catalog":
            path = self.get_path()
        else:
            raise ValueError(f"invalid place {place}")
        subprocess.check_call(["open", str(path)])

    def isfile(self) -> bool:
        # returns whether this 'file' is a file
        return self.kind == ArticleKind.electronic

    def isnofile(self) -> bool:
        return not self.isfile()

    def issupplement(self) -> bool:
        return self.type == ArticleType.SUPPLEMENT

    def isredirect(self) -> bool:
        return self.kind == ArticleKind.redirect

    def numeric_year(self) -> int:
        return to_int(self.year)

    def numeric_start_page(self) -> int:
        return to_int(self.start_page)

    def is_page_in_range(self, page: int) -> bool:
        if self.pages:
            try:
                pages = int(self.pages)
            except ValueError:
                return False
            return page <= pages
        elif self.start_page and self.end_page:
            try:
                start_page = int(self.start_page)
                end_page = int(self.end_page)
            except ValueError:
                return False
            return page in range(start_page, end_page + 1)
        else:
            return False

    # Authors

    def getAuthors(
        self,
        separator: str = ";",  # Text between two authors
        lastSeparator: Optional[str] = None,  # Text between last two authors
        separatorWithTwoAuthors: Optional[
            str
        ] = None,  # Text between authors if there are only two
        asArray: bool = False,  # Return authors as an array
        capitalizeNames: bool = False,  # Whether to capitalize names
        spaceInitials: bool = False,  # Whether to space initials
        initialsBeforeName: bool = False,  # Whether to place initials before the surname
        firstInitialsBeforeName: bool = False,  # Whether to place the first author's initials before their surname
        includeInitials: bool = True,  # Whether to include initials
    ) -> Any:
        if lastSeparator is None:
            lastSeparator = separator
        if separatorWithTwoAuthors is None:
            separatorWithTwoAuthors = lastSeparator
        array = self._getAuthors()
        if asArray:
            return array
        out = ""
        num_authors = len(array)
        for i, author in enumerate(array):
            # Separators
            if i > 0:
                if i < num_authors - 1:
                    out += f"{separator} "
                elif i == 1:
                    out += f"{separatorWithTwoAuthors} "
                else:
                    out += f"{lastSeparator} "

            # Process author
            if capitalizeNames:
                author = (author[0].upper(), *author[1:])
            if spaceInitials and len(author) > 1:
                initials = re.sub(r"\.(?![- ]|$)", ". ", author[1])
                author = (author[0], initials, *author[2:])
            if len(author) > 1 and includeInitials:
                if firstInitialsBeforeName if i == 0 else initialsBeforeName:
                    author_str = author[1] + " " + author[0]
                else:
                    author_str = author[0] + ", " + author[1]
                if len(author) > 2:
                    author_str += ", " + author[2]
            else:
                author_str = author[0]
            out += author_str
        return out

    @staticmethod
    def explode_authors(input: str) -> List[Sequence[str]]:
        authors = input.split("; ")

        def map_fn(author: str) -> Sequence[str]:
            arr = author.split(", ")
            if len(arr) > 1:
                return arr
            else:
                return (author, "")

        return [map_fn(author) for author in authors]

    def reverse_authors(self) -> None:
        authors = self._getAuthors()
        self.authors = self.implode_authors(reversed(authors))
        self.save()

    def countAuthors(self) -> int:
        return len(self._getAuthors())

    def getPaleoBioDBAuthors(self) -> Dict[str, str]:
        authors = self._getAuthors()

        def author_fn(author: Sequence[str]) -> Tuple[str, str]:
            name = author[0]
            if len(author) > 2:
                name += ", " + author[2]
            return (author[1], name)

        authors = [author_fn(author) for author in authors]
        output = {
            "author1init": "",
            "author1last": "",
            "author2init": "",
            "author2last": "",
            "otherauthors": "",
        }
        if len(authors) > 0:
            output["author1init"] = authors[0][0]
            output["author1last"] = authors[0][1]
        if len(authors) > 1:
            output["author2init"] = authors[1][0]
            output["author2last"] = authors[1][1]
        if len(authors) > 2:
            output["otherauthors"] = ", ".join(
                " ".join(author) for author in authors[2:]
            )
        return output

    def taxonomicAuthority(self) -> Tuple[str, str]:
        return (
            self.getAuthors(separator=",", lastSeparator=" &", includeInitials=False),
            self.year,
        )

    def author_set(self) -> Set[str]:
        return {last for last, *_ in self._getAuthors()}

    def _getAuthors(self) -> List[Sequence[str]]:
        """Should return output like

        [
            ('Zijlstra', 'J.S.'),
            ('Smith', 'J.', 'Jr.'),
        ]

        for a paper by "Zijlstra, J.S.; Smith, J., Jr."."""
        if self.authors is None:
            return []
        return self.explode_authors(self.authors)

    def getEnclosingAuthors(self, **kwargs: Any) -> str:
        obj = self.getEnclosing()
        if obj is None:
            return ""
        if not obj.authors:
            return ""
        # why?
        if obj.authors == self.authors:
            return ""
        return obj.getAuthors(**kwargs)

    @staticmethod
    def implode_authors(authors: Iterable[Sequence[str]]) -> str:
        """Turns a list as returned by _getAuthors into a single string."""
        return "; ".join(
            ", ".join(part for part in author if part) for author in authors
        )

    def add_comment(
        self, kind: Optional[ArticleCommentKind] = None, text: Optional[str] = None
    ) -> "ArticleComment":
        return ArticleComment.create_interactively(article=self, kind=kind, text=text)

    def add_tag(self, tag: adt.ADT) -> None:
        if self.tags is None:
            self.tags = [tag]
        else:
            self.tags = self.tags + (tag,)

    def has_tag(self, tag_cls: Type[adt.ADT]) -> bool:
        for tag in self.get_tags(self.tags, tag_cls):
            return True
        return False

    def geturl(self) -> Optional[str]:
        # get the URL for this file from the data given
        if self.url:
            return self.url
        if self.doi:
            return f"http://dx.doi.org/{self.doi}"
        tries = {
            ArticleTag.JSTOR: "http://www.jstor.org/stable/",
            ArticleTag.HDL: "http://hdl.handle.net/",
            ArticleTag.PMID: "http://www.ncbi.nlm.nih.gov/pubmed/",
            ArticleTag.PMC: "http://www.ncbi.nlm.nih.gov/pmc/articles/PMC",
        }
        for identifier, url in tries.items():
            value = self.getIdentifier(identifier)
            if value:
                return url + value
        return None

    def openurl(self) -> bool:
        # open the URL associated with the file
        url = self.geturl()
        if not url:
            print("No URL to open")
            return False
        else:
            subprocess.check_call(["open", url])
            return True

    def getIdentifier(self, identifier: Type[adt.ADT]) -> Optional[str]:
        for tag in self.get_tags(self.tags, identifier):
            return tag.text
        return None

    def getEnclosing(self: T) -> Optional[T]:
        if self.parent is not None:
            return cast(T, self.parent)
        else:
            return None

    def concise_markdown_link(self) -> str:
        authors_list = self._getAuthors()
        if len(authors_list) > 2:
            authors = f"{authors_list[0][0]} et al."
        else:
            authors, _ = self.taxonomicAuthority()
        name = self.name.replace(" ", "_")
        return f"[{authors} ({self.year})](/a/{name})"

    def markdown_link(self) -> str:
        cite = self.cite().replace("<i>", "_").replace("</i>", "_")
        return f"[{cite}](/a/{self.id})"

    def cite(self, citetype: str = "paper") -> str:
        if self.issupplement():
            return self.parent.cite(citetype=citetype)
        if citetype in _CITE_FUNCTIONS:
            return _CITE_FUNCTIONS[citetype](self)
        else:
            raise ValueError(f"unknown citetype {citetype}")

    def finddoi(self) -> bool:
        if self.doi:
            return True
        if self.journal and self.volume and self.start_page:
            print(f"Trying to find DOI for file {self.name}... ")
            query_dict = {
                "pid": _options.crossrefid,
                "title": self.journal,
                "volume": self.volume,
                "spage": self.start_page,
                "noredirect": "true",
            }
            url = "http://www.crossref.org/openurl"
            response = requests.get(url, query_dict)
            if not response.ok:
                print(f"Failed to retrieve data: {response.text}")
                return False
            xml = BeautifulSoup(response.text, features="lxml")
            try:
                doi = xml.crossref_result.doi.text
            except AttributeError:
                print("nothing found")
                self.triedfinddoi = True
                return False
            else:
                print(f"found doi {doi}")
                self.doi = doi
                return True
        return False

    def display(self) -> None:
        print(self.cite())

    def display_names(self, full: bool = False) -> None:
        print(repr(self))
        new_names = sorted(self.new_names, key=lambda nam: nam.numeric_page_described())
        if new_names:
            print(f"New names ({len(new_names)}):")
            for nam in new_names:
                nam.display(full=full)
        tss_names = list(self.type_source_names)
        if tss_names:
            print(f"Type specimen source ({len(tss_names)}):")
            for nam in tss_names:
                nam.display(full=full)

    def __str__(self) -> str:
        return f"{{{self.name}}}"

    def __repr__(self) -> str:
        return f"{{{self.name}: {self.cite()}}}"


class ArticleComment(BaseModel):
    article = ForeignKeyField(Article, related_name="comments", db_column="article_id")
    kind = EnumField(ArticleCommentKind)
    date = IntegerField()
    text = TextField()

    call_sign = "AC"

    class Meta:
        db_table = "article_comment"

    @classmethod
    def make(
        cls, article: Article, kind: ArticleCommentKind, text: str, **kwargs: Any
    ) -> "ArticleComment":
        return cls.create(
            article=article, kind=kind, text=text, date=int(time.time()), **kwargs
        )

    @classmethod
    def create_interactively(
        cls,
        article: Optional[Article] = None,
        kind: Optional[ArticleCommentKind] = None,
        text: Optional[str] = None,
        **kwargs: Any,
    ) -> "ArticleComment":
        if article is None:
            article = cls.get_value_for_foreign_key_field_on_class(
                "article", allow_none=False
            )
        assert article is not None
        if kind is None:
            kind = getinput.get_enum_member(
                ArticleCommentKind, prompt="kind> ", allow_empty=False
            )
        if text is None:
            text = getinput.get_line(prompt="text> ")
        assert text is not None
        return cls.make(article=article, kind=kind, text=text, **kwargs)

    def get_description(self) -> str:
        components = [
            self.kind.name,
            datetime.datetime.fromtimestamp(self.date).strftime("%b %d, %Y %H:%M:%S"),
        ]
        return f'{self.text} ({"; ".join(components)})'


Citer = Callable[[Article], str]
_CITE_FUNCTIONS: Dict[str, Citer] = {}


def register_cite_function(name: str) -> Callable[[Citer], Citer]:
    def decorator(citer: Citer) -> Citer:
        _CITE_FUNCTIONS[name] = citer
        return citer

    return decorator


class ArticleTag(adt.ADT):
    # identifiers
    ISBN(text=str, tag=1)  # type: ignore
    Eurobats(text=str, tag=2)  # type: ignore
    HDL(text=str, tag=3)  # type: ignore
    JSTOR(text=str, tag=4)  # type: ignore
    PMID(text=str, tag=5)  # type: ignore
    ISSN(text=str, tag=6)  # type: ignore
    PMC(text=str, tag=7)  # type: ignore

    # other
    Edition(text=str, tag=8)  # type: ignore
    FullIssue(comment=str, tag=9)  # type: ignore
    PartLocation(  # type: ignore
        parent=Article, start_page=int, end_page=int, comment=str, tag=10
    )
    NonOriginal(comment=str, tag=10)  # type: ignore
