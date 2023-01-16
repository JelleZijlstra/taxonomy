from __future__ import annotations
import json
from taxonomy import getinput
from peewee import CharField, ForeignKeyField, TextField
from typing import Any, TypeVar
from collections.abc import Iterable

from .base import ADTField, BaseModel
from ..constants import SOURCE_LANGUAGE_SYNONYMS, SourceLanguage
from ..helpers import to_int
from ..openlibrary import get_from_isbn
from ... import events, adt

from .citation_group import CitationGroup
from .person import AuthorTag, Person, get_new_authors_list

T = TypeVar("T", bound="Book")


class Book(BaseModel):
    creation_event = events.Event["Book"]()
    save_event = events.Event["Book"]()
    label_field = "title"
    call_sign = "B"

    author_tags = ADTField(lambda: AuthorTag, null=True)
    year = CharField(null=True)
    title = CharField(null=False)
    subtitle = CharField(null=True)
    pages = CharField(null=True)  # number of pages in book
    isbn = CharField(null=True)
    publisher = CharField(null=True)
    tags = ADTField(lambda: BookTag, null=True)
    citation_group = ForeignKeyField(CitationGroup, null=True)
    dewey = CharField(null=True)
    loc = CharField(null=True)
    data = TextField(null=True)
    category = CharField(null=True)

    class Meta:
        db_table = "book"

    @classmethod
    def create_interactively(
        cls, title: str | None = None, **kwargs: Any
    ) -> Book | None:
        if title is None:
            title = cls.getter("title").get_one_key("title> ")
        if title is None:
            return None
        book = cls.create(title=title, **kwargs)
        book.fill_required_fields()
        book.edit()
        return book

    @classmethod
    def create_many(cls) -> None:
        while True:
            try:
                book = cls.create_from_isbn()
                if book is None:
                    break
            except RuntimeError as e:
                if e.args[0] == 404:
                    print("Could not resolve ISBN")
                    cls.create_interactively()
                else:
                    raise

    @classmethod
    def fix_all(cls) -> None:
        for book in cls.select_valid():
            getinput.print_header(repr(book))
            book.full_data()
            if book.category is None:
                book.fill_field("category")
            if not book.tags:
                print("No tags!")
                book.edit()
            else:
                for tag in book.tags:
                    if tag.language is SourceLanguage.other:
                        print("Set language!", tag)
                        book.edit()

    @classmethod
    def create_from_isbn(cls, isbn: str | None = None) -> Book | None:
        if isbn is None:
            isbn = cls.getter("isbn").get_one_key("isbn for new book> ")
        if isbn is None:
            return None
        data = get_from_isbn(isbn)
        book = cls.create(
            title=data["title"], isbn=isbn, data=json.dumps(data, separators=(",", ":"))
        )
        book.expand_open_library_data(data)
        book.fill_required_fields()
        return book

    def expand_open_library_data(self, data: dict[str, Any]) -> None:
        if data.get("subtitle"):
            self.subtitle = data["subtitle"]
        if data.get("publishers"):
            self.publisher = data["publishers"][0]
        if data.get("dewey_decimal_class"):
            self.dewey = data["dewey_decimal_class"][0]
        if data.get("lc_classifications"):
            self.loc = data["lc_classifications"][0]
        if data.get("publish_date"):
            self.year = data["publish_date"]
        self.data = json.dumps(data, separators=(",", ":"))
        self.title = data["title"]
        if data.get("isbn_13"):
            self.isbn = data["isbn_13"][0]
        if data.get("number_of_pages"):
            self.pages = data["number_of_pages"]
        if data.get("authors"):
            author_tags = []
            for author in data["authors"]:
                ol_id = author["key"].split("/")[2]
                person = Person.get_or_create_from_ol_id(ol_id)
                author_tags.append(AuthorTag.Author(person=person))
            self.author_tags = author_tags  # type: ignore
        for language in data.get("languages", []):
            identifier = language["key"].split("/")[2]
            if identifier == "mul":
                continue
            source_lang = SOURCE_LANGUAGE_SYNONYMS[identifier]
            self.add_tag(BookTag.Language(language=source_lang))
        if data.get("publish_places"):
            place = data["publish_places"][0]
            self.citation_group = CitationGroup.get_or_create_city(place)

    def edit(self) -> None:
        self.fill_field("tags")

    def get_authors(self) -> list[Person]:
        if self.author_tags is None:
            return []
        return [author.person for author in self.author_tags]

    def get_value_for_field(self, field: str, default: str | None = None) -> Any:
        if field == "author_tags" and not self.author_tags:
            return get_new_authors_list()
        else:
            return super().get_value_for_field(field, default=default)

    def get_required_fields(self) -> Iterable[str]:
        yield from [
            field
            for field in self._meta.fields
            if field not in ("subtitle", "data", "loc", "dewey")
        ]

    def numeric_year(self) -> int:
        return to_int(self.year)

    def add_tag(self, tag: adt.ADT) -> None:
        if self.tags is None:
            self.tags = [tag]
        else:
            self.tags = self.tags + (tag,)

    def has_tag(self, tag_cls: type[adt.ADT]) -> bool:
        tag_id = tag_cls._tag
        for tag in self.get_raw_tags_field("tags"):
            if tag[0] == tag_id:
                return True
        return False

    def __repr__(self) -> str:
        authors = ", ".join(map(str, self.get_authors()))
        return ", ".join(
            str(piece)
            for piece in [
                authors,
                self.year,
                self.title,
                self.subtitle,
                self.publisher,
                self.isbn,
            ]
            if piece is not None
        )


class BookTag(adt.ADT):
    Language(language=SourceLanguage, tag=1)  # type: ignore
    OriginalLanguage(language=SourceLanguage, tag=2)  # type: ignore
    BookEdition(text=str, tag=3)  # type: ignore


def sort_key(book: Book) -> tuple[str, ...]:
    return (
        book.dewey or "",
        ", ".join(repr(author.sort_key()) for author in book.get_authors()),
        book.year or "",
        book.title or "",
    )


def print_prefix(prefix: str) -> None:
    books = Book.select_valid().filter(Book.dewey.startswith(prefix))
    for book in sorted(books, key=sort_key):
        print(book.dewey, repr(book))
