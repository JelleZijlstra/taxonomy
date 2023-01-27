from __future__ import annotations
from bs4 import BeautifulSoup
import builtins
import datetime
from functools import lru_cache
import os
from pathlib import Path
from peewee import (
    CharField,
    DeferredForeignKey,
    ForeignKeyField,
    IntegerField,
    TextField,
)
import pprint
import requests
import shutil
import subprocess
import time
from typing import Any, ClassVar, NamedTuple, TypeVar, cast
from collections.abc import Callable, Iterable

from ..base import (
    ADTField,
    BaseModel,
    EnumField,
    get_completer,
    get_tag_based_derived_field,
)
from ...constants import (
    ArticleCommentKind,
    ArticleKind,
    ArticleType,
    SourceLanguage,
    DateSource,
)
from ...helpers import to_int, clean_strings_recursively, get_date_object, is_valid_date
from ... import models
from .... import config, events, adt, getinput, uitools

from ..citation_group import CitationGroup
from ..person import AuthorTag, Person, PersonLevel, get_new_authors_list
from .folder_tree import FolderTree

T = TypeVar("T", bound="Article")

_options = config.get_options()
_TYPE_TO_FIELDS = {
    ArticleType.JOURNAL: [
        "author_tags",
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
        "author_tags",
        "year",
        "title",
        "start_page",
        "end_page",
        "parent",
        "url",
    ],
    ArticleType.BOOK: [
        "author_tags",
        "year",
        "title",
        "pages",
        "publisher",
        "citation_group",
    ],
    ArticleType.THESIS: [
        "author_tags",
        "year",
        "title",
        "pages",
        "citation_group",
        "series",
    ],
    ArticleType.SUPPLEMENT: ["title", "parent"],
    ArticleType.WEB: ["author_tags", "year", "title", "url"],
    ArticleType.MISCELLANEOUS: ["author_tags", "year", "title", "url"],
}


class LsFile(NamedTuple):
    name: str
    path: list[str]


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
    author_tags = ADTField(lambda: AuthorTag, null=True)
    year = CharField(null=True)  # year published
    # title (chapter title for book chapter; book title for full book or thesis)
    title = CharField(null=True)
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

    path = CharField(null=True)  # path to file (contains None if "file" is not a file)
    ids = CharField(
        null=True
    )  # array of properties for various less-common identifiers
    bools = CharField(null=True)  # array of boolean flags
    kind = EnumField(ArticleKind)
    parent = DeferredForeignKey("Article", null=True)
    tags = ADTField(lambda: ArticleTag, null=True)
    citation_group = ForeignKeyField(CitationGroup, null=True)

    folder_tree: ClassVar[FolderTree] = FolderTree()
    save_event.on(folder_tree.add)
    derived_fields = [
        get_tag_based_derived_field(
            "partially_suppressed_names",
            lambda: models.Name,
            "tags",
            lambda: models.NameTag.PartiallySuppressedBy,
            1,
        ),
        get_tag_based_derived_field(
            "fully_suppressed_names",
            lambda: models.Name,
            "tags",
            lambda: models.NameTag.FullySuppressedBy,
            1,
        ),
        get_tag_based_derived_field(
            "conserved_names",
            lambda: models.Name,
            "tags",
            lambda: models.NameTag.Conserved,
            1,
        ),
        get_tag_based_derived_field(
            "spelling_selections",
            lambda: models.Name,
            "tags",
            lambda: models.NameTag.SelectionOfSpelling,
            2,
        ),
        get_tag_based_derived_field(
            "priority_selections",
            lambda: models.Name,
            "tags",
            lambda: models.NameTag.SelectionOfPriority,
            2,
        ),
        get_tag_based_derived_field(
            "priority_reversals",
            lambda: models.Name,
            "tags",
            lambda: models.NameTag.ReversalOfPriority,
            1,
        ),
        get_tag_based_derived_field(
            "type_designations",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.TypeDesignation,
            1,
        ),
        get_tag_based_derived_field(
            "commission_type_designations",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.CommissionTypeDesignation,
            1,
        ),
        get_tag_based_derived_field(
            "lectotype_designations",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.LectotypeDesignation,
            1,
        ),
        get_tag_based_derived_field(
            "neotype_designations",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.NeotypeDesignation,
            1,
        ),
        get_tag_based_derived_field(
            "specimen_details",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.SpecimenDetail,
            2,
        ),
        get_tag_based_derived_field(
            "location_details",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.LocationDetail,
            2,
        ),
        get_tag_based_derived_field(
            "collection_details",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.CollectionDetail,
            2,
        ),
        get_tag_based_derived_field(
            "citation_details",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.CitationDetail,
            2,
        ),
        get_tag_based_derived_field(
            "definition_details",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.DefinitionDetail,
            2,
        ),
        get_tag_based_derived_field(
            "etymology_details",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.EtymologyDetail,
            2,
        ),
        get_tag_based_derived_field(
            "type_species_details",
            lambda: models.Name,
            "type_tags",
            lambda: models.TypeTag.TypeSpeciesDetail,
            2,
        ),
        get_tag_based_derived_field(
            "biographies",
            lambda: models.Person,
            "tags",
            lambda: models.tags.PersonTag.Biography,
            1,
        ),
    ]

    @property
    def place_of_publication(self) -> str | None:
        if self.type not in (ArticleType.BOOK, ArticleType.WEB):
            return None
        if self.citation_group is not None:
            return self.citation_group.name
        else:
            return None

    @property
    def institution(self) -> str | None:
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
        return query.filter(
            cls.kind != ArticleKind.redirect, cls.kind != ArticleKind.removed
        )

    def get_redirect_target(self) -> Article | None:
        if (
            self.kind is ArticleKind.redirect
            or self.kind is ArticleKind.alternative_version
        ):
            return self.parent
        return None

    def is_invalid(self) -> bool:
        return self.kind in (ArticleKind.redirect, ArticleKind.removed)

    def should_skip(self) -> bool:
        return self.kind is ArticleKind.redirect

    def edit(self) -> None:
        self.fill_field("tags")

    def edittitle(self) -> None:
        def save_handler(new_title: str, full: bool = True) -> None:
            self.title = new_title
            self.format()
            print("New title: " + self.title)
            self.edit_until_clean()

        return uitools.edittitle(
            self.title or "",
            save_handler=save_handler,
            callbacks=[
                uitools.Callback("o", "Open this file", self.openf),
                uitools.Callback("f", "Edit this file", self.edit),
            ],
            get_title=lambda: self.title or "",
        )

    @classmethod
    def bfind(
        cls,
        *args: Any,
        quiet: bool = False,
        sort_key: Callable[[Article], Any] | None = None,
        journal: str | None = None,
        **kwargs: Any,
    ) -> list[Article]:
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
            "specify_authors": self.specify_authors,
            "recompute_authors_from_doi": self.recompute_authors_from_doi,
            "recompute_authors_from_jstor": self.recompute_authors_from_jstor,
            "print_doi_information": self.print_doi_information,
            "expand_doi": lambda: self.expand_doi(verbose=True, set_fields=True),
            "display_names": self.display_names,
            "display_type_localities": self.display_type_localities,
            "modernize_in_press": self.modernize_in_press,
            "open_url": self.openurl,
            "remove": self.remove,
            "merge": self.merge,
            "make_alternative_version": self.make_alternative_version,
            "add_child": self.add_child,
            "edittitle": self.edittitle,
            "change_folder": self.change_folder,
            "move": self.move,
            "add_to_clipboard": self.add_to_clipboard,
            "removefirstpage": self.removefirstpage,
        }

    def modernize_in_press(self) -> None:
        self.year = None
        self.volume = None
        self.issue = None
        self.start_page = None
        self.end_page = None
        self.expand_doi(verbose=True, set_fields=True)

    def full_data(self) -> None:
        """Provide information for a file."""
        super().full_data()
        if self.kind == ArticleKind.electronic:
            subprocess.call(["ls", "-l", str(self.get_path())])

    def add_to_clipboard(self) -> None:
        getinput.add_to_clipboard(self.name)

    def remove(self, force: bool = False) -> None:
        """Remove a file. If force is True, do not ask for confirmation."""
        if not force:
            if not getinput.yes_no(
                f"Are you sure you want to remove file {self.name}?"
            ):
                return
        if self.kind == ArticleKind.electronic and self.path:
            os.unlink(self.get_path())
        print(f"File {self.name} removed.")
        self.kind = ArticleKind.removed  # type: ignore

    def merge(self, target: Article | None = None, force: bool = False) -> None:
        """Merges this file into another file."""
        if target is None:
            target = self.getter(None).get_one("merge target> ")
        if target is None:
            return
        if self == target:
            print("Can't merge into yourself")
            return
        if self.kind == ArticleKind.electronic:
            if not force:
                if not getinput.yes_no(
                    "Are you sure you want to remove the electronic copy of"
                    f" {self.name}?"
                ):
                    return
            os.unlink(self.get_path())
        self.kind = ArticleKind.redirect  # type: ignore
        self.path = None
        self.parent = target

    def make_alternative_version(self, target: Article | None = None) -> None:
        """Make this version into an alternative version of another file."""
        if target is None:
            target = self.getter(None).get_one("target> ")
        if target is None:
            return
        if self == target:
            print("Can't merge into yourself")
            return
        self.kind = ArticleKind.alternative_version  # type: ignore
        self.parent = target

    def change_folder(self) -> None:
        if self.kind is not ArticleKind.electronic:
            return
        old_path = self.get_path()
        if not models.article.set_path.folder_suggestions(self, allow_skip=True):
            return
        new_path = self.get_path()
        if old_path != new_path:
            subprocess.check_call(["mv", "-n", str(old_path), str(new_path)])

    def move(self, newname: str | None = None) -> None:
        while newname is None:
            newname = self.getter("name").get_one_key(
                default=self.name, prompt="New name: "
            )
            if newname is None:
                return
            if self.has(newname):
                print(f"New name already exists: {newname}")
                newname = None
        oldname = self.name
        if oldname == newname:
            return
        if self.kind is ArticleKind.electronic:
            oldpath = self.get_path()
            # change the name internally
            self.name = newname
            # Move the physical file first. This may fail if the physical file was moved to a different
            # directory, so do it first lest we leave the catalog in an inconsistent state.
            try:
                newpath = self.get_path()
                subprocess.check_call(["mv", "-n", str(oldpath), str(newpath)])
            except BaseException:
                self.name = oldname
                raise
        else:
            self.name = newname
        # make redirect
        self.create_redirect_static(oldname, self)

    def removefirstpage(self) -> bool:
        temp_path = self.get_path().parent / "tmp.pdf"
        path = self.get_path()
        subprocess.check_call(
            [
                "gs",
                "-dBATCH",
                "-dNOPAUSE",
                "-q",
                "-sDEVICE=pdfwrite",
                "-dFirstPage=2",
                f"-sOUTPUTFILE={temp_path}",
                str(path),
            ]
        )
        # open files for review
        subprocess.check_call(["open", str(temp_path)])
        self.openf()
        if getinput.yes_no("Do you want to replace the file?"):
            shutil.move(str(temp_path), str(path))
            return True
        else:
            os.unlink(temp_path)
            return False

    def get_value_to_show_for_field(self, field: str | None) -> str:
        if field is None:
            return self.name
        return getattr(self, field)

    def get_value_for_field(self, field: str, default: str | None = None) -> Any:
        if field == "author_tags" and not self.author_tags:
            return get_new_authors_list()
        else:
            return super().get_value_for_field(field, default=default)

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

    def get_completers_for_adt_field(self, field: str) -> getinput.CompleterMap:
        for field_name, tag_cls in [("author_tags", AuthorTag)]:
            if field == field_name:
                completers: dict[
                    tuple[type[adt.ADT], str], getinput.Completer[Any]
                ] = {}
                for tag in tag_cls._tag_to_member.values():
                    for attribute, typ in tag._attributes.items():
                        completer: getinput.Completer[Any] | None
                        if typ is Article:
                            completer = get_completer(Article, "name")
                        elif typ is Person:
                            completer = get_completer(Person, None)
                        else:
                            completer = None
                        if completer is not None:
                            completers[(tag, attribute)] = completer
                return completers
        return {}

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

    def path_list(self) -> list[str]:
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
        if self.kind not in (ArticleKind.electronic, ArticleKind.alternative_version):
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
        if self.kind == ArticleKind.electronic:
            return True
        if self.parent is not None:
            return self.parent.isfile()
        return False

    def isnofile(self) -> bool:
        return not self.isfile()

    def issupplement(self) -> bool:
        return self.type == ArticleType.SUPPLEMENT

    def isredirect(self) -> bool:
        return self.kind == ArticleKind.redirect

    def is_in_press(self) -> bool:
        return self.start_page == "in press"

    def is_full_issue(self) -> bool:
        return any(self.get_tags(self.tags, ArticleTag.FullIssue))

    def numeric_year(self) -> int:
        return self.get_date_object().year

    def get_date_object(self) -> datetime.date:
        return get_date_object(self.year)

    def valid_numeric_year(self) -> int | None:
        if is_valid_date(self.year):
            return self.numeric_year()
        else:
            return None

    def numeric_start_page(self) -> int:
        return to_int(self.start_page)

    def numeric_end_page(self) -> int:
        return to_int(self.end_page)

    def ispdf(self) -> bool:
        return self.kind is ArticleKind.electronic and self.name.endswith(".pdf")

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

    def getpdfcontent(self) -> str:
        if not self.ispdf() or self.isredirect():
            raise ValueError(f"attempt to get PDF content for non-file {self}")
        return _getpdfcontent(str(self.get_path()))

    # Authors

    def get_authors(self) -> list[Person]:
        if self.type is ArticleType.SUPPLEMENT and self.parent is not None:
            return self.parent.get_authors()
        if self.author_tags is None:
            return []
        return [author.person for author in self.author_tags]

    def reverse_authors(self) -> None:
        authors = self.get_authors()
        self.author_tags = [
            AuthorTag.Author(person=person) for person in reversed(authors)  # type: ignore
        ]

    def taxonomicAuthority(self) -> tuple[str, str]:
        return (Person.join_authors(self.get_authors()), self.year or "")

    def author_set(self) -> set[int]:
        return {pair[1] for pair in self.get_raw_tags_field("author_tags")}

    def add_comment(
        self, kind: ArticleCommentKind | None = None, text: str | None = None
    ) -> ArticleComment | None:
        return ArticleComment.create_interactively(article=self, kind=kind, text=text)

    def add_misc_data(self, text: str) -> None:
        if self.misc_data is None:
            self.misc_data = text
        else:
            self.misc_data = f"{self.misc_data} {text}"

    def add_tag(self, tag: adt.ADT) -> None:
        if self.tags is None:
            self.tags = [tag]
        else:
            self.tags = self.tags + (tag,)

    def has_tag(self, tag_cls: ArticleTag._Constructor) -> bool:  # type: ignore
        tag_id = tag_cls._tag
        for tag in self.get_raw_tags_field("tags"):
            if tag[0] == tag_id:
                return True
        return False

    def geturl(self) -> str | None:
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

    def getIdentifier(self, identifier: builtins.type[adt.ADT]) -> str | None:
        for tag in self.get_tags(self.tags, identifier):
            if hasattr(tag, "text"):
                return tag.text
        return None

    def getEnclosing(self: T) -> T | None:
        if self.parent is not None:
            return cast(T, self.parent)
        else:
            return None

    def resolve_redirect(self) -> Article:
        if target := self.get_redirect_target():
            return target
        return self

    def concise_markdown_link(self) -> str:
        authors_list = self.get_authors()
        if len(authors_list) > 2:
            authors = f"{authors_list[0].taxonomic_authority()} et al."
        else:
            authors, _ = self.taxonomicAuthority()
        return f"[{authors} ({self.valid_numeric_year() or self.year})](/a/{self.id})"

    def markdown_link(self) -> str:
        cite = self.cite()
        return f"[{cite}](/a/{self.id})"

    def format(self, *, quiet: bool = False) -> bool:
        self.specify_authors()
        return super().format(quiet=quiet)

    def lint(self, autofix: bool = True) -> Iterable[str]:
        try:
            repr(self)
        except Exception as e:
            yield f"{self.id}: cannot display due to {e}"
            return
        if self.kind is ArticleKind.removed:
            return
        yield from models.article.lint.run_linters(self, autofix)

    def cite(self, citetype: str = "paper") -> str:
        if self.issupplement() and self.parent is not None:
            return self.parent.cite(citetype=citetype)
        if citetype in _CITE_FUNCTIONS:
            return _CITE_FUNCTIONS[citetype](self)
        else:
            raise ValueError(f"unknown citetype {citetype}")

    def finddoi(self) -> bool:
        if self.doi:
            return True
        if (
            self.type is ArticleType.JOURNAL
            and self.citation_group
            and self.volume
            and self.start_page
        ):
            print(f"Trying to find DOI for file {self.name}... ")
            query_dict = {
                "pid": _options.crossrefid,
                "title": self.citation_group.name,
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

    def print_doi_information(self) -> None:
        if not self.doi:
            return
        result = models.article.add_data.get_doi_json(self.doi)
        if result:
            pprint.pprint(result)

    def maybe_remove_corrupt_doi(self) -> None:
        if self.doi is None:
            return
        if not models.article.add_data.is_doi_valid(self.doi):
            print(f"{self}: remove invalid DOI: {self.doi}")
            self.add_misc_data(f"Removed invalid doi: {self.doi}.")
            self.doi = None

    def expand_doi(
        self, overwrite: bool = False, verbose: bool = False, set_fields: bool = True
    ) -> dict[str, Any]:
        if not self.doi:
            return {}
        data = models.article.add_data.expand_doi_json(self.doi)
        if set_fields:
            models.article.add_data.set_multi(
                self, data, only_new=not overwrite, verbose=verbose
            )
        return data

    def set_multi(self, data: dict[str, Any]) -> None:
        for key, value in clean_strings_recursively(data).items():
            self.set_from_raw(key, value)
        # Somehow this doesn't always autosave
        self.save()

    def set_from_raw(self, attr: str, value: Any) -> None:
        if attr == "author_tags":
            self.set_author_tags_from_raw(value)
        elif attr == "journal":
            self.citation_group = CitationGroup.get_or_create(value)
        elif attr in self.fields():
            setattr(self, attr, value)

    def set_author_tags_from_raw(
        self,
        value: Any,
        confirm_creation: bool = False,
        confirm_replacement: bool = False,
    ) -> None:
        for params in value:
            if params["family_name"].isupper():
                params["family_name"] = params["family_name"].title()
        if confirm_creation:
            for params in value:
                print(params)
            if not getinput.yes_no("Change authors? "):
                return
        new_tags = [
            AuthorTag.Author(person=Person.get_or_create_unchecked(**params))
            for params in value
        ]
        if self.author_tags is not None:
            if len(self.author_tags) == len(new_tags):
                new_tags = [
                    existing
                    if existing.person.is_more_specific_than(new.person)
                    else new
                    for existing, new in zip(self.author_tags, new_tags, strict=True)
                ]
            getinput.print_diff(self.author_tags, new_tags)
        if confirm_replacement:
            if not getinput.yes_no("Replace authors? "):
                self.fill_field("author_tags")
                return
        self.author_tags = new_tags  # type: ignore

    def specify_authors(
        self,
        level: PersonLevel | None = PersonLevel.initials_only,
        should_open: bool = True,
    ) -> None:
        if self.has_tag(ArticleTag.InitialsOnly):
            return
        opened = False
        for author in self.get_authors():
            if level is not None and author.get_level() is not level:
                continue
            if not opened and should_open:
                self.openf()
                opened = True
            author.edit_tag_sequence_on_object(
                self, "author_tags", AuthorTag.Author, "articles"
            )
        if level is not None:
            bad_authors = [
                author for author in self.get_authors() if author.get_level() is level
            ]
            if bad_authors:
                print(f"Remaining authors at level {level}: {bad_authors}")
                if getinput.yes_no("Add InitialsOnly tag? "):
                    self.add_tag(ArticleTag.InitialsOnly)
                else:
                    self.edit()

    def recompute_authors_from_jstor(
        self, confirm: bool = True, force: bool = False
    ) -> None:
        if not self.doi:
            return
        if not force and all(
            author.get_level() > PersonLevel.initials_only
            for author in self.get_authors()
        ):
            return
        data = models.article.add_data.get_jstor_data(self)
        self._recompute_authors_from_data(data, confirm)

    def recompute_authors_from_doi(
        self, confirm: bool = True, force: bool = False
    ) -> None:
        if not self.doi:
            return
        if not force and not all(
            author.get_level() >= PersonLevel.initials_only
            for author in self.get_authors()
        ):
            return
        data = models.article.add_data.expand_doi_json(self.doi)
        self._recompute_authors_from_data(data, confirm)

    def _recompute_authors_from_data(self, data: dict[str, Any], confirm: bool) -> None:
        if not data or "author_tags" not in data:
            print(f"Skipping because of no authors in {data}")
            return
        if not confirm and len(data["author_tags"]) != len(
            self.get_raw_tags_field("author_tags")
        ):
            print(f"Skipping because of length mismatch in {data}")
            return
        models.article.add_data.set_author_tags_from_raw(
            self, data["author_tags"], only_new=False, interactive=confirm
        )

    @classmethod
    def recompute_all_incomplete_authors(cls, limit: int | None = None) -> None:
        for art in (
            cls.select_valid().filter(cls.doi != None, cls.doi != "").limit(limit)
        ):
            if art.doi.startswith("10.2307/"):
                continue  # JSTOR dois aren't real
            authors = art.get_authors()
            if authors and all(
                author.get_level() is PersonLevel.initials_only for author in authors
            ):
                print(art)
                print(art.get_authors())
                art.recompute_authors_from_doi(confirm=False)
                getinput.flush()

    def display(self, full: bool = False) -> None:
        print(self.cite())

    def display_names(self, full: bool = False, organized: bool = True) -> None:
        print(repr(self))
        new_names = sorted(
            models.Name.add_validity_check(self.new_names),
            key=lambda nam: nam.numeric_page_described(),
        )
        if new_names:
            print(f"New names ({len(new_names)}):")
            if full or not organized:
                for nam in new_names:
                    nam.display(full=full)
            else:
                pairs = [(nam.get_description(), nam.taxon) for nam in new_names]
                models.taxon.display_organized(pairs)

    def display_type_localities(self) -> None:
        print(repr(self))
        new_names = sorted(
            models.Name.add_validity_check(self.new_names).filter(
                models.Name.type_locality != None
            ),
            key=lambda nam: nam.type_locality.name,
        )
        if new_names:
            print(f"New names ({len(new_names)}):")
            current_tl: models.Location | None = None
            for nam in new_names:
                if nam.type_locality != current_tl:
                    print(f"    {nam.type_locality!r}")
                    current_tl = nam.type_locality
                print(f"{' ' * 8}{nam.get_description()}", end="")

    def get_page_title(self) -> str:
        return self.cite()

    def __str__(self) -> str:
        return f"{{{self.name}}}"

    def __repr__(self) -> str:
        return f"{{{self.name}: {self.cite()}}}"

    def add_child(self) -> Article | None:
        return self.create_interactively(
            kind=ArticleKind.part, type=ArticleType.CHAPTER, parent=self
        )

    @classmethod
    def has(cls, file: str) -> bool:
        """Returns whether an entry of this name exists."""
        return cls.select().filter(name=file).count() > 0

    @classmethod
    def maybe_get(cls, file: str) -> Article | None:
        try:
            return cls.get(name=file)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_interactively(
        cls, name: str | None = None, **kwargs: Any
    ) -> Article | None:
        if name is None:
            name = cls.getter("name").get_one_key("name> ")
        if name is None:
            return None

        article = cls.create_nofile_static(name, **kwargs)
        if article is None:
            return None
        models.article.add_data.add_data_for_new_file(article)
        return article

    @classmethod
    def create_redirect(
        cls, name: str | None = None, target: Article | None = None
    ) -> Article | None:
        if name is None:
            name = cls.getter("name").get_one_key("name> ")
        if name is None:
            return None
        if cls.has(name):
            print(f"{name} already exists")
            return None
        if target is None:
            target = cls.getter("name").get_one("redirect target> ")
        if target is None:
            return None
        return cls.create_redirect_static(name, target)

    @classmethod
    def create_redirect_static(cls, name: str, target: Article) -> Article:
        return cls.make(
            name, kind=ArticleKind.redirect, type=ArticleType.REDIRECT, parent=target
        )

    @classmethod
    def create_nofile_static(cls, name: str, **kwargs: Any) -> Article | None:
        if cls.has(name):
            print(f"{name} already exists")
            return None
        if Path(name).suffix:
            print(f"{name}: NOFILE path may not have a suffix")
            return None
        kwargs.setdefault("kind", ArticleKind.no_copy)
        return cls.make(name, **kwargs)

    @classmethod
    def make(cls, name: str, **values: Any) -> Article:
        dt = datetime.datetime.now()
        return cls.create(
            name=name,
            addmonth=str(dt.month),
            addday=str(dt.day),
            addyear=str(dt.year),
            **values,
        )

    @classmethod
    def get_foldertree(cls) -> FolderTree:
        if not cls.folder_tree.is_empty():
            return cls.folder_tree
        for file in cls.select_valid():
            if file.path:
                cls.folder_tree.add(file)
        return cls.folder_tree


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
    ) -> ArticleComment:
        return cls.create(
            article=article, kind=kind, text=text, date=int(time.time()), **kwargs
        )

    @classmethod
    def create_interactively(
        cls,
        article: Article | None = None,
        kind: ArticleCommentKind | None = None,
        text: str | None = None,
        **kwargs: Any,
    ) -> ArticleComment:
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
CiterT = TypeVar("CiterT", bound=Citer)
_CITE_FUNCTIONS: dict[str, Citer] = {}


def register_cite_function(name: str) -> Callable[[CiterT], CiterT]:
    def decorator(citer: CiterT) -> CiterT:
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
    # TODO: Why does this exist? Should be on the CitationGroup
    ArticleISSN(text=str, tag=6)  # type: ignore
    PMC(text=str, tag=7)  # type: ignore

    # other
    Edition(text=str, tag=8)  # type: ignore
    FullIssue(comment=str, tag=9)  # type: ignore
    PartLocation(  # type: ignore
        parent=Article, start_page=int, end_page=int, comment=str, tag=10
    )
    NonOriginal(comment=str, tag=10)  # type: ignore
    # The article doesn't give full names for the authors
    InitialsOnly(tag=11)  # type: ignore
    # We can't fill_data_from_paper() because the article is in a language
    # I don't understand.
    NeedsTranslation(language=SourceLanguage, tag=12)  # type: ignore
    # Ignore lints with a specific label
    IgnoreLint(label=str, comment=str, tag=13)  # type: ignore

    PublicationDate(source=DateSource, date=str, comment=str, tag=14)  # type: ignore
    LSIDArticle(text=str, tag=15)  # type: ignore


@lru_cache
def _getpdfcontent(path: str) -> str:
    # only get first page
    return subprocess.run(
        ["pdftotext", path, "-", "-l", "1"], stdout=subprocess.PIPE
    ).stdout.decode("utf-8", "replace")
