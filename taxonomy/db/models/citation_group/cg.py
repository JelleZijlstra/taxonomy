import builtins
import enum
import sqlite3
import subprocess
from collections.abc import Iterable
from typing import Any, ClassVar, NotRequired, Self, TypeVar

from clirm import Field, Query

from taxonomy import adt, events, getinput
from taxonomy.apis import bhl
from taxonomy.apis.cloud_search import SearchField, SearchFieldType
from taxonomy.apis.zoobank import article_lsid_has_valid_data
from taxonomy.db import constants, helpers, models
from taxonomy.db.constants import URL, ArticleIdentifier, Managed, Markdown, Regex
from taxonomy.db.derived_data import DerivedField, LazyType
from taxonomy.db.models.base import ADTField, BaseModel, LintConfig
from taxonomy.db.models.region import Region

CGTagT = TypeVar("CGTagT", bound="CitationGroupTag")


class CitationGroupStatus(enum.Enum):
    normal = 0
    deleted = 1
    redirect = 2
    child = 3


class CitationGroup(BaseModel):
    creation_event = events.Event["CitationGroup"]()
    save_event = events.Event["CitationGroup"]()
    label_field = "name"
    grouping_field = "type"
    call_sign = "CG"
    excluded_fields: ClassVar[set[str]] = {"tags", "archive"}
    clirm_table_name = "citation_group"

    name = Field[str]()
    region = Field[Region | None]("region_id", related_name="citation_groups")
    status = Field[CitationGroupStatus]("deleted", default=CitationGroupStatus.normal)
    type = Field[constants.ArticleType]()
    target = Field[Self | None]("target_id", related_name="redirects")
    tags = ADTField["CitationGroupTag"](is_ordered=False)
    archive = Field[str | None]()

    derived_fields: ClassVar[list[DerivedField[Any]]] = [
        DerivedField(
            "ordered_names",
            LazyType(lambda: list[models.Name]),
            lambda cg: models.name.name.get_ordered_names(cg.names),
        ),
        DerivedField(
            "ordered_articles",
            LazyType(lambda: list[models.Article]),
            lambda cg: models.article.article.get_ordered_articles(cg.article_set),
        ),
    ]

    search_fields: ClassVar[list[SearchField]] = [
        SearchField(SearchFieldType.text, "name"),
        SearchField(SearchFieldType.literal, "type"),
        SearchField(SearchFieldType.text_array, "tags", highlight_enabled=True),
    ]

    def get_search_dicts(self) -> list[dict[str, Any]]:
        tags = []
        for tag in self.tags or ():
            if isinstance(tag, CitationGroupTag.OnlineRepository):
                tags.append(f"Repository: {tag.url}")
            elif isinstance(tag, CitationGroupTag.ISSN):
                tags.append(f"ISSN: {tag.text}")
            elif isinstance(tag, CitationGroupTag.BHLBibliography):
                tags.append(f"BHL: {tag.text}")
            elif isinstance(tag, CitationGroupTag.ISSNOnline):
                tags.append(f"ISSN (online): {tag.text}")
            elif isinstance(tag, CitationGroupTag.CitationGroupURL):
                tags.append(f"URL: {tag.text}")
            elif isinstance(tag, CitationGroupTag.DatingTools):
                tags.append(f"Dating tools: {tag.text}")
        data = {"name": self.name, "type": self.type.name, "tags": tags}
        return [data]

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(
            CitationGroup.status.is_in(
                (CitationGroupStatus.normal, CitationGroupStatus.child)
            )
        )

    def should_skip(self) -> bool:
        return self.status is CitationGroupStatus.deleted

    def get_redirect_target(self) -> "CitationGroup | None":
        if self.status is CitationGroupStatus.redirect:
            return self.target
        return None

    def is_invalid(self) -> bool:
        return self.status in (
            CitationGroupStatus.deleted,
            CitationGroupStatus.redirect,
        )

    @classmethod
    def create_interactively(
        cls, name: str | None = None, **kwargs: Any
    ) -> "CitationGroup":
        if name is None:
            name = cls.getter("name").get_one_key("name> ", allow_empty=False)
        obj = cls.create(name=name, **kwargs)
        obj.fill_required_fields()
        return obj

    @classmethod
    def get_or_create(cls, name: str) -> "CitationGroup | None":
        try:
            return cls.get(name=name)
        except cls.DoesNotExist:
            print(f"Failed to find a CitationGroup named {name}...")
            return cls.getter("name").get_one()

    @classmethod
    def get_or_create_city(cls, name: str) -> "CitationGroup | None":
        cg = cls.select_one(name=name, type=constants.ArticleType.BOOK)
        if cg is None:
            print(f"Creating CitationGroup for {name}")
            return cls.create_interactively(name, type=constants.ArticleType.BOOK)
        return cg

    def get_required_fields(self) -> Iterable[str]:
        yield "name"
        yield "type"
        if self.type not in (
            constants.ArticleType.ERROR,
            constants.ArticleType.REDIRECT,
        ):
            yield "region"

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        yield from models.citation_group.lint.LINT.run(self, cfg)

    @classmethod
    def clear_lint_caches(cls) -> None:
        models.citation_group.lint.LINT.clear_caches()

    def has_tag(self, tag: adt.ADT) -> bool:
        if self.tags is None:
            return False
        return any(my_tag is tag for my_tag in self.tags)

    def get_tag(self, tag_cls: builtins.type[CGTagT]) -> CGTagT | None:
        if self.tags is None:
            return None
        for tag in self.tags:
            if tag is tag_cls or isinstance(tag, tag_cls):
                return tag
        return None

    def add_tag(self, tag: adt.ADT) -> None:
        if self.tags is None:
            self.tags = [tag]
        else:
            self.tags = (*self.tags, tag)  # type: ignore[assignment]

    def apply_to_patterns(self) -> None:
        getinput.add_to_clipboard(self.name)
        while True:
            pattern = getinput.get_line("pattern to apply to> ")
            if not pattern:
                break
            if len(pattern) < 3:
                print(f"Pattern too short: {pattern}")
                continue
            self.add_for_pattern(pattern)

    def add_for_pattern(self, pattern: str) -> None:
        if getinput.yes_no("Apply pattern? "):
            for nam in models.Name.bfind(
                models.Name.verbatim_citation != None,
                models.Name.citation_group == None,
                models.Name.verbatim_citation % f"*{pattern}*",
            ):
                nam.display()
                nam.citation_group = self
        if getinput.yes_no("Save pattern? "):
            CitationGroupPattern.make(pattern=pattern, citation_group=self)

    def fill_field_for_names(self, field: str | None = None) -> None:
        if field is None:
            field = getinput.get_with_completion(
                models.Name.get_field_names(),
                message="field> ",
                history_key=(type(self), "fill_field_for_names"),
                disallow_other=True,
            )
        if field is None:
            return

        for name in sorted(
            self.get_names(),
            key=lambda nam: (nam.taxonomic_authority(), nam.year or ""),
        ):
            name.load()
            name.fill_field_if_empty(field)

    def for_years(
        self,
        start_year: int,
        end_year: int | None = None,
        *,
        author: str | None = None,
        include_articles: bool = False,
    ) -> list["models.Name"]:
        def condition(year: int) -> bool:
            if end_year is not None:
                return year in range(start_year, end_year)
            else:
                return year == start_year

        nams = self.get_names()
        nams = [
            nam
            for nam in nams
            if nam.original_citation is None and condition(nam.numeric_year())
        ]
        if author is not None:
            nams = [
                nam
                for nam in nams
                if any(author == person.family_name for person in nam.get_authors())
                and nam.original_citation is not None
            ]
        self._display_nams(nams)
        if include_articles:
            for art in sorted(
                self.get_articles(),
                key=lambda art: (
                    art.numeric_year(),
                    art.numeric_start_page(),
                    art.name,
                ),
            ):
                if condition(art.numeric_year()):
                    print(f"    {{{art.name}}}: {art.cite()}")
        return nams

    def _for_years_interactive(self) -> None:
        start_year_str = getinput.get_line(
            "start year> ", validate=str.isnumeric, allow_none=True
        )
        if start_year_str is None:
            return
        end_year_str = getinput.get_line(
            "end year> ", validate=str.isnumeric, allow_none=True
        )
        include_articles = getinput.yes_no("include articles? ")
        self.for_years(
            start_year=int(start_year_str),
            end_year=int(end_year_str) if end_year_str else None,
            include_articles=include_articles,
        )

    def display(
        self, *, depth: int = 0, full: bool = False, include_articles: bool = False
    ) -> None:
        nams = self.get_names()
        arts = list(self.get_articles())
        region_str = f"{self.region.name}; " if self.region else ""
        print(
            f"{' ' * depth}{self.name} ({region_str}{self.count_and_range(nams)}/{self.count_and_range(arts)})"
        )
        if full:
            self._display_nams(nams, depth=depth)
        if include_articles:
            for art in sorted(arts, key=lambda art: (art.numeric_year(), art.name)):
                print(f"{' ' * (depth + 4)}{{{art.name}}}: {art.cite()}")

    def count_and_range(self, objs: list[Any]) -> str:
        years = [obj.numeric_year() for obj in objs]
        years = [year for year in years if year != 0]
        if not objs:
            return "—"
        elif years:
            return f"{len(objs)}, {min(years)}—{max(years)}"
        else:
            return f"{len(objs)}"

    def get_citable_name(self) -> str:
        if self.target is not None:
            return self.target.get_citable_name()
        return self.name

    def delete(self) -> None:
        assert (
            len(self.get_names()) == 0
        ), f"cannot delete {self} because it contains names"
        assert (
            self.get_articles().count() == 0
        ), f"cannot delete {self} because it contains articles"
        self.status = CitationGroupStatus.deleted

    def edit(self) -> None:
        self.fill_field("tags")

    def edit_all_members(self) -> None:
        for nam in self.get_names():
            nam.display()
            nam.edit()
        for art in self.get_articles():
            art.display()
            art.edit()

    def has_lint_ignore(self, label: str) -> bool:
        return models.citation_group.lint.LINT.is_ignoring_lint(self, label)

    def get_adt_callbacks(self) -> getinput.CallbackMap:
        return {
            **super().get_adt_callbacks(),
            "delete": self.delete,
            "merge": self.merge_interactive,
            "display_organized": self.display_organized,
            "display_full": lambda: self.display(full=True, include_articles=True),
            "add_alias": self.add_alias,
            "edit_all_members": self.edit_all_members,
            "print_field_value_for_articles": self.print_field_value_for_articles,
            "lint_articles": lambda: self.lint_object_list(self.get_sorted_articles()),
            "lint_names": lambda: self.lint_object_list(self.get_names()),
            "missing_high_names": self.print_missing_high_names,
            "for_years": self._for_years_interactive,
            "fill_field_for_names": self.fill_field_for_names,
            "interactively_add_bhl_urls": self.interactively_add_bhl_urls,
            "validate_bhl_urls": self.validate_bhl_urls,
            "open_bhl_pages": self.open_bhl_pages,
            "make_child": self.make_child,
        }

    def open_url(self) -> None:
        for tag in self.tags:
            if isinstance(tag, CitationGroupTag.CitationGroupURL):
                subprocess.check_call(["open", tag.text])
            elif isinstance(tag, CitationGroupTag.BHLBibliography):
                url = f"https://www.biodiversitylibrary.org/bibliography/{tag.text}"
                subprocess.check_call(["open", url])

    def open_bhl_pages(self) -> None:
        for nam in self.get_names():
            nam.load()
            if not nam.has_type_tag(models.name.TypeTag.AuthorityPageLink):
                continue
            nam.open_url()
            nam.edit()

    def validate_bhl_urls(self) -> None:
        for art in self.get_articles():
            if art.url is None:
                continue
            art.display()
            if not bhl.print_data_for_possible_bhl_url(art.url):
                continue
            if not getinput.yes_no("Keep URL? ", callbacks=art.get_adt_callbacks()):
                art.url = None
        for nam in self.get_names():
            nam.display()
            for tag in nam.get_tags(
                nam.type_tags, models.name.TypeTag.AuthorityPageLink
            ):
                if not bhl.print_data_for_possible_bhl_url(tag.url):
                    continue
                if not getinput.yes_no("Keep URL? ", callbacks=nam.get_adt_callbacks()):
                    nam.remove_type_tag(tag)

    def make_child(self) -> None:
        target = self.getter(None).get_one("parent> ")
        if target is None:
            return
        self.target = target
        self.status = CitationGroupStatus.child

    def merge_interactive(self) -> None:
        other = self.getter(None).get_one("merge target> ")
        if other is None:
            return
        series = models.Article.getter("series").get_one_key("series (optional)> ")
        self.merge(other, series)

    def merge(self, other: "CitationGroup", series: str | None = None) -> None:
        if self == other:
            print("Cannot merge into yourself")
            return
        for nam in self.get_names():
            print(f"Changing CG on {nam}")
            nam.citation_group = other
        for art in self.get_articles():
            print(f"Changing CG on {art}")
            art.citation_group = other
            if series:
                if not art.series:
                    art.series = series
                else:
                    print(f"Warning: skipping {art} because it has series {art.series}")
        for book in self.get_books():
            print(f"Changing CG on {book}")
            book.citation_group = other
        if other.region is None and self.region is not None:
            print(f"Setting region: {self.region}")
            other.region = self.region
        self.target = other
        self.type = constants.ArticleType.REDIRECT
        self.status = CitationGroupStatus.redirect

    def add_alias(self) -> "CitationGroup | None":
        alias_name = self.getter("name").get_one_key("alias> ")
        if alias_name is None:
            return None
        return CitationGroup.create(
            name=alias_name, type=constants.ArticleType.REDIRECT, target=self
        )

    def get_books(self) -> Query["models.Book"]:
        return models.Book.select_valid().filter(models.Book.citation_group == self)

    def get_articles(self) -> Query["models.Article"]:
        return models.Article.select_valid().filter(
            models.Article.citation_group == self
        )

    def get_sorted_articles(self) -> list["models.Article"]:
        return sorted(self.get_articles(), key=models.article.article.volume_sort_key)

    def get_names(self) -> list["models.Name"]:
        names = models.Name.add_validity_check(self.names)
        return sorted(names, key=lambda nam: nam.sort_key())

    def is_year_in_range(self, year: int) -> str | None:
        year_range = self.get_tag(CitationGroupTag.YearRange)
        if not year_range:
            return None
        if year_range.start and year < int(year_range.start):
            return f"{year} is before start of {year_range} for {self}"
        if year_range.end and year > int(year_range.end):
            return f"{year} is after end of {year_range} for {self}"
        return None

    def get_active_year_range(self) -> tuple[int, int] | None:
        years: set[int] = set()
        for nam in self.get_names():
            year = nam.numeric_year()
            if year:
                years.add(year)
        for art in self.get_articles():
            year = art.numeric_year()
            if year:
                years.add(year)
        if tag := self.get_tag(CitationGroupTag.YearRange):
            if tag.start:
                years.add(int(tag.start))
            if tag.end:
                years.add(int(tag.end))
        if years:
            return min(years), max(years)
        return None

    def get_bhl_title_ids(self) -> list[int]:
        tags = self.get_tags(self.tags, CitationGroupTag.BHLBibliography)
        return [int(tag.text) for tag in tags]

    def get_issns(self) -> Iterable[str]:
        for tag in self.tags:
            if isinstance(tag, (CitationGroupTag.ISSN, CitationGroupTag.ISSNOnline)):
                yield tag.text

    def should_have_bhl_link_in_year(self, year: int) -> bool:
        if not self.get_tag(CitationGroupTag.BHLBibliography):
            return False
        if tag := self.get_tag(CitationGroupTag.BHLYearRange):
            if tag.start and year < int(tag.start):
                return False
            if tag.end and year > int(tag.end):
                return False
        return True

    def display_organized(self, depth: int = 0) -> None:
        region_str = f" ({self.region.name})" if self.region else ""
        print(f"{' ' * depth}{self.name}{region_str}")
        nams = [(repr(nam), nam.taxon) for nam in self.get_names()]
        models.taxon.display_organized(nams)

    def print_missing_high_names(self) -> None:
        self._display_nams(nam for nam in self.names if nam.is_high_mammal())

    def print_field_value_for_articles(self, field: str | None = None) -> None:
        if field is None:
            field = models.Article.prompt_for_field_name()
        if not field:
            return
        by_value: dict[str, list[models.Article]] = {}
        for art in self.get_articles():
            value = getattr(art, field)
            if value is not None:
                by_value.setdefault(value, []).append(art)
        for series, arts in sorted(by_value.items()):
            print(f"- {series} ({len(arts)})")

    def interactively_add_bhl_urls(self) -> None:
        cfg = LintConfig(manual_mode=True)
        arts = self.get_articles().filter(
            ~models.Article.url.contains("biodiversitylibrary.org/page/"),
            ~models.Article.url.contains("biodiversitylibrary.org/part/"),
        )
        for art in sorted(arts, key=models.article.article.volume_sort_key):
            # Bypass Article.format() so we don't have to set all the authors
            BaseModel.format(art, quiet=True)
            if models.article.lint.LINT.is_ignoring_lint(art, "must_have_bhl"):
                continue
            for _ in models.article.lint.infer_bhl_page(art, cfg):
                pass

    def may_have_article_identifier(
        self, article_identifier: ArticleIdentifier, year: int
    ) -> bool:
        for t in self.tags or ():
            if (
                isinstance(t, CitationGroupTag.MayHaveIdentifier)
                and t.identifier == article_identifier
                and (t.min_year is None or year >= t.min_year)
                and (t.max_year is None or year <= t.max_year)
            ):
                return True
        return False

    def must_have_article_identifier(
        self, article_identifier: ArticleIdentifier, year: int
    ) -> bool:
        for t in self.tags or ():
            if (
                isinstance(t, CitationGroupTag.MustHaveIdentifier)
                and t.identifier == article_identifier
                and (t.min_year is None or year >= t.min_year)
                and (t.max_year is None or year <= t.max_year)
            ):
                return True
        return False

    def get_invalid_lsids(self) -> Iterable[tuple[models.Article, str]]:
        for art in self.get_articles():
            for tag in art.get_tags(art.tags, models.article.ArticleTag.LSIDArticle):
                if not article_lsid_has_valid_data(tag.text):
                    yield art, tag.text

    def _display_nams(self, nams: Iterable["models.Name"], depth: int = 0) -> None:
        for nam in sorted(nams, key=lambda nam: nam.sort_key()):
            # Make it easier to see names that don't have a citation yet
            if nam.original_citation is not None:
                continue
            print(f"{' ' * (depth + 4)}{nam}")
            print(f"{' ' * (depth + 8)}{nam.verbatim_citation}")

    def __repr__(self) -> str:
        return (
            f"{self.name} ({self.type.name};"
            f" {self.region.name if self.region else '(unknown)'})"
        )


class CitationGroupPattern(BaseModel):
    label_field = "pattern"
    call_sign = "CGP"
    clirm_table_name = "citation_group_pattern"

    pattern = Field[str]()
    citation_group = Field[CitationGroup]("citation_group_id", related_name="patterns")

    @classmethod
    def make(
        cls, pattern: str, citation_group: CitationGroup
    ) -> "CitationGroupPattern":
        pattern = helpers.simplify_string(pattern)
        try:
            return cls.create(pattern=pattern, citation_group=citation_group)
        except sqlite3.IntegrityError:
            existing = cls.get(pattern=pattern)
            if existing.citation_group != citation_group:
                raise ValueError(
                    f"Conflicting CG for existing pattern: {existing.citation_group}"
                ) from None
            return existing


class CitationGroupTag(adt.ADT):
    # Must have articles for all citations in this group
    MustHave(tag=1)  # type: ignore[name-defined]
    # Ignore this CG in find_potential_citations()
    IgnorePotentialCitations(tag=2)  # type: ignore[name-defined]
    # Like MustHave, but only for articles published after this year
    MustHaveAfter(tag=3, year=Managed)  # type: ignore[name-defined]
    # Articles in this citation group must have a series set.
    MustHaveSeries(comment=NotRequired[Markdown], tag=11)  # type: ignore[name-defined]
    # Information on where to find it.
    OnlineRepository(url=URL, comment=NotRequired[Markdown], tag=12)  # type: ignore[name-defined]
    ISSN(text=Managed, tag=13)  # type: ignore[name-defined]
    BHLBibliography(text=Managed, tag=14)  # type: ignore[name-defined]
    # ISSN for online edition
    ISSNOnline(text=Managed, tag=15)  # type: ignore[name-defined]
    CitationGroupURL(text=URL, tag=16)  # type: ignore[name-defined]
    # The journal existed during this period
    YearRange(start=Managed, end=NotRequired[Managed], tag=17)  # type: ignore[name-defined]
    # If a journal got renamed, a reference to the previous name
    Predecessor(cg=CitationGroup, tag=18)  # type: ignore[name-defined]
    # Series may be present and must conform to the regex in the tag
    SeriesRegex(text=Regex, tag=19)  # type: ignore[name-defined]
    # Volumes must conform to this regex
    VolumeRegex(text=Regex, tag=20)  # type: ignore[name-defined]
    # Issues must conform to this regex
    IssueRegex(text=Regex, tag=21)  # type: ignore[name-defined]
    # Control start and end page (see citation-group.md)
    PageRegex(start_page_regex=NotRequired[Regex], pages_regex=NotRequired[Regex], allow_standard=bool, tag=22)  # type: ignore[name-defined]
    # Comments on how to date publications in this journal
    DatingTools(text=Markdown, tag=23)  # type: ignore[name-defined]
    # Link to a relevant page in docs/biblio/
    BiblioNote(text=Managed, tag=24)  # type: ignore[name-defined]
    # Articles must have a month or day in publication date
    # (only enforced for articles containing new names)
    MustHavePreciseDate(tag=25)  # type: ignore[name-defined]
    # Articles must have a URL (or DOI, HDL, etc.)
    MustHaveURL(tag=26)  # type: ignore[name-defined]
    URLPattern(text=URL, tag=27)  # type: ignore[name-defined]
    # Do not add more BHL bibliographies based on children
    SkipExtraBHLBibliographies(tag=28)  # type: ignore[name-defined]
    CitationGroupComment(text=Markdown, tag=29)  # type: ignore[name-defined]
    # This exists mostly so we can avoid complaining about missing BHL links. Should
    # not be exposed on the website.
    BHLYearRange(start=NotRequired[Managed], end=NotRequired[Markdown], tag=30)  # type: ignore[name-defined]
    IgnoreLintCitationGroup(label=Managed, comment=NotRequired[Markdown], tag=31)  # type: ignore[name-defined]
    ArticleNumberRegex(text=Regex, tag=32)  # type: ignore[name-defined]

    # DOI data includes an article number but it should not be used as the primary identifier.
    ArticleNumberIsSecondary(comment=NotRequired[Markdown], tag=33)  # type: ignore[name-defined]

    MustHaveIdentifier(identifier=ArticleIdentifier, min_year=NotRequired[int], max_year=NotRequired[int], tag=34)  # type: ignore[name-defined]
    MayHaveIdentifier(identifier=ArticleIdentifier, min_year=NotRequired[int], max_year=NotRequired[int], tag=35)  # type: ignore[name-defined]
