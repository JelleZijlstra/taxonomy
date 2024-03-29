import builtins
import functools
import re
import subprocess
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import date
from typing import Any, NotRequired, TypeVar

from peewee import BooleanField, CharField, ForeignKeyField, IntegrityError

from taxonomy.apis import bhl
from taxonomy.apis.cloud_search import SearchField, SearchFieldType

from ... import adt, config, events, getinput
from .. import constants, helpers, models
from ..derived_data import DerivedField, LazyType
from .base import ADTField, BaseModel, EnumField, LintConfig
from .region import Region

CGTagT = TypeVar("CGTagT", bound="CitationGroupTag")


class CitationGroup(BaseModel):
    creation_event = events.Event["CitationGroup"]()
    save_event = events.Event["CitationGroup"]()
    label_field = "name"
    grouping_field = "type"
    call_sign = "CG"
    excluded_fields = {"tags", "archive"}

    name = CharField()
    region = ForeignKeyField(Region, related_name="citation_groups", null=True)
    deleted = BooleanField(default=False)
    type = EnumField(constants.ArticleType)
    target = ForeignKeyField("self", related_name="redirects", null=True)
    tags = ADTField(lambda: CitationGroupTag, null=True, is_ordered=False)
    archive = CharField(null=True)

    class Meta:
        db_table = "citation_group"

    derived_fields = [
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

    search_fields = [
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
        return query.filter(CitationGroup.deleted == False)

    def should_skip(self) -> bool:
        return self.deleted

    def get_redirect_target(self) -> "CitationGroup | None":
        return self.target

    def is_invalid(self) -> bool:
        return self.deleted or self.target is not None

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
        if not self.tags:
            return
        num_bhl_biblios = 0
        for tag in self.tags:
            if tag is CitationGroupTag.MustHave or isinstance(
                tag, CitationGroupTag.MustHaveAfter
            ):
                if (
                    not self.archive
                    and not self.get_tag(CitationGroupTag.CitationGroupURL)
                    and not self.get_tag(CitationGroupTag.BHLBibliography)
                ):
                    yield f"{self}: has MustHave tag but no URL"
            if isinstance(tag, CitationGroupTag.MustHaveAfter):
                if issue := helpers.is_valid_year(tag.year):
                    yield f"{self}: invalid MustHaveAfterTag {tag}: {issue}"
            if isinstance(tag, CitationGroupTag.MustHaveSeries) and not self.get_tag(
                CitationGroupTag.SeriesRegex
            ):
                yield f"{self}: MustHaveSeries tag but no SeriesRegex tag"
            if isinstance(tag, CitationGroupTag.OnlineRepository):
                yield f"{self}: use of deprecated OnlineRepository tag"
            if isinstance(tag, (CitationGroupTag.ISSN, CitationGroupTag.ISSNOnline)):
                # TODO check that the checksum digit is right
                if not re.fullmatch(r"^\d{4}-\d{3}[X\d]$", tag.text):
                    yield f"{self}: invalid ISSN {tag}"
            if isinstance(tag, CitationGroupTag.BHLBibliography):
                if not tag.text.isnumeric():
                    yield f"{self}: invalid BHL tag {tag}"
                num_bhl_biblios += 1
            if isinstance(tag, CitationGroupTag.YearRange):
                if issue := helpers.is_valid_year(tag.start):
                    yield f"{self}: invalid start year in {tag}: {issue}"
                if tag.end and (issue := helpers.is_valid_year(tag.end)):
                    yield f"{self}: invalid end year in {tag}: {issue}"
                if tag.start and tag.end and int(tag.start) > int(tag.end):
                    yield f"{self}: {tag}: start is after end"
                if tag.end and int(tag.end) > date.today().year:
                    yield f"{self}: {tag} is predicting the future"
            if isinstance(tag, CitationGroupTag.BiblioNote):
                if tag.text not in get_biblio_pages():
                    yield f"{self}: references non-existent page {tag.text!r}"
            # TODO if there is a Predecessor, check that the YearRange tags make sense
            if isinstance(
                tag,
                (
                    CitationGroupTag.SeriesRegex,
                    CitationGroupTag.VolumeRegex,
                    CitationGroupTag.IssueRegex,
                ),
            ):
                if issue := helpers.is_valid_regex(tag.text):
                    yield f"{self}: invalid tag {tag}: {issue}"
            if isinstance(tag, CitationGroupTag.PageRegex):
                if tag.start_page_regex is not None:
                    if issue := helpers.is_valid_regex(tag.start_page_regex):
                        yield f"{self}: invalid start_page_regex in tag {tag}: {issue}"
                if tag.pages_regex is not None:
                    if issue := helpers.is_valid_regex(tag.pages_regex):
                        yield f"{self}: invalid pages_regex in tag {tag}: {issue}"

        tags = sorted(set(self.tags))
        counts = Counter(type(tag) for tag in tags)
        for tag_type, count in counts.items():
            if count > 1 and tag_type not in (
                CitationGroupTag.Predecessor,
                CitationGroupTag.CitationGroupURL,
                CitationGroupTag.ISSN,
                CitationGroupTag.ISSNOnline,
                CitationGroupTag.BHLBibliography,
            ):
                yield f"{self}: multiple {tag_type} tags"

        if tuple(tags) != tuple(self.tags):
            message = f"{self}: changing tags"
            getinput.print_diff(sorted(self.tags), tags)
            if cfg.autofix:
                print(message)
                self.tags = tags  # type: ignore
            else:
                yield message

        if num_bhl_biblios > 5:
            yield f"{self}: has {num_bhl_biblios} BHL bibliographies"

        if not num_bhl_biblios:
            yield from self.infer_bhl_biblio(cfg)
        if not self.has_tag(CitationGroupTag.SkipExtraBHLBibliographies):
            yield from self.infer_bhl_biblio_from_children(cfg)

    def infer_bhl_biblio_from_children(self, cfg: LintConfig) -> Iterable[str]:
        if self.type is not constants.ArticleType.JOURNAL:
            return
        bibliographies: dict[int, list[object]] = defaultdict(list)
        for nam in self.get_names():
            for tag in nam.get_tags(
                nam.type_tags, models.name.TypeTag.AuthorityPageLink
            ):
                if biblio := bhl.get_bhl_bibliography_from_url(tag.url):
                    bibliographies[biblio].append(nam)
        for art in self.get_articles():
            if art.url:
                if biblio := bhl.get_bhl_bibliography_from_url(art.url):
                    bibliographies[biblio].append(art)
        if not bibliographies:
            return
        existing = self.get_bhl_title_ids()
        for biblio in existing:
            bibliographies.pop(biblio, None)
        if not bibliographies:
            return
        message = (
            f"{self}: inferred BHL tags {bibliographies} "
            f"from child articles and names"
        )
        if cfg.autofix:
            print(message)
            for biblio in bibliographies:
                self.add_tag(CitationGroupTag.BHLBibliography(text=str(biblio)))
        else:
            yield message

    def infer_bhl_biblio(
        self, cfg: LintConfig, interactive_mode: bool = False
    ) -> Iterable[str]:
        if self.type is not constants.ArticleType.JOURNAL:
            return
        title_dict = bhl.get_title_to_data()
        name = self.name.casefold()
        if name not in title_dict:
            return
        candidates = title_dict[name]
        if len(candidates) > 1:
            urls = [cand["TitleURL"] for cand in candidates]
            message = f"{self}: multiple possible BHL entries: {urls}"
            if interactive_mode:
                getinput.print_header(self)
                print(message)

                def open_all() -> None:
                    for cand in candidates:
                        subprocess.check_call(["open", cand["TitleURL"]])

                data = getinput.choose_one(
                    candidates,
                    callbacks={**self.get_adt_callbacks(), "open_all": open_all},
                    history_key=(self, "infer_bhl_biblio"),
                )
                if data is None:
                    return
                # help pyanalyze, which picks "object" as the type otherwise
                assert isinstance(data, dict)
            else:
                return
        else:
            data = candidates[0]
            active_years = self.get_active_year_range()
            if active_years is None:
                message = f"{self}: no active years, but may match {data['TitleURL']}"
                if interactive_mode:
                    print(message)
                    subprocess.check_call(["open", data["TitleURL"]])
                    if not getinput.yes_no(
                        "Accept anyway? ", callbacks=self.get_adt_callbacks()
                    ):
                        return
                else:
                    yield message
                return
            my_start_year, my_end_year = active_years
            if not data["StartYear"]:
                return
            if my_start_year < int(data["StartYear"]) or (
                data["EndYear"] and my_end_year > int(data["EndYear"])
            ):
                yield f"{self}: active years {my_start_year}-{my_end_year} don't match {data['TitleURL']} {data['StartYear']}-{data['EndYear']}"
                return
        message = f"{self}: inferred BHL tag {data['TitleID']}"
        if cfg.autofix:
            print(message)
            self.add_tag(CitationGroupTag.BHLBibliography(text=str(data["TitleID"])))
        else:
            yield message

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
            self.tags = self.tags + (tag,)

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
            name = name.reload()
            name.fill_field_if_empty(field)

    def for_years(
        self,
        start_year: int,
        end_year: int | None = None,
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
        self, depth: int = 0, full: bool = False, include_articles: bool = False
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

    def delete(self) -> None:
        assert (
            len(self.get_names()) == 0
        ), f"cannot delete {self} because it contains names"
        assert (
            self.get_articles().count() == 0
        ), f"cannot delete {self} because it contains articles"
        self.deleted = True

    def edit(self) -> None:
        self.fill_field("tags")

    def edit_all_members(self) -> None:
        for nam in self.get_names():
            nam.display()
            nam.edit()
        for art in self.get_articles():
            art.display()
            art.edit()

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
            "lint_articles": lambda: models.Article.lint_all(query=self.get_articles()),
            "lint_names": lambda: models.Name.lint_all(
                query=models.Name.add_validity_check(self.names)
            ),
            "missing_high_names": self.print_missing_high_names,
            "for_years": self._for_years_interactive,
            "fill_field_for_names": self.fill_field_for_names,
            "interactively_add_bhl_urls": self.interactively_add_bhl_urls,
            "validate_bhl_urls": self.validate_bhl_urls,
        }

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
        self.type = constants.ArticleType.REDIRECT  # type: ignore

    def add_alias(self) -> "CitationGroup | None":
        alias_name = self.getter("name").get_one_key("alias> ")
        if alias_name is None:
            return None
        return CitationGroup.create(
            name=alias_name, type=constants.ArticleType.REDIRECT, target=self
        )

    def get_books(self) -> Any:
        return models.Book.select_valid().filter(models.Book.citation_group == self)

    def get_articles(self) -> Any:
        return models.Article.select_valid().filter(
            models.Article.citation_group == self
        )

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
        for art in self.get_articles():
            if art.doi is not None:
                continue
            for _ in models.article.lint.infer_bhl_page(art, interactive_mode=True):
                pass

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

    pattern = CharField(null=False)
    citation_group = ForeignKeyField(CitationGroup, related_name="patterns", null=False)

    class Meta:
        db_table = "citation_group_pattern"

    @classmethod
    def make(
        cls, pattern: str, citation_group: CitationGroup
    ) -> "CitationGroupPattern":
        pattern = helpers.simplify_string(pattern)
        try:
            return cls.create(pattern=pattern, citation_group=citation_group)
        except IntegrityError:
            existing = cls.get(pattern=pattern)
            if existing.citation_group != citation_group:
                raise ValueError(
                    f"Conflicting CG for existing pattern: {existing.citation_group}"
                ) from None
            return existing


@functools.cache
def get_biblio_pages() -> set[str]:
    options = config.get_options()
    biblio_dir = options.taxonomy_repo / "docs" / "biblio"
    return {path.stem for path in biblio_dir.glob("*.md")}


class CitationGroupTag(adt.ADT):
    # Must have articles for all citations in this group
    MustHave(tag=1)  # type: ignore
    # Ignore in find_potential_citations()
    IgnorePotentialCitations(tag=2)  # type: ignore
    # Like MustHave, but only for articles published after this year
    MustHaveAfter(tag=3, year=str)  # type: ignore
    # Articles in this citation group must have a series set.
    MustHaveSeries(comment=NotRequired[str], tag=11)  # type: ignore
    # Information on where to find it.
    OnlineRepository(url=str, comment=NotRequired[str], tag=12)  # type: ignore
    ISSN(text=str, tag=13)  # type: ignore
    BHLBibliography(text=str, tag=14)  # type: ignore
    # ISSN for online edition
    ISSNOnline(text=str, tag=15)  # type: ignore
    CitationGroupURL(text=str, tag=16)  # type: ignore
    # The journal existed during this period
    YearRange(start=str, end=NotRequired[str], tag=17)  # type: ignore
    # If a journal got renamed, a reference to the previous name
    Predecessor(cg=CitationGroup, tag=18)  # type: ignore
    # Series may be present and must conform to the regex in the tag
    SeriesRegex(text=str, tag=19)  # type: ignore
    # Volumes must conform to this regex
    VolumeRegex(text=str, tag=20)  # type: ignore
    # Issues must conform to this regex
    IssueRegex(text=str, tag=21)  # type: ignore
    # Control start and end page (see citation-group.md)
    PageRegex(start_page_regex=NotRequired[str], pages_regex=NotRequired[str], allow_standard=bool, tag=22)  # type: ignore
    # Comments on how to date publications in this journal
    DatingTools(text=str, tag=23)  # type: ignore
    # Link to a relevant page in docs/biblio/
    BiblioNote(text=str, tag=24)  # type: ignore
    # Articles must have a month or day in publication date
    # (only enforced for articles containing new names)
    MustHavePreciseDate(tag=25)  # type: ignore
    # Articles must have a URL (or DOI, HDL, etc.)
    MustHaveURL(tag=26)  # type: ignore
    URLPattern(text=str, tag=27)  # type: ignore
    # Do not add more BHL bibliographies based on children
    SkipExtraBHLBibliographies(tag=28)  # type: ignore
    CitationGroupComment(text=str, tag=29)  # type: ignore
