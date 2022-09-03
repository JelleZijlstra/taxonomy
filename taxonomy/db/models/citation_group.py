from typing import Any, Iterable, List, Optional, Type

from peewee import BooleanField, CharField, ForeignKeyField, IntegrityError

from .. import constants, helpers, models
from ... import adt, events, getinput

from .base import BaseModel, EnumField, ADTField
from .region import Region


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
    tags = ADTField(lambda: CitationGroupTag, null=True)
    archive = CharField(null=True)

    class Meta(object):
        db_table = "citation_group"

    @classmethod
    def add_validity_check(cls, query: Any) -> Any:
        return query.filter(CitationGroup.deleted == False)

    def should_skip(self) -> bool:
        return self.deleted

    @classmethod
    def create_interactively(
        cls, name: Optional[str] = None, **kwargs: Any
    ) -> "CitationGroup":
        if name is None:
            name = cls.getter("name").get_one_key("name> ", allow_empty=False)
        obj = cls.create(name=name, **kwargs)
        obj.fill_required_fields()
        return obj

    @classmethod
    def get_or_create(cls, name: str) -> Optional["CitationGroup"]:
        try:
            return cls.get(name=name)
        except cls.DoesNotExist:
            print(f"Failed to find a CitationGroup named {name}...")
            return cls.getter("name").get_one()

    @classmethod
    def get_or_create_city(cls, name: str) -> Optional["CitationGroup"]:
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

    def has_tag(self, tag: adt.ADT) -> bool:
        if self.tags is None:
            return False
        return any(my_tag is tag for my_tag in self.tags)

    def get_tag(self, tag_cls: Type[adt.ADT]) -> Optional[adt.ADT]:
        if self.tags is None:
            return None
        for tag in self.tags:
            if tag is tag_cls or isinstance(tag, tag_cls):
                return tag
        return None

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

    def for_years(
        self,
        start_year: int,
        end_year: Optional[int] = None,
        author: Optional[str] = None,
        include_articles: bool = False,
    ) -> List["models.Name"]:
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

    def display(
        self, depth: int = 0, full: bool = True, include_articles: bool = False
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

    def count_and_range(self, objs: List[Any]) -> str:
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

    def merge(self, other: "CitationGroup", series: Optional[str] = None) -> None:
        for nam in self.get_names():
            print(f"Changing CG on {nam}")
            nam.citation_group = other
            nam.save()
        for art in self.get_articles():
            print(f"Changing CG on {art}")
            art.citation_group = other
            if series:
                if not art.series:
                    art.series = series
                else:
                    print(f"Warning: skipping {art} because it has series {art.series}")
            art.save()
        for book in self.get_books():
            print(f"Changing CG on {book}")
            book.citation_group = other
            book.save()
        if other.region is None and self.region is not None:
            print(f"Setting region: {self.region}")
            other.region = self.region
        self.target = other
        self.type = constants.ArticleType.REDIRECT  # type: ignore

    def get_books(self) -> Any:
        return models.Book.select_valid().filter(models.Book.citation_group == self)

    def get_articles(self) -> Any:
        return models.Article.select_valid().filter(
            models.Article.citation_group == self
        )

    def get_names(self) -> List["models.Name"]:
        names = self.names.filter(models.Name.status != constants.Status.removed)
        return sorted(names, key=lambda nam: nam.sort_key())

    def display_organized(self, depth: int = 0) -> None:
        region_str = f" ({self.region.name})" if self.region else ""
        print(f"{' ' * depth}{self.name}{region_str}")
        nams = [(repr(nam), nam.taxon) for nam in self.get_names()]
        models.taxon.display_organized(nams)

    def _display_nams(self, nams: Iterable["models.Name"], depth: int = 0) -> None:
        for nam in sorted(nams, key=lambda nam: nam.sort_key()):
            # Make it easier to see names that don't have a citation yet
            if nam.original_citation is not None:
                continue
            print(f"{' ' * (depth + 4)}{nam}")
            print(f"{' ' * (depth + 8)}{nam.verbatim_citation}")

    def __repr__(self) -> str:
        return f"{self.name} ({self.type.name}; {self.region.name if self.region else '(unknown)'})"


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
                )
            return existing


class CitationGroupTag(adt.ADT):
    # Must have articles for all citations in this group
    MustHave(tag=1)  # type: ignore
    # Ignore in find_potential_citations()
    IgnorePotentialCitations(tag=2)  # type: ignore
    # Like MustHave, but only for articles published after this year
    MustHaveAfter(tag=3, year=str)  # type: ignore
    # Articles in this citation group must have a series set.
    MustHaveSeries(comment=str, tag=11)  # type: ignore
    # Information on where to find it.
    OnlineRepository(url=str, comment=str, tag=12)  # type: ignore
