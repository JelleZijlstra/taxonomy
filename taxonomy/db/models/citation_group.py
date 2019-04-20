from typing import Any, Iterable, List, Optional

from peewee import BooleanField, CharField, ForeignKeyField

from .. import constants, models
from ... import events, getinput

from .base import BaseModel, EnumField
from .region import Region


class CitationGroup(BaseModel):
    creation_event = events.Event["CitationGroup"]()
    save_event = events.Event["CitationGroup"]()
    label_field = "name"
    call_sign = "CG"

    name = CharField()
    region = ForeignKeyField(Region, related_name="citation_groups", null=True)
    deleted = BooleanField(default=False)
    type = EnumField(constants.ArticleType)
    target = ForeignKeyField("self", related_name="redirects", null=True)

    class Meta(object):
        db_table = "citation_group"

    @classmethod
    def select_valid(cls, *args: Any) -> Any:
        return cls.select(*args).filter(CitationGroup.deleted == False)

    @classmethod
    def create_interactively(
        cls, name: Optional[str] = None, **kwargs: Any
    ) -> "CitationGroup":
        if name is None:
            name = models.Article.getter("journal").get_one_key("name> ")
        obj = cls.create(name=name, **kwargs)
        obj.fill_required_fields()
        return obj

    @classmethod
    def get_or_create(cls, name: str) -> "CitationGroup":
        try:
            return cls.get(name=name)
        except cls.DoesNotExist:
            print(f"Creating new CitationGroup named {name}...")
            return cls.create_interactively(name=name)

    def get_required_fields(self) -> Iterable[str]:
        yield "name"
        yield "type"
        if self.type not in (constants.ArticleType.ERROR, constants.ArticleType.REDIRECT):
            yield "region"

    def apply_to_patterns(self) -> None:
        first = True
        while True:
            default = f"{self.name}*" if first else ""
            pattern = getinput.get_line("pattern to apply to> ", default=default)
            if not pattern:
                break
            else:
                first = False
                self.add_for_pattern(pattern)

    def add_for_pattern(self, pattern: str) -> None:
        for nam in models.Name.bfind(
            models.Name.verbatim_citation != None,
            models.Name.citation_group == None,
            models.Name.verbatim_citation % pattern,
        ):
            nam.display()
            nam.citation_group = self

    def for_years(
        self,
        start_year: int,
        end_year: Optional[int] = None,
        author: Optional[str] = None,
    ) -> List["models.Name"]:
        nams = list(self.nams)
        if end_year is not None:
            nams = [
                nam for nam in nams if nam.numeric_year() in range(start_year, end_year)
            ]
        else:
            nams = [nam for nam in nams if nam.numeric_year() == start_year]
        if author is not None:
            nams = [nam for nam in nams if author in nam.authority]
        self._display_nams(nams)
        return sorted(nams, key=lambda nam: nam.sort_key())

    def display(
        self, depth: int = 0, full: bool = True, include_articles: bool = False
    ) -> None:
        name_count = self.names.count()
        article_count = self.get_articles().count()
        region_str = f"{self.region.name}; " if self.region else ""
        print(f"{' ' * depth}{self.name} ({region_str}{name_count}/{article_count})")
        if full:
            self._display_nams(self.names, depth=depth)
        if include_articles:
            for art in sorted(self.get_articles(), key=lambda art: (art.year, art.name)):
                print(f"{' ' * (depth + 4)}{art.cite()}")

    def delete(self) -> None:
        assert (
            self.names.count() == 0
        ), f"cannot delete {self} because it contains names"
        assert (
            self.get_articles().count() == 0
        ), f"cannot delete {self} because it contains articles"
        self.deleted = True

    def merge(self, other: "CitationGroup") -> None:
        for nam in self.names:
            nam.citation_group = other
            nam.save()
        for art in self.get_articles():
            art.citation_group = other
            art.save()
        self.target = other
        self.type = constants.ArticleType.REDIRECT

    def get_articles(self) -> Any:
        return models.Article.select_valid().filter(
            models.Article.citation_group == self
        )

    def _display_nams(self, nams: Iterable["models.Name"], depth: int = 0) -> None:
        for nam in sorted(nams, key=lambda nam: nam.sort_key()):
            print(f"{' ' * (depth + 4)}{nam}")
            print(f"{' ' * (depth + 8)}{nam.verbatim_citation}")

    def __repr__(self) -> str:
        return repr(self.name)
