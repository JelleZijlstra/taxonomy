"""

Dates of publication for issues.

"""

from __future__ import annotations

from collections.abc import Iterable
from functools import cache

from peewee import CharField, ForeignKeyField

from ... import events, getinput
from ...adt import ADT
from .. import helpers
from .article import Article
from .base import ADTField, BaseModel, LintConfig, database
from .citation_group import CitationGroup


class IssueDate(BaseModel):
    creation_event = events.Event["IssueDate"]()
    save_event = events.Event["IssueDate"]()
    call_sign = "ID"
    label_field = "id"

    citation_group = ForeignKeyField(CitationGroup)
    series = CharField(null=True)
    volume = CharField(null=False)
    issue = CharField(null=True)
    start_page = CharField(null=True)
    end_page = CharField(null=True)
    date = CharField(null=False)
    tags = ADTField(lambda: IssueDateTag, null=True)

    class Meta:
        db_table = "issue_date"

    @classmethod
    def create_many(cls) -> None:
        while True:
            specimen = cls.create_interactively()
            if specimen is None:
                break
            print("Created specimen:")
            specimen.full_data()
            print("==================================")
            specimen.edit()

    def edit(self) -> None:
        self.fill_field("tags")

    def lint(self, cfg: LintConfig) -> Iterable[str]:
        if not helpers.is_valid_date(self.date):
            yield f"{self}: invalid date {self.date}"
        if self.start_page is not None and not self.start_page.isnumeric():
            yield f"{self}: invalid start page: {self.start_page}"
        if self.end_page is not None and not self.end_page.isnumeric():
            yield f"{self}: invalid start page: {self.end_page}"

    @classmethod
    def has_data(cls, cg: CitationGroup) -> bool:
        return cg.id in _get_cgs_with_issue_dates()

    def __str__(self) -> str:
        parts = [self.citation_group.name, " "]
        if self.series:
            parts.append(f"({self.series})")
        parts.append(self.volume)
        if self.issue:
            parts.append(f"({self.issue})")
        if self.start_page and self.end_page:
            parts += [":", self.start_page, "–", self.end_page]
        parts += [" (published ", self.date, ")"]
        return "".join(parts)

    @classmethod
    def find_interactively(cls, edit: bool = True) -> IssueDate | None:
        available_cgs = [
            CitationGroup.get(id=cgid) for cgid in _get_cgs_with_issue_dates()
        ]
        cg = getinput.choose_one_by_name(available_cgs, message="citation group> ")
        if cg is None:
            return None
        return cls.find_interactively_in_cg(cg, edit=edit)

    @classmethod
    def find_interactively_in_cg(
        cls, cg: CitationGroup, edit: bool = True
    ) -> IssueDate | None:
        options = list(cls.select_valid().filter(cls.citation_group == cg))
        volumes = sorted({issue_date.volume for issue_date in options})
        volume = getinput.choose_one_by_name(volumes, message="volume> ")
        if volume is None:
            return None
        filtered_options = [
            issue_date for issue_date in options if issue_date.volume == volume
        ]
        chosen = getinput.choose_one(filtered_options, message="issue_date> ")
        if chosen is None:
            return None
        if edit:
            chosen.edit()
        return chosen

    @classmethod
    def find_matching_issue(
        cls,
        cg: CitationGroup,
        series: str | None,
        volume: str,
        start_page: int,
        end_page: int,
    ) -> IssueDate | str | None:
        """Find the issue that contains these pages.

        Return value:
        - None if there is no data for these pages.
        - str if there is some mistake, e.g. the page range covers multiple issues.
        - IssueDate if the range can be unambiguously associated with an issue.

        """
        if cg.id not in _get_cgs_with_issue_dates():
            return None
        candidates = list(
            cls.select_valid().filter(
                cls.citation_group == cg, cls.volume == volume, cls.series == series
            )
        )
        if not candidates:
            return None
        for candidate in candidates:
            if candidate.start_page is None or candidate.end_page is None:
                continue
            if int(candidate.start_page) <= start_page and end_page <= int(
                candidate.end_page
            ):
                return candidate
        if (
            len(candidates) == 1
            and candidates[0].start_page is candidates[0].end_page is None
        ):
            return candidates[0]
        candidates = sorted(candidates, key=lambda c: int(c.start_page))
        return f"Cannot find matching issue for {start_page}-{end_page}: {candidates}"


class IssueDateTag(ADT):
    CommentIssueDate(text=str, source=Article, tag=1)  # type: ignore


@cache
def _get_cgs_with_issue_dates() -> set[int]:
    cursor = database.execute_sql(
        "SELECT DISTINCT `citation_group_id` FROM `issue_date`"
    )
    return {cg_id for (cg_id,) in cursor}


IssueDate.creation_event.on(
    lambda id: _get_cgs_with_issue_dates().add(id.citation_group.id)
)
