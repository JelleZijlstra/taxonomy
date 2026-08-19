"""Dates of publication for issues."""

import re
from collections.abc import Iterable
from functools import cache
from typing import NotRequired

from clirm import Field

from taxonomy import events, getinput
from taxonomy.adt import ADT
from taxonomy.db import helpers
from taxonomy.db.constants import Markdown

from .article import Article
from .base import ADTField, BaseModel, LintConfig
from .citation_group import CitationGroup
from .lint import field_issue
from .lint_types import LintResult

PageNumber = tuple[int, bool]


def parse_page_number(page: str) -> PageNumber | None:
    """Parse an ordinary page or the second occurrence of a duplicated page."""
    match = re.fullmatch(r"([0-9]+)(bis)?", page)
    if match is None:
        return None
    return int(match.group(1)), match.group(2) is not None


def page_range_contains(
    outer_start: str, outer_end: str, inner_start: str, inner_end: str
) -> bool:
    """Return whether two ranges use the same pagination and one contains the other."""
    parsed = [
        parse_page_number(page)
        for page in (outer_start, outer_end, inner_start, inner_end)
    ]
    if any(page is None for page in parsed):
        return False
    outer_start_page, outer_end_page, inner_start_page, inner_end_page = parsed
    assert outer_start_page is not None
    assert outer_end_page is not None
    assert inner_start_page is not None
    assert inner_end_page is not None
    variants = {
        outer_start_page[1],
        outer_end_page[1],
        inner_start_page[1],
        inner_end_page[1],
    }
    return (
        len(variants) == 1
        and outer_start_page[0] <= inner_start_page[0]
        and inner_end_page[0] <= outer_end_page[0]
    )


class IssueDate(BaseModel):
    creation_event = events.Event["IssueDate"]()
    save_event = events.Event["IssueDate"]()
    call_sign = "ID"
    label_field = "id"
    clirm_table_name = "issue_date"

    citation_group = Field[CitationGroup]("citation_group_id")
    series = Field[str | None]()
    volume = Field[str]()
    issue = Field[str | None]()
    start_page = Field[str | None]()
    end_page = Field[str | None]()
    date = Field[str]()
    tags = ADTField["IssueDateTag"](is_ordered=False)

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

    def lint(self, cfg: LintConfig) -> Iterable[LintResult]:
        if self.issue is not None and "–" in self.issue:
            message = f"{self}: dash in issue: {self.issue}"
            yield field_issue(message, self, "issue", self.issue.replace("–", "-"))
        if not helpers.is_valid_date(self.date):
            yield f"{self}: invalid date {self.date}"
        parsed_start = (
            None if self.start_page is None else parse_page_number(self.start_page)
        )
        parsed_end = None if self.end_page is None else parse_page_number(self.end_page)
        if self.start_page is not None and parsed_start is None:
            yield f"{self}: invalid start page: {self.start_page}"
        if self.end_page is not None and parsed_end is None:
            yield f"{self}: invalid end page: {self.end_page}"
        if parsed_start is not None and parsed_end is not None:
            if parsed_start[1] != parsed_end[1]:
                yield f"{self}: inconsistent page-number variants"
            elif parsed_end[0] < parsed_start[0]:
                yield f"{self}: end page is before start page"

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
    def find_interactively(cls, *, edit: bool = True) -> IssueDate | None:
        available_cgs = [
            CitationGroup.get(id=cgid) for cgid in _get_cgs_with_issue_dates()
        ]
        cg = getinput.choose_one_by_name(available_cgs, message="citation group> ")
        if cg is None:
            return None
        return cls.find_interactively_in_cg(cg, edit=edit)

    @classmethod
    def find_interactively_in_cg(
        cls, cg: CitationGroup, *, edit: bool = True
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
        start_page: str,
        end_page: str,
        *,
        issue: str | None = None,
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
        matches: list[IssueDate] = []
        for candidate in candidates:
            if candidate.start_page is None or candidate.end_page is None:
                continue
            if page_range_contains(
                candidate.start_page, candidate.end_page, start_page, end_page
            ):
                matches.append(candidate)
        if issue is not None:
            issue_matches = [
                candidate for candidate in matches if candidate.issue == issue
            ]
            if issue_matches:
                matches = issue_matches
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            identities = {
                (
                    candidate.issue,
                    candidate.start_page,
                    candidate.end_page,
                    candidate.date,
                )
                for candidate in matches
            }
            if len(identities) == 1:
                # Duplicate records with identical bibliographic scope are
                # equivalent evidence for Article date inference.
                return matches[0]
            return f"Multiple matching issues for {start_page}-{end_page}: {matches}"
        if (
            len(candidates) == 1
            and candidates[0].start_page is candidates[0].end_page is None
        ):
            return candidates[0]
        candidates = sorted(
            candidates,
            key=lambda candidate: (
                parse_page_number(candidate.start_page or "") or (10**9, False)
            ),
        )
        return f"Cannot find matching issue for {start_page}-{end_page}: {candidates}"


class IssueDateTag(ADT):
    Comment(text=Markdown, optional_source=NotRequired[Article], tag=1)  # type: ignore[name-defined]


@cache
def _get_cgs_with_issue_dates() -> set[int]:
    cursor = IssueDate.clirm.select(
        "SELECT DISTINCT `citation_group_id` FROM `issue_date`"
    )
    return {cg_id for (cg_id,) in cursor}


IssueDate.creation_event.on(
    lambda isd: _get_cgs_with_issue_dates().add(isd.citation_group.id)
)
