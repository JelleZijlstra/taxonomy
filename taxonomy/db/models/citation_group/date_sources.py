"""Eligibility of machine publication dates, independent of the inferred year."""

import re

from taxonomy.db.constants import DateSource

from .cg import CitationGroup, CitationGroupTag

MACHINE_DATE_SOURCES = frozenset(
    {
        DateSource.doi_published,
        DateSource.doi_published_print,
        DateSource.doi_published_online,
        DateSource.doi_published_other,
        DateSource.jstor,
    }
)


def get_unreliable_date_sources(
    citation_group: CitationGroup | None,
    *,
    series: str | None = None,
    volume: str | None = None,
    issue: str | None = None,
) -> dict[DateSource, str]:
    """Return excluded sources and reasons, including unresolved rule scopes.

    A known mismatch takes precedence over missing metadata in another selector.
    Invalid rules cannot disable human evidence; invalid regexes fail closed for
    machine evidence and are independently diagnosed by CitationGroup lint.
    """
    if citation_group is None:
        return {}
    excluded: dict[DateSource, str] = {}
    for tag in citation_group.get_tags(
        citation_group.tags, CitationGroupTag.UnreliablePublicationDate
    ):
        if tag.source not in MACHINE_DATE_SOURCES:
            continue
        missing = []
        mismatch = False
        if tag.series is not None:
            if tag.series == "":
                mismatch = bool(series)
            elif series is None:
                missing.append("series")
            elif series != tag.series:
                mismatch = True
        for field, value, pattern in (
            ("volume", volume, tag.volume_regex),
            ("issue", issue, tag.issue_regex),
        ):
            if pattern is None:
                continue
            if not value:
                missing.append(field)
                continue
            try:
                if re.fullmatch(pattern, value) is None:
                    mismatch = True
            except re.error:
                missing.append(f"valid {field} regex")
        if mismatch:
            continue
        reason = f"{citation_group.name}: {tag.comment}"
        if missing:
            reason += f" (rule scope unresolved: missing {', '.join(missing)})"
        excluded[tag.source] = reason
    return excluded
