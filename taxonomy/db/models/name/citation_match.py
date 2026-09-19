"""Read-only discovery of possible original citations, with review reasons."""

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from taxonomy.db import helpers
from taxonomy.db.constants import ArticleType
from taxonomy.db.models.article import Article
from taxonomy.db.models.article.article import ArticleTag
from taxonomy.db.models.citation_group import CitationGroup
from taxonomy.db.models.classification_entry.ce import ClassificationEntry

from .name import Name, TypeTag
from .page import parse_page_text


def author_names(obj: Article | Name) -> frozenset[str]:
    return frozenset(
        helpers.simplify_string(person.family_name) for person in obj.get_authors()
    )


def _page_numbers(text: str | None) -> Iterable[str]:
    for page in parse_page_text(text):
        if page.is_raw:
            continue
        # Legacy descriptions such as "pl. VIII, fig. 4, 5, 9, 10" must not
        # turn figure numbers into page numbers. Parenthesized details have
        # already been separated by parse_page_text.
        if re.search(r"\bfigs?\.", page.text):
            break
        if page.is_range():
            yield from page.text.split("-")
        else:
            yield page.text


def _name_pages(name: Name) -> tuple[str, ...]:
    pages = tuple(_page_numbers(name.page_described))
    if pages:
        return pages
    for tag in name.get_tags(name.type_tags, TypeTag.StructuredVerbatimCitation):
        if tag.start_page:
            return tuple(_page_numbers(tag.start_page)) + tuple(
                _page_numbers(tag.end_page)
            )
    return ()


def page_or_range(name: Name) -> tuple[int | None, int | None]:
    pages = [int(page) for page in _name_pages(name) if page.isdigit()]
    if not pages:
        return None, None
    return min(pages), max(pages) if len(pages) > 1 else None


def _ordinal(page: str | None) -> tuple[str, int] | None:
    if not page:
        return None
    if page.isdigit():
        return "arabic", int(page)
    if helpers.is_valid_roman_numeral(page.lower()):
        return "roman", helpers.parse_roman_numeral(page.lower())
    return None


def _contains_page(article: Article, page: str) -> bool | None:
    ordinal = _ordinal(page)
    if ordinal is None:
        return None
    kind, number = ordinal
    if article.pages and article.pages.isdigit() and kind == "arabic":
        return 1 <= number <= int(article.pages)
    start, end = _ordinal(article.start_page), _ordinal(article.end_page)
    if start is None or end is None:
        if article.pages and (match := re.fullmatch(r"(\d+)-(\d+)", article.pages)):
            start, end = _ordinal(match[1]), _ordinal(match[2])
    if start is not None and end is not None and start[0] == end[0]:
        return start[0] == kind and start[1] <= number <= end[1]
    return None


def _page_match(article: Article, pages: tuple[str, ...]) -> tuple[bool | None, bool]:
    matches = [_contains_page(article, page) for page in pages]
    if False in matches:
        return False, None in matches
    return (True if True in matches else None), None in matches


def _lineage(article: Article) -> Iterable[Article]:
    seen: set[Article] = set()
    while article not in seen:
        seen.add(article)
        yield article
        if article.parent is None:
            break
        article = article.parent


@dataclass(frozen=True)
class CitationMatch:
    article: Article
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class _ArticleInfo:
    article: Article
    year: int
    authors: frozenset[str]
    group: CitationGroup | None
    is_book: bool


class CitationIndex:
    """Index once per command; never retain live database objects between runs."""

    def __init__(self, articles: Iterable[Article]) -> None:
        self.by_group: dict[tuple[CitationGroup | None, int], list[_ArticleInfo]] = (
            defaultdict(list)
        )
        self.by_author: dict[tuple[str, int], list[_ArticleInfo]] = defaultdict(list)
        for article in articles:
            if article.is_invalid() or article.lacks_full_text():
                continue
            if article.has_tag(ArticleTag.MustUseChildren):
                continue
            year = article.valid_numeric_year()
            if year is None:
                continue
            lineage = list(_lineage(article))
            group = next(
                (part.citation_group for part in lineage if part.citation_group), None
            )
            info = _ArticleInfo(
                article,
                year,
                author_names(article),
                group,
                any(
                    part.type in (ArticleType.BOOK, ArticleType.CHAPTER)
                    for part in lineage
                ),
            )
            self.by_group[group, year].append(info)
            for author in info.authors:
                self.by_author[author, year].append(info)

    def find(
        self,
        name: Name,
        *,
        aggressive: bool = True,
        classification_entries: Iterable[ClassificationEntry] = (),
    ) -> list[CitationMatch]:
        if name.original_citation is not None:
            return []
        year = name.valid_numeric_year()
        if year is None:
            return []
        authors = author_names(name)
        pages = _name_pages(name)
        svc = name.get_type_tag(TypeTag.StructuredVerbatimCitation)
        ignored = {
            tag.article
            for tag in name.get_tags(
                name.type_tags, TypeTag.IgnorePotentialCitationFrom
            )
        }
        pool: dict[Article, _ArticleInfo] = {}
        if name.citation_group is not None:
            for candidate_year in range(year - 5, year + 6):
                for info in self.by_group.get(
                    (name.citation_group, candidate_year), ()
                ):
                    pool[info.article] = info
        if aggressive and (
            name.citation_group is None or name.citation_group.type is ArticleType.BOOK
        ):
            for author in authors:
                for info in self.by_author.get((author, year), ()):
                    if name.citation_group is None or (
                        info.is_book
                        and (
                            name.citation_group.name == "book"
                            or _source_support(name, info.article)
                        )
                    ):
                        pool[info.article] = info

        matches: list[CitationMatch] = []
        without_pages: list[tuple[_ArticleInfo, tuple[str, ...]]] = []
        entries: tuple[ClassificationEntry, ...] | None = None
        for info in pool.values():
            article = info.article
            if article in ignored:
                continue
            page_match, partial_pages = _page_match(article, pages)
            if page_match is False:
                continue
            same_group = (
                name.citation_group is not None and info.group == name.citation_group
            )
            same_authors = authors <= info.authors
            same_year = year == info.year
            structured = bool(
                same_group
                and svc
                and svc.volume is not None
                and svc.volume == article.volume
                and (svc.series is None or svc.series == article.series)
                and abs(year - info.year) <= 5
                and page_match is True
            )
            reasons: list[str] = []
            if structured:
                reasons.append("structured volume, series and pages match")
            elif (
                same_year and same_authors and (same_group or (aggressive and authors))
            ):
                reasons.append("author and year match")
            elif (
                aggressive
                and same_group
                and authors
                and same_authors
                and article.volume == str(year)
                and abs(year - info.year) == 1
                and page_match is True
            ):
                reasons.append(
                    "Name year matches journal volume; publication year differs"
                )
            elif aggressive and same_group and same_year and page_match is True:
                if entries is None:
                    entries = tuple(classification_entries)
                if not any(
                    _matching_entry(entry, name, article, pages) for entry in entries
                ):
                    continue
                reasons.append(
                    "mapped classification entry matches name and page; author differs"
                )
            else:
                continue

            if info.group != name.citation_group:
                reasons.append("different or missing citation group")
            elif article.citation_group != info.group:
                reasons.append("citation group inherited from parent")
            if not same_year:
                reasons.append(f"year discrepancy: Name {year}, Article {info.year}")
            if structured and not same_authors:
                reasons.append("author differs")
            if page_match is None:
                if aggressive and authors and same_authors and same_year:
                    without_pages.append((info, tuple(reasons)))
                continue
            reasons.append("numeric or Roman pages match")
            if partial_pages:
                reasons.append("some page references cannot be compared")
            matches.append(CitationMatch(article, tuple(reasons)))

        # Missing pages are weak evidence. Offer an otherwise unique match, or a
        # work explicitly supported by a title or source-bearing tag on the Name.
        for info, fallback_reasons in without_pages:
            support = _source_support(name, info.article)
            # A known page with unknown Article pagination, or a missing
            # citation group, needs corroboration beyond uniqueness in our
            # incomplete library.
            unique = (
                len(without_pages) == 1
                and not matches
                and name.citation_group is not None
                and not any(_ordinal(page) is not None for page in pages)
            )
            if support or unique:
                matches.append(
                    CitationMatch(
                        info.article,
                        (
                            *fallback_reasons,
                            "pages missing or not comparable",
                            support or "unique author/year candidate",
                        ),
                    )
                )
        parents = {
            parent for match in matches for parent in list(_lineage(match.article))[1:]
        }
        return sorted(
            (match for match in matches if match.article not in parents),
            key=lambda match: match.article.sort_key(),
        )


def _matching_entry(
    entry: ClassificationEntry, name: Name, article: Article, pages: tuple[str, ...]
) -> bool:
    original_name = name.corrected_original_name or name.original_name
    return bool(
        original_name
        and entry.mapped_name == name
        and entry.article == article
        and not entry.is_invalid()
        and helpers.simplify_string(entry.name)
        == helpers.simplify_string(original_name)
        and set(_page_numbers(entry.page)) & set(pages)
    )


def _source_support(name: Name, article: Article) -> str | None:
    for tag in name.type_tags:
        if getattr(tag, "source", None) == article:
            return "existing source-bearing tag"
    if name.verbatim_citation:
        citation = helpers.simplify_string(name.verbatim_citation)
        for part in _lineage(article):
            if part.title:
                title = helpers.simplify_string(part.title)
                if len(title) >= 20 and title in citation:
                    return "title present in verbatim citation"
    return None
