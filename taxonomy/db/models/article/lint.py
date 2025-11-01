"""Lint steps for Articles."""

import bisect
import pprint
import re
import subprocess
import unicodedata
import urllib.parse
from collections import defaultdict
from collections.abc import Collection, Generator, Hashable, Iterable, Sequence
from functools import cache
from typing import Any

import requests

from taxonomy import getinput, urlparse
from taxonomy.apis import bhl
from taxonomy.apis.zoobank import clean_lsid, get_zoobank_data_for_act, is_valid_lsid
from taxonomy.db import helpers, models
from taxonomy.db.constants import ArticleKind, ArticleType, DateSource
from taxonomy.db.models.base import ADTField, BaseModel, LintConfig
from taxonomy.db.models.citation_group.cg import CitationGroup, CitationGroupTag
from taxonomy.db.models.citation_group.lint import get_biblio_pages
from taxonomy.db.models.issue_date import IssueDate
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.person import AuthorTag, is_more_specific_than

from .article import Article, ArticleComment, ArticleTag, PresenceStatus
from .name_parser import get_name_parser


def remove_unused_ignores(art: Article, unused: Collection[str]) -> None:
    new_tags = []
    for tag in art.tags:
        if isinstance(tag, ArticleTag.IgnoreLint) and tag.label in unused:
            print(f"{art}: removing unused IgnoreLint tag: {tag}")
        else:
            new_tags.append(tag)
    art.tags = new_tags  # type: ignore[assignment]


def get_ignores(art: Article) -> Iterable[IgnoreLint]:
    return art.get_tags(art.tags, ArticleTag.IgnoreLint)


LINT = Lint(Article, get_ignores, remove_unused_ignores)


@LINT.add("tags")
def check_tags(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not art.tags:
        return
    tags: list[ArticleTag] = []
    original_tags = list(art.tags)
    for tag in original_tags:
        if isinstance(tag, ArticleTag.LSIDArticle):
            if not tag.text:
                continue
            tag = ArticleTag.LSIDArticle(clean_lsid(tag.text), tag.present_in_article)
            if not is_valid_lsid(tag.text):
                yield f"invalid LSID {tag.text}"
        elif isinstance(tag, ArticleTag.BiblioNoteArticle):
            if tag.text not in get_biblio_pages():
                yield f"references non-existent page {tag.text!r}"
        elif isinstance(tag, ArticleTag.HDL):
            if not is_valid_hdl(tag.text):
                yield f"invalid HDL {tag.text!r}"
        elif isinstance(tag, ArticleTag.JSTOR):
            jstor_id = tag.text
            if len(jstor_id) < 4 or not jstor_id.isnumeric():
                yield f"invalid JSTOR id {jstor_id!r}"
        elif isinstance(tag, ArticleTag.PublicationDate):
            if art.doi is None and tag.source in (
                DateSource.doi_published,
                DateSource.doi_published_online,
                DateSource.doi_published_other,
                DateSource.doi_published_print,
            ):
                continue
            # Remove empty tags
            if not tag.date:
                if not tag.comment:
                    continue
                else:
                    yield f"tag has comment but no date: {tag}"
        elif isinstance(tag, ArticleTag.KnownAlternativeYear):
            if not tag.year.isnumeric():
                yield f"invalid alternative date {tag.year!r}"
        tags.append(tag)
    tags = sorted(set(tags))
    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {art}")
            getinput.print_diff(sorted(original_tags), tags)
        if cfg.autofix:
            art.tags = tags  # type: ignore[assignment]
        else:
            yield f"{art}: needs change to tags"


@LINT.add("name")
def check_name(art: Article, cfg: LintConfig) -> Iterable[str]:
    # Names are restricted to printable ASCII because a long time ago I stored
    # files on a file system that didn't handle non-ASCII properly. It's probably
    # safe to lift this restriction by now though.
    if not re.fullmatch(r"^[ -~]+$", art.name):
        yield "name contains invalid characters"
    parser = get_name_parser(art.name)
    if parser.error_occurred():
        parser.print_errors()
        yield "name failed to parse"
    if parser.extension:
        if not art.kind.is_electronic():
            yield (
                f"non-electronic article (kind {art.kind!r}) should not have a"
                " file extension"
            )
    elif art.kind.is_electronic():
        yield "electronic article should have a file extension"


@LINT.add("path")
def check_path(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.kind.is_electronic():
        if art.path is None or art.path == "NOFILE":
            yield "electronic article should have a path"
    elif art.path is not None:
        message = (
            f"non-electronic article (kind {art.kind!r}) should have no"
            f" path, but has {art.path}"
        )
        if cfg.autofix:
            print(f"{art}: {message}")
            art.path = None
        else:
            yield message


@LINT.add("type_kind")
def check_type_and_kind(art: Article, cfg: LintConfig) -> Iterable[str]:
    # The difference between kind and type is:
    # * kind is about how this article is stored in the database (electronic copy,
    #   physical copy, etc.)
    # * type is about what kind of publication it is (journal, book, etc.)
    # Thus redirect should primarily be a *kind*. We have the *type* too for legacy
    # reasons but *kind* should be primary.
    if art.type is None:
        yield "type is None"
    if art.type is ArticleType.REDIRECT and art.kind is not ArticleKind.redirect:
        yield "conflicting signals on whether it is a redirect"
    if art.type is ArticleType.ERROR:
        yield "type is ERROR"
    if (
        art.kind in (ArticleKind.redirect, ArticleKind.alternative_version)
        and art.parent is None
    ):
        yield f"is {art.kind.name} but has no parent"
    if art.type in (ArticleType.SUPPLEMENT, ArticleType.CHAPTER) and art.parent is None:
        yield f"is {art.type.name} but has no parent"
    if art.kind is ArticleKind.no_copy:
        if art.parent is not None and art.parent.kind is not ArticleKind.no_copy:
            yield f"is no_copy but has a parent of kind {art.parent.kind!r}"
        if art.url is not None:
            parsed = urllib.parse.urlparse(art.url)
            if parsed.netloc in (
                "www.biodiversitylibrary.org",
                "biodiversitylibrary.org",
            ):
                if cfg.autofix:
                    print(f"{art}: set kind to reference")
                    art.kind = ArticleKind.reference
                else:
                    yield "has a BHL URL and should be of kind 'reference'"


@LINT.add("article_number")
def check_article_number(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not art.article_number:
        return
    if art.type is not ArticleType.JOURNAL:
        yield "only journal articles should have an article number"
    if art.citation_group is not None:
        tag = art.citation_group.get_tag(CitationGroupTag.ArticleNumberRegex)
        if tag is not None:
            if not re.fullmatch(tag.text, art.article_number):
                yield f"article number {art.article_number!r} does not match pattern {tag.text!r}"

            # Only do this if there's an ArticleNumberRegex so we know the article number is right
            if (
                art.start_page == "1"
                and art.end_page is not None
                and not art.citation_group.get_tag(
                    CitationGroupTag.ArticleNumberIsSecondary
                )
            ):
                message = f"replace start and end page {art.start_page}-{art.end_page}, since there is an article number"
                if cfg.autofix:
                    print(f"{art}: {message}")
                    art.pages = art.end_page
                    art.start_page = None
                    art.end_page = None
                else:
                    yield message
        else:
            yield f"citation group {art.citation_group} has no ArticleNumberRegex tag"
        if (
            art.start_page is not None
            and art.end_page is None
            and art.start_page == art.article_number
        ):
            message = f"replace start page {art.start_page} with article number {art.article_number}"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.start_page = None
            else:
                yield message


@LINT.add("transfer_article_number")
def transfer_article_number(art: Article, cfg: LintConfig) -> Iterable[str]:
    if (
        art.article_number is not None
        or art.start_page is None
        or art.end_page is not None
        or art.citation_group is None
    ):
        return
    cg = art.citation_group
    tag = cg.get_tag(CitationGroupTag.ArticleNumberRegex)
    if tag is None:
        return
    if not re.fullmatch(tag.text, art.start_page):
        return
    message = f"transferring start page {art.start_page} to article number"
    if cfg.autofix:
        print(f"{art}: {message}")
        art.article_number = art.start_page
        art.start_page = None
    else:
        yield message


SOURCE_PRIORITY = {
    # Without an lsid, online publication doesn't count
    False: [
        DateSource.decision,
        DateSource.external,
        DateSource.internal,
        DateSource.doi_published_print,
        DateSource.doi_published,
    ],
    True: [
        DateSource.decision,
        DateSource.external,
        DateSource.internal,
        DateSource.doi_published,
        DateSource.doi_published_online,
        DateSource.doi_published_print,
    ],
}


def infer_publication_date_from_tags(
    tags: Sequence[ArticleTag] | None,
) -> tuple[str | None, list[str]]:
    if not tags:
        return None, []
    by_source = defaultdict(list)
    has_lsid = False
    for tag in tags:
        if isinstance(tag, ArticleTag.PublicationDate):
            by_source[tag.source].append(tag)
        elif (
            isinstance(tag, ArticleTag.LSIDArticle)
            # "inferred" strictly doesn't count but we'll allow it
            and tag.present_in_article
            in (PresenceStatus.present, PresenceStatus.inferred)
        ):
            has_lsid = True
    for source in SOURCE_PRIORITY[has_lsid]:
        if tags_of_source := by_source[source]:
            if (
                len(tags_of_source) > 1
                and len(_unique_dates(tag.date for tag in tags_of_source)) > 1
            ):
                return None, [
                    f"has multiple tags for source {source}: {tags_of_source}"
                ]
            return max((tag.date for tag in tags_of_source), key=len), []
    return None, []


def _unique_dates(dates: Iterable[str]) -> set[str]:
    dates = set(dates)
    return {
        date
        for date in dates
        if not any(other != date and other.startswith(date) for other in dates)
    }


def infer_publication_date(art: Article) -> tuple[str | None, str | None, list[str]]:
    if art.type in (ArticleType.CHAPTER, ArticleType.SUPPLEMENT):
        if parent := art.parent:
            return parent.year, None, []
    date: str | None
    if data := infer_publication_date_from_issue_date(art):
        date, issue = data
        return date, issue, []
    date, errors = infer_publication_date_from_tags(art.tags)
    return date, None, errors


def infer_publication_date_from_issue_date(
    art: Article,
) -> tuple[str, str | None] | None:
    if (
        art.type is ArticleType.JOURNAL
        and art.citation_group
        and art.volume
        and art.start_page
        and art.end_page
        and art.start_page.isnumeric()
        and art.end_page.isnumeric()
    ):
        issue_date = IssueDate.find_matching_issue(
            art.citation_group,
            art.series,
            art.volume,
            int(art.start_page),
            int(art.end_page),
        )
        if isinstance(issue_date, IssueDate):
            return issue_date.date, issue_date.issue
    return None


@LINT.add("year")
def check_year(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.kind is ArticleKind.alternative_version:
        return
    if art.year is not None:
        # use hyphens
        year = art.year.replace("–", "-")

        # remove spaces around the dash
        if match := re.match(r"(\d{4})\s+-\s+(\d{4})", year):
            year = f"{match.group(1)}-{match.group(2)}"

        yield from _maybe_clean(art, "year", year, cfg)

        if art.year != "undated" and not helpers.is_valid_date(art.year):
            yield f"invalid year {art.year!r}"
        if helpers.is_date_range(art.year) and not any(
            art.get_tags(art.tags, ArticleTag.MustUseChildren)
        ):
            yield f"must have MustUseChildren tag because date is a range: {art.year}"

    inferred, issue, messages = infer_publication_date(art)
    yield from messages
    if issue is not None and issue != art.issue:
        message = f"issue mismatch: inferred {issue}, actual {art.issue}"
        if cfg.autofix and art.issue is None:
            print(f"{art}: {message}")
            art.issue = issue
        else:
            yield message

    if inferred is not None and inferred != art.year:
        # Ignore obviously wrong ones (though eventually we should retire this)
        if (
            inferred.startswith("20")
            and art.year is not None
            and art.numeric_year() < 1990
        ):
            return
        is_more_specific = helpers.is_more_specific_date(inferred, art.year)
        if is_more_specific:
            message = f"tags yield more specific date {inferred} instead of {art.year}"
        else:
            message = f"year mismatch: inferred {inferred}, actual {art.year}"
        can_autofix = is_more_specific
        try:
            inferred_year = int(inferred[:4])
        except ValueError:
            return
        if art.year is None or (
            inferred_year is not None
            and not has_new_names(art)
            and abs(art.numeric_year() - inferred_year) <= 1
        ):
            can_autofix = True
        if cfg.autofix and can_autofix:
            print(f"{art}: {message}")
            art.year = inferred
        else:
            yield message


@LINT.add("precise_date")
def check_precise_date(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.citation_group is None:
        return
    if not art.citation_group.has_tag(CitationGroupTag.MustHavePreciseDate):
        return
    if not has_new_names(art):
        return
    if art.year is not None and "-" not in art.year:
        yield f"is in {art.citation_group} but has imprecise date {art.year}"


def get_next_article_with_earlier_date(art: Article) -> Article | None:
    group = models.citation_group.ordering.get_group_for_article(art)
    if group is None:
        return None
    index = group.index(art)
    if index == len(group) - 1:
        return None
    next = group[index + 1]
    my_date = art.get_date_object()
    next_date = next.get_date_object()
    if my_date <= next_date:
        return None
    return next


@LINT.add(
    "date_order", clear_caches=lambda: models.citation_group.ordering.clear_all_caches()
)
def check_date_ordering(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.citation_group is None:
        return
    if art.citation_group.type is not ArticleType.JOURNAL:
        return
    next = get_next_article_with_earlier_date(art)
    if next is None:
        return

    # TODO: Eventually remove these conditions.
    if not LINT.is_ignoring_lint(art, "date_order"):
        if art.numeric_year() > 2010:
            return None  # online publishing is too thoroughly out of order
        if art.year is not None:
            my_date = art.get_date_object()
            next_date = next.get_date_object()
            if my_date.year == next_date.year and "-" not in art.year:
                return None
            if (
                my_date.year == next_date.year
                and my_date.month == next_date.month
                and art.year.count("-") == 1
            ):
                return None
            # For now only consider cases where the year conflicts
            if my_date.year == next_date.year:
                return None

    models.citation_group.ordering.clear_cache_for_cg(art.citation_group)
    next = get_next_article_with_earlier_date(art)
    if next is None:
        return
    if not LINT.is_ignoring_lint(art, "date_order"):
        print("- next: ", end="")
        next.display()
        print("- self: ", end="")
        art.display()
    yield f"date {art.year} is after next article {next.year} for {next!r}"


@LINT.add("infer_precise_date")
def infer_precise_date(art: Article, cfg: LintConfig) -> Iterable[str]:
    siblings = get_inferred_date_from_position(art)
    if siblings is None:
        return
    before, after = siblings
    message = (
        f"inferred publication date of {after.year} based on position between"
        f" {before!r} and {after!r}"
    )
    if cfg.autofix:
        print(f"{art}: {message}")
        art.year = after.year
    else:
        yield message


def get_inferred_date_from_position(art: Article) -> tuple[Article, Article] | None:
    if (
        art.citation_group is None
        or art.volume is None
        or art.start_page is None
        or art.year is None
        or not art.start_page.isnumeric()
        or "-" in art.year
    ):
        return None
    # If there is a DOI, we can get more reliable data
    if art.doi is not None:
        return None
    siblings = [
        art
        for art in Article.select_valid().filter(
            Article.citation_group == art.citation_group,
            Article.series == art.series,
            Article.volume == art.volume,
            Article.year.contains("-"),
        )
        if art.start_page is not None
        and art.start_page.isnumeric()
        and art.has_tag(ArticleTag.PublicationDate)
    ]
    if len(siblings) <= 1:
        return None
    siblings = sorted(siblings, key=lambda art: int(art.start_page))
    index = bisect.bisect_left(
        siblings, int(art.start_page), key=lambda art: int(art.start_page)
    )
    if index == 0 or index == len(siblings):
        return None
    if siblings[index - 1].year != siblings[index].year:
        return None
    return siblings[index - 1], siblings[index]


@LINT.add("unsupported_year", disabled=True)
def check_unsupported_year(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.id < 67_000 and (
        not has_new_names(art)
        or art.type in (ArticleType.CHAPTER, ArticleType.PART, ArticleType.SUPPLEMENT)
    ):
        return
    if not has_unsupported_publication_date(art):
        return
    yield f"precise date {art.year} is not supported by any evidence"


def has_unsupported_publication_date(art: Article) -> bool:
    if art.year is None or "-" not in art.year or helpers.is_date_range(art.year):
        return False
    if art.has_tag(ArticleTag.PublicationDate):
        return False
    if get_inferred_date_from_position(art) is not None:
        return False
    if infer_publication_date_from_issue_date(art) is not None:
        return False
    return True


def text_contains_date(art: Article) -> bool:
    if art.year is None:
        return False
    date = art.get_date_object()
    if art.year.count("-") == 2:
        day = date.strftime("%d").lstrip("0")
        possible_dates = {
            date.strftime(f"{day} %B %Y"),
            date.strftime(f"%B {day}, %Y"),
            date.strftime(f"%B {day}st, %Y"),
            date.strftime(f"%B {day}nd, %Y"),
            date.strftime(f"%B {day}th, %Y"),
        }
    else:
        possible_dates = {date.strftime("%B %Y"), date.strftime("%B, %Y")}
    possible_dates = {date.casefold() for date in possible_dates}
    pages = art.get_all_pdf_pages()
    if pages:
        first_page = re.sub(r"\s+", " ", pages[0].casefold())
        if any(date.casefold() in first_page for date in possible_dates):
            return True
    if page_id := get_bhl_page_id(art):
        text = bhl.get_page_metadata(page_id).get("OcrText", "")
        text = re.sub(r"\s+", " ", text.casefold())
        if any(date.casefold() in text for date in possible_dates):
            return True
    return False


@LINT.add("add_internal_publication_date")
def add_internal_publication_date(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.year is None or not has_unsupported_publication_date(art):
        return
    if text_contains_date(art):
        tag = ArticleTag.PublicationDate(DateSource.internal, art.year)
        message = f"adding PublicationDate tag for {art.year}: {tag}"
        if cfg.autofix:
            print(f"{art}: adding PublicationDate tag for {art.year}")
            art.add_tag(tag)
        else:
            yield message


_JSTOR_URL_REGEX = r"https?://www\.jstor\.org/stable/(\d+)"
_JSTOR_DOI_PREFIX = "10.2307/"


def is_valid_hdl(hdl: str) -> bool:
    return bool(re.fullmatch(r"^\d+(\.\d+)?\/\S+$", hdl))


def is_valid_doi(doi: str) -> bool:
    return bool(re.fullmatch(r"^10\.[A-Za-z0-9\.\/\[\]<>\-;:_()+#]+$", doi))


@LINT.add("must_have_url")
def check_must_have_url(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.citation_group is None:
        return
    if not art.citation_group.has_tag(CitationGroupTag.MustHaveURL):
        return
    url = art.geturl()
    if url is not None:
        tag = art.citation_group.get_tag(CitationGroupTag.URLPattern)
        if tag is not None:
            if not re.fullmatch(tag.text, url):
                yield f"url {url} does not match pattern {tag.text!r}"
        return
    yield f"has no URL, but is in {art.citation_group}"


@LINT.add("must_have_publisher")
def check_must_have_publisher(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.type is not ArticleType.BOOK:
        return
    if art.citation_group is not None and art.publisher is not None:
        return
    if not has_new_names(art):
        return
    if art.citation_group is None:
        yield "book is missing citation group"
    if art.publisher is None:
        yield "book is missing publisher"


@LINT.add("url")
def check_url(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.url is None:
        return
    parsed_url = urlparse.parse_url(art.url)
    match parsed_url:
        case urlparse.HDLUrl(hdl, query=None):
            message = f"inferred HDL {hdl} from url {art.url}"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.add_tag(ArticleTag.HDL(hdl))
                art.url = None
            else:
                yield message
            return
        case urlparse.JStorUrl(jstor_id):
            message = f"inferred JStor id {jstor_id} from url {art.url}"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.add_tag(ArticleTag.JSTOR(jstor_id))
                art.url = None
            else:
                yield message
        case urlparse.DOIURL(doi):
            message = f"inferred DOI {doi} from url {art.url}"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.doi = doi
                art.url = None
            else:
                yield message
        case _:
            stringified = str(parsed_url)
            if stringified != art.url:
                message = f"reformatted url to {parsed_url} from {art.url}"
                if cfg.autofix:
                    print(f"{art}: {message}")
                    art.url = stringified
                else:
                    yield message

            for message in parsed_url.lint():
                yield f"URL {art.url}: {message}"


@LINT.add("doi")
def check_doi(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.doi is None:
        return
    cleaned = urllib.parse.unquote(art.doi)
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        cleaned = cleaned.removeprefix(prefix)
    yield from _maybe_clean(art, "doi", cleaned, cfg)
    if not is_valid_doi(art.doi):
        yield f"invalid doi {art.doi!r}"
    if art.doi.startswith(_JSTOR_DOI_PREFIX):
        jstor_id = art.doi.removeprefix(_JSTOR_DOI_PREFIX).removeprefix("/")
        message = (
            f"inferred JStor id {jstor_id} from doi {art.doi} (CG"
            f" {art.citation_group})"
        )
        if cfg.autofix:
            print(f"{art}: {message}")
            art.add_tag(ArticleTag.JSTOR(jstor_id))
            art.doi = None
        else:
            yield message


@LINT.add("bhl_item_from_bibliography", requires_network=True)
def bhl_item_from_bibliography(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.url is None:
        return
    match urlparse.parse_url(art.url):
        case urlparse.BhlBibliography(bib_id):
            metadata = bhl.get_title_metadata(bib_id)
            if "Items" in metadata and len(metadata["Items"]) == 1:
                item_id = metadata["Items"][0]["ItemID"]
                new_url = f"https://www.biodiversitylibrary.org/item/{item_id}"
                message = f"inferred BHL item {item_id} from bibliography {art.url}"
                if cfg.autofix:
                    print(f"{art}: {message}")
                    art.url = new_url
                else:
                    yield message


@LINT.add("bhl_part_from_page", requires_network=True)
def bhl_part_from_page(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.url is None or art.title is None:
        return
    parsed = urlparse.parse_url(art.url)
    if not isinstance(parsed, urlparse.BhlPage):
        return
    metadata = bhl.get_page_metadata(parsed.page_id)
    if "ItemID" not in metadata:
        return
    item_metadata = bhl.get_item_metadata(int(metadata["ItemID"]))
    if item_metadata is None or "Parts" not in item_metadata:
        return
    matching_part = None
    for part in item_metadata["Parts"]:
        if helpers.simplify_string(part["Title"]) != helpers.simplify_string(art.title):
            continue
        part_metadata = bhl.get_part_metadata(part["PartID"])
        if "StartPageID" not in part_metadata:
            continue
        if str(part_metadata["StartPageID"]) == str(parsed.page_id):
            matching_part = part
            break
    if matching_part is None:
        return
    message = f"inferred BHL part {matching_part['PartID']} from page {art.url}"
    if cfg.autofix:
        print(f"{art}: {message}")
        art.url = str(urlparse.BhlPart(matching_part["PartID"]))
    else:
        yield message


def _get_bhl_page_ids_from_names(art: Article) -> set[int]:
    new_names = list(art.get_new_names())
    if not new_names:
        return set()
    known_pages = [
        tag
        for nam in new_names
        for tag in nam.get_tags(nam.type_tags, models.name.TypeTag.AuthorityPageLink)
    ]
    bhl_page_ids = set()
    for tag in known_pages:
        match urlparse.parse_url(tag.url):
            case urlparse.BhlPage(page_id):
                bhl_page_ids.add(page_id)
    return bhl_page_ids


@LINT.add("must_have_bhl_from_names", requires_network=True)
def must_have_bhl_url_from_names(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not should_look_for_bhl_url(art):
        return
    bhl_page_ids = _get_bhl_page_ids_from_names(art)
    if not bhl_page_ids:
        return
    yield f"has new names with BHL page IDs {bhl_page_ids} but no BHL URL"


@LINT.add("bhl_page_from_names", requires_network=True)
def infer_bhl_page_from_names(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not should_look_for_bhl_url(art):
        if cfg.verbose:
            print(f"{art}: not looking for BHL URL")
        return
    bhl_page_ids = _get_bhl_page_ids_from_names(art)
    if not bhl_page_ids:
        if cfg.verbose:
            print(f"{art}: no BHL page IDs from names")
        return
    item_ids = set()
    for page_id in bhl_page_ids:
        metadata = bhl.get_page_metadata(page_id)
        if "ItemID" in metadata:
            item_ids.add(int(metadata["ItemID"]))
    if len(item_ids) > 1:
        yield f"names are on multiple BHL items: {item_ids} (from {bhl_page_ids})"
        return
    if not item_ids:
        if cfg.verbose:
            print(f"{art}: no BHL item IDs from names")
        return
    item_id = item_ids.pop()
    item_metadata = bhl.get_item_metadata(item_id)
    if art.title is None:
        if cfg.verbose:
            print(f"{art}: no title")
    elif item_metadata is not None and "Parts" in item_metadata:
        for part in item_metadata["Parts"]:
            simplified_part_title = helpers.simplify_string(part["Title"])
            simplified_my_title = helpers.simplify_string(art.title)
            if simplified_part_title != simplified_my_title:
                if cfg.verbose:
                    print(
                        f"{art}: title {art.title!r} ({simplified_my_title!r}) does not match part title"
                        f" {part['Title']!r} ({simplified_part_title!r})"
                    )
                continue
            part_id = part["PartID"]
            part_metadata = bhl.get_part_metadata(part_id)
            if part_metadata is None:
                if cfg.verbose:
                    print(f"{art}: no metadata for part {part_id}")
                continue
            part_page_ids = {page["PageID"] for page in part_metadata["Pages"]}
            if bhl_page_ids <= part_page_ids:
                message = f"inferred BHL part {part_id} from names"
                if cfg.autofix:
                    print(f"{art}: {message}")
                    art.set_or_replace_url(
                        f"https://www.biodiversitylibrary.org/part/{part_id}"
                    )
                else:
                    yield message
                return
            elif cfg.verbose:
                print(
                    f"{art}: not all known pages ({bhl_page_ids}) are in part {part_id} {part_page_ids}"
                )
    elif cfg.verbose:
        print(f"{art}: no item metadata for {item_id}")

    if art.start_page is None or art.end_page is None:
        if cfg.verbose:
            print(f"{art}: no start or end page")
        return

    start_page = art.start_page
    end_page = art.end_page
    possible_start_pages = bhl.get_possible_pages(item_id, start_page)
    possible_end_pages = bhl.get_possible_pages(item_id, end_page)
    if len(possible_start_pages) != len(possible_end_pages):
        if cfg.verbose:
            print(
                f"{art}: different number of possible start and end pages"
                f" {possible_start_pages} {possible_end_pages}"
            )
        return
    page_mapping = bhl.get_page_id_to_index(item_id)
    for possible_start_page, possible_end_page in zip(
        possible_start_pages, possible_end_pages, strict=True
    ):
        if not bhl.is_contiguous_range(
            item_id, possible_start_page, possible_end_page, page_mapping
        ):
            if cfg.verbose:
                print(
                    f"{art}: non-contiguous range {possible_start_page}–{possible_end_page}"
                )
            continue
        start_page_idx = page_mapping[possible_start_page]
        end_page_idx = page_mapping[possible_end_page]
        if not all(
            start_page_idx <= page_mapping[page_id] <= end_page_idx
            for page_id in bhl_page_ids
        ):
            if cfg.verbose:
                print(
                    f"{art}: not all known pages are in range {possible_start_page}–{possible_end_page}"
                )
            continue
        message = f"inferred BHL page {possible_start_page} from names"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.set_or_replace_url(
                f"https://www.biodiversitylibrary.org/page/{possible_start_page}"
            )
        else:
            yield message


def should_look_for_bhl_url(art: Article) -> bool:
    if art.type is ArticleType.SUPPLEMENT:
        return False
    return not has_bhl_url(art)


def has_bhl_url(art: Article) -> bool:
    if art.url is None:
        return False
    return isinstance(urlparse.parse_url(art.url), urlparse.BhlUrl)


def get_bhl_page_id(art: Article) -> int | None:
    if art.url is None:
        return None
    match urlparse.parse_url(art.url):
        case urlparse.BhlPage(page_id):
            return page_id
        case urlparse.BhlPart(part_id):
            part_metadata = bhl.get_part_metadata(part_id)
            if part_metadata is not None and "StartPageID" in part_metadata:
                return part_metadata["StartPageID"]
    return None


def should_require_bhl_link(art: Article) -> bool:
    if LINT.is_ignoring_lint(art, "must_have_bhl"):
        return True
    match art.type:
        case ArticleType.JOURNAL:
            cg = art.citation_group
            if cg is None:
                return False
            year = art.numeric_year()
            if not cg.should_have_bhl_link_in_year(year):
                return False
            if art.addyear < "2024" and not has_new_names(art):
                return False
            return True
        case ArticleType.CHAPTER | ArticleType.PART:
            if art.parent is None:
                return False
            return has_bhl_url(art.parent)
        case ArticleType.BOOK:
            if art.numeric_year() > 1922:
                # probably not in BHL
                return False
            if art.geturl() is not None:
                # probably got it from elsewhere
                return False
            if art.addyear >= "2024":
                return True
            return has_new_names(art)
    return False


def has_new_names(art: Article) -> bool:
    if art.get_new_names().count() > 0 or art.get_classification_entries().count() > 0:
        return True
    for child in art.get_children():
        if has_new_names(child):
            return True
    return False


@LINT.add("must_have_bhl", requires_network=True)
def must_have_bhl_link(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not should_look_for_bhl_url(art):
        return
    if not should_require_bhl_link(art):
        return
    yield "should have BHL link"


@LINT.add("bhl_page", requires_network=True)
def infer_bhl_page(art: Article, cfg: LintConfig = LintConfig()) -> Iterable[str]:
    if not should_look_for_bhl_url(art):
        return
    page_obj = get_inferred_bhl_page(art, cfg)
    if page_obj is None:
        return
    message = f"inferred BHL page {page_obj} from {art.start_page}–{art.end_page}"
    if art.url is not None:
        message += f" (replacing {art.url})"
    if cfg.autofix:
        print(f"{art}: {message}")
        art.set_or_replace_url(page_obj.page_url)
    else:
        yield message
    print(page_obj.page_url)


def get_inferred_bhl_page(art: Article, cfg: LintConfig) -> bhl.PossiblePage | None:
    if (
        art.start_page is None
        or art.end_page is None
        or art.year is None
        or art.title is None
    ):
        return None
    year = art.numeric_year()
    cg = art.get_citation_group()
    if art.type in (ArticleType.CHAPTER, ArticleType.PART) and art.parent is not None:
        url = art.parent.url
        if url is None:
            return None
        item_id = bhl.get_bhl_item_from_url(url)
        if item_id is None:
            return None
        start_pages = bhl.get_possible_pages(item_id, art.start_page)
        if len(start_pages) != 1:
            return None
        end_pages = bhl.get_possible_pages(item_id, art.end_page)
        if len(end_pages) != 1:
            return None
        page_mapping = bhl.get_page_id_to_index(item_id)
        if not bhl.is_contiguous_range(
            item_id, start_pages[0], end_pages[0], page_mapping
        ):
            return None
        page_metadata = bhl.get_page_metadata(start_pages[0])
        return bhl.PossiblePage(
            start_pages[0],
            art.start_page,
            contains_text=True,
            contains_end_page=True,
            year_matches=True,
            ocr_text=page_metadata.get("OCRText", ""),
            item_id=item_id,
            min_distance=0,
        )
    else:
        if cg is None:
            return None
        title_ids = cg.get_bhl_title_ids()
        if not title_ids:
            return None
        contains_text = [art.title]
        start_page = art.start_page
        end_page = art.end_page
        possible_pages = sorted(
            bhl.find_possible_pages(
                title_ids,
                year=year,
                start_page=start_page,
                end_page=end_page,
                volume=art.volume,
                contains_text=contains_text,
            ),
            key=lambda page: page.sort_key(),
        )
    confident_pages = [page for page in possible_pages if page.is_confident]
    # Even if there is more than one, just pick the first
    if len(confident_pages) >= 1:
        return confident_pages[0]
    if not cfg.manual_mode:
        return None
    if cg is not None and not cg.should_have_bhl_link_in_year(year):
        return None
    if possible_pages:
        getinput.print_header(art.name)
        art.display()
        print(f"Unconfirmed pages found: {possible_pages}")
        for page in possible_pages[:4]:
            if page.min_distance >= 50:
                continue
            getinput.print_header(page)
            ocr_text = page.ocr_text.replace("\n\n", "\n")
            print(ocr_text)
            art.display()
            if getinput.yes_no("Is this the correct page?"):
                return page

        def open_urls() -> None:
            for page in possible_pages[:2]:
                print(page)
                subprocess.check_call(["open", page.page_url])

        callbacks = {**art.get_adt_callbacks(), "u": open_urls}
        url_to_page = {page.page_url: page for page in possible_pages}
        choice = getinput.choose_one_by_name(list(url_to_page), callbacks=callbacks)
        if choice is None:
            return None
        print("Chose", choice, "for", art)
        return url_to_page[choice]
    return None


@LINT.add("bhl_page_from_other_articles", requires_network=True)
def infer_bhl_page_from_other_articles(
    art: Article, cfg: LintConfig = LintConfig()
) -> Iterable[str]:
    if not should_look_for_bhl_url(art):
        return
    page_id = get_inferred_bhl_page_from_articles(art, cfg)
    if page_id is None:
        return
    message = f"inferred BHL page {page_id} from other articles in {art.citation_group} {art.volume}"
    if art.url is not None:
        message += f" (replacing {art.url})"
    page_url = f"https://www.biodiversitylibrary.org/page/{page_id}"
    if cfg.autofix:
        print(f"{art}: {message}")
        art.set_or_replace_url(page_url)
    else:
        yield message
    print(page_url)


def get_inferred_bhl_page_from_articles(art: Article, cfg: LintConfig) -> int | None:
    if art.volume is None:
        if cfg.verbose:
            print(f"{art}: no volume")
        return None
    if (
        art.start_page is None
        or art.end_page is None
        or not art.start_page.isnumeric()
        or not art.end_page.isnumeric()
    ):
        if cfg.verbose:
            print(f"{art}: no numeric page range")
        return None
    cg = art.get_citation_group()
    if cg is None:
        if cfg.verbose:
            print(f"{art}: no journal")
        return None
    other_articles = list(
        Article.select_valid().filter(
            Article.citation_group == cg,
            Article.volume == art.volume,
            Article.issue == art.issue,
            Article.url != None,
            Article.series == art.series,
        )
    )
    if not other_articles:
        if cfg.verbose:
            print(f"{art}: no other articles")
        return None
    inferred_pages: dict[int, set[Article]] = {}
    for other_art in other_articles:
        if other_art.numeric_year() != art.numeric_year():
            if cfg.verbose:
                print(f"{other_art}: different year")
            continue
        if other_art.url is None:
            if cfg.verbose:
                print(f"{other_art}: no URL")
            continue
        match urlparse.parse_url(other_art.url):
            case urlparse.BhlPage(page_id):
                existing_page_id = page_id
            case urlparse.BhlPart(part_id):
                part_metadata = bhl.get_part_metadata(part_id)
                existing_page_id = int(part_metadata["StartPageID"])
            case _:
                if cfg.verbose:
                    print(f"{other_art}: {other_art.url} is not a BHL page URL")
                continue
        if other_art.start_page == art.start_page:
            inferred_pages.setdefault(existing_page_id, set()).add(other_art)
        if other_art.start_page is None or not other_art.start_page.isnumeric():
            if cfg.verbose:
                print(f"{other_art}: {other_art.start_page} is not numeric")
            continue
        diff = int(art.start_page) - int(other_art.start_page)
        page_metadata = bhl.get_page_metadata(existing_page_id)
        item_id = int(page_metadata["ItemID"])

        # Check start page
        page_mapping, pages = bhl.get_filtered_pages_and_indices(item_id)
        existing_page_idx = page_mapping.get(existing_page_id)
        if existing_page_idx is None:
            if cfg.verbose:
                print(f"{other_art}: no index for page {existing_page_id}")
            continue
        expected_page_idx = existing_page_idx + diff
        if not (0 <= expected_page_idx < len(pages)):
            if cfg.verbose:
                print(f"{other_art}: {expected_page_idx} is out of range")
            continue
        inferred_page_id = pages[expected_page_idx]["PageID"]
        if diff > 0:
            start = existing_page_id
            end = inferred_page_id
        else:
            start = inferred_page_id
            end = existing_page_id
        if not bhl.is_contiguous_range(
            item_id,
            start,
            end,
            allow_unnumbered=False,
            verbose=cfg.verbose,
            ignore_plates=True,
        ):
            if cfg.verbose:
                print(
                    f"{other_art}: {existing_page_id} and {inferred_page_id} are not"
                    " contiguous"
                )
            continue

        # Check end page
        this_art_diff = int(art.end_page) - int(art.start_page)
        expected_end_page_idx = expected_page_idx + this_art_diff
        if not (0 <= expected_end_page_idx < len(pages)):
            if cfg.verbose:
                print(
                    f"{other_art}: end page index {expected_end_page_idx} is out of range"
                )
            continue
        inferred_end_page_id = pages[expected_end_page_idx]["PageID"]
        if not bhl.is_contiguous_range(
            item_id,
            inferred_page_id,
            inferred_end_page_id,
            allow_unnumbered=False,
            verbose=cfg.verbose,
            ignore_plates=True,
        ):
            if cfg.verbose:
                print(
                    f"{other_art}: start {inferred_page_id} and end {inferred_end_page_id} are not"
                    " contiguous"
                )
            continue

        possible_pages = bhl.get_possible_pages(item_id, art.start_page)
        if inferred_page_id not in possible_pages:
            if cfg.verbose:
                print(
                    f"{other_art}: {inferred_page_id} not in possible pages {possible_pages}"
                )
            continue
        inferred_pages.setdefault(inferred_page_id, set()).add(other_art)
    if len(inferred_pages) != 1:
        if cfg.verbose:
            print(f"{art}: no single inferred page from other names ({inferred_pages})")
        if inferred_pages and cfg.interactive:
            for page_id, arts in inferred_pages.items():
                url = f"https://www.biodiversitylibrary.org/page/{page_id}"
                getinput.print_header(f"{url} ({len(arts)})")
                subprocess.check_call(["open", url])
                for other_art in arts:
                    print(f"- {other_art!r}")
            art.edit()
        return None
    (inferred_page_id,) = inferred_pages
    return inferred_page_id


# remove final period and curly quotes, italicize cyt b, other stuff
_TITLE_REGEXES = [
    # clean up HTML tags Zootaxa likes to put in
    (r"<(\/)?em>", r"<\1i>"),
    (r"<\/?(p|strong)>", ""),
    (r"<br />", ""),
    # remove stuff like final periods
    (r"(?<!\\)\.$|^\s+|\s+$|(?<=^<i>)\s+|<i><\/i>|☆", ""),
    # get rid of curly quotes
    (r'^" |(?<= )" |&quot;', '"'),
    (r"[`‘’]", "'"),
    # italicize cyt b
    (r"(?<=\bcytochrome[- ])b\b", "_b_"),
    # lowercase and normalize i tags
    (r"(<|<\/)I(?=>)", r"\1i"),
    (r"([,:();]+)<\/i>", r"</i>\1"),
    (r"<i>([,:();]+)", r"\1<i>"),
    (r"<\/i>\s+<i>|\s+", " "),
    (r"(?<![ \"'\-\(])<i>", " _"),
    (r"</i>(?![ \"'\-\),\.])", "_ "),
    (r"</?i>", "_"),
    (r"\s+", " "),
    (r'(?<=[ (])_(["\'])([A-Z][a-z \.]+)\1_(?=[ ,)])', r"\1_\2_\1"),
    (r'(?<=[ (])"_([A-Z][a-z \.]+)"(?=[ ,)])', r'_"\1"'),
    (r"(?<=[ (])_([A-Z][\.a-z ]+), ([A-Za-z\., ]+)_(?=[ ,)])", r"_\1_, _\2_"),
    (r"_([A-Z][a-z]+) \(([A-Z][a-z]+)\) ([a-z]+)_", r"_\1_ (_\2_) _\3_"),
    (r"_([A-Z][a-z]+) \(([A-Z][a-z]+)_\)", r"_\1_ (_\2_)"),
    (r"_(([A-Z][a-z]+ )?[a-z]+)([-\N{EN DASH}])([a-z]+)_", r"_\1_\3_\4_"),
    (r"_([A-Z][a-z]+)?([-\N{EN DASH}])([A-Z][a-z]+)_", r"_\1_\2_\3_"),
    (r"\)\._(?= |$)", r"_)."),
    (r"(^| )_\?([A-Z][a-z])", r"\1?_\2"),
    (r"\(_([A-Z][a-z]+)\) ([a-z]+)_", r"\(_\1_\) _\2_"),
    (r",_ ", "_, "),
    (r" _(\()?([A-Za-z]+\??)\)_( |$)", r" \1_\2_)\3"),
    (r" _([A-Za-z ]+)\?_(?= |$)", r" _\1_?"),
]


@LINT.add("title")
def check_title(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.title is None:
        return
    new_title = art.title
    for regex, replacement in _TITLE_REGEXES:
        new_title = re.sub(regex, replacement, new_title)
    yield from _maybe_clean(art, "title", new_title, cfg)
    # DOI titles tend to produce this kind of mess
    if re.search(r"[A-Z] [A-Z] [A-Z]", new_title):
        yield f"spaced caps in title {art.title!r}"
    if re.search("<(?!sub|sup|/sub|/sup)", new_title):
        yield f"possible HTML tag in {art.title!r}"
    yield from md_lint(new_title)


CONTEXTS = {"2_n": False, "n_=": True, "p_3": True}


def md_lint(text: str) -> Iterable[str]:
    italics_start = None
    last_i = len(text) - 1
    for i, c in enumerate(text):
        if c == "_":
            is_closing = None
            if i == 0:
                is_closing = False
            elif i == last_i:
                is_closing = True
            if is_closing is None:
                previous = text[i - 1]
                next = text[i + 1]
                if previous == " ":
                    is_closing = False
                elif next == " ":
                    is_closing = True
                elif previous in (
                    "(",
                    "\N{EM DASH}",
                    "\N{EN DASH}",
                    "[",
                    "'",
                    '"',
                    "-",
                    "=",
                    "/",
                    "†",
                    "?",
                    "«",
                    "„",
                ):
                    is_closing = False
                elif next in (
                    ")",
                    ",",
                    ":",
                    "\N{EM DASH}",
                    "\N{EN DASH}",
                    "-",
                    "]",
                    ";",
                    '"',
                    "'",
                    ".",
                    "?",
                    "/",
                    "»",
                ):
                    is_closing = True
                elif text[:i].endswith("Cyt") and next == "b":
                    is_closing = False  # Cyt_b_
                elif text[i + 1 :].startswith("(?)"):
                    is_closing = True
                else:
                    context = f"{previous}_{next}"
                    is_closing = CONTEXTS.get(context)
            if is_closing is False:
                if italics_start is not None:
                    yield (f"incorrectly paired underscores at position {i}: {text}")
                italics_start = i + 1
            elif is_closing is True:
                if italics_start is None:
                    yield (f"incorrectly paired underscores at position {i}: {text}")
                italics_start = None
            else:
                yield f"underscore in unexpected position at {i}: {text}"
        elif italics_start is not None:
            if c not in (" ", ".", '"', "'") and not c.isalpha():
                yield f"unexpected italicized character at {i}: {text}"


@LINT.add("journal_specific")
def journal_specific_cleanup(art: Article, cfg: LintConfig) -> Iterable[str]:
    cg = art.citation_group
    if cg is None:
        return
    if message := cg.is_year_in_range(art.numeric_year()):
        yield message
    if art.series is None and cg.get_tag(CitationGroupTag.MustHaveSeries):
        yield f"missing a series, but {cg} requires one"
    if cg.type is ArticleType.JOURNAL:
        if may_have_series := cg.get_tag(CitationGroupTag.SeriesRegex):
            if art.series is not None and not re.fullmatch(
                may_have_series.text, art.series
            ):
                yield (
                    f"series {art.series} does not match regex"
                    f" {may_have_series.text} for {cg}"
                )
        elif art.series is not None:
            yield f"is in {cg}, which does not support series"
    if cg.name == "Proceedings of the Zoological Society of London":
        year = art.numeric_year()
        if art.volume is None:
            return  # other checks will complain
        try:
            volume = int(art.volume)
        except ValueError:
            if not re.match(r"^190[1-5]-II?", art.volume):
                yield f"unrecognized PZSL volume {art.volume}"
            return
        if 1901 <= volume <= 1905:
            yield "PZSL volume between 1901 and 1905 should have -I or -II"
        elif 1831 <= volume <= 1936:
            # Some of the 1851 volume was published in 1854
            if volume not in (year, year - 1, year - 2, year - 3):
                yield (
                    f"PZSL article has mismatched volume and year: {volume} vs. {year}"
                )
        elif 107 <= volume <= 145:
            if not (1937 <= year <= 1965):
                yield (
                    f"PZSL article has mismatched volume and year: {volume} vs. {year}"
                )
        else:
            yield f"Invalid PZSL volume: {volume}"
        if 107 <= volume <= 113:
            if art.series not in ("A", "B"):
                yield "PZSL articles in volumes 107–113 must have series"
        elif art.series:
            yield "PZSL article may not have series"
    jnh = "Journal of Natural History Series "
    if cg.name.startswith(jnh):
        message = "fixing Annals and Magazine citation group"
        if cfg.autofix:
            art.series = str(int(cg.name.removeprefix(jnh)))
            art.citation_group = CitationGroup.get_or_create(
                "Annals and Magazine of Natural History"
            )
            print(f"{art}: {message}")
        else:
            yield message
    if art.issue and should_not_have_issue(art):
        message = f"{cg} article should not have issue {art.issue}"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.issue = None
        else:
            yield message


def should_not_have_issue(art: Article) -> bool:
    cg = art.citation_group
    if cg is None:
        return False
    return cg.name == "American Museum Novitates" or (
        cg.name == "Bulletin of the American Museum of Natural History"
        and art.numeric_year() > 1990
    )


@LINT.add("citation_group")
def check_citation_group(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.type is ArticleType.JOURNAL:
        if art.citation_group is None:
            yield "journal article is missing a citation group"
            return
        if art.citation_group.type is not ArticleType.JOURNAL:
            yield f"citation group {art.citation_group} is not a journal"
    elif art.type is ArticleType.BOOK:
        if art.citation_group is None:
            # should ideally error but too much backlog
            return
        if art.citation_group.type is not ArticleType.BOOK:
            yield f"citation group {art.citation_group} is not a city"
    elif art.type is ArticleType.THESIS:
        if art.citation_group is None:
            yield "thesis is missing a citation group"
            return
        if art.citation_group.type is not ArticleType.THESIS:
            yield f"citation group {art.citation_group} is not a university"
    elif art.type is ArticleType.SUPPLEMENT:
        return  # don't care
    elif art.citation_group is not None:
        yield f"should not have a citation group (type {art.type!r})"


def _clean_string_field(value: str) -> str:
    value = unicodedata.normalize("NFC", value)
    value = re.sub(r"([`’‘]|&apos;)", "'", value)
    value = re.sub(r"[“”]", '"', value)
    value = value.replace("&amp;", "&")
    return re.sub(r"\s+", " ", value)


@LINT.add("string_fields")
def check_string_fields(art: Article, cfg: LintConfig) -> Iterable[str]:
    for field in art.fields():
        value = getattr(art, field)
        if not isinstance(value, str):
            continue
        cleaned = _clean_string_field(value)
        if cleaned.isnumeric() and field in (
            "series",
            "volume",
            "issue",
            "start_page",
            "end_page",
        ):
            cleaned = cleaned.lstrip("0")
        yield from _maybe_clean(art, field, cleaned, cfg)
        if "??" in value:
            yield f"double question mark in field {field}: {value!r}"


@LINT.add("required")
def check_required_fields(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.title is None and not art.is_full_issue():
        yield "missing title"
    if (
        not art.author_tags
        and art.type is not ArticleType.SUPPLEMENT
        and not art.is_full_issue()
        # TODO: these should also have authors
        and art.kind is not ArticleKind.no_copy
    ):
        yield "missing author_tags"


DEFAULT_VOLUME_REGEX = r"(Suppl\. )?\d{1,4}"
DEFAULT_ISSUE_REGEX = r"\d{1,3}|\d{1,2}-\d{1,2}|Suppl\. \d{1,2}"


@LINT.add("journal")
def check_journal(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.type is not ArticleType.JOURNAL:
        return
    cg = art.citation_group
    if cg is None:
        yield "journal article must have a citation group"
        return
    if art.volume is not None:
        volume = art.volume.replace("–", "-")
        yield from _maybe_clean(art, "volume", volume, cfg)
        tag = cg.get_tag(CitationGroupTag.VolumeRegex)
        rgx = tag.text if tag else DEFAULT_VOLUME_REGEX
        if not re.fullmatch(rgx, volume):
            message = f"regex {tag.text!r}" if tag else "default volume regex"
            yield f"volume {volume!r} does not match {message} (CG {cg})"
    elif not art.is_in_press():
        yield "missing volume"
    if art.issue is not None:
        issue = re.sub(r"[–_]", "-", art.issue)
        issue = re.sub(r"^(\d+)/(\d+)$", r"\1–\2", issue)
        yield from _maybe_clean(art, "issue", issue, cfg)
        tag = cg.get_tag(CitationGroupTag.IssueRegex)
        rgx = tag.text if tag else DEFAULT_ISSUE_REGEX
        if not re.fullmatch(rgx, issue):
            message = f"regex {tag.text!r}" if tag else "default issue regex"
            yield f"issue {issue!r} does not match {message} (CG {cg})"


@LINT.add("pages")
def check_start_end_page(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.type is not ArticleType.JOURNAL:
        return  # TODO: similar check for chapters
    cg = art.citation_group
    if cg is None:
        return  # error emitted in check_journal()
    if art.is_full_issue():
        return
    start_page: str | None = art.start_page
    end_page: str | None = art.end_page
    if start_page is None:
        if art.article_number is None:
            yield "missing start page"
        return
    if art.is_in_press():
        if end_page is not None:
            yield "in press article has end_page"
        return
    tag = cg.get_tag(CitationGroupTag.PageRegex)
    allow_standard = tag is None or tag.allow_standard

    # Standard pages: both numeric, at most 4 digits, end >= start
    if (
        allow_standard
        and end_page is not None
        and start_page.isnumeric()
        and len(start_page) <= 4
        and end_page.isnumeric()
        and len(end_page) <= 4
    ):
        if int(end_page) < int(start_page):
            yield f"end page is before start page: {start_page}"
        return
    if tag is None:
        yield f"invalid start and end page {start_page}-{end_page}"
        return

    if end_page is None:
        if not tag.start_page_regex:
            yield "missing end_page"
            return
        if not re.fullmatch(tag.start_page_regex, start_page):
            yield (
                f"start page {start_page} does not match regex"
                f" {tag.start_page_regex} for {cg}"
            )
        return

    if not tag.pages_regex:
        yield f"invalid start and end page {start_page}-{end_page}"
        return

    if not re.fullmatch(tag.pages_regex, start_page):
        yield (
            f"start page {start_page} does not match regex {tag.pages_regex} for {cg}"
        )
    if not re.fullmatch(tag.pages_regex, end_page):
        yield f"end page {end_page} does not match regex {tag.pages_regex} for {cg}"


@LINT.add("infer_lsid")
def infer_lsid_from_names(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.numeric_year() < 2012:
        return
    tags = list(art.get_tags(art.tags, ArticleTag.LSIDArticle))
    if any(
        tag.present_in_article in (PresenceStatus.present, PresenceStatus.inferred)
        for tag in tags
    ):
        return
    new_names = list(art.get_new_names())
    if not new_names:
        return
    act_lsids = [
        clean_lsid(tag.text).casefold()
        for nam in new_names
        for tag in nam.get_tags(nam.type_tags, models.name.TypeTag.LSIDName)
    ]
    if not act_lsids:
        return
    pages = art.get_all_pdf_pages()
    cleaned_text = "".join(
        re.sub(r"\s", "", page).replace("-", "-").casefold() for page in pages
    ).replace("-", "")
    for lsid in act_lsids:
        if lsid.replace("-", "") not in cleaned_text:
            continue
        try:
            datas = get_zoobank_data_for_act(lsid)
        except requests.exceptions.HTTPError as e:
            print(f"Error retrieving ZooBank data for {lsid}: {e!r}")
            continue
        for zoobank_data in datas:
            if zoobank_data.citation_lsid:
                new_tag = ArticleTag.LSIDArticle(
                    zoobank_data.citation_lsid, PresenceStatus.inferred
                )
                message = f"adding inferred LSID: {new_tag}"
                if cfg.autofix:
                    print(f"{art}: {message}")
                    art.add_tag(new_tag)
                else:
                    yield message


@LINT.add("lsid")
def check_lsid(art: Article, cfg: LintConfig) -> Iterable[str]:
    tags = list(art.get_tags(art.tags, ArticleTag.LSIDArticle))
    if not tags:
        return
    by_status = defaultdict(set)
    by_lsid = defaultdict(set)
    for tag in tags:
        if tag.text:
            by_status[tag.present_in_article].add(tag.text)
            by_lsid[tag.text].add(tag.present_in_article)
    dupes = False
    for lsid, statuses in by_lsid.items():
        if len(statuses) > 1:
            yield f"LSID present with multiple statuses: {lsid}, {statuses}"
            dupes = True
    if dupes and cfg.autofix:
        # present > inferred > absent
        new_tags = []
        existing_tags = list(art.tags)
        for tag in existing_tags:
            if isinstance(tag, ArticleTag.LSIDArticle):
                if tag.present_in_article is PresenceStatus.absent and (
                    tag.text in by_status[PresenceStatus.present]
                    or tag.text in by_status[PresenceStatus.inferred]
                ):
                    continue
                elif (
                    tag.present_in_article is PresenceStatus.inferred
                    and tag.text in by_status[PresenceStatus.present]
                ):
                    continue
            new_tags.append(tag)
        if existing_tags != new_tags:
            print(f"{art}: changing tags")
            getinput.print_diff(existing_tags, new_tags)
            art.tags = new_tags  # type: ignore[assignment]

    if (
        not by_status[PresenceStatus.probably_absent]
        and not by_status[PresenceStatus.to_be_determined]
    ):
        return
    pages = art.get_all_pdf_pages()
    cleaned_text = "".join(
        re.sub(r"\s", "", page).replace("-", "-").casefold() for page in pages
    )

    tbd_present = []
    tbd_absent = []
    prob_absent = []
    prob_present = []
    for lsid in by_status[PresenceStatus.probably_absent]:
        if clean_lsid(lsid).casefold() in cleaned_text:
            prob_present.append(lsid)
        else:
            prob_absent.append(lsid)
    for lsid in by_status[PresenceStatus.to_be_determined]:
        if clean_lsid(lsid).casefold() in cleaned_text:
            tbd_present.append(lsid)
        else:
            tbd_absent.append(lsid)
    if tbd_present:
        yield f"LSID {', '.join(tbd_present)} is present in article"
    if tbd_absent:
        yield f"LSID {', '.join(tbd_absent)} is absent in article"
    if prob_present:
        yield f"LSID {', '.join(prob_present)} is in fact present in article"
    if prob_absent:
        yield f"LSID {', '.join(prob_absent)} is really absent in article"
    if cfg.autofix:
        new_tags = []
        existing_tags = list(art.tags)
        for tag in existing_tags:
            if isinstance(tag, ArticleTag.LSIDArticle):
                if (
                    tag.present_in_article is PresenceStatus.probably_absent
                    and tag.text in prob_absent
                ):
                    new_tags.append(
                        ArticleTag.LSIDArticle(tag.text, PresenceStatus.absent)
                    )
                elif (
                    tag.present_in_article is PresenceStatus.to_be_determined
                    and tag.text in tbd_present
                ):
                    new_tags.append(
                        ArticleTag.LSIDArticle(tag.text, PresenceStatus.present)
                    )
                elif (
                    tag.present_in_article is PresenceStatus.to_be_determined
                    and tag.text in tbd_absent
                ):
                    new_tags.append(
                        ArticleTag.LSIDArticle(tag.text, PresenceStatus.absent)
                    )
                else:
                    # Others left for manual check
                    new_tags.append(tag)
            else:
                new_tags.append(tag)
        if new_tags != existing_tags:
            print(f"{art}: adjusting LSID tags: {existing_tags} -> {new_tags}")
            art.tags = new_tags  # type: ignore[assignment]


@LINT.add("must_use_children")
def check_must_use_children(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not any(art.get_tags(art.tags, ArticleTag.MustUseChildren)):
        return
    for field in Article.clirm_backrefs:
        if (
            field is Article.parent
            or field is ArticleComment.article
            or field is models.name.NameComment.source
        ):
            continue
        refs = [obj for obj in getattr(art, field.related_name) if not obj.is_invalid()]
        if not refs:
            continue
        yield (
            f"has references in {field.model_cls.__name__}.{field.name} that should be"
            f" moved to children: {refs}"
        )
        if cfg.interactive:
            for ref in refs:
                if not hasattr(ref, field.name):
                    continue
                ref.display()
                ref.fill_field(field.name)
    for field in Article.derived_fields:
        refs = art.get_derived_field(field.name)
        if not refs:
            continue
        num_refs = [
            get_num_referencing_tags(ref, art, interactive=cfg.interactive)
            for ref in refs
        ]
        refs = [ref for ref, num in zip(refs, num_refs, strict=True) if num > 0]
        if not refs:
            continue
        yield (
            f"has references in tags in {field.name} that should be moved to children:"
            f" {refs}"
        )


def get_num_referencing_tags(
    model: BaseModel, art: Article, *, interactive: bool = True
) -> int:
    num_references = 0
    for field in model.clirm_fields.values():
        if not isinstance(field, ADTField):
            continue

        def map_fn(existing: Article) -> Article:
            if existing != art:
                return existing
            nonlocal num_references
            num_references += 1
            if not interactive:
                return existing
            model.display()
            choice = Article.getter(None).get_one(callbacks=model.get_adt_callbacks())
            if choice is None:
                return existing
            return choice

        model.map_tags_by_type(field, Article, map_fn)
    return num_references


def _maybe_clean(
    art: Article, field: str, cleaned: Any, cfg: LintConfig
) -> Iterable[str]:
    current = getattr(art, field)
    if cleaned != current:
        message = f"clean {field} {current!r} -> {cleaned!r}"
        if cfg.autofix:
            print(f"{art}: {message}")
            setattr(art, field, cleaned)
        else:
            yield message


@LINT.add("specify_authors")
def specify_authors(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.type is ArticleType.SUPPLEMENT:
        return
    if art.has_tag(ArticleTag.InitialsOnly):
        return
    if not art.has_initials_only_authors():
        return
    if cfg.autofix:
        art.specify_authors()
    yield "has initials-only authors"


@cache
def get_cgs_with_dois() -> set[int]:
    arts = Article.select_valid().filter(
        Article.type == ArticleType.JOURNAL, Article.doi != None
    )
    return {art.citation_group.id for art in arts if art.citation_group is not None}


@LINT.add(
    "find_doi",
    disabled=True,
    requires_network=True,
    clear_caches=get_cgs_with_dois.cache_clear,
)  # false positives
def find_doi(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.doi is not None or art.has_tag(ArticleTag.JSTOR):
        return
    if art.citation_group is None:
        return
    if art.citation_group.id not in get_cgs_with_dois():
        return
    doi = models.article.add_data.get_doi_from_crossref(art)
    if doi is None:
        return
    message = f"found DOI {doi}"
    if cfg.autofix and not LINT.is_ignoring_lint(art, "find_doi"):
        art.doi = doi
        print(f"{art}: {message}")
    else:
        yield message


def has_valid_children(art: Article) -> bool:
    return Article.add_validity_check(art.article_set).count() > 0


def dupe_fixer(key_val: Hashable, arts: list[Article], cfg: LintConfig) -> None:
    if not cfg.interactive:
        return
    getinput.print_header(key_val)
    for art in arts:
        print(repr(art))
        art.add_to_history(None)  # for merge()
    name_to_art = {art.name: art for art in arts}

    def open_all() -> None:
        for art in arts:
            art.openf()

    def full_data() -> None:
        for art in arts:
            getinput.print_header(art)
            art.full_data()

    def display() -> None:
        for art in arts:
            print(repr(art))

    def mark_as_partial_page() -> None:
        for art in arts:
            art.add_tag(ArticleTag.PartialPage)

    while True:
        choice = getinput.get_with_completion(
            [art.name for art in arts],
            history_key=key_val,
            disallow_other=True,
            callbacks={
                "open_all": open_all,
                "full_data": full_data,
                "display": display,
                "partial": mark_as_partial_page,
            },
        )
        if not choice:
            break
        if choice in name_to_art:
            name_to_art[choice].edit()


@LINT.add_duplicate_finder(
    "dupe_doi",
    query=Article.select_valid().filter(
        Article.doi != None,
        Article.type != ArticleType.SUPPLEMENT,
        Article.kind != ArticleKind.alternative_version,
    ),
    fixer=dupe_fixer,
)
def dupe_doi(art: Article) -> str | None:
    if art.type is ArticleType.SUPPLEMENT:
        return None
    if art.kind == ArticleKind.alternative_version:
        return None
    if art.has_tag(ArticleTag.GeneralDOI):
        return None
    return art.doi


@LINT.add_duplicate_finder(
    "dupe_journal",
    query=Article.select_valid().filter(
        Article.type == ArticleType.JOURNAL,
        Article.kind != ArticleKind.alternative_version,
    ),
    fixer=dupe_fixer,
)
def dupe_journal(art: Article) -> tuple[object, ...] | None:
    if art.kind == ArticleKind.alternative_version:
        return None
    if art.is_in_press():
        return None
    if art.has_tag(ArticleTag.Incomplete):
        return None
    if art.has_tag(ArticleTag.PartialPage):
        return None
    if art.citation_group is None:
        return None
    return (
        art.citation_group.name,
        art.series,
        art.volume,
        art.issue,
        art.start_page,
        art.end_page,
        art.article_number,
    )


@LINT.add_duplicate_finder(
    "dupe_journal",
    query=Article.select_valid().filter(
        Article.type == ArticleType.JOURNAL,
        Article.kind != ArticleKind.alternative_version,
    ),
    fixer=dupe_fixer,
)
def dupe_journal_with_title(art: Article) -> tuple[object, ...] | None:
    if art.kind == ArticleKind.alternative_version:
        return None
    if art.is_in_press():
        return None
    if art.has_tag(ArticleTag.Incomplete):
        return None
    if art.citation_group is None:
        return None
    return (
        art.citation_group.name,
        art.title,
        art.volume,
        art.issue,
        art.start_page,
        art.end_page,
    )


@LINT.add("data_from_doi", requires_network=True)
def data_from_doi(art: Article, cfg: LintConfig) -> Iterable[str]:
    if (
        art.doi is None
        or art.kind is ArticleKind.alternative_version
        or art.has_tag(ArticleTag.GeneralDOI)
    ):
        return
    data = models.article.add_data.expand_doi_json(art.doi)
    if not data:
        return
    yield from _check_doi_title(art, data)
    yield from _check_doi_volume(art, data)
    yield from _check_doi_issue(art, data, cfg)
    yield from _check_doi_start_page(art, data)
    yield from _check_doi_end_page(art, data)
    yield from _check_doi_article_number(art, data, cfg)
    yield from _check_doi_isbn(art, data, cfg)
    yield from _check_doi_tags(art, data, cfg)
    yield from _check_doi_authors(art, data, cfg)


def _check_doi_title(art: Article, data: dict[str, Any]) -> Iterable[str]:
    if "title" not in data or art.title is None:
        return
    title = data["title"]
    title = re.sub(r"^[IVXCL]+\.\s*[–—\-]?", "", title)
    title = re.sub(r"Citation for this article.*", "", title)
    title = re.sub(r"^Chapter \d+\.?\s*", "", title)
    title = re.sub(r"<sup>\d+</sup>", "", title)
    title = title.replace(' class="HeadingRunIn"', "")
    simplified_doi = helpers.simplify_string(title, clean_words=False).rstrip("*")
    if not simplified_doi:
        return
    simplified_art = helpers.simplify_string(art.title, clean_words=False).rstrip("*")
    if simplified_doi == simplified_art:
        return
    # Red List data puts the authors at the end of the title
    if (
        art.doi is not None
        and art.doi.startswith("10.2305/IUCN.UK")
        and simplified_doi.startswith(simplified_art)
    ):
        return
    if not LINT.is_ignoring_lint(art, "data_from_doi"):
        getinput.diff_strings(simplified_doi, simplified_art)
    yield f"title mismatch: {data['title']} (DOI) vs. {art.title} (article)"


def _check_doi_volume(art: Article, data: dict[str, Any]) -> Iterable[str]:
    if (
        not data.get("volume")
        or art.volume is None
        or data["volume"].lstrip("0") == art.volume
    ):
        return
    # Happens for Am. Mus. Novitates
    if "issue" in data and data["issue"] == art.volume:
        return
    if not data["volume"].isnumeric() or not art.volume.isnumeric():
        return
    # Probably a totally different convention
    if abs(int(data["volume"]) - int(art.volume)) > 1000:
        return
    yield f"volume mismatch: {data['volume']} (DOI) vs. {art.volume} (article)"


def _check_doi_issue(
    art: Article, data: dict[str, Any], cfg: LintConfig
) -> Iterable[str]:
    if not data.get("issue"):
        return
    if art.issue is None:
        if not should_not_have_issue(art):
            message = f"adding issue {data['issue']} from DOI"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.issue = data["issue"]
            else:
                yield message
        return
    doi_issue = data["issue"].replace("/", "-").replace("–", "-")
    if doi_issue == "1":
        return
    if doi_issue.lstrip("0") == art.issue:
        return
    if not doi_issue.isnumeric() or not art.issue.isnumeric():
        return
    # Probably a totally different convention
    if abs(int(doi_issue) - int(art.issue)) > 10:
        return
    yield f"issue mismatch: {data['issue']} (DOI) vs. {art.issue} (article)"


def _check_doi_start_page(art: Article, data: dict[str, Any]) -> Iterable[str]:
    if not data.get("start_page") or art.start_page is None:
        return
    if not data["start_page"].isnumeric():
        return
    if data["start_page"].lstrip("0") == art.start_page:
        return
    if data["start_page"] == data.get("end_page") and art.start_page != art.end_page:
        return
    yield f"start page mismatch: {data['start_page']} (DOI) vs. {art.start_page} (article)"


def _check_doi_end_page(art: Article, data: dict[str, Any]) -> Iterable[str]:
    if not data.get("end_page") or art.start_page is None:
        return
    if data["end_page"] == 1 or not data["end_page"].isnumeric():
        return
    if data["end_page"].lstrip("0") == art.end_page:
        return
    if data.get("start_page") == data["end_page"] and art.start_page != art.end_page:
        return
    yield f"end page mismatch: {data['end_page']} (DOI) vs. {art.end_page} (article)"


def _check_doi_article_number(
    art: Article, data: dict[str, Any], cfg: LintConfig
) -> Iterable[str]:
    if not data.get("article_number"):
        return
    if art.article_number is None:
        message = f"adding article number {data['article_number']} from DOI"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.article_number = data["article_number"]
        else:
            yield message
    elif data["article_number"] != art.article_number and not re.fullmatch(
        rf"(e|[a-z]+\.){re.escape(data["article_number"])}", art.article_number
    ):
        yield f"article number mismatch: {data['article_number']} (DOI) vs. {art.article_number} (article)"


def _check_doi_isbn(
    art: Article, data: dict[str, Any], cfg: LintConfig
) -> Iterable[str]:
    if "isbn" not in data:
        return
    existing = art.get_identifier(ArticleTag.ISBN)
    if existing is None:
        message = f"adding ISBN {data['isbn']} from DOI"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.add_tag(ArticleTag.ISBN(data["isbn"]))
        else:
            yield message
        return
    existing_cleaned = existing.replace("-", "").replace(" ", "")
    new_cleaned = data["isbn"].replace("-", "").replace(" ", "")
    if existing_cleaned != new_cleaned:
        yield f"ISBN mismatch: {data['isbn']} (DOI) vs. {existing} (article)"


def _check_doi_tags(
    art: Article, data: dict[str, Any], cfg: LintConfig
) -> Iterable[str]:
    for tag in data["tags"]:
        if tag not in art.tags:
            message = f"adding tag {tag} from DOI"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.add_tag(tag)
            else:
                yield message


def _check_doi_authors(
    art: Article, data: dict[str, Any], cfg: LintConfig
) -> Iterable[str]:
    if "author_tags" not in data:
        return
    doi_authors = data["author_tags"]
    if len(doi_authors) != len(art.author_tags):
        return
    new_authors = []
    for doi_author, tag in zip(doi_authors, art.author_tags, strict=True):
        art_author = tag.person
        if is_more_specific_than(doi_author, art_author):
            new_authors.append(AuthorTag.Author(doi_author.create_person()))
        else:
            new_authors.append(tag)
    if new_authors == list(art.author_tags):
        return
    message = "updating authors from DOI"
    getinput.print_diff(art.author_tags, new_authors)
    if cfg.autofix:
        print(f"{art}: {message}")
        art.author_tags = new_authors  # type: ignore[assignment]
    else:
        yield message


@LINT.add("raw_page_regex")
def must_have_raw_page_regex(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.has_tag(ArticleTag.RawPageRegex) or art.parent is not None:
        return
    has_raw_pages: list[object] = []
    for nam in art.get_new_names_with_children():
        if any(
            page.is_raw for page in models.name.page.parse_page_text(nam.page_described)
        ):
            has_raw_pages.append(nam)
    for ce in art.get_classification_entries_with_children():
        if any(page.is_raw for page in models.name.page.parse_page_text(ce.page)):
            has_raw_pages.append(ce)
    if len(has_raw_pages) < 3:
        return
    yield f"missing RawPageRegex tag, but has {len(has_raw_pages)} raw pages (examples: {has_raw_pages[:3]})"


@LINT.add("consistent_bhl_pages")
def check_consistent_bhl_pages(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not art.has_bhl_link():
        return
    bhl_page_to_page_to_objects: dict[
        str,
        dict[
            str,
            list[models.classification_entry.ClassificationEntry | models.name.Name],
        ],
    ] = {}
    page_to_bhl_page_to_objects: dict[
        str,
        dict[
            str,
            list[models.classification_entry.ClassificationEntry | models.name.Name],
        ],
    ] = {}
    for nam in art.get_new_names():
        for tag in nam.type_tags:
            if isinstance(tag, models.name.TypeTag.AuthorityPageLink):
                bhl_page_to_page_to_objects.setdefault(tag.url, {}).setdefault(
                    tag.page, []
                ).append(nam)
                page_to_bhl_page_to_objects.setdefault(tag.page, {}).setdefault(
                    tag.url, []
                ).append(nam)
    for ce in art.get_classification_entries():
        for tag in ce.tags:
            if isinstance(
                tag, models.classification_entry.ClassificationEntryTag.PageLink
            ):
                bhl_page_to_page_to_objects.setdefault(tag.url, {}).setdefault(
                    tag.page, []
                ).append(ce)
                page_to_bhl_page_to_objects.setdefault(tag.page, {}).setdefault(
                    tag.url, []
                ).append(ce)

    for bhl_page, page_to_objects in bhl_page_to_page_to_objects.items():
        if len(page_to_objects) == 1:
            continue
        yield f"multiple pages link to BHL page {bhl_page}: {pprint.pformat(page_to_objects)}"
    for page, bhl_page_to_objects in page_to_bhl_page_to_objects.items():
        if len(bhl_page_to_objects) == 1:
            continue
        yield f"multiple BHL pages link to page {page}: {pprint.pformat(bhl_page_to_objects)}"


def lint_referenced_text(text: str, prefix: str = "") -> Generator[str, None, str]:
    for ref in helpers.extract_sources(text):
        if ("/" in ref or "#" in ref) and re.search(r"^[a-z]+[/#]", ref):
            continue  # "n/123" style
        if ":" in ref:
            continue  # Status automatically changed from available to partially_suppressed because of PartiallySuppressedBy({Diademodon-conserved.pdf: International Commission on Zoological Nomenclature. 1985. Opinion 1324. _Diademodon_ Seeley, 1894 and _Diademodon tetragonus_ Seeley, 1894 conserved by the suppression of Cynochampsa _Owen_, 1859 and _Cynochampsa laniaria_ Owen, 1859 (Reptilia, Therapsida). Bulletin of Zoological Nomenclature 42(2):185-187.}, 'Official Index No. 1151')
        try:
            art = Article.select().filter(Article.name == ref).get()
        except Article.DoesNotExist:
            yield f"{prefix}referenced article {ref} does not exist"
        else:
            if target := art.get_redirect_target():
                text = text.replace(f"{{{ref}}}", f"{{{target.name}}}")
    return text
