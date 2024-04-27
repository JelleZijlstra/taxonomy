"""

Lint steps for Articles.

"""

import bisect
import re
import subprocess
import unicodedata
import urllib.parse
from collections import defaultdict
from collections.abc import Collection, Iterable, Sequence
from typing import Any

import httpx
import requests

from taxonomy import getinput
from taxonomy.apis import bhl
from taxonomy.apis.zoobank import clean_lsid, get_zoobank_data_for_act, is_valid_lsid
from taxonomy.db import helpers, models
from taxonomy.db.constants import ArticleKind, ArticleType, DateSource

from ..base import ADTField, BaseModel, LintConfig
from ..citation_group.cg import CitationGroup, CitationGroupTag
from ..citation_group.lint import get_biblio_pages
from ..issue_date import IssueDate
from ..lint import IgnoreLint, Lint
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


LINT = Lint[Article](get_ignores, remove_unused_ignores)


@LINT.add("name")
def check_name(art: Article, cfg: LintConfig) -> Iterable[str]:
    # Names are restricted to printable ASCII because a long time ago I stored
    # files on a file system that didn't handle non-ASCII properly. It's probably
    # safe to lift this restriction by now though.
    if not re.fullmatch(r"^[ -~]+$", art.name):
        yield "name contains invalid characters"
    parser = get_name_parser(art.name)
    if parser.errorOccurred():
        parser.printErrors()
        yield "name failed to parse"
    if parser.extension:
        if not art.kind.is_electronic():
            yield (
                f"non-electronic article (kind {art.kind!r}) should not have a"
                " file extension"
            )
    else:
        if art.kind.is_electronic():
            yield "electronic article should have a file extension"


@LINT.add("path")
def check_path(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.kind.is_electronic():
        if art.path is None or art.path == "NOFILE":
            yield "electronic article should have a path"
    else:
        if art.path is not None:
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
    if not art.year:
        return
    if art.kind is ArticleKind.alternative_version:
        return
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
    if inferred is not None and inferred != art.year:
        # Ignore obviously wrong ones (though eventually we should retire this)
        if inferred.startswith("20") and art.numeric_year() < 1990:
            return
        is_more_specific = helpers.is_more_specific_date(inferred, art.year)
        if is_more_specific:
            message = f"tags yield more specific date {inferred} instead of {art.year}"
        else:
            message = f"year mismatch: inferred {inferred}, actual {art.year}"
        if cfg.autofix and is_more_specific:
            print(f"{art}: {message}")
            art.year = inferred
        else:
            yield message
    if issue is not None and issue != art.issue:
        message = f"issue mismatch: inferred {issue}, actual {art.issue}"
        if cfg.autofix and art.issue is None:
            print(f"{art}: {message}")
            art.issue = issue
        else:
            yield message


@LINT.add("precise_date")
def check_precise_date(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.citation_group is None:
        return
    if not art.citation_group.has_tag(CitationGroupTag.MustHavePreciseDate):
        return
    if art.get_new_names().count() == 0:
        return
    if art.year is not None and "-" not in art.year:
        yield f"is in {art.citation_group} but has imprecise date {art.year}"


@LINT.add("infer_precise_date")
def infer_precise_date(art: Article, cfg: LintConfig) -> Iterable[str]:
    if (
        art.citation_group is None
        or art.volume is None
        or art.start_page is None
        or art.year is None
        or not art.start_page.isnumeric()
        or "-" in art.year
    ):
        return
    # If there is a DOI, we can get more reliable data
    if art.doi is not None:
        return
    siblings = [
        art
        for art in Article.select_valid().filter(
            Article.citation_group == art.citation_group,
            Article.series == art.series,
            Article.volume == art.volume,
            Article.year.contains("-"),
        )
        if art.start_page.isnumeric()
    ]
    if len(siblings) <= 1:
        return
    siblings = sorted(siblings, key=lambda art: int(art.start_page))
    index = bisect.bisect_left(
        siblings, int(art.start_page), key=lambda art: int(art.start_page)
    )
    if index == 0 or index == len(siblings):
        return
    if siblings[index - 1].year != siblings[index].year:
        return
    message = (
        f"inferred publication date of {siblings[index].year} based on position between"
        f" {siblings[index - 1]!r} and {siblings[index]!r}"
    )
    if cfg.autofix:
        print(f"{art}: {message}")
        art.year = siblings[index].year
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


@LINT.add("url")
def check_url(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.url is None:
        return
    hdl = _infer_hdl_from_url(art.url)
    if hdl is not None:
        message = f"inferred HDL {hdl} from url {art.url}"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.add_tag(ArticleTag.HDL(hdl))
            art.url = None
        else:
            yield message
        return

    if match := re.fullmatch(_JSTOR_URL_REGEX, art.url):
        # put JSTOR in
        jstor_id = match.group(1)
        message = f"inferred JStor id {jstor_id} from url {art.url}"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.add_tag(ArticleTag.JSTOR(jstor_id))
            art.url = None
        else:
            yield message
        return

    parsed_url = bhl.parse_possible_bhl_url(art.url)
    stringified = str(parsed_url)
    if stringified != art.url:
        message = f"reformatted url to {parsed_url} from {art.url}"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.url = stringified
        else:
            yield message

    match parsed_url:
        case bhl.ParsedUrl(bhl.UrlType.biostor_ref, _):
            if (bhl_page := get_inferred_bhl_page(art, cfg)) is not None:
                message = f"inferred BHL page {bhl_page} from url {art.url}"
                if cfg.autofix:
                    print(f"{art}: {message}")
                    art.url = bhl_page.page_url
                else:
                    yield message
            elif (bhl_url := get_bhl_url_from_biostor(parsed_url.payload)) is not None:
                message = f"inferred BHL page {bhl_url} from url {art.url}"
                if cfg.autofix:
                    print(f"{art}: {message}")
                    art.url = bhl_url
                else:
                    yield message

    if parsed_url.url_type in (
        bhl.UrlType.biostor_ref,
        bhl.UrlType.other_bhl,
        bhl.UrlType.other_biostor,
    ):
        yield f"unacceptable URL type {parsed_url.url_type} for {art.url}"


def get_bhl_url_from_biostor(biostor_id: str) -> str | None:
    response = httpx.get(f"https://biostor.org/reference/{biostor_id}")
    response.raise_for_status()
    data = response.text
    # Line to match:
    # BHL: <a href="https://www.biodiversitylibrary.org/page/7784406"
    if match := re.search(
        r"BHL: <a href=\"https://www\.biodiversitylibrary\.org/page/(\d+)\"", data
    ):
        return str(bhl.ParsedUrl(bhl.UrlType.bhl_page, match.group(1)))
    return None


@LINT.add("doi")
def check_doi(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.doi is None:
        return
    cleaned = urllib.parse.unquote(art.doi)
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


@LINT.add("infer_doi")
def infer_doi(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.doi is None and art.url is not None:
        doi = infer_doi_from_url(art.url)
        if doi is not None:
            message = f"inferred doi {doi} from url {art.url}"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.doi = doi
                art.url = None
            else:
                yield message


DOI_EXTRACTION_REGEXES = [
    r"https?:\/\/(?:dx\.)?doi\.org\/(10\..+)",
    r"https?:\/\/www\.bioone\.org\/doi\/(?:full|abs|pdf)\/(.*)",
    r"https?:\/\/onlinelibrary\.wiley\.com\/doi\/(.*?)\/(abs|full|pdf|abstract)",
]


def infer_doi_from_url(url: str) -> str | None:
    for rgx in DOI_EXTRACTION_REGEXES:
        if match := re.match(rgx, url):
            return match.group(1)
    return None


def _infer_hdl_from_url(url: str) -> str | None:
    for prefix in ("http://hdl.handle.net/", "https://hdl.handle.net/"):
        if url.startswith(prefix):
            return url.removeprefix(prefix)
    if match := re.search(
        r"^http:\/\/(digitallibrary\.amnh\.org\/dspace|deepblue\.lib\.umich\.edu)\/handle\/(.*)$",
        url,
    ):
        return match.group(2)
    return None


@LINT.add("bhl_item_from_bibliography")
def bhl_item_from_bibliography(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.url is None:
        return
    parsed = bhl.parse_possible_bhl_url(art.url)
    if parsed.url_type is not bhl.UrlType.bhl_bibliography:
        return
    metadata = bhl.get_title_metadata(int(parsed.payload))
    if "Items" in metadata and len(metadata["Items"]) == 1:
        item_id = metadata["Items"][0]["ItemID"]
        new_url = f"https://www.biodiversitylibrary.org/item/{item_id}"
        message = f"inferred BHL item {item_id} from bibliography {art.url}"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.url = new_url
        else:
            yield message


@LINT.add("bhl_part_from_page")
def bhl_part_from_page(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.url is None or art.title is None:
        return
    parsed = bhl.parse_possible_bhl_url(art.url)
    if parsed.url_type is not bhl.UrlType.bhl_page:
        return
    metadata = bhl.get_page_metadata(int(parsed.payload))
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
        if part_metadata["StartPageID"] == parsed.payload:
            matching_part = part
            break
    if matching_part is None:
        return
    message = f"inferred BHL part {matching_part['PartID']} from page {art.url}"
    if cfg.autofix:
        print(f"{art}: {message}")
        art.url = f"https://www.biodiversitylibrary.org/part/{matching_part['PartID']}"
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
        parsed = bhl.parse_possible_bhl_url(tag.url)
        if parsed.url_type is bhl.UrlType.bhl_page:
            bhl_page_ids.add(int(parsed.payload))
    return bhl_page_ids


@LINT.add("must_have_bhl_from_names")
def must_have_bhl_url_from_names(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not should_look_for_bhl_url(art):
        return
    bhl_page_ids = _get_bhl_page_ids_from_names(art)
    if not bhl_page_ids:
        return
    yield f"has new names with BHL page IDs {bhl_page_ids} but no BHL URL"


@LINT.add("bhl_page_from_names")
def infer_bhl_page_from_names(
    art: Article, cfg: LintConfig, verbose: bool = False
) -> Iterable[str]:
    if not should_look_for_bhl_url(art):
        if verbose:
            print(f"{art}: not looking for BHL URL")
        return
    bhl_page_ids = _get_bhl_page_ids_from_names(art)
    if not bhl_page_ids:
        if verbose:
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
        if verbose:
            print(f"{art}: no BHL item IDs from names")
        return
    item_id = item_ids.pop()
    item_metadata = bhl.get_item_metadata(item_id)
    if art.title is None:
        if verbose:
            print(f"{art}: no title")
    elif item_metadata is not None and "Parts" in item_metadata:
        for part in item_metadata["Parts"]:
            simplified_part_title = helpers.simplify_string(part["Title"])
            simplified_my_title = helpers.simplify_string(art.title)
            if simplified_part_title != simplified_my_title:
                if verbose:
                    print(
                        f"{art}: title {art.title!r} ({simplified_my_title!r}) does not match part title"
                        f" {part['Title']!r} ({simplified_part_title!r})"
                    )
                continue
            part_id = part["PartID"]
            part_metadata = bhl.get_part_metadata(part_id)
            if part_metadata is None:
                if verbose:
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
            else:
                if verbose:
                    print(
                        f"{art}: not all known pages ({bhl_page_ids}) are in part {part_id} {part_page_ids}"
                    )
    else:
        if verbose:
            print(f"{art}: no item metadata for {item_id}")

    if art.start_page is None or art.end_page is None:
        if verbose:
            print(f"{art}: no start or end page")
        return

    start_page = art.start_page
    end_page = art.end_page
    possible_start_pages = bhl.get_possible_pages(item_id, start_page)
    possible_end_pages = bhl.get_possible_pages(item_id, end_page)
    if len(possible_start_pages) != len(possible_end_pages):
        if verbose:
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
            if verbose:
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
            if verbose:
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
    match bhl.parse_possible_bhl_url(art.url).url_type:
        case (
            bhl.UrlType.bhl_page
            | bhl.UrlType.bhl_item
            | bhl.UrlType.bhl_part
            | bhl.UrlType.bhl_bibliography
        ):
            return True
    return False


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
            if art.addyear < "2024" and not art.get_new_names().count():
                return False
            return True
        case ArticleType.CHAPTER | ArticleType.PART:
            if art.parent is None:
                return False
            return has_bhl_url(art.parent)
    return False


@LINT.add("must_have_bhl")
def must_have_bhl_link(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not should_look_for_bhl_url(art):
        return
    if not should_require_bhl_link(art):
        return
    yield "should have BHL link"


@LINT.add("bhl_page")
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
    if art.type in (ArticleType.CHAPTER, ArticleType.PART):
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


@LINT.add("bhl_page_from_other_articles")
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
        parsed = bhl.parse_possible_bhl_url(other_art.url)
        if parsed.url_type is bhl.UrlType.bhl_page:
            existing_page_id = int(parsed.payload)
        elif parsed.url_type is bhl.UrlType.bhl_part:
            part_metadata = bhl.get_part_metadata(int(parsed.payload))
            existing_page_id = int(part_metadata["StartPageID"])
        else:
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
        else:
            if art.series is not None:
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
        else:
            if art.series:
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
    if (
        cg.name == "American Museum Novitates"
        or (
            cg.name == "Bulletin of the American Museum of Natural History"
            and art.numeric_year() > 1990
        )
    ) and art.issue:
        message = f"{cg} article should not have issue {art.issue}"
        if cfg.autofix:
            print(f"{art}: {message}")
            art.issue = None
        else:
            yield message


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
        # TODO these should also have authors
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
    else:
        if not art.is_in_press():
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
        return  # TODO similar check for chapters
    cg = art.citation_group
    if cg is None:
        return  # error emitted in check_journal()
    if art.is_full_issue():
        return
    start_page: str | None = art.start_page
    end_page: str | None = art.end_page
    if start_page is None:
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
        tags.append(tag)
    tags = sorted(set(tags))
    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {art}")
            getinput.print_diff(sorted(original_tags), tags)
        if cfg.autofix:
            art.tags = tags  # type: ignore
        else:
            yield f"{art}: needs change to tags"


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
        refs = list(getattr(art, field.related_name))
        if not refs:
            continue
        yield (
            f"has references in {field.model_cls.__name__}.{field.name} that should be"
            f" moved to children: {refs}"
        )
        if cfg.interactive:
            for ref in refs:
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
    model: BaseModel, art: Article, interactive: bool = True
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
    if art.has_tag(ArticleTag.InitialsOnly):
        return
    if not art.has_initials_only_authors():
        return
    if art.addyear < "2021" and not art.get_new_names().count():
        return
    if cfg.autofix:
        art.specify_authors()
    yield "has initials-only authors"


def has_valid_children(art: Article) -> bool:
    return Article.add_validity_check(art.article_set).count() > 0
