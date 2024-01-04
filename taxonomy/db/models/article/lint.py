"""

Lint steps for Articles.

"""

import bisect
import functools
import re
import unicodedata
import urllib.parse
from collections import defaultdict
from collections.abc import Callable, Generator, Iterable, Sequence
from typing import Any

import requests

from .... import getinput
from ....apis.zoobank import clean_lsid, get_zoobank_data_for_act, is_valid_lsid
from ... import helpers, models
from ...constants import ArticleKind, ArticleType, DateSource
from ..base import ADTField, BaseModel, LintConfig
from ..citation_group import CitationGroup, CitationGroupTag
from ..issue_date import IssueDate
from .article import Article, ArticleComment, ArticleTag, PresenceStatus
from .name_parser import get_name_parser

Linter = Callable[[Article, LintConfig], Iterable[str]]
IgnorableLinter = Callable[[Article, LintConfig], Generator[str, None, set[str]]]

LINTERS = []
DISABLED_LINTERS = []


def get_ignored_lints(art: Article) -> set[str]:
    tags = art.get_tags(art.tags, ArticleTag.IgnoreLint)
    return {tag.label for tag in tags}


def make_linter(
    label: str, *, disabled: bool = False
) -> Callable[[Linter], IgnorableLinter]:
    def decorator(linter: Linter) -> IgnorableLinter:
        @functools.wraps(linter)
        def wrapper(art: Article, cfg: LintConfig) -> Generator[str, None, set[str]]:
            issues = list(linter(art, cfg))
            if not issues:
                return set()
            ignored_lints = get_ignored_lints(art)
            if label in ignored_lints:
                return {label}
            for issue in issues:
                yield f"{art}: {issue} [{label}]"
            return set()

        if disabled:
            DISABLED_LINTERS.append(wrapper)
        else:
            LINTERS.append(wrapper)
        return wrapper

    return decorator


@make_linter("name")
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


@make_linter("path")
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


@make_linter("type_kind")
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
                    art.kind = ArticleKind.reference  # type: ignore
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


@make_linter("year")
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


@make_linter("precise_date")
def check_precise_date(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.citation_group is None:
        return
    if not art.citation_group.has_tag(CitationGroupTag.MustHavePreciseDate):
        return
    if art.new_names.count() == 0:
        return
    if art.year is not None and "-" not in art.year:
        yield f"is in {art.citation_group} but has imprecise date {art.year}"


@make_linter("infer_precise_date")
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


_JSTOR_URL_PREFIX = "http://www.jstor.org/stable/"
_JSTOR_DOI_PREFIX = "10.2307/"


def is_valid_hdl(hdl: str) -> bool:
    return bool(re.fullmatch(r"^\d+(\.\d+)?\/(\d+)$", hdl))


def is_valid_doi(doi: str) -> bool:
    return bool(re.fullmatch(r"^10\.[A-Za-z0-9\.\/\[\]<>\-;:_()+]+$", doi))


@make_linter("must_have_url")
def check_must_have_url(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.citation_group is None:
        return
    if not art.citation_group.has_tag(CitationGroupTag.MustHaveURL):
        return
    url = art.geturl()
    if url is not None:
        return
    yield f"has no URL, but is in {art.citation_group}"


@make_linter("url")
def clean_up_url(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.doi is not None:
        cleaned = urllib.parse.unquote(art.doi)
        yield from _maybe_clean(art, "doi", cleaned, cfg)
        if not is_valid_doi(art.doi):
            yield f"invalid doi {art.doi!r}"
    if art.doi is None and art.url is not None:
        doi = _infer_doi_from_url(art.url)
        if doi is not None:
            message = f"inferred doi {doi} from url {art.url}"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.doi = doi
                art.url = None
            else:
                yield message

    if hdl := art.getIdentifier(ArticleTag.HDL):
        if not is_valid_hdl(hdl):
            yield f"invalid HDL {hdl!r}"
    elif art.url is not None:
        hdl = _infer_hdl_from_url(art.url)
        if hdl is not None:
            message = f"inferred HDL {hdl} from url {art.url}"
            if cfg.autofix:
                print(f"{art}: {message}")
                art.add_tag(ArticleTag.HDL(hdl))
                art.url = None
            else:
                yield message

    if jstor_id := art.getIdentifier(ArticleTag.JSTOR):
        if len(jstor_id) < 4 or not jstor_id.isnumeric():
            yield f"invalid JSTOR id {jstor_id!r}"
    else:
        if art.url is not None:
            if art.url.startswith(_JSTOR_URL_PREFIX):
                # put JSTOR in
                jstor_id = art.url.removeprefix(_JSTOR_URL_PREFIX)
                message = f"inferred JStor id {jstor_id} from url {art.url}"
                if cfg.autofix:
                    print(f"{art}: {message}")
                    art.add_tag(ArticleTag.JSTOR(jstor_id))
                    art.url = None
                else:
                    yield message
        if art.doi is not None:
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


def _infer_doi_from_url(url: str) -> str | None:
    if url.startswith("http://dx.doi.org/"):
        return url[len("http://dx.doi.org/") :]

    if match := re.search(
        r"^http:\/\/www\.bioone\.org\/doi\/(full|abs|pdf)\/(.*)$", url
    ):
        return match.group(2)

    if match := re.search(
        r"^http:\/\/onlinelibrary\.wiley\.com\/doi\/(.*?)\/(abs|full|pdf|abstract)$",
        url,
    ):
        return match.group(1)
    return None


def _infer_hdl_from_url(url: str) -> str | None:
    if url.startswith("http://hdl.handle.net/"):
        return url[22:]
    if match := re.search(
        r"^http:\/\/(digitallibrary\.amnh\.org\/dspace|deepblue\.lib\.umich\.edu)\/handle\/(.*)$",
        url,
    ):
        return match.group(2)
    return None


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


@make_linter("title")
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


@make_linter("journal_specific")
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


@make_linter("citation_group")
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


@make_linter("string_fields")
def check_string_fields(art: Article, cfg: LintConfig) -> Iterable[str]:
    for field in art.fields():
        value = getattr(art, field)
        if not isinstance(value, str):
            continue
        cleaned = _clean_string_field(value)
        yield from _maybe_clean(art, field, cleaned, cfg)
        if "??" in value:
            yield f"double question mark in field {field}: {value!r}"


@make_linter("required")
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


@make_linter("journal")
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


@make_linter("pages")
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


@make_linter("tags")
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


@make_linter("infer_lsid")
def infer_lsid_from_names(art: Article, cfg: LintConfig) -> Iterable[str]:
    if art.numeric_year() < 2012:
        return
    tags = list(art.get_tags(art.tags, ArticleTag.LSIDArticle))
    if any(
        tag.present_in_article in (PresenceStatus.present, PresenceStatus.inferred)
        for tag in tags
    ):
        return
    new_names = list(art.new_names)
    if not new_names:
        return
    act_lsids = []
    for nam in new_names:
        nam_tags = list(nam.get_tags(nam.type_tags, models.name.TypeTag.LSIDName))
        for tag in nam_tags:
            act_lsids.append(clean_lsid(tag.text).casefold())
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


@make_linter("lsid")
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


@make_linter("must_use_children")
def check_must_use_children(art: Article, cfg: LintConfig) -> Iterable[str]:
    if not any(art.get_tags(art.tags, ArticleTag.MustUseChildren)):
        return
    for field in Article._meta.backrefs:
        if (
            field is Article.parent
            or field is ArticleComment.article
            or field is models.name.NameComment.source
        ):
            continue
        refs = list(getattr(art, field.backref))
        if not refs:
            continue
        yield (
            f"has references in {field.model.__name__}.{field.name} that should be"
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
    for field in model._meta.fields.values():
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


def run_linters(
    art: Article, cfg: LintConfig, *, include_disabled: bool = False
) -> Iterable[str]:
    if include_disabled:
        linters = [*LINTERS, *DISABLED_LINTERS]
    else:
        linters = [*LINTERS]

    used_ignores = set()
    for linter in linters:
        used_ignores |= yield from linter(art, cfg)
    actual_ignores = get_ignored_lints(art)
    unused = actual_ignores - used_ignores
    if unused:
        if cfg.autofix:
            tags = art.tags or ()
            new_tags = []
            for tag in tags:
                if isinstance(tag, ArticleTag.IgnoreLint) and tag.label in unused:
                    print(f"{art}: removing unused IgnoreLint tag: {tag}")
                else:
                    new_tags.append(tag)
            art.tags = new_tags  # type: ignore
        else:
            yield f"{art}: has unused IgnoreLint tags {', '.join(unused)}"
