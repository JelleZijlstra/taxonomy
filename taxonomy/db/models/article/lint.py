"""

Lint steps for Articles.

"""
from collections.abc import Iterable, Callable
import re
import urllib.parse
from typing import Any
from .article import Article, ArticleTag
from .name_parser import get_name_parser
from ..citation_group import CitationGroup
from ...constants import ArticleKind, ArticleType

Linter = Callable[[Article, bool], Iterable[str]]


def check_name_parser(art: Article, autofix: bool = True) -> Iterable[str]:
    parser = get_name_parser(art.name)
    if parser.errorOccurred():
        parser.printErrors()
        yield f"{art}: name failed to parse"
    if parser.extension:
        if art.kind is not ArticleKind.electronic:
            yield f"{art}: non-electronic article (kind {art.kind!r}) should not have a file extension"
    else:
        if art.kind is ArticleKind.electronic:
            yield f"{art}: electronic article should have a file extension"


def check_path(art: Article, autofix: bool = True) -> Iterable[str]:
    if art.kind is ArticleKind.electronic:
        if art.path is None or art.path == "NOFILE":
            yield f"{art}: electronic article should have a path"
    else:
        if art.path is not None:
            message = f"{art}: non-electronic article (kind {art.kind!r}) should have no path, but has {art.path}"
            if autofix:
                print(message)
                art.path = None
            else:
                yield message


def check_type_and_kind(art: Article, autofix: bool = True) -> Iterable[str]:
    # The difference between kind and type is:
    # * kind is about how this article is stored in the database (electronic copy,
    #   physical copy, etc.)
    # * type is about what kind of publication it is (journal, book, etc.)
    # Thus redirect should primarily be a *kind*. We have the *type* too for legacy
    # reasons but *kind* should be primary.
    if art.type is ArticleType.REDIRECT and art.kind is not ArticleKind.redirect:
        yield f"{art}: conflicting signals on whether it is a redirect"
    if art.type is ArticleType.ERROR:
        yield f"{art}: type is ERROR"


def check_year(art: Article, autofix: bool = True) -> Iterable[str]:
    if not art.year:
        return
    # use en dashes
    year = art.year.replace("-", "–")

    # "November 2012" -> "2012"
    if match := re.match(r"([a-zA-Z]+)\s+(\d{4})", year):
        year = match.group(2)

    # remove spaces around the dash
    if match := re.match(r"(\d{4})\s+–\s+(\d{4})", year):
        year = f"{match.group(1)}–{match.group(2)}"

    # 2014-02-13 -> 2014
    if match := re.match(r"(\d{4})–\d{2}–\d{2}", year):
        year = match.group(1)

    yield from _maybe_clean(art, "year", year, autofix)

    if art.year != "undated" and not re.fullmatch(r"^\d{4}(–\d{4})?$", art.year):
        yield f"{art}: invalid year {art.year!r}"


_JSTOR_URL_PREFIX = "http://www.jstor.org/stable/"
_JSTOR_DOI_PREFIX = "10.2307/"


def is_valid_hdl(hdl: str) -> bool:
    return bool(re.fullmatch(r"^\d+(\.\d+)?\/(\d+)$", hdl))


def is_valid_doi(doi: str) -> bool:
    return bool(re.fullmatch(r"^10\.[A-Za-z0-9\.\/\[\]<>\-;:_()+]+$", doi))


def clean_up_url(art: Article, autofix: bool = True) -> Iterable[str]:
    if art.doi is not None:
        cleaned = urllib.parse.unquote(art.doi)
        yield from _maybe_clean(art, "doi", cleaned, autofix)
        if not is_valid_doi(art.doi):
            yield f"{art}: invalid doi {art.doi!r}"
    if art.doi is None and art.url is not None:
        doi = _infer_doi_from_url(art.url)
        if doi is not None:
            message = f"{art}: inferred doi {doi} from url {art.url}"
            if autofix:
                print(message)
                art.doi = doi
                art.url = None
            else:
                yield message

    if hdl := art.getIdentifier(ArticleTag.HDL):
        if not is_valid_hdl(hdl):
            yield f"{art}: invalid HDL {hdl!r}"
    elif art.url is not None:
        hdl = _infer_hdl_from_url(art.url)
        if hdl is not None:
            message = f"{art}: inferred HDL {hdl} from url {art.url}"
            if autofix:
                print(message)
                art.add_tag(ArticleTag.HDL(hdl))
                art.url = None
            else:
                yield message

    if jstor_id := art.getIdentifier(ArticleTag.JSTOR):
        if len(jstor_id) < 4 or not jstor_id.isnumeric():
            yield f"{art}: invalid JSTOR id {jstor_id!r}"
    else:
        if art.url is not None:
            if art.url.startswith(_JSTOR_URL_PREFIX):
                # put JSTOR in
                jstor_id = art.url.removeprefix(_JSTOR_URL_PREFIX)
                message = f"{art}: inferred JStor id {jstor_id} from url {art.url}"
                if autofix:
                    print(message)
                    art.add_tag(ArticleTag.JSTOR(jstor_id))
                    art.url = None
                else:
                    yield message
        if art.doi is not None:
            if art.doi.startswith(_JSTOR_DOI_PREFIX):
                jstor_id = art.doi.removeprefix(_JSTOR_DOI_PREFIX).removeprefix("/")
                message = f"{art}: inferred JStor id {jstor_id} from doi {art.doi} (CG {art.citation_group})"
                if autofix:
                    print(message)
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
    (r"</?i>", "_"),
]


def check_title(art: Article, autofix: bool = True) -> Iterable[str]:
    if art.title is None:
        return
    new_title = art.title
    for regex, replacement in _TITLE_REGEXES:
        new_title = re.sub(regex, replacement, new_title)
    yield from _maybe_clean(art, "title", new_title, autofix)
    # DOI titles tend to produce this kind of mess
    if re.search(r"[A-Z] [A-Z] [A-Z]", art.title):
        yield f"{art}: spaced caps in title {art.title!r}"


def journal_specific_cleanup(art: Article, autofix: bool = True) -> Iterable[str]:
    cg = art.citation_group
    if cg is None:
        return
    if cg.name == "Proceedings of the Zoological Society of London":
        year = art.numeric_year()
        if year is None:
            return
        try:
            volume = int(art.volume)
        except ValueError:
            yield f"{art}: unrecognized PZSL volume {art.volume}"
            return
        if volume < 1800:
            # PZSL volumes are numbered by year, but the web version numbers them starting
            # with 1831 = 1.
            new_volume = str(volume + 1830)
            yield from _maybe_clean(art, "volume", new_volume, autofix)
        elif year > 1980:
            # This is not strictly correct because many issues were published
            # later than their nominal year, but it is a lot closer to being correct than 2009.
            yield from _maybe_clean(art, "year", art.volume, autofix)
    jnh = "Journal of Natural History Series "
    if cg.name.startswith(jnh):
        message = f"{art}: fixing Annals and Magazine citation group"
        if autofix:
            art.series = str(int(cg.name.removeprefix(jnh)))
            art.citation_group = CitationGroup.get_or_create(
                "Annals and Magazine of Natural History"
            )
            print(message)
        else:
            yield message


def check_citation_group(art: Article, autofix: bool = True) -> Iterable[str]:
    if art.type is ArticleType.JOURNAL:
        if art.citation_group is None:
            yield f"{art}: journal article is missing a citation group"
            return
        if art.citation_group.type is not ArticleType.JOURNAL:
            yield f"{art}: citation group {art.citation_group} is not a journal"
    elif art.type is ArticleType.BOOK:
        if art.citation_group is None:
            # should ideally error but too much backlog
            return
        if art.citation_group.type is not ArticleType.BOOK:
            yield f"{art}: citation group {art.citation_group} is not a city"
    elif art.type is ArticleType.THESIS:
        if art.citation_group is None:
            yield f"{art}: thesis is missing a citation group"
            return
        if art.citation_group.type is not ArticleType.THESIS:
            yield f"{art}: citation group {art.citation_group} is not a university"
    elif art.citation_group is not None:
        yield f"{art}: should not have a citation group (type {art.type!r})"


def _maybe_clean(
    art: Article, field: str, cleaned: Any, autofix: bool
) -> Iterable[str]:
    current = getattr(art, field)
    if cleaned != current:
        message = f"{art}: clean {field} {current!r} -> {cleaned!r}"
        if autofix:
            print(message)
            setattr(art, field, cleaned)
        else:
            yield message


LINTERS: list[Linter] = [
    check_name_parser,
    check_path,
    check_type_and_kind,
    check_year,
    clean_up_url,
    check_title,
    journal_specific_cleanup,
]
