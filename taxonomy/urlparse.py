"""Parsing and formatting URLs."""

import re
import urllib.parse
from abc import abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

BHL_DOMAINS = {"biodiversitylibrary.org", "www.biodiversitylibrary.org"}
JSTOR_DOMAINS = {"www.jstor.org", "jstor.org"}
DEPRECATED_DOMAINS = {"biostor.org"}
DEPRECATED_TOPLEVEL = {"hul.harvard.edu"}
# not www.ingentaconnect.com, at least some articles (Herpetological Journal) have no DOI
# not www.publish.csiro.au, books don't have DOIs
SHOULD_HAVE_DOI_DOMAINS = {
    "www.sciencedirect.com",
    "link.springer.com",
    "www.tandfonline.com",
    "www.pnas.org",
    "www.degruyter.com",
    "linkinghub.elsevier.com",
    "www.nature.com",
    "www.ncbi.nlm.nih.gov",
    "www.springerlink.com",
    "www.mapress.com",
    "academic.oup.com",
    "www.checklist.org.br",
}
SHOULD_HAVE_DOI_TOPLEVEL = {"oxfordjournals.org"}
GALLICA_DOMAIN = "gallica.bnf.fr"


@dataclass
class ParsedUrl:
    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    def lint(self) -> Iterable[str]:
        yield from []


@dataclass
class BhlUrl(ParsedUrl):
    pass


@dataclass
class BhlBibliography(BhlUrl):
    bibliography_id: int

    def __str__(self) -> str:
        return (
            f"https://www.biodiversitylibrary.org/bibliography/{self.bibliography_id}"
        )


@dataclass
class BhlItem(BhlUrl):
    item_id: int

    def __str__(self) -> str:
        return f"https://www.biodiversitylibrary.org/item/{self.item_id}"


@dataclass
class BhlPage(BhlUrl):
    page_id: int

    def __str__(self) -> str:
        return f"https://www.biodiversitylibrary.org/page/{self.page_id}"


@dataclass
class BhlPart(BhlUrl):
    part_id: int

    def __str__(self) -> str:
        return f"https://www.biodiversitylibrary.org/part/{self.part_id}"


@dataclass
class GoogleBooksUrl(ParsedUrl):
    """
    Google Books URLs have two useful parameters:
    - id: the volume ID
    - pg: the page number to link to directly

    We allow links with only "id" to link to the whole volume, and links
    with "id" and "pg" to link to a specific page.

    Other URL parameters include:
    - newbks: something about the "new" Google Books interface
    - newbks_redir: similar
    - dq: search query that led to the book
    - lpg: the page number that the user originally landed on
    - f: unknown
    - q: unknown

    We strip out these parameters to make the links simpler and more consistent.

    """


@dataclass
class GoogleBooksVolume(GoogleBooksUrl):
    volume_id: str

    def __str__(self) -> str:
        return f"https://books.google.com/books?id={self.volume_id}"


@dataclass
class GoogleBooksPage(GoogleBooksUrl):
    volume_id: str
    page: str

    def __str__(self) -> str:
        return f"https://books.google.com/books?id={self.volume_id}&pg={self.page}"


@dataclass
class GallicaUrl(ParsedUrl):
    volume_id: str

    def lint(self) -> Iterable[str]:
        if not re.fullmatch(r"bpt6k\d+[a-z]?", self.volume_id):
            yield f"invalid volume ID {self.volume_id!r}"


@dataclass
class GallicaVolume(GallicaUrl):
    suffixes: Sequence[str] = ()

    def lint(self) -> Iterable[str]:
        yield from super().lint()
        if self.suffixes:
            yield f"unexpected suffixes {self.suffixes!r}"

    def __str__(self) -> str:
        suf = "".join(f".{suffix}" for suffix in self.suffixes)
        return f"https://{GALLICA_DOMAIN}/ark:/12148/{self.volume_id}{suf}"


@dataclass
class GallicaPage(GallicaUrl):
    page_number: str
    suffixes: Sequence[str] = ()

    def lint(self) -> Iterable[str]:
        yield from super().lint()
        if len(self.suffixes) > 1:
            yield f"too many suffixes {self.suffixes!r}"
        elif self.suffixes:
            suf = self.suffixes[0]
            if suf not in {"image", "item"}:
                yield f"invalid item type {suf!r}"

    def __str__(self) -> str:
        suf = "".join(
            f".{suffix}"
            for suffix in self.suffixes
            if not _can_skip_gallica_suffix(suffix)
        )
        return f"https://{GALLICA_DOMAIN}/ark:/12148/{self.volume_id}/{self.page_number}{suf}"


def _can_skip_gallica_suffix(suffix: str) -> bool:
    if suffix == "langEN":
        return True
    if suffix.startswith("r="):
        return True
    return False


@dataclass
class HDLUrl(ParsedUrl):
    hdl: str
    query: str | None = None

    def __str__(self) -> str:
        base = f"https://hdl.handle.net/{self.hdl}"
        if self.query is not None:
            return f"{base}?{self.query}"
        return base


@dataclass
class JStorUrl(ParsedUrl):
    jstor_id: str

    def __str__(self) -> str:
        return f"https://www.jstor.org/stable/{self.jstor_id}"


@dataclass
class DOIURL(ParsedUrl):
    doi: str

    def __str__(self) -> str:
        return f"https://doi.org/{self.doi}"


@dataclass
class PMCUrl(ParsedUrl):
    pmc_id: str

    def __str__(self) -> str:
        return f"https://www.ncbi.nlm.nih.gov/pmc/articles/{self.pmc_id}"


@dataclass
class PubMedUrl(ParsedUrl):
    pmid: str

    def __str__(self) -> str:
        return f"https://www.ncbi.nlm.nih.gov/pubmed/{self.pmid}"


@dataclass
class DeepBlueUrl(ParsedUrl):
    handle: str
    suffix: str

    def __str__(self) -> str:
        return f"https://deepblue.lib.umich.edu/bitstream/{self.handle}/{self.suffix}"


@dataclass
class OtherUrl(ParsedUrl):
    split_url: urllib.parse.SplitResult

    def __str__(self) -> str:
        return urllib.parse.urlunsplit(self.split_url)

    def lint(self) -> Iterable[str]:
        if self.split_url.scheme == "":
            yield "URL has no scheme"
        elif self.split_url.scheme not in {"http", "https"}:
            yield f"URL has unknown scheme {self.split_url.scheme}"
        if self.split_url.netloc == "":
            yield "URL has no netloc"
        elif "." not in self.split_url.netloc:
            yield f"URL has invalid netloc {self.split_url.netloc!r}"
        if self.split_url.netloc in BHL_DOMAINS:
            yield "invalid BHL URL"
        if self.split_url.netloc in SHOULD_HAVE_DOI_DOMAINS:
            yield "URL should be replaced with a DOI"
        for toplevel in SHOULD_HAVE_DOI_TOPLEVEL:
            if self.split_url.netloc.endswith(toplevel):
                yield "URL should be replaced with a DOI"
        if self.split_url.netloc in DEPRECATED_DOMAINS:
            yield f"URL uses deprecated domain {self.split_url.netloc}"
        for toplevel in DEPRECATED_TOPLEVEL:
            if self.split_url.netloc.endswith(toplevel):
                yield f"URL uses deprecated top-level domain {toplevel}"
        if self.split_url.netloc in JSTOR_DOMAINS:
            yield "invalid JSTOR URL"
        if is_google_domain(self.split_url.netloc):
            yield "unrecognized Google URL"
        if self.split_url.netloc == GALLICA_DOMAIN:
            yield "invalid Gallica URL"


def parse_url(url: str) -> ParsedUrl:
    split = urllib.parse.urlsplit(url)
    if split.netloc in BHL_DOMAINS:
        match = re.fullmatch(r"/([a-z]+)/(\d+)", split.path)
        if match is not None:
            match match.group(1):
                case "bibliography":
                    return BhlBibliography(int(match.group(2)))
                case "item" | "itempdf":
                    return BhlItem(int(match.group(2)))
                case "page":
                    return BhlPage(int(match.group(2)))
                case "part" | "partpdf":
                    return BhlPart(int(match.group(2)))
    elif re.fullmatch(r"books\.google(\.co)?\.[a-z]+", split.netloc):
        query_dict = urllib.parse.parse_qs(split.query)
        if (
            "id" in query_dict
            and "pg" in query_dict
            and len(query_dict["id"]) == 1
            and len(query_dict["pg"]) == 1
        ):
            return GoogleBooksPage(query_dict["id"][0], query_dict["pg"][0])
        elif (
            "id" in query_dict and "pg" not in query_dict and len(query_dict["id"]) == 1
        ):
            return GoogleBooksVolume(query_dict["id"][0])
    elif re.fullmatch(r"(www\.)?google(\.co)?\.[a-z]+", split.netloc):
        match = re.fullmatch(r"/books/edition/[^/]+/([^/]+)", split.path)
        if match is not None:
            book_id = match.group(1)
            query_dict = urllib.parse.parse_qs(split.query)
            if "pg" in query_dict:
                return GoogleBooksPage(book_id, query_dict["pg"][0])
            else:
                return GoogleBooksVolume(book_id)
    elif split.netloc in JSTOR_DOMAINS:
        if match := re.fullmatch(r"/stable/(\d+)", split.path):
            return JStorUrl(match.group(1))
    elif split.netloc == "hdl.handle.net":
        return HDLUrl(split.path.lstrip("/"), split.query if split.query else None)
    elif split.netloc == "deepblue.lib.umich.edu":
        if match := re.fullmatch(r"/handle/(.+)", split.path):
            return HDLUrl(match.group(1))
        elif match := re.fullmatch(r"/bitstream/([^/]+/[^/]+)/(.+)", split.path):
            return DeepBlueUrl(match.group(1), match.group(2))
    elif split.netloc in ("dx.doi.org", "doi.org"):
        return DOIURL(split.path.lstrip("/"))
    elif split.netloc == "www.bioone.org":
        match = re.fullmatch(r"/doi/(?:full|abs|pdf)/(.+)", split.path)
        if match is not None:
            return DOIURL(match.group(1))
    elif split.netloc == "onlinelibrary.wiley.com":
        match = re.fullmatch(r"/doi/(.+?)/(abs|full|pdf|abstract)", split.path)
        if match is not None:
            return DOIURL(match.group(1))
    elif split.netloc == GALLICA_DOMAIN and split.path.startswith("/ark:/12148/"):
        text = split.path.removeprefix("/ark:/12148/")
        if "/" in text:
            volume_id, tail = text.split("/", 1)
            page_number, *suffixes = tail.split(".")
            return GallicaPage(volume_id, page_number, suffixes)
        else:
            volume_id, *suffixes = text.split(".")
            return GallicaVolume(volume_id, suffixes)
    elif split.netloc == "www.ncbi.nlm.nih.gov":
        match = re.fullmatch(r"/pmc/articles/(PMC\d+)/?", split.path)
        if match is not None:
            return PMCUrl(match.group(1))
        match = re.fullmatch(r"/pubmed/(\d+)", split.path)
        if match is not None:
            return PubMedUrl(match.group(1))

    # TODO: other domains for which to consider parsing more specifically:
    # - archive.org
    # - hathitrust.org
    return OtherUrl(split)


def is_valid_url(url: str) -> bool:
    parsed = parse_url(url)
    return not any(parsed.lint())


def is_google_domain(domain: str) -> bool:
    return re.fullmatch(r"(?:books\.|www\.)?google\.[a-z]+", domain) is not None
