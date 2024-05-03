"""Parsing and formatting URLs."""

import re
import urllib.parse
from abc import abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass

BHL_DOMAINS = {"biodiversitylibrary.org", "www.biodiversitylibrary.org"}
DEPRECATED_DOMAINS = {"biostor.org"}
# Also consider: www.mapress.com, www.springerlink.com, linkinghub.elsevier.com, link.springer.com,
# mbe.oxfordjournals.org, www.pnas.org, www.ingentaconnect.com
SHOULD_HAVE_DOI_DOMAINS = {"www.sciencedirect.com"}


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
    pass


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
        if self.split_url.netloc in BHL_DOMAINS:
            yield "invalid BHL URL"
        if self.split_url.netloc in SHOULD_HAVE_DOI_DOMAINS:
            yield "URL should be replaced with a DOI"
        if self.split_url.netloc in DEPRECATED_DOMAINS:
            yield f"URL uses deprecated domain {self.split_url.netloc}"


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
    elif re.fullmatch(r"books\.google\.[a-z]+", split.netloc):
        query_dict = urllib.parse.parse_qs(split.query)
        if query_dict.keys() == {"id"} and len(query_dict["id"]) == 1:
            return GoogleBooksVolume(query_dict["id"][0])
        elif (
            query_dict.keys() == {"id", "pg"}
            and len(query_dict["id"]) == 1
            and len(query_dict["pg"]) == 1
        ):
            return GoogleBooksPage(query_dict["id"][0], query_dict["pg"][0])
        # TODO: if there are other URL parameters, drop them
    # TODO: other domains for which to consider parsing more specifically:
    # - google.com (replace with books.google.com)
    # - archive.org
    # - hathitrust.org
    # - gallica.bnf.fr
    return OtherUrl(split)
