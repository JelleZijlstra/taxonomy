"""Conservative, offline page evidence for source quotations.

Matching never changes quotation text. Unmapped PDF pages participate in ambiguity
checks, and BHL searches use only explicitly known pages with already cached OCR.
"""

import functools
import json
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import Levenshtein

from taxonomy import config, search, urlparse
from taxonomy.db import helpers, url_cache
from taxonomy.db.url_cache import CacheDomain


@dataclass(frozen=True)
class PageText:
    page: str | None
    text: str
    page_link: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "text", normalize(self.text))


@dataclass(frozen=True)
class QuoteMatch:
    page: PageText | None = None
    ambiguous: bool = False


def normalize(text: str) -> str:
    text = search.preprocess_page_text(text).casefold()
    return " ".join(re.findall(r"[^\W_]+", text))


def extract_page_annotation(text: str) -> tuple[str, str] | None:
    """Recognize trailing editorial annotations, never leading source references.

    For example, Moreau et al. (1946) prefix entries with ``[p. 516]`` referring
    to Allen's checklist, not to the page containing the quotation.
    """
    if text.rstrip().endswith("@"):
        return None  # Square brackets are explicitly marked as original.
    annotation = r"\[pp?\.\s*([0-9]+(?:\s*[-–]\s*[0-9]+)?|[ivxlcdm]+)\]"
    matches = list(re.finditer(annotation, text))
    if len(matches) != 1:
        return None
    match = matches[0]
    before, after = text[: match.start()].strip(), text[match.end() :].strip()
    if not before or after:
        return None
    page = re.sub(r"\s*[-–]\s*", "-", match[1])
    if not page[0].isdigit() and not helpers.is_valid_roman_numeral(page):
        return None
    if "-" in page:
        start, end = map(int, page.split("-"))
        if start > end:
            return None
    return before or after, page


def page_from_containing_quotes(
    text: str, donors: Iterable[tuple[str, str]]
) -> str | None:
    needle = normalize(text)
    if not needle:
        return None
    pages = set()
    for quote, page in donors:
        haystack = normalize(quote)
        if needle == haystack:
            pages.add(page)
        elif (
            len(needle) >= 20
            and len(needle.split()) >= 3
            and f" {needle} " in f" {haystack} "
        ):
            # A substring of a multi-page quotation does not locate its page.
            if not re.fullmatch(r"[0-9]+|[ivxlcdm]+", page):
                return None
            pages.add(page)
    return next(iter(pages)) if len(pages) == 1 else None


def infer_pdf_page_numbers(pages: Sequence[str]) -> dict[int, str]:
    """Use at least three nearby margin numbers agreeing on physical-page offset.

    Never extrapolate beyond those anchors. Conflicting offsets and repeated printed
    numbers (bound works or numbering restarts) are not usable mappings.
    """
    candidates: dict[int, set[int]] = defaultdict(set)
    for index, text in enumerate(pages):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if len(lines) < 5:
            continue
        for line in [*lines[:2], *lines[-2:]]:
            if re.fullmatch(r"[1-9][0-9]{0,4}", line):
                candidates[index].add(int(line))
    anchors = {i: next(iter(ns)) for i, ns in candidates.items() if len(ns) == 1}
    runs: list[list[int]] = []
    for index, number in anchors.items():
        if (
            runs
            and index - runs[-1][-1] <= 2
            and number - index == anchors[runs[-1][-1]] - runs[-1][-1]
        ):
            runs[-1].append(index)
        else:
            runs.append([index])
    mapping = {}
    for run in runs:
        if len(run) < 3:
            continue
        offset = anchors[run[0]] - run[0]
        for index in range(run[0], run[-1] + 1):
            expected = index + offset
            if index in candidates and candidates[index] != {expected}:
                continue
            mapping[index] = str(expected)
    # Count even rejected anchors: a second numbering sequence can be very short.
    occurrences: dict[str, set[int]] = defaultdict(set)
    for index, numbers in candidates.items():
        for number in numbers:
            occurrences[str(number)].add(index)
    for index, printed_page in mapping.items():
        occurrences[printed_page].add(index)
    return {i: number for i, number in mapping.items() if len(occurrences[number]) == 1}


def _match_strength(needle: str, haystack: str) -> int:
    """Return 2 for a strong match, 1 for an ambiguous near-match, else 0."""
    if f" {needle} " in f" {haystack} ":
        return 2
    words = needle.split()
    if len(needle) < 80 or len(needle) > 2000 or len(words) < 12:
        return 0
    tokens = haystack.split()
    if len(set(words) & set(tokens)) < len(set(words)) * 0.8:
        return 0
    strong_limit = min(3, len(needle) // 50)
    loose_limit = min(6, len(needle) // 20)
    numbers = re.findall(r"\d+", needle)
    result = 0
    for count in (len(words) - 1, len(words), len(words) + 1):
        for start in range(len(tokens) - count + 1):
            window = " ".join(tokens[start : start + count])
            distance = Levenshtein.distance(needle, window, score_cutoff=loose_limit)
            if distance <= loose_limit:
                result = 1
                if distance <= strong_limit and re.findall(r"\d+", window) == numbers:
                    return 2
    return result


def find_quote_page(
    text: str, pages: Sequence[PageText], *, partial: bool = False
) -> QuoteMatch:
    """Require one strong match and no second plausible match, even if unmapped."""
    needle = normalize(text)
    if len(needle) < 30 or len(needle.split()) < 5:
        return QuoteMatch()
    if partial and (len(needle) < 80 or len(needle.split()) < 12):
        return QuoteMatch()
    matches = [
        (page, strength)
        for page in pages
        if (strength := _match_strength(needle, page.text))
    ]
    if not matches:
        return QuoteMatch()
    if len(matches) != 1:
        return QuoteMatch(ambiguous=True)
    page, strength = matches[0]
    if strength != 2 or page.page is None:
        return QuoteMatch(ambiguous=True)
    return QuoteMatch(page=page)


@functools.lru_cache(maxsize=32)
def _read_pdf(path: Path, mtime_ns: int, size: int) -> tuple[PageText, ...]:
    pages = path.read_text().split("\f")
    mapping = infer_pdf_page_numbers(pages)
    return tuple(PageText(mapping.get(i), text) for i, text in enumerate(pages))


def cached_pdf_pages(article_id: int) -> tuple[PageText, ...]:
    path = config.get_options().pdf_text_path / f"{article_id}.txt"
    try:
        stat = path.stat()
        # Bound memory and work on unusually large or corrupt cached extracts.
        if stat.st_size > 20_000_000:
            return ()
        return _read_pdf(path, stat.st_mtime_ns, stat.st_size)
    except OSError, UnicodeError:
        return ()


def _cached_bhl(domain: CacheDomain, identifier: int) -> dict[str, Any] | None:
    raw = url_cache.get_cached_value(domain, str(identifier))
    if raw is None:
        return None
    try:
        data = json.loads(raw)
        if data["Status"] != "ok" or len(data["Result"]) != 1:
            return None
        result = data["Result"][0]
        return result if isinstance(result, dict) else None
    except ValueError, KeyError, TypeError:
        return None


def cached_bhl_pages(
    known_pages: set[str], links: Iterable[tuple[str, str]], source_url: str | None
) -> tuple[PageText, ...]:
    """Resolve only known page numbers/links, without calling BHL or filling caches."""
    page_ids: dict[int, set[str]] = defaultdict(set)
    for page, link in links:
        parsed = urlparse.parse_url(link)
        if isinstance(parsed, urlparse.BhlPage) and page in known_pages:
            page_ids[parsed.page_id].add(page)
    metadata = None
    if source_url:
        parsed = urlparse.parse_url(source_url)
        if isinstance(parsed, urlparse.BhlItem):
            metadata = _cached_bhl(CacheDomain.bhl_item, parsed.item_id)
        elif isinstance(parsed, urlparse.BhlPart):
            metadata = _cached_bhl(CacheDomain.bhl_part, parsed.part_id)
        elif isinstance(parsed, urlparse.BhlPage):
            page_data = _cached_bhl(CacheDomain.bhl_page, parsed.page_id)
            if page_data and page_data.get("ItemID"):
                metadata = _cached_bhl(CacheDomain.bhl_item, int(page_data["ItemID"]))
    if metadata:
        for page_data in metadata.get("Pages", []):
            for number in page_data.get("PageNumbers", []):
                page = number.get("Number", "")
                if number.get("Prefix") in ("Page", "p.") and page in known_pages:
                    page_ids[int(page_data["PageID"])].add(page)
    # Do not resolve a printed page to one of several scans or vice versa.
    counts = Counter(page for values in page_ids.values() for page in values)
    result = []
    if len(page_ids) > 40:
        return ()
    for page_id, values in page_ids.items():
        if len(values) != 1:
            continue
        (page,) = values
        if counts[page] != 1:
            continue
        data = _cached_bhl(CacheDomain.bhl_page, page_id)
        if data and isinstance(data.get("OcrText"), str) and data["OcrText"].strip():
            result.append(
                PageText(
                    page,
                    data["OcrText"],
                    f"https://www.biodiversitylibrary.org/page/{page_id}",
                )
            )
    return tuple(result)


def clear_caches() -> None:
    _read_pdf.cache_clear()
