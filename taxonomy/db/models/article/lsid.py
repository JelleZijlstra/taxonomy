"""Extract ZooBank publication LSIDs from article text."""

import re
from dataclasses import dataclass

from taxonomy.db import helpers

_UUID_PATTERN = (
    r"[0-9a-f]{8}\s*-\s*[0-9a-f]{4}\s*-\s*[0-9a-f]{4}"
    r"\s*-\s*[0-9a-f]{4}\s*-\s*[0-9a-f]{12}"
)
_PUBLICATION_LSID_RE = re.compile(
    rf"urn\s*:\s*lsid\s*:\s*zoobank\s*\.\s*org\s*:\s*pub\s*:\s*"
    rf"(?P<uuid>(?<![0-9a-f]){_UUID_PATTERN}(?![0-9a-f]))",
    re.IGNORECASE,
)
_EXPLICIT_SELF_MARKER_RE = re.compile(
    r"(?:"
    r"(?:the\s+)?lsid(?:\s*\([^)]{0,80}\))?\s+(?:for|of)\s+"
    r"(?:this\s+)?publication\s+is\s*:?"
    r"|this\s+(?:publication|published\s+work|work|article).{0,140}"
    r"registered\s+(?:in|with)\s+zoobank(?:\s+under(?:\s+lsid)?)?"
    r"|this\s+article\s+is\s+registered\s+in\s+zoobank\s+under"
    r"|publication\s+lsid\s*:?"
    r"|zoobank\s+registration\s*:?"
    r")\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class PublicationLSIDMatch:
    """A publication LSID whose surrounding text identifies the current article."""

    lsid: str
    page: int
    evidence: str
    context: str


def _canonicalize_uuid(text: str) -> str:
    return re.sub(r"\s", "", text).upper()


def _normalize_for_title_match(text: str) -> str:
    simplified = helpers.simplify_string(text, clean_words=False)
    return re.sub(r"[^a-z0-9]", "", simplified)


def _title_follows_lsid(title: str, text_after_lsid: str) -> bool:
    normalized_title = _normalize_for_title_match(title)
    if len(normalized_title) < 30:
        return False
    # A moderately long prefix tolerates subtitles omitted from publisher headers while
    # remaining distinctive enough to identify the current article.
    title_anchor = normalized_title[: min(100, len(normalized_title))]
    normalized_after = _normalize_for_title_match(text_after_lsid[:6000])
    return title_anchor in normalized_after[:2500]


def extract_safe_publication_lsid(
    pages: list[str], title: str | None
) -> PublicationLSIDMatch | None:
    """Return a publication LSID only when the PDF identifies it as its own.

    All publication LSIDs in the PDF are considered, including references. Multiple
    distinct UUIDs are therefore ambiguous and rejected. A unique UUID is accepted only
    from the first two PDF pages with either an explicit self-identifying phrase or, on
    page one, a publisher-header position immediately before the article's title.
    """
    raw_matches = [
        (page_number, page, match)
        for page_number, page in enumerate(pages, start=1)
        for match in _PUBLICATION_LSID_RE.finditer(page)
    ]
    if not raw_matches:
        return None
    lsids = {_canonicalize_uuid(match.group("uuid")) for _, _, match in raw_matches}
    if len(lsids) != 1:
        return None
    lsid = next(iter(lsids))
    for page_number, page, match in raw_matches:
        if page_number > 2:
            continue
        preceding = re.sub(
            r"\s+", " ", page[max(0, match.start() - 350) : match.start()]
        )
        if _EXPLICIT_SELF_MARKER_RE.search(preceding):
            evidence = "explicit self-identification"
        elif (
            page_number == 1
            and title is not None
            and _title_follows_lsid(title, page[match.end() :])
        ):
            evidence = "first-page header before matching title"
        else:
            continue
        context = re.sub(
            r"\s+", " ", page[max(0, match.start() - 180) : match.end() + 180]
        ).strip()
        return PublicationLSIDMatch(lsid, page_number, evidence, context)
    return None
