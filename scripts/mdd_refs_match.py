"""
Parse notes/mdd/mdd_refs.txt and try to associate each reference with an Article.

Heuristics (in order):
- DOI containment in Article.doi
- Exact title match after normalization
- Title probe + year filter (Article.title contains probe and Article.year contains year)

Outputs a per-heuristic breakdown and lists unmatched references for follow-up.
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import subprocess
import sys
import time
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

from rapidfuzz import fuzz
from rapidfuzz.distance import Levenshtein as _Lev
from unidecode import unidecode

from taxonomy.db.constants import ArticleKind
from taxonomy.db.models import Article
from taxonomy.db.models.citation_group.cg import CitationGroup

MDD_FILE = Path("notes/mdd/mdd_refs.txt")


@dataclass(frozen=True)
class Ref:
    raw: str
    year: str | None
    title: str | None
    journal: str | None
    doi: str | None


_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
_DOI_RE = re.compile(r"10\.\d{4,9}/\S+", re.IGNORECASE)
_DOI_WORD_RE = re.compile(r"\bdoi\b", re.IGNORECASE)


def normalize_doi(doi: str) -> str:
    doi = doi.strip()
    doi = re.sub(r"^(https?://(dx\.)?doi\.org/)", "", doi, flags=re.IGNORECASE)
    return doi.strip().rstrip(".)")


def normalize_title(text: str) -> str:
    t = _DOI_RE.sub(" ", text)
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t


def extract_year(text: str) -> str | None:
    m = _YEAR_RE.search(text)
    return m.group(0) if m else None


def _split_after_year(text: str) -> tuple[str | None, str | None]:
    """Return (title, trailing) by splitting after the year and walking until
    the true end of the title.

    We treat a period as the end of the title only if it is followed by
    whitespace and then an uppercase letter (start of journal), or end-of-string.
    Periods followed by lowercase letters or commas (e.g., "sp. nov.,") are
    considered part of the title and we keep scanning.
    """
    # Try both "(YYYY)" and plain "YYYY".
    m = re.search(r"\((?:19|20)\d{2}\)\.?\s*", text)
    if not m:
        m = re.search(r"\b(?:19|20)\d{2}\.?\s*", text)
    if not m:
        return None, None
    start = m.end()
    rest = text[start:]
    # Handle ISO-like dates immediately following the year, e.g. "2025-06-18. Title ..."
    # When splitting after YEAR, skip an optional "-MM-DD" sequence (with optional trailing period).
    # Support just month (YYYY-MM) or full date (YYYY-MM-DD)
    mdate = re.match(r"^\s*-\d{2}(?:-\d{2})?(?:[\.\s]|$)", rest)
    if mdate:
        rest = rest[mdate.end() :]

    def _next_non_space(idx: int) -> int | None:
        n = len(rest)
        i = idx
        while i < n and rest[i].isspace():
            i += 1
        return i if i < n else None

    boundary = None
    for i, ch in enumerate(rest):
        if ch == ".":
            nxt = _next_non_space(i + 1)
            if nxt is None:
                boundary = i
                break
            if rest[nxt].isupper():
                boundary = i
                break
    if boundary is None:
        # Fallback: whole rest until possible DOI hint or EOL
        # Try to cut before a DOI occurrence if present
        doi_m = _DOI_WORD_RE.search(rest) or _DOI_RE.search(rest)
        if doi_m:
            boundary = doi_m.start() - 1 if doi_m.start() > 0 else 0
        else:
            boundary = len(rest)
    title = re.sub(r"\s+", " ", rest[: boundary + 1]).strip()
    trailing = rest[boundary + 1 :]
    return (title if title else None), trailing


def extract_title(text: str) -> str | None:
    title, _ = _split_after_year(text)
    return title


def extract_journal(text: str) -> str | None:
    # Use the more robust split to find what's after the title
    _, trailing = _split_after_year(text)
    if trailing is None:
        return None
    rest = trailing.strip()
    # Cut off any DOI mention and what's after
    rest = _DOI_WORD_RE.split(rest)[0]
    rest = rest.strip()

    # Match a journal-like name at the start, stopping before volume/issue markers or numbers
    journ_re = re.compile(
        r"^([A-Za-z .&'\-/]+?)"  # journal name characters
        r"(?=(?:[,;]?\s*(?:\d|\(|:|no\.?|vol\.?|volume|issue|supp(?:lement)?|ser\.?|series))|\s*$)",
        re.IGNORECASE,
    )
    m = journ_re.search(rest)
    if m:
        j = m.group(1).strip(" .,/;-")
        # If there's a comma and the remainder after the comma contains digits (e.g., volume or DOI suffix),
        # drop the comma and what's after.
        if "," in rest:
            before, after = rest.split(",", 1)
            if any(ch.isdigit() for ch in after):
                j = before.strip(" .,/;-")
        if j:
            return j
    # Fallback: take text before first comma if it looks alphabetic enough
    head = rest.split(",", 1)[0].strip()
    if re.search(r"[A-Za-z]{3}", head):
        return head.strip(" .,/;-") or None
    return None


def extract_doi(text: str) -> str | None:
    m = _DOI_RE.search(text)
    if m:
        return normalize_doi(m.group(0))
    return None


def extract_doi_suffix_tokens(text: str) -> list[str]:
    """Extract possible DOI suffix-like tokens from a reference.

    Heuristic: alphanumeric tokens 6-16 chars containing both letters and digits.
    Intended to catch journal DOI suffixes like "zlad096" used by OUP.
    """
    tokens = re.findall(r"\b([A-Za-z0-9._-]{6,16})\b", text)
    out: list[str] = []
    for tok in tokens:
        t = tok.strip("._-")
        if len(t) < 6 or len(t) > 16:
            continue
        has_alpha = any(c.isalpha() for c in t)
        has_digit = any(c.isdigit() for c in t)
        if not (has_alpha and has_digit):
            continue
        out.append(t.lower())
    # de-duplicate preserve order
    seen: set[str] = set()
    uniq: list[str] = []
    for t in out:
        if t in seen:
            continue
        seen.add(t)
        uniq.append(t)
    return uniq


def read_refs(path: Path, *, limit: int | None = None) -> list[Ref]:
    lines = [ln.rstrip() for ln in path.read_text().splitlines()]
    # Heuristic: each non-empty line that contains a year marks a reference; ignore others
    refs: list[str] = []
    for ln in lines:
        if not ln.strip():
            continue
        if _YEAR_RE.search(ln):
            refs.append(ln.strip())
    if limit is not None:
        refs = refs[:limit]
    out: list[Ref] = []
    for raw in refs:
        year = extract_year(raw)
        title = extract_title(raw)
        journ = extract_journal(raw)
        doi = extract_doi(raw)
        out.append(Ref(raw, year, title, journ, doi))
    return out


@dataclass(frozen=True, kw_only=True)
class ArticleInfo:
    id: int
    title: str | None
    year: str | None
    doi: str | None
    norm_title: str | None
    alnum_norm_title: str | None
    years: tuple[str, ...]
    cg_name_norm: str | None


def _extract_years_from_article(a: Article) -> tuple[str, ...]:
    # Safer: re.findall on 4-digit pattern without capturing group again here
    yrs2 = re.findall(r"\b(?:19|20)\d{2}\b", a.year or "")
    return tuple(dict.fromkeys(yrs2))


def _year_within_one_ref(ref_year: str | None, ai: ArticleInfo) -> bool:
    if not ref_year:
        return True
    try:
        ry = int(ref_year)
    except ValueError:
        return True
    ai_years = []
    for y in ai.years:
        try:
            ai_years.append(int(y))
        except ValueError:
            continue
    if not ai_years:
        return True
    return any(abs(ay - ry) <= 1 for ay in ai_years)


def _normalize_title_for_index(s: str | None) -> str | None:
    if not s:
        return None
    # Remove underscores used as italic markers; strip HTML-like italics; collapse whitespace; lowercase
    s2 = s.replace("_", "")
    # Strip simple HTML italics tags if present
    s2 = re.sub(r"</?i>", "", s2, flags=re.IGNORECASE)
    # Remove periods
    s2 = s2.replace(".", "").replace(",", "").replace('"', "").replace("'", "").strip()
    # Replace en dashes etc.
    s2 = s2.replace("–", "-").replace("—", "-").replace("―", "-")
    # Replace curly quotes with straight quotes
    s2 = s2.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
    # Use unidecode to normalize accented characters to ASCII equivalents if possible
    s2 = unidecode(s2)
    # Collapse whitespace and lowercase
    s2 = re.sub(r"\s+", " ", s2).strip().lower()
    return s2


def _edit_distance(a: str, b: str) -> int:
    """Compute edit distance between two short strings.

    Uses rapidfuzz Levenshtein when available; otherwise a simple DP fallback.
    """
    try:
        return int(_Lev.distance(a, b))
    except Exception:
        pass
    # DP fallback
    n, m = len(a), len(b)
    if n == 0:
        return m
    if m == 0:
        return n
    # ensure n <= m to keep memory small
    if n > m:
        a, b = b, a
        n, m = m, n
    prev = list(range(n + 1))
    curr = [0] * (n + 1)
    for j in range(1, m + 1):
        curr[0] = j
        bj = b[j - 1]
        for i in range(1, n + 1):
            cost = 0 if a[i - 1] == bj else 1
            curr[i] = min(prev[i] + 1, curr[i - 1] + 1, prev[i - 1] + cost)
        prev, curr = curr, prev
    return prev[n]


_STOPWORDS = {
    "a",
    "an",
    "the",
    "of",
    "in",
    "on",
    "and",
    "for",
    "to",
    "with",
    "from",
    "by",
    "at",
    "as",
    "into",
    "about",
}


def _sig_words(norm_title: str | None) -> set[str]:
    if not norm_title:
        return set()
    words = re.findall(r"[a-z]+", norm_title)
    return {w for w in words if len(w) >= 4 and w not in _STOPWORDS}


def _normalize_journal_name(s: str | None) -> str | None:
    if not s:
        return None
    # Lowercase, remove punctuation-like separators, collapse whitespace
    s2 = s.lower()
    s2 = re.sub(r"[\-_.,:;()\[\]{}]", " ", s2)
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2 or None


def _norm_alnum(s: str | None) -> str:
    if not s:
        return ""
    s2 = s.replace("_", "")
    s2 = re.sub(r"</?i>", "", s2, flags=re.IGNORECASE)
    s2 = re.sub(r"[^A-Za-z0-9]+", " ", s2).strip().lower()
    return s2


type ArticleIndexes = tuple[
    dict[str, list[ArticleInfo]],
    dict[str, list[ArticleInfo]],
    dict[str, list[ArticleInfo]],
    list[ArticleInfo],
]


def _build_article_indexes() -> ArticleIndexes:
    all_infos: list[ArticleInfo] = []
    doi_index: dict[str, list[ArticleInfo]] = {}
    title_index: dict[str, list[ArticleInfo]] = {}
    year_index: dict[str, list[ArticleInfo]] = {}
    try:
        for a in Article.select_valid().filter(
            Article.kind != ArticleKind.alternative_version
        ):
            info = ArticleInfo(
                id=a.id,
                title=a.title,
                year=a.year,
                doi=a.doi,
                norm_title=_normalize_title_for_index(a.title),
                alnum_norm_title=_norm_alnum(a.title),
                years=_extract_years_from_article(a),
                cg_name_norm=_normalize_journal_name(
                    getattr(a.get_citation_group() or object(), "name", None)
                ),
            )
            all_infos.append(info)
            # Index DOI(s)
            if a.doi:
                for m in _DOI_RE.finditer(a.doi):
                    doi = normalize_doi(m.group(0))
                    if doi:
                        doi_index.setdefault(doi, []).append(info)
            # Index normalized title
            if info.norm_title:
                title_index.setdefault(info.norm_title, []).append(info)
            # Index by years present in the string
            for y in info.years:
                year_index.setdefault(y, []).append(info)
    except Exception:
        pass
    return doi_index, title_index, year_index, all_infos


# Public API for integration
def get_article_indexes() -> ArticleIndexes:
    """Build in-memory indexes over Articles for reuse by callers."""
    return _build_article_indexes()


def parse_ref(raw: str) -> Ref:
    """Parse a single reference string into a Ref structure."""
    return Ref(
        raw=raw,
        year=extract_year(raw),
        title=extract_title(raw),
        journal=extract_journal(raw),
        doi=extract_doi(raw),
    )


def resolve_reference(
    raw: str, indexes: ArticleIndexes | None = None
) -> tuple[str | None, int | None]:
    """Resolve a reference text to an Article id using this module's heuristics.

    Returns (reason, article_id) or (None, None) if not found.
    """
    if indexes is None:
        doi_index, title_index, year_index, all_infos = _build_article_indexes()
    else:
        doi_index, title_index, year_index, all_infos = indexes
    r = parse_ref(raw)
    reason, arts = match_article(
        r,
        doi_index=doi_index,
        title_index=title_index,
        year_index=year_index,
        all_infos=all_infos,
    )
    if reason and arts:
        return reason, arts[0].id
    return None, None


def match_article(
    r: Ref,
    *,
    doi_index: dict[str, list[ArticleInfo]],
    title_index: dict[str, list[ArticleInfo]],
    year_index: dict[str, list[ArticleInfo]],
    all_infos: list[ArticleInfo],
) -> tuple[str | None, list[ArticleInfo]]:
    def _best_by_journal_or_year(candidates: list[ArticleInfo]) -> ArticleInfo | None:
        if not candidates:
            return None
        # If ref has a journal, prefer the candidate whose citation group matches best.
        if r.journal:
            ref_j = _normalize_journal_name(r.journal)
            if ref_j:

                def jscore(ai: ArticleInfo) -> int:
                    if not ai.cg_name_norm:
                        return 0
                    if fuzz:
                        try:
                            return int(fuzz.ratio(ref_j, ai.cg_name_norm))
                        except Exception:
                            return 0
                    # crude non-fuzzy: exact gets 100, long containment 90, else 0
                    if ref_j == ai.cg_name_norm:
                        return 100
                    if len(ref_j) >= 12 and (
                        ref_j in ai.cg_name_norm or ai.cg_name_norm in ref_j
                    ):
                        return 90
                    return 0

                scored = sorted(candidates, key=lambda ai: jscore(ai), reverse=True)
                top = jscore(scored[0])
                second = jscore(scored[1]) if len(scored) > 1 else -1
                if top >= 90 and top - second >= 5:
                    return scored[0]
                # If several tie on journal, continue to year closeness among those with top score
                tied = [ai for ai in candidates if jscore(ai) == top]
                candidates = tied

        # Prefer closest year distance; tie-breaker: presence of DOI
        def year_dist(ai: ArticleInfo) -> int:
            if not r.year:
                return 0
            try:
                ry = int(r.year)
            except ValueError:
                return 0
            ds: list[int] = []
            for y in ai.years:
                try:
                    ds.append(abs(int(y) - ry))
                except ValueError:
                    continue
            return min(ds) if ds else 0

        candidates.sort(
            key=lambda ai: (year_dist(ai), 0 if (ai.doi or "").strip() else 1)
        )
        if len(candidates) >= 2:
            yd0, yd1 = year_dist(candidates[0]), year_dist(candidates[1])
            doi0, doi1 = bool((candidates[0].doi or "").strip()), bool(
                (candidates[1].doi or "").strip()
            )
            if yd0 == yd1 and doi0 == doi1:
                return None
        return candidates[0]

    # 1) DOI exact
    if r.doi:
        arts = doi_index.get(r.doi)
        if arts:
            arts_ok = [ai for ai in arts if _year_within_one_ref(r.year, ai)]
            if len(arts_ok) == 1:
                return "doi", [arts_ok[0]]

    # 2) Exact normalized title
    if r.title:
        norm_ref = _normalize_title_for_index(r.title)
        if norm_ref and norm_ref in title_index:
            arts = title_index[norm_ref]
            arts_ok = [ai for ai in arts if _year_within_one_ref(r.year, ai)]
            if len(arts_ok) == 1:
                return "title", [arts_ok[0]]
            elif len(arts_ok) > 1:
                best = _best_by_journal_or_year(arts_ok)
                if best is not None:
                    return "title_best", [best]
            else:
                # No year-consistent candidates; relax year constraint a bit for exact title equality
                best = _best_by_journal_or_year(arts)
                if best is not None:
                    # Accept if closest year within 2 years when titles are exactly equal
                    if not r.year:
                        return "title_relaxed", [best]
                    try:
                        ry = int(r.year)
                        best_years = [int(y) for y in best.years if y.isdigit()]
                    except Exception:
                        best_years = []
                    if best_years and min(abs(ry - y) for y in best_years) <= 2:
                        return "title_relaxed", [best]

        # 2b) Exact title match ignoring punctuation/dash variants (alnum-only)
        ref_alnum = _norm_alnum(r.title)
        if ref_alnum:
            alnum_matches = [ai for ai in all_infos if ai.alnum_norm_title == ref_alnum]
            alnum_ok = [ai for ai in alnum_matches if _year_within_one_ref(r.year, ai)]
            if len(alnum_ok) == 1:
                return "title_alnum", [alnum_ok[0]]
            elif len(alnum_ok) > 1:
                best = _best_by_journal_or_year(alnum_ok)
                if best is not None:
                    return "title_alnum_best", [best]
            else:
                # Relax for exact alnum equality as well
                best = _best_by_journal_or_year(alnum_matches)
                if best is not None:
                    if not r.year:
                        return "title_alnum_relaxed", [best]
                    try:
                        ry = int(r.year)
                        best_years = [int(y) for y in best.years if y.isdigit()]
                    except Exception:
                        best_years = []
                    if best_years and min(abs(ry - y) for y in best_years) <= 2:
                        return "title_alnum_relaxed", [best]

        # 3) Year + title probe (in-memory, conservative for short or generic titles)
        if r.year and norm_ref:
            word_count = len(norm_ref.split())
            char_len = len(norm_ref)
            # Only attempt substring matching for sufficiently descriptive titles
            if word_count >= 4 or char_len >= 30:
                probe = norm_ref[:40]
                pattern = re.compile(rf"\b{re.escape(probe)}\b")
                subset = year_index.get(r.year, [])
                ys_l = [
                    ai
                    for ai in subset
                    if ai.norm_title and pattern.search(ai.norm_title)
                ]
                sig_ref = _sig_words(norm_ref)
                ys_ok = []
                for ai in ys_l:
                    if not _year_within_one_ref(r.year, ai):
                        continue
                    # Require sufficient overlap on significant words to avoid generic matches
                    overlap = len(sig_ref & _sig_words(ai.norm_title))
                    if overlap >= 2:
                        ys_ok.append(ai)
                if len(ys_ok) == 1:
                    return "year+title", [ys_ok[0]]

        # 4) Optional fuzzy on all or year subset
        if fuzz:
            cand = year_index.get(r.year, all_infos) if r.year else all_infos
            scored = []
            for ai in cand:
                if not ai.title:
                    continue
                scored.append((fuzz.token_set_ratio(r.title, ai.title), ai))
            scored.sort(reverse=True, key=lambda t: t[0])
            if scored and scored[0][0] >= 95:
                # Ensure the top score is meaningfully better than the next
                if len(scored) == 1 or scored[1][0] <= scored[0][0] - 5:
                    top = scored[0][1]
                    if _year_within_one_ref(r.year, top):
                        # Also require at least 2 significant-word overlaps
                        if (
                            len(
                                _sig_words(norm_ref)
                                & _sig_words(_normalize_title_for_index(top.title))
                            )
                            < 2
                        ):
                            pass
                        else:
                            return "fuzzy_title", [top]

    # 4.5) DOI suffix tokens + journal match (requires uniqueness)
    if r.journal:
        ref_j = _normalize_journal_name(r.journal)
        if ref_j:
            suffixes = extract_doi_suffix_tokens(r.raw)
            if suffixes:
                cands: list[ArticleInfo] = []
                for sfx in suffixes:
                    for ai in all_infos:
                        if not ai.doi:
                            continue
                        if ai.doi.lower().endswith(sfx) and _year_within_one_ref(
                            r.year, ai
                        ):
                            if ai.cg_name_norm:
                                ok = False
                                if fuzz and fuzz.ratio(ref_j, ai.cg_name_norm) >= 90:
                                    ok = True
                                elif ref_j == ai.cg_name_norm or (
                                    len(ref_j) >= 12
                                    and (
                                        ref_j in ai.cg_name_norm
                                        or ai.cg_name_norm in ref_j
                                    )
                                ):
                                    ok = True
                                if ok:
                                    cands.append(ai)
                # unique
                if len(cands) == 1:
                    return "doi_suffix", [cands[0]]

    # 5) Journal + year via citation group is not considered sufficiently confident
    # to auto-match (kept only for suggestions below).

    # 6) Fallback: accept best suggestion if score is strong and titles are very similar
    suggestions = suggest_candidates(
        r,
        doi_index=doi_index,
        title_index=title_index,
        year_index=year_index,
        all_infos=all_infos,
    )
    if suggestions:
        best_reason, best_ai, best_score = suggestions[0]
        second_score = suggestions[1][2] if len(suggestions) > 1 else -1
        margin = best_score - second_score
        # Always allow DOI/DOI suffix if clearly best
        if best_reason in {"doi", "doi_suffix"} and margin >= 5:
            return best_reason, [best_ai]
        # Otherwise require high title similarity and overlap
        if r.title and best_ai.title:
            sim = _title_similarity(r.title, best_ai.title)
            overlap = len(
                _sig_words(_normalize_title_for_index(r.title))
                & _sig_words(_normalize_title_for_index(best_ai.title))
            )
            # Thresholds tuned to avoid generic matches (e.g., "Mammals")
            if sim >= 92 and overlap >= 2 and margin >= 8:
                return "best_title", [best_ai]

    return None, []


def suggest_candidates(
    r: Ref,
    *,
    doi_index: dict[str, list[ArticleInfo]],
    title_index: dict[str, list[ArticleInfo]],
    year_index: dict[str, list[ArticleInfo]],
    all_infos: list[ArticleInfo],
) -> list[tuple[str, ArticleInfo, int]]:
    """Return up to a handful of best candidates with simple confidence scores.

    Scores are heuristic: doi=100, title=95, year+title=80, fuzzy=ratio, journal=60.
    Year consistency (±1) is enforced for all suggestions when ref year is known.
    """
    suggestions: list[tuple[str, ArticleInfo, int]] = []
    # DOI suggestions
    if r.doi:
        for ai in doi_index.get(r.doi) or []:
            if _year_within_one_ref(r.year, ai):
                suggestions.append(("doi", ai, 100))
    # Exact title
    if r.title:
        norm_ref = _normalize_title_for_index(r.title)
        if norm_ref and norm_ref in title_index:
            for ai in title_index[norm_ref]:
                if _year_within_one_ref(r.year, ai):
                    suggestions.append(("title", ai, 95))
        # Year+title probe
        if r.year and norm_ref:
            word_count = len(norm_ref.split())
            char_len = len(norm_ref)
            if word_count >= 4 or char_len >= 30:
                probe = norm_ref[:40]
                pattern = re.compile(rf"\b{re.escape(probe)}\b")
                for ai in year_index.get(r.year) or []:
                    if ai.norm_title and pattern.search(ai.norm_title):
                        if _year_within_one_ref(r.year, ai):
                            suggestions.append(("year+title", ai, 80))
        # Fuzzy title over year subset or all
        if fuzz:
            cand = year_index.get(r.year, all_infos) if r.year else all_infos
            scored = []
            for ai in cand:
                if not ai.title:
                    continue
                scored.append((fuzz.token_set_ratio(r.title, ai.title), ai))
            scored.sort(reverse=True, key=lambda t: t[0])
            for score, ai in scored[:3]:
                if score >= 85 and _year_within_one_ref(r.year, ai):
                    suggestions.append(("fuzzy_title", ai, int(score)))
    # Journal+year subset (suggestions): compare to citation group name
    if r.journal and r.year:
        ref_j = _normalize_journal_name(r.journal)
        if ref_j:
            for ai in year_index.get(r.year) or []:
                if not ai.cg_name_norm:
                    continue
                good = False
                if fuzz:
                    if fuzz.ratio(ref_j, ai.cg_name_norm) >= 90:
                        good = True
                elif ref_j == ai.cg_name_norm or (
                    len(ref_j) >= 12
                    and (ref_j in ai.cg_name_norm or ai.cg_name_norm in ref_j)
                ):
                    good = True
                if good and _year_within_one_ref(r.year, ai):
                    suggestions.append(("year+journal_cg", ai, 60))

    # DOI suffix suggestions
    if r.journal:
        ref_j = _normalize_journal_name(r.journal)
        if ref_j:
            for sfx in extract_doi_suffix_tokens(r.raw):
                for ai in all_infos:
                    if not ai.doi or not ai.cg_name_norm:
                        continue
                    if not ai.doi.lower().endswith(sfx):
                        continue
                    good = False
                    if fuzz:
                        if fuzz.ratio(ref_j, ai.cg_name_norm) >= 90:
                            good = True
                    elif ref_j == ai.cg_name_norm or (
                        len(ref_j) >= 12
                        and (ref_j in ai.cg_name_norm or ai.cg_name_norm in ref_j)
                    ):
                        good = True
                    if good and _year_within_one_ref(r.year, ai):
                        suggestions.append(("doi_suffix", ai, 92))

    # Deduplicate by article id keeping highest score per reason ordering
    seen: set[int] = set()
    ordered: list[tuple[str, ArticleInfo, int]] = []
    # Order by score desc, then by reason priority
    reason_priority = {
        "doi": 0,
        "title": 1,
        "year+title": 2,
        "fuzzy_title": 3,
        "year+journal_substr_in_title": 4,
    }
    for reason, ai, score in sorted(
        suggestions, key=lambda t: (-t[2], reason_priority.get(t[0], 99))
    ):
        if ai.id in seen:
            continue
        seen.add(ai.id)
        ordered.append((reason, ai, score))
        if len(ordered) >= 3:
            break
    return ordered


def _title_similarity(a: str, b: str) -> int:
    an = _normalize_title_for_index(a) or ""
    bn = _normalize_title_for_index(b) or ""
    if not an or not bn:
        return 0
    if fuzz:
        try:
            return int(fuzz.token_set_ratio(an, bn))
        except Exception:
            pass
    # approximate similarity from edit distance
    d = _edit_distance(an, bn)
    denom = max(len(an), len(bn)) or 1
    sim = round(100 * (1 - d / denom))
    return max(0, min(100, sim))


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Match MDD references to Article records")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--show-matches", action="store_true")
    ap.add_argument("--show-unmatched", action="store_true")
    ap.add_argument(
        "--open-scholar",
        action="store_true",
        help="Open Google Scholar searches for up to N unmatched refs (title only)",
    )
    ap.add_argument(
        "--open-scholar-limit",
        type=int,
        default=20,
        help="Maximum number of Scholar searches to open when --open-scholar is set (default: 20)",
    )
    ap.add_argument(
        "--open-scholar-sleep",
        type=float,
        default=2.0,
        help="Seconds to sleep between opening Scholar searches (default: 2.0)",
    )
    ap.add_argument(
        "--unmatched-out",
        type=str,
        default="notes/mdd/mdd_refs_unmatched.csv",
        help="Path to write unmatched references CSV",
    )
    ap.add_argument(
        "--matches-csv-out",
        type=str,
        default="notes/mdd/mdd_refs_matches.csv",
        help="Path to write matched references CSV",
    )
    args = ap.parse_args(argv)

    refs = read_refs(MDD_FILE, limit=args.limit)
    doi_index, title_index, year_index, all_infos = _build_article_indexes()
    total = len(refs)
    matched = 0
    unmatched: list[Ref] = []
    by_reason: dict[str, int] = {}
    matches: list[tuple[Ref, str, list[ArticleInfo]]] = []

    for r in refs:
        reason, arts = match_article(
            r,
            doi_index=doi_index,
            title_index=title_index,
            year_index=year_index,
            all_infos=all_infos,
        )
        if reason is None:
            unmatched.append(r)
        else:
            matched += 1
            by_reason[reason] = by_reason.get(reason, 0) + 1
            matches.append((r, reason, arts))
            if args.show_matches:
                print(f"MATCH [{reason}] {r.raw}")
                for ai in arts[:3]:
                    print(
                        f"  -> Article[{ai.id}]: {ai.title} | year={ai.year} | doi={ai.doi}"
                    )

    print(f"Processed references: {total}")
    print(f"Matched: {matched}")
    print(f"Unmatched: {len(unmatched)}")
    if by_reason:
        print("Breakdown:")
        for k in ("doi", "title", "year+title", "fuzzy_title"):
            if k in by_reason:
                print(f"- {k}: {by_reason[k]}")

    if args.show_unmatched and unmatched:
        print("\nUnmatched examples (up to 20):")
        for r in unmatched[:20]:
            print(f"- {r.raw}")
            print(f"  year={r.year} title={r.title} journal={r.journal} doi={r.doi}")

    # Optionally open Google Scholar for unmatched refs with valid journals
    if args.open_scholar and unmatched:
        # Build normalized set of known citation group names
        valid_journals: set[str] = set()
        try:
            for cg in CitationGroup.select_valid():
                if cg.name:
                    v = _normalize_journal_name(cg.name)
                    if v:
                        valid_journals.add(v)
                try:
                    cn = cg.get_citable_name()
                except Exception:
                    cn = None
                if cn:
                    v2 = _normalize_journal_name(cn)
                    if v2:
                        valid_journals.add(v2)
        except Exception:
            pass
        # Exclude journals we generally can't access
        excluded = {
            _normalize_journal_name("Bionomina"),
            _normalize_journal_name("Zootaxa"),
        }
        opener = shutil.which("open")
        # Build and sort candidate openings by normalized journal, then title
        candidates: list[tuple[str, Ref]] = []
        for r in unmatched:
            if not r.title or not r.journal:
                continue
            ref_j = _normalize_journal_name(r.journal)
            if not ref_j or ref_j not in valid_journals:
                continue
            if ref_j in excluded:
                continue
            candidates.append((ref_j, r))
        candidates.sort(key=lambda x: (x[0], (x[1].title or "")))

        opened = 0
        for _, r in candidates:
            if opened >= args.open_scholar_limit:
                break
            q = quote_plus(f'"{r.title}"')
            url = f"https://scholar.google.com/scholar?q={q}"
            if opener:
                subprocess.run([opener, url], check=False)
            else:
                webbrowser.open(url)
            opened += 1
            # Sleep between openings to avoid rate limits / captchas
            if args.open_scholar_sleep > 0:
                time.sleep(args.open_scholar_sleep)
        print(f"Opened Scholar searches: {opened} (limit: {args.open_scholar_limit})")

    # Write unmatched file (CSV with parsed fields and up to 3 suggested candidates)
    try:
        unmatched_path = Path(args.unmatched_out)
        unmatched_path.parent.mkdir(parents=True, exist_ok=True)
        with unmatched_path.open("w", newline="", encoding="utf-8") as f:
            cw = csv.writer(f)
            cw.writerow(
                [
                    "ref_raw",
                    "ref_year",
                    "ref_title",
                    "ref_journal",
                    "ref_doi",
                    "best1_reason",
                    "best1_article_id",
                    "best1_title",
                    "best1_year",
                    "best1_doi",
                    "best1_score",
                    "best1_citation",
                    "best2_reason",
                    "best2_article_id",
                    "best2_title",
                    "best2_year",
                    "best2_doi",
                    "best2_score",
                    "best2_citation",
                    "best3_reason",
                    "best3_article_id",
                    "best3_title",
                    "best3_year",
                    "best3_doi",
                    "best3_score",
                    "best3_citation",
                ]
            )
            for r in unmatched:
                sugg = suggest_candidates(
                    r,
                    doi_index=doi_index,
                    title_index=title_index,
                    year_index=year_index,
                    all_infos=all_infos,
                )
                row = [r.raw, r.year or "", r.title or "", r.journal or "", r.doi or ""]
                # Append up to 3 suggestions
                for i in range(3):
                    if i < len(sugg):
                        reason, ai, score = sugg[i]
                        citation = ""
                        try:
                            art = Article.get(id=ai.id)
                            citation = art.cite()
                        except Exception:
                            citation = ""
                        row += [
                            reason,
                            str(ai.id),
                            ai.title or "",
                            ai.year or "",
                            ai.doi or "",
                            str(score),
                            citation,
                        ]
                    else:
                        row += ["", "", "", "", "", "", ""]
                cw.writerow(row)
        print(f"Wrote unmatched CSV: {unmatched_path} ({len(unmatched)} refs)")
    except Exception as e:
        print(f"Warning: failed to write unmatched list: {e}")

    # Write matches CSV
    try:
        csv_path = Path(args.matches_csv_out)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "ref_raw",
                    "ref_year",
                    "ref_title",
                    "ref_journal",
                    "ref_doi",
                    "reason",
                    "candidate_rank",
                    "article_id",
                    "article_title",
                    "article_year",
                    "article_doi",
                    "match_year",
                    "title_distance",
                    "match_journal",
                    "match_doi",
                    "article_citation",
                ]
            )
            for r, reason, arts in matches:
                for idx, ai in enumerate(arts, start=1):
                    citation = ""
                    # Compute match signals and title distance
                    year_match = "0"
                    title_distance = ""
                    journal_match = "0"
                    doi_match = "0"
                    try:
                        art = Article.get(id=ai.id)
                        citation = art.cite()
                        # Year match: ref year contained in article.year or equals valid_numeric_year
                        if r.year:
                            try:
                                vy = art.valid_numeric_year()
                            except Exception:
                                vy = None
                            art_year_str = art.year or ""
                            if (vy is not None and str(vy) == r.year) or (
                                r.year in art_year_str
                            ):
                                year_match = "1"
                        # Title distance: normalized titles, integer edit distance
                        if r.title:
                            ref_norm = _normalize_title_for_index(r.title)
                            art_norm = _normalize_title_for_index(art.title)
                            if ref_norm and art_norm:
                                title_distance = str(_edit_distance(ref_norm, art_norm))
                        # Journal match: substring of citation group name
                        if r.journal:
                            try:
                                cg = art.get_citation_group()
                                cg_name = cg.name if cg is not None else ""
                            except Exception:
                                cg_name = ""
                            if cg_name and r.journal.lower() in cg_name.lower():
                                journal_match = "1"
                        # DOI match: exact DOI found in article.doi text
                        if r.doi and art.doi:
                            dois = [
                                normalize_doi(m.group(0))
                                for m in _DOI_RE.finditer(art.doi)
                            ]
                            if r.doi in dois:
                                doi_match = "1"
                    except Exception:
                        citation = ""
                    writer.writerow(
                        [
                            r.raw,
                            r.year or "",
                            r.title or "",
                            r.journal or "",
                            r.doi or "",
                            reason,
                            idx,
                            ai.id,
                            ai.title or "",
                            ai.year or "",
                            ai.doi or "",
                            year_match,
                            title_distance,
                            journal_match,
                            doi_match,
                            citation,
                        ]
                    )
        print(
            f"Wrote matches CSV: {csv_path} ({sum(len(a) for _,_,a in matches)} rows)"
        )
    except Exception as e:
        print(f"Warning: failed to write matches CSV: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
