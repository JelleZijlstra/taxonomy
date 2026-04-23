# ruff: noqa: E402

import argparse
import contextlib
import csv
import importlib
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from functools import cache
from pathlib import Path
from typing import Any, cast

import httpx
import requests

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

lib = importlib.import_module("data_import.lib")

from taxonomy import urlparse
from taxonomy.db import helpers
from taxonomy.db.constants import ArticleIdentifier, ArticleType, NamingConvention
from taxonomy.db.models import Article, CitationGroup, CitationGroupTag, Name, Person
from taxonomy.db.models.article import ArticleTag, api_data, batlit
from taxonomy.db.models.article import lint as article_lint
from taxonomy.db.models.base import LintConfig

DEFAULT_INPUT = lib.DATA_DIR / "chiroptera-hmw-refs-parsed.csv"
DEFAULT_OUTPUT = lib.DATA_DIR / "chiroptera-hmw-refs-taxonomy-matches.csv"
LOOKUP_MODES = ("off", "cached", "network")
INDEX_PROGRESS_EVERY = 5000
ROW_PROGRESS_EVERY = 500

MATCH_FIELDS = [
    "source_row",
    "taxonomy_match_status",
    "taxonomy_match_score",
    "taxonomy_match_method",
    "taxonomy_match_reasons",
    "taxonomy_candidate_count",
    "taxonomy_top_candidates",
    "taxonomy_article_id",
    "taxonomy_article_name",
    "taxonomy_citation",
    "doi",
    "doi_source",
    "batlit_id",
    "batlit_url",
    "batlit_zenodo_doi",
    "batlit_citation",
    "batlit_source",
    "bhl_url",
    "bhl_source",
]

EXPECTED_TYPES = {
    "journal_article": {ArticleType.JOURNAL},
    "scientific_description": {
        ArticleType.BOOK,
        ArticleType.CHAPTER,
        ArticleType.JOURNAL,
        ArticleType.PART,
    },
    "book_chapter": {ArticleType.CHAPTER, ArticleType.PART},
    "book": {ArticleType.BOOK},
    "report": {ArticleType.BOOK, ArticleType.MISCELLANEOUS},
    "thesis": {ArticleType.THESIS},
    "web": {ArticleType.WEB, ArticleType.MISCELLANEOUS},
}
STOP_TITLE_WORDS = {
    "a",
    "an",
    "and",
    "de",
    "des",
    "du",
    "for",
    "in",
    "la",
    "le",
    "les",
    "of",
    "on",
    "the",
    "to",
    "und",
    "with",
}


@dataclass(frozen=True)
class ArticleRecord:
    id: int
    name: str
    type: ArticleType | None
    type_name: str
    kind_name: str
    year: str
    year_num: int | None
    title: str
    title_key: str
    title_tokens: frozenset[str]
    authors: tuple[str, ...]
    author_aliases: tuple[frozenset[str], ...]
    author_key: str
    citation_group_id: int | None
    citation_group: str
    citation_group_aliases: tuple[str, ...]
    citation_group_keys: frozenset[str]
    volume: str
    issue: str
    start_page: str
    end_page: str
    pages: str
    doi: str
    url: str


@dataclass
class Candidate:
    article: ArticleRecord
    methods: set[str] = field(default_factory=set)
    taxon_supports: list[tuple[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class ScoredCandidate:
    article: ArticleRecord
    score: int
    methods: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass
class ArticleIndex:
    records: list[ArticleRecord]
    by_id: dict[int, ArticleRecord]
    by_year_author: dict[tuple[int, str], list[ArticleRecord]]
    by_year_title_token: dict[tuple[int, str], list[ArticleRecord]]
    by_url: dict[str, list[ArticleRecord]]
    by_cg_year: dict[tuple[int, int], list[ArticleRecord]]
    by_cg_year_volume_page: dict[tuple[int, int, str, str], list[ArticleRecord]]
    citation_group_by_alias: dict[str, set[int]]
    citation_group_aliases_by_id: dict[int, tuple[str, ...]]
    citation_group_sample_article_id: dict[int, int]
    citation_groups_with_doi: frozenset[int]


@dataclass
class MatchSummary:
    status_counts: Counter[str] = field(default_factory=Counter)
    link_counts: Counter[str] = field(default_factory=Counter)
    doi_sources: Counter[str] = field(default_factory=Counter)
    batlit_sources: Counter[str] = field(default_factory=Counter)
    bhl_sources: Counter[str] = field(default_factory=Counter)


@dataclass(frozen=True)
class LearnedMappings:
    citation_group_by_container_key: dict[str, int]


@dataclass(frozen=True)
class RowEvaluation:
    row_number: int
    input_row: dict[str, str]
    status: str
    match: ScoredCandidate | None
    scored: tuple[ScoredCandidate, ...]
    output_row: dict[str, str]


class FreshNetworkCall(RuntimeError):
    pass


_FUZZY_CITATION_GROUP_CACHE: dict[str, tuple[int, ...]] = {}


@contextlib.contextmanager
def lookup_mode(mode: str) -> Iterator[None]:
    if mode != "cached":
        yield
        return

    def raise_for_network(*_args: object, **_kwargs: object) -> None:
        raise FreshNetworkCall("fresh network call blocked in cached lookup mode")

    original_httpx_get = httpx.get
    original_requests_get = requests.get
    httpx.get = cast(Any, raise_for_network)
    requests.get = cast(Any, raise_for_network)
    try:
        yield
    finally:
        httpx.get = original_httpx_get
        requests.get = original_requests_get


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def compact_key(text: str) -> str:
    return normalize_text(text).replace(" ", "")


def normalize_page(page: str) -> str:
    page = page.strip().lower()
    page = page.replace("–", "-")
    page = re.sub(r"\s+", "", page)
    return page.rstrip(".,;:")


def first_page(pages: str) -> str:
    pages = pages.strip()
    if not pages:
        return ""
    pages = re.split(r"[,;]", pages, maxsplit=1)[0]
    return normalize_page(re.split(r"\s*[-–]\s*", pages, maxsplit=1)[0])


def last_page(pages: str) -> str:
    pages = pages.strip()
    if not pages:
        return ""
    pages = re.split(r"[,;]", pages, maxsplit=1)[0]
    parts = re.split(r"\s*[-–]\s*", pages, maxsplit=1)
    if len(parts) == 1:
        return ""
    return normalize_page(parts[1])


def title_tokens(text: str) -> frozenset[str]:
    return frozenset(
        token
        for token in normalize_text(text).split()
        if len(token) >= 3 and token not in STOP_TITLE_WORDS
    )


def best_title_token(tokens: Iterable[str]) -> str | None:
    sorted_tokens = sorted(tokens, key=lambda token: (-len(token), token))
    return sorted_tokens[0] if sorted_tokens else None


def ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def token_ratio(left: frozenset[str], right: frozenset[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def abbreviation_token_ratio(short: str, long: str) -> float:
    short_tokens = [token for token in normalize_text(short).split() if len(token) > 1]
    long_tokens = normalize_text(long).split()
    if not short_tokens or not long_tokens:
        return 0.0
    matched = 0
    start_at = 0
    for short_token in short_tokens:
        for index in range(start_at, len(long_tokens)):
            long_token = long_tokens[index]
            if long_token.startswith(short_token) or short_token.startswith(long_token):
                matched += 1
                start_at = index + 1
                break
    return matched / len(short_tokens)


def normalize_url(url: str) -> str:
    url = url.strip().rstrip(".")
    url = re.sub(r"^https?://", "", url, flags=re.IGNORECASE)
    url = url.removeprefix("www.")
    return url.rstrip("/").lower()


def numeric_year(year: str) -> int | None:
    if match := re.search(r"\d{4}", year):
        return int(match.group(0))
    return None


def roman_to_int(text: str) -> int | None:
    values = {"i": 1, "v": 5, "x": 10, "l": 50, "c": 100, "d": 500, "m": 1000}
    text = text.lower()
    if not text or any(char not in values for char in text):
        return None
    total = 0
    previous = 0
    for char in reversed(text):
        value = values[char]
        if value < previous:
            total -= value
        else:
            total += value
            previous = value
    return total


def page_number(page: str) -> int | None:
    page = normalize_page(page)
    if page.isdigit():
        return int(page)
    return roman_to_int(page)


def split_author_families(authors: str) -> tuple[str, ...]:
    authors = authors.replace(" et al.", "")
    pieces = re.split(r"\s*(?:&|,)\s*", authors)
    family_names = []
    for piece in pieces:
        piece = piece.strip()
        if not piece:
            continue
        if re.fullmatch(r"(?:[A-ZÁÉÍÓÚÄÖÜØ]\.)+", piece):
            continue
        if re.fullmatch(r"[A-ZÁÉÍÓÚÄÖÜØ](?:\.[A-ZÁÉÍÓÚÄÖÜØ]\.)?", piece):
            continue
        family_names.append(primary_author_key(piece))
    return tuple(family_names)


def row_author_aliases(authors: str) -> tuple[set[str], ...]:
    authors = authors.replace(" et al.", "")
    pieces = re.split(r"\s*(?:&|,)\s*", authors)
    aliases = []
    for piece in pieces:
        piece = piece.strip()
        if not piece:
            continue
        if re.fullmatch(r"(?:[A-ZÁÉÍÓÚÄÖÜØ]\.)+", piece):
            continue
        if re.fullmatch(r"[A-ZÁÉÍÓÚÄÖÜØ](?:\.[A-ZÁÉÍÓÚÄÖÜØ]\.)?", piece):
            continue
        aliases.append(author_piece_aliases(piece))
    return tuple(aliases)


def first_row_author_for_crossref(authors: str) -> str:
    authors = authors.replace(" et al.", "")
    pieces = re.split(r"\s*(?:&|,)\s*", authors)
    for piece in pieces:
        piece = piece.strip()
        if not piece:
            continue
        if re.fullmatch(r"(?:[A-ZÁÉÍÓÚÄÖÜØ]\.)+", piece):
            continue
        if re.fullmatch(r"[A-ZÁÉÍÓÚÄÖÜØ](?:\.[A-ZÁÉÍÓÚÄÖÜØ]\.)?", piece):
            continue
        if "," in piece:
            return piece.split(",", maxsplit=1)[0].strip()
        tokens = [token for token in re.split(r"\s+", piece) if token]
        if len(tokens) >= 2:
            return tokens[0]
        return piece
    return ""


def author_piece_aliases(piece: str) -> set[str]:
    piece = piece.strip()
    aliases = {compact_key(piece)}
    tokens = [token for token in re.split(r"\s+", piece) if token]
    if "," in piece:
        aliases.add(compact_key(piece.split(",", maxsplit=1)[0]))
    elif len(tokens) >= 2:
        aliases.add(compact_key(tokens[0]))
        aliases.add(compact_key(tokens[-1]))
    return {alias for alias in aliases if alias}


def primary_author_key(piece: str) -> str:
    piece = piece.strip()
    if "," in piece:
        return compact_key(piece.split(",", maxsplit=1)[0])
    tokens = [token for token in re.split(r"\s+", piece) if token]
    if len(tokens) >= 2:
        return compact_key(tokens[0])
    return compact_key(piece)


def person_author_aliases(person: Person) -> set[str]:
    aliases: set[str] = set()
    family_name = person.family_name
    aliases.add(compact_key(family_name))
    if person.naming_convention in (
        NamingConvention.russian,
        NamingConvention.ukrainian,
    ):
        aliases.add(compact_key(helpers.romanize_russian(family_name)))
    if person.naming_convention in (
        NamingConvention.pinyin,
        NamingConvention.chinese,
        NamingConvention.vietnamese,
    ):
        if person.given_names:
            aliases.add(compact_key(f"{family_name} {person.given_names}"))
    return {alias for alias in aliases if alias}


def article_author_keys(article: Article) -> tuple[str, ...]:
    return tuple(
        compact_key(author.taxonomic_authority()) for author in article.get_authors()
    )


def article_author_text(author_keys: Sequence[str]) -> str:
    return " & ".join(author_keys)


@cache
def get_article(article_id: int) -> Article:
    return Article(article_id)


def get_citation_group_aliases(article: Article) -> tuple[str, ...]:
    citation_group = article.citation_group
    if citation_group is None:
        return ()
    aliases = {citation_group.name}
    abbreviated = citation_group.get_abbreviated_title()
    if abbreviated:
        aliases.add(abbreviated)
    for tag in citation_group.tags or ():
        if isinstance(tag, CitationGroupTag.AlternativeName):
            aliases.add(tag.text)
    return tuple(sorted(aliases))


def make_article_record(article: Article) -> ArticleRecord:
    aliases = get_citation_group_aliases(article)
    authors = article_author_keys(article)
    author_aliases = tuple(
        frozenset(person_author_aliases(author)) for author in article.get_authors()
    )
    title = article.title or ""
    citation_group = article.citation_group.name if article.citation_group else ""
    return ArticleRecord(
        id=article.id,
        name=article.name,
        type=article.type,
        type_name=article.type.name if article.type else "",
        kind_name=article.kind.name,
        year=article.year or "",
        year_num=article.valid_numeric_year(),
        title=title,
        title_key=normalize_text(title),
        title_tokens=title_tokens(title),
        authors=authors,
        author_aliases=author_aliases,
        author_key=(
            sorted(author_aliases[0])[0]
            if author_aliases
            else (authors[0] if authors else "")
        ),
        citation_group_id=article.citation_group.id if article.citation_group else None,
        citation_group=citation_group,
        citation_group_aliases=aliases,
        citation_group_keys=frozenset(normalize_text(alias) for alias in aliases),
        volume=article.volume or "",
        issue=article.issue or "",
        start_page=normalize_page(article.start_page or ""),
        end_page=normalize_page(article.end_page or ""),
        pages=article.pages or "",
        doi=article.doi or "",
        url=article.url or "",
    )


def build_article_index() -> ArticleIndex:
    print("Building taxonomy Article index...", flush=True)
    records = []
    for count, article in enumerate(Article.select_valid(), start=1):
        records.append(make_article_record(article))
        if count % INDEX_PROGRESS_EVERY == 0:
            print(f"  Indexed {count} Articles...", flush=True)
    print(f"Indexed {len(records)} Articles.", flush=True)
    by_id = {record.id: record for record in records}
    by_year_author: dict[tuple[int, str], list[ArticleRecord]] = defaultdict(list)
    by_year_title_token: dict[tuple[int, str], list[ArticleRecord]] = defaultdict(list)
    by_url: dict[str, list[ArticleRecord]] = defaultdict(list)
    by_cg_year: dict[tuple[int, int], list[ArticleRecord]] = defaultdict(list)
    by_cg_year_volume_page: dict[tuple[int, int, str, str], list[ArticleRecord]] = (
        defaultdict(list)
    )
    citation_group_by_alias: dict[str, set[int]] = defaultdict(set)
    citation_group_aliases_by_id: dict[int, set[str]] = defaultdict(set)
    citation_group_sample_article_id: dict[int, int] = {}
    citation_groups_with_doi: set[int] = set()

    for record in records:
        if record.year_num is not None:
            for author_alias in (
                record.author_aliases[0] if record.author_aliases else ()
            ):
                by_year_author[(record.year_num, author_alias)].append(record)
            if token := best_title_token(record.title_tokens):
                by_year_title_token[(record.year_num, token)].append(record)
            if record.citation_group_id is not None:
                by_cg_year[(record.citation_group_id, record.year_num)].append(record)
                if record.volume and record.start_page:
                    by_cg_year_volume_page[
                        (
                            record.citation_group_id,
                            record.year_num,
                            compact_key(record.volume),
                            record.start_page,
                        )
                    ].append(record)
        for url in {record.url, f"https://doi.org/{record.doi}" if record.doi else ""}:
            key = normalize_url(url)
            if key:
                by_url[key].append(record)
        for alias_key in record.citation_group_keys:
            if alias_key and record.citation_group_id is not None:
                citation_group_by_alias[alias_key].add(record.citation_group_id)
        if record.citation_group_id is not None:
            citation_group_aliases_by_id[record.citation_group_id].update(
                record.citation_group_aliases
            )
            citation_group_sample_article_id.setdefault(
                record.citation_group_id, record.id
            )
            if record.doi:
                citation_groups_with_doi.add(record.citation_group_id)

    return ArticleIndex(
        records=records,
        by_id=by_id,
        by_year_author=by_year_author,
        by_year_title_token=by_year_title_token,
        by_url=by_url,
        by_cg_year=by_cg_year,
        by_cg_year_volume_page=by_cg_year_volume_page,
        citation_group_by_alias=citation_group_by_alias,
        citation_group_aliases_by_id={
            citation_group_id: tuple(sorted(aliases))
            for citation_group_id, aliases in citation_group_aliases_by_id.items()
        },
        citation_group_sample_article_id=citation_group_sample_article_id,
        citation_groups_with_doi=frozenset(citation_groups_with_doi),
    )


def add_candidate(
    candidates: dict[int, Candidate],
    record: ArticleRecord,
    method: str,
    *,
    taxon_label: str = "",
    page_text: str = "",
) -> None:
    candidate = candidates.setdefault(record.id, Candidate(record))
    candidate.methods.add(method)
    if taxon_label:
        candidate.taxon_supports.append((taxon_label, page_text))


def row_years(row: dict[str, str]) -> list[int]:
    year = numeric_year(row["year"])
    if year is None:
        return []
    return [year - 1, year, year + 1]


def row_title(row: dict[str, str]) -> str:
    return row["title"] or row["book_title"]


def row_container(row: dict[str, str]) -> str:
    return row["container_title"] or row["book_title"] or row["publisher"]


def row_container_key(row: dict[str, str]) -> str:
    return normalize_text(row_container(row))


def learned_citation_group_ids(
    row: dict[str, str], learned_mappings: LearnedMappings | None
) -> set[int]:
    if learned_mappings is None:
        return set()
    container_key = row_container_key(row)
    if not container_key:
        return set()
    citation_group_id = learned_mappings.citation_group_by_container_key.get(
        container_key
    )
    if citation_group_id is None:
        return set()
    return {citation_group_id}


def row_citation_group_ids(
    row: dict[str, str],
    index: ArticleIndex,
    learned_mappings: LearnedMappings | None = None,
) -> set[int]:
    exact = index.citation_group_by_alias.get(row_container_key(row), set())
    if exact:
        return exact
    return learned_citation_group_ids(row, learned_mappings)


def citation_group_match_score(container: str, alias: str) -> float:
    return max(
        ratio(normalize_text(container), normalize_text(alias)),
        abbreviation_token_ratio(container, alias),
    )


def row_doi_citation_group_ids(
    row: dict[str, str],
    index: ArticleIndex,
    learned_mappings: LearnedMappings | None = None,
) -> tuple[int, ...]:
    exact = row_citation_group_ids(row, index, learned_mappings)
    if exact:
        return tuple(sorted(exact))
    container = row["container_title"]
    if not container:
        return ()
    cached = _FUZZY_CITATION_GROUP_CACHE.get(container)
    if cached is not None:
        return cached
    by_id: dict[int, float] = {}
    best_score = 0.0
    for citation_group_id, aliases in index.citation_group_aliases_by_id.items():
        score = max(citation_group_match_score(container, alias) for alias in aliases)
        by_id[citation_group_id] = score
        best_score = max(best_score, score)
    if best_score < 0.8:
        _FUZZY_CITATION_GROUP_CACHE[container] = ()
        return ()
    matched = sorted(
        (
            citation_group_id
            for citation_group_id, score in by_id.items()
            if score >= max(0.8, best_score - 0.02)
        ),
        key=lambda citation_group_id: by_id[citation_group_id],
        reverse=True,
    )[:3]
    result = tuple(matched)
    _FUZZY_CITATION_GROUP_CACHE[container] = result
    return result


def row_citation_group_aliases(
    row: dict[str, str],
    index: ArticleIndex,
    learned_mappings: LearnedMappings | None = None,
) -> set[str]:
    aliases = set()
    container = row_container(row)
    if container:
        aliases.add(container)
    for citation_group_id in row_citation_group_ids(row, index, learned_mappings):
        aliases.update(index.citation_group_aliases_by_id.get(citation_group_id, ()))
    return aliases


def iter_taxon_labels(described_taxa: str) -> Iterable[str]:
    for group in described_taxa.split(" | "):
        for label in group.split(","):
            label = label.strip()
            if label:
                yield label


def scientific_taxon_page_groups(
    row: dict[str, str],
) -> list[tuple[str, tuple[str, ...]]]:
    if row["reference_type"] != "scientific_description":
        return []
    match = re.match(
        r"^.+? \(\d{4}(?:–\d{4})?[a-z]*\)\. (?P<body>.*)$", row["raw_reference"]
    )
    if match is None:
        return []
    body = match.group("body")
    groups = []
    previous_end = 0
    for bracket_match in re.finditer(r"\[([^]]+)\]", body):
        preceding = body[previous_end : bracket_match.start()].strip().rstrip(",")
        if ":" in preceding:
            page_text = preceding.rsplit(":", maxsplit=1)[-1].strip().rstrip(",")
        else:
            page_text = preceding.strip().rstrip(",")
        taxa = tuple(
            label.strip()
            for label in bracket_match.group(1).split(",")
            if label.strip()
        )
        if taxa:
            groups.append((page_text, taxa))
        previous_end = bracket_match.end()
    return groups


@cache
def original_citation_ids_for_taxon(label: str) -> tuple[int, ...]:
    article_ids = set()
    try:
        names = list(Name.select_valid().filter(Name.corrected_original_name == label))
    except Exception:
        return ()
    for name in names:
        for candidate_name in {name, name.resolve_variant()}:
            if candidate_name.original_citation is not None:
                article_ids.add(candidate_name.original_citation.id)
    return tuple(sorted(article_ids))


def find_candidates(
    row: dict[str, str],
    index: ArticleIndex,
    learned_mappings: LearnedMappings | None = None,
) -> dict[int, Candidate]:
    candidates: dict[int, Candidate] = {}
    year = numeric_year(row["year"])
    years = row_years(row)

    taxon_groups = scientific_taxon_page_groups(row)
    if taxon_groups:
        for page_text, labels in taxon_groups:
            for label in labels:
                for article_id in original_citation_ids_for_taxon(label):
                    record = index.by_id.get(article_id)
                    if record is not None:
                        add_candidate(
                            candidates,
                            record,
                            f"described_taxon:{label}",
                            taxon_label=label,
                            page_text=page_text,
                        )
    else:
        for label in iter_taxon_labels(row["described_taxa"]):
            for article_id in original_citation_ids_for_taxon(label):
                record = index.by_id.get(article_id)
                if record is not None:
                    add_candidate(
                        candidates,
                        record,
                        f"described_taxon:{label}",
                        taxon_label=label,
                    )

    if row["url"]:
        for record in index.by_url.get(normalize_url(row["url"]), ()):
            add_candidate(candidates, record, "url")

    row_authors = split_author_families(row["authors"])
    if row_authors:
        for candidate_year in years:
            for record in index.by_year_author.get(
                (candidate_year, row_authors[0]), ()
            ):
                add_candidate(candidates, record, "year_author")

    title = row_title(row)
    if title and year is not None:
        token = best_title_token(title_tokens(title))
        if token is not None:
            for candidate_year in years:
                for record in index.by_year_title_token.get(
                    (candidate_year, token), ()
                ):
                    add_candidate(candidates, record, "year_title")

    cg_ids = row_citation_group_ids(row, index, learned_mappings)
    row_volume = compact_key(row["volume"])
    row_start_page = first_page(row["pages"])
    for cg_id in cg_ids:
        for candidate_year in years:
            for record in index.by_cg_year.get((cg_id, candidate_year), ()):
                add_candidate(candidates, record, "citation_group_year")
            if row_volume and row_start_page:
                for record in index.by_cg_year_volume_page.get(
                    (cg_id, candidate_year, row_volume, row_start_page), ()
                ):
                    add_candidate(candidates, record, "citation_group_volume_page")

    return candidates


def score_authors(
    row_author_alias_sets: tuple[set[str], ...], article: ArticleRecord
) -> tuple[int, list[str]]:
    if not row_author_alias_sets or not article.author_aliases:
        return 0, []
    score = 0
    reasons = []
    first_row_aliases = row_author_alias_sets[0]
    first_article_aliases = article.author_aliases[0]
    if first_row_aliases & first_article_aliases:
        score += 12
        reasons.append("first author exact")
    else:
        author_ratio = max(
            ratio(row_alias, article_alias)
            for row_alias in first_row_aliases
            for article_alias in first_article_aliases
        )
        if author_ratio >= 0.8:
            score += 7
            reasons.append(f"first author fuzzy {author_ratio:.2f}")
        else:
            score -= 8
            reasons.append("first author differs")
    overlap = 0
    for row_aliases in row_author_alias_sets:
        if any(
            row_aliases & article_aliases for article_aliases in article.author_aliases
        ):
            overlap += 1
    if overlap:
        score += min(
            8,
            round(
                8
                * overlap
                / max(len(row_author_alias_sets), len(article.author_aliases))
            ),
        )
        reasons.append(f"{overlap} author overlap")
    return score, reasons


def score_year(row: dict[str, str], article: ArticleRecord) -> tuple[int, list[str]]:
    row_year = numeric_year(row["year"])
    if row_year is None or article.year_num is None:
        return 0, []
    delta = abs(row_year - article.year_num)
    if delta == 0:
        return 15, ["year exact"]
    if delta == 1:
        return 9, ["year differs by 1"]
    if delta == 2:
        return 5, ["year differs by 2"]
    return -10, [f"year differs by {delta}"]


def score_title(row: dict[str, str], article: ArticleRecord) -> tuple[int, list[str]]:
    title = row_title(row)
    if not title or not article.title:
        return 0, []
    title_key = normalize_text(title)
    title_score = max(
        ratio(title_key, article.title_key),
        token_ratio(title_tokens(title), article.title_tokens),
    )
    if title_score >= 0.96:
        return 25, ["title exact/fuzzy high"]
    if title_score >= 0.85:
        return 22, [f"title fuzzy {title_score:.2f}"]
    if title_score >= 0.7:
        return 15, [f"title fuzzy {title_score:.2f}"]
    if title_score >= 0.55:
        return 8, [f"title fuzzy {title_score:.2f}"]
    return -8, [f"title differs {title_score:.2f}"]


def score_container(
    row: dict[str, str], article: ArticleRecord
) -> tuple[int, list[str]]:
    container = row["container_title"] or row["book_title"] or row["publisher"]
    if not container or not article.citation_group_keys:
        return 0, []
    container_key = normalize_text(container)
    if container_key in article.citation_group_keys:
        return 18, ["citation group exact/abbreviation"]
    best = max(ratio(container_key, alias) for alias in article.citation_group_keys)
    best_abbreviation = max(
        abbreviation_token_ratio(container, alias)
        for alias in article.citation_group_aliases
    )
    if best_abbreviation >= 0.8:
        return 16, [f"citation group abbreviation {best_abbreviation:.2f}"]
    if best >= 0.9:
        return 14, [f"citation group fuzzy {best:.2f}"]
    if best >= 0.75:
        return 8, [f"citation group fuzzy {best:.2f}"]
    return 0, []


def score_volume_pages(
    row: dict[str, str], article: ArticleRecord
) -> tuple[int, list[str]]:
    score = 0
    reasons = []
    if row["volume"] and article.volume:
        if compact_key(row["volume"]) == compact_key(article.volume):
            score += 10
            reasons.append("volume exact")
        else:
            score -= 5
            reasons.append("volume differs")
    if row["issue"] and article.issue:
        if compact_key(row["issue"]) == compact_key(article.issue):
            score += 4
            reasons.append("issue exact")
    row_start = first_page(row["pages"])
    row_end = last_page(row["pages"])
    if row_start and article.start_page:
        if row_start == article.start_page:
            score += 10
            reasons.append("start page exact")
        elif row["reference_type"] != "scientific_description":
            score -= 5
            reasons.append("start page differs")
        else:
            row_page = page_number(row_start)
            start_page = page_number(article.start_page)
            end_page = page_number(article.end_page)
            if (
                row_page is not None
                and start_page is not None
                and end_page is not None
                and start_page <= row_page <= end_page
            ):
                score += 6
                reasons.append("description page in article range")
    if row_end and article.end_page and row_end == article.end_page:
        score += 5
        reasons.append("end page exact")
    return score, reasons


def score_type(row: dict[str, str], article: ArticleRecord) -> tuple[int, list[str]]:
    expected = EXPECTED_TYPES.get(row["reference_type"], set())
    if article.type in expected:
        return 8, ["type compatible"]
    return 0, []


def score_taxon_support(candidate: Candidate) -> tuple[int, list[str]]:
    if not candidate.taxon_supports:
        return 0, []
    unique_taxa = {label for label, _page_text in candidate.taxon_supports}
    exact_page_matches = 0
    range_page_matches = 0
    article_start = page_number(candidate.article.start_page)
    article_end = page_number(candidate.article.end_page)
    for _label, page_text in candidate.taxon_supports:
        page_token = first_page(page_text)
        if not page_token:
            continue
        if candidate.article.start_page and page_token == candidate.article.start_page:
            exact_page_matches += 1
            continue
        row_page = page_number(page_token)
        if (
            row_page is not None
            and article_start is not None
            and article_end is not None
            and article_start <= row_page <= article_end
        ):
            range_page_matches += 1
    score = 10 * min(len(unique_taxa), 4)
    reasons = [f"{len(unique_taxa)} described taxa support"]
    if exact_page_matches:
        score += 8 * min(exact_page_matches, 3)
        reasons.append(f"{exact_page_matches} exact taxon-page matches")
    elif range_page_matches:
        score += 4 * min(range_page_matches, 3)
        reasons.append(f"{range_page_matches} taxon pages in article range")
    return score, reasons


def score_candidate(row: dict[str, str], candidate: Candidate) -> ScoredCandidate:
    article = candidate.article
    score = 0
    reasons = []
    if any(method.startswith("described_taxon:") for method in candidate.methods):
        score += 75
        reasons.append("matched original citation from described taxon")
    if "url" in candidate.methods:
        score += 90
        reasons.append("URL exact")
    for scorer in (
        score_type,
        lambda row, article: score_authors(row_author_aliases(row["authors"]), article),
        score_year,
        score_title,
        score_container,
        score_volume_pages,
    ):
        part_score, part_reasons = scorer(row, article)
        score += part_score
        reasons.extend(part_reasons)
    taxon_score, taxon_reasons = score_taxon_support(candidate)
    score += taxon_score
    reasons.extend(taxon_reasons)
    return ScoredCandidate(
        article=article,
        score=max(0, score),
        methods=tuple(sorted(candidate.methods)),
        reasons=tuple(reasons),
    )


def candidate_signature(candidate: ScoredCandidate) -> tuple[str, ...]:
    article = candidate.article
    return (
        article.type_name,
        str(article.year_num or ""),
        article.title_key,
        normalize_text(article.citation_group),
        compact_key(article.volume),
        compact_key(article.issue),
        article.start_page,
        article.authors[0] if article.authors else "",
    )


def collapse_scored_candidates(
    scored: Sequence[ScoredCandidate],
) -> list[ScoredCandidate]:
    by_signature: dict[tuple[str, ...], ScoredCandidate] = {}
    for candidate in scored:
        signature = candidate_signature(candidate)
        existing = by_signature.get(signature)
        if existing is None or (candidate.score, -candidate.article.id) > (
            existing.score,
            -existing.article.id,
        ):
            by_signature[signature] = candidate
    return sorted(
        by_signature.values(),
        key=lambda candidate: (candidate.score, -candidate.article.id),
        reverse=True,
    )


def classify_match(
    row: dict[str, str], scored: Sequence[ScoredCandidate]
) -> tuple[str, ScoredCandidate | None]:
    if not scored:
        return "unmatched", None
    best = scored[0]
    runner_up = scored[1] if len(scored) > 1 else None
    gap = best.score - runner_up.score if runner_up is not None else best.score
    if row["reference_type"] in {"book", "book_chapter", "report", "thesis"}:
        if best.score >= 65 and gap >= 12:
            return "matched", best
    if best.score >= 88 and gap >= 3:
        return "matched", best
    if best.score >= 72 and gap >= 8:
        return "matched", best
    if best.score >= 72:
        return "ambiguous", best
    return "unmatched", None


def top_candidate_summary(scored: Sequence[ScoredCandidate]) -> str:
    return " | ".join(
        f"{candidate.article.id}:{candidate.score}:{candidate.article.name}"
        for candidate in scored[:5]
    )


def article_citation(article: Article) -> str:
    try:
        return article.cite()
    except Exception as error:
        return f"[could not cite Article {article.id}: {error}]"


def get_article_bhl_url(article: Article) -> str:
    urls = [article.url or ""]
    for tag in article.get_tags(article.tags, ArticleTag.AlternativeURL):
        urls.append(tag.url)
    for url in urls:
        if not url:
            continue
        try:
            parsed = urlparse.parse_url(url)
        except Exception:  # noqa: S112
            continue
        if isinstance(parsed, urlparse.BhlUrl):
            return str(parsed)
    return ""


def batlit_url(identifier: str) -> str:
    if identifier.startswith(("http://", "https://")):
        return identifier
    return f"https://www.zotero.org/groups/bat_literature_project/items/{identifier}"


def batlit_key(identifier: str) -> str:
    return identifier.rstrip("/").rsplit("/", maxsplit=1)[-1]


def infer_bhl_url(article: Article, mode: str) -> str:
    cfg = LintConfig(autofix=False, interactive=False, manual_mode=False)
    try:
        with lookup_mode(mode):
            if page := article_lint.get_inferred_bhl_page(article, cfg):
                return page.page_url
            if page_id := article_lint.get_inferred_bhl_page_from_articles(
                article, cfg
            ):
                return f"https://www.biodiversitylibrary.org/page/{page_id}"
    except (Exception, FreshNetworkCall):
        return ""
    return ""


def get_article_batlit(article: Article) -> tuple[str, str, str, str]:
    for tag in article.get_tags(article.tags, ArticleTag.BatLit):
        row = batlit.build_batlit_index().by_id.get(tag.zotero_id)
        citation = batlit.cite_row(row) if row is not None else ""
        return (
            batlit_key(tag.zotero_id),
            batlit_url(tag.zotero_id),
            getattr(tag, "zenodo_doi", ""),
            citation,
        )
    matches = [
        (row, match)
        for row, match in batlit.find_matches(article)
        if match.is_acceptable()
    ]
    if len(matches) != 1:
        return "", "", "", ""
    row, _match = matches[0]
    return (
        batlit_key(row["id"]),
        batlit_url(row["id"]),
        row["alternativeDoi"],
        batlit.cite_row(row),
    )


def row_batlit_candidates(
    row: dict[str, str],
    doi: str,
    index: ArticleIndex,
    learned_mappings: LearnedMappings | None = None,
) -> list[batlit.BatLitRow]:
    batlit_index = batlit.build_batlit_index()
    candidates: list[batlit.BatLitRow] = []
    if doi:
        candidates.extend(batlit_index.by_doi.get(doi.casefold(), ()))
    title = row_title(row)
    if title:
        candidates.extend(batlit_index.by_title.get(helpers.simplify_string(title), ()))
    if row["volume"]:
        for alias in row_citation_group_aliases(row, index, learned_mappings):
            candidates.extend(
                batlit_index.by_journal_volume.get(
                    (helpers.simplify_string(alias), row["volume"]), ()
                )
            )
    return list({candidate["id"]: candidate for candidate in candidates}.values())


def score_batlit_row(
    row: dict[str, str], batlit_row: batlit.BatLitRow, doi: str
) -> tuple[int, list[str]]:
    score = 0
    reasons = []
    if doi and doi.casefold() == batlit_row["doi"].casefold():
        score += 90
        reasons.append("DOI exact")
    title = row_title(row)
    if title and batlit_row["title"]:
        title_score = ratio(normalize_text(title), normalize_text(batlit_row["title"]))
        if title_score >= 0.95:
            score += 30
            reasons.append("title exact/fuzzy high")
        elif title_score >= 0.85:
            score += 22
            reasons.append(f"title fuzzy {title_score:.2f}")
        elif title_score >= 0.7:
            score += 12
            reasons.append(f"title fuzzy {title_score:.2f}")
    row_year = numeric_year(row["year"])
    batlit_year = numeric_year(batlit_row["date"])
    if row_year is not None and batlit_year is not None:
        if row_year == batlit_year:
            score += 12
            reasons.append("year exact")
        elif abs(row_year - batlit_year) == 1:
            score += 6
            reasons.append("year differs by 1")
    row_authors = split_author_families(row["authors"])
    batlit_authors = tuple(
        compact_key(author.strip().split(",", maxsplit=1)[0])
        for author in batlit_row["authors"].split("|")
        if author.strip()
    )
    if row_authors and batlit_authors:
        if row_authors[0] == batlit_authors[0]:
            score += 10
            reasons.append("first author exact")
        overlap = len(set(row_authors) & set(batlit_authors))
        if overlap:
            score += min(6, overlap)
            reasons.append(f"{overlap} author overlap")
    if row["volume"] and row["volume"] == batlit_row["volume"]:
        score += 6
        reasons.append("volume exact")
    if first_page(row["pages"]) and first_page(row["pages"]) == first_page(
        batlit_row["pages"]
    ):
        score += 6
        reasons.append("start page exact")
    return min(score, 100), reasons


def get_row_batlit(
    row: dict[str, str],
    doi: str,
    index: ArticleIndex,
    learned_mappings: LearnedMappings | None = None,
) -> tuple[str, str, str, str, str]:
    scored = []
    for candidate in row_batlit_candidates(row, doi, index, learned_mappings):
        score, reasons = score_batlit_row(row, candidate, doi)
        scored.append((score, reasons, candidate))
    scored.sort(key=lambda item: item[0], reverse=True)
    if not scored:
        return "", "", "", "", ""
    best_score, reasons, best = scored[0]
    runner_up = scored[1][0] if len(scored) > 1 else 0
    if best_score < 72 or best_score - runner_up < 8:
        return "", "", "", "", ""
    return (
        batlit_key(best["id"]),
        batlit_url(best["id"]),
        best["alternativeDoi"],
        batlit.cite_row(best),
        f"batlit search (score {best_score})",
    )


def article_doi(article: Article, *, mode: str) -> tuple[str, str]:
    if article.doi:
        return article.doi, "taxonomy Article.doi"
    if mode == "off":
        return "", ""
    cfg = LintConfig(autofix=False, interactive=False, manual_mode=False)
    try:
        with lookup_mode(mode):
            for doi in api_data.get_candidate_dois_from_crossref(article):
                data = api_data.expand_doi_json(doi)
                if data and article_lint.is_candidate_doi_acceptable(
                    article, doi, data, cfg
                ):
                    return doi, f"Crossref inferred from taxonomy Article ({mode})"
    except (Exception, FreshNetworkCall):
        return "", ""
    return "", ""


def citation_group_from_index(
    index: ArticleIndex, citation_group_id: int
) -> CitationGroup | None:
    article_id = index.citation_group_sample_article_id.get(citation_group_id)
    if article_id is None:
        return None
    return get_article(article_id).citation_group


def citation_group_is_doi_likely(
    citation_group_id: int, row_year: int | None, index: ArticleIndex
) -> bool:
    if citation_group_id in index.citation_groups_with_doi:
        return True
    if row_year is None:
        return False
    citation_group = citation_group_from_index(index, citation_group_id)
    if citation_group is None:
        return False
    return citation_group.may_have_article_identifier(ArticleIdentifier.doi, row_year)


def row_doi_candidates_from_title_search(
    row: dict[str, str], citation_group_id: int, index: ArticleIndex, *, mode: str
) -> Iterable[str]:
    row_year = numeric_year(row["year"])
    title = row_title(row)
    if not title or row_year is None:
        return ()
    if (
        not citation_group_is_doi_likely(citation_group_id, row_year, index)
        and row_year < 2000
    ):
        return ()
    citation_group = citation_group_from_index(index, citation_group_id)
    if citation_group is None:
        return ()
    title_query = helpers.simplify_string(
        title, clean_words=False, keep_whitespace=True
    )
    if not title_query:
        return ()
    seen: set[str] = set()
    dois = []
    for issn in citation_group.get_issns():
        params = {
            "query.title": title_query,
            "filter": (
                f"issn:{issn},from-pub-date:{row_year - 2},until-pub-date:{row_year + 2}"
            ),
            "rows": 8,
            "select": "DOI",
        }
        try:
            with lookup_mode(mode):
                data = api_data.get_crossref_search_by_journal(
                    json.dumps({"issn": issn, "params": params})
                )
        except (Exception, FreshNetworkCall):  # noqa: S112
            continue
        try:
            items = json.loads(data).get("message", {}).get("items", ())
        except Exception:  # noqa: S112
            continue
        for item in items:
            doi = item.get("DOI")
            if doi and doi not in seen:
                seen.add(doi)
                dois.append(doi)
    return dois


def compact_query(query: dict[str, str]) -> dict[str, str]:
    return {key: value for key, value in query.items() if value}


def row_openurl_queries(
    row: dict[str, str], *, journal_title: str = ""
) -> list[dict[str, str]]:
    title = row_title(row)
    if not title:
        return []
    year = numeric_year(row["year"])
    first_author = first_row_author_for_crossref(row["authors"])
    start_page = first_page(row["pages"])
    base = {"pid": api_data._options.crossrefid, "noredirect": "true"}
    queries = [
        compact_query(
            {
                **base,
                "atitle": helpers.simplify_string(
                    title, clean_words=False, keep_whitespace=True
                ),
                "aulast": first_author,
                "date": str(year) if year is not None else "",
                "title": journal_title or row["container_title"],
                "volume": row["volume"],
                "issue": row["issue"],
                "spage": start_page or "",
            }
        ),
        compact_query(
            {
                **base,
                "title": row["container_title"],
                "aulast": first_author,
                "date": str(year) if year is not None else "",
                "volume": row["volume"],
                "issue": row["issue"],
                "spage": start_page or "",
            }
        ),
        compact_query(
            {
                **base,
                "atitle": helpers.simplify_string(
                    title, clean_words=False, keep_whitespace=True
                ),
                "aulast": first_author,
                "date": str(year) if year is not None else "",
                "volume": row["volume"],
                "issue": row["issue"],
                "spage": start_page or "",
            }
        ),
    ]
    unique_queries = []
    seen = set()
    for query in queries:
        if len(query) <= len(base):
            continue
        key = tuple(sorted(query.items()))
        if key not in seen:
            seen.add(key)
            unique_queries.append(query)
    return unique_queries


def row_matches_doi_data(
    row: dict[str, str],
    data: dict[str, object],
    *,
    citation_group_aliases: Iterable[str] = (),
) -> bool:
    title = row_title(row)
    doi_title = data.get("title")
    if not isinstance(doi_title, str) or not title:
        return False
    title_score = max(
        ratio(normalize_text(title), normalize_text(doi_title)),
        token_ratio(title_tokens(title), title_tokens(doi_title)),
    )
    prefix_match = (
        len(title) > 12
        and len(doi_title) > 12
        and (
            normalize_text(title).startswith(normalize_text(doi_title))
            or normalize_text(doi_title).startswith(normalize_text(title))
        )
    )
    if title_score < 0.72 and not prefix_match:
        return False
    score = 0
    if title_score >= 0.96:
        score += 8
    elif title_score >= 0.88:
        score += 7
    elif title_score >= 0.8:
        score += 5
    elif prefix_match:
        score += 4
    else:
        score += 2
    row_year = numeric_year(row["year"])
    doi_year = numeric_year(str(data.get("year") or ""))
    if row_year is not None and doi_year is not None:
        delta = abs(row_year - doi_year)
        if delta == 0:
            score += 4
        elif delta == 1:
            score += 3
        elif delta == 2:
            score += 1
    doi_volume = compact_key(str(data.get("volume") or ""))
    if row["volume"] and doi_volume:
        if compact_key(row["volume"]) == doi_volume:
            score += 3
        else:
            score -= 2
    doi_issue = compact_key(str(data.get("issue") or ""))
    if row["issue"] and doi_issue and compact_key(row["issue"]) == doi_issue:
        score += 1
    doi_start = normalize_page(str(data.get("start_page") or ""))
    doi_end = normalize_page(str(data.get("end_page") or ""))
    row_start = first_page(row["pages"])
    row_end = last_page(row["pages"])
    if row_start and doi_start:
        if row_start == doi_start:
            score += 4
        else:
            row_page = page_number(row_start)
            doi_start_num = page_number(doi_start)
            doi_end_num = page_number(doi_end)
            if (
                row_page is not None
                and doi_start_num is not None
                and doi_end_num is not None
                and doi_start_num <= row_page <= doi_end_num
            ):
                score += 2
    if row_end and doi_end and row_end == doi_end:
        score += 1
    doi_journal = str(data.get("journal") or "")
    if doi_journal and citation_group_aliases:
        best_container = max(
            citation_group_match_score(doi_journal, alias)
            for alias in citation_group_aliases
        )
        if best_container >= 0.95:
            score += 3
        elif best_container >= 0.82:
            score += 2
    doi_authors = data.get("author_tags") or []
    row_authors = row_author_aliases(row["authors"])
    doi_author_aliases: list[set[str]] = []
    for author in doi_authors if isinstance(doi_authors, Iterable) else ():
        family_name = getattr(author, "family_name", "")
        if family_name:
            doi_author_aliases.append({compact_key(family_name)})
    if row_authors and doi_author_aliases:
        overlap = 0
        for row_aliases in row_authors:
            if any(row_aliases & doi_aliases for doi_aliases in doi_author_aliases):
                overlap += 1
        if overlap:
            score += min(3, overlap)
    return score >= 9 and (title_score >= 0.8 or prefix_match or score >= 12)


def row_doi_from_crossref(
    row: dict[str, str],
    index: ArticleIndex,
    *,
    mode: str,
    learned_mappings: LearnedMappings | None = None,
) -> tuple[str, str]:
    if mode == "off":
        return "", ""
    if row["reference_type"] not in {"journal_article", "scientific_description"}:
        return "", ""
    citation_group_ids = row_doi_citation_group_ids(row, index, learned_mappings)
    citation_group_aliases: set[str] = set()
    seen: set[str] = set()
    for citation_group_id in citation_group_ids:
        citation_group_aliases.update(
            index.citation_group_aliases_by_id.get(citation_group_id, ())
        )
        citation_group = citation_group_from_index(index, citation_group_id)
        if citation_group is None:
            continue
        if row["volume"] and first_page(row["pages"]):
            queries = [
                {
                    "pid": api_data._options.crossrefid,
                    "title": citation_group.name,
                    "volume": row["volume"],
                    "spage": first_page(row["pages"]),
                    "noredirect": "true",
                }
            ]
            for issn in citation_group.get_issns():
                queries.append(
                    {
                        "pid": api_data._options.crossrefid,
                        "issn": issn,
                        "volume": row["volume"],
                        "spage": first_page(row["pages"]),
                        "noredirect": "true",
                    }
                )
            for query in queries:
                try:
                    with lookup_mode(mode):
                        doi = api_data._try_query(query)
                except (Exception, FreshNetworkCall):  # noqa: S112
                    continue
                if not doi or doi in seen:
                    continue
                seen.add(doi)
                try:
                    with lookup_mode(mode):
                        data = api_data.expand_doi_json(doi)
                except (Exception, FreshNetworkCall):  # noqa: S112
                    continue
                if data and row_matches_doi_data(
                    row, data, citation_group_aliases=citation_group_aliases
                ):
                    return doi, f"Crossref inferred from parsed reference ({mode})"
        for query in row_openurl_queries(row, journal_title=citation_group.name):
            try:
                with lookup_mode(mode):
                    doi = api_data._try_query(query)
            except (Exception, FreshNetworkCall):  # noqa: S112
                continue
            if not doi or doi in seen:
                continue
            seen.add(doi)
            try:
                with lookup_mode(mode):
                    data = api_data.expand_doi_json(doi)
            except (Exception, FreshNetworkCall):  # noqa: S112
                continue
            if data and row_matches_doi_data(
                row, data, citation_group_aliases=citation_group_aliases
            ):
                return doi, f"Crossref inferred from parsed reference ({mode})"
        for doi in row_doi_candidates_from_title_search(
            row, citation_group_id, index, mode=mode
        ):
            if doi in seen:
                continue
            seen.add(doi)
            try:
                with lookup_mode(mode):
                    data = api_data.expand_doi_json(doi)
            except (Exception, FreshNetworkCall):  # noqa: S112
                continue
            if data and row_matches_doi_data(
                row, data, citation_group_aliases=citation_group_aliases
            ):
                return doi, f"Crossref inferred from parsed reference ({mode})"
    fallback_aliases = set(citation_group_aliases)
    if row["container_title"]:
        fallback_aliases.add(row["container_title"])
    for query in row_openurl_queries(row):
        try:
            with lookup_mode(mode):
                doi = api_data._try_query(query)
        except (Exception, FreshNetworkCall):  # noqa: S112
            continue
        if not doi or doi in seen:
            continue
        seen.add(doi)
        try:
            with lookup_mode(mode):
                data = api_data.expand_doi_json(doi)
        except (Exception, FreshNetworkCall):  # noqa: S112
            continue
        if data and row_matches_doi_data(
            row, data, citation_group_aliases=fallback_aliases
        ):
            return doi, f"Crossref inferred from parsed reference ({mode})"
    return "", ""


def update_summary(summary: MatchSummary, row: dict[str, str]) -> None:
    summary.status_counts[row["taxonomy_match_status"]] += 1
    if row["taxonomy_article_id"]:
        summary.link_counts["taxonomy"] += 1
    if row["doi"]:
        summary.link_counts["doi"] += 1
        summary.doi_sources[row["doi_source"] or "unknown"] += 1
    if row["batlit_id"]:
        summary.link_counts["batlit"] += 1
        summary.batlit_sources[row["batlit_source"] or "unknown"] += 1
    if row["bhl_url"]:
        summary.link_counts["bhl"] += 1
        summary.bhl_sources[row["bhl_source"] or "unknown"] += 1


def print_progress(processed: int, total: int, summary: MatchSummary) -> None:
    print(
        f"  {processed}/{total} rows | taxonomy={summary.link_counts['taxonomy']} doi={summary.link_counts['doi']} batlit={summary.link_counts['batlit']} bhl={summary.link_counts['bhl']}",
        flush=True,
    )


def print_summary(summary: MatchSummary, total_rows: int) -> None:
    print("Summary:", flush=True)
    print(
        f"  taxonomy matches: {summary.link_counts['taxonomy']} / {total_rows} (matched={summary.status_counts['matched']}, ambiguous={summary.status_counts['ambiguous']}, unmatched={summary.status_counts['unmatched']})",
        flush=True,
    )
    print(f"  DOI links: {summary.link_counts['doi']}", flush=True)
    for source, count in summary.doi_sources.most_common():
        print(f"    {count}: {source}", flush=True)
    print(f"  BatLit links: {summary.link_counts['batlit']}", flush=True)
    for source, count in summary.batlit_sources.most_common():
        print(f"    {count}: {source}", flush=True)
    print(f"  BHL links: {summary.link_counts['bhl']}", flush=True)
    for source, count in summary.bhl_sources.most_common():
        print(f"    {count}: {source}", flush=True)


def evaluate_row(
    row_number: int,
    row: dict[str, str],
    index: ArticleIndex,
    *,
    doi_mode: str,
    bhl_mode: str,
    learned_mappings: LearnedMappings | None = None,
) -> RowEvaluation:
    candidates = find_candidates(row, index, learned_mappings)
    scored = sorted(
        (score_candidate(row, candidate) for candidate in candidates.values()),
        key=lambda candidate: candidate.score,
        reverse=True,
    )
    scored = collapse_scored_candidates(scored)
    status, match = classify_match(row, scored)
    output_row = {
        **row,
        **external_columns(
            row_number,
            row,
            status,
            match,
            scored,
            index,
            doi_mode=doi_mode,
            bhl_mode=bhl_mode,
            learned_mappings=learned_mappings,
        ),
    }
    return RowEvaluation(
        row_number=row_number,
        input_row=row,
        status=status,
        match=match,
        scored=tuple(scored),
        output_row=output_row,
    )


def run_matching_pass(
    rows: Sequence[dict[str, str]],
    index: ArticleIndex,
    *,
    doi_mode: str,
    bhl_mode: str,
    learned_mappings: LearnedMappings | None = None,
    label: str,
) -> tuple[list[RowEvaluation], MatchSummary]:
    print(f"{label}: matching references...", flush=True)
    evaluations = []
    summary = MatchSummary()
    for row_number, row in enumerate(rows, start=1):
        evaluation = evaluate_row(
            row_number,
            row,
            index,
            doi_mode=doi_mode,
            bhl_mode=bhl_mode,
            learned_mappings=learned_mappings,
        )
        evaluations.append(evaluation)
        update_summary(summary, evaluation.output_row)
        if row_number % ROW_PROGRESS_EVERY == 0 or row_number == len(rows):
            print_progress(row_number, len(rows), summary)
    return evaluations, summary


def secure_taxonomy_match(evaluation: RowEvaluation) -> bool:
    if evaluation.status != "matched" or evaluation.match is None:
        return False
    runner_up = evaluation.scored[1] if len(evaluation.scored) > 1 else None
    gap = (
        evaluation.match.score - runner_up.score
        if runner_up is not None
        else evaluation.match.score
    )
    if evaluation.match.score >= 100:
        return True
    return evaluation.match.score >= 88 and gap >= 12


def infer_citation_group_id_from_journal_name(
    journal_name: str, index: ArticleIndex
) -> int | None:
    exact = index.citation_group_by_alias.get(normalize_text(journal_name), set())
    if len(exact) == 1:
        return next(iter(exact))
    best_id = None
    best_score = 0.0
    second_score = 0.0
    for citation_group_id, aliases in index.citation_group_aliases_by_id.items():
        score = max(
            citation_group_match_score(journal_name, alias) for alias in aliases
        )
        if score > best_score:
            second_score = best_score
            best_score = score
            best_id = citation_group_id
        elif score > second_score:
            second_score = score
    if best_id is None:
        return None
    if best_score >= 0.92 and best_score - second_score >= 0.05:
        return best_id
    return None


def build_learned_mappings(
    evaluations: Sequence[RowEvaluation], index: ArticleIndex, *, doi_mode: str
) -> LearnedMappings:
    support: defaultdict[str, Counter[int]] = defaultdict(Counter)
    for evaluation in evaluations:
        container = evaluation.input_row["container_title"]
        container_key = normalize_text(container)
        if not container_key:
            continue
        if (
            secure_taxonomy_match(evaluation)
            and evaluation.match is not None
            and evaluation.match.article.citation_group_id is not None
        ):
            support[container_key][evaluation.match.article.citation_group_id] += 2
        doi = evaluation.output_row["doi"]
        if not doi:
            continue
        try:
            with lookup_mode(doi_mode):
                data = api_data.expand_doi_json(doi)
        except (Exception, FreshNetworkCall):  # noqa: S112
            continue
        if not data:
            continue
        journal_name = data.get("journal")
        if not isinstance(journal_name, str) or not journal_name:
            continue
        citation_group_id = infer_citation_group_id_from_journal_name(
            journal_name, index
        )
        if citation_group_id is not None:
            support[container_key][citation_group_id] += 1
    learned = {}
    for container_key, counter in support.items():
        top = counter.most_common(2)
        if not top:
            continue
        best_id, best_support = top[0]
        second_support = top[1][1] if len(top) > 1 else 0
        if best_support >= 2 and best_support > second_support:
            learned[container_key] = best_id
    return LearnedMappings(citation_group_by_container_key=learned)


def print_round_gains(
    first_pass: Sequence[RowEvaluation], final_rows: Sequence[dict[str, str]]
) -> None:
    taxonomy_new_matches = 0
    doi_new_links = 0
    batlit_new_links = 0
    bhl_new_links = 0
    for first, final_row in zip(first_pass, final_rows, strict=True):
        if (
            final_row["taxonomy_match_status"] == "matched"
            and first.output_row["taxonomy_match_status"] != "matched"
        ):
            taxonomy_new_matches += 1
        if final_row["doi"] and not first.output_row["doi"]:
            doi_new_links += 1
        if final_row["batlit_id"] and not first.output_row["batlit_id"]:
            batlit_new_links += 1
        if final_row["bhl_url"] and not first.output_row["bhl_url"]:
            bhl_new_links += 1
    print(
        f"Round 2 gains: taxonomy_new_matches={taxonomy_new_matches} doi_new_links={doi_new_links} batlit_new_links={batlit_new_links} bhl_new_links={bhl_new_links}",
        flush=True,
    )


def taxonomy_status_rank(status: str) -> int:
    return {"matched": 2, "ambiguous": 1, "unmatched": 0}.get(status, -1)


def taxonomy_score_from_row(row: dict[str, str]) -> int:
    score = row["taxonomy_match_score"]
    return int(score) if score.isdigit() else -1


def merge_round_rows(first: RowEvaluation, second: RowEvaluation) -> dict[str, str]:
    merged = dict(second.output_row)
    first_row = first.output_row
    second_row = second.output_row
    taxonomy_fields = (
        "taxonomy_match_status",
        "taxonomy_match_score",
        "taxonomy_match_method",
        "taxonomy_match_reasons",
        "taxonomy_candidate_count",
        "taxonomy_top_candidates",
        "taxonomy_article_id",
        "taxonomy_article_name",
        "taxonomy_citation",
    )
    first_taxonomy = (
        taxonomy_status_rank(first_row["taxonomy_match_status"]),
        taxonomy_score_from_row(first_row),
    )
    second_taxonomy = (
        taxonomy_status_rank(second_row["taxonomy_match_status"]),
        taxonomy_score_from_row(second_row),
    )
    if first_taxonomy > second_taxonomy:
        for field in taxonomy_fields:
            merged[field] = first_row[field]
    if not merged["doi"] and first_row["doi"]:
        for field in ("doi", "doi_source"):
            merged[field] = first_row[field]
    if not merged["batlit_id"] and first_row["batlit_id"]:
        for field in (
            "batlit_id",
            "batlit_url",
            "batlit_zenodo_doi",
            "batlit_citation",
            "batlit_source",
        ):
            merged[field] = first_row[field]
    if not merged["bhl_url"] and first_row["bhl_url"]:
        for field in ("bhl_url", "bhl_source"):
            merged[field] = first_row[field]
    return merged


def external_columns(
    row_number: int,
    row: dict[str, str],
    status: str,
    match: ScoredCandidate | None,
    scored: Sequence[ScoredCandidate],
    index: ArticleIndex,
    *,
    doi_mode: str,
    bhl_mode: str,
    learned_mappings: LearnedMappings | None = None,
) -> dict[str, str]:
    out = dict.fromkeys(MATCH_FIELDS, "")
    out["source_row"] = str(row_number)
    out["taxonomy_match_status"] = status
    out["taxonomy_candidate_count"] = str(len(scored))
    out["taxonomy_top_candidates"] = top_candidate_summary(scored)
    article = (
        get_article(match.article.id)
        if status == "matched" and match is not None
        else None
    )
    doi = ""
    if article is not None:
        doi, doi_source = article_doi(article, mode=doi_mode)
        out["doi"] = doi
        out["doi_source"] = doi_source
        bhl_url = get_article_bhl_url(article)
        if bhl_url:
            out["bhl_url"] = bhl_url
            out["bhl_source"] = "taxonomy Article URL"
        elif bhl_mode != "off" and (bhl_url := infer_bhl_url(article, bhl_mode)):
            out["bhl_url"] = bhl_url
            out["bhl_source"] = f"BHL inferred from taxonomy Article ({bhl_mode})"
        batlit_id, batlit_url, batlit_zenodo_doi, batlit_citation = get_article_batlit(
            article
        )
        if batlit_id:
            out["batlit_id"] = batlit_id
            out["batlit_url"] = batlit_url
            out["batlit_zenodo_doi"] = batlit_zenodo_doi
            out["batlit_citation"] = batlit_citation
            out["batlit_source"] = "taxonomy Article tag/search"
    if not out["doi"] and doi_mode != "off":
        out["doi"], out["doi_source"] = row_doi_from_crossref(
            row, index, mode=doi_mode, learned_mappings=learned_mappings
        )
        doi = out["doi"]
    if not out["batlit_id"]:
        batlit_id, batlit_url, batlit_zenodo_doi, batlit_citation, batlit_source = (
            get_row_batlit(
                row, doi or out["doi"], index, learned_mappings=learned_mappings
            )
        )
        if batlit_id:
            out["batlit_id"] = batlit_id
            out["batlit_url"] = batlit_url
            out["batlit_zenodo_doi"] = batlit_zenodo_doi
            out["batlit_citation"] = batlit_citation
            out["batlit_source"] = batlit_source
    if article is None:
        return out
    assert match is not None
    matched_record = match.article
    out.update(
        {
            "taxonomy_match_score": str(match.score),
            "taxonomy_match_method": " | ".join(match.methods),
            "taxonomy_match_reasons": " | ".join(match.reasons),
            "taxonomy_article_id": str(matched_record.id),
            "taxonomy_article_name": matched_record.name,
            "taxonomy_citation": article_citation(get_article(matched_record.id)),
        }
    )
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match parsed HMW Chiroptera references to taxonomy database Articles."
    )
    parser.set_defaults(doi_mode="cached", bhl_mode="cached")
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Stage 2 parsed CSV to read (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Stage 3 CSV to write (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--doi-mode",
        choices=LOOKUP_MODES,
        help="DOI inference mode: off, cached-only, or cached plus fresh network calls (default: cached).",
    )
    parser.add_argument(
        "--bhl-mode",
        choices=LOOKUP_MODES,
        help="BHL inference mode: off, cached-only, or cached plus fresh network calls (default: cached).",
    )
    parser.add_argument(
        "--infer-doi",
        action="store_const",
        const="network",
        dest="doi_mode",
        help="Deprecated alias for --doi-mode=network.",
    )
    parser.add_argument(
        "--infer-bhl",
        action="store_const",
        const="network",
        dest="bhl_mode",
        help="Deprecated alias for --bhl-mode=network.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(
        f"Reading parsed references from {args.input} (doi_mode={args.doi_mode}, bhl_mode={args.bhl_mode})...",
        flush=True,
    )
    with args.input.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        input_fields = reader.fieldnames or []
    print(f"Loaded {len(rows)} parsed references.", flush=True)

    index = build_article_index()
    output_fields = [*input_fields, *MATCH_FIELDS]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    first_pass, _first_summary = run_matching_pass(
        rows, index, doi_mode=args.doi_mode, bhl_mode=args.bhl_mode, label="Round 1"
    )
    learned_mappings = build_learned_mappings(first_pass, index, doi_mode=args.doi_mode)
    print(
        f"Learned {len(learned_mappings.citation_group_by_container_key)} citation-group mappings from round 1.",
        flush=True,
    )
    second_pass, _second_summary = run_matching_pass(
        rows,
        index,
        doi_mode=args.doi_mode,
        bhl_mode=args.bhl_mode,
        learned_mappings=learned_mappings,
        label="Round 2",
    )
    final_rows = [
        merge_round_rows(first, second)
        for first, second in zip(first_pass, second_pass, strict=True)
    ]
    summary = MatchSummary()
    for row in final_rows:
        update_summary(summary, row)
    print_round_gains(first_pass, final_rows)
    print(f"Writing {args.output}...", flush=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(f, output_fields)
        writer.writeheader()
        for row in final_rows:
            writer.writerow(row)
    print(f"Wrote taxonomy matches for {len(rows)} references to {args.output}")
    print_summary(summary, len(rows))


if __name__ == "__main__":
    main()
