from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import requests

from taxonomy.db.constants import ArticleType
from taxonomy.db.models.article.article import Article, ArticleTag
from taxonomy.db.models.citation_group.cg import CitationGroup
from taxonomy.db.url_cache import CacheDomain, cached

# --- API helpers -------------------------------------------------------------

IDCONV_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
EUROPEPMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"


@cached(CacheDomain.europe_pmc_search)
def _europe_pmc_cached(params_json: str) -> str:
    params = json.loads(params_json)
    resp = requests.get(
        EUROPEPMC_SEARCH_URL,
        params=params,
        timeout=20,
        headers={"User-Agent": "taxonomy-pmid-infer/1.0"},
    )
    resp.raise_for_status()
    return resp.text


@cached(CacheDomain.ncbi_idconv)
def _idconv_cached(params_json: str) -> str:
    params = json.loads(params_json)
    resp = requests.get(
        IDCONV_URL,
        params=params,
        timeout=20,
        headers={"User-Agent": "taxonomy-pmid-infer/1.0"},
    )
    resp.raise_for_status()
    return resp.text


def _http_get_json_europe_pmc(params: dict[str, str]) -> dict[str, Any] | None:
    try:
        text = _europe_pmc_cached(json.dumps(params, sort_keys=True))
        return json.loads(text)
    except requests.RequestException as e:
        print(f"HTTP error for Europe PMC: {e}")
        return None


def _http_get_json_idconv(params: dict[str, str]) -> dict[str, Any] | None:
    try:
        text = _idconv_cached(json.dumps(params, sort_keys=True))
        return json.loads(text)
    except requests.RequestException as e:
        print(f"HTTP error for NCBI idconv: {e}")
        return None


def lookup_pmid_via_idconv(identifier: str) -> str | None:
    """Use NCBI idconv to map DOI or PMCID to PMID.

    Accepts either a DOI (e.g., "10.1000/xyz") or a PMCID (e.g., "PMC123456").
    """
    params = {"format": "json", "ids": identifier}
    data = _http_get_json_idconv(params)
    if not data:
        return None
    for rec in data.get("records", []):
        pmid = rec.get("pmid")
        if pmid:
            return str(pmid)
    return None


def lookup_pmid_via_europe_pmc_by_doi(doi: str) -> str | None:
    params = {"format": "json", "pageSize": "25", "query": f"DOI:{doi}"}
    data = _http_get_json_europe_pmc(params)
    if not data:
        return None
    results = (data.get("resultList") or {}).get("result") or []
    for rec in results:
        # Prefer exact DOI match, and presence of pmid
        if str(rec.get("doi", "")).lower() == doi.lower():
            pmid = rec.get("pmid")
            if pmid:
                return str(pmid)
    # fallback: first record with pmid
    for rec in results:
        pmid = rec.get("pmid")
        if pmid:
            return str(pmid)
    return None


def lookup_pmid_via_europe_pmc_by_metadata(
    *, title: str | None, journal: str | None, year: str | None
) -> str | None:
    # Keep this conservative: title phrase + year; journal if available.
    if not title:
        return None
    q = f'TITLE:"{title}"'
    if year and year.isdigit():
        q += f" AND PUB_YEAR:{year}"
    if journal:
        q += f' AND JOURNAL:"{journal}"'
    params = {"format": "json", "pageSize": "25", "query": q}
    data = _http_get_json_europe_pmc(params)
    if not data:
        return None
    results = (data.get("resultList") or {}).get("result") or []
    # Try strict normalized title match first
    norm = lambda s: " ".join(s.lower().split())
    target_title = norm(title)
    for rec in results:
        rec_title = rec.get("title")
        pmid = rec.get("pmid")
        if pmid and rec_title and norm(rec_title) == target_title:
            return str(pmid)
    # Fallback: first result with pmid
    for rec in results:
        pmid = rec.get("pmid")
        if pmid:
            return str(pmid)
    return None


# --- Core logic --------------------------------------------------------------


@dataclass
class Candidate:
    id: int
    name: str
    doi: str | None
    pmcid: str | None
    title: str | None
    journal: str | None
    year: str | None


def iter_articles_missing_pmid(*, only_if_existing: bool = False) -> Iterable[Article]:
    cgs_with_pmids = set()
    if only_if_existing:
        arts = Article.select_valid().filter(
            Article.tags.contains(f"[{ArticleTag.PMID._tag},")
        )
        for art in arts:
            if art.has_tag(ArticleTag.PMID):
                if art.citation_group:
                    cgs_with_pmids.add(art.citation_group)
    for art in Article.select_valid().filter(
        Article.type == ArticleType.JOURNAL, Article.year > "1950"
    ):
        if art.get_identifier(ArticleTag.PMID):
            continue
        if cgs_with_pmids and art.citation_group not in cgs_with_pmids:
            continue
        yield art


def get_pmcid_from_tags(art: Article) -> str | None:
    value = art.get_identifier(ArticleTag.PMC)
    if not value:
        return None
    if value.startswith("PMC"):
        return value
    # Europe/NLM sometimes store numeric only; normalize with PMC prefix
    return f"PMC{value}"


def to_candidate(art: Article) -> Candidate:
    return Candidate(
        id=art.id,
        name=art.name,
        doi=art.doi,
        pmcid=get_pmcid_from_tags(art),
        title=art.title,
        journal=art.citation_group.name if art.citation_group else None,
        year=art.year,
    )


def infer_pmid_for_article(art: Article, *, allow_metadata: bool = False) -> str | None:
    cand = to_candidate(art)

    # 1) If PMCID present, map via idconv
    if cand.pmcid:
        pmid = lookup_pmid_via_idconv(cand.pmcid)
        if pmid:
            return pmid

    # 2) If DOI present, try idconv and then Europe PMC
    if cand.doi:
        pmid = lookup_pmid_via_idconv(cand.doi)
        if pmid:
            return pmid
        pmid = lookup_pmid_via_europe_pmc_by_doi(cand.doi)
        if pmid:
            return pmid

    # 3) Conservative metadata search (title + year [+ journal])
    if allow_metadata:
        pmid = lookup_pmid_via_europe_pmc_by_metadata(
            title=cand.title, journal=cand.journal, year=cand.year
        )
        if pmid:
            return pmid
    return None


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Infer and add PMID tags to articles")
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of articles processed"
    )
    parser.add_argument(
        "--apply", action="store_true", help="Apply changes (default is dry-run)"
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between API calls (politeness)",
    )
    parser.add_argument(
        "--enable-metadata",
        action="store_true",
        help="Also try title/journal/year search on Europe PMC (more coverage, slightly riskier)",
    )
    parser.add_argument(
        "--assume-journal-consistency",
        action="store_true",
        help="If one article from a journal does not have a PMID, assume others won't either (speeds up processing)",
    )
    parser.add_argument(
        "--only-if-existing",
        action="store_true",
        help="Only run for articles in journals where we already have a PMID",
    )
    args = parser.parse_args(argv)

    processed = 0
    added = 0
    skipped_journals: set[CitationGroup] = set()
    num_skipped_due_to_journal = 0
    for art in iter_articles_missing_pmid(only_if_existing=args.only_if_existing):
        if args.limit is not None and processed >= args.limit:
            break
        processed += 1
        if art.citation_group in skipped_journals:
            num_skipped_due_to_journal += 1
            continue

        pmid = infer_pmid_for_article(art, allow_metadata=args.enable_metadata)
        if pmid:
            print(f"{art.id}: {art.name} -> PMID {pmid}")
            if args.apply:
                art.add_tag(ArticleTag.PMID(pmid))
            added += 1
        else:
            print(f"{art.id}: {art.name} -> no PMID found")
            if args.assume_journal_consistency and art.citation_group:
                skipped_journals.add(art.citation_group)

        if args.sleep:
            time.sleep(args.sleep)

    print(
        f"Processed {processed} articles; {'added' if args.apply else 'would add'} {added} PMIDs."
    )
    if num_skipped_due_to_journal > 0:
        print(
            f"Skipped {num_skipped_due_to_journal} articles due to journal consistency."
        )
    if not args.apply:
        print("Dry run; rerun with --apply to write tags.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
