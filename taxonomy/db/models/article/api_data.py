"""Get data about an article from an API."""

import json
import re
import traceback
import urllib.parse
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import clirm
import httpx
import requests
from bs4 import BeautifulSoup

from taxonomy import config, parsing
from taxonomy.apis.util import RateLimiter
from taxonomy.db import helpers
from taxonomy.db.constants import ArticleType, DateSource
from taxonomy.db.helpers import clean_string, trimdoi
from taxonomy.db.models.citation_group import CitationGroup
from taxonomy.db.models.person import VirtualPerson
from taxonomy.db.url_cache import CacheDomain, cached, dirty_cache

from .article import Article, ArticleTag
from .lint import infer_publication_date_from_tags

RawData = dict[str, Any]
_options = config.get_options()


@lru_cache
def get_doi_json(doi: str) -> dict[str, Any] | None:
    try:
        return json.loads(get_doi_json_cached(doi))
    except Exception:
        traceback.print_exc()
        print(f"Could not resolve DOI {doi}")
        return None


def clear_doi_cache(doi: str) -> None:
    get_doi_json.cache_clear()
    dirty_cache(CacheDomain.doi, doi)


@cached(CacheDomain.doi)
def get_doi_json_cached(doi: str) -> str:
    # "Good manners" section in https://api.crossref.org/swagger-ui/index.html
    url = f"https://api.crossref.org/works/{urllib.parse.quote(doi)}?mailto=jelle.zijlstra@gmail.com"
    response = httpx.get(url)
    if response.status_code == 404:
        # Cache "null" for missing data
        return json.dumps(None)
    response.raise_for_status()
    return response.text


@cached(CacheDomain.crossref_openurl)
def _get_doi_from_crossref_inner(params: str) -> str:
    query_dict = json.loads(params)
    url = "https://www.crossref.org/openurl"
    response = httpx.get(url, params=query_dict)
    response.raise_for_status()
    return response.text


@cached(CacheDomain.crossref_search_by_journal)
def get_crossref_search_by_journal(data_str: str) -> str:
    data = json.loads(data_str)
    issn = data["issn"]
    params = data["params"]
    url = f"https://api.crossref.org/journals/{issn}/works"
    response = httpx.get(url, params=params)
    if response.status_code == 404:
        return "{}"
    response.raise_for_status()
    return response.text


@cached(CacheDomain.doi_resolution)
def get_doi_resolution(doi: str) -> str:
    url = f"https://doi.org/api/handles/{doi}"
    response = httpx.get(url)
    response.raise_for_status()
    text = response.text
    data = json.loads(text)
    if data["responseCode"] == 2:  # error
        raise ValueError(f"Error resolvoing {doi}: {data}")
    return text


@cached(CacheDomain.is_doi_valid)
def _is_doi_valid(doi: str) -> str:
    try:
        get_doi_resolution(doi)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return "false"
        raise
    return "true"


def is_doi_valid(doi: str) -> bool:
    result = _is_doi_valid(doi)
    return result == "true"


def get_doi_from_crossref(art: Article) -> str | None:
    if art.citation_group is None or art.volume is None or art.start_page is None:
        return None
    query_dict = {
        "pid": _options.crossrefid,
        "title": art.citation_group.name,
        "volume": art.volume,
        "spage": art.start_page,
        "noredirect": "true",
    }
    data = _get_doi_from_crossref_inner(json.dumps(query_dict))
    xml = BeautifulSoup(data, features="xml")
    try:
        return xml.crossref_result.doi.text
    except AttributeError:
        return None


def get_candidate_dois_from_crossref(art: Article) -> Iterable[str]:
    if art.citation_group is None or art.volume is None:
        return
    seen_dois: set[str] = set()
    if art.start_page is not None:
        query_dict = {
            "pid": _options.crossrefid,
            "title": art.citation_group.name,
            "volume": art.volume,
            "spage": art.start_page,
            "noredirect": "true",
        }
        if doi := _try_query(query_dict):
            seen_dois.add(doi)
            yield doi

        for issn in art.citation_group.get_issns():
            query_dict = {
                "pid": _options.crossrefid,
                "issn": issn,
                "volume": art.volume,
                "spage": art.start_page,
                "noredirect": "true",
            }
            if doi := _try_query(query_dict):
                if doi in seen_dois:
                    continue
                seen_dois.add(doi)
                yield doi

    if art.title:
        title = helpers.simplify_string(
            art.title, clean_words=False, keep_whitespace=True
        )
        for issn in art.citation_group.get_issns():
            params = {
                "query.title": title,
                "filter": (
                    f"issn:{issn},from-pub-date:{art.numeric_year() - 2},until-pub-date:{art.numeric_year() + 2}"
                ),
                "rows": 5,
                "select": "DOI",
            }
            data = get_crossref_search_by_journal(
                json.dumps({"issn": issn, "params": params})
            )
            result = json.loads(data)
            if items := result.get("message", {}).get("items"):
                for item in items:
                    doi = item.get("DOI")
                    if doi and doi not in seen_dois:
                        seen_dois.add(doi)
                        yield doi


def _try_query(query_dict: dict[str, str]) -> str | None:
    data = _get_doi_from_crossref_inner(json.dumps(query_dict))
    xml = BeautifulSoup(data, features="xml")
    try:
        return xml.crossref_result.doi.text
    except AttributeError:
        return None


@lru_cache
def get_pubmed_esummary(pmid: str) -> dict[str, Any] | None:
    try:
        return json.loads(get_pubmed_esummary_cached(pmid))
    except Exception:
        traceback.print_exc()
        print(f"Could not fetch PubMed summary for PMID {pmid}")
        return None


_pubmed_rate_limiter = RateLimiter(min_interval=0.34)


@cached(CacheDomain.pubmed_esummary)
def get_pubmed_esummary_cached(pmid: str) -> str:
    _pubmed_rate_limiter.wait()
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    response = requests.get(
        url,
        params={"db": "pubmed", "id": pmid, "retmode": "json"},
        timeout=20,
        headers={"User-Agent": "taxonomy-pubmed-lint/1.0"},
    )
    response.raise_for_status()
    return response.text


def expand_pubmed_json(pmid: str) -> RawData:
    """Map PubMed ESummary JSON to our article RawData-like dict.

    Conservative mapping: only fields that are relatively stable.
    """
    raw = get_pubmed_esummary(pmid)
    if not raw:
        return {}
    result = raw.get("result") or {}
    # results include a "uids" list and entries keyed by uid
    rec = result.get(pmid)
    if not isinstance(rec, dict):
        return {}

    data: RawData = {}
    title = rec.get("title")
    if isinstance(title, str) and title.strip():
        data["title"] = clean_string(title)

    journal = rec.get("fulljournalname")
    if isinstance(journal, str) and journal.strip():
        data["journal"] = clean_string(journal)

    volume = rec.get("volume")
    if isinstance(volume, str) and volume.strip():
        data["volume"] = volume.removeprefix("0")

    issue = rec.get("issue")
    if isinstance(issue, str) and issue.strip():
        if "volume" in data:
            data["issue"] = issue.removeprefix("0")
        else:
            data["volume"] = issue.removeprefix("0")

    pages = rec.get("pages")
    if isinstance(pages, str) and pages.strip():
        m = re.fullmatch(r"^(\w+)[\-–](\w+)$", pages)
        if m:
            start_page = m.group(1)
            end_page = m.group(2)
            if len(end_page) < len(start_page):
                # turn e.g. "9967-72" into "9967-9972"
                end_page = start_page[: len(start_page) - len(end_page)] + end_page
            data["start_page"] = start_page
            data["end_page"] = end_page
        elif pages.isnumeric():
            data["start_page"] = data["end_page"] = pages
        else:
            data["start_page"] = pages

    # Year from pubdate string (e.g., "2021 Jan 15")
    pubdate = rec.get("pubdate")
    if isinstance(pubdate, str):
        m = re.search(r"(19|20)\d{2}", pubdate)
        if m:
            data["year"] = m.group(0)

    # DOI from articleids
    for idrec in rec.get("articleids") or []:
        if idrec.get("idtype") == "doi" and idrec.get("value"):
            data["doi"] = trimdoi(str(idrec["value"]))
            break

    # Always include the PMID as a tag
    data["tags"] = [ArticleTag.PMID(pmid)]
    return data


# -------------------- Europe PMC helpers (for PMCID lookup) --------------------

EUROPEPMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"


@cached(CacheDomain.europe_pmc_search)
def _europe_pmc_cached(params_json: str) -> str:
    query_dict = json.loads(params_json)
    response = requests.get(
        EUROPEPMC_SEARCH_URL,
        params=query_dict,
        timeout=20,
        headers={"User-Agent": "taxonomy-pmc-lint/1.0"},
    )
    response.raise_for_status()
    return response.text


def expand_pmc_json(pmcid: str) -> RawData:
    """Fetch citation metadata for a PMCID using Europe PMC search.

    Returns a RawData dict similar in shape to expand_pubmed_json.
    """
    pmcid = pmcid if pmcid.upper().startswith("PMC") else f"PMC{pmcid}"
    params = {"format": "json", "pageSize": "25", "query": f"PMCID:{pmcid}"}
    try:
        text = _europe_pmc_cached(json.dumps(params, sort_keys=True))
        data = json.loads(text)
    except Exception:
        traceback.print_exc()
        return {}
    results = (data.get("resultList") or {}).get("result") or []
    rec: dict[str, Any] | None = None
    for r in results:
        if str(r.get("pmcid", "")).upper() == pmcid.upper():
            rec = r
            break
    if rec is None:
        return {}

    out: RawData = {}
    title = rec.get("title")
    if isinstance(title, str) and title.strip():
        out["title"] = clean_string(title)
    journal = rec.get("journalTitle") or rec.get("journal")
    if isinstance(journal, str) and journal.strip():
        out["journal"] = clean_string(journal)
    volume = rec.get("volume")
    if isinstance(volume, str) and volume.strip():
        out["volume"] = volume.removeprefix("0")
    issue = rec.get("issue")
    if isinstance(issue, str) and issue.strip():
        out["issue"] = issue.removeprefix("0")
    pages = rec.get("pageInfo") or rec.get("pages")
    if isinstance(pages, str) and pages.strip():
        m = re.fullmatch(r"^(\w+)[\-–](\w+)$", pages)
        if m:
            start_page = m.group(1)
            end_page = m.group(2)
            if len(end_page) < len(start_page):
                end_page = start_page[: len(start_page) - len(end_page)] + end_page
            out["start_page"] = start_page
            out["end_page"] = end_page
        elif pages.isnumeric():
            out["start_page"] = out["end_page"] = pages
        else:
            out["start_page"] = pages
    pub_year = rec.get("pubYear") or rec.get("year")
    if isinstance(pub_year, str) and pub_year.isnumeric():
        out["year"] = pub_year
    doi = rec.get("doi")
    if isinstance(doi, str) and doi:
        out["doi"] = trimdoi(doi)
    pmid = rec.get("pmid")
    if isinstance(pmid, str) and pmid.isnumeric():
        out.setdefault("tags", [])
        out["tags"].append(ArticleTag.PMID(pmid))
    return out


def get_europe_pmc_record(pmcid: str) -> dict[str, Any] | None:
    """Return the raw Europe PMC result dict for an exact PMCID match, if any."""
    pmcid = pmcid if pmcid.upper().startswith("PMC") else f"PMC{pmcid}"
    params = {"format": "json", "pageSize": "25", "query": f"PMCID:{pmcid}"}
    try:
        text = _europe_pmc_cached(json.dumps(params, sort_keys=True))
        data = json.loads(text)
    except Exception:
        traceback.print_exc()
        return None
    results = (data.get("resultList") or {}).get("result") or []
    for r in results:
        if str(r.get("pmcid", "")).upper() == pmcid.upper():
            return r
    return None


# -------------------- PMCID inference helpers --------------------


@cached(CacheDomain.ncbi_idconv)
def _idconv_cached(params_json: str) -> str:
    params = json.loads(params_json)
    url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
    response = requests.get(
        url, params=params, timeout=20, headers={"User-Agent": "taxonomy-idconv/1.0"}
    )
    response.raise_for_status()
    return response.text


def _normalize_pmcid_text(pmc: str | None) -> str | None:
    if not pmc:
        return None
    pmc = pmc.strip()
    if not pmc:
        return None
    return pmc


def get_pmcid_from_idconv(identifier: str) -> str | None:
    """Return PMCID (numeric text) from NCBI idconv given PMID or DOI or PMCID."""
    params = {"format": "json", "ids": identifier}
    try:
        text = _idconv_cached(json.dumps(params, sort_keys=True))
        data = json.loads(text)
        for rec in data.get("records", []):
            pmc = _normalize_pmcid_text(rec.get("pmcid"))
            if pmc:
                return pmc
    except Exception:
        traceback.print_exc()
    return None


def get_pmcid_from_doi_via_europe_pmc(doi: str) -> str | None:
    params = {"format": "json", "pageSize": "25", "query": f"DOI:{doi}"}
    try:
        text = _europe_pmc_cached(json.dumps(params, sort_keys=True))
        data = json.loads(text)
    except Exception:
        traceback.print_exc()
        return None
    results = (data.get("resultList") or {}).get("result") or []
    for rec in results:
        if str(rec.get("doi", "")).lower() == doi.lower():
            pmc = _normalize_pmcid_text(rec.get("pmcid"))
            if pmc:
                return pmc
    for rec in results:
        pmc = _normalize_pmcid_text(rec.get("pmcid"))
        if pmc:
            return pmc
    return None


def get_pmcid_from_metadata(
    *, title: str | None, year: str | None, journal: str | None
) -> str | None:
    if not title:
        return None
    q = f'TITLE:"{title}"'
    if year and year.isdigit():
        q += f" AND PUB_YEAR:{year}"
    if journal:
        q += f' AND JOURNAL:"{journal}"'
    params = {"format": "json", "pageSize": "25", "query": q}
    try:
        text = _europe_pmc_cached(json.dumps(params, sort_keys=True))
        data = json.loads(text)
    except Exception:
        traceback.print_exc()
        return None
    results = (data.get("resultList") or {}).get("result") or []
    norm = lambda s: " ".join(s.lower().split())
    tgt = norm(title)
    for rec in results:
        pmc = _normalize_pmcid_text(rec.get("pmcid"))
        if not pmc:
            continue
        if rec.get("title") and norm(str(rec.get("title"))) == tgt:
            return pmc
    for rec in results:
        pmc = _normalize_pmcid_text(rec.get("pmcid"))
        if pmc:
            return pmc
    return None


# values from http://www.crossref.org/schema/queryResultSchema/crossref_query_output2.0.xsd
doi_type_to_article_type = {
    "journal_title": ArticleType.JOURNAL,
    "journal_issue": ArticleType.JOURNAL,
    "journal_volume": ArticleType.JOURNAL,
    "journal_article": ArticleType.JOURNAL,
    "conference_paper": ArticleType.CHAPTER,
    "component": ArticleType.CHAPTER,
    "book_chapter": ArticleType.CHAPTER,
    "book_content": ArticleType.CHAPTER,
    "dissertation": ArticleType.THESIS,
    "conference_title": ArticleType.BOOK,
    "conference_series": ArticleType.BOOK,
    "book_title": ArticleType.BOOK,
    "book_series": ArticleType.BOOK,
    "report-paper_title": ArticleType.MISCELLANEOUS,
    "report-paper_series": ArticleType.MISCELLANEOUS,
    "report-paper_content": ArticleType.MISCELLANEOUS,
    "standard_title": ArticleType.MISCELLANEOUS,
    "standard_series": ArticleType.MISCELLANEOUS,
    "standard_content": ArticleType.MISCELLANEOUS,
    "book": ArticleType.BOOK,
    "monograph": ArticleType.BOOK,
    "edited-book": ArticleType.BOOK,
}
for _key, _value in list(doi_type_to_article_type.items()):
    # usage seems to be inconsistent, let's just use both
    doi_type_to_article_type[_key.replace("-", "_")] = _value
    doi_type_to_article_type[_key.replace("_", "-")] = _value


FIELD_TO_DATE_SOURCE = {
    "published": DateSource.doi_published,
    "published-print": DateSource.doi_published_print,
    "published-online": DateSource.doi_published_online,
    "published-other": DateSource.doi_published_other,
}


def expand_doi_json(doi: str) -> RawData:
    result = get_doi_json(doi)
    if result is None:
        return {}
    work = result["message"]
    data: RawData = {"doi": doi}
    typ = ArticleType.ERROR
    if work["type"] in doi_type_to_article_type:
        data["type"] = typ = doi_type_to_article_type[work["type"]]

    if titles := work.get("title"):
        title = titles[0]
        if title.isupper():
            # all uppercase title; let's clean it up a bit
            # this won't give correct capitalization, but it'll be better than all-uppercase
            title = title[0] + title[1:].lower()
        data["title"] = clean_string(title)

    for key in ("author", "editor"):
        if author_raw := work.get(key):
            authors = []
            for author in author_raw:
                # doi:10.24272/j.issn.2095-8137.2020.132 has some stray authors that look like
                # they should be affiliations.
                if "family" not in author:
                    continue
                family_name = clean_string(author["family"])
                if family_name.isupper():
                    family_name = family_name.title()
                initials = given_names = None
                if given := author.get("given"):
                    given = clean_string(given.title())
                    if given:
                        if given[-1].isupper():
                            given = given + "."
                        given = re.sub(r"\b([A-Z]) ", r"\1.", given)
                        if parsing.matches_grammar(
                            given.replace(" ", ""), parsing.initials_pattern
                        ):
                            initials = given.replace(" ", "")
                        else:
                            given_names = re.sub(r"\. ([A-Z]\.)", r".\1", given)
                authors.append(
                    VirtualPerson(
                        family_name=family_name,
                        initials=initials,
                        given_names=given_names,
                    )
                )
            if authors:
                data["author_tags"] = authors
            break

    if volume := work.get("volume"):
        data["volume"] = volume.removeprefix("0")
    if issue := work.get("issue"):
        if "volume" in data:
            data["issue"] = issue.removeprefix("0")
        else:
            data["volume"] = issue.removeprefix("0")
    if publisher := work.get("publisher"):
        data["publisher"] = clean_string(publisher)
    if location := work.get("publisher-location"):
        try:
            cg = (
                CitationGroup.select_valid()
                .filter(CitationGroup.name == location)
                .get()
            )
            data["citation_group"] = cg
        except clirm.DoesNotExist:
            pass

    if page := work.get("page"):
        if typ in (ArticleType.JOURNAL, ArticleType.CHAPTER):
            if match := re.fullmatch(r"^(\d+)-(\d+)$", page):
                data["start_page"] = match.group(1)
                data["end_page"] = match.group(2)
            elif page.isnumeric():
                data["start_page"] = data["end_page"] = page
            else:
                data["start_page"] = page
        else:
            data["pages"] = page
    if article_number := work.get("article-number"):
        data["article_number"] = article_number

    if isbns := work.get("ISBN"):
        isbn = isbns[0]
    else:
        isbn = None

    if typ is ArticleType.BOOK:
        data["isbn"] = isbn

    if container_title := get_container_title(work):
        if typ is ArticleType.JOURNAL:
            data["journal"] = container_title
        elif typ is ArticleType.CHAPTER:
            data["parent_info"] = {"title": container_title, "isbn": isbn}

    data["tags"] = []
    for field, date_source in FIELD_TO_DATE_SOURCE.items():
        if field in work:
            parts = work[field]["date-parts"][0]
            pieces = [str(parts[0])]
            if len(parts) > 1:
                pieces.append(f"{parts[1]:02}")
            if len(parts) > 2:
                pieces.append(f"{parts[2]:02}")
            data["tags"].append(
                ArticleTag.PublicationDate(source=date_source, date="-".join(pieces))
            )
    year, _ = infer_publication_date_from_tags(data["tags"])
    if year:
        data["year"] = year
    return data


def get_container_title(work: dict[str, Any]) -> str | None:
    if container_title := work.get("container-title"):
        title = container_title[0]
        return title.replace("&amp;", "&").replace("’", "'")
    return None


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


@dataclass
class PMIDCandidate:
    id: int
    name: str
    doi: str | None
    pmcid: str | None
    title: str | None
    journal: str | None
    year: str | None


def get_pmcid_from_tags(art: Article) -> str | None:
    value = art.get_identifier(ArticleTag.PMC)
    if not value:
        return None
    if value.startswith("PMC"):
        return value
    # Europe/NLM sometimes store numeric only; normalize with PMC prefix
    return f"PMC{value}"


def to_pmid_candidate(art: Article) -> PMIDCandidate:
    return PMIDCandidate(
        id=art.id,
        name=art.name,
        doi=art.doi,
        pmcid=get_pmcid_from_tags(art),
        title=art.title,
        journal=art.citation_group.name if art.citation_group else None,
        year=art.year,
    )


def infer_pmid_for_article(art: Article, *, allow_metadata: bool = False) -> str | None:
    cand = to_pmid_candidate(art)

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
