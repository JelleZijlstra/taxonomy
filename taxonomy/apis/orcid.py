"""Read public ORCID data through the registered Public API.

ORCID responses change over time, so they use expiring persistent cache entries.
Only actual HTTP requests pass through the module-wide rate limiter; cache hits do
not sleep.
"""

from __future__ import annotations

import datetime
import functools
import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import httpx

from taxonomy import config
from taxonomy.db import helpers
from taxonomy.db.models.person import is_valid_orcid, normalize_orcid
from taxonomy.db.url_cache import (
    CacheDomain,
    dirty_cache,
    get_expiring_cached_value,
    set_expiring_cached_value,
)

from .util import RateLimiter

ORCID_BASE_URL = "https://pub.orcid.org/v3.0"
ORCID_TOKEN_URL = "https://orcid.org/oauth/token"  # noqa: S105
ORCID_JSON_MEDIA_TYPE = "application/vnd.orcid+json"
REQUEST_TIMEOUT = 30.0

# The registered Public API currently permits 12 requests per second and 100,000
# reads per day. One request per second is intentionally much more conservative and
# keeps even a continuously running process below the daily quota.
MIN_REQUEST_INTERVAL = 1.0
rate_limiter = RateLimiter(min_interval=MIN_REQUEST_INTERVAL)

# These are base lifetimes. Each newly written entry receives stable per-key jitter
# in the range ±20%, spreading refreshes without making tests or runs nondeterministic.
DOI_SEARCH_CACHE_TTL = datetime.timedelta(days=365 * 2)
EMPTY_DOI_SEARCH_CACHE_TTL = datetime.timedelta(days=180)
PROFILE_CACHE_TTL = datetime.timedelta(days=365)
NOT_FOUND_CACHE_TTL = datetime.timedelta(days=7)
CACHE_TTL_JITTER = 0.2


class OrcidConfigurationError(RuntimeError):
    """Raised when Public API credentials are missing from taxonomy.ini."""


class OrcidAPIError(RuntimeError):
    """Raised when ORCID returns valid HTTP but unusable API data."""


@dataclass(frozen=True)
class OrcidSearchResult:
    """Public identity fields returned by ORCID expanded search."""

    orcid: str
    given_names: str | None
    family_names: str | None
    credit_name: str | None
    other_names: tuple[str, ...]
    institution_names: tuple[str, ...]


@dataclass(frozen=True)
class OrcidWork:
    """One public ORCID work summary carrying a self DOI."""

    doi: str
    title: str | None
    journal_title: str | None
    publication_year: int | None
    source_name: str | None
    translated_title: str | None = None


@dataclass(frozen=True)
class OrcidProfile:
    """The public identity and DOI-bearing works parsed from an ORCID record."""

    orcid: str
    given_names: str | None
    family_names: str | None
    credit_name: str | None
    other_names: tuple[str, ...]
    works: tuple[OrcidWork, ...]


def _required_credentials() -> tuple[str, str]:
    options = config.get_options()
    if not options.orcid_client_id or not options.orcid_client_secret:
        raise OrcidConfigurationError(
            "ORCID Public API access requires orcid_client_id and "
            "orcid_client_secret in taxonomy.ini"
        )
    return options.orcid_client_id, options.orcid_client_secret


@functools.cache
def get_access_token() -> str:
    """Obtain and retain this process's long-lived ``/read-public`` token."""
    client_id, client_secret = _required_credentials()
    rate_limiter.wait()
    response = httpx.post(
        ORCID_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": "/read-public",
        },
        headers={"Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    try:
        payload = response.json()
        token = payload["access_token"]
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise OrcidAPIError("ORCID token response has no access_token") from exc
    if not isinstance(token, str) or not token:
        raise OrcidAPIError("ORCID token response has an invalid access_token")
    return token


def _get_json_text(
    path: str, *, params: dict[str, str] | None = None
) -> tuple[str, bool]:
    """Return validated JSON text and whether an ORCID resource was found."""
    access_token = get_access_token()
    rate_limiter.wait()
    response = httpx.get(
        f"{ORCID_BASE_URL}/{path.lstrip('/')}",
        params=params,
        headers={
            "Accept": ORCID_JSON_MEDIA_TYPE,
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "taxonomy-orcid-client/1.0",
        },
        timeout=REQUEST_TIMEOUT,
        follow_redirects=True,
    )
    if response.status_code == 404 or (
        response.status_code == 409 and path.rstrip("/").endswith("/record")
    ):
        # ORCID returns 409/error 9044 for a deactivated iD. At the public
        # evidence boundary this is equivalent to a missing record: there is no
        # current identity or work list to parse. Restrict the special case to
        # record endpoints so an unrelated 409 from search still fails loudly.
        return "null", False
    response.raise_for_status()
    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise OrcidAPIError(f"ORCID returned non-JSON data for {path!r}") from exc
    if not isinstance(payload, dict):
        raise OrcidAPIError(
            f"ORCID returned unexpected JSON data for {path!r}: {payload!r}"
        )
    return response.text, True


def _get_cached_json(
    domain: CacheDomain, key: str, *, path: str, params: dict[str, str] | None = None
) -> dict[str, Any] | None:
    cached = get_expiring_cached_value(domain, key)
    if cached is None:
        cached, found = _get_json_text(path, params=params)
        set_expiring_cached_value(
            domain, key, cached, ttl=_cache_ttl(domain, key, cached, found=found)
        )
    try:
        payload = json.loads(cached)
    except json.JSONDecodeError as exc:
        # This should only be possible for a cache entry written by older code.
        dirty_cache(domain, key)
        raise OrcidAPIError(f"cached ORCID data for {key!r} is not JSON") from exc
    if payload is not None and not isinstance(payload, dict):
        dirty_cache(domain, key)
        raise OrcidAPIError(f"cached ORCID data for {key!r} is not an object")
    return payload


def _jittered_cache_ttl(
    base_ttl: datetime.timedelta, domain: CacheDomain, key: str
) -> datetime.timedelta:
    """Apply stable per-key jitter in the range ±20 percent."""
    digest = hashlib.sha256(f"{domain.value}:{key}".encode()).digest()
    unit_interval = int.from_bytes(digest[:8]) / (2**64 - 1)
    factor = 1 + CACHE_TTL_JITTER * (2 * unit_interval - 1)
    return base_ttl * factor


def _cache_ttl(
    domain: CacheDomain, key: str, content: str, *, found: bool
) -> datetime.timedelta:
    if not found:
        base_ttl = NOT_FOUND_CACHE_TTL
    elif domain is CacheDomain.orcid_record:
        base_ttl = PROFILE_CACHE_TTL
    elif domain is CacheDomain.orcid_doi_search:
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise OrcidAPIError("ORCID DOI search response is not JSON") from exc
        is_empty = isinstance(payload, dict) and _is_empty_doi_search_payload(payload)
        base_ttl = EMPTY_DOI_SEARCH_CACHE_TTL if is_empty else DOI_SEARCH_CACHE_TTL
    else:
        raise ValueError(f"unsupported ORCID cache domain {domain}")
    return _jittered_cache_ttl(base_ttl, domain, key)


def _is_empty_doi_search_payload(payload: dict[str, Any]) -> bool:
    raw_results = payload.get("expanded-result")
    return raw_results == [] or (
        raw_results is None and payload.get("num-found") in (0, "0")
    )


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str) and item)


def _nested_value(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    return _optional_string(value.get("value"))


def _parse_other_names(value: object) -> tuple[str, ...]:
    if not isinstance(value, dict):
        return ()
    raw_names = value.get("other-name")
    if not isinstance(raw_names, list):
        return ()
    names = {
        content
        for raw_name in raw_names
        if isinstance(raw_name, dict)
        and (content := _optional_string(raw_name.get("content"))) is not None
    }
    return tuple(sorted(names))


def _parse_publication_year(value: object) -> int | None:
    if not isinstance(value, dict):
        return None
    raw_year = _nested_value(value.get("year"))
    if raw_year is None or not raw_year.isdigit() or len(raw_year) != 4:
        return None
    return int(raw_year)


def _parse_source_name(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    return _nested_value(value.get("source-name"))


def _iter_work_summaries(record: dict[str, Any]) -> Iterable[dict[str, Any]]:
    activities = record.get("activities-summary")
    if not isinstance(activities, dict):
        return
    works = activities.get("works")
    if not isinstance(works, dict):
        return
    groups = works.get("group")
    if not isinstance(groups, list):
        return
    for group in groups:
        if not isinstance(group, dict):
            continue
        summaries = group.get("work-summary")
        if not isinstance(summaries, list):
            continue
        for summary in summaries:
            if isinstance(summary, dict):
                yield summary


def _self_dois(summary: dict[str, Any]) -> set[str]:
    external_ids = summary.get("external-ids")
    if not isinstance(external_ids, dict):
        return set()
    raw_ids = external_ids.get("external-id")
    if not isinstance(raw_ids, list):
        return set()
    dois: set[str] = set()
    for raw_id in raw_ids:
        if not isinstance(raw_id, dict):
            continue
        id_type = _optional_string(raw_id.get("external-id-type"))
        relationship = _optional_string(raw_id.get("external-id-relationship"))
        if id_type is None or id_type.casefold() != "doi":
            continue
        # Only ``self`` identifies this work. A ``part-of`` DOI may identify the
        # journal or book, and ``version-of`` may identify a different Article.
        if relationship is None or relationship.casefold() != "self":
            continue
        normalized = raw_id.get("external-id-normalized")
        raw_doi = _nested_value(normalized) or _optional_string(
            raw_id.get("external-id-value")
        )
        if raw_doi is not None and (doi := normalize_doi(raw_doi)):
            dois.add(doi)
    return dois


def normalize_doi(doi: str) -> str:
    """Return the bare lowercase DOI used by ORCID's search index."""
    doi = helpers.trimdoi(doi).lower()
    for prefix in (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
    ):
        doi = doi.removeprefix(prefix)
    return doi


def search_orcids_by_doi(doi: str) -> list[OrcidSearchResult]:
    """Return public ORCID profiles that list ``doi`` as their own work.

    The result is not a complete paper author list: researchers without an ORCID or
    with a private/incomplete works list will not be returned.
    """
    doi = normalize_doi(doi)
    if not doi:
        raise ValueError("DOI must not be empty")
    payload = _get_cached_json(
        CacheDomain.orcid_doi_search,
        doi,
        path="expanded-search/",
        params={"q": f'doi-self:"{doi}"', "rows": "1000"},
    )
    if payload is None:
        return []
    if _is_empty_doi_search_payload(payload):
        return []
    raw_results = payload.get("expanded-result")
    if not isinstance(raw_results, list):
        raise OrcidAPIError("ORCID expanded search result is not a list")
    results: dict[str, OrcidSearchResult] = {}
    for raw in raw_results:
        if not isinstance(raw, dict):
            continue
        raw_orcid = raw.get("orcid-id")
        if not isinstance(raw_orcid, str):
            continue
        orcid = normalize_orcid(raw_orcid)
        if not is_valid_orcid(orcid):
            raise OrcidAPIError(f"ORCID search returned invalid iD {raw_orcid!r}")
        results[orcid] = OrcidSearchResult(
            orcid=orcid,
            given_names=_optional_string(raw.get("given-names")),
            family_names=_optional_string(raw.get("family-names")),
            credit_name=_optional_string(raw.get("credit-name")),
            other_names=_string_tuple(raw.get("other-name")),
            institution_names=_string_tuple(raw.get("institution-name")),
        )
    return [results[orcid] for orcid in sorted(results)]


def get_orcid_record(orcid: str) -> dict[str, Any] | None:
    """Return all public data for an ORCID record, or ``None`` for a 404."""
    orcid = normalize_orcid(orcid)
    if not is_valid_orcid(orcid):
        raise ValueError(f"invalid ORCID {orcid!r}")
    return _get_cached_json(CacheDomain.orcid_record, orcid, path=f"{orcid}/record")


def parse_orcid_record(record: dict[str, Any]) -> OrcidProfile:
    """Parse the public identity and self-DOI work summaries in ``record``."""
    identifier = record.get("orcid-identifier")
    raw_orcid = (
        _optional_string(identifier.get("path"))
        if isinstance(identifier, dict)
        else None
    ) or _optional_string(record.get("path"))
    if raw_orcid is None:
        raise OrcidAPIError("ORCID record has no identifier path")
    parsed_orcid = normalize_orcid(raw_orcid.strip("/"))
    if not is_valid_orcid(parsed_orcid):
        raise OrcidAPIError(f"ORCID record has invalid identifier {raw_orcid!r}")

    person = record.get("person")
    if not isinstance(person, dict):
        person = {}
    name = person.get("name")
    if not isinstance(name, dict):
        name = {}

    parsed_works: list[OrcidWork] = []
    for summary in _iter_work_summaries(record):
        title = summary.get("title")
        if not isinstance(title, dict):
            title = {}
        for doi in _self_dois(summary):
            parsed_works.append(
                OrcidWork(
                    doi=doi,
                    title=_nested_value(title.get("title")),
                    journal_title=_nested_value(summary.get("journal-title")),
                    publication_year=_parse_publication_year(
                        summary.get("publication-date")
                    ),
                    source_name=_parse_source_name(summary.get("source")),
                    translated_title=_nested_value(title.get("translated-title")),
                )
            )
    parsed_works.sort(
        key=lambda work: (
            work.doi,
            work.title or "",
            work.translated_title or "",
            work.publication_year or 0,
            work.source_name or "",
        )
    )
    return OrcidProfile(
        orcid=parsed_orcid,
        given_names=_nested_value(name.get("given-names")),
        family_names=_nested_value(name.get("family-name")),
        credit_name=_nested_value(name.get("credit-name")),
        other_names=_parse_other_names(person.get("other-names")),
        works=tuple(parsed_works),
    )


def get_orcid_profile(orcid: str) -> OrcidProfile | None:
    """Return public evidence, or ``None`` for a missing/deactivated record."""
    record = get_orcid_record(orcid)
    if record is None:
        return None
    return parse_orcid_record(record)


def clear_orcid_doi_cache(doi: str) -> None:
    """Expire a DOI search immediately."""
    dirty_cache(CacheDomain.orcid_doi_search, normalize_doi(doi))


def clear_orcid_record_cache(orcid: str) -> None:
    """Expire a public-record response immediately."""
    dirty_cache(CacheDomain.orcid_record, normalize_orcid(orcid))


def clear_orcid_memory_caches() -> None:
    """Forget the in-process access token, for example after credential rotation."""
    get_access_token.cache_clear()
