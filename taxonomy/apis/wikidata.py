"""Read Wikidata entities and resolve Wikipedia links without editing either wiki.

Responses retain all statements, references, ranks and qualifiers. The separate
URL cache stores the original retrieval time, not the time of the latest cache hit.
API documentation: https://www.mediawiki.org/wiki/Wikibase/API and
https://www.mediawiki.org/wiki/API:Pageprops .
"""

from __future__ import annotations

import datetime
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx

from taxonomy.db.url_cache import (
    CacheDomain,
    dirty_cache,
    get_expiring_cached_value,
    set_expiring_cached_value,
)

from .util import RateLimiter

API_URL = "https://www.wikidata.org/w/api.php"
CACHE_TTL = datetime.timedelta(days=30)
MISSING_TTL = datetime.timedelta(days=1)
rate_limiter = RateLimiter(min_interval=1.0)


class WikidataAPIError(ValueError):
    """An API response cannot safely be interpreted as identity evidence."""


def is_valid_qid(value: str) -> bool:
    return re.fullmatch(r"Q[1-9][0-9]*", value) is not None


@dataclass(frozen=True)
class Snapshot:
    retrieved_at: str
    response: dict[str, Any]


def _get_snapshot(
    domain: CacheDomain,
    key: str,
    url: str,
    params: dict[str, str],
    validate: Callable[[dict[str, Any]], bool],
) -> Snapshot:
    cached = get_expiring_cached_value(domain, key)
    if cached is not None:
        try:
            envelope = json.loads(cached)
            if datetime.datetime.fromisoformat(envelope["retrieved_at"]).tzinfo is None:
                raise ValueError("Cached retrieval time has no timezone")
            validate(envelope["response"])
        except ValueError, KeyError, TypeError, AttributeError:
            dirty_cache(domain, key)
        else:
            return Snapshot(envelope["retrieved_at"], envelope["response"])
    rate_limiter.wait()
    response = httpx.get(
        url,
        params={"format": "json", "formatversion": "2", "maxlag": "5", **params},
        headers={
            "User-Agent": (
                "taxonomy-wikidata/1.0 (https://github.com/JelleZijlstra/taxonomy)"
            ),
            "Accept": "application/json",
        },
        timeout=30.0,
        follow_redirects=True,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or "error" in payload or "errors" in payload:
        raise WikidataAPIError(f"Wikimedia API error: {payload!r}")
    found = validate(payload)
    retrieved_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
    set_expiring_cached_value(
        domain,
        key,
        json.dumps({"retrieved_at": retrieved_at, "response": payload}),
        ttl=CACHE_TTL if found else MISSING_TTL,
    )
    return Snapshot(retrieved_at, payload)


def _entity(payload: dict[str, Any]) -> dict[str, Any] | None:
    entities = payload.get("entities")
    if not isinstance(entities, dict) or len(entities) != 1:
        raise WikidataAPIError("Expected exactly one entity")
    entity = next(iter(entities.values()))
    if not isinstance(entity, dict):
        raise WikidataAPIError("Malformed entity")
    if "missing" in entity:
        return None
    qid = entity.get("id")
    if (
        not isinstance(qid, str)
        or not is_valid_qid(qid)
        or entity.get("type") != "item"
        or not isinstance(entity.get("claims"), dict)
        or not isinstance(entity.get("lastrevid"), int)
    ):
        raise WikidataAPIError("Incomplete item response")
    for claims in entity["claims"].values():
        if not isinstance(claims, list) or any(
            not isinstance(claim, dict) or not isinstance(claim.get("mainsnak"), dict)
            for claim in claims
        ):
            raise WikidataAPIError("Malformed item statements")
    return entity


@dataclass(frozen=True)
class Item:
    requested_qid: str
    snapshot: Snapshot

    @property
    def entity(self) -> dict[str, Any] | None:
        return _entity(self.snapshot.response)


def get_item(qid: str) -> Item:
    if not is_valid_qid(qid):
        raise ValueError(f"Invalid Wikidata ID: {qid!r}")
    snapshot = _get_snapshot(
        CacheDomain.wikidata_item,
        qid,
        API_URL,
        {"action": "wbgetentities", "ids": qid, "redirects": "yes"},
        lambda payload: _entity(payload) is not None,
    )
    return Item(qid, snapshot)


def wikipedia_title(url: str) -> tuple[str, str]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not re.fullmatch(r"[a-z][a-z0-9-]*\.wikipedia\.org", parsed.netloc)
        or not parsed.path.startswith("/wiki/")
    ):
        raise ValueError(f"Unsupported Wikipedia URL: {url!r}")
    title = unquote(parsed.path.removeprefix("/wiki/")).replace("_", " ")
    if not title or any(char in title for char in "|\x1f\n\r"):
        raise ValueError(f"Invalid Wikipedia title: {title!r}")
    return parsed.netloc, title


def _page(payload: dict[str, Any]) -> dict[str, Any]:
    query = payload.get("query")
    if not isinstance(query, dict):
        raise WikidataAPIError("Missing Wikipedia query result")
    pages = query.get("pages")
    if not isinstance(pages, list) or len(pages) != 1:
        raise WikidataAPIError("Expected exactly one Wikipedia page")
    redirects = query.get("redirects", [])
    if not isinstance(redirects, list) or any(
        not isinstance(redirect, dict) for redirect in redirects
    ):
        raise WikidataAPIError("Malformed Wikipedia redirects")
    page = pages[0]
    if not isinstance(page, dict) or not isinstance(page.get("title"), str):
        raise WikidataAPIError("Malformed Wikipedia page")
    if "invalid" in page:
        raise WikidataAPIError("Invalid Wikipedia title")
    props = page.get("pageprops", {})
    if not isinstance(props, dict):
        raise WikidataAPIError("Malformed page properties")
    qid = props.get("wikibase_item")
    if qid is not None and (not isinstance(qid, str) or not is_valid_qid(qid)):
        raise WikidataAPIError("Invalid Wikipedia item ID")
    return page


@dataclass(frozen=True)
class WikipediaLink:
    url: str
    snapshot: Snapshot

    @property
    def qid(self) -> str | None:
        page = _page(self.snapshot.response)
        if "missing" in page:
            return None
        return page.get("pageprops", {}).get("wikibase_item")

    @property
    def needs_review(self) -> bool:
        page = _page(self.snapshot.response)
        parsed = urlsplit(self.url)
        return bool(
            parsed.fragment
            or parsed.query
            or page.get("ns") != 0
            or "disambiguation" in page.get("pageprops", {})
            or any(
                redirect.get("tofragment")
                for redirect in self.snapshot.response["query"].get("redirects", [])
            )
        )


def resolve_wikipedia(url: str) -> WikipediaLink:
    host, title = wikipedia_title(url)
    # Titles may be longer than the URL cache's key limit.
    key = hashlib.sha256(f"{host}/{title}".encode()).hexdigest()
    snapshot = _get_snapshot(
        CacheDomain.wikipedia_item,
        key,
        f"https://{host}/w/api.php",
        {
            "action": "query",
            "prop": "pageprops|info",
            "ppprop": "wikibase_item|disambiguation",
            "titles": title,
            "redirects": "1",
        },
        lambda payload: bool(_page(payload).get("pageprops", {}).get("wikibase_item")),
    )
    return WikipediaLink(url, snapshot)
