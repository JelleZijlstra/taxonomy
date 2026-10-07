import json
from unittest.mock import Mock

import httpx
import pytest

from taxonomy.apis import wikidata
from taxonomy.db.url_cache import CacheDomain


def _payload() -> dict[str, object]:
    return {
        "entities": {
            "Q42": {
                "id": "Q42",
                "type": "item",
                "lastrevid": 123,
                "claims": {},
                "labels": {"en": {"value": "Douglas Adams"}},
                "sitelinks": {"enwiki": {"title": "Douglas Adams"}},
            }
        }
    }


@pytest.fixture
def api_get(monkeypatch: pytest.MonkeyPatch) -> Mock:
    monkeypatch.setattr(wikidata, "get_expiring_cached_value", Mock(return_value=None))
    monkeypatch.setattr(wikidata, "set_expiring_cached_value", Mock())
    monkeypatch.setattr(wikidata.rate_limiter, "wait", Mock())
    response = Mock()
    response.json.return_value = _payload()
    get = Mock(return_value=response)
    monkeypatch.setattr(httpx, "get", get)
    get.cache_writer = Mock()
    monkeypatch.setattr(wikidata, "set_expiring_cached_value", get.cache_writer)
    return get


def test_full_snapshot_cache_retains_original_retrieval_time(
    api_get: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = wikidata.get_item("Q42")
    saved = api_get.cache_writer.call_args.args[2]
    monkeypatch.setattr(wikidata, "get_expiring_cached_value", Mock(return_value=saved))
    second = wikidata.get_item("Q42")
    assert first == second
    assert second.snapshot.response == _payload()
    api_get.assert_called_once()
    assert api_get.cache_writer.call_args.kwargs["ttl"] == wikidata.CACHE_TTL


def test_redirect_keeps_requested_and_resolved_qids(api_get: Mock) -> None:
    item = wikidata.get_item("Q123")
    assert item.requested_qid == "Q123"
    assert item.entity is not None and item.entity["id"] == "Q42"
    assert api_get.call_args.kwargs["params"]["redirects"] == "yes"


def test_missing_item_is_negatively_cached(api_get: Mock) -> None:
    api_get.return_value.json.return_value = {
        "entities": {"Q42": {"id": "Q42", "missing": True}}
    }
    assert wikidata.get_item("Q42").entity is None
    assert api_get.cache_writer.call_args.kwargs["ttl"] == wikidata.MISSING_TTL


@pytest.mark.parametrize(
    "payload",
    [
        {"error": {"code": "maxlag"}},
        {},
        {"entities": {}},
        {"entities": {"Q42": {"id": "Q42"}}},
    ],
)
def test_api_failure_is_not_cached(api_get: Mock, payload: object) -> None:
    api_get.return_value.json.return_value = payload
    with pytest.raises(wikidata.WikidataAPIError):
        wikidata.get_item("Q42")
    api_get.cache_writer.assert_not_called()


def test_http_failure_is_not_cached(api_get: Mock) -> None:
    api_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
        "limited", request=Mock(), response=Mock()
    )
    with pytest.raises(httpx.HTTPStatusError):
        wikidata.get_item("Q42")
    api_get.cache_writer.assert_not_called()


@pytest.mark.parametrize(
    "qid", ["q42", "Q0", "Q01", "Q-1", "Q４２", "https://www.wikidata.org/wiki/Q42"]
)
def test_invalid_ids_never_fetch(api_get: Mock, qid: str) -> None:
    with pytest.raises(ValueError, match="Invalid Wikidata ID"):
        wikidata.get_item(qid)
    api_get.assert_not_called()


def test_wikipedia_redirect_and_unicode_title(api_get: Mock) -> None:
    payload = {
        "query": {
            "redirects": [{"from": "René Smith", "to": "René Smith (naturalist)"}],
            "pages": [
                {
                    "pageid": 5,
                    "ns": 0,
                    "title": "René Smith (naturalist)",
                    "lastrevid": 12,
                    "pageprops": {"wikibase_item": "Q42"},
                }
            ],
        }
    }
    api_get.return_value.json.return_value = payload
    link = wikidata.resolve_wikipedia("https://fr.wikipedia.org/wiki/Ren%C3%A9_Smith")
    assert link.qid == "Q42"
    assert not link.needs_review
    assert link.snapshot.response == payload
    assert api_get.call_args.kwargs["params"]["titles"] == "René Smith"
    assert api_get.call_args.args[0] == "https://fr.wikipedia.org/w/api.php"


@pytest.mark.parametrize(
    ("page", "redirects", "url_suffix"),
    [
        ({"pageprops": {"wikibase_item": "Q42", "disambiguation": ""}}, [], ""),
        ({"pageprops": {"wikibase_item": "Q42"}}, [{"tofragment": "Biography"}], ""),
        ({"pageprops": {"wikibase_item": "Q42"}}, [], "#Biography"),
        ({"pageprops": {"wikibase_item": "Q42"}}, [], "?oldid=123"),
        ({"ns": 1, "pageprops": {"wikibase_item": "Q42"}}, [], ""),
    ],
)
def test_wikipedia_ambiguous_pages_need_review(
    api_get: Mock,
    page: dict[str, object],
    redirects: list[dict[str, str]],
    url_suffix: str,
) -> None:
    api_get.return_value.json.return_value = {
        "query": {
            "pages": [{"ns": 0, "title": "Smith", **page}],
            "redirects": redirects,
        }
    }
    assert wikidata.resolve_wikipedia(
        f"https://en.wikipedia.org/wiki/Smith{url_suffix}"
    ).needs_review


@pytest.mark.parametrize("page", [{"missing": True}, {"pageprops": {}}])
def test_unlinked_wikipedia_page_is_not_a_match(
    api_get: Mock, page: dict[str, object]
) -> None:
    api_get.return_value.json.return_value = {
        "query": {"pages": [{"ns": 0, "title": "Smith", **page}]}
    }
    assert wikidata.resolve_wikipedia("https://en.wikipedia.org/wiki/Smith").qid is None
    assert api_get.cache_writer.call_args.kwargs["ttl"] == wikidata.MISSING_TTL


@pytest.mark.parametrize(
    "url",
    [
        "https://example.org/wiki/Smith",
        "https://en.wikipedia.org.evil.org/wiki/Smith",
        "https://user@en.wikipedia.org/wiki/Smith",
        "https://en.wikipedia.org:443/wiki/Smith",
        "https://en.wikipedia.org/wiki/Smith%7CJones",
    ],
)
def test_unsupported_url_never_fetches(api_get: Mock, url: str) -> None:
    with pytest.raises(ValueError, match="Wikipedia"):
        wikidata.resolve_wikipedia(url)
    api_get.assert_not_called()


def test_cached_page_fragment_still_needs_review(
    api_get: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = {
        "query": {
            "pages": [
                {"title": "Smith", "ns": 0, "pageprops": {"wikibase_item": "Q42"}}
            ]
        }
    }
    monkeypatch.setattr(
        wikidata,
        "get_expiring_cached_value",
        Mock(
            return_value=json.dumps(
                {"retrieved_at": "2026-01-01T00:00:00+00:00", "response": payload}
            )
        ),
    )
    assert wikidata.resolve_wikipedia(
        "https://en.wikipedia.org/wiki/Smith#Other_person"
    ).needs_review
    api_get.assert_not_called()


def test_malformed_cache_is_refetched(
    api_get: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        wikidata, "get_expiring_cached_value", Mock(return_value='{"response": {}}')
    )
    dirty = Mock()
    monkeypatch.setattr(wikidata, "dirty_cache", dirty)
    assert wikidata.get_item("Q42").entity is not None
    api_get.assert_called_once()
    dirty.assert_called_once_with(CacheDomain.wikidata_item, "Q42")
