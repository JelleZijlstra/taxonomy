from __future__ import annotations

import datetime
import json
from unittest.mock import Mock

import httpx
import pytest

from taxonomy import config
from taxonomy.apis import orcid
from taxonomy.db.url_cache import CacheDomain


@pytest.fixture(autouse=True)
def _clear_access_token() -> None:
    orcid.get_access_token.cache_clear()


def test_get_access_token_uses_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config,
        "get_options",
        Mock(
            return_value=config.Options(
                orcid_client_id="APP-EXAMPLE",
                orcid_client_secret="example-secret",  # noqa: S106
            )
        ),
    )
    monkeypatch.setattr(orcid.rate_limiter, "wait", Mock())
    response = Mock()
    response.json.return_value = {"access_token": "example-token"}
    post = Mock(return_value=response)
    monkeypatch.setattr(httpx, "post", post)

    assert orcid.get_access_token() == "example-token"

    post.assert_called_once_with(
        orcid.ORCID_TOKEN_URL,
        data={
            "client_id": "APP-EXAMPLE",
            "client_secret": "example-secret",
            "grant_type": "client_credentials",
            "scope": "/read-public",
        },
        headers={"Accept": "application/json"},
        timeout=orcid.REQUEST_TIMEOUT,
    )
    response.raise_for_status.assert_called_once_with()


def test_get_access_token_requires_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "get_options", Mock(return_value=config.Options()))

    with pytest.raises(orcid.OrcidConfigurationError):
        orcid.get_access_token()


def test_get_json_text_authenticates_and_rate_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    def get_access_token() -> str:
        events.append("token")
        return "t"

    monkeypatch.setattr(orcid, "get_access_token", Mock(side_effect=get_access_token))
    monkeypatch.setattr(
        orcid.rate_limiter, "wait", Mock(side_effect=lambda: events.append("wait"))
    )
    response = Mock(status_code=200, text='{"expanded-result": []}')
    response.json.return_value = {"expanded-result": []}
    get = Mock(return_value=response)
    monkeypatch.setattr(httpx, "get", get)

    assert orcid._get_json_text(
        "expanded-search/", params={"q": 'doi-self:"10.1/example"'}
    ) == ('{"expanded-result": []}', True)
    assert events == ["token", "wait"]
    get.assert_called_once_with(
        f"{orcid.ORCID_BASE_URL}/expanded-search/",
        params={"q": 'doi-self:"10.1/example"'},
        headers={
            "Accept": orcid.ORCID_JSON_MEDIA_TYPE,
            "Authorization": "Bearer t",
            "User-Agent": "taxonomy-orcid-client/1.0",
        },
        timeout=orcid.REQUEST_TIMEOUT,
        follow_redirects=True,
    )


def test_get_json_text_treats_deactivated_record_as_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orcid, "get_access_token", Mock(return_value="t"))
    monkeypatch.setattr(orcid.rate_limiter, "wait", Mock())
    response = Mock(status_code=409)
    monkeypatch.setattr(httpx, "get", Mock(return_value=response))

    assert orcid._get_json_text("0000-0001-9355-2389/record") == ("null", False)
    response.raise_for_status.assert_not_called()


def test_get_json_text_does_not_hide_other_conflicts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orcid, "get_access_token", Mock(return_value="t"))
    monkeypatch.setattr(orcid.rate_limiter, "wait", Mock())
    response = Mock(status_code=409)
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "conflict", request=Mock(), response=Mock()
    )
    monkeypatch.setattr(httpx, "get", Mock(return_value=response))

    with pytest.raises(httpx.HTTPStatusError):
        orcid._get_json_text("expanded-search/")


def test_search_orcids_by_doi_reads_expiring_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cached = json.dumps(
        {
            "expanded-result": [
                {
                    "orcid-id": "0000-0002-1694-233X",
                    "given-names": "Josiah",
                    "family-names": "Carberry",
                    "credit-name": "J. Carberry",
                    "other-name": ["J. S. Carberry"],
                    "institution-name": ["Brown University"],
                }
            ]
        }
    )
    get_cached = Mock(return_value=cached)
    monkeypatch.setattr(orcid, "get_expiring_cached_value", get_cached)
    request = Mock()
    monkeypatch.setattr(orcid, "_get_json_text", request)

    assert orcid.search_orcids_by_doi("https://doi.org/10.1234/EXAMPLE") == [
        orcid.OrcidSearchResult(
            orcid="0000-0002-1694-233X",
            given_names="Josiah",
            family_names="Carberry",
            credit_name="J. Carberry",
            other_names=("J. S. Carberry",),
            institution_names=("Brown University",),
        )
    ]
    get_cached.assert_called_once_with(CacheDomain.orcid_doi_search, "10.1234/example")
    request.assert_not_called()


@pytest.mark.parametrize(
    ("response", "base_ttl"),
    [
        ({"expanded-result": []}, orcid.EMPTY_DOI_SEARCH_CACHE_TTL),
        ({"num-found": 0, "expanded-result": None}, orcid.EMPTY_DOI_SEARCH_CACHE_TTL),
        (
            {"expanded-result": [{"orcid-id": "0000-0002-1694-233X"}]},
            orcid.DOI_SEARCH_CACHE_TTL,
        ),
    ],
)
def test_doi_search_uses_result_specific_jittered_ttl(
    monkeypatch: pytest.MonkeyPatch,
    response: dict[str, object],
    base_ttl: datetime.timedelta,
) -> None:
    key = "10.1234/example"
    content = json.dumps(response)
    monkeypatch.setattr(orcid, "get_expiring_cached_value", Mock(return_value=None))
    monkeypatch.setattr(orcid, "_get_json_text", Mock(return_value=(content, True)))
    set_cached = Mock()
    monkeypatch.setattr(orcid, "set_expiring_cached_value", set_cached)

    orcid.search_orcids_by_doi(key)

    set_cached.assert_called_once_with(
        CacheDomain.orcid_doi_search,
        key,
        content,
        ttl=orcid._jittered_cache_ttl(base_ttl, CacheDomain.orcid_doi_search, key),
    )


def test_get_orcid_record_negatively_caches_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orcid, "get_expiring_cached_value", Mock(return_value=None))
    monkeypatch.setattr(orcid, "_get_json_text", Mock(return_value=("null", False)))
    set_cached = Mock()
    monkeypatch.setattr(orcid, "set_expiring_cached_value", set_cached)

    assert orcid.get_orcid_record("0000-0002-1694-233X") is None

    set_cached.assert_called_once_with(
        CacheDomain.orcid_record,
        "0000-0002-1694-233X",
        "null",
        ttl=orcid._jittered_cache_ttl(
            orcid.NOT_FOUND_CACHE_TTL, CacheDomain.orcid_record, "0000-0002-1694-233X"
        ),
    )


def test_get_orcid_record_uses_profile_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    key = "0000-0002-1694-233X"
    content = '{"orcid-identifier":{"path":"0000-0002-1694-233X"}}'
    monkeypatch.setattr(orcid, "get_expiring_cached_value", Mock(return_value=None))
    monkeypatch.setattr(orcid, "_get_json_text", Mock(return_value=(content, True)))
    set_cached = Mock()
    monkeypatch.setattr(orcid, "set_expiring_cached_value", set_cached)

    assert orcid.get_orcid_record(key) == {
        "orcid-identifier": {"path": "0000-0002-1694-233X"}
    }
    set_cached.assert_called_once_with(
        CacheDomain.orcid_record,
        key,
        content,
        ttl=orcid._jittered_cache_ttl(
            orcid.PROFILE_CACHE_TTL, CacheDomain.orcid_record, key
        ),
    )


def test_cache_ttl_jitter_is_stable_and_bounded() -> None:
    base_ttl = datetime.timedelta(days=100)
    first = orcid._jittered_cache_ttl(
        base_ttl, CacheDomain.orcid_record, "0000-0002-1694-233X"
    )
    repeated = orcid._jittered_cache_ttl(
        base_ttl, CacheDomain.orcid_record, "0000-0002-1694-233X"
    )
    other = orcid._jittered_cache_ttl(
        base_ttl, CacheDomain.orcid_record, "0000-0003-1577-6568"
    )

    assert first == repeated
    assert base_ttl * 0.8 <= first <= base_ttl * 1.2
    assert other != first


def test_cache_ttl_policy() -> None:
    assert datetime.timedelta(days=730) == orcid.DOI_SEARCH_CACHE_TTL
    assert datetime.timedelta(days=180) == orcid.EMPTY_DOI_SEARCH_CACHE_TTL
    assert datetime.timedelta(days=365) == orcid.PROFILE_CACHE_TTL
    assert orcid.CACHE_TTL_JITTER == 0.2


def test_parse_orcid_record_reads_public_names_and_self_dois() -> None:
    record = {
        "orcid-identifier": {"path": "0000-0002-1694-233X"},
        "person": {
            "name": {
                "given-names": {"value": "Josiah"},
                "family-name": {"value": "Carberry"},
                "credit-name": {"value": "J. S. Carberry"},
            },
            "other-names": {
                "other-name": [{"content": "J. Carberry"}, {"content": "J. Carberry"}]
            },
        },
        "activities-summary": {
            "works": {
                "group": [
                    {
                        "work-summary": [
                            {
                                "title": {
                                    "title": {"value": "A useful paper"},
                                    "translated-title": {
                                        "value": "Un article utile",
                                        "language-code": "fr",
                                    },
                                },
                                "journal-title": {"value": "Journal of Examples"},
                                "publication-date": {"year": {"value": "2024"}},
                                "source": {"source-name": {"value": "Crossref"}},
                                "external-ids": {
                                    "external-id": [
                                        {
                                            "external-id-type": "doi",
                                            "external-id-value": (
                                                "https://doi.org/10.1/EXAMPLE"
                                            ),
                                            "external-id-relationship": "self",
                                        },
                                        {
                                            "external-id-type": "doi",
                                            "external-id-value": "10.2/container",
                                            "external-id-relationship": "part-of",
                                        },
                                    ]
                                },
                            }
                        ]
                    }
                ]
            }
        },
    }

    assert orcid.parse_orcid_record(record) == orcid.OrcidProfile(
        orcid="0000-0002-1694-233X",
        given_names="Josiah",
        family_names="Carberry",
        credit_name="J. S. Carberry",
        other_names=("J. Carberry",),
        works=(
            orcid.OrcidWork(
                doi="10.1/example",
                title="A useful paper",
                journal_title="Journal of Examples",
                publication_year=2024,
                source_name="Crossref",
                translated_title="Un article utile",
            ),
        ),
    )


def test_get_orcid_profile_returns_none_for_missing_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orcid, "get_orcid_record", Mock(return_value=None))

    assert orcid.get_orcid_profile("0000-0002-1694-233X") is None


def test_public_api_interval_is_conservative() -> None:
    assert orcid.MIN_REQUEST_INTERVAL >= 1.0
