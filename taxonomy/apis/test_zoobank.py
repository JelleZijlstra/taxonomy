from unittest.mock import Mock, call

import pytest
import requests

from taxonomy.apis import zoobank
from taxonomy.db.url_cache import CacheDomain


def test_get_json_response_validates_before_returning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = Mock(status_code=200, text='[{"referenceuuid": "abc"}]')
    get = Mock(return_value=response)
    monkeypatch.setattr(requests, "get", get)

    assert (
        zoobank._get_json_response(
            "https://zoobank.example/reference", expected_count=1
        )
        == response.text
    )

    get.assert_called_once_with(
        "https://zoobank.example/reference", timeout=zoobank.REQUEST_TIMEOUT
    )
    response.raise_for_status.assert_called_once_with()


@pytest.mark.parametrize(
    ("status_code", "text"),
    [(404, "not found"), (200, "<html>Human verification</html>"), (200, "[]")],
)
def test_get_json_response_rejects_unusable_publication_data(
    monkeypatch: pytest.MonkeyPatch, status_code: int, text: str
) -> None:
    response = Mock(status_code=status_code, text=text)
    monkeypatch.setattr(requests, "get", Mock(return_value=response))

    with pytest.raises(zoobank.ZooBankUnavailableError):
        zoobank._get_json_response(
            "https://zoobank.example/reference", expected_count=1
        )


def test_get_json_response_propagates_http_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = Mock(status_code=500, text="server error")
    response.raise_for_status.side_effect = requests.HTTPError("server error")
    monkeypatch.setattr(requests, "get", Mock(return_value=response))

    with pytest.raises(requests.HTTPError):
        zoobank._get_json_response("https://zoobank.example/reference")


def test_old_empty_publication_cache_is_treated_as_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_data = Mock(return_value="[]")
    monkeypatch.setattr(zoobank, "_get_zoobank_publication_data", get_data)

    with pytest.raises(zoobank.ZooBankUnavailableError):
        zoobank.get_zoobank_data_for_article("FC07ACBE-03F7-414A-BB64-1BB0711766BF")


def test_get_zoobank_data_for_act_normalizes_lsid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_data = Mock(return_value="[]")
    monkeypatch.setattr(zoobank, "_get_zoobank_act_data", get_data)

    assert (
        zoobank.get_zoobank_data_for_act(
            "urn:lsid:zoobank.org:act:fc07acbe-03f7-414a-bb64-1bb0711766bf"
        )
        == []
    )

    get_data.assert_called_once_with("FC07ACBE-03F7-414A-BB64-1BB0711766BF")


def test_is_valid_lsid_accepts_hexadecimal_uuid() -> None:
    assert zoobank.is_valid_lsid("C19C1251-0557-473B-A59D-E10F8F4BAA32")


@pytest.mark.parametrize(
    "lsid",
    ["644A2B53-7AA7-4DED-8F5A-OBE0729AA1CC", "C19C1251-0557-473B-A59D-E10F8F4BAA3G"],
)
def test_is_valid_lsid_rejects_non_hexadecimal_uuid(lsid: str) -> None:
    assert not zoobank.is_valid_lsid(lsid)


def test_act_404_is_negatively_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    error = zoobank.ZooBankNotFoundError("not found")
    get_negative = Mock(return_value=None)
    set_negative = Mock()
    monkeypatch.setattr(zoobank, "get_expiring_cached_value", get_negative)
    monkeypatch.setattr(zoobank, "set_expiring_cached_value", set_negative)
    monkeypatch.setattr(zoobank, "_get_json_response", Mock(side_effect=error))
    monkeypatch.setattr(zoobank.rate_limiter, "wait", Mock())

    with pytest.raises(zoobank.ZooBankNotFoundError):
        zoobank._get_zoobank_data_with_negative_cache(
            "C19C1251-0557-473B-A59D-E10F8F4BAA32",
            negative_domain=CacheDomain.zoobank_act_negative,
            url="https://zoobank.example/act",
        )

    get_negative.assert_called_once_with(
        CacheDomain.zoobank_act_negative, "C19C1251-0557-473B-A59D-E10F8F4BAA32"
    )
    set_negative.assert_called_once_with(
        CacheDomain.zoobank_act_negative,
        "C19C1251-0557-473B-A59D-E10F8F4BAA32",
        "not found",
        ttl=zoobank.NEGATIVE_CACHE_TTL,
    )


def test_cached_act_404_skips_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        zoobank, "get_expiring_cached_value", Mock(return_value="not found")
    )
    get_json = Mock()
    monkeypatch.setattr(zoobank, "_get_json_response", get_json)

    with pytest.raises(zoobank.ZooBankNotFoundError, match="not found"):
        zoobank._get_zoobank_data_with_negative_cache(
            "C19C1251-0557-473B-A59D-E10F8F4BAA32",
            negative_domain=CacheDomain.zoobank_act_negative,
            url="https://zoobank.example/act",
        )

    get_json.assert_not_called()


def test_clear_zoobank_cache_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    dirty_cache = Mock()
    monkeypatch.setattr(zoobank, "dirty_cache", dirty_cache)

    zoobank.clear_zoobank_act_cache("Pseudovespertiliavus_parva")
    zoobank.clear_zoobank_publication_cache(
        "urn:lsid:zoobank.org:pub:fc07acbe-03f7-414a-bb64-1bb0711766bf"
    )

    assert dirty_cache.call_args_list == [
        call(CacheDomain.zoobank_act, "Pseudovespertiliavus_parva"),
        call(CacheDomain.zoobank_act_negative, "Pseudovespertiliavus_parva"),
        call(CacheDomain.zoobank_publication, "FC07ACBE-03F7-414A-BB64-1BB0711766BF"),
        call(
            CacheDomain.zoobank_publication_negative,
            "FC07ACBE-03F7-414A-BB64-1BB0711766BF",
        ),
    ]
