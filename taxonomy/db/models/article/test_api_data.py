import json
from collections.abc import Callable
from typing import Any
from unittest.mock import Mock

import pytest

from taxonomy.db.models.article import api_data


def _search_results(*results: dict[str, str]) -> dict[str, Any]:
    return {"resultList": {"result": list(results)}}


@pytest.mark.parametrize(
    ("lookup", "expected"),
    [
        (api_data.get_pmcid_from_doi_via_europe_pmc, "PMC6394112"),
        (api_data.lookup_pmid_via_europe_pmc_by_doi, "30837778"),
    ],
)
def test_doi_lookup_does_not_fall_back_to_unmatched_result(
    monkeypatch: pytest.MonkeyPatch, lookup: Callable[[str], str | None], expected: str
) -> None:
    response = _search_results(
        {"doi": "10.1093/jmammal/gyy181", "pmcid": "PMC6394112", "pmid": "30837778"}
    )
    monkeypatch.setattr(
        api_data, "_europe_pmc_cached", Mock(return_value=json.dumps(response))
    )

    assert lookup("10.1093/jmammal/gyv133") is None
    assert lookup("10.1093/jmammal/gyy181") == expected


@pytest.mark.parametrize(
    "lookup",
    [api_data.get_pmcid_from_metadata, api_data.lookup_pmid_via_europe_pmc_by_metadata],
)
def test_metadata_lookup_rejects_nonmatching_title(
    monkeypatch: pytest.MonkeyPatch, lookup: Callable[..., str | None]
) -> None:
    response = _search_results(
        {
            "title": (
                "Erratum: Gray wolf mortality patterns in Wisconsin from 1979 to 2012."
            ),
            "pubYear": "2019",
            "pmcid": "PMC6394112",
            "pmid": "30837778",
        }
    )
    cached = Mock(return_value=json.dumps(response))
    monkeypatch.setattr(api_data, "_europe_pmc_cached", cached)

    assert (
        lookup(title="Erratum", year="2015-09-29", journal="Journal of Mammalogy")
        is None
    )
    assert "PUB_YEAR:2015" in json.loads(cached.call_args.args[0])["query"]


@pytest.mark.parametrize(
    "lookup",
    [api_data.get_pmcid_from_metadata, api_data.lookup_pmid_via_europe_pmc_by_metadata],
)
def test_metadata_lookup_requires_unique_match(
    monkeypatch: pytest.MonkeyPatch, lookup: Callable[..., str | None]
) -> None:
    response = _search_results(
        {"title": "Erratum", "pubYear": "2015", "pmcid": "PMC1", "pmid": "1"},
        {"title": "Erratum", "pubYear": "2015", "pmcid": "PMC2", "pmid": "2"},
    )
    monkeypatch.setattr(
        api_data, "_europe_pmc_cached", Mock(return_value=json.dumps(response))
    )

    assert lookup(title="Erratum", year="2015", journal="Journal of Mammalogy") is None


@pytest.mark.parametrize(
    ("lookup", "expected"),
    [
        (api_data.get_pmcid_from_metadata, "PMC1"),
        (api_data.lookup_pmid_via_europe_pmc_by_metadata, "1"),
    ],
)
def test_metadata_lookup_returns_unique_exact_match(
    monkeypatch: pytest.MonkeyPatch, lookup: Callable[..., str | None], expected: str
) -> None:
    response = _search_results(
        {"title": "Erratum", "pubYear": "2015", "pmcid": "PMC1", "pmid": "1"}
    )
    monkeypatch.setattr(
        api_data, "_europe_pmc_cached", Mock(return_value=json.dumps(response))
    )

    assert (
        lookup(title="Erratum", year="2015-09-29", journal="Journal of Mammalogy")
        == expected
    )


def test_pmid_inference_does_not_fall_back_from_doi_to_pmcid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = api_data.PMIDCandidate(
        id=1,
        name="erratum.pdf",
        doi="10.1093/jmammal/gyv133",
        pmcid="PMC6394112",
        title="Erratum",
        journal="Journal of Mammalogy",
        year="2015-09-29",
    )
    monkeypatch.setattr(api_data, "to_pmid_candidate", Mock(return_value=candidate))
    idconv = Mock(
        side_effect=lambda identifier: (
            "30837778" if identifier.startswith("PMC") else None
        )
    )
    monkeypatch.setattr(api_data, "lookup_pmid_via_idconv", idconv)
    monkeypatch.setattr(
        api_data, "lookup_pmid_via_europe_pmc_by_doi", Mock(return_value=None)
    )

    assert api_data.infer_pmid_for_article(Mock()) is None
    idconv.assert_called_once_with("10.1093/jmammal/gyv133")
