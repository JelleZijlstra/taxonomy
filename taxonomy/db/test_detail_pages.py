import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from taxonomy import config
from taxonomy.db import detail_pages, url_cache
from taxonomy.db.detail_pages import PageText
from taxonomy.db.url_cache import CacheDomain

QUOTE = "The holotype was collected in the dense forest near the northern bank of the river, at an altitude of 1200 metres."


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("[p. 123] The quotation", None),
        ("The quotation [p. 123]", ("The quotation", "123")),
        ("The quotation [pp. 12 – 14]", ("The quotation", "12-14")),
        ("The quotation [p. xii]", ("The quotation", "xii")),
        ("The [p. 123] quotation", None),
        ("[p. 123] quotation [p. 124]", None),
        ("[p. 123]", None),
        ("Quotation [p. 123] @", None),
        ("Quotation [p. iiii]", ("Quotation", "iiii")),
        ("Quotation [pp. 14-12]", None),
    ],
)
def test_extract_page_annotation(text: str, expected: tuple[str, str] | None) -> None:
    assert detail_pages.extract_page_annotation(text) == expected


def test_substring_pages_require_agreement_and_boundaries() -> None:
    quote = "Northern bank of the river"
    assert (
        detail_pages.page_from_containing_quotes(
            quote, [(f"Collected at the {quote}.", "12")]
        )
        == "12"
    )
    assert (
        detail_pages.page_from_containing_quotes(
            quote, [(quote, "12"), (f"At the {quote}", "13")]
        )
        is None
    )
    assert (
        detail_pages.page_from_containing_quotes(quote, [(f"At the {quote}", "12-13")])
        is None
    )
    assert (
        detail_pages.page_from_containing_quotes(quote, [(quote, "12-13")]) == "12-13"
    )
    assert (
        detail_pages.page_from_containing_quotes("Paris", [("Near Paris", "12")])
        is None
    )
    assert detail_pages.page_from_containing_quotes("Paris", [("Paris", "12")]) == "12"
    assert (
        detail_pages.page_from_containing_quotes(quote, [(f"{quote}banks", "12")])
        is None
    )


def _pdf_page(number: int | None, text: str = "body text") -> str:
    return f"{number if number is not None else 'Heading'}\nHeading\n{text}\nAnother line\nFinal line\nFooter"


def test_pdf_mapping_requires_consistent_printed_anchors() -> None:
    pages = ["Cover", *(_pdf_page(n) for n in (10, 11, 12)), "Unnumbered end"]
    assert detail_pages.infer_pdf_page_numbers(pages) == {1: "10", 2: "11", 3: "12"}
    assert detail_pages.infer_pdf_page_numbers(pages[:3]) == {}
    assert detail_pages.infer_pdf_page_numbers(
        [_pdf_page(n) for n in (10, 11, None, 13)]
    ) == {0: "10", 1: "11", 2: "12", 3: "13"}
    assert (
        detail_pages.infer_pdf_page_numbers([_pdf_page(n) for n in (10, None, 11, 12)])
        == {}
    )


def test_pdf_mapping_rejects_restarts_and_conflicting_numbers() -> None:
    pages = [_pdf_page(n) for n in (10, 11, 12, 10, 11, 12)]
    assert detail_pages.infer_pdf_page_numbers(pages) == {}
    pages = [_pdf_page(n) for n in (10, 11, 12)] + [_pdf_page(11)]
    assert detail_pages.infer_pdf_page_numbers(pages) == {0: "10", 2: "12"}
    pages = [_pdf_page(n) for n in (10, 11, 12, 13)]
    pages[1] += "\n99"
    assert 1 not in detail_pages.infer_pdf_page_numbers(pages)


def test_pdf_mapping_ignores_body_numbers() -> None:
    pages = [f"Heading\nHeading\n{n}\ntext\ntext\nFooter" for n in (10, 11, 12)]
    assert detail_pages.infer_pdf_page_numbers(pages) == {}


@pytest.mark.parametrize(
    "text",
    [
        QUOTE,
        QUOTE.replace("collected", "col-\nlected").replace(" ", "  "),
        QUOTE.replace("collected", "coilected"),
    ],
)
def test_quote_search_normalizes_and_allows_small_ocr_errors(text: str) -> None:
    match = detail_pages.find_quote_page(
        QUOTE, [PageText("12", text), PageText("13", "unrelated text")]
    )
    assert match.page is not None
    assert match.page.page == "12"
    assert not match.ambiguous


@pytest.mark.parametrize("second_page", ["13", None, "12"])
def test_quote_search_rejects_repeated_matches_even_on_unmapped_pages(
    second_page: str | None,
) -> None:
    match = detail_pages.find_quote_page(
        QUOTE, [PageText("12", QUOTE), PageText(second_page, QUOTE)]
    )
    assert match.page is None
    assert match.ambiguous


def test_quote_search_rejects_near_matches_and_changed_numbers() -> None:
    near = QUOTE.replace("1200", "1201")
    match = detail_pages.find_quote_page(QUOTE, [PageText("12", near)])
    assert match.page is None
    assert match.ambiguous
    match = detail_pages.find_quote_page(
        QUOTE, [PageText("12", QUOTE), PageText("13", near)]
    )
    assert match.page is None
    assert match.ambiguous


def test_short_quotes_and_partial_search() -> None:
    assert detail_pages.find_quote_page("Paris", [PageText("12", "Paris")]).page is None
    quote = "Collected near the banks of the river"
    assert detail_pages.find_quote_page(quote, [PageText("12", quote)]).page is not None
    assert (
        detail_pages.find_quote_page(quote, [PageText("12", quote)], partial=True).page
        is None
    )
    assert detail_pages.find_quote_page(QUOTE, [PageText(None, QUOTE)]).ambiguous


def test_pdf_cache_is_read_only_and_invalidates_on_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        config, "get_options", lambda: SimpleNamespace(pdf_text_path=tmp_path)
    )
    assert detail_pages.cached_pdf_pages(123) == ()
    assert list(tmp_path.iterdir()) == []
    path = tmp_path / "123.txt"
    path.write_text(
        "\f".join(_pdf_page(n, QUOTE if n == 11 else "unrelated") for n in (10, 11, 12))
    )
    match = detail_pages.find_quote_page(QUOTE, detail_pages.cached_pdf_pages(123))
    assert match.page is not None
    assert match.page.page == "11"
    path.write_text(
        "\f".join(
            _pdf_page(n, QUOTE if n == 12 else "other unrelated content")
            for n in (10, 11, 12)
        )
    )
    match = detail_pages.find_quote_page(QUOTE, detail_pages.cached_pdf_pages(123))
    assert match.page is not None
    assert match.page.page == "12"


def _cache(
    monkeypatch: pytest.MonkeyPatch, values: dict[tuple[CacheDomain, str], object]
) -> list[tuple[CacheDomain, str]]:
    calls = []

    def get(domain: CacheDomain, key: str) -> str | None:
        calls.append((domain, key))
        result = values.get((domain, key))
        return (
            None if result is None else json.dumps({"Status": "ok", "Result": [result]})
        )

    monkeypatch.setattr(url_cache, "get_cached_value", get)
    return calls


def test_bhl_only_reads_cached_known_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        {"PageID": n, "PageNumbers": [{"Prefix": "Page", "Number": str(n)}]}
        for n in (12, 13, 14)
    ]
    calls = _cache(
        monkeypatch,
        {
            (CacheDomain.bhl_item, "1"): {"Pages": pages},
            (CacheDomain.bhl_page, "12"): {"OcrText": QUOTE},
            (CacheDomain.bhl_page, "14"): {"OcrText": QUOTE},
        },
    )
    result = detail_pages.cached_bhl_pages(
        {"12", "13"}, [], "https://www.biodiversitylibrary.org/item/1"
    )
    assert [page.page for page in result] == ["12"]
    assert (CacheDomain.bhl_page, "14") not in calls
    assert len(calls) == 3


def test_bhl_ambiguous_numbering_and_conflicting_links(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _cache(
        monkeypatch,
        {
            (CacheDomain.bhl_item, "1"): {
                "Pages": [
                    {"PageID": n, "PageNumbers": [{"Prefix": "Page", "Number": "12"}]}
                    for n in (1, 2)
                ]
            },
            (CacheDomain.bhl_page, "1"): {"OcrText": QUOTE},
            (CacheDomain.bhl_page, "2"): {"OcrText": QUOTE},
        },
    )
    assert (
        detail_pages.cached_bhl_pages(
            {"12"}, [], "https://www.biodiversitylibrary.org/item/1"
        )
        == ()
    )
    assert (
        detail_pages.cached_bhl_pages(
            {"12", "13"},
            [
                ("12", "https://www.biodiversitylibrary.org/page/1"),
                ("13", "https://www.biodiversitylibrary.org/page/1"),
            ],
            None,
        )
        == ()
    )
