from typing import Any

from taxonomy.apis import bhl


def _page(page_id: int, number: str) -> dict[str, Any]:
    return {
        "PageID": page_id,
        "PageNumbers": [{"Prefix": "Page", "Number": number}],
        "PageTypes": [{"PageTypeName": "Text"}],
    }


def test_get_matching_pages_selects_bis_occurrence() -> None:
    pages = [_page(1, "160"), _page(2, "160")]

    assert bhl._get_matching_pages(pages, "160") == [1, 2]
    assert bhl._get_matching_pages(pages, "160bis") == [2]
