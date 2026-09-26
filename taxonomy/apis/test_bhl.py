from typing import Any

import pytest

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


@pytest.mark.parametrize(
    ("number", "anchors", "expected"),
    [
        (11, [(10, 100), (13, 60)], 90),
        (12, [(10, 100), (13, 60)], 200),
        (9, [(10, 100), (13, 60)], None),
        (14, [(10, 100), (13, 60)], None),
        (11, [(10, 100)], None),
        (11, [(10, 100), (12, 60)], None),  # Extra scan.
        (11, [(10, 100), (14, 60)], None),  # Missing scan.
        (11, [(10, 60), (13, 100)], None),  # Reversed anchors.
        (11, [(10, 100), (13, 999)], None),  # Different item.
        (11, [(10, 100), (10, 90), (13, 60)], None),
        (11, [(10, 100), (11, 200), (13, 60)], None),
        (12, [(10, 100), (11, 200), (13, 60)], None),
        (11, [(10, 100), (10, 100), (13, 60)], 90),
    ],
)
def test_infer_page_from_anchors(
    monkeypatch: pytest.MonkeyPatch,
    number: int,
    anchors: list[tuple[int, int]],
    expected: int | None,
) -> None:
    pages = [_page(page_id, "") for page_id in (100, 90, 200, 60)]
    for page in pages:
        page["PageNumbers"] = []
    monkeypatch.setattr(bhl, "get_item_metadata", lambda _: {"Pages": pages})
    assert bhl.infer_page_from_anchors(1, number, anchors) == expected


def test_infer_page_from_anchors_rejects_conflicting_bhl_number(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [_page(100, "10"), _page(101, "99"), _page(102, "12")]
    monkeypatch.setattr(bhl, "get_item_metadata", lambda _: {"Pages": pages})
    assert bhl.infer_page_from_anchors(1, 11, [(10, 100), (12, 102)]) is None
