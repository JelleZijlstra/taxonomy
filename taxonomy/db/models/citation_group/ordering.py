from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable
from typing import TypeVar

from taxonomy.db import models
from taxonomy.db.helpers import sift

from .cg import CitationGroup

T = TypeVar("T")
KeyT = TypeVar("KeyT", bound=Hashable)


def bucket(objs: Iterable[T], key: Callable[[T], KeyT]) -> dict[KeyT, list[T]]:
    buckets: dict[KeyT, list[T]] = {}
    for obj in objs:
        buckets.setdefault(key(obj), []).append(obj)
    return buckets


def not_none(obj: T | None) -> T:
    if obj is None:
        raise ValueError("Expected non-None value")
    return obj


def get_group_for_article(art: models.Article) -> list[models.Article] | None:
    if art.citation_group is None:
        return None
    order = get_ordered_articles(art.citation_group)
    for group in order:
        if art in group:
            return group
    return None


_order_cache: dict[CitationGroup, list[list[models.Article]]] = {}


def clear_cache_for_cg(cg: CitationGroup) -> None:
    _order_cache.pop(cg, None)


def clear_all_caches() -> None:
    _order_cache.clear()


def get_ordered_articles(cg: CitationGroup) -> list[list[models.Article]]:
    """Order articles by assumed publication date.

    Returns a list of lists, where each list is a set of articles that should have been
    published in order.

    """
    if cg in _order_cache:
        return _order_cache[cg]
    by_series = bucket(cg.get_articles(), lambda a: a.series)
    orders: list[list[models.Article]] = []
    for arts in by_series.values():
        ordered, unordered = order_articles_in_single_series(arts)
        orders.append(ordered)
        for art in unordered:
            orders.append([art])
    _order_cache[cg] = orders
    return orders


def order_articles_in_single_series(
    arts: list[models.Article],
) -> tuple[list[models.Article], list[models.Article]]:
    unsortable = []
    has_volume, has_no_volume = sift(
        arts,
        lambda a: a.volume is not None
        and get_number_from_possible_range(a.volume) is not None,
    )
    unsortable += has_no_volume
    main_order = []
    by_volume = bucket(
        has_volume, lambda a: get_number_from_possible_range(not_none(a.volume))
    )
    for _, volume_arts in sorted(by_volume.items()):
        ordered, unordered = order_articles_in_single_volume(volume_arts)
        main_order += ordered
        unsortable += unordered
    return main_order, unsortable


def get_number_from_possible_range(val: str) -> int | None:
    if "-" in val:
        val, end = val.split("-", maxsplit=1)
        if not end.isdigit():
            return None
    if not val.isdigit():
        return None
    return int(val)


def order_articles_in_single_volume(
    arts: list[models.Article],
) -> tuple[list[models.Article], list[models.Article]]:
    unsortable = []
    # Sort articles without an issue at the beginning; this might catch cases where we inconsistently
    # add an issue.
    has_issue, has_non_num_issue = sift(
        arts,
        lambda a: a.issue is None
        or get_number_from_possible_range(a.issue) is not None,
    )
    unsortable += has_non_num_issue
    main_order = []
    by_issue = bucket(
        has_issue,
        lambda a: 0 if a.issue is None else get_number_from_possible_range(a.issue),
    )
    for _, issue_arts in sorted(by_issue.items()):
        ordered, unordered = order_articles_in_single_issue(issue_arts)
        main_order += ordered
        unsortable += unordered
    return main_order, unsortable


def order_articles_in_single_issue(
    arts: list[models.Article],
) -> tuple[list[models.Article], list[models.Article]]:
    unsortable = []
    has_page, has_no_page = sift(
        arts, lambda a: a.start_page is not None and a.start_page.isnumeric()
    )
    unsortable += has_no_page
    main_order = []
    by_page = bucket(has_page, lambda a: int(not_none(a.start_page)))
    for _, page_arts in sorted(by_page.items()):
        main_order += page_arts
    return main_order, unsortable
