from collections.abc import Iterable
from typing import Any

from taxonomy import getinput
from taxonomy.db.models.article.article import Article
from taxonomy.db.models.name import Name
from taxonomy.db.models.name.name import NameTag

from . import schema, search


def get_known_usages(nam: Name) -> Iterable[Article]:
    nam = nam.resolve_variant()
    for related_nam in nam.taxon.get_names():
        if related_nam.resolve_variant() == nam:
            for ce in related_nam.classification_entries:
                yield ce.article
    for tag in nam.tags:
        if isinstance(tag, NameTag.ValidUse):
            yield tag.source


def usage_search(nam: Name, *, max_hits: int = 50) -> Iterable[tuple[Article, str]]:
    if nam.corrected_original_name is None:
        print("No corrected_original_name for name", nam)
        return
    existing = set(get_known_usages(nam))
    client = search.get_client()
    response = client.search(
        query=nam.corrected_original_name,
        queryParser="simple",
        highlight=search.get_highlight_param(),
        queryOptions='{"fields":["text^1"]}',
        size=max_hits,
        start=0,
    )["hits"]
    for hit in response["hit"]:
        pair = resolve_hit(hit)
        if pair is not None:
            if pair[0] in existing:
                continue
            existing.add(pair[0])
            if pair[0].is_unpublished() or pair[0].numeric_year() < 1980:
                print(f"Skipping {pair[0]}")
                continue
            yield pair


def resolve_hit(hit: dict[str, Any]) -> tuple[Article, str] | None:
    document_id = hit["id"]
    pieces = document_id.split("/")
    if len(pieces) == 3:
        call_sign, oid, page = pieces
        context = f"Page {page}: "
    else:
        call_sign, oid = pieces
        context = ""
    model_cls = schema.get_by_call_sign(call_sign)
    obj = model_cls(int(oid))
    if not isinstance(obj, Article):
        print(f"Unexpected call sign {call_sign!r} for {model_cls.__name__}")
        return None
    highlights = " .. ".join(
        value for value in hit["highlights"].values() if "**" in value
    )
    return obj, context + highlights


def run_and_print(nam: Name, *, max_hits: int = 50, interactive: bool = True) -> None:
    print(nam.resolve_variant().make_usage_list())
    getinput.print_header("Search results")
    for hit in usage_search(nam, max_hits=max_hits):
        getinput.print_header(hit[0])
        hit[0].display()
        print(hit[1])
        if interactive:
            hit[0].edit()
        print()
