"""Shared cleanup and page inference for source detail tags."""

import re
from collections.abc import Iterable, Sequence

from taxonomy import adt
from taxonomy.db import detail_pages, helpers, models
from taxonomy.db.constants import StringCleanupOption
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.lint import (
    field_fix,
    field_issue,
    fixes_issue,
    replace_tag_fix,
    replace_tag_issue,
)
from taxonomy.db.models.lint_types import LintResult


def normalized_text(tag: adt.ADT) -> str:
    """Use the same noninteractive text cleanup as general field lint."""
    options = helpers.get_string_cleanup_options(type(tag).__annotations__["text"])
    return helpers.interactive_clean_string(
        tag.text,
        normalize_sex_symbols=StringCleanupOption.normalize_sex_symbols in options,
        normalize_detail_ocr=StringCleanupOption.normalize_detail_ocr in options,
        interactive=False,
    )


def clean_duplicate_details(
    owner: models.Name | models.ClassificationEntry,
) -> Iterable[LintResult]:
    field = "type_tags" if isinstance(owner, models.Name) else "tags"
    original = getattr(owner, field)
    details = _details(owner)
    normalized = [
        tag.replace(text=normalized_text(tag)) if tag in details else tag
        for tag in original
    ]
    new_tags = []
    for index, tag in enumerate(normalized):
        if original[index] in details and tag in normalized[:index]:
            continue
        if original[index] in details and any(
            type(other) is type(tag) and other.text == tag.text
            # Only remove a quotation if another tag preserves every populated
            # field, including its source and ClassificationEntry provenance.
            and all(
                getattr(tag, attr) in (None, getattr(other, attr))
                for attr in type(tag)._attributes
                if attr != "text"
            )
            and other != tag
            for other in normalized
        ):
            continue
        new_tags.append(tag)
    if tuple(new_tags) != tuple(original):
        yield field_issue(
            "normalize and remove redundant detail tags", owner, field, tuple(new_tags)
        )


def _numbered_pages(page: str | None) -> set[str]:
    if page is None:
        return set()
    if re.fullmatch(r"[0-9]+|[ivxlcdm]+", page):
        return {page}
    if match := re.fullmatch(r"([0-9]+)-([0-9]+)", page):
        start, end = map(int, match.groups())
        if 0 <= end - start <= 10:
            return {str(number) for number in range(start, end + 1)}
    return set()


def _details(owner: models.Name | models.ClassificationEntry) -> Sequence[adt.ADT]:
    if isinstance(owner, models.Name):
        return [
            tag
            for tag in owner.type_tags
            if isinstance(tag, models.name.name.SOURCE_TAGS)
        ]
    return [
        tag
        for tag in owner.tags
        if isinstance(tag, models.classification_entry.ce.DETAIL_TAGS)
    ]


def _source(
    owner: models.Name | models.ClassificationEntry, tag: adt.ADT
) -> models.Article:
    return tag.source if isinstance(owner, models.Name) else owner.article


def _same_context(
    owner: models.Name | models.ClassificationEntry, left: adt.ADT, right: adt.ADT
) -> bool:
    if _source(owner, left) != _source(owner, right):
        return False
    if isinstance(owner, models.Name):
        return (
            left.classification_entry is None
            or right.classification_entry is None
            or left.classification_entry == right.classification_entry
        )
    return True


def _page_within_source(source: models.Article | None, page: str) -> bool:
    if source is None:
        return True
    start, end = source.start_page, source.end_page
    if not start or not end or not start.isdecimal() or not end.isdecimal():
        return True
    match = re.fullmatch(r"([0-9]+)(?:-([0-9]+))?", page)
    if match is None:
        return True
    first = int(match[1])
    last = int(match[2]) if match[2] is not None else first
    return int(start) <= first <= last <= int(end)


def _move_annotation(
    owner: models.Name | models.ClassificationEntry,
    tag: adt.ADT,
    text: str,
    page: str,
    field: str,
) -> Iterable[LintResult]:
    fixes = [
        replace_tag_fix(owner, tag, tag.replace(text=text, page=page), field=field)
    ]
    if isinstance(owner, models.Name) and tag.classification_entry is not None:
        ce = tag.classification_entry
        if ce.article == tag.source:
            if isinstance(tag, models.name.TypeTag.LocationDetail):
                if ce.type_locality == tag.text:
                    fixes.append(field_fix(ce, "type_locality", text))
            else:
                ce_tag_type = models.name.name.DETAIL_TAG_TO_CE_TAG[type(tag)]
                for ce_tag in ce.get_tags(ce.tags, ce_tag_type):
                    if ce_tag.text != tag.text:
                        continue
                    if ce_tag.page not in (None, page):
                        yield f"{tag} has page annotation {page!r} conflicting with linked CE tag {ce_tag}"
                        return
                    fixes.append(
                        replace_tag_fix(
                            ce, ce_tag, ce_tag.replace(text=text, page=page)
                        )
                    )
    yield fixes_issue(f"move page annotation {page!r} out of {tag}", *fixes)


def infer_detail_pages(
    owner: models.Name | models.ClassificationEntry,
) -> Iterable[LintResult]:
    details = _details(owner)
    field = "type_tags" if isinstance(owner, models.Name) else "tags"
    for tag in details:
        annotation = detail_pages.extract_page_annotation(tag.text)
        if annotation is not None:
            text, page = annotation
            if tag.page is not None and tag.page != page:
                yield f"{tag} has page annotation {page!r} conflicting with page {tag.page!r}"
            elif not _page_within_source(_source(owner, tag), page):
                yield f"{tag} has page annotation {page!r} outside its source's page range; leaving quotation unchanged"
            else:
                yield from _move_annotation(owner, tag, text, page, field)
            continue
        if tag.page is not None:
            continue
        donors = [
            (other.text, other.page)
            for other in details
            if other is not tag
            and other.page is not None
            and _same_context(owner, tag, other)
            and _page_within_source(_source(owner, other), other.page)
        ]
        inferred_page = detail_pages.page_from_containing_quotes(tag.text, donors)
        if inferred_page is not None:
            yield replace_tag_issue(
                f"infer page {inferred_page!r} from a containing quotation for {tag}",
                owner,
                tag,
                tag.replace(page=inferred_page),
                field=field,
            )


def _known_pages(
    owner: models.Name | models.ClassificationEntry,
    source: models.Article,
    details: Sequence[adt.ADT],
) -> tuple[set[str], set[tuple[str, str]]]:
    pages: set[str] = set()
    links: set[tuple[str, str]] = set()
    ces = set()
    for tag in details:
        if _source(owner, tag) != source:
            continue
        pages.update(_numbered_pages(tag.page))
        if tag.page_link and len(_numbered_pages(tag.page)) == 1:
            links.add((tag.page, tag.page_link))
        if isinstance(owner, models.Name) and tag.classification_entry is not None:
            ces.add(tag.classification_entry)
    if isinstance(owner, models.Name):
        ces.update(owner.get_classification_entries())
        if owner.original_citation == source:
            pages.update(_numbered_pages(owner.page_described))
            for tag in owner.get_tags(
                owner.type_tags, models.name.TypeTag.AuthorityPageLink
            ):
                if tag.confirmed and len(_numbered_pages(tag.page)) == 1:
                    pages.add(tag.page)
                    links.add((tag.page, tag.url))
    else:
        ces.add(owner)
    for ce in ces:
        if ce.article != source:
            continue
        pages.update(_numbered_pages(ce.page))
        for tag in ce.get_tags(
            ce.tags, models.classification_entry.ClassificationEntryTag.PageLink
        ):
            if len(_numbered_pages(tag.page)) == 1:
                pages.add(tag.page)
                links.add((tag.page, tag.url))
    pages = {page for page in pages if _page_within_source(source, page)}
    links = {(page, link) for page, link in links if page in pages}
    return pages, links


def infer_detail_pages_from_text(
    owner: models.Name | models.ClassificationEntry, cfg: LintConfig
) -> Iterable[LintResult]:
    if not cfg.is_resource_available(LintResource.SLOW):
        return
    details = _details(owner)
    field = "type_tags" if isinstance(owner, models.Name) else "tags"
    sources = {_source(owner, tag) for tag in details if tag.page is None}
    for source in sources:
        if source is None:
            continue
        pdf = detail_pages.cached_pdf_pages(source.id)
        known, links = _known_pages(owner, source, details)
        bhl = detail_pages.cached_bhl_pages(known, links, source.url) if known else ()
        for tag in details:
            if tag.page is not None or _source(owner, tag) != source:
                continue
            # Explicit annotations, including conflicts, belong to the cheap lint.
            if detail_pages.extract_page_annotation(tag.text) is not None:
                continue
            pdf_match = detail_pages.find_quote_page(tag.text, pdf)
            bhl_match = detail_pages.find_quote_page(tag.text, bhl, partial=True)
            if pdf_match.ambiguous or bhl_match.ambiguous:
                continue
            evidence = [
                match.page for match in (pdf_match, bhl_match) if match.page is not None
            ]
            if not evidence or len({page.page for page in evidence}) != 1:
                continue
            page = evidence[0].page
            if any(link == tag.page_link and number != page for number, link in links):
                continue
            if page is not None and not _page_within_source(source, page):
                continue
            updated = tag.replace(page=page)
            if bhl_match.page is not None:
                link = bhl_match.page.page_link
                if tag.page_link is not None and tag.page_link != link:
                    continue
                updated = updated.replace(page_link=link)
            kind = "cached PDF text" if pdf_match.page is not None else "cached BHL OCR"
            yield replace_tag_issue(
                f"infer page {page!r} from {kind} for {tag}",
                owner,
                tag,
                updated,
                field=field,
            )
