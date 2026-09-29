import json
import re
from collections.abc import Iterable

import clirm

from taxonomy.db.models.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import (
    add_tag_fix,
    field_fix,
    field_issue,
    fields_issue,
    fixes_issue,
)
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import Name, TypeTag

from .base import LINT


@LINT.add("verbatim")
def clean_up_verbatim(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.verbatim_citation is not None and nam.original_citation is not None:
        message = (
            f"cleaning up verbatim citation: {nam.original_citation.name},"
            f" {nam.verbatim_citation}"
        )
        data = nam._load_data()
        value: object = nam.verbatim_citation
        if "verbatim_citation" in data:
            existing = data["verbatim_citation"]
            if isinstance(existing, list):
                value = [*existing, value]
            else:
                value = [existing, value]
        data["verbatim_citation"] = value
        yield fields_issue(
            message, (nam, "data", json.dumps(data)), (nam, "verbatim_citation", None)
        )
    if nam.citation_group is not None and nam.original_citation is not None:
        message = (
            f"cleaning up citation group: {nam.original_citation.name},"
            f" {nam.citation_group}"
        )
        yield field_issue(message, nam, "citation_group", None)


@LINT.add("verbatim_to_citation_detail")
def verbatim_to_citation_detail(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.verbatim_citation is None:
        return
    original = nam.verbatim_citation
    remaining = original
    new_tags: list[TypeTag.CitationDetail] = []  # type: ignore[name-defined]
    for whole_match, source, text in re.findall(
        r"(\[From \{([^}]+)\}: ([^\]]+)\])", remaining
    ):
        try:
            source_art = (
                Article.select().filter(Article.name == source).get().resolve_redirect()
            )
        except clirm.DoesNotExist:
            continue
        tag = TypeTag.CitationDetail(text, source_art)
        new_tags.append(tag)
        remaining = remaining.replace(whole_match, "")
    match = re.fullmatch(
        r"([^\]]+(?: \[[^\[\]\{\}]+\])?|[^{]+) \[from \{([^\}]+)\}\]", remaining
    )
    if match:
        text, source = match.groups()
        try:
            source_art = (
                Article.select().filter(Article.name == source).get().resolve_redirect()
            )
        except clirm.DoesNotExist:
            pass
        else:
            tag = TypeTag.CitationDetail(text, source_art)
            new_tags.append(tag)
            remaining = ""

    if new_tags:
        yield fixes_issue(
            f"convert verbatim citation to CitationDetail tags: {new_tags}",
            *(add_tag_fix(nam, tag, field="type_tags") for tag in new_tags),
            field_fix(nam, "verbatim_citation", remaining or None),
        )

    # if nam.verbatim_citation is not None and "{" in nam.verbatim_citation:
    #     yield f"unhandled verbatim citation: {nam.verbatim_citation}"


@LINT.add("verbatim_from_tags")
def verbatim_citation_from_tags(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.verbatim_citation:
        return
    if "verbatim_citation" not in nam.get_required_fields():
        return
    tags = list(nam.get_tags(nam.type_tags, TypeTag.CitationDetail))
    if not tags:
        return
    longest = max(tags, key=lambda tag: len(tag.text))
    message = f"setting verbatim citation from tag {longest}"
    yield field_issue(message, nam, "verbatim_citation", longest.text)
