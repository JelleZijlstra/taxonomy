import json
import re
from collections.abc import Iterable

import clirm

from taxonomy.db import models
from taxonomy.db.constants import ArticleType
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.citation_group import lint as cg_lint
from taxonomy.db.models.lint import (
    add_tag_fix,
    add_tag_issue,
    field_fix,
    field_issue,
    fields_issue,
    fixes_issue,
    replace_tag_issue,
)
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name import parse_citations
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
    if not models.name.required_fields.is_desired(nam, "verbatim_citation"):
        return
    tags = list(nam.get_tags(nam.type_tags, TypeTag.CitationDetail))
    if not tags:
        return
    longest = max(tags, key=lambda tag: len(tag.text))
    message = f"setting verbatim citation from tag {longest}"
    yield field_issue(message, nam, "verbatim_citation", longest.text)


@LINT.add("structured_verbatim_citation")
def add_structured_verbatim_citation(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    """Add or update a StructuredVerbatimCitation tag based on verbatim_citation.

    Uses regex heuristics in parse_citations.py to extract series/volume/issue/pages
    from Name.verbatim_citation. Only emits/updates the tag if at least one field is
    parsed.
    """
    if nam.verbatim_citation is None:
        return
    parsed = parse_citations.parse_citation(nam.verbatim_citation)
    if not any(
        [parsed.series, parsed.volume, parsed.issue, parsed.start_page, parsed.end_page]
    ):
        return
    new_tag = TypeTag.StructuredVerbatimCitation(
        series=parsed.series,
        volume=parsed.volume,
        issue=parsed.issue,
        start_page=parsed.start_page,
        end_page=parsed.end_page,
    )
    existing = nam.get_type_tag(TypeTag.StructuredVerbatimCitation)

    if existing is None:
        msg = f"add StructuredVerbatimCitation from verbatim_citation: {new_tag}"
        yield add_tag_issue(msg, nam, new_tag, field="type_tags")
        return
    else:
        # Only re-add series if the journal supports it
        if existing.series:
            series = existing.series
        elif (
            new_tag.series
            and nam.citation_group
            and models.citation_group.lint.get_series_regex(nam.citation_group)
        ):
            series = new_tag.series
        else:
            series = None

        merged = TypeTag.StructuredVerbatimCitation(
            series=series,
            volume=existing.volume or new_tag.volume,
            issue=existing.issue or new_tag.issue,
            start_page=existing.start_page or new_tag.start_page,
            end_page=existing.end_page or new_tag.end_page,
            url=existing.url,
        )
        if merged != existing:
            msg = f"update StructuredVerbatimCitation from {existing} to {merged}"
            yield replace_tag_issue(msg, nam, existing, merged, field="type_tags")


@LINT.add("structured_verbatim_citation_fields")
def check_structured_verbatim_citation_fields(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    """Validate StructuredVerbatimCitation fields against CitationGroup rules.

    - Series/volume/issue must match the CitationGroup's regex tags when present;
      otherwise defaults are used from cg_lint.
    - If start_page and end_page are both present and numeric, end_page >= start_page.
    """
    # Find a citation group to validate against
    cg = nam.get_citation_group()
    if cg is None:
        return

    for tag in nam.get_tags(nam.type_tags, TypeTag.StructuredVerbatimCitation):
        # series
        series_regex = cg_lint.get_series_regex(cg)
        if series_regex is not None:
            if tag.series is not None and not re.fullmatch(series_regex, tag.series):
                yield f"series {tag.series!r} does not match regex {series_regex!r} for {cg}"
        elif tag.series is not None and cg.type is ArticleType.JOURNAL:
            # Only enforced for journals as per Article logic
            yield f"is in {cg}, which does not support series"

        # volume
        if tag.volume is not None:
            normalized_volume = tag.volume.replace("–", "-")
            if normalized_volume != tag.volume:
                msg = (
                    f"normalize SVC volume dash {tag.volume!r} -> {normalized_volume!r}"
                )
                new_tag = TypeTag.StructuredVerbatimCitation(
                    volume=normalized_volume,
                    issue=tag.issue,
                    start_page=tag.start_page,
                    end_page=tag.end_page,
                    series=tag.series,
                    url=tag.url,
                )
                yield replace_tag_issue(msg, nam, tag, new_tag, field="type_tags")
                tag = new_tag
            rgx = cg_lint.get_volume_regex(cg)
            if not re.fullmatch(rgx, tag.volume):
                yield f"volume {tag.volume!r} does not match {cg_lint.describe_volume_regex(cg)} (CG {cg})"

        # issue
        if tag.issue is not None:
            normalized_issue = re.sub(r"[–_]", "-", tag.issue)
            normalized_issue = re.sub(r"^(\d+)/(\d+)$", r"\1–\2", normalized_issue)
            if normalized_issue != tag.issue:
                msg = f"normalize SVC issue formatting {tag.issue!r} -> {normalized_issue!r}"
                new_tag = TypeTag.StructuredVerbatimCitation(
                    volume=tag.volume,
                    issue=normalized_issue,
                    start_page=tag.start_page,
                    end_page=tag.end_page,
                    series=tag.series,
                    url=tag.url,
                )
                yield replace_tag_issue(msg, nam, tag, new_tag, field="type_tags")
                tag = new_tag
            rgx = cg_lint.get_issue_regex(cg)
            if not re.fullmatch(rgx, tag.issue):
                yield f"issue {tag.issue!r} does not match {cg_lint.describe_issue_regex(cg)} (CG {cg})"

        # pages: ensure end >= start if both numeric
        if (
            tag.start_page
            and tag.end_page
            and tag.start_page.isdigit()
            and tag.end_page.isdigit()
        ):
            start_page = tag.start_page
            end_page = tag.end_page
            # Autofix shorthand like "452-58" -> "452-458" when it increases end_page beyond start_page
            if len(end_page) < len(start_page):
                try:
                    prefix = start_page[: len(start_page) - len(end_page)]
                    candidate = prefix + end_page
                    if int(candidate) > int(start_page):
                        msg = f"expand SVC end_page {end_page!r} -> {candidate!r}"
                        new_tag = TypeTag.StructuredVerbatimCitation(
                            volume=tag.volume,
                            issue=tag.issue,
                            start_page=start_page,
                            end_page=candidate,
                            series=tag.series,
                            url=tag.url,
                        )
                        yield replace_tag_issue(
                            msg, nam, tag, new_tag, field="type_tags"
                        )
                        end_page = candidate
                except ValueError:
                    pass
            if int(end_page) < int(start_page):
                yield f"end_page {end_page!r} is less than start_page {start_page!r} in StructuredVerbatimCitation"
