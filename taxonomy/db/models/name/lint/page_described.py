import re
from collections.abc import Iterable

from taxonomy.db.constants import ArticleType
from taxonomy.db.models.article import Article
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import field_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import Name
from taxonomy.db.models.name.page import check_page, parse_page_text

from .base import LINT


@LINT.add("matches_citation")
def check_matches_citation(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None or nam.page_described is None:
        return
    art = nam.original_citation
    yield from check_page_matches_citation(art, nam.page_described)


def check_page_matches_citation(art: Article, page_text: str) -> Iterable[str]:
    if art.type not in (ArticleType.JOURNAL, ArticleType.CHAPTER, ArticleType.PART):
        return
    start_page = art.numeric_start_page()
    end_page = art.numeric_end_page()
    if not start_page or not end_page:
        return
    page_range = range(start_page, end_page + 1)
    for page in parse_page_text(page_text):
        if page.is_raw:
            continue
        try:
            numeric_page = int(page.text)
        except ValueError:
            continue
        if numeric_page not in page_range:
            yield f"{page_text} is not in {start_page}–{end_page} for {art}"


@LINT.add("no_page_ranges")
def no_page_ranges(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.page_described is None:
        return
    # Only applicable if there is a citation. This ensures we check
    # if the range is appropriate when adding a citation.
    if nam.original_citation is None:
        return
    for part in parse_page_text(nam.page_described):
        if not part.is_raw and re.fullmatch(r"[0-9]+-[0-9]+", part.text):
            # Ranges should only be used in very rare cases (e.g., where the
            # name itself literally extends across multiple pages). Enforce
            # an explicit IgnoreLint in such cases.
            yield f"page_described contains range: {part}"


@LINT.add("infer_page_described")
def infer_page_described(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if (
        nam.page_described is not None
        or nam.verbatim_citation is None
        or nam.original_citation is not None
    ):
        return
    cite = nam.verbatim_citation
    while True:
        new_cite = re.sub(r"\[[^\[\]]+\]", " ", cite).strip().rstrip(".")
        new_cite = re.sub(r"\([A-Za-z ]+\)", " ", new_cite).strip().rstrip(".")
        # remove trailing date
        new_cite = re.sub(
            r", (\d{1,2} )?([A-Z][a-z][a-z][a-z]?\.?\s)?1[789]\d{2}[a-e]?$",
            "",
            new_cite,
        ).strip()
        # remove trailing "fig." or "pl."
        new_cite = re.sub(r", ?(pl|pls|fig|figs|pi)\. ?\d[\d\-]*\.?$", "", new_cite)
        # remove trailing "fig." or "pl."
        new_cite = re.sub(r", \d+ ?(pl|pls|fig|figs|pi)\.?$", "", new_cite)
        if cite == new_cite:
            break
        cite = new_cite
    if match := re.search(r"(?:\bp ?\.|\bS\.|:)\s*(\d{1,4})\.?$", cite):
        page = match.group(1)
    else:
        return
    message = f"infer page {page!r} from {nam.verbatim_citation!r}"
    yield field_issue(message, nam, "page_described", page)


@LINT.add("page_described")
def check_page_described(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.page_described is None:
        return
    # For now ignore names without a citation
    if nam.original_citation is None:
        return

    new_page = yield from check_page(
        nam.page_described, get_raw_page_regex=nam.original_citation.get_raw_page_regex
    )
    if new_page != nam.page_described:
        yield field_issue(
            f"fixed page {nam.page_described!r} -> {new_page!r}",
            nam,
            "page_described",
            new_page,
        )
