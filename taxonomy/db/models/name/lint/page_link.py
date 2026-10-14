import itertools
import subprocess
from collections.abc import Iterable

from taxonomy import getinput, urlparse
from taxonomy.apis import bhl
from taxonomy.db import models
from taxonomy.db.models.article import Article, ArticleTag
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.classification_entry.ce import ClassificationEntryTag
from taxonomy.db.models.item_file import ItemFile
from taxonomy.db.models.lint import add_tag_issue, replace_tag_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import ExperimentalLintCondition as Exp
from taxonomy.db.models.name.name import Name, TypeTag
from taxonomy.db.models.name.page import get_unique_page_text

from .base import LINT


def _autoset_original_citation_url(nam: Name) -> None:
    if nam.original_citation is None:
        return
    nam.display()
    nam.original_citation.display()
    nam.open_url()
    url = getinput.get_line(
        "URL: ", callbacks=nam.original_citation.get_wrapped_adt_callbacks()
    )
    if not url:
        return
    nam.original_citation.set_or_replace_url(url)


@LINT.add("authority_page_link", requires_network=True)
def check_must_have_authority_page_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.original_citation is None
        or not nam.original_citation.has_bhl_link_with_pages()
    ):
        return
    pages_with_links = _get_pages_with_links(nam)
    for page in get_unique_page_text(nam.page_described):
        if page not in pages_with_links:
            yield f"must have authority page link for {page}"


@LINT.add("check_bhl_page", requires_network=True)
def check_bhl_page(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.original_citation is None:
        return
    wrong_bhl_pages = nam.original_citation.has_tag(ArticleTag.BHLWrongPageNumbers)
    for tag in nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink):
        parsed = urlparse.parse_url(tag.url)
        if not isinstance(parsed, urlparse.BhlPage):
            continue
        if wrong_bhl_pages:
            page_metadata = bhl.get_page_metadata(parsed.page_id)
            try:
                page_number = page_metadata["PageNumbers"][0]["Number"]
            except LookupError:
                pass
            else:
                if page_number == tag.page:
                    yield f"page number {tag.page} matches BHL data for {tag.url}, but {nam.original_citation} is marked as having wrong page numbers"
        if nam.original_citation.url is None:
            yield f"name has BHL page, but original citation has no URL: {nam.original_citation}"
            if cfg.interactive:
                nam.original_citation.format(quiet=True)
                if nam.original_citation.url is None:
                    _autoset_original_citation_url(nam)
            continue
        parsed_url = urlparse.parse_url(nam.original_citation.url)
        if not isinstance(parsed_url, urlparse.BhlUrl):
            yield f"name has BHL page, but citation has non-BHL URL {nam.original_citation.url}"
            if cfg.interactive:
                _autoset_original_citation_url(nam)
            continue
        yield from _check_bhl_item_matches(nam, tag, cfg)
        yield from _check_bhl_bibliography_matches(nam, tag, cfg)


@LINT.add("item_file_for_authority_link", requires_network=True)
def item_file_for_authority_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """If no original_citation but AuthorityPageLink exists, and that link maps to a
    BHL item for which we have an ItemFile, report it.

    This helps surface cases where an ItemFile exists that could be used to
    derive the original citation.
    """
    if nam.original_citation is not None:
        return
    tags = list(nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink))
    seen: set[ItemFile] = set()
    for tag in tags:
        matches = [
            itf
            for itf in models.item_file.get_matching_item_files(tag.url)
            if itf not in seen
        ]
        seen.update(matches)
        for itf in matches:
            message = f"authority page link points to existing ItemFile {itf}"
            yield message
            if cfg.interactive:
                print(f"{nam}: {message}")
                itf.burst()


_seen_pages: set[tuple[int, str]] = set()


def _should_require_unchecked_page_link(nam: Name, cfg: LintConfig) -> bool:
    if nam.original_citation is not None:
        return False
    if any(isinstance(tag, TypeTag.AuthorityPageLink) for tag in nam.type_tags):
        return False
    if Exp(above_name_id=140_000).should_apply(nam, cfg):
        return True
    if nam.page_described is None or not nam.page_described.isnumeric():
        return False
    structured_cite = nam.get_type_tag(TypeTag.StructuredVerbatimCitation)
    if structured_cite is not None and structured_cite.volume is not None:
        return True
    return False


@LINT.add(
    "unchecked_authority_page_link",
    requires_network=True,
    clear_caches=_seen_pages.clear,
)
def unchecked_authority_page_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """If no AuthorityPageLink exists but candidate BHL pages do, surface them.

    In interactive mode, open each candidate URL and ask the user to add it as an
    AuthorityPageLink.
    """
    if not _should_require_unchecked_page_link(nam, cfg):
        return
    # Compute candidates (same logic used by mdd_names.py via get_candidate_bhl_pages)
    candidates = [
        cand
        for cand in get_candidate_bhl_pages(nam, verbose=cfg.verbose)
        if cand.volume_matches
    ]
    if not candidates:
        return
    candidates = sorted(candidates, key=lambda page: page.sort_key())
    urls = [page.page_url for page in candidates]
    if cfg.interactive and not LINT.is_ignoring_lint(
        nam, "unchecked_authority_page_link"
    ):
        nam.display()
        if nam.original_citation is not None:
            nam.original_citation.display()
        for page in candidates[:2]:
            if (nam.id, page.page_url) in _seen_pages:
                continue
            _seen_pages.add((nam.id, page.page_url))
            print(page.page_url)
            subprocess.check_call(["open", page.page_url])
            if getinput.yes_no(
                "Add as AuthorityPageLink? ", callbacks=nam.get_wrapped_adt_callbacks()
            ):
                tag = TypeTag.AuthorityPageLink(
                    url=page.page_url, confirmed=True, page=str(page.page_number)
                )
                nam.add_type_tag(tag)
            break
    max_list = " | ".join(sorted(urls))
    yield f"possible authority page links: {max_list}"


def _check_bhl_item_matches(
    nam: Name,
    tag: TypeTag.AuthorityPageLink,  # type: ignore[name-defined]
    cfg: LintConfig,
) -> Iterable[LintResult]:
    item_id = bhl.get_bhl_item_from_url(tag.url)
    if item_id is None:
        yield f"cannot find BHL item for {tag.url}"
        return
    if nam.original_citation is None or nam.original_citation.url is None:
        return
    citation_item_ids = list(nam.original_citation.get_possible_bhl_item_ids())
    if not citation_item_ids:
        return
    if item_id not in citation_item_ids:
        yield (
            f"BHL item mismatch: {item_id} (name) not in {citation_item_ids} (citation)"
        )
        replacement = [
            page
            for page in get_candidate_bhl_pages(nam, verbose=False)
            if page.item_id in citation_item_ids and page.is_confident
        ]
        if len(replacement) == 1:
            new_tag = TypeTag.AuthorityPageLink(
                url=replacement[0].page_url, confirmed=True, page=tag.page
            )
            yield from _replace_page_link(nam, tag, new_tag, cfg)
            return
        if nam.original_citation.url is not None and nam.page_described is not None:
            parsed_url = urlparse.parse_url(nam.original_citation.url)
            if isinstance(parsed_url, urlparse.BhlPart):
                pages = bhl.get_possible_pages_from_part(
                    parsed_url.part_id, nam.page_described
                )
                if len(pages) == 1:
                    new_tag = TypeTag.AuthorityPageLink(
                        url=str(urlparse.BhlPage(pages[0])),
                        confirmed=True,
                        page=nam.page_described,
                    )
                    yield from _replace_page_link(nam, tag, new_tag, cfg)
                    return
        if cfg.manual_mode:
            nam.display()
            nam.original_citation.display()
            nam.open_url()
            nam.edit_until_clean()


def _replace_page_link(
    nam: Name, existing_tag: TypeTag, new_tag: TypeTag, cfg: LintConfig
) -> Iterable[LintResult]:
    message = f"replace {existing_tag} with {new_tag}"
    yield replace_tag_issue(message, nam, existing_tag, new_tag, field="type_tags")


def _check_bhl_bibliography_matches(
    nam: Name,
    tag: TypeTag.AuthorityPageLink,  # type: ignore[name-defined]
    cfg: LintConfig,
) -> Iterable[LintResult]:
    bibliography_id = bhl.get_bhl_bibliography_from_url(tag.url)
    if bibliography_id is None:
        if not bhl.is_item_missing_bibliography(tag.url):
            yield f"cannot find BHL bibliography for {tag.url}"
        return
    if nam.original_citation is None or nam.original_citation.url is None:
        return
    citation_biblio_ids = list(
        nam.original_citation.get_possible_bhl_bibliography_ids()
    )
    if bibliography_id not in citation_biblio_ids:
        yield f"BHL item mismatch: {bibliography_id} (name) not in {citation_biblio_ids} (citation)"
        replacement = [
            page
            for page in get_candidate_bhl_pages(nam, verbose=False)
            if page.item_id in citation_biblio_ids and page.is_confident
        ]
        if len(replacement) == 1:
            new_tag = TypeTag.AuthorityPageLink(
                url=replacement[0].page_url, confirmed=True, page=tag.page
            )
            yield replace_tag_issue(
                f"replace {tag.url} with {replacement[0].page_url}",
                nam,
                tag,
                new_tag,
                field="type_tags",
            )


def _get_pages_with_links(nam: Name) -> set[str]:
    return {tag.page for tag in nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink)}


def _should_look_for_page_links(nam: Name) -> bool:
    if not nam.page_described:
        return False
    pages = get_unique_page_text(nam.page_described)
    pages_with_links = _get_pages_with_links(nam)
    return not all(page in pages_with_links for page in pages)


def _maybe_add_bhl_page(
    nam: Name, cfg: LintConfig, page_obj: bhl.PossiblePage
) -> Iterable[LintResult]:
    message = f"inferred BHL page {page_obj}"
    tag = TypeTag.AuthorityPageLink(
        url=page_obj.page_url, confirmed=True, page=str(page_obj.page_number)
    )
    yield add_tag_issue(message, nam, tag, field="type_tags")
    print(page_obj.page_url)


@LINT.add("infer_bhl_page", requires_network=True)
def infer_bhl_page(
    nam: Name, cfg: LintConfig = LintConfig(autofix=False, interactive=False)
) -> Iterable[LintResult]:
    if not _should_look_for_page_links(nam):
        if cfg.verbose:
            print(f"{nam}: Skip because no page or enough tags")
        return
    confident_candidates = [
        page
        for page in get_candidate_bhl_pages(nam, verbose=cfg.verbose)
        if page.is_confident
    ]
    for _, group_iter in itertools.groupby(
        confident_candidates, lambda page: page.page_number
    ):
        group = list(group_iter)
        if len(group) == 1:
            yield from _maybe_add_bhl_page(nam, cfg, group[0])
        else:
            if cfg.verbose or cfg.manual_mode:
                print(f"Reject for {nam} because multiple pages with name:")
                for page_obj in group:
                    print(page_obj.page_url)
            if cfg.manual_mode:
                nam.display()
                if nam.original_citation is not None:
                    nam.original_citation.display()
                for page_obj in group:
                    if not _should_look_for_page_links(nam):
                        break
                    print(page_obj.page_url)
                    subprocess.check_call(["open", page_obj.page_url])
                    if getinput.yes_no(
                        "confirm? ", callbacks=nam.get_wrapped_adt_callbacks()
                    ):
                        yield from _maybe_add_bhl_page(nam, cfg, page_obj)
                        break


def get_candidate_bhl_pages(
    nam: Name, *, verbose: bool = False
) -> Iterable[bhl.PossiblePage]:
    if nam.page_described is None or nam.year is None:
        if verbose:
            print(f"{nam}: Skip because no page or year")
        return
    tags = list(nam.get_tags(nam.type_tags, TypeTag.AuthorityPageLink))
    known_pages = [
        parsed_url.page_id
        for tag in tags
        if isinstance((parsed_url := urlparse.parse_url(tag.url)), urlparse.BhlPage)
    ]
    year = nam.numeric_year()
    structured_cite = nam.get_type_tag(TypeTag.StructuredVerbatimCitation)
    contains_text: list[str] = []
    if nam.original_name is not None:
        contains_text.append(nam.original_name)
    if nam.corrected_original_name is not None:
        contains_text.append(nam.corrected_original_name)
    if not contains_text:
        return
    if nam.original_citation is not None:
        known_item_id = nam.original_citation.get_bhl_item_id()
    else:
        known_item_id = None
    if known_item_id is None:
        cg = nam.get_citation_group()
        if cg is None:
            if verbose:
                print(f"{nam}: Skip because no citation group")
            return
        title_ids = cg.get_bhl_title_ids()
        if not title_ids:
            if verbose:
                print(f"{nam}: Skip because citation group has no BHLBibliography tag")
            return
    else:
        title_ids = []

    pages = get_unique_page_text(nam.page_described)
    for page in pages:
        possible_pages = list(
            bhl.find_possible_pages(
                title_ids,
                year=year,
                start_page=page,
                volume=structured_cite.volume if structured_cite else None,
                contains_text=contains_text,
                known_item_id=known_item_id,
            )
        )
        possible_pages = [
            page for page in possible_pages if page.page_id not in known_pages
        ]
        confident_pages = [page for page in possible_pages if page.is_confident]
        if not confident_pages:
            if verbose:
                print(f"Reject for {nam} because no confident pages")
                for page_obj in possible_pages:
                    print(page_obj.page_url)
            right_volume_pages = [
                page for page in possible_pages if page.volume_matches
            ]
            if right_volume_pages:
                yield from right_volume_pages
            else:
                yield from possible_pages
        else:
            yield from confident_pages


def maybe_infer_page_from_other_name(
    *,
    cfg: LintConfig,
    other_nam: object,
    url: str,
    my_page: str,
    their_page: str,
    is_same_page: bool,
) -> int | None:
    parsed = urlparse.parse_url(url)
    if not isinstance(parsed, urlparse.BhlPage):
        if cfg.verbose:
            print(f"{other_nam}: {url} is not a BHL page URL")
        return None
    existing_page_id = parsed.page_id
    if my_page == their_page and is_same_page:
        return existing_page_id
    if not my_page.isnumeric() or not their_page.isnumeric():
        if cfg.verbose:
            print(f"{other_nam}: {my_page} or {their_page} is not numeric")
        return None
    diff = int(my_page) - int(their_page)
    page_metadata = bhl.get_page_metadata(existing_page_id)
    item_id = int(page_metadata["ItemID"])
    item_metadata = bhl.get_item_metadata(item_id)
    if item_metadata is None:
        if cfg.verbose:
            print(f"{other_nam}: no metadata for item {item_id}")
        return None
    page_mapping = bhl.get_page_id_to_index(item_id)
    existing_page_idx = page_mapping.get(existing_page_id)
    if existing_page_idx is None:
        if cfg.verbose:
            print(f"{other_nam}: no index for page {existing_page_id}")
        return None
    expected_page_idx = existing_page_idx + diff
    if not (0 <= expected_page_idx < len(item_metadata["Pages"])):
        if cfg.verbose:
            print(f"{other_nam}: {expected_page_idx} is out of range")
        return None
    inferred_page_id = item_metadata["Pages"][expected_page_idx]["PageID"]
    if diff > 0:
        start = existing_page_id
        end = inferred_page_id
    else:
        start = inferred_page_id
        end = existing_page_id
    if not bhl.is_contiguous_range(
        item_id, start, end, page_mapping, allow_unnumbered=False, verbose=cfg.verbose
    ):
        if cfg.verbose:
            print(
                f"{other_nam}: {existing_page_id} and {inferred_page_id} are not"
                " contiguous"
            )
        return None
    possible_pages = bhl.get_possible_pages(item_id, my_page)
    if inferred_page_id not in possible_pages:
        if cfg.verbose:
            print(
                f"{other_nam}: {inferred_page_id} not in possible pages {possible_pages}"
            )
        return None
    return inferred_page_id


@LINT.add("infer_page_from_other_names")
def infer_page_from_other_names(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if not _should_look_for_page_links(nam):
        if cfg.verbose:
            print(f"{nam}: not looking for BHL URL")
        return
    if nam.page_described is None:
        if cfg.verbose:
            print(f"{nam}: no page described")
        return
    if nam.original_citation is None:
        if cfg.verbose:
            print(f"{nam}: no original citation")
        return
    pages = get_unique_page_text(nam.page_described)
    for page in pages:
        other_new_names = [
            nam
            for nam in nam.original_citation.get_new_names().filter(
                Name.page_described.contains(page)
            )
            if nam.has_type_tag(TypeTag.AuthorityPageLink)
        ]
        if not other_new_names:
            if cfg.verbose:
                print(f"{nam}: {page}: no other new names")
            return
        inferred_pages: set[str] = set()
        for other_nam in other_new_names:
            for tag in other_nam.get_tags(
                other_nam.type_tags, TypeTag.AuthorityPageLink
            ):
                if tag.page == page:
                    inferred_pages.add(tag.url)
        if len(inferred_pages) != 1:
            if cfg.verbose:
                print(
                    f"{nam}: no single inferred page from other names ({inferred_pages})"
                )
            continue
        (url,) = inferred_pages
        tag = TypeTag.AuthorityPageLink(url=url, confirmed=True, page=page)
        if tag in nam.type_tags:
            if cfg.verbose:
                print(f"{nam}: already has {tag}")
            continue
        message = f"inferred URL {url} from other names (add {tag})"
        yield add_tag_issue(message, nam, tag, field="type_tags")


@LINT.add("infer_bhl_page_from_other_names", requires_network=True)
def infer_bhl_page_from_other_names(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if not _should_look_for_page_links(nam):
        if cfg.verbose:
            print(f"{nam}: not looking for BHL URL")
        return
    if nam.page_described is None:
        if cfg.verbose:
            print(f"{nam}: no page described")
        return
    if nam.original_citation is None:
        if cfg.verbose:
            print(f"{nam}: no original citation")
        return
    # Just so we don't waste effort adding incorrect pages before the link has been
    # confirmed on the article.
    if not nam.original_citation.has_bhl_link():
        if cfg.verbose:
            print(f"{nam}: original citation has no BHL link")
        return
    pages = get_unique_page_text(nam.page_described)
    for page in pages:
        other_new_names = [
            nam
            for nam in nam.original_citation.get_new_names().filter(
                Name.page_described.contains(page)
            )
            if nam.has_type_tag(TypeTag.AuthorityPageLink)
        ]
        if not other_new_names:
            if cfg.verbose:
                print(f"{nam}: no other new names")
            return
        inferred_pages: set[int] = set()
        for other_nam in other_new_names:
            for tag in other_nam.get_tags(
                other_nam.type_tags, TypeTag.AuthorityPageLink
            ):
                inferred_page = maybe_infer_page_from_other_name(
                    cfg=cfg,
                    other_nam=other_nam,
                    my_page=page,
                    their_page=tag.page,
                    is_same_page=tag.page == page,
                    url=tag.url,
                )
                if inferred_page is not None:
                    inferred_pages.add(inferred_page)
        if len(inferred_pages) != 1:
            if cfg.verbose:
                print(
                    f"{nam}: no single inferred page from other names ({inferred_pages})"
                )
            continue
        (inferred_page_id,) = inferred_pages
        tag = TypeTag.AuthorityPageLink(
            url=f"https://www.biodiversitylibrary.org/page/{inferred_page_id}",
            confirmed=True,
            page=page,
        )
        if tag in nam.type_tags:
            if cfg.verbose:
                print(f"{nam}: already has {tag}")
            continue
        message = f"inferred BHL page {inferred_page_id} from other names (add {tag})"
        yield add_tag_issue(message, nam, tag, field="type_tags")


@LINT.add("bhl_page_from_classification_entries", requires_network=True)
def infer_bhl_page_from_classification_entries(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    if not _should_look_for_page_links(nam):
        return
    ce = nam.get_mapped_classification_entry()
    if ce is None or ce.article != nam.original_citation:
        return
    new_tags = [
        TypeTag.AuthorityPageLink(url=tag.url, confirmed=True, page=tag.page)
        for tag in ce.tags
        if isinstance(tag, ClassificationEntryTag.PageLink)
    ]
    if not new_tags:
        return
    for tag in new_tags:
        yield add_tag_issue(
            f"inferred BHL page from classification entry {ce}: {tag}",
            nam,
            tag,
            field="type_tags",
        )


@LINT.add("bhl_page_from_article", requires_network=True)
def infer_bhl_page_from_article(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if not _should_look_for_page_links(nam):
        if cfg.verbose:
            print(f"{nam}: not looking for BHL URL")
        return
    if nam.page_described is None:
        if cfg.verbose:
            print(f"{nam}: no page described")
        return
    art = nam.original_citation
    if art is None or art.url is None:
        if cfg.verbose:
            print(f"{nam}: no original citation or URL")
        return
    pages_with_links = _get_pages_with_links(nam)
    for page_described in get_unique_page_text(nam.page_described):
        if page_described in pages_with_links:
            continue
        maybe_pair = infer_bhl_page_id(page_described, nam, art, cfg)
        if maybe_pair is not None:
            page_id, message = maybe_pair
            tag = TypeTag.AuthorityPageLink(
                url=f"https://www.biodiversitylibrary.org/page/{page_id}",
                confirmed=True,
                page=page_described,
            )
            message = f"inferred BHL page {page_id} from {message} (add {tag})"
            yield add_tag_issue(message, nam, tag, field="type_tags")


def infer_bhl_page_id(
    page: str, obj: object, art: Article, cfg: LintConfig
) -> tuple[int, str] | None:
    inferred = _infer_bhl_page_id_from_metadata(page, obj, art, cfg)
    if inferred is not None:
        return inferred
    if not page.isdecimal() or not cfg.is_resource_available(LintResource.NETWORK):
        return None
    item_ids = set(art.get_possible_bhl_item_ids())
    if not item_ids:
        return None
    links = [
        (tag.page, tag.url)
        for nam in art.get_new_names()
        for tag in nam.type_tags
        if isinstance(tag, TypeTag.AuthorityPageLink)
    ]
    links.extend(
        (tag.page, tag.url)
        for ce in art.get_classification_entries()
        for tag in ce.tags
        if isinstance(tag, ClassificationEntryTag.PageLink)
    )
    if art.start_page is not None and art.url is not None:
        links.append((art.start_page, art.url))
    anchors = [
        (int(number), parsed.page_id)
        for number, url in links
        if number.isdecimal()
        and isinstance(parsed := urlparse.parse_url(url), urlparse.BhlPage)
    ]
    candidates = {
        inferred_page
        for item_id in item_ids
        # This fallback fills missing numbering, not ambiguous numbered pages.
        if not bhl.get_possible_pages(item_id, page)
        and (inferred_page := bhl.infer_page_from_anchors(item_id, int(page), anchors))
        is not None
    }
    if len(candidates) == 1:
        (page_id,) = candidates
        return page_id, "interpolated between known page links"
    return None


def _infer_bhl_page_id_from_metadata(
    page: str, obj: object, art: Article, cfg: LintConfig
) -> tuple[int, str] | None:
    if art.id == 9306 and page.isnumeric():
        # {Marsupialia, Monotremata-British Museum.pdf}. BHL version doesn't have
        # page numbers but we can interpolate them.
        numeric_page = int(page)
        # page 1 through 20 = 37986429 - 37986410 (in decreasing order)
        if 1 <= numeric_page <= 20:
            page_id = 37986429 - (numeric_page - 1)
            return page_id, "interpolated from known range"
        # page 21 through 53 = 37986377 - 37986409
        if 21 <= numeric_page <= 53:
            page_id = 37986377 + (numeric_page - 21)
            return page_id, "interpolated from known range"
        # page 54 through 401 (end) = 37986448 - 37986795
        if 54 <= numeric_page <= 401:
            page_id = 37986448 + (numeric_page - 54)
            return page_id, "interpolated from known range"
    elif art.id == 35530 and page.isnumeric():
        # {Mammalia-in BMNH (Gray 1843).pdf}. BHL has wrong page numbers.
        # The copy in BHL always has a pair of odd, even pages (e.g. 1 and 2),
        # then two blank pages. In addition, there's an extra pair of pages
        # after page 6.
        numeric_page = int(page)
        # page 1 = 53729558
        # page 6 = 53729567
        # page 7 = 53729572
        # page 214 = 53729985
        if 1 <= numeric_page <= 6:
            is_odd = (numeric_page % 2) == 1
            if is_odd:
                page_id = 53729558 + ((numeric_page - 1) // 2) * 4
            else:
                page_id = 53729558 + ((numeric_page - 2) // 2) * 4 + 1
            return page_id, "corrected from known BHL page numbering error"
        if 7 <= numeric_page <= 214:
            is_odd = (numeric_page % 2) == 1
            if is_odd:
                page_id = 53729560 + ((numeric_page - 1) // 2) * 4
            else:
                page_id = 53729560 + ((numeric_page - 2) // 2) * 4 + 1
            return page_id, "corrected from known BHL page numbering error"
    if not cfg.is_resource_available(LintResource.NETWORK):
        return None
    if art.url is None:
        return None
    match urlparse.parse_url(art.url):
        case urlparse.BhlPage(start_page_id):
            if art.start_page is not None and art.start_page == art.end_page == page:
                return start_page_id, "article citation where start_page == end_page"
            # TODO: where the link is a BhlPage but the article is a book and so there's no
            # end_page, we can try harder.
            return _infer_bhl_page_from_article_page(obj, art, cfg, start_page_id, page)
        case urlparse.BhlItem(item_id):
            pages = bhl.get_possible_pages(item_id, page)
            if len(pages) != 1:
                if cfg.verbose:
                    print(f"{obj}: no single page for {item_id} {pages}")
                return None
            return pages[0], "item citation"
        case urlparse.BhlPart(part_id):
            pages = bhl.get_possible_pages_from_part(part_id, page)
            if len(pages) != 1:
                if cfg.verbose:
                    print(f"{obj}: no single page for {part_id} {pages} (using {page})")
                return None
            return pages[0], "part citation"
    return None


def _infer_bhl_page_from_article_page(
    obj: object, art: Article, cfg: LintConfig, start_page_id: int, page_described: str
) -> tuple[int, str] | None:
    if art.end_page is None:
        if cfg.verbose:
            print(f"{obj}: no citation")
        return None
    start_page_metadata = bhl.get_page_metadata(start_page_id)
    if not start_page_metadata:
        if cfg.verbose:
            print(f"{obj}: no metadata for start page")
        return None
    item_id = int(start_page_metadata["ItemID"])
    possible_end_pages = bhl.get_possible_pages(item_id, art.end_page)
    page_mapping = bhl.get_page_id_to_index(item_id)
    possible_end_pages = [
        page
        for page in possible_end_pages
        if bhl.is_contiguous_range(item_id, start_page_id, page, page_mapping)
    ]
    possible_pages = bhl.get_possible_pages(item_id, page_described)
    possible_pages = [
        page
        for page in possible_pages
        if bhl.is_contiguous_range(item_id, start_page_id, page, page_mapping)
    ]
    if len(possible_pages) != len(possible_end_pages):
        if cfg.verbose:
            print(
                f"{obj}: different number of possible description and end pages"
                f" {possible_pages} {possible_end_pages}"
            )
        return None
    if not possible_pages:
        if cfg.verbose:
            print(f"{obj}: no possible pages in item {item_id}")
        return None
    for possible_page, possible_end_page in zip(
        possible_pages, possible_end_pages, strict=True
    ):
        if not bhl.is_contiguous_range(
            item_id, start_page_id, possible_page, page_mapping
        ):
            if cfg.verbose:
                print(
                    f"{obj}: page {possible_page} is not contiguous with start page {start_page_id}"
                )
            continue
        if not bhl.is_contiguous_range(
            item_id, possible_page, possible_end_page, page_mapping
        ):
            if cfg.verbose:
                print(
                    f"{obj}: end page {possible_end_page} is not contiguous with {possible_page}"
                )
            continue

        return possible_page, "page citation"
    return None
