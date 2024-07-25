"""Lint steps for classification entries."""

import itertools
import re
import subprocess
from collections import Counter, defaultdict
from collections.abc import Container, Iterable
from dataclasses import dataclass, field
from itertools import takewhile

from taxonomy import getinput, urlparse
from taxonomy.apis import bhl
from taxonomy.db import helpers
from taxonomy.db.constants import Group, NomenclatureStatus, Rank
from taxonomy.db.models.article.article import Article, ArticleTag
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.name import Name, TypeTag
from taxonomy.db.models.name.lint import (
    extract_pages,
    infer_bhl_page_id,
    maybe_infer_page_from_other_name,
    name_combination_name_sort_key,
)
from taxonomy.db.models.name.name import clean_original_name
from taxonomy.db.models.taxon import Taxon

from .ce import ClassificationEntry, ClassificationEntryTag


def remove_unused_ignores(ce: ClassificationEntry, unused: Container[str]) -> None:
    new_tags = []
    for tag in ce.tags:
        if (
            isinstance(tag, ClassificationEntryTag.IgnoreLintClassificationEntry)
            and tag.label in unused
        ):
            print(f"{ce}: removing unused IgnoreLint tag: {tag}")
        else:
            new_tags.append(tag)
    ce.tags = new_tags  # type: ignore[assignment]


def get_ignores(ce: ClassificationEntry) -> Iterable[IgnoreLint]:
    return ce.get_tags(ce.tags, ClassificationEntryTag.IgnoreLintClassificationEntry)


LINT = Lint(ClassificationEntry, get_ignores, remove_unused_ignores)


@LINT.add("rank")
def check_rank(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.rank is Rank.other and not any(
        ce.get_tags(ce.tags, ClassificationEntryTag.TextualRank)
    ):
        yield "missing TextualRank tag"


@LINT.add("tags")
def check_tags(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    counts = Counter(type(tag) for tag in ce.tags)
    if counts[ClassificationEntryTag.TextualRank] > 1:
        yield "multiple TextualRank tags"
    elif counts[ClassificationEntryTag.TextualRank] == 1 and ce.rank is not Rank.other:
        yield "unexpected TextualRank tag"
    if counts[ClassificationEntryTag.CorrectedName] > 1:
        yield "multiple CorrectedName tags"
    elif (
        counts[ClassificationEntryTag.CorrectedName] == 1
        and ce.get_corrected_name_without_tags() == ce.get_corrected_name()
    ):
        yield "unnecessary CorrectedName tag"


@LINT.add("parent")
def check_parent(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.parent is not None and not articles_match(ce.article, ce.parent.article):
        yield "parent from different article"


def articles_match(child_art: Article, parent_art: Article) -> bool:
    if child_art == parent_art:
        return True
    if child_art.parent is not None:
        if child_art.parent == parent_art:
            return True
        if child_art.parent == parent_art.parent:
            return True
    return False


@LINT.add("move_to_child")
def check_move_to_child(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.parent is None:
        return
    if ce.article == ce.parent.article.parent:
        message = f"move to child citation {ce.parent.article}"
        if cfg.autofix:
            print(f"{ce}: {message}")
            ce.article = ce.parent.article
        else:
            yield message


@LINT.add("missing_mapped_name")
def check_missing_mapped_name(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if ce.rank is Rank.informal:
        return
    if ce.mapped_name is not None:
        return
    if ce.is_synonym_without_full_name():
        return
    candidates = list(get_filtered_possible_mapped_names(ce))
    if len(candidates) == 1:
        inferred = candidates[0]
        message = f"inferred mapped_name: {inferred}"
        if cfg.autofix:
            print(f"{ce}: {message}")
            ce.mapped_name = inferred
        else:
            yield message
    elif cfg.verbose and candidates:
        print(f"{ce}: missing mapped_name (candidates: {candidates})")


@LINT.add("mapped_name")
def check_mapped_name(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.mapped_name is not None:
        corrected_name = ce.get_corrected_name()
        if corrected_name is None:
            return
        match ce.mapped_name.group:
            case Group.high | Group.genus:
                # root name and corrected original name are different in the case of justified emendations
                if corrected_name not in (
                    ce.mapped_name.root_name,
                    ce.mapped_name.corrected_original_name,
                ):
                    yield f"mapped_name root_name does not match: {corrected_name} vs {ce.mapped_name.root_name}"
            case Group.family:
                allowed = [
                    ce.mapped_name.original_name,
                    ce.mapped_name.corrected_original_name,
                ]
                if ce.mapped_name.original_name is not None:
                    allowed.append(clean_original_name(ce.mapped_name.original_name))
                if corrected_name not in allowed and ce.name not in allowed:
                    yield f"mapped_name original_name does not match: {corrected_name} vs {ce.mapped_name.corrected_original_name}"
                    if cfg.interactive and getinput.yes_no(
                        f"Add new synonym for {ce}?"
                    ):
                        new_name = ce.add_family_group_synonym(ce.mapped_name.type)
                        if new_name is not None:
                            ce.mapped_name = new_name
            case Group.species:
                root_name = corrected_name.split()[-1]
                if root_name not in ce.mapped_name.get_root_name_forms() and not (
                    ce.mapped_name.nomenclature_status is NomenclatureStatus.as_emended
                    and ce.mapped_name.corrected_original_name is not None
                    and root_name == ce.mapped_name.corrected_original_name.split()[-1]
                ):
                    yield f"mapped_name root_name does not match: {root_name} vs {ce.mapped_name.root_name}"
                    if cfg.interactive and getinput.yes_no(
                        f"Add incorrect subsequent spelling for {ce}?"
                    ):
                        new_name = ce.add_incorrect_subsequent_spelling(ce.mapped_name)
                        if new_name is not None:
                            ce.mapped_name = new_name
                if (
                    ce.mapped_name.corrected_original_name != corrected_name
                    and ce.rank is not Rank.synonym
                ):
                    yield f"mapped_name corrected_original_name does not match: {corrected_name} vs {ce.mapped_name.corrected_original_name}"
                    mapped_root = ce.mapped_name.resolve_variant()
                    alternatives = [
                        nam
                        for nam in Name.select_valid().filter(
                            Name.taxon == ce.mapped_name.taxon,
                            Name.corrected_original_name == corrected_name,
                        )
                        if nam.resolve_variant() == mapped_root
                    ]
                    if alternatives:
                        alternatives = sorted(
                            alternatives, key=name_combination_name_sort_key
                        )
                        new_name = alternatives[0]
                        message = f"mapped_name corrected_original_name does not match; change to {new_name}"
                        if cfg.autofix:
                            print(f"{ce}: {message}")
                            ce.mapped_name = new_name
                        else:
                            yield message
    elif ce.rank is not Rank.informal and not ce.is_synonym_without_full_name():
        yield "missing mapped_name"


def infer_mapped_name(ce: ClassificationEntry) -> Name | None:
    nams = list(get_filtered_possible_mapped_names(ce))
    if len(nams) == 1:
        return nams[0]
    return None


def get_genera_with_shared_species(genera: Iterable[Name]) -> Iterable[Taxon]:
    taxa = set()
    for genus in genera:
        try:
            taxa.add(genus.taxon.parent_of_rank(Rank.genus))
        except ValueError:
            pass
        for nam in Name.select_valid().filter(
            Name.group == Group.species, Name.original_parent == genus
        ):
            try:
                taxa.add(nam.taxon.parent_of_rank(Rank.genus))
            except ValueError:
                pass
    return taxa


def get_filtered_possible_mapped_names(ce: ClassificationEntry) -> Iterable[Name]:
    seen_names = set()
    candidates = []
    for nam_or_pair in get_possible_mapped_names(ce):
        if isinstance(nam_or_pair, tuple):
            nam, metadata = nam_or_pair
        else:
            nam = nam_or_pair
            metadata = CandidateMetadata()
        if nam in seen_names:
            continue
        seen_names.add(nam)
        candidates.append(CandidateName(ce, nam, metadata))
    if not candidates:
        return []
    candidates = sorted(candidates, key=lambda c: c.get_score())
    best_score = candidates[0].get_score()
    matching = takewhile(lambda c: c.get_score() == best_score, candidates)
    return [c.name for c in matching]


@dataclass
class CandidateMetadata:
    is_direct_match: bool = False
    is_shared_genus: bool = False
    is_sister_genus: bool = False


@dataclass
class CandidateName:
    ce: ClassificationEntry
    name: Name
    metadata: CandidateMetadata
    _score: int | None = field(init=False, default=None)

    def get_score(self) -> int:
        if self._score is not None:
            return self._score
        score = 0
        corrected_name = self.ce.get_corrected_name()
        if corrected_name is None:
            corrected_name = self.name
        if self.name.corrected_original_name != corrected_name:
            score += 10
        associated_taxa = Taxon.select_valid().filter(Taxon.base_name == self.name)
        if not any(t.valid_name == corrected_name for t in associated_taxa):
            score += 2
        if self.ce.year is not None and str(self.name.numeric_year()) != self.ce.year:
            score += 5
        if (
            self.name.group is not Group.family
            and self.name.get_date_object() > self.ce.article.get_date_object()
        ):
            score += 5
        if (
            self.ce.authority is not None
            and self.name.taxonomic_authority() != self.ce.authority
        ):
            score += 5
        if self.name.nomenclature_status in (
            NomenclatureStatus.subsequent_usage,
            NomenclatureStatus.name_combination,
        ):
            score += 1
        if self.name.nomenclature_status in (
            NomenclatureStatus.incorrect_subsequent_spelling,
            NomenclatureStatus.variant,
            NomenclatureStatus.unjustified_emendation,
        ):
            score += 1
        if not self.name.nomenclature_status.can_preoccupy():
            score += 1
        if self.name.group is Group.species:
            genus_name, *_, root_name = corrected_name.split()
            if root_name != self.name.root_name:
                score += 1
            if (
                self.name.original_parent is None
                or self.name.original_parent.corrected_original_name != genus_name
            ):
                score += 1
            if not self.metadata.is_direct_match and not self.metadata.is_shared_genus:
                score += 1
            name_genus_name, *_ = self.name.taxon.valid_name.split()
            if genus_name != name_genus_name:
                score += 1

        self._score = score
        return score


def yield_family_names(possibilities: Iterable[Name]) -> Iterable[Name]:
    by_type: dict[Name, list[Name]] = defaultdict(list)
    for name in possibilities:
        if name.type is None:
            yield name
        else:
            by_type[name.type].append(name)
    for names in by_type.values():
        yield min(names, key=lambda n: n.get_date_object())


def get_possible_mapped_names(
    ce: ClassificationEntry,
) -> Iterable[Name | tuple[Name, CandidateMetadata]]:
    group = ce.get_group()
    corrected_name = ce.get_corrected_name()
    if corrected_name is None:
        return
    if group is Group.high:
        yield from Name.select_valid().filter(
            Name.group == Group.high, Name.corrected_original_name == corrected_name
        )
    elif group is Group.family:
        possibilies = Name.select_valid().filter(
            Name.group == Group.family, Name.original_name == ce.name
        )
        names = list(yield_family_names(possibilies))
        if names:
            yield from names
        else:
            root_name = helpers.strip_standard_suffixes(ce.name)
            possibilies = Name.select_valid().filter(
                Name.group == Group.family, Name.root_name == root_name
            )
            yield from yield_family_names(possibilies)
    elif group is Group.genus:
        yield from Name.select_valid().filter(
            Name.group == Group.genus, Name.corrected_original_name == corrected_name
        )
    elif group is Group.species:
        count = 0
        for nam in Name.select_valid().filter(
            Name.group == Group.species, Name.corrected_original_name == corrected_name
        ):
            count += 1
            yield nam, CandidateMetadata(is_direct_match=True)
        for taxon in Taxon.select_valid().filter(Taxon.valid_name == corrected_name):
            count += 1
            yield taxon.base_name, CandidateMetadata(is_direct_match=True)
        if count == 0:
            genus_name, *_, root_name = corrected_name.split()
            normalized_root_name = helpers.normalize_root_name_for_homonymy(root_name)
            genus_candidates = Name.select_valid().filter(
                Name.group == Group.genus, Name.corrected_original_name == genus_name
            )
            shared_genera = list(get_genera_with_shared_species(genus_candidates))
            for genus in shared_genera:
                for nam in genus.all_names():
                    if (
                        nam.group == Group.species
                        and nam.get_normalized_root_name_for_homonymy()
                        == normalized_root_name
                    ):
                        count += 1
                        yield nam, CandidateMetadata(is_shared_genus=True)
            if count == 0:
                sister_genera = {
                    genus
                    for sister in shared_genera
                    if sister.parent is not None
                    for genus in sister.parent.children_of_rank(Rank.genus)
                }
                for genus in sister_genera:
                    if genus in shared_genera:
                        continue
                    for nam in genus.all_names():
                        if (
                            nam.group == Group.species
                            and nam.get_normalized_root_name_for_homonymy()
                            == normalized_root_name
                        ):
                            count += 1
                            yield nam, CandidateMetadata(is_sister_genus=True)


@LINT.add("corrected_name")
def check_corrected_name(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    corrected_name = ce.get_corrected_name()
    if corrected_name is None and ce.rank is not Rank.informal:
        yield "cannot infer corrected name; add CorrectedName tag"
    if corrected_name is not None:
        group = ce.get_group()
        match group:
            case Group.species:
                if ce.rank is Rank.synonym and re.fullmatch(r"[a-z]+", corrected_name):
                    return
                if not re.fullmatch(r"[A-Z][a-z]+( [a-z]+){1,3}", corrected_name):
                    yield f"incorrect species name format: {corrected_name}"
            case _:
                if not re.fullmatch(r"[A-Z][a-z]+", corrected_name):
                    yield f"incorrect name format: {corrected_name}"


@LINT.add("authority_page_link")
def check_must_have_authority_page_link(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if ce.has_tag(ClassificationEntryTag.PageLink):
        return
    if not ce.article.has_bhl_link_with_pages():
        return
    yield "must have page link"


@LINT.add("check_bhl_page")
def check_bhl_page(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    wrong_bhl_pages = ce.article.has_tag(ArticleTag.BHLWrongPageNumbers)
    for tag in ce.get_tags(ce.tags, ClassificationEntryTag.PageLink):
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
                    yield f"page number {tag.page} matches BHL data for {tag.url}, but {ce.article} is marked as having wrong page numbers"
        if ce.article.url is None:
            yield f"name has BHL page, but original citation has no URL: {ce.article}"
            continue
        parsed_url = urlparse.parse_url(ce.article.url)
        if not isinstance(parsed_url, urlparse.BhlUrl):
            yield f"name has BHL page, but citation has non-BHL URL {ce.article.url}"
            continue
        yield from _check_bhl_item_matches(ce, tag)
        yield from _check_bhl_bibliography_matches(ce, tag)


def _check_bhl_item_matches(
    ce: ClassificationEntry,
    tag: ClassificationEntryTag.PageLink,  # type:ignore[name-defined]
) -> Iterable[str]:
    item_id = bhl.get_bhl_item_from_url(tag.url)
    if item_id is None:
        yield f"cannot find BHL item for {tag.url}"
        return
    if ce.article.url is None:
        return
    citation_item_ids = list(ce.article.get_possible_bhl_item_ids())
    if not citation_item_ids:
        return
    if item_id not in citation_item_ids:
        yield f"BHL item mismatch: {item_id} (name) not in {citation_item_ids} (citation)"


def _check_bhl_bibliography_matches(
    ce: ClassificationEntry,
    tag: ClassificationEntryTag.PageLink,  # type:ignore[name-defined]
) -> Iterable[str]:
    bibliography_id = bhl.get_bhl_bibliography_from_url(tag.url)
    if bibliography_id is None:
        yield f"cannot find BHL bibliography for {tag.url}"
        return
    if ce.article.url is None:
        return
    citation_biblio_ids = list(ce.article.get_possible_bhl_bibliography_ids())
    if bibliography_id not in citation_biblio_ids:
        yield f"BHL item mismatch: {bibliography_id} (name) not in {citation_biblio_ids} (citation)"


def _should_look_for_page_links(ce: ClassificationEntry) -> bool:
    if ce.page is None:
        return False
    if not ce.article.has_bhl_link():
        return False
    pages = list(extract_pages(ce.page))
    tags = list(ce.get_tags(ce.tags, ClassificationEntryTag.PageLink))
    if len(tags) >= len(pages):
        return False
    return True


def _maybe_add_bhl_page(
    ce: ClassificationEntry, cfg: LintConfig, page_obj: bhl.PossiblePage
) -> Iterable[str]:
    message = f"inferred BHL page {page_obj}"
    if cfg.autofix:
        print(f"{ce}: {message}")
        tag = ClassificationEntryTag.PageLink(
            url=page_obj.page_url, page=str(page_obj.page_number)
        )
        ce.add_tag(tag)
    else:
        yield message
    print(page_obj.page_url)


@LINT.add("infer_bhl_page")
def infer_bhl_page(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if not _should_look_for_page_links(ce):
        if cfg.verbose:
            print(f"{ce}: Skip because no page or enough tags")
        return
    confident_candidates = [
        page
        for page in get_candidate_bhl_pages(ce, verbose=cfg.verbose)
        if page.is_confident
    ]
    for _, group_iter in itertools.groupby(
        confident_candidates, lambda page: page.page_number
    ):
        group = list(group_iter)
        if len(group) == 1:
            yield from _maybe_add_bhl_page(ce, cfg, group[0])
        else:
            if cfg.verbose or cfg.manual_mode:
                print(f"Reject for {ce} because multiple pages with name:")
                for page_obj in group:
                    print(page_obj.page_url)
            if cfg.manual_mode:
                ce.display()
                ce.article.display()
                for page_obj in group:
                    if not _should_look_for_page_links(ce):
                        break
                    print(page_obj.page_url)
                    subprocess.check_call(["open", page_obj.page_url])
                    if getinput.yes_no("confirm? ", callbacks=ce.get_adt_callbacks()):
                        yield from _maybe_add_bhl_page(ce, cfg, page_obj)
                        break


def get_candidate_bhl_pages(
    ce: ClassificationEntry, *, verbose: bool = False
) -> Iterable[bhl.PossiblePage]:
    tags = list(ce.get_tags(ce.tags, ClassificationEntryTag.PageLink))
    known_pages = [
        parsed_url.page_id
        for tag in tags
        if isinstance((parsed_url := urlparse.parse_url(tag.url)), urlparse.BhlPage)
    ]
    year = ce.article.numeric_year()
    contains_text: list[str] = [ce.name]
    known_item_id = ce.article.get_bhl_item_id()
    if known_item_id is None:
        if verbose:
            print(f"{ce}: Skip because no BHL item on article")
        return

    pages = list(extract_pages(ce.page))
    for page in pages:
        possible_pages = list(
            bhl.find_possible_pages(
                [],
                year=year,
                start_page=page,
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
                print(f"Reject for {ce} because no confident pages")
                for page_obj in possible_pages:
                    print(page_obj.page_url)
            yield from possible_pages
        else:
            yield from confident_pages


@LINT.add("infer_bhl_page_from_mapped_name")
def infer_bhl_page_from_mapped_name(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if not _should_look_for_page_links(ce):
        return
    if ce.mapped_name is None:
        return
    if ce.article != ce.mapped_name.original_citation:
        return
    new_tags = [
        ClassificationEntryTag.PageLink(url=tag.url, page=tag.page)
        for tag in ce.mapped_name.type_tags
        if isinstance(tag, TypeTag.AuthorityPageLink)
    ]
    if not new_tags:
        return
    message = f"inferred BHL page from mapped name {ce.mapped_name}: {new_tags}"
    if cfg.autofix:
        print(f"{ce}: {message}")
        for tag in new_tags:
            ce.add_tag(tag)
    else:
        yield message


@LINT.add("infer_bhl_page_from_other_names")
def infer_bhl_page_from_other_names(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if not _should_look_for_page_links(ce):
        if cfg.verbose:
            print(f"{ce}: not looking for BHL URL")
        return
    if ce.page is None:
        if cfg.verbose:
            print(f"{ce}: no page")
        return
    # Just so we don't waste effort adding incorrect pages before the link has been
    # confirmed on the article.
    if not ce.article.has_bhl_link():
        if cfg.verbose:
            print(f"{ce}: original citation has no BHL link")
        return
    pages = list(extract_pages(ce.page))
    if len(pages) != 1:
        if cfg.verbose:
            print(f"{ce}: no single page from {ce.page}")
        return
    (page,) = pages
    other_names = [
        ce
        for ce in ce.article.get_classification_entries()
        if ce.has_tag(ClassificationEntryTag.PageLink)
    ]
    if not other_names:
        if cfg.verbose:
            print(f"{ce}: no other new names")
        return
    inferred_pages: set[int] = set()
    for other_nam in other_names:
        for tag in other_nam.get_tags(other_nam.tags, ClassificationEntryTag.PageLink):
            inferred_page_id = maybe_infer_page_from_other_name(
                cfg=cfg,
                other_nam=other_nam,
                url=tag.url,
                my_page=page,
                their_page=tag.page,
                is_same_page=ce.page == other_nam.page,
            )
            if inferred_page_id is not None:
                inferred_pages.add(inferred_page_id)
    if len(inferred_pages) != 1:
        if cfg.verbose:
            print(f"{ce}: no single inferred page from other names ({inferred_pages})")
        return
    (inferred_page_id,) = inferred_pages
    tag = ClassificationEntryTag.PageLink(
        url=f"https://www.biodiversitylibrary.org/page/{inferred_page_id}", page=page
    )
    message = f"inferred BHL page {inferred_page_id} from other names (add {tag})"
    if cfg.autofix:
        print(f"{ce}: {message}")
        ce.add_tag(tag)
    else:
        yield message


@LINT.add("bhl_page_from_article")
def infer_bhl_page_from_article(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if not _should_look_for_page_links(ce):
        if cfg.verbose:
            print(f"{ce}: not looking for BHL URL")
        return
    if ce.page is None:
        if cfg.verbose:
            print(f"{ce}: no page described")
        return
    art = ce.article
    if art is None or art.url is None:
        if cfg.verbose:
            print(f"{ce}: no original citation or URL")
        return
    for page_described in extract_pages(ce.page):
        if any(
            isinstance(tag, ClassificationEntryTag.PageLink)
            and tag.page == page_described
            for tag in ce.tags
        ):
            continue
        maybe_pair = infer_bhl_page_id(page_described, ce, art, cfg)
        if maybe_pair is not None:
            page_id, message = maybe_pair
            tag = ClassificationEntryTag.PageLink(
                url=f"https://www.biodiversitylibrary.org/page/{page_id}",
                page=page_described,
            )
            message = f"inferred BHL page {page_id} from {message} (add {tag})"
            if cfg.autofix:
                print(f"{ce}: {message}")
                ce.add_tag(tag)
            else:
                yield message


@LINT.add("infer_page_from_name")
def infer_page_from_name(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.page is not None:
        return
    if ce.mapped_name is None:
        return
    if ce.mapped_name.page_described is None:
        return
    if ce.mapped_name.original_citation != ce.article:
        return
    message = f"inferred page from mapped name: {ce.mapped_name.page_described}"
    if cfg.autofix:
        print(f"{ce}: {message}")
        ce.page = ce.mapped_name.page_described
    else:
        yield message
