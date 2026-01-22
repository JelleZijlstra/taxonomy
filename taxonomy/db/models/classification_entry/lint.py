"""Lint steps for classification entries."""

import itertools
import re
import subprocess
from collections import Counter, defaultdict
from collections.abc import Collection, Container, Iterable
from dataclasses import dataclass, field
from itertools import takewhile
from typing import assert_never

from taxonomy import getinput, urlparse
from taxonomy.apis import bhl
from taxonomy.apis.zoobank import clean_lsid, is_valid_lsid
from taxonomy.db import helpers, models
from taxonomy.db.constants import SYNONYM_RANKS, Group, NomenclatureStatus, Rank
from taxonomy.db.models.article.article import Article, ArticleTag
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.name import Name, TypeTag
from taxonomy.db.models.name.lint import (
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
    if ce.rank.needs_textual_rank and not any(
        ce.get_tags(ce.tags, ClassificationEntryTag.TextualRank)
    ):
        yield "missing TextualRank tag"
    if ce.rank is Rank.synonym:
        group = ce.get_group()
        new_rank = helpers.GROUP_TO_SYNONYM_RANK[group]
        message = f"change rank to {new_rank!r}"
        if cfg.autofix:
            print(f"{ce}: {message}")
            ce.rank = new_rank
        else:
            yield message


@LINT.add("tags")
def check_tags(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    counts = Counter(type(tag) for tag in ce.tags)
    if counts[ClassificationEntryTag.TextualRank] > 1:
        yield "multiple TextualRank tags"
    elif (
        counts[ClassificationEntryTag.TextualRank] == 1
        and not ce.rank.needs_textual_rank
    ):
        yield "unexpected TextualRank tag"
    if counts[ClassificationEntryTag.CorrectedName] > 1:
        yield "multiple CorrectedName tags"
    elif (
        counts[ClassificationEntryTag.CorrectedName] == 1
        and ce.get_corrected_name_without_tags() == ce.get_corrected_name()
    ):
        yield "unnecessary CorrectedName tag"
    if counts[ClassificationEntryTag.ReferencedUsage] > 1:
        yield "multiple ReferencedUsage tags"
    new_tags = []
    for tag in ce.tags:
        if isinstance(tag, ClassificationEntryTag.ReferencedUsage):
            if (
                ce.mapped_name is not None
                and tag.ce == ce.mapped_name.get_mapped_classification_entry()
            ):
                yield "removing redundant ReferencedUsage tag"
            else:
                if ce.mapped_name is not None and tag.ce.mapped_name is not None:
                    referenced = tag.ce.mapped_name.resolve_variant(
                        misidentification=True
                    )
                    mapped = ce.mapped_name.resolve_variant(misidentification=True)
                    if referenced != mapped:
                        yield f"ReferencedUsage tag {tag} (resolving to {referenced}) does not match mapped_name {mapped}"
                new_tags.append(tag)
        elif isinstance(tag, ClassificationEntryTag.PageLink):
            new_url = yield from models.name.lint.check_page_link(
                tag_url=tag.url, tag_page=tag.page, page_described=ce.page
            )
            new_tags.append(
                ClassificationEntryTag.PageLink(
                    url=new_url, page=tag.page if tag.page is not None else "NA"
                )
            )
        elif isinstance(tag, ClassificationEntryTag.CorrectedName):
            if ce.get_corrected_name_without_tags() == tag.text:
                yield "removing redundant CorrectedName tag"
            else:
                new_tags.append(tag)

        elif isinstance(tag, ClassificationEntryTag.LSIDCE):
            lsid = clean_lsid(tag.text)
            tag = ClassificationEntryTag.LSIDCE(lsid)
            if not is_valid_lsid(lsid):
                yield f"invalid LSID {lsid}"
            new_tags.append(tag)

        else:
            new_tags.append(tag)
    new_tags_tuple = tuple(sorted(set(new_tags)))
    if ce.tags != new_tags_tuple:
        getinput.print_diff(ce.tags, new_tags_tuple)
        message = "change tags"
        if cfg.autofix:
            print(f"{ce}: {message}")
            ce.tags = new_tags_tuple  # type: ignore[assignment]
        else:
            yield message


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
    if ce.mapped_name is not None:
        return
    if not must_have_mapped_name(ce):
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


def must_have_mapped_name(ce: ClassificationEntry) -> bool:
    if ce.rank is Rank.informal:
        return False
    if ClassificationEntryTag.Informal in ce.tags:
        return False
    return True


def _expand_candidates(candidates: Iterable[Name]) -> Iterable[Name]:
    for nam in candidates:
        yield nam
        for tag in (
            models.name.NameTag.UnavailableVersionOf,
            models.name.NameTag.SubsequentUsageOf,
        ):
            target = nam.get_tag_target(tag)
            if target is not None:
                yield target


@LINT.add("mapped_name_inference")
def check_mapped_name_inference(
    ce: ClassificationEntry, cfg: LintConfig, *, conservative: bool = False
) -> Iterable[str]:
    if ce.mapped_name is None:
        return
    if not conservative:
        candidates = list(
            get_filtered_possible_mapped_names(ce, resolve_variants=False)
        )
    else:
        candidates = []
        for nam_or_pair in get_possible_mapped_names(ce):
            if isinstance(nam_or_pair, tuple):
                nam, _ = nam_or_pair
            else:
                nam = nam_or_pair
            candidates.append(nam)
    candidates = list(_expand_candidates(candidates))
    if candidates and ce.mapped_name not in candidates:
        yield f"mapped_name {ce.mapped_name} not in inferred candidates {candidates}"
        if ce.rank.is_synonym and not LINT.is_ignoring_lint(
            ce, "mapped_name_inference"
        ):
            filtered_cands = list(
                get_filtered_possible_mapped_names(ce, resolve_variants=False)
            )
            if len(filtered_cands) == 1:
                best_cand = filtered_cands[0]
                if best_cand.resolve_variant() == ce.mapped_name.resolve_variant():
                    message = f"change mapped_name to inferred candidate {best_cand}"
                    if cfg.autofix:
                        print(f"{ce}: {message}")
                        ce.mapped_name = best_cand
                    else:
                        yield message


def get_allowed_family_group_names(nam: Name) -> Container[str]:
    allowed = []
    if nam.original_name is not None:
        allowed.append(nam.original_name)
        allowed.append(clean_original_name(nam.original_name))
    if nam.corrected_original_name is not None:
        allowed.append(nam.corrected_original_name)
    return allowed


@LINT.add("predates_mapped_name")
def check_predates_mapped_name(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if ce.mapped_name is None:
        return
    if ce.rank.is_synonym:
        return  # ignore synonyms for now
    if (ce.article.is_unpublished(), ce.article.get_date_object()) < (
        (
            ce.mapped_name.original_citation.is_unpublished()
            if ce.mapped_name.original_citation is not None
            else False
        ),
        ce.mapped_name.get_date_object(),
    ) and ce.article.get_date_object() < ce.mapped_name.get_date_object():
        yield f"predates mapped name {ce.mapped_name}"


@LINT.add("mapped_name")
def check_mapped_name(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.mapped_name is not None:
        if (
            ce.page is None
            and ce.mapped_name.original_citation == ce.article
            and ce.mapped_name.page_described is not None
        ):
            yield "mapped_name has page, but name has no page"
            if cfg.autofix:
                print(f"{ce}: adding page {ce.mapped_name.page_described}")
                ce.page = ce.mapped_name.page_described
        # Don't worry about synonyms; if the source puts them in the "high" bucket but we decide
        # it's actually a family-group name, it's still correctly marked "synonym_high" in the source.
        if not ce.rank.is_synonym:
            if ce.get_group() is not ce.mapped_name.group:
                yield f"mapped_name group does not match: {ce.get_group()!r} vs {ce.mapped_name.group!r}"
            if target := ce.mapped_name.get_tag_target(
                models.name.NameTag.UnavailableVersionOf
            ):
                if (
                    ce.article.year >= target.year
                    and ce.article != ce.mapped_name.original_citation
                ):
                    message = (
                        f"mapped to unavailable version of {target}, but postdates it"
                    )
                    if cfg.autofix:
                        print(f"{ce}: {message}")
                        ce.mapped_name = target
                    else:
                        yield message

        corrected_name = ce.get_corrected_name()
        if corrected_name is None:
            return
        match ce.mapped_name.group:
            case Group.high | Group.genus:
                # root name and corrected original name are different in the case of justified emendations
                if corrected_name != ce.mapped_name.corrected_original_name:
                    yield f"mapped_name corrected_original_name does not match: {corrected_name} (CE) vs {ce.mapped_name.root_name} (name)"
                    if corrected_name == ce.mapped_name.root_name:
                        # Justified emendation; replace with emended version
                        emended_version = ce.mapped_name.get_tag_target(
                            models.name.NameTag.AsEmendedBy
                        )
                        if emended_version is not None:
                            message = f"mapped_name corrected_original_name does not match; change to emended version {emended_version}"
                            if cfg.autofix:
                                print(f"{ce}: {message}")
                                ce.mapped_name = emended_version
                            else:
                                yield message
            case Group.family:
                if corrected_name != ce.mapped_name.corrected_original_name:
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
                    ce.mapped_name.corrected_original_name is not None
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
                    and not ce.rank.is_synonym
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
    elif must_have_mapped_name(ce):
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


def get_filtered_possible_mapped_names(
    ce: ClassificationEntry, *, resolve_variants: bool = True
) -> Iterable[Name]:
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
    if len(candidates) == 1:
        return [candidates[0].name]
    candidates = sorted(candidates, key=lambda c: c.get_score())
    best_score = candidates[0].get_score()
    matching = list(takewhile(lambda c: c.get_score() == best_score, candidates))
    if len(matching) > 1 and resolve_variants:
        return list({c.name.resolve_variant() for c in matching})
    return [c.name for c in matching]


def print_candidate_report(ce: ClassificationEntry) -> None:
    candidates = []
    for nam_or_pair in get_possible_mapped_names(ce):
        if isinstance(nam_or_pair, tuple):
            nam, metadata = nam_or_pair
        else:
            nam = nam_or_pair
            metadata = CandidateMetadata()
        candidates.append(CandidateName(ce, nam, metadata))
    if not candidates:
        print(f"{ce}: no candidates found")
        return
    candidates = sorted(candidates, key=lambda c: c.get_score())
    print(f"{ce}: candidate mapped names:")
    for candidate in candidates:
        print(
            f"  Score {candidate.get_score():3}: {candidate.name} (metadata: {candidate.metadata})"
        )


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
        """Possible improvements:

        - Better homonym differentiation. A couple of genera with homonymous names get
          wrong scores because their parent taxon is informal (e.g., _Spectrum_ and some
          others in {Mammalia (Daudin 1802)}). We could perhaps look at the species in
          the genus (though that risks circularity), or at sister genera.
        - Better handling of cases where the same name is assigned to different authorities.
          It's a bit questionable what do here anyway. Example: {Carnivora Africa (Setzer 1971).pdf}
          has "_Vulpes_ Oken, 1816", but we map it to _Vulpes_ Frisch, 1775 (a valid name), not
          _Vulpes_ Oken, 1816 (which is unavailable). Maybe we should map it to the Oken name anyway?
        """
        if self._score is not None:
            return self._score
        score = 0
        corrected_name = self.ce.get_corrected_name()
        if corrected_name is None:
            corrected_name = self.ce.name
        ce_group = self.ce.get_group()
        if self.name.corrected_original_name != corrected_name:
            score += 20

        if self.name.original_citation != self.ce.article:
            score += 50
        else:
            mapped_ce = self.name.get_mapped_classification_entry()
            if mapped_ce is not None and mapped_ce != self.ce:
                score += 50

        if self.name.group is not ce_group:
            score += 15
        associated_taxa = Taxon.select_valid().filter(Taxon.base_name == self.name)
        if not any(t.valid_name == corrected_name for t in associated_taxa):
            score += 2
        if self.ce.year is not None and self.ce.year not in [
            str(self.name.numeric_year()),
            str(self.name.resolve_variant().numeric_year()),
        ]:
            score += 10
        if self.ce.authority is not None and self.ce.authority not in [
            self.name.taxonomic_authority(),
            self.name.resolve_variant().taxonomic_authority(),
        ]:
            score += 10

        # Should help distinguish homonyms
        associated_ces = [
            _get_parent_with_mapped_name(self.ce),
            _get_child_with_mapped_name(self.ce),
        ]
        for ce in associated_ces:
            if ce is not None and ce.mapped_name is not None:
                mapped_parent = ce.mapped_name.taxon
                for derived_field in ("class_", "order", "family"):
                    parent_parent = mapped_parent.get_derived_field(derived_field)
                    my_parent = self.name.taxon.get_derived_field(derived_field)
                    if my_parent != parent_parent:
                        score += 5
        if (
            self.name.group is not Group.family
            and self.name.nomenclature_status
            not in (
                NomenclatureStatus.name_combination,
                NomenclatureStatus.incorrect_subsequent_spelling,
            )
            and self.name.get_date_object() > self.ce.article.get_date_object()
        ):
            score += 10
        if (
            self.name.group is Group.family
            and self.name.original_rank is not self.ce.rank
        ):
            score += 5
        if self.name.nomenclature_status is NomenclatureStatus.misidentification:
            score += 5
        elif self.name.nomenclature_status is NomenclatureStatus.subsequent_usage:
            score += 3
        elif self.name.nomenclature_status in (
            NomenclatureStatus.name_combination,
            NomenclatureStatus.preoccupied,
            NomenclatureStatus.incorrect_subsequent_spelling,
            NomenclatureStatus.variant,
            NomenclatureStatus.unjustified_emendation,
        ):
            score += 1
        elif not self.name.nomenclature_status.can_preoccupy():
            score += 2
        if self.name.group is Group.species:
            if " " in corrected_name:
                genus_name, *_, root_name = corrected_name.split()
                if root_name != self.name.root_name:
                    score += 2
                if (
                    self.name.original_parent is None
                    or self.name.original_parent.corrected_original_name != genus_name
                ):
                    score += 2
                if (
                    not self.metadata.is_direct_match
                    and not self.metadata.is_shared_genus
                ):
                    score += 2
                name_genus_name, *_ = self.name.taxon.valid_name.split()
                if genus_name != name_genus_name:
                    score += 2
                name_original_parent = self.name.original_parent
                ce_parent = self.ce.parent_of_rank(Rank.genus)
                if (
                    name_original_parent is not None
                    and ce_parent is not None
                    and ce_parent.mapped_name is not None
                ):
                    if name_original_parent != ce_parent.mapped_name:
                        score += 10
            elif self.name.root_name != corrected_name:
                score += 2

        if ce_group is Group.species and " " not in corrected_name:  # bare synonym
            if self.ce.parent is not None and self.ce.parent.mapped_name is not None:
                try:
                    ce_parent_species = self.ce.parent.mapped_name.taxon.parent_of_rank(
                        Rank.species
                    )
                except ValueError:
                    ce_parent_species = None
            else:
                ce_parent_species = None
            try:
                name_species = self.name.taxon.parent_of_rank(Rank.species)
            except ValueError:
                name_species = None
            if ce_parent_species is not None and name_species is not None:
                if ce_parent_species != name_species:
                    score += 5

        self._score = score
        return score


def _get_parent_with_mapped_name(ce: ClassificationEntry) -> ClassificationEntry | None:
    parent = ce.parent
    seen = {ce}
    while parent is not None:
        if parent in seen:
            break
        seen.add(parent)
        if parent.mapped_name is not None:
            return parent
        parent = parent.parent
    return None


def _get_child_with_mapped_name(ce: ClassificationEntry) -> ClassificationEntry | None:
    for child in ce.get_children():
        if child.mapped_name is not None:
            return child
    return None


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
        yield from Name.select_valid().filter(
            Name.group == Group.family, Name.corrected_original_name == corrected_name
        )
    elif group is Group.family:
        options = (ce.name, corrected_name)
        possibilies = Name.select_valid().filter(
            Name.group == Group.family,
            (
                Name.original_name.is_in(options)
                | Name.corrected_original_name.is_in(options)
            ),
        )
        names = list(possibilies)
        if names:
            yield from names
        else:
            root_name = helpers.strip_standard_suffixes(ce.name)
            possibilies = Name.select_valid().filter(
                Name.group == Group.family, Name.root_name == root_name
            )
            yield from possibilies
        yield from Name.select_valid().filter(
            Name.group == Group.high, Name.corrected_original_name == corrected_name
        )
    elif group is Group.genus:
        yield from Name.select_valid().filter(
            Name.group == Group.genus, Name.corrected_original_name == corrected_name
        )
    elif group is Group.species:
        if " " in corrected_name:
            yield from get_species_group_mapped_names(ce, corrected_name)
        else:
            yield from bare_synonym_mapped_names(ce, corrected_name)


def bare_synonym_mapped_names(
    ce: ClassificationEntry, corrected_name: str
) -> Iterable[Name]:
    if ce.parent is None or ce.parent.mapped_name is None:
        return
    try:
        taxon = ce.parent.mapped_name.taxon.parent_of_rank(Rank.species)
    except ValueError:
        return
    nams = taxon.all_names()
    direct_candidates = list(
        get_candidates_from_names_for_bare_synonym(
            nams, ce, corrected_name, check_year=False
        )
    )
    yield from direct_candidates
    if taxon.parent is not None and taxon.parent.parent is not None:
        parent_nams = taxon.parent.parent.all_names()
        parent_candidates = list(
            get_candidates_from_names_for_bare_synonym(parent_nams, ce, corrected_name)
        )
        yield from parent_candidates
    else:
        parent_candidates = []
    if ce.year and ce.authority:
        matching_year_candidates = [
            nam
            for nam in Name.select_valid().filter(
                Name.group == Group.species,
                Name.root_name == corrected_name,
                Name.year.startswith(ce.year),
            )
            if ce.authority in nam.taxonomic_authority()
        ]
        yield from matching_year_candidates
    else:
        matching_year_candidates = []
    if direct_candidates or parent_candidates or matching_year_candidates:
        return
    yield from get_candidates_from_names_for_bare_synonym(
        nams, ce, corrected_name, fuzzy=True
    )


def get_candidates_from_names_for_bare_synonym(
    nams: Iterable[Name],
    ce: ClassificationEntry,
    corrected_name: str,
    *,
    fuzzy: bool = False,
    check_year: bool = True,
) -> Iterable[Name]:
    for nam in nams:
        if ce.year is None:
            continue
        if fuzzy:
            condition = (
                models.name_complex.normalize_root_name_for_homonymy(
                    corrected_name, nam.species_name_complex
                )
                == nam.get_normalized_root_name_for_homonymy()
            )
        else:
            condition = corrected_name in nam.get_root_name_forms()
        if not condition:
            continue
        if check_year:
            try:
                ce_year = int(ce.year)
            except ValueError:
                pass
            else:
                nam_year = nam.numeric_year()
                nam_origin_year = nam.resolve_variant().numeric_year()
                if abs(ce_year - nam_year) > 10 or abs(ce_year - nam_origin_year) > 10:
                    continue
        yield nam


def get_species_group_mapped_names(
    ce: ClassificationEntry, corrected_name: str
) -> Iterable[tuple[Name, CandidateMetadata]]:
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
        normalized_root_name = models.name_complex.normalize_root_name_for_homonymy(
            root_name, None
        )
        genus_candidates = Name.select_valid().filter(
            Name.group == Group.genus, Name.root_name == genus_name
        )
        shared_genera = list(get_genera_with_shared_species(genus_candidates))
        for genus in shared_genera:
            for nam in genus.all_names():
                if nam.group == Group.species and _root_name_matches(
                    nam, normalized_root_name
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
                    if nam.group == Group.species and _root_name_matches(
                        nam, normalized_root_name
                    ):
                        count += 1
                        yield nam, CandidateMetadata(is_sister_genus=True)


def _root_name_matches(nam: Name, root_name: str) -> bool:
    if nam.root_name == root_name:
        return True
    if nam.get_normalized_root_name_for_homonymy() == root_name:
        return True
    normalized_without_sc = models.name_complex.normalize_root_name_for_homonymy(
        nam.root_name, None
    )
    return normalized_without_sc == root_name


@LINT.add("corrected_name")
def check_corrected_name(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.rank is Rank.informal or ClassificationEntryTag.Informal in ce.tags:
        return
    corrected_name = ce.get_corrected_name()
    if corrected_name is None:
        if not re.search(r"^[A-Z]\. ", ce.name):
            others = list(
                ClassificationEntry.select_valid().filter(
                    ClassificationEntry.name == ce.name,
                    ClassificationEntry.rank == ce.rank,
                    ClassificationEntry.id != ce.id,
                )
            )
            if others:
                corrected_names_list = [other.get_corrected_name() for other in others]
                corrected_names = {cn for cn in corrected_names_list if cn is not None}
                if len(corrected_names) == 1:
                    other_corrected_name = corrected_names.pop()
                    message = (
                        f"infer corrected name from other CE: {other_corrected_name}"
                    )
                    if cfg.autofix:
                        print(f"{ce}: {message}")
                        tag = ClassificationEntryTag.CorrectedName(other_corrected_name)
                        ce.add_tag(tag)
                    else:
                        yield message
                    return
        yield "cannot infer corrected name; add CorrectedName tag"
        return
    if ce.get_corrected_name_without_tags() is None and not re.search(
        r"^[A-Z]\. ", ce.name
    ):
        earliest_other = list(
            ClassificationEntry.select_valid()
            .filter(
                ClassificationEntry.name == ce.name, ClassificationEntry.rank == ce.rank
            )
            .order_by(ClassificationEntry.id)
            .limit(1)
        )
        if earliest_other and earliest_other[0].id != ce.id:
            if corrected_name != earliest_other[0].get_corrected_name():
                yield f"corrected name {corrected_name} differs from earlier CE {earliest_other[0]}: {earliest_other[0].get_corrected_name()}"

    if ce.rank is Rank.division:
        if not re.fullmatch(r"[A-Z][a-z]+ Division", corrected_name):
            yield f"incorrect division name format: {corrected_name}"
    else:
        group = ce.get_group()
        match group:
            case Group.species:
                if ce.rank.is_synonym and re.fullmatch(r"[a-z]+", corrected_name):
                    return
                if not re.fullmatch(r"[A-Z][a-z]+( [a-z]+){1,3}", corrected_name):
                    yield f"incorrect species name format: {corrected_name}"
            case Group.high:
                if not re.fullmatch(models.name.lint.CON_HIGH_REGEX, corrected_name):
                    yield f"incorrect name format: {corrected_name}"
            case Group.family | Group.genus:
                if not re.fullmatch(models.name.lint.CON_REGEX, corrected_name):
                    yield f"incorrect name format: {corrected_name}"
            case _:
                assert_never(group)


@LINT.add("page_link", requires_network=True)
def check_must_have_page_link(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if not ce.article.has_bhl_link_with_pages():
        return
    pages_with_links = _get_existing_page_links(ce)
    for page in models.name.page.get_unique_page_text(ce.page):
        if page not in pages_with_links:
            yield f"must have authority page link for {page}"


@LINT.add("check_bhl_page", requires_network=True)
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
        if not bhl.is_item_missing_bibliography(tag.url):
            yield f"cannot find BHL bibliography for {tag.url}"
        return
    if ce.article.url is None:
        return
    citation_biblio_ids = list(ce.article.get_possible_bhl_bibliography_ids())
    if bibliography_id not in citation_biblio_ids:
        yield f"BHL item mismatch: {bibliography_id} (name) not in {citation_biblio_ids} (citation)"


def _should_look_for_page_links(ce: ClassificationEntry) -> bool:
    if not ce.page:
        return False
    pages = models.name.page.get_unique_page_text(ce.page)
    pages_with_links = _get_existing_page_links(ce)
    return not all(page in pages_with_links for page in pages)


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


@LINT.add("infer_bhl_page", requires_network=True)
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
                    if getinput.yes_no(
                        "confirm? ", callbacks=ce.get_wrapped_adt_callbacks()
                    ):
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

    for page in models.name.page.get_unique_page_text(ce.page):
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


@LINT.add("infer_page_from_mapped_name")
def infer_page_from_mapped_name(
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
    new_tags = [tag for tag in new_tags if tag not in ce.tags]
    if not new_tags:
        return
    message = f"inferred page from mapped name {ce.mapped_name}: {new_tags}"
    if cfg.autofix:
        print(f"{ce}: {message}")
        for tag in new_tags:
            ce.add_tag(tag)
    else:
        yield message


@LINT.add("infer_page_from_other_names")
def infer_page_from_other_names(
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
    pages = models.name.page.get_unique_page_text(ce.page)
    for page in pages:
        other_ces = [
            ce
            for ce in ce.article.get_classification_entries().filter(
                ClassificationEntry.page.contains(page)
            )
            if ce.has_tag(ClassificationEntryTag.PageLink)
        ]
        if not other_ces:
            if cfg.verbose:
                print(f"{ce}: {page}: no other classification entries")
            return
        inferred_pages: set[str] = set()
        for other_ce in other_ces:
            for tag in other_ce.get_tags(
                other_ce.tags, ClassificationEntryTag.PageLink
            ):
                if tag.page == page:
                    inferred_pages.add(tag.url)
        if len(inferred_pages) != 1:
            if cfg.verbose:
                print(
                    f"{ce}: no single inferred page from other names ({inferred_pages})"
                )
            continue
        (url,) = inferred_pages
        tag = ClassificationEntryTag.PageLink(url=url, page=page)
        if tag in ce.tags:
            if cfg.verbose:
                print(f"{ce}: already has {tag}")
            continue
        message = f"inferred URL {url} from other names (add {tag})"
        if cfg.autofix:
            print(f"{ce}: {message}")
            ce.add_tag(tag)
        else:
            yield message


@LINT.add("bhl_page_from_article", requires_network=True)
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
    page_links = _get_existing_page_links(ce)
    for page_described in models.name.page.get_unique_page_text(ce.page):
        if page_described in page_links:
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


@LINT.add("infer_bhl_page_from_other_names", requires_network=True)
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
    pages = models.name.page.get_unique_page_text(ce.page)
    for page in pages:
        other_names = [
            ce
            for ce in ce.article.get_classification_entries().filter(
                ClassificationEntry.page.contains(page)
            )
            if ce.has_tag(ClassificationEntryTag.PageLink)
        ]
        if not other_names:
            if cfg.verbose:
                print(f"{ce}: no other new names")
            return
        inferred_pages: set[int] = set()
        for other_nam in other_names:
            for tag in other_nam.get_tags(
                other_nam.tags, ClassificationEntryTag.PageLink
            ):
                inferred_page_id = maybe_infer_page_from_other_name(
                    cfg=cfg,
                    other_nam=other_nam,
                    url=tag.url,
                    my_page=page,
                    their_page=tag.page,
                    is_same_page=tag.page == page,
                )
                if inferred_page_id is not None:
                    inferred_pages.add(inferred_page_id)
        if len(inferred_pages) != 1:
            if cfg.verbose:
                print(
                    f"{ce}: no single inferred page from other names ({inferred_pages})"
                )
            continue
        (inferred_page_id,) = inferred_pages
        tag = ClassificationEntryTag.PageLink(
            url=f"https://www.biodiversitylibrary.org/page/{inferred_page_id}",
            page=page,
        )
        if tag in ce.tags:
            if cfg.verbose:
                print(f"{ce}: already has inferred tag {tag}")
            continue
        message = f"inferred BHL page {inferred_page_id} from other names (add {tag})"
        if cfg.autofix:
            print(f"{ce}: {message}")
            ce.add_tag(tag)
        else:
            yield message


def _get_existing_page_links(ce: ClassificationEntry) -> set[str]:
    return {tag.page for tag in ce.get_tags(ce.tags, ClassificationEntryTag.PageLink)}


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


_EXCLUDED_RANKS = [Rank.informal, Rank.informal_species, *SYNONYM_RANKS]


def _resolve_name(
    nam: Name,
    tags: tuple[type[models.name.NameTag], ...] = (
        models.name.NameTag.UnavailableVersionOf,
    ),
) -> Name:
    nam = nam.resolve_redirect()
    for tag in tags:
        if target := nam.get_tag_target(tag):
            return _resolve_name(target)
    return nam


def _get_ce_key(ce: ClassificationEntry) -> tuple[Rank, str] | None:
    corrected_name = ce.get_corrected_name()
    if corrected_name is None:
        return None
    group = ce.get_group()
    match group:
        case Group.family:
            grouped_rank = helpers.get_grouped_family_group_rank(
                ce.rank, corrected_name
            )
            return grouped_rank, corrected_name
        case Group.high:
            return Rank.unranked, corrected_name
        case Group.genus:
            return Rank.genus, corrected_name
        case Group.species:
            return Rank.species, corrected_name
        case _:
            assert_never(group)


@LINT.add("mapped_name_matches_other_ces")
def check_mapped_name_matches_other_ces(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if ce.mapped_name is None or ce.rank is Rank.informal or ce.rank.is_synonym:
        return
    group = ce.get_group()
    corrected_name = ce.get_corrected_name()
    if corrected_name is None:
        return
    ce_key = _get_ce_key(ce)
    if ce_key is None:
        return
    others = [
        other_ce
        # static analysis: ignore[incompatible_argument]
        for other_ce in ClassificationEntry.select_valid().filter(
            ClassificationEntry.name == ce.name,
            ClassificationEntry.id != ce.id,
            ClassificationEntry.mapped_name != ce.mapped_name,
            ~ClassificationEntry.rank.is_in(_EXCLUDED_RANKS),
        )
        if not LINT.is_ignoring_lint(other_ce, "mapped_name_matches_other_ces")
        and other_ce.mapped_name is not None
        and _resolve_name(other_ce.mapped_name) != _resolve_name(ce.mapped_name)
        and _get_ce_key(other_ce) == ce_key
    ]
    if others:
        yield f"mapped to {ce.mapped_name}, but other names are mapped differently:\n{'\n'.join(f' - {other!r}' for other in others)}"
    if group != ce.mapped_name.group:
        possibilities = [
            nam
            for nam in ce.mapped_name.taxon.get_names()
            if nam.group == group and nam.corrected_original_name == corrected_name
        ]
        if len(possibilities) == 1:
            message = f"change to map to {possibilities[0]}"
            if cfg.autofix:
                print(f"{ce}: {message}")
                ce.mapped_name = possibilities[0]
            else:
                yield message


def get_applicable_nomenclature_statuses(
    ce: ClassificationEntry,
) -> Iterable[NomenclatureStatus]:
    for tag in ce.tags:
        if isinstance(tag, ClassificationEntryTag.CECondition):
            yield tag.status
    if ce.rank is Rank.infrasubspecific:
        yield NomenclatureStatus.infrasubspecific
    yield from models.name.lint.get_inherent_nomenclature_statuses_from_article(
        ce.article
    )


@LINT.add("maps_to_unavailable")
def check_maps_to_unavailable(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if ce.mapped_name is None or ce.rank.is_synonym:
        return
    mapped = ce.mapped_name.resolve_variant()
    if mapped.group is Group.high:
        return
    if not mapped.is_unavailable():
        return
    status = mapped.nomenclature_status
    if status in (
        NomenclatureStatus.fully_suppressed,
        NomenclatureStatus.misidentification,
        NomenclatureStatus.not_based_on_a_generic_name,
        NomenclatureStatus.based_on_homonym,
    ):
        return
    applicable_statuses = set(get_applicable_nomenclature_statuses(ce))
    if not applicable_statuses:
        yield f"mapped to unavailable name {mapped} (via {ce.mapped_name}), but lacks CECondition tag"
        return
    most_serious = min(
        applicable_statuses, key=models.name.lint.nomenclature_status_priority
    )
    if models.name.lint.nomenclature_status_priority(
        most_serious
    ) > models.name.lint.nomenclature_status_priority(status):
        yield f"mapped to unavailable name {mapped} (via {ce.mapped_name}) of status {status}, but has less serious CECondition tag for status {most_serious}"


@LINT.add("condition_from_mapped")
def infer_condition_from_mapped(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if ce.mapped_name is None or ce.mapped_name.original_citation != ce.article:
        return
    applicable_statuses = set(get_applicable_nomenclature_statuses(ce))
    name_tags = list(
        ce.mapped_name.get_tags(ce.mapped_name.tags, models.name.NameTag.Condition)
    )
    for tag in name_tags:
        if tag.status in applicable_statuses:
            continue
        new_tag = ClassificationEntryTag.CECondition(tag.status, comment=tag.comment)
        message = f"inferred CECondition tag from mapped name: {new_tag}"
        if cfg.autofix:
            print(f"{ce}: {message}")
            ce.add_tag(new_tag)
        else:
            yield message


# disabled for now because the ZooBank website is down and some of the entries
# seem dubious (e.g. Sorex minutus minutus); we may want to do this only manually,
# in cases where the ZooBank entry was manually verified to match the CE
@LINT.add("lsid_from_mapped", disabled=True)
def infer_lsid_from_mapped(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.mapped_name is None or ce.mapped_name.original_citation != ce.article:
        return
    for tag in ce.mapped_name.type_tags:
        if isinstance(tag, models.name.TypeTag.LSIDName):
            new_tag = ClassificationEntryTag.LSIDCE(tag.text)
            if new_tag in ce.tags:
                continue
            message = f"inferred LSID from mapped name: {new_tag}"
            if cfg.autofix:
                print(f"{ce}: {message}")
                ce.add_tag(new_tag)
            else:
                yield message


@LINT.add("from_mapped")
def infer_data_from_mapped(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.mapped_name is None or ce.mapped_name.get_mapped_classification_entry() != ce:
        return
    if ce.type_locality is None:
        tags = [
            tag
            for tag in ce.mapped_name.get_tags(
                ce.mapped_name.type_tags, models.name.TypeTag.LocationDetail
            )
            if tag.source == ce.article
        ]
        if len(tags) == 1:
            tag = tags[0]
            message = f"inferred type locality from mapped name: {tag}"
            if cfg.autofix:
                print(f"{ce}: {message}")
                ce.type_locality = tag.text
            else:
                yield message
        elif tags:
            message = f"multiple possible type localities from mapped name: {', '.join(f'"{tag.text}"' for tag in tags)}"
            yield message
    existing_specimen_details = {
        tag.text
        for tag in ce.tags
        if isinstance(tag, ClassificationEntryTag.TypeSpecimenData)
    }
    specimen_details = [
        tag
        for tag in ce.mapped_name.get_tags(
            ce.mapped_name.type_tags, models.name.TypeTag.SpecimenDetail
        )
        if tag.source == ce.article and tag.text not in existing_specimen_details
    ]
    if specimen_details:
        for tag in specimen_details:
            message = f"inferred type specimen detail from mapped name: {tag}"
            if cfg.autofix:
                print(f"{ce}: {message}")
                ce.add_tag(ClassificationEntryTag.TypeSpecimenData(tag.text))
            else:
                yield message


@LINT.add("vacuous_type_locality")
def check_vacuous_type_locality(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if ce.type_locality is None:
        return
    if models.name.lint.is_empty_location_detail(ce.type_locality):
        message = f"type locality is vacuous: {ce.type_locality!r}"
        if cfg.autofix:
            print(f"{ce}: {message}")
            ce.type_locality = None
        else:
            yield message


@LINT.add("infer_duplicate")
def infer_duplicate(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    possible_dupes = list(
        # Wrong type inference for <
        ClassificationEntry.select_valid().filter(  # static analysis: ignore[incompatible_argument]
            ClassificationEntry.id < ce.id,
            ClassificationEntry.article == ce.article,
            ClassificationEntry.name == ce.name,
            ClassificationEntry.parent == ce.parent,
            ClassificationEntry.mapped_name == ce.mapped_name,
            ClassificationEntry.rank == ce.rank,
        )
    )
    if not possible_dupes:
        return
    is_synonym = ce.rank.is_synonym
    possible_dupes = [
        other for other in possible_dupes if other.rank.is_synonym == is_synonym
    ]
    if ce.authority is not None:
        possible_dupes = [
            other for other in possible_dupes if other.authority == ce.authority
        ]
    if ce.year is not None:
        possible_dupes = [other for other in possible_dupes if other.year == ce.year]
    if len(possible_dupes) != 1:
        return
    dupe = possible_dupes[0]
    message = f"merge into {dupe}"
    if cfg.autofix:
        print(f"{ce}: {message}")
        parts = []
        if dupe.page is not None:
            parts.append(dupe.page)
        if ce.page is not None:
            parts.append(ce.page)
        dupe.page = ", ".join(parts)
        dupe.tags += ce.tags
        ce.merge(dupe)
    else:
        yield message


@LINT.add("check_page")
def check_page(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.page is not None:

        def set_page(page: str) -> None:
            print(f"set page to {page} on {ce}")
            ce.set_page(page)

        yield from models.name.page.check_page(
            ce.page,
            set_page=set_page,
            obj=ce,
            cfg=cfg,
            get_raw_page_regex=ce.article.get_raw_page_regex,
        )


@LINT.add("matches_citation")
def check_matches_citation(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.page is None:
        return
    yield from models.name.lint.check_page_matches_citation(ce.article, ce.page)


@LINT.add("parent_rank")
def check_parent_rank(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.parent is None:
        return
    match ce.rank:
        case Rank.subspecies | Rank.variety:
            if ce.parent.rank is not Rank.species and not (
                ce.rank is Rank.variety and ce.parent.rank is Rank.subspecies
            ):
                message = f"parent rank {ce.parent.rank.name} does not match child rank {ce.rank.name}"
                corrected_name = ce.get_corrected_name()
                new_parent = None
                if corrected_name is not None:
                    gen, sp, *_ = corrected_name.split()
                    expected_species = f"{gen} {sp}"
                    possible_parents = [
                        ce
                        for ce in ce.parent.get_children()
                        if ce.rank is Rank.species
                        and ce.get_corrected_name() == expected_species
                    ]
                    if len(possible_parents) == 1:
                        new_parent = possible_parents[0]
                        message += f"; change parent to {new_parent}"
                yield message
                if cfg.autofix and new_parent is not None:
                    ce.parent = new_parent


def find_referenced_usage(ce: ClassificationEntry) -> ClassificationEntry | None:
    if ce.mapped_name is None:
        return None
    possibilities = []
    resolved_mapped = ce.mapped_name.resolve_variant()
    for nam in ce.mapped_name.taxon.get_names():
        if nam.resolve_variant() != resolved_mapped:
            continue
        for mapped_ce in nam.get_classification_entries():
            author, year = mapped_ce.article.taxonomic_authority()
            if ce.year == year and ce.authority == author:
                possibilities.append(mapped_ce)
    if len(possibilities) == 1:
        return possibilities[0]
    return None


def get_possible_years(*objs: Article | Name) -> Iterable[int]:
    for obj in objs:
        match obj:
            case Article() as art:
                yield art.numeric_year()
                for tag in art.get_tags(art.tags, ArticleTag.KnownAlternativeYear):
                    yield int(tag.year)
            case Name() as nam:
                yield nam.numeric_year()
                if nam.original_citation is not None:
                    yield from get_possible_years(nam.original_citation)


def is_acceptable_year(
    my_year: int, alternatives: Collection[Article | Name]
) -> str | None:
    possible_years = set(get_possible_years(*alternatives))
    if not any(abs(my_year - possible_year) <= 2 for possible_year in possible_years):
        return f"year {my_year} does not match {possible_years} (from {alternatives})"
    return None


@LINT.add("needs_referenced_usage")
def check_needs_referenced_usage(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    for tag in ce.get_tags(ce.tags, ClassificationEntryTag.ReferencedUsage):
        if ce.year is not None and ce.year.isnumeric():
            message = is_acceptable_year(int(ce.year), [tag.ce.article])
            if message is not None:
                yield f"{message} for referenced usage {tag.ce}"
        break
    else:
        if ce.mapped_name is None:
            return
        if _should_ignore_referenced_usage_check(ce, cfg):
            return
        if ce.year is not None and ce.year.isnumeric():
            my_year = int(ce.year)
            possible_names = {ce.mapped_name, ce.mapped_name.resolve_variant()}
            message = is_acceptable_year(my_year, possible_names)
            if message is not None:
                referenced_usage = find_referenced_usage(ce)
                if referenced_usage is not None:
                    message += f" (maybe {referenced_usage}?)"
                yield message
                if referenced_usage is not None and cfg.autofix:
                    ce.add_tag(ClassificationEntryTag.ReferencedUsage(referenced_usage))


def _should_ignore_referenced_usage_check(
    ce: ClassificationEntry, cfg: LintConfig
) -> bool:
    if cfg.enable_all:
        return False
    # TODO: make this return False more often
    if LINT.is_ignoring_lint(ce, "needs_referenced_usage"):
        return False
    if ce.get_group() is Group.family:
        return True
    return False


@LINT.add("original_parent_matches")
def original_parent_matches(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.mapped_name is None:
        return
    if ce.mapped_name.group is not Group.species:
        return
    if ce.rank.is_synonym:
        return
    mapped_original_parent = ce.mapped_name.original_parent
    if mapped_original_parent is None:
        return
    ce_parent_genus = ce.get_original_parent_ce()
    if ce_parent_genus is None or ce_parent_genus.mapped_name is None:
        return
    tag_classes = (
        models.name.NameTag.UnavailableVersionOf,
        models.name.NameTag.MisidentificationOf,
        models.name.NameTag.SubsequentUsageOf,
    )
    if _resolve_name(ce_parent_genus.mapped_name, tag_classes) != _resolve_name(
        mapped_original_parent, tag_classes
    ):
        message = f"original parent genus {mapped_original_parent} does not match CE parent genus {ce_parent_genus.mapped_name}"
        yield message
