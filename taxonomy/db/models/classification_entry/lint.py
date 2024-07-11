"""Lint steps for classification entries."""

from collections import Counter, defaultdict
from collections.abc import Container, Iterable
from dataclasses import dataclass, field
from itertools import takewhile

from taxonomy import getinput
from taxonomy.db import helpers
from taxonomy.db.constants import Group, NomenclatureStatus, Rank
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.name import Name, NameTag
from taxonomy.db.models.taxon import Taxon

from .ce import ClassificationEntry, ClassificationEntryTag


def remove_unused_ignores(ce: ClassificationEntry, unused: Container[str]) -> None:
    pass  # no lint ignores to remove


def get_ignores(ce: ClassificationEntry) -> Iterable[IgnoreLint]:
    return []


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


@LINT.add("parent")
def check_parent(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.parent is not None and ce.parent.article != ce.article:
        yield "parent from different article"


@LINT.add("missing_mapped_name")
def check_missing_mapped_name(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
    if ce.rank is Rank.synonym:
        return
    if ce.mapped_name is None:
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
        if ce.mapped_name.nomenclature_status is NomenclatureStatus.name_combination:
            target = ce.mapped_name.get_tag_target(NameTag.NameCombinationOf)
            message = f"mapped_name is name_combination; change target to {target}"
            if cfg.autofix and target is not None:
                print(f"{ce}: {message}")
                ce.mapped_name = target
            else:
                yield message
        match ce.mapped_name.group:
            case Group.high | Group.genus:
                if ce.name != ce.mapped_name.root_name:
                    yield f"mapped_name root_name does not match: {ce.name} vs {ce.mapped_name.root_name}"
            case Group.family:
                if ce.name not in (
                    ce.mapped_name.original_name,
                    ce.mapped_name.corrected_original_name,
                ):
                    yield f"mapped_name original_name does not match: {ce.name} vs {ce.mapped_name.original_name}"
                    if cfg.interactive:
                        if getinput.yes_no(f"Add new synonym for {ce}?"):
                            ce.mapped_name = ce.add_family_group_synonym(
                                ce.mapped_name.type
                            )
            case Group.species:
                root_name = ce.name.split()[-1]
                if root_name not in ce.mapped_name.get_root_name_forms():
                    yield f"mapped_name root_name does not match: {root_name} vs {ce.mapped_name.root_name}"
                    if cfg.interactive:
                        if getinput.yes_no(
                            f"Add incorrect subsequent spelling for {ce}?"
                        ):
                            ce.mapped_name = ce.add_incorrect_subsequent_spelling(
                                ce.mapped_name
                            )
    elif ce.rank not in (Rank.synonym, Rank.informal):
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
        if self.name.corrected_original_name != self.ce.name:
            score += 1
        if self.name.original_name != self.ce.name:
            score += 1
        associated_taxa = Taxon.select_valid().filter(Taxon.base_name == self.name)
        if not any(t.valid_name == self.ce.name for t in associated_taxa):
            score += 1
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
            genus_name, *_, root_name = self.ce.name.split()
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
    group = helpers.group_of_rank(ce.rank)
    if group is Group.high:
        yield from Name.select_valid().filter(
            Name.group == Group.high, Name.corrected_original_name == ce.name
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
            Name.group == Group.genus, Name.corrected_original_name == ce.name
        )
    elif group is Group.species:
        count = 0
        for nam in Name.select_valid().filter(
            Name.group == Group.species, Name.corrected_original_name == ce.name
        ):
            count += 1
            yield nam, CandidateMetadata(is_direct_match=True)
        for taxon in Taxon.select_valid().filter(Taxon.valid_name == ce.name):
            count += 1
            yield taxon.base_name, CandidateMetadata(is_direct_match=True)
        if count == 0:
            genus_name, *_, root_name = ce.name.split()
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
