"""Lint steps for classification entries."""

from collections import defaultdict
from collections.abc import Container, Iterable

from taxonomy.db import helpers
from taxonomy.db.constants import Group, NomenclatureStatus, Rank
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import IgnoreLint, Lint
from taxonomy.db.models.name import Name, NameTag
from taxonomy.db.models.taxon import Taxon

from .ce import ClassificationEntry


def remove_unused_ignores(ce: ClassificationEntry, unused: Container[str]) -> None:
    pass  # no lint ignores to remove


def get_ignores(ce: ClassificationEntry) -> Iterable[IgnoreLint]:
    return []


LINT = Lint(ClassificationEntry, get_ignores, remove_unused_ignores)


@LINT.add("parent")
def check_parent(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if ce.parent is not None and ce.parent.article != ce.article:
        yield "parent from different article"


@LINT.add("missing_mapped_name")
def check_missing_mapped_name(
    ce: ClassificationEntry, cfg: LintConfig
) -> Iterable[str]:
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
        else:
            yield f"missing mapped_name (candidates: {candidates})"


@LINT.add("mapped_name")
def check_mapped_name(ce: ClassificationEntry, cfg: LintConfig) -> Iterable[str]:
    if (
        ce.mapped_name is not None
        and ce.mapped_name.nomenclature_status is NomenclatureStatus.name_combination
    ):
        target = ce.mapped_name.get_tag_target(NameTag.NameCombinationOf)
        message = f"mapped_name is name_combination; change target to {target}"
        if cfg.autofix and target is not None:
            print(f"{ce}: {message}")
            ce.mapped_name = target
        else:
            yield message


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
    ce_date_obj = ce.article.get_date_object()
    for nam in get_possible_mapped_names(ce):
        if nam.get_date_object() > ce_date_obj:
            continue
        if nam.nomenclature_status in (
            NomenclatureStatus.subsequent_usage,
            NomenclatureStatus.name_combination,
        ):
            continue
        yield nam


def get_possible_mapped_names(ce: ClassificationEntry) -> Iterable[Name]:
    group = helpers.group_of_rank(ce.rank)
    if group is Group.high:
        yield from Name.select_valid().filter(
            Name.group == Group.high, Name.corrected_original_name == ce.name
        )
    elif group is Group.family:
        possibilies = Name.select_valid().filter(
            Name.group == Group.family, Name.original_name == ce.name
        )
        by_type: dict[Name, list[Name]] = defaultdict(list)
        for name in possibilies:
            if name.type is None:
                yield name
            else:
                by_type[name.type].append(name)
        for names in by_type.values():
            yield min(names, key=lambda n: n.get_date_object())
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
            yield nam
        for taxon in Taxon.select_valid().filter(Taxon.valid_name == ce.name):
            count += 1
            yield taxon.base_name
        if count == 0:
            genus_name, *_, root_name = ce.name.split()
            normalized_root_name = helpers.normalize_root_name_for_homonymy(root_name)
            genus_candidates = Name.select_valid().filter(
                Name.group == Group.genus, Name.corrected_original_name == genus_name
            )
            variants = []
            for genus in get_genera_with_shared_species(genus_candidates):
                for nam in genus.all_names():
                    if (
                        nam.group == Group.species
                        and nam.get_normalized_root_name_for_homonymy()
                        == normalized_root_name
                    ):
                        if nam.nomenclature_status in (
                            NomenclatureStatus.incorrect_subsequent_spelling,
                            NomenclatureStatus.variant,
                            NomenclatureStatus.unjustified_emendation,
                        ):
                            variants.append(nam)
                        else:
                            count += 1
                            yield nam
            if count == 0:
                yield from variants
