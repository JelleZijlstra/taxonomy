from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from functools import cache
from typing import Generic, TypeVar

from taxonomy.db.constants import (
    GenderArticle,
    Group,
    NomenclatureStatus,
    SpeciesNameKind,
)
from taxonomy.db.models.base import LintConfig, LintResource
from taxonomy.db.models.lint import field_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name.name import Name
from taxonomy.db.models.name_complex import (
    NameComplex,
    NameEnding,
    SpeciesNameComplex,
    SpeciesNameEnding,
)

from .base import LINT

T = TypeVar("T")


@cache
def get_name_complex_finder() -> Callable[[str], tuple[NameComplex, str] | None]:
    endings_tree: SuffixTree[NameEnding] = SuffixTree()
    for ending in NameEnding.select_valid():
        endings_tree.add(ending.ending, ending)

    def finder(root_name: str) -> tuple[NameComplex, str] | None:
        endings = list(endings_tree.lookup(root_name))
        if not endings:
            return None
        inferred = max(endings, key=lambda e: -len(e.ending)).name_complex
        return inferred, f"matches endings {endings}"

    return finder


@LINT.add("infer_name_complex", clear_caches=get_name_complex_finder.cache_clear)
def infer_name_complex(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.name_complex is not None:
        return
    if nam.group is not Group.genus:
        return
    if not nam.nomenclature_status.requires_name_complex():
        return
    finder = get_name_complex_finder()
    result = finder(nam.root_name)
    if result is None:
        return
    nc, reason = result
    message = f"inferred name complex {nc} from root name {nam.root_name} ({reason})"
    yield field_issue(message, nam, "name_complex", nc)


NameComplex.creation_event.on(lambda _: get_name_complex_finder.cache_clear())
NameEnding.creation_event.on(lambda _: get_name_complex_finder.cache_clear())
NameComplex.save_event.on(lambda _: get_name_complex_finder.cache_clear())
NameEnding.save_event.on(lambda _: get_name_complex_finder.cache_clear())


@cache
def get_species_name_complex_finder() -> (
    Callable[[str], tuple[SpeciesNameComplex, str] | None]
):
    endings_tree: SuffixTree[SpeciesNameEnding] = SuffixTree()
    full_names: dict[str, tuple[SpeciesNameComplex, str]] = {}
    for ending in SpeciesNameEnding.select_valid():
        for form in ending.name_complex.get_forms(ending.ending):
            if ending.full_name_only:
                full_names[form] = (ending.name_complex, f"matches ending {ending}")
            else:
                endings_tree.add(form, ending)
    for snc in SpeciesNameComplex.filter(
        SpeciesNameComplex.kind == SpeciesNameKind.adjective
    ):
        if snc.stem is not None:
            for form in snc.get_forms(snc.stem):
                full_names[form] = (snc, "matches a form of the stem")

    def finder(root_name: str) -> tuple[SpeciesNameComplex, str] | None:
        if root_name in full_names:
            return full_names[root_name]
        endings = list(endings_tree.lookup(root_name))
        if not endings:
            return None
        inferred = max(endings, key=lambda e: -len(e.ending)).name_complex
        return inferred, f"matches endings {endings}"

    return finder


SpeciesNameComplex.creation_event.on(
    lambda _: get_species_name_complex_finder.cache_clear()
)
SpeciesNameEnding.creation_event.on(
    lambda _: get_species_name_complex_finder.cache_clear()
)
SpeciesNameComplex.save_event.on(
    lambda _: get_species_name_complex_finder.cache_clear()
)
SpeciesNameEnding.save_event.on(lambda _: get_species_name_complex_finder.cache_clear())


@LINT.add(
    "infer_species_name_complex_from_endings",
    required_resources={LintResource.SLOW},
    clear_caches=get_species_name_complex_finder.cache_clear,
)
def infer_species_name_complex(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.species_name_complex is not None:
        return
    if nam.group is not Group.species:
        return
    finder = get_species_name_complex_finder()
    result = finder(nam.root_name)
    if result is None:
        return
    snc, reason = result
    message = (
        f"inferred species name complex {snc} from root name {nam.root_name} ({reason})"
    )
    yield field_issue(message, nam, "species_name_complex", snc)


class SuffixTree(Generic[T]):
    def __init__(self) -> None:
        self.children: dict[str, SuffixTree[T]] = defaultdict(SuffixTree)
        self.values: list[T] = []

    def add(self, key: str, value: T) -> None:
        self._add(iter(reversed(key)), value)

    def count(self) -> int:
        return len(self.values) + sum(child.count() for child in self.children.values())

    def lookup(self, key: str) -> Iterable[T]:
        yield from self._lookup(iter(reversed(key)))

    def _add(self, key: Iterator[str], value: T) -> None:
        try:
            char = next(key)
        except StopIteration:
            self.values.append(value)
        else:
            self.children[char]._add(key, value)

    def _lookup(self, key: Iterator[str]) -> Iterable[T]:
        yield from self.values
        try:
            char = next(key)
        except StopIteration:
            pass
        else:
            if char in self.children:
                yield from self.children[char]._lookup(key)


_checked_root_names: set[str] = set()


@LINT.add(
    "infer_species_name_complex",
    required_resources={LintResource.SLOW},
    clear_caches=_checked_root_names.clear,
)
def infer_species_name_complex_from_other_names(
    nam: Name, cfg: LintConfig
) -> Iterable[LintResult]:
    if nam.group is not Group.species:
        return
    # Name combinations inherit their name complex from their parent, let's ignore them here
    if nam.nomenclature_status is NomenclatureStatus.name_combination:
        return
    # Fast path
    if nam.species_name_complex is not None and nam.root_name in _checked_root_names:
        return
    other_names = Name.select_valid().filter(
        Name.root_name == nam.root_name,
        Name.species_name_complex != None,
        Name.nomenclature_status != NomenclatureStatus.name_combination,
    )
    sc_to_nams: dict[SpeciesNameComplex, list[Name]] = {}
    for other_name in other_names:
        if other_name.species_name_complex is None:
            continue
        sc_to_nams.setdefault(other_name.species_name_complex, []).append(nam)

    if len(sc_to_nams) == 1:
        _checked_root_names.add(nam.root_name)
        if nam.species_name_complex is None:
            sc = next(iter(sc_to_nams))
            message = f"inferred species name complex {sc} from other names"
            yield field_issue(message, nam, "species_name_complex", sc)
    else:
        if nam.corrected_original_name is None:
            return
        root_from_con = nam.corrected_original_name.split()[-1]
        relevant_names = [
            other_name
            for other_name in other_names
            if other_name.corrected_original_name is not None
            and other_name.corrected_original_name.split()[-1] == root_from_con
        ]
        if not relevant_names:
            return
        relevant_names = sorted(
            relevant_names, key=lambda nam: (nam.get_date_object(), nam.id)
        )
        earliest = relevant_names[0]
        if earliest.species_name_complex != nam.species_name_complex:
            yield f"species name complex {nam.species_name_complex} does not match that of {earliest} ({earliest.species_name_complex})"


@LINT.add("name_complex")
def check_name_complex(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if (
        nam.name_complex is not None
        and nam.original_citation is not None
        and nam.name_complex.code_article is GenderArticle.assumed
    ):
        yield "'assumed' name_complex for name with original_citation"
    try:
        nam.get_stem()
    except ValueError as e:
        yield f"bad name complex {nam.name_complex}: {e}"
    if (
        nam.species_name_complex is not None
        and nam.species_name_complex.kind is SpeciesNameKind.adjective
    ):
        try:
            nam.species_name_complex.get_stem_from_name(nam.root_name)
        except ValueError as e:
            yield f"has invalid name complex {nam.species_name_complex}: {e!r}"
