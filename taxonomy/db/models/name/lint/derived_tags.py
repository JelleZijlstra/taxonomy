"""Compare source quotations with derived data; infer only unambiguous additions."""

import re
from collections.abc import Callable, Iterable, Sequence
from functools import cache

from taxonomy.db import helpers
from taxonomy.db.constants import (
    AgeClass,
    Group,
    NamingConvention,
    PersonType,
    SpeciesGroupType,
    SpecimenOrgan,
)
from taxonomy.db.models.base import LintConfig
from taxonomy.db.models.lint import add_tag_issue
from taxonomy.db.models.lint_types import LintResult
from taxonomy.db.models.name import derived_tags as extract
from taxonomy.db.models.name.name import Name, TypeTag
from taxonomy.db.models.person import Person

from .base import LINT


def _details(nam: Name, *, location: bool = False) -> Iterable[TypeTag]:
    """Do not mix original types and subsequently selected name-bearing types."""
    if nam.group is not Group.species:
        return
    if nam.species_type_kind not in (
        SpeciesGroupType.holotype,
        SpeciesGroupType.lectotype,
        SpeciesGroupType.neotype,
    ):
        return
    sources = None
    designation_cls = {
        SpeciesGroupType.lectotype: TypeTag.LectotypeDesignation,
        SpeciesGroupType.neotype: TypeTag.NeotypeDesignation,
    }.get(nam.species_type_kind)
    if designation_cls is not None:
        sources = {
            tag.optional_source
            for tag in nam.get_tags(nam.type_tags, designation_cls)
            if tag.valid and tag.optional_source is not None
        }
    detail_cls = TypeTag.LocationDetail if location else TypeTag.SpecimenDetail
    for detail in nam.get_tags(nam.type_tags, detail_cls):
        if nam.species_type_kind is SpeciesGroupType.holotype and re.search(
            r"\b(?:neotype|lectotype)\b", detail.text, re.IGNORECASE
        ):
            continue
        if sources is None or detail.source in sources:
            yield detail


def _compatible(item: extract.Evidence[TypeTag], tag: TypeTag) -> bool:
    return any(
        extract.tags_compatible(value, tag)
        for value in (item.value, *item.alternatives)
    )


def _organ_supported(
    value: TypeTag.Organ, existing: Sequence[TypeTag.Organ]  # type: ignore[name-defined]
) -> bool:
    organs = {tag.organ for tag in existing}
    if value.organ in organs:
        return True
    # A whole skeleton may be represented either as one tag or as skull plus
    # postcranium. This is a difference of representation, not source evidence.
    if value.organ in (SpecimenOrgan.skull, SpecimenOrgan.postcranial_skeleton):
        return SpecimenOrgan.skeleton in organs
    if value.organ is SpecimenOrgan.skeleton:
        return {SpecimenOrgan.skull, SpecimenOrgan.postcranial_skeleton} <= organs
    return False


def _check(
    nam: Name,
    tag_cls: type[TypeTag],
    extractor: Callable[[str], Sequence[extract.Evidence[TypeTag]]],
    *,
    location: bool = False,
) -> Iterable[LintResult]:
    if nam.has_conflicting_type_data(tag_cls.__name__):
        return
    details = list(_details(nam, location=location))
    evidence = [(item, detail) for detail in details for item in extractor(detail.text)]
    if not evidence:
        return
    existing = list(nam.get_tags(nam.type_tags, tag_cls))
    if existing:
        for item, detail in evidence:
            if tag_cls is TypeTag.Organ:
                assert isinstance(item.value, TypeTag.Organ)
                organs = list(nam.get_tags(nam.type_tags, TypeTag.Organ))
                compatible = _organ_supported(item.value, organs)
            else:
                compatible = any(_compatible(item, tag) for tag in existing)
            if not compatible:
                if tag_cls is TypeTag.Organ:
                    yield (
                        f"{item.value} extracted from {detail} is not represented "
                        f"in existing Organ tags: {existing}"
                    )
                else:
                    yield (
                        f"{item.value} extracted from {detail} conflicts with existing "
                        f"{tag_cls.__name__} tags: {existing}"
                    )
        return
    if not any(item.certain for item, _ in evidence):
        return
    if tag_cls is TypeTag.Date and not all(
        extract.allows_date_inference(detail.text) for detail in details
    ):
        return
    if tag_cls is not TypeTag.Organ:
        for index, (item, detail) in enumerate(evidence):
            if any(
                not any(
                    _compatible(item, value)
                    for value in (other.value, *other.alternatives)
                )
                for other, _ in evidence[index + 1 :]
            ):
                yield f"cannot infer {tag_cls.__name__}: conflicting source evidence {[(str(item.value), str(detail)) for item, detail in evidence]}"
                return
    if tag_cls is TypeTag.Altitude and any(
        not extract.altitudes_equivalent(evidence[0][0].value, item.value)
        for item, _ in evidence[1:]
    ):
        yield f"cannot infer one Altitude from overlapping but different ranges: {[(str(item.value), str(detail)) for item, detail in evidence]}"
        return
    seen = set()
    if not location and not all(
        extract.allows_inference(detail.text) for detail in details
    ):
        return
    for item, detail in evidence:
        if not item.certain or item.value in seen:
            continue
        # No* means absent from a particular source, not universally absent.
        no_tag_cls = getattr(TypeTag, f"No{tag_cls.__name__}", None)
        if no_tag_cls is not None and any(
            tag.source == detail.source
            for tag in nam.get_tags(nam.type_tags, no_tag_cls)
        ):
            yield f"{item.value} extracted from {detail} conflicts with {no_tag_cls.__name__}"
            continue
        if (
            isinstance(item.value, TypeTag.Date)
            and nam.species_type_kind is not SpeciesGroupType.neotype
            and nam.year
            and (earliest := helpers.get_earliest_collection_date(item.value.date))
            is not None
            and earliest > nam.get_date_object()
        ):
            yield f"{item.value} extracted from {detail} postdates the name's publication in {nam.year}"
            continue
        yield add_tag_issue(
            f"add {item.value} inferred from {detail}",
            nam,
            item.value,
            field="type_tags",
        )
        seen.add(item.value)
        if tag_cls is not TypeTag.Organ:
            return


@LINT.add("location_detail_altitude", disabled=True)
def check_location_detail_altitude(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    yield from _check(nam, TypeTag.Altitude, extract.extract_altitude, location=True)


@LINT.add("specimen_detail_age", disabled=True)
def check_specimen_detail_age(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    yield from _check(nam, TypeTag.Age, extract.extract_age)


@LINT.add("specimen_detail_gender", disabled=True)
def check_specimen_detail_gender(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    yield from _check(nam, TypeTag.Gender, extract.extract_gender)


@LINT.add("specimen_detail_date", disabled=True)
def check_specimen_detail_date(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    yield from _check(nam, TypeTag.Date, extract.extract_date)


@LINT.add("specimen_detail_organ", disabled=True)
def check_specimen_detail_organ(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    # Fossil preparations need much richer interpretation (sides, elements,
    # counts, and associations) than this bounded recent-specimen parser.
    if nam.taxon.age not in (AgeClass.extant, AgeClass.recently_extinct):
        return
    yield from _check(nam, TypeTag.Organ, extract.extract_organ)


def _person_forms(person: Person, *, abbreviated: bool = True) -> set[str]:
    family = " ".join(filter(None, (person.tussenvoegsel, person.family_name)))
    given_forms = {person.given_names, person.get_initials()}
    if abbreviated and (initials := person.get_initials()):
        # Abbreviated catalog names must not conflict with fuller initials.
        parts = initials.rstrip(".").split(".")
        given_forms.update(".".join(parts[:i]) for i in range(1, len(parts)))
    forms = {
        extract.normalize_person_name(f"{given} {family}")
        for given in given_forms
        if given
    }
    # Respect family-first conventions, including Vietnamese middle names.
    if person.naming_convention in (
        NamingConvention.vietnamese,
        NamingConvention.pinyin,
        NamingConvention.chinese,
        NamingConvention.japanese,
        NamingConvention.korean,
        NamingConvention.hungarian,
    ) and (person.given_names or person.initials):
        forms.add(extract.normalize_person_name(person.get_full_name()))
    return forms


def _matches_person(text: str, person: Person) -> bool:
    if extract.normalize_person_name(text) in _person_forms(person):
        return True
    family = " ".join(filter(None, (person.tussenvoegsel, person.family_name)))
    if not text.casefold().endswith(" " + family.casefold()):
        return False
    given = text[: -len(family)].strip()
    stored = person.given_names or person.get_initials()
    if not stored:
        return True  # A family-name-only Person cannot contradict given names.
    left = re.findall(r"[^\W\d_]+", given.casefold())
    right = re.findall(r"[^\W\d_]+", stored.casefold())
    return bool(left and right) and all(
        a == b or (min(len(a), len(b)) == 1 and a[0] == b[0])
        for a, b in zip(left, right, strict=False)
    )


@cache
def _person_index() -> dict[str, set[Person]]:
    index: dict[str, set[Person]] = {}
    for person in Person.select_valid():
        if person.is_invalid():
            continue
        target = person
        if person.type is PersonType.alias:
            if person.target is None or person.target.is_invalid():
                continue
            target = person.target
        for form in _person_forms(person):
            index.setdefault(form, set()).add(target)
    return index


@LINT.add(
    "specimen_detail_collector", clear_caches=_person_index.cache_clear, disabled=True
)
def check_specimen_detail_collector(nam: Name, cfg: LintConfig) -> Iterable[LintResult]:
    if nam.has_conflicting_type_data("CollectedBy"):
        return
    details = list(_details(nam))
    evidence = [
        (item, detail)
        for detail in details
        for item in extract.extract_collectors(detail.text)
    ]
    if not evidence:
        return
    existing = [tag.person for tag in nam.get_tags(nam.type_tags, TypeTag.CollectedBy)]
    if existing:
        for item, detail in evidence:
            key = extract.normalize_person_name(item.value)
            if any(_matches_person(item.value, person) for person in existing):
                continue
            # A source can abbreviate a given name differently. Only report a
            # conflict when the extracted name identifies a different Person.
            candidates = _person_index().get(key, set())
            if len(candidates) == 1 and not candidates.intersection(existing):
                yield f"collector {item.value!r} extracted from {detail} conflicts with CollectedBy tags: {existing}"
        return
    # Resolve complete accounts, not just the recognizable members of a team.
    if not all(extract.allows_inference(detail.text) for detail in details):
        return
    by_detail: dict[TypeTag, set[Person]] = {}
    for item, detail in evidence:
        candidates = _person_index().get(
            extract.normalize_person_name(item.value), set()
        )
        if len(candidates) != 1 or not item.certain:
            return
        person = next(iter(candidates))
        if extract.normalize_person_name(item.value) not in _person_forms(
            person, abbreviated=False
        ):
            return
        # One initial is insufficient evidence for a new person association.
        if re.fullmatch(r"[A-Z]\.?\s+[^\s]+", item.value):
            return
        by_detail.setdefault(detail, set()).update(candidates)
    teams = list(by_detail.values())
    if any(team != teams[0] for team in teams[1:]):
        yield f"cannot infer CollectedBy: conflicting collector accounts {[(item.value, str(detail)) for item, detail in evidence]}"
        return
    if any(
        tag.source in {detail.source for detail in by_detail}
        for tag in nam.get_tags(nam.type_tags, TypeTag.NoCollector)
    ):
        yield f"extracted collectors conflict with NoCollector: {[(item.value, str(detail)) for item, detail in evidence]}"
        return
    for person in sorted(teams[0], key=lambda person: person.id):
        tag = TypeTag.CollectedBy(person)
        yield add_tag_issue(
            f"add {tag} inferred from {list(by_detail)}", nam, tag, field="type_tags"
        )
