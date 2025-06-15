"""Code for parsing and processing type specimen texts.

The main interface is parse_type_specimen().

"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from typing import Protocol, TypeVar, assert_never

from taxonomy.parsing import extract_collection_from_type_specimen


@dataclass(frozen=True)
class SimpleSpecimen:
    text: str

    def stringify(self) -> str:
        return self.text

    def sort_key(self) -> tuple[object, ...]:
        return (1, _possibly_numeric_sort_key(self.text))

    @property
    def institution_code(self) -> str | None:
        return extract_collection_from_type_specimen(self.text)


@dataclass(frozen=True)
class TripletSpecimen:
    institution_code: str
    collection_code: str
    catalog_number: str

    def stringify(self) -> str:
        return f"{self.institution_code}:{self.collection_code}:{self.catalog_number}"

    def sort_key(self) -> tuple[object, ...]:
        return (
            0,
            self.institution_code,
            self.collection_code,
            _possibly_numeric_sort_key(self.catalog_number),
        )


def _possibly_numeric_sort_key(text: str) -> tuple[object, ...]:
    numeric_match = re.fullmatch(r"(.+) (\d+)", text)
    return (
        numeric_match is not None,
        (
            (numeric_match.group(1), int(numeric_match.group(2)))
            if numeric_match is not None
            else text
        ),
    )


@dataclass(frozen=True)
class InformalSpecimen:
    """Represents 'BMNH "informal number"."""

    institution_code: str
    number: str

    def stringify(self) -> str:
        return f'{self.institution_code} "{self.number}"'

    def sort_key(self) -> tuple[object, ...]:
        return (2, self.institution_code, self.number)


@dataclass(frozen=True)
class SpecialSpecimen:
    """Represents 'BMNH (lost)'."""

    institution_code: str
    label: str

    def stringify(self) -> str:
        return f"{self.institution_code} ({self.label})"

    def sort_key(self) -> tuple[object, ...]:
        return (3, self.institution_code, self.label)


BaseSpecimen = SimpleSpecimen | TripletSpecimen | InformalSpecimen | SpecialSpecimen


@dataclass(frozen=True)
class InformalWithoutInstitution:
    """Represents an informal number without an institution code.

    Used for former numbers.

    """

    number: str

    def stringify(self) -> str:
        return f'"{self.number}"'

    def sort_key(self) -> tuple[object, ...]:
        return (4, self.number)


@dataclass(frozen=True)
class Specimen:
    base: BaseSpecimen
    comment: str | None = None
    future_texts: Sequence[BaseSpecimen] = field(default_factory=list)
    extra_texts: Sequence[BaseSpecimen] = field(default_factory=list)
    former_texts: Sequence[BaseSpecimen | InformalWithoutInstitution] = field(
        default_factory=list
    )

    def stringify(self) -> str:
        text = self.base.stringify()
        if self.comment is not None:
            text += f" ({self.comment}!)"
        text += "".join(
            f" (=> {future.stringify()})"
            for future in sort_specimens(self.future_texts)
        )
        text += "".join(
            f" (+ {extra.stringify()})" for extra in sort_specimens(self.extra_texts)
        )
        text += "".join(
            f" (= {former.stringify()})" for former in sort_specimens(self.former_texts)
        )
        return text

    def sort_key(self) -> tuple[object, ...]:
        return (
            1,
            self.base.sort_key(),
            self.comment or "",
            tuple(sort_specimens(self.future_texts)),
            tuple(sort_specimens(self.extra_texts)),
            tuple(sort_specimens(self.former_texts)),
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.base,
                self.comment,
                tuple(self.future_texts),
                tuple(self.extra_texts),
                tuple(self.former_texts),
            )
        )


@dataclass(frozen=True)
class SpecimenRange:
    start: Specimen
    end: Specimen

    def stringify(self) -> str:
        return f"{self.start.stringify()} through {self.end.stringify()}"

    def sort_key(self) -> tuple[object, ...]:
        return (0, self.start.sort_key(), self.end.sort_key())


AnySpecimen = Specimen | SpecimenRange


def _split_type_spec_string(text: str) -> Iterable[str]:
    current: list[str] = []
    parens = 0
    just_saw_comma = False
    for i, c in enumerate(text):
        if just_saw_comma:
            just_saw_comma = False
            if c != " ":
                raise ValueError(f"expected space at position {i}, not {c!r}")
            continue
        if c == "(":
            parens += 1
        elif c == ")":
            parens -= 1
        elif c == "," and parens == 0:
            just_saw_comma = True
            if not current:
                raise ValueError(f"unexpected comma at position {i}")
            yield "".join(current)
            current.clear()
            continue
        current.append(c)
    if current:
        yield "".join(current)
    else:
        raise ValueError("comma at end of string")
    if parens != 0:
        raise ValueError("unbalanced parentheses")


_SPECIAL_SUFFIXES = (
    " (unnumbered)",
    " (no number given)",
    " (no numbers given)",
    " (lost)",
)


def _parse_single_specimen(text: str) -> Specimen:
    formers: list[BaseSpecimen | InformalWithoutInstitution] = []
    futures: list[BaseSpecimen] = []
    extras: list[BaseSpecimen] = []
    comment = None
    while text.endswith(")") and not text.endswith(_SPECIAL_SUFFIXES):
        if " (" in text:
            text, end = text.rsplit(" (", maxsplit=1)
            tail = end.removesuffix(")")
            if tail.endswith("!"):
                if comment is not None:
                    raise ValueError(f"cannot have two comments in {text}")
                comment = tail.rstrip("!")
            elif tail.startswith("=>"):
                future = _parse_base_specimen(tail.removeprefix("=>").strip())
                futures.append(future)
            elif tail.startswith("+"):
                extra = _parse_base_specimen(tail.removeprefix("+").strip())
                extras.append(extra)
            elif tail.startswith("="):
                former = tail.removeprefix("=").strip()
                if former.startswith('"') and former.endswith('"'):
                    formers.append(InformalWithoutInstitution(former.strip('"')))
                else:
                    formers.append(_parse_base_specimen(former))
            else:
                raise ValueError(f"invalid parenthesized text in {text!r}")
        else:
            raise ValueError(f"invalid parenthesized text in {text!r}")
    return Specimen(
        _parse_base_specimen(text),
        comment=comment,
        former_texts=formers,
        future_texts=futures,
        extra_texts=extras,
    )


_BMNH_COLLECTION_SYNONYMS = {
    "Mammals": "Mamm",
    "Amphibians": "Amph",
    "Reptiles": "Rept",
}


def _parse_base_specimen(text: str) -> BaseSpecimen:
    if text.endswith(_SPECIAL_SUFFIXES):
        text, end = text.rsplit(" (", maxsplit=1)
        return SpecialSpecimen(text, end.removesuffix(")"))
    elif text.endswith('"'):
        coll, number = text.split(" ", maxsplit=1)
        if not number.startswith('"'):
            raise ValueError(f"invalid informal specimen {text!r}")
        return InformalSpecimen(coll, number.strip('"'))
    elif match := re.fullmatch(r"^([A-Za-z]+):([^:]+):(.*)$", text):
        return TripletSpecimen(match.group(1), match.group(2), match.group(3))
    else:
        # As a temporary measure, facilitate migrating BMNH to the triplet system
        if text.startswith("BMNH ") and text.count(" ") >= 2:
            institution, collection, number = text.split(" ", maxsplit=2)
            collection = _BMNH_COLLECTION_SYNONYMS.get(collection, collection)
            return TripletSpecimen(institution, collection, number)
        return SimpleSpecimen(text)


class _Sortable(Protocol):
    def sort_key(self) -> tuple[object, ...]:
        raise NotImplementedError


_SortableT = TypeVar("_SortableT", bound=_Sortable)


def sort_specimens(specimens: Iterable[_SortableT]) -> list[_SortableT]:
    return sorted(specimens, key=lambda s: s.sort_key())


def stringify_specimen_list(specimens: Iterable[AnySpecimen]) -> str:
    return ", ".join(spec.stringify() for spec in sort_specimens(set(specimens)))


def parse_type_specimen(text: str) -> list[AnySpecimen]:
    specs: list[AnySpecimen] = []
    for chunk in _split_type_spec_string(text):
        if " through " in chunk:
            left, right = chunk.split(" through ", maxsplit=1)
            left_spec = _parse_single_specimen(left)
            if not isinstance(left_spec.base, (SimpleSpecimen, TripletSpecimen)):
                raise ValueError(
                    f"range must contain a simple specimen, not {left_spec}"
                )
            right_spec = _parse_single_specimen(right)
            if not isinstance(right_spec.base, (SimpleSpecimen, TripletSpecimen)):
                raise ValueError(
                    f"range must contain a simple specimen, not {right_spec}"
                )
            specs.append(SpecimenRange(left_spec, right_spec))
        else:
            specs.append(_parse_single_specimen(chunk))
    return specs


def get_instution_code(specimen: AnySpecimen) -> str | None:
    if isinstance(specimen, Specimen):
        return specimen.base.institution_code
    elif isinstance(specimen, SpecimenRange):
        return specimen.start.base.institution_code
    else:
        assert_never(specimen)


def type_specimens_equal(left: str, right: str) -> bool:
    try:
        left_specs = [_simplify(spec) for spec in parse_type_specimen(left)]
        right_specs = [_simplify(spec) for spec in parse_type_specimen(right)]
    except ValueError:
        return False
    return set(left_specs) == set(right_specs)


def _simplify(spec: AnySpecimen) -> AnySpecimen:
    if isinstance(spec, Specimen):
        return replace(spec, future_texts=[], extra_texts=[], former_texts=[])
    elif isinstance(spec, SpecimenRange):
        return spec
    else:
        assert_never(spec)
