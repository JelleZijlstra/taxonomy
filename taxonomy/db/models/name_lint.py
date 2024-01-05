from __future__ import annotations

"""

Lint steps for Names.

"""

import enum
import functools
import itertools
import json
import re
from collections import defaultdict
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypeVar, assert_never, cast, get_args

import requests

from ... import adt, getinput
from ...apis.zoobank import clean_lsid, get_zoobank_data, is_valid_lsid
from .. import helpers
from ..constants import (
    ArticleKind,
    ArticleType,
    CommentKind,
    DateSource,
    FillDataLevel,
    Group,
    NamingConvention,
    NomenclatureStatus,
    SpeciesGroupType,
    SpeciesNameKind,
    SpecimenOrgan,
    Status,
    TypeSpeciesDesignation,
)
from .article import Article, ArticleTag, PresenceStatus
from .base import LintConfig
from .collection import Collection, CollectionTag
from .name import STATUS_TO_TAG, Name, NameComment, NameTag, TypeTag
from .person import AuthorTag, PersonLevel

T = TypeVar("T")
ADTT = TypeVar("ADTT", bound=adt.ADT)

Linter = Callable[[Name, LintConfig], Iterable[str]]
IgnorableLinter = Callable[[Name, LintConfig], Generator[str, None, set[str]]]

LINTERS = []
DISABLED_LINTERS = []


def get_ignored_lints(nam: Name) -> set[str]:
    tags = nam.get_tags(nam.type_tags, TypeTag.IgnoreLintName)
    return {tag.label for tag in tags}


def make_linter(
    label: str, *, disabled: bool = False
) -> Callable[[Linter], IgnorableLinter]:
    def decorator(linter: Linter) -> IgnorableLinter:
        @functools.wraps(linter)
        def wrapper(
            nam: Name, cfg: LintConfig, **kwargs: Any
        ) -> Generator[str, None, set[str]]:
            # static analysis: ignore[incompatible_call]
            issues = list(linter(nam, cfg, **kwargs))
            if not issues:
                return set()
            ignored_lints = get_ignored_lints(nam)
            if label in ignored_lints:
                return {label}
            for issue in issues:
                yield f"{nam}: {issue} [{label}]"
            return set()

        if disabled:
            DISABLED_LINTERS.append(wrapper)
        else:
            LINTERS.append(wrapper)
        return wrapper

    return decorator


def replace_arg(tag: ADTT, arg: str, val: object) -> ADTT:
    kwargs = {**tag.__dict__, arg: val}
    return type(tag)(**kwargs)


def get_tag_fields_of_type(tag: adt.ADT, typ: type[T]) -> Iterable[tuple[str, T]]:
    tag_type = type(tag)
    for arg_name, arg_type in tag_type._attributes.items():
        if arg_type is typ:
            if (val := getattr(tag, arg_name)) is None:
                continue
            yield arg_name, val


UNIQUE_TAGS = (
    TypeTag.Altitude,
    TypeTag.Coordinates,
    TypeTag.Date,
    TypeTag.Gender,
    TypeTag.Age,
    TypeTag.DifferentAuthority,
    TypeTag.TextualOriginalRank,
    TypeTag.GenusCoelebs,
)
ORGAN_REPLACEMENTS = [
    (r"^both$", r"L, R"),
    (r"(^|, )partial ([LR])(?=$|, )", r"\1\2 part"),
    (r"(^|, )(proximal|distal|shaft|part) ([LR])(?=$|, )", r"\1\3 \2"),
    (r"^partial$", "part"),
    (r"^(L |R )?fragments$", r"\1parts"),
    (r"^(L |R )?fragment$", r"\1part"),
    (r"^(multiple|several|many|vertebrae)$", ">1"),
]

LONG_BONES = (
    SpecimenOrgan.fibula,
    SpecimenOrgan.radius,
    SpecimenOrgan.ulna,
    SpecimenOrgan.tibia,
    SpecimenOrgan.femur,
    SpecimenOrgan.humerus,
)
PHALANX_ORGANS = {
    SpecimenOrgan.phalanx,
    SpecimenOrgan.phalanx_manus,
    SpecimenOrgan.phalanx_pes,
}
PROXIMAL_DISTAL_ORGANS = {
    *LONG_BONES,
    *PHALANX_ORGANS,
    SpecimenOrgan.metapodial,
    SpecimenOrgan.carpometacarpal,
    SpecimenOrgan.tarsometatarsus,
    SpecimenOrgan.clavicle,
    SpecimenOrgan.tibiotarsus,
    SpecimenOrgan.metacarpal,
    SpecimenOrgan.metatarsal,
    SpecimenOrgan.carpal,
    SpecimenOrgan.pubis,
    SpecimenOrgan.ischium,
    SpecimenOrgan.ilium,
    SpecimenOrgan.coracoid,
    SpecimenOrgan.scapula,
    SpecimenOrgan.pelvis,
    SpecimenOrgan.calcaneum,
    SpecimenOrgan.astragalus,
    SpecimenOrgan.rib,
    SpecimenOrgan.manus,
    SpecimenOrgan.pes,
}
ALLOW_ANTERIOR_POSTERIOR = {
    SpecimenOrgan.shell,
    SpecimenOrgan.skull,
    SpecimenOrgan.vertebra,
    SpecimenOrgan.calcaneum,
    SpecimenOrgan.mandible,
    SpecimenOrgan.dentary,
}
PAIRED_ORGANS = {
    *PROXIMAL_DISTAL_ORGANS,
    SpecimenOrgan.furcula,
    SpecimenOrgan.petrosal,
    SpecimenOrgan.patella,
    SpecimenOrgan.hyoid,
    SpecimenOrgan.carpal,
    SpecimenOrgan.scapulocoracoid,
    SpecimenOrgan.girdle,
    SpecimenOrgan.antler,
    SpecimenOrgan.horn_core,
    SpecimenOrgan.frontlet,
    SpecimenOrgan.limb,
    SpecimenOrgan.prepubis,
    SpecimenOrgan.predentary,
    SpecimenOrgan.dentary,
    SpecimenOrgan.premaxilla,
    SpecimenOrgan.maxilla,
    SpecimenOrgan.mandible,  # only for nonmammals
    SpecimenOrgan.tooth,
    SpecimenOrgan.osteoderm,
    SpecimenOrgan.rib,
    SpecimenOrgan.gastralia,
    *PHALANX_ORGANS,
}
COUNTED_ORGANS = {
    SpecimenOrgan.carpal,
    SpecimenOrgan.metapodial,
    SpecimenOrgan.metacarpal,
    SpecimenOrgan.metatarsal,
    SpecimenOrgan.carpal,
    SpecimenOrgan.limb,
    SpecimenOrgan.tooth,
    SpecimenOrgan.gastralia,
    SpecimenOrgan.osteoderm,
    SpecimenOrgan.vertebra,
    SpecimenOrgan.rib,
    *PHALANX_ORGANS,
}
TOOTHED_ORGANS = {
    SpecimenOrgan.dentary,
    SpecimenOrgan.palate,
    SpecimenOrgan.tooth,
    SpecimenOrgan.mandible,
    SpecimenOrgan.maxilla,
    SpecimenOrgan.premaxilla,
    SpecimenOrgan.skull,
}
ALLOW_RESTRICTED_RAW_TEXT = {
    SpecimenOrgan.skull,
    SpecimenOrgan.mandible,
    SpecimenOrgan.sternum,
    SpecimenOrgan.osteoderm,
    SpecimenOrgan.manus,
    SpecimenOrgan.pes,
    SpecimenOrgan.tissue_sample,
    SpecimenOrgan.whole_animal,
    SpecimenOrgan.in_alcohol,
    SpecimenOrgan.skin,
}
CHECKED_ORGANS = {
    *PAIRED_ORGANS,
    *COUNTED_ORGANS,
    *PROXIMAL_DISTAL_ORGANS,
    *TOOTHED_ORGANS,
    *ALLOW_RESTRICTED_RAW_TEXT,
    SpecimenOrgan.egg,
    SpecimenOrgan.gastralia,
    SpecimenOrgan.interclavicle,
    SpecimenOrgan.baculum,
    SpecimenOrgan.caudal_tube,
    SpecimenOrgan.skeleton,
    SpecimenOrgan.pelvis,
    SpecimenOrgan.shell,
    SpecimenOrgan.postcranial_skeleton,
}
# Only one not chedked is "other", it should probably stay that way
# Other possible improvements:
# - Sort the comma-separated parts
# - Rename the "condition" tag to "comment"
# - Restrict some organs to specific taxonomic groups (antler, frontlet, predentary, horn_core, shell)
# - Lint against overlapping teeth (both LP2-M3 and LP3-M3, both ?P3 and P?3). Also both "P2" and "RP2"
# - Make sure "3 L M" gets turned into "3 LM"


CATEGORIES = ["i", "c", "p", "m"]


def specify_organ(
    organ: SpecimenOrgan, detail: str
) -> tuple[SpecimenOrgan, str] | None:
    if organ is SpecimenOrgan.pes:
        if detail == "phalanges":
            return SpecimenOrgan.phalanx_pes, ">1"
        elif detail == "phalanx":
            return SpecimenOrgan.phalanx_pes, "1"
        elif re.fullmatch(r"^(L |R )?Mt(I|II|III|IV|V)$", detail):
            return SpecimenOrgan.metatarsal, detail
        elif detail == "metatarsals":
            return SpecimenOrgan.metatarsal, ">1"
        elif detail == "metatarsal":
            return SpecimenOrgan.metatarsal, "1"
    elif organ is SpecimenOrgan.manus:
        if detail == "phalanges":
            return SpecimenOrgan.phalanx_manus, ">1"
        elif detail == "phalanx":
            return SpecimenOrgan.phalanx_manus, "1"
        elif re.fullmatch(r"^(L |R )?Mc(I|II|III|IV|V)$", detail):
            return SpecimenOrgan.metacarpal, detail
        elif detail == "metacarpals":
            return SpecimenOrgan.metacarpal, ">1"
        elif detail == "metacarpal":
            return SpecimenOrgan.metacarpal, "1"
    if organ is not SpecimenOrgan.in_alcohol:
        if detail.startswith(("L ", "R ")):
            new_detail, text = detail.split(" ", maxsplit=1)
        else:
            new_detail = ""
            text = detail
        try:
            new_organ = SpecimenOrgan[text]
        except KeyError:
            pass
        else:
            return new_organ, new_detail
    if match := re.fullmatch(r"partial ([a-z]+)", detail):
        return organ, f"{match.group(1)} part"
    if match := re.fullmatch(r"([a-z]+) fragments", detail):
        return organ, f"{match.group(1)} parts"
    if match := re.fullmatch(r"([a-z]+) fragment", detail):
        return organ, f"{match.group(1)} part"
    if detail == "fragments":
        return organ, "parts"
    if detail == "fragment":
        return organ, "part"
    return None


def maybe_replace_tags(
    organ: SpecimenOrgan, detail: str, condition: str | None
) -> tuple[list[TypeTag.Organ], list[str]]:  # type: ignore[name-defined]
    if organ in (SpecimenOrgan.mandible, SpecimenOrgan.dentary):
        if " with " in detail:
            before, after = detail.split(" with ", maxsplit=1)
            new_organ = SpecimenOrgan.dentary
            rgx = r"[a-z][\da-z\-\?]*(, [a-z][\da-z\-\?]*)*"
            if match := re.fullmatch(
                "([LR]) (dentary|lower jaw|ramus|dentary fragment|mandible)", before
            ):
                side = match.group(1)
            elif before in ("dentary", "lower jaw", "ramus", "dentary fragment"):
                side = ""
            elif before == "mandible" and "L" in detail and "R" in detail:
                side = ""
                new_organ = SpecimenOrgan.mandible
                rgx = r"[LR][a-z][\da-z\-\?]*(, [LR][a-z][\da-z\-\?]*)*"
            else:
                side = None
            if side is not None:
                if re.fullmatch(rgx, after):
                    pieces = after.split(", ")
                    new_detail = ", ".join(side + piece for piece in pieces)
                    return [TypeTag.Organ(new_organ, new_detail, condition)], []
    if organ in (SpecimenOrgan.maxilla, SpecimenOrgan.skull):
        if " with " in detail:
            before, after = detail.split(" with ", maxsplit=1)
            new_organ = SpecimenOrgan.maxilla
            if match := re.fullmatch("([LR]) (maxilla|upper jaw|palate)", before):
                side = match.group(1)
                if match.group(2) == "palate":
                    new_organ = SpecimenOrgan.palate
            elif before in ("maxilla", "upper jaw"):
                side = ""
            elif before == "palate":
                side = ""
                new_organ = SpecimenOrgan.palate
            else:
                side = None
            if side is not None:
                if re.fullmatch(r"[A-Z][\dA-Z\-\?]*(, [A-Z][\dA-Z\-\?]*)*", after):
                    pieces = after.split(", ")
                    new_detail = ", ".join(side + piece for piece in pieces)
                    return [TypeTag.Organ(new_organ, new_detail, condition)], []
    if organ is SpecimenOrgan.skull:
        if " with " in detail:
            before, after = detail.split(" with ", maxsplit=1)
            if re.fullmatch(r"(partial |broken )?(skull|cranium)", before):
                if re.fullmatch(r"[A-Z][\dA-Z\-\?]*(, [A-Z][\dA-Z\-\?]*)*", after):
                    return [TypeTag.Organ(SpecimenOrgan.skull, after, condition)], []
    parts = detail.split(", ")
    remaining_parts = []
    new_tags = []
    if len(parts) == 1 or (
        not condition and "L" not in parts and "R" not in parts and "(" not in detail
    ):
        for part in parts:
            new_pair = specify_organ(organ, part)
            if new_pair is None:
                remaining_parts.append(part)
            else:
                new_organ, new_detail = new_pair
                new_tags.append(TypeTag.Organ(new_organ, new_detail, condition))
    else:
        remaining_parts = parts
    return new_tags, remaining_parts


class ParseException(Exception):
    pass


@dataclass(kw_only=True, frozen=True)
class OrganCount:
    more_than: bool = False
    approximately: bool = False
    count: int

    def __str__(self) -> str:
        pieces = []
        if self.more_than:
            pieces.append(">")
        if self.approximately:
            pieces.append("~")
        pieces.append(str(self.count))
        return "".join(pieces)

    def validate(self, organ: SpecimenOrgan, parsed: ParsedOrgan) -> Iterable[str]:
        if self.count < 1:
            yield f"count must be a positive integer, not {self.count}"
        if organ in COUNTED_ORGANS:
            return
        if isinstance(parsed.base, Shell):
            return
        if organ in TOOTHED_ORGANS:
            if isinstance(parsed.base, (Tooth, RawText)) or parsed.base is None:
                return
            if isinstance(parsed.base, AlternativeOrgan) and all(
                isinstance(poss, Tooth) for poss in parsed.base.possibilities
            ):
                return
        yield f"organ {organ.name!r} does not allow a count: {self.count}"

    def sort_key(self) -> tuple[object, ...]:
        return ("OrganCount", self.count, self.more_than, self.approximately)


@dataclass(frozen=True)
class Metacarpal:
    position: int

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if organ is not SpecimenOrgan.metacarpal:
            yield "metacarpals must be marked as metacarpals"
        if not (1 <= self.position <= 5):
            yield f"metacarpal out of range: {self.position}"

    def sort_key(self) -> tuple[object, ...]:
        return ("Metacarpal", self.position)

    def __str__(self) -> str:
        return "Mc" + helpers.make_roman_numeral(self.position)


@dataclass(frozen=True)
class Metatarsal:
    position: int

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if organ is not SpecimenOrgan.metatarsal:
            yield "metatarsals must be marked as metatarsals"
        if not (1 <= self.position <= 5):
            yield f"metatarsal out of range: {self.position}"

    def sort_key(self) -> tuple[object, ...]:
        return ("Metatarsal", self.position)

    def __str__(self) -> str:
        return "Mt" + helpers.make_roman_numeral(self.position)


ORDERED_TOOTH_CATEGORIES = [
    "di",
    "i",
    "if",
    "dc",
    "c",
    "a",
    "dp",
    "p",
    "pmf",
    "m",
    "mf",
]
ToothCategory = Literal["i", "c", "p", "m", "di", "dc", "dp", "a", "if", "mf", "pmf"]


@dataclass(kw_only=True, frozen=True)
class Tooth:
    side: Literal["L", "R", None] = None
    uncertain_category: bool = False
    category: ToothCategory
    is_upper: bool
    uncertain_position: bool = False
    position: int | None = None

    def __str__(self, *, skip_side: bool = False, skip_category: bool = False) -> str:
        parts: list[str] = []
        if not skip_side and self.side is not None:
            parts.append(self.side)
        if not skip_category:
            if self.uncertain_category:
                parts.append("?")
            category: str = self.category
            if self.is_upper:
                category = category.upper()
            parts.append(category)
        if self.uncertain_position:
            parts.append("?")
        if self.position is not None:
            parts.append(str(self.position))
        return "".join(parts)

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if organ not in TOOTHED_ORGANS:
            yield f"teeth cannot be present in {organ.name!r}"
        if self.uncertain_position and self.position is None:
            yield "invalidly positioned ?"
        if self.is_upper and organ in (SpecimenOrgan.mandible, SpecimenOrgan.dentary):
            yield f"upper teeth are not allowed in {organ.name!r}"
        if not self.is_upper and organ in (
            SpecimenOrgan.maxilla,
            SpecimenOrgan.premaxilla,
            SpecimenOrgan.skull,
            SpecimenOrgan.palate,
        ):
            yield f"lower teeth are not allowed in {organ.name!r}"

    def sort_key(self) -> tuple[object, ...]:
        return (
            "Tooth",
            self.is_upper,
            self.side or "",
            ORDERED_TOOTH_CATEGORIES.index(self.category),
            self.position or 0,
            self.uncertain_category,
            self.uncertain_position,
        )


@dataclass(frozen=True)
class ToothRange:
    start: Tooth
    end: Tooth

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if self.start.is_upper != self.end.is_upper:
            yield "range contains both lower and upper teeth"
        if self.start.side != self.end.side:
            yield "range contains both left and right teeth"
        start_index = ORDERED_TOOTH_CATEGORIES.index(self.start.category)
        end_index = ORDERED_TOOTH_CATEGORIES.index(self.end.category)
        if start_index > end_index:
            yield f"invalid range: {self.end.category} is before {self.start.category}"
        if self.start.category == self.end.category:
            if self.start.position is None:
                yield (
                    "invalid range: start and end are in same category, but no start"
                    " position given"
                )
            elif self.end.position is None:
                yield (
                    "invalid range: start and end are in same category, but no end"
                    " position given"
                )
            elif self.start.position >= self.end.position:
                yield f"invalid range from {self.start.position} to {self.end.position}"
        yield from self.start.validate(organ)
        yield from self.end.validate(organ)

    def sort_key(self) -> tuple[object, ...]:
        return self.start.sort_key() + self.end.sort_key()

    def __str__(self) -> str:
        skip_side = self.start.side == self.end.side
        skip_category = (
            skip_side
            and not self.start.uncertain_category
            and not self.end.uncertain_category
            and self.start.category == self.end.category
        )
        # https://github.com/python/mypy/issues/16735
        end = self.end.__str__(skip_side=skip_side, skip_category=skip_category)  # type: ignore[call-arg]
        return f"{self.start}-{end}"


SHELL_TEXTS = (
    # cingulates
    "cephalic shield",
    # both
    "carapace",
    "plastron",
    # turtles
    "bridge",
    "costal",
    "marginal",
    "neural",
    "epineural",
    "nuchal",
    "peripheral",
    "pleural",
    "pygal",
    "suprapygal",
    "epiplastron",
    "entoplastron",
    "hyoplastron",
    "hypoplastron",
    "hyo-hypoplastron",
    "xiphiplastron",
)


@dataclass(frozen=True)
class Shell:
    text: str
    position: int | None = None

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if organ is not SpecimenOrgan.shell:
            yield f"{organ.name!r} is not a shell"

    def sort_key(self) -> tuple[object, ...]:
        return ("Shell", self.text, self.position or 0)

    def __str__(self) -> str:
        if self.position is not None:
            return f"{self.text} {self.position}"
        return self.text

    @classmethod
    def maybe_parse(cls, text: str) -> Shell | None:
        if text in SHELL_TEXTS:
            return Shell(text)
        if match := re.fullmatch(r"([a-z]+) (\d+)", text):
            if match.group(1) in SHELL_TEXTS:
                return Shell(match.group(1), int(match.group(2)))
        return None


VERTEBRA_ABBREVIATIONS = {
    "cervical": "C",
    "dorsal": "D",
    "thoracic": "T",
    "lumbar": "L",
    "sacral": "S",
    "caudal": "Ca",
}
VertebraGroup = Literal["C", "D", "T", "L", "S", "Ca", "sternal"]
ALLOWED_GROUPS = set(get_args(VertebraGroup))
ORDERED_GROUPS: list[VertebraGroup] = ["C", "D", "T", "L", "S", "Ca", "sternal"]

# TODO: hemal arches and chevrons are the same? If so, which term should we use?
AfterText = Literal[
    "centrum",
    "neural spine",
    "neural arch",
    "hemal arch",
    "chevron",
    "epiphysis",
    "neurapophysis",
    "diapophysis",
    "intercentrum",
]
AFTER_TEXT_VARIANTS: dict[str, AfterText] = {
    "chevrons": "chevron",
    "haemal arches": "hemal arch",
    "haemal arch": "hemal arch",
    "hemal arches": "hemal arch",
    "neural spines": "neural spine",
    "neural arches": "neural arch",
    "centra": "centrum",
    "epiphyses": "epiphysis",
    "neurapophyses": "neurapophysis",
    "diapophyses": "diapophysis",
    **{text: text for text in get_args(AfterText)},
}


@dataclass(frozen=True)
class Vertebra:
    group: VertebraGroup | None = None
    position: int | None = None
    after_text: AfterText | None = None

    def __str__(self) -> str:
        parts: list[str] = []
        if self.group is not None:
            parts.append(self.group)
        if self.position is not None:
            parts.append(str(self.position))
        if self.after_text is not None:
            if parts:
                parts.append(" ")
            parts.append(self.after_text)
        return "".join(parts)

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if self.position is not None and self.group is None:
            yield "if position is given, group must be given"
        if self.group is None and self.after_text is None:
            yield "at least one of group and after_text must be set"
        if self.group == "sternal" and organ is not SpecimenOrgan.rib:
            yield "there are no sternal vertebrae"

    def sort_key(self) -> tuple[object, ...]:
        if self.group is None:
            group = len(ORDERED_GROUPS)
        else:
            group = ORDERED_GROUPS.index(self.group)
        return ("Vertebra", group, self.position or float("inf"), self.after_text or "")

    @classmethod
    def maybe_parse(cls, text: str) -> Vertebra | VertebraRange | None:
        if "-" in text:
            left, right = text.split("-", maxsplit=1)
            left_vert = cls._maybe_parse_single(left)
            if left_vert is None:
                return None
            if right.isnumeric():
                return VertebraRange(left_vert, cls(left_vert.group, int(right)))
            right_vert = cls._maybe_parse_single(right)
            if right_vert is None:
                return None
            return VertebraRange(left_vert, right_vert)
        return cls._maybe_parse_single(text)

    @classmethod
    def _maybe_parse_single(cls, text: str) -> Vertebra | None:
        if text in AFTER_TEXT_VARIANTS:
            return cls(after_text=AFTER_TEXT_VARIANTS[text])
        after_text = None
        for variant, after_text_candidate in AFTER_TEXT_VARIANTS.items():
            if text.endswith(" " + variant):
                text = text.removesuffix(" " + variant)
                after_text = after_text_candidate
                break
        if text == "atlas":
            return cls("C", 1, after_text)
        elif text == "axis":
            return cls("C", 2, after_text)
        if text in ALLOWED_GROUPS:
            return cls(cast(VertebraGroup, text), after_text=after_text)
        if group := VERTEBRA_ABBREVIATIONS.get(text.rstrip("s")):
            return cls(cast(VertebraGroup, group), after_text=after_text)
        if match := re.fullmatch(r"([A-Z]a?)(\d+)", text):
            group = match.group(1)
            if group in ALLOWED_GROUPS:
                return cls(
                    cast(VertebraGroup, group),
                    int(match.group(2)),
                    after_text=after_text,
                )
        return None


@dataclass(frozen=True)
class VertebraRange:
    start: Vertebra
    end: Vertebra

    def __str__(self) -> str:
        if self.start.group == self.end.group:
            return f"{self.start}-{self.end.position}"
        return f"{self.start}-{self.end}"

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        yield from self.start.validate(organ)
        yield from self.start.validate(organ)
        if self.start.group is None:
            yield "group must be set for vertebrae in a range"
        if self.end.group is None:
            yield "group must be set for vertebrae in a range"
        if (
            self.start.group == self.end.group
            and self.start.position is not None
            and self.end.position is not None
        ):
            if self.start.position >= self.end.position:
                yield f"invalid vertebral range: {self}"

    def sort_key(self) -> tuple[object, ...]:
        return self.start.sort_key() + self.end.sort_key()


@dataclass(frozen=True)
class Phalanx:
    digit: int
    position: int | Literal["ungual"] | None = None

    def __str__(self) -> str:
        if self.position is None:
            return helpers.make_roman_numeral(self.digit)
        return f"{helpers.make_roman_numeral(self.digit)}-{self.position}"

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if organ not in PHALANX_ORGANS:
            yield f"{organ.name!r} cannot have phalanges"
        if not 1 <= self.digit <= 5:
            yield f"invalid digit {self.digit}"
        if isinstance(self.position, int) and not 1 <= self.position <= 5:
            yield f"invalid position {self.position}"

    def sort_key(self) -> tuple[object, ...]:
        pos: float | int
        match self.position:
            case None:
                pos = 0
            case "ungual":
                pos = float("inf")
            # https://github.com/python/mypy/issues/16736
            case pos:  # type: ignore[misc]
                pass
        # TODO pyanalyze bug
        # static analysis: ignore[possibly_undefined_name]
        return ("Phalanx", self.digit, pos)

    @classmethod
    def maybe_parse(cls, text: str) -> Phalanx | None:
        if "-" in text:
            digit_text, position_text = text.split("-", maxsplit=1)
            try:
                digit = helpers.parse_roman_numeral(digit_text)
            except ValueError:
                return None
            if position_text == "ungual":
                return Phalanx(digit, "ungual")
            if not position_text.isnumeric() or len(position_text) != 1:
                return None
            try:
                position = int(position_text)
            except ValueError:
                return None
            return Phalanx(digit, position)
        else:
            try:
                digit = helpers.parse_roman_numeral(text)
            except ValueError:
                return None
            else:
                return Phalanx(digit)
        return None


@dataclass(frozen=True)
class AlternativeOrgan:
    possibilities: Sequence[OrganBase]

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if len(self.possibilities) < 2:
            yield f"must have at least 2 possibilities: {self.possibilities}"
        for possibility in self.possibilities:
            yield from _validate_organ_base(possibility, organ)

    def sort_key(self) -> tuple[object, ...]:
        return tuple(
            itertools.chain.from_iterable(
                _organ_base_sort_key(poss) for poss in self.possibilities
            )
        )

    def __hash__(self) -> int:
        return hash(tuple(self.possibilities))

    def __str__(self) -> str:
        if len(self.possibilities) > 1 and all(
            isinstance(possibility, Tooth) for possibility in self.possibilities
        ):
            first, *rest = sorted(
                cast(Sequence[Tooth], self.possibilities), key=_organ_base_sort_key
            )
            skip_side = True
            skip_category = not first.uncertain_category
            for poss in rest:
                skip_side = skip_side and first.side == poss.side
                skip_category = (
                    skip_side
                    and skip_category
                    and not poss.uncertain_category
                    and first.category == poss.category
                    and first.is_upper == poss.is_upper
                )
            rest_string = "/".join(
                # https://github.com/python/mypy/issues/16735
                poss.__str__(skip_side=skip_side, skip_category=skip_category)  # type: ignore[call-arg]
                for poss in rest
            )
            return f"{first}/{rest_string}"
        return "/".join(str(poss) for poss in self.possibilities)


@dataclass(frozen=True)
class RawText:
    text: str

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if organ not in CHECKED_ORGANS:
            return
        if organ in ALLOW_RESTRICTED_RAW_TEXT and re.fullmatch(
            r"[a-z]+([ \-][a-z]+)?", self.text
        ):
            return
        yield f"unrecognized text {self.text!r} for organ {organ.name!r}"

    def sort_key(self) -> tuple[object, ...]:
        return ("RawText", self.text)

    def __str__(self) -> str:
        return self.text


OrganBaseLiteral = Literal[
    "shaft",
    "pelvic",
    "pectoral",
    "fore",
    "hind",
    "edentulous",
    "complete",
    "symphysis",
    "premaxillary",
    "maxillary",
    "mandibular",
    "sacrum",
    "synsacrum",
    "pygostyle",
    "column",
    "ungual",
]
_LITERAL_ORGAN_BASES = set(get_args(OrganBaseLiteral))
OrganBase = (
    Metacarpal
    | Metatarsal
    | Phalanx
    | Vertebra
    | VertebraRange
    | Tooth
    | ToothRange
    | Shell
    | AlternativeOrgan
    | RawText
    | OrganBaseLiteral
)


def _organ_base_sort_key(base: OrganBase | None) -> tuple[object, ...]:
    match base:
        case None:
            return ("",)
        case str():  # static analysis: ignore[impossible_pattern]
            return ("str", base)
        case _:
            # TODO: pyanalyze bug
            assert not isinstance(base, str)
            return base.sort_key()
    assert False, "unreachable"


def _validate_organ_base(base: OrganBase | None, organ: SpecimenOrgan) -> Iterable[str]:
    match base:
        case None:
            pass
        case "shaft":
            if organ not in LONG_BONES:
                yield f"'shaft' is not valid for organ {organ.name!r}"
        case "pelvic" | "pectoral":
            if organ not in (SpecimenOrgan.girdle, SpecimenOrgan.osteoderm):
                yield f"{base!r} is valid only for 'girdle', not {organ.name!r}"
        case "fore" | "hind":
            if organ is not SpecimenOrgan.limb:
                yield f"{base!r} is valid only for 'limb', not {organ.name!r}"
        case "edentulous" | "complete":
            if organ not in (
                SpecimenOrgan.mandible,
                SpecimenOrgan.dentary,
                SpecimenOrgan.maxilla,
                SpecimenOrgan.premaxilla,
                SpecimenOrgan.skull,
                SpecimenOrgan.palate,
            ):
                yield (
                    f"{base!r} is valid only for tooth-bearing organs, not"
                    f" {organ.name!r}"
                )
        case "symphysis":
            if organ not in (SpecimenOrgan.mandible, SpecimenOrgan.dentary):
                yield f"{base!r} is valid only for lower jaws, not {organ.name!r}"
        case "premaxillary" | "maxillary" | "mandibular":
            if organ is not SpecimenOrgan.tooth:
                yield f"{base!r} is valid only for 'tooth', not {organ.name!r}"
        case "sacrum" | "pygostyle" | "column" | "synsacrum":
            if organ is not SpecimenOrgan.vertebra:
                yield f"{base!r} is valid only for 'vertebra', not {organ.name!r}"
        case "ungual":
            if organ not in PHALANX_ORGANS:
                yield f"{base!r} is valid only for phalanges, not {organ.name!r}"
        case _:
            yield from base.validate(organ)


SINGLE_TOOTH_REGEX = re.compile(
    r"""
        (?P<side>[LR])?
        (?P<category_doubt>\??)
        (?P<category>[A-Za-z]{0,3})
        (?P<position_doubt>\??)
        (?P<position>\d*)
    """,
    re.VERBOSE,
)


def _parse_single_tooth(
    text: str,
    *,
    side_preset: Literal["L", "R"] | None = None,
    category_preset: ToothCategory | None = None,
    is_upper_preset: bool | None = None,
) -> Tooth | None:
    if match := SINGLE_TOOTH_REGEX.fullmatch(text):
        if not (side := match.group("side")):
            side = side_preset
        if raw_category := match.group("category"):
            if raw_category.isupper():
                is_upper = True
            elif raw_category.islower():
                is_upper = False
            elif (
                raw_category[0] == "d"
                and len(raw_category) == 2
                and raw_category[1].isupper()
            ):
                is_upper = True
            else:
                raise ParseException(f"mixed case in {raw_category!r}")
            category = raw_category.lower()
        elif category_preset is not None and is_upper_preset is not None:
            is_upper = is_upper_preset
            category = category_preset
        else:
            return None
        if category not in ORDERED_TOOTH_CATEGORIES:
            return None
        if match.group("position"):
            position = int(match.group("position"))
        else:
            position = None

        # Position should not be set for canines
        if position == 1 and category in ("c", "dc"):
            position = None
        return Tooth(
            side=cast(Literal["L", "R"], side),
            uncertain_category=bool(match.group("category_doubt")),
            category=cast(ToothCategory, category),
            is_upper=is_upper,
            uncertain_position=bool(match.group("position_doubt")),
            position=position,
        )
    return None


def _parse_tooth(text: str) -> Tooth | ToothRange | AlternativeOrgan | None:
    if "/" in text:
        parts = text.split("/")
        first = _parse_single_tooth(parts[0])
        if first is None:
            return None
        possibilities = [first]
        for part in parts[1:]:
            tooth = _parse_single_tooth(
                part,
                side_preset=first.side,
                is_upper_preset=first.is_upper,
                category_preset=first.category,
            )
            if tooth is None:
                return None
            possibilities.append(tooth)
        return AlternativeOrgan(possibilities)
    if "-" in text:
        start, end = text.split("-", maxsplit=1)
        start_tooth = _parse_single_tooth(start)
        if start_tooth is None:
            return None
        end_tooth = _parse_single_tooth(
            end,
            side_preset=start_tooth.side,
            is_upper_preset=start_tooth.is_upper,
            category_preset=start_tooth.category,
        )
        if end_tooth is None:
            return None
        return ToothRange(start_tooth, end_tooth)
    return _parse_single_tooth(text)


def _parse_organ_base(text: str, organ: SpecimenOrgan) -> OrganBase:
    if text in _LITERAL_ORGAN_BASES:
        return cast(OrganBase, text)
    if organ in TOOTHED_ORGANS:
        tooth = _parse_tooth(text)
        if tooth is not None:
            return tooth
    if organ in (SpecimenOrgan.vertebra, SpecimenOrgan.rib):
        if vertebra := Vertebra.maybe_parse(text):
            return vertebra
    if phalanx := Phalanx.maybe_parse(text):
        return phalanx
    if "/" in text:
        pieces = text.split("/")
        return AlternativeOrgan([_parse_organ_base(piece, organ) for piece in pieces])
    if text.startswith("Mc"):
        numeral = text.removeprefix("Mc")
        try:
            position = helpers.parse_roman_numeral(numeral)
        except ValueError as e:
            raise ParseException(str(e)) from None
        return Metacarpal(position)
    if text.startswith("Mt"):
        numeral = text.removeprefix("Mt")
        try:
            position = helpers.parse_roman_numeral(numeral)
        except ValueError as e:
            raise ParseException(str(e)) from None
        return Metatarsal(position)
    if shell := Shell.maybe_parse(text):
        return shell
    return RawText(text)


@dataclass(kw_only=True, frozen=True)
class ParsedOrgan:
    is_uncertain: bool = False
    count: OrganCount | None = None
    side: Literal["L", "R", None] = None
    anatomical_direction: Literal[
        "proximal", "distal", "anterior", "posterior", None
    ] = None
    base: OrganBase | None = None
    part_text: Literal["part", "parts", None] = None

    def validate(self, organ: SpecimenOrgan) -> Iterable[str]:
        if self.count is not None:
            yield from self.count.validate(organ, self)
        if (
            self.side is not None
            and organ not in PAIRED_ORGANS
            and not isinstance(self.base, (RawText, Shell))
            and self.part_text is None
            and self.anatomical_direction is None
        ):
            yield f"organ {organ.name!r} does not allow a left/right side: {self.side}"
        match self.anatomical_direction:
            case "proximal" | "distal":
                if organ not in PROXIMAL_DISTAL_ORGANS and not (
                    organ is SpecimenOrgan.shell and isinstance(self.base, RawText)
                ):
                    yield (
                        f"organ {organ.name!r} does not allow proximal/distal"
                        f" specification: {self.anatomical_direction}"
                    )
            case "anterior" | "posterior":
                if organ not in ALLOW_ANTERIOR_POSTERIOR:
                    yield (
                        f"organ {organ.name!r} does not allow anterior/posterior"
                        f" specification: {self.anatomical_direction}"
                    )
            case None:
                pass
            case _:
                assert_never(self.anatomical_direction)
        yield from _validate_organ_base(self.base, organ)

    def sort_key(self) -> tuple[object, ...]:
        return (
            self.side or "",
            _organ_base_sort_key(self.base),
            self.is_uncertain,
            self.count.sort_key() if self.count is not None else ("",),
            self.anatomical_direction or "",
            self.part_text or "",
        )

    def __str__(self) -> str:
        parts = [
            self.count,
            self.side,
            self.anatomical_direction,
            self.base,
            self.part_text,
        ]
        text = " ".join(str(part) for part in parts if part is not None)
        if self.is_uncertain:
            return "?" + text
        else:
            return text

    @classmethod
    def parse(cls, text: str, organ: SpecimenOrgan) -> ParsedOrgan:
        is_uncertain = False
        count = base = part_text = side = anatomical_direction = None
        if text.startswith("?"):
            is_uncertain = True
            text = text.removeprefix("?")
        affixes: tuple[str, ...] = (
            "part",
            "parts",
            "proximal",
            "distal",
            "anterior",
            "posterior",
        )
        if organ is not SpecimenOrgan.vertebra:
            affixes = affixes + ("L", "R")
        while True:
            text, affix = remove_affix(text, affixes)
            if affix is not None:
                match affix:
                    case "L" | "R":
                        if side is not None:
                            raise ParseException(
                                f"multiple values for side: {side} and {affix}"
                            )
                        side = affix
                    case "part" | "parts":
                        if part_text is not None:
                            raise ParseException(
                                f"multiple values for part: {part_text} and {affix}"
                            )
                        part_text = affix
                    case "proximal" | "distal" | "anterior" | "posterior":
                        if anatomical_direction is not None:
                            raise ParseException(
                                "multiple values for anatomical direction:"
                                f" {anatomical_direction} and {affix}"
                            )
                        anatomical_direction = affix
            elif match := re.match(
                r"^(?P<gt>>?)(?P<approx>~?)(?P<count>\d+)(?= |$)(?P<rest>.*$)", text
            ):
                if count is not None:
                    raise ParseException("multiple counts found")
                count = OrganCount(
                    more_than=bool(match.group("gt")),
                    approximately=bool(match.group("approx")),
                    count=int(match.group("count")),
                )
                text = match.group("rest").strip()
            else:
                break
        if text:
            base = _parse_organ_base(text, organ)
        return ParsedOrgan(
            is_uncertain=is_uncertain,
            count=count,
            side=side,
            anatomical_direction=anatomical_direction,
            base=base,
            part_text=part_text,
        )


def remove_affix(text: str, affixes: Sequence[str]) -> tuple[str, str | None]:
    if text in affixes:
        return "", text
    for affix in affixes:
        new_text = text.removeprefix(affix + " ")
        if new_text != text:
            return new_text, affix
        new_text = text.removesuffix(" " + affix)
        if new_text != text:
            return new_text, affix
    return text, None


def parse_organ_detail(detail: str, organ: SpecimenOrgan) -> list[ParsedOrgan]:
    parts = detail.split(", ")
    return [ParsedOrgan.parse(part, organ) for part in parts]


def check_organ_tag_with_parser(
    organ: SpecimenOrgan, detail: str
) -> Generator[str, None, str]:
    if organ not in CHECKED_ORGANS:
        return detail
    try:
        parsed_list = parse_organ_detail(detail, organ)
    except ParseException as e:
        yield f"{e} while parsing {detail!r}"
        return detail
    for parsed in parsed_list:
        for issue in parsed.validate(organ):
            yield f"{issue} (from text {detail!r})"
    return ", ".join(
        str(parsed) for parsed in sorted(set(parsed_list), key=ParsedOrgan.sort_key)
    )


def check_organ_tag(tag: TypeTag.Organ) -> Generator[str, None, list[TypeTag.Organ]]:  # type: ignore[name-defined]
    if not tag.detail:
        return [tag]
    detail = tag.detail
    for rgx, replacement in ORGAN_REPLACEMENTS:
        detail = re.sub(rgx, replacement, detail)
    condition = tag.condition
    if not condition:
        if match := re.fullmatch(r"^([^\(\)]+) \(([^\(\)]+)\)", detail):
            detail = match.group(1)
            condition = match.group(2)
    organ = tag.organ
    new_tags, remaining_parts = maybe_replace_tags(organ, detail, condition)
    if not remaining_parts:
        return new_tags
    detail = ", ".join(remaining_parts)
    # Move museum numbers from "detail" to "condition"
    if (
        re.fullmatch(
            r"^[A-Z]{2,}(-[A-Z]+)? [A-Z]?(\d{2,}[\-\d/\.]*|\d(\.\d+)+)[a-z]?$", detail
        )
        and not condition
    ):
        condition = detail
        detail = ""
    detail = yield from check_organ_tag_with_parser(organ, detail)
    new_tags.append(TypeTag.Organ(organ, detail, condition))
    return new_tags


@make_linter("type_tags")
def check_type_tags_for_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.type_tags:
        return
    tags: list[TypeTag] = []
    original_tags = list(nam.type_tags)
    by_type: dict[type[TypeTag], list[TypeTag]] = {}
    for tag in original_tags:
        by_type.setdefault(type(tag), []).append(tag)
        for arg_name, art in get_tag_fields_of_type(tag, Article):
            if art.kind is ArticleKind.removed:
                print(f"{nam} references a removed Article in {tag}")
                yield f"bad article in tag {tag}"
            elif art.kind is ArticleKind.redirect:
                print(f"{nam} references a redirected Article in {tag} -> {art.parent}")
                if art.parent is None or art.parent.should_skip():
                    yield f"bad redirected article in tag {tag}"
                elif cfg.autofix:
                    tag = replace_arg(tag, arg_name, art.parent)
        for arg_name, tag_nam in get_tag_fields_of_type(tag, Name):
            if tag_nam.is_invalid():
                print(
                    f"{nam} references a removed Name in argument {arg_name} to {tag}"
                )
                yield f"bad name in tag {tag}"
        for arg_name, coll in get_tag_fields_of_type(tag, Collection):
            if coll.must_use_children():
                yield f"must use child collection in argument {arg_name} to {tag}"

        if isinstance(
            tag,
            (
                TypeTag.FormerRepository,
                TypeTag.FutureRepository,
                TypeTag.ExtraRepository,
            ),
        ):
            if tag.repository in nam.get_repositories():
                yield (
                    f"{tag.repository} is marked as a {type(tag).__name__}, but it is"
                    " the current repository"
                )

        if (
            isinstance(
                tag,
                (
                    TypeTag.LocationDetail,
                    TypeTag.SpecimenDetail,
                    TypeTag.EtymologyDetail,
                    TypeTag.TypeSpeciesDetail,
                ),
            )
            and tag.text == ""
            and tag.source is None
        ):
            message = f"{tag} has no text and no source"
            if cfg.autofix:
                print(f"{nam}: {message}")
                continue
            else:
                yield message

        if isinstance(tag, TypeTag.ProbableRepository) and nam.collection is not None:
            message = f"has {tag} but colllection is set to {nam.collection}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                continue
            else:
                yield message

        if isinstance(tag, TypeTag.CommissionTypeDesignation):
            if nam.type != tag.type:
                print(
                    f"{nam} has {nam.type} as its type, but the Commission has"
                    f" designated {tag.type}"
                )
                if cfg.autofix:
                    nam.type = tag.type
            if (
                nam.genus_type_kind
                != TypeSpeciesDesignation.designated_by_the_commission
            ):
                print(
                    f"{nam} has {nam.genus_type_kind}, but its type was set by the"
                    " Commission"
                )
                if cfg.autofix:
                    nam.genus_type_kind = (
                        TypeSpeciesDesignation.designated_by_the_commission  # type: ignore
                    )
            tags.append(tag)
        elif isinstance(tag, TypeTag.Date):
            date = tag.date
            try:
                date = helpers.standardize_date(date)
            except ValueError:
                print(f"{nam} has date {tag.date}, which cannot be parsed")
                yield "unparseable date"
            if date is None:
                continue
            tags.append(TypeTag.Date(date))
        elif isinstance(tag, TypeTag.Altitude):
            if (
                not re.match(r"^-?\d+([\-\.]\d+)?$", tag.altitude)
                or tag.altitude == "000"
            ):
                print(f"{nam} has altitude {tag}, which cannot be parsed")
                yield f"bad altitude tag {tag}"
            tags.append(tag)
        elif isinstance(tag, TypeTag.LocationDetail):
            coords = helpers.extract_coordinates(tag.text)
            if coords and not any(
                isinstance(t, TypeTag.Coordinates) for t in original_tags
            ):
                tags.append(TypeTag.Coordinates(coords[0], coords[1]))
                print(
                    f"{nam}: adding coordinates {tags[-1]} extracted from {tag.text!r}"
                )
            tags.append(tag)
        elif isinstance(tag, TypeTag.Coordinates):
            try:
                lat = helpers.standardize_coordinates(tag.latitude, is_latitude=True)
            except helpers.InvalidCoordinates as e:
                print(f"{nam} has invalid latitude {tag.latitude}: {e}")
                yield f"invalid latitude {tag.latitude}"
                lat = tag.latitude
            try:
                longitude = helpers.standardize_coordinates(
                    tag.longitude, is_latitude=False
                )
            except helpers.InvalidCoordinates as e:
                print(f"{nam} has invalid longitude {tag.longitude}: {e}")
                yield f"invalid longitude {tag.longitude}"
                longitude = tag.longitude
            tags.append(TypeTag.Coordinates(lat, longitude))
        elif isinstance(tag, TypeTag.LSIDName):
            lsid = clean_lsid(tag.text)
            tags.append(TypeTag.LSIDName(lsid))
            if not is_valid_lsid(lsid):
                yield f"invalid LSID {lsid}"
        elif isinstance(tag, TypeTag.TypeSpecimenLink):
            if not tag.url.startswith(("http://", "https://")):
                yield f"invalid type specimen URL {tag.url!r}"
            tags.append(TypeTag.TypeSpecimenLink(fix_type_specimen_link(tag.url)))
        elif isinstance(tag, TypeTag.TypeSpecimenLinkFor):
            if not tag.url.startswith(("http://", "https://")):
                yield f"invalid type specimen URL {tag.url!r}"
            tags.append(
                TypeTag.TypeSpecimenLinkFor(
                    fix_type_specimen_link(tag.url), tag.specimen
                )
            )
        elif isinstance(tag, TypeTag.Organ) and tag.detail:
            new_tags = yield from check_organ_tag(tag)
            tags += new_tags
        else:
            tags.append(tag)
        # TODO: for lectotype and subsequent designations, ensure the earliest valid one is used.

    for tag_type, tags_of_type in by_type.items():
        if tag_type in UNIQUE_TAGS and len(tags_of_type) > 1:
            yield f"has multiple tags of type {tag_type}: {tags_of_type}"
    if nam.collection is not None and nam.collection.id == MULTIPLE_ID:
        repos = by_type.get(TypeTag.Repository, [])
        if len(repos) < 2:
            yield (
                "name with collection 'multiple' must have multiple Repository tags:"
                f" {repos}"
            )
    elif TypeTag.Repository in by_type:
        yield f"name may not have Repository tags: {by_type[TypeTag.Repository]}"

    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {nam}")
            getinput.print_diff(sorted(original_tags), tags)
        if cfg.autofix:
            nam.type_tags = tags  # type: ignore


@make_linter("dedupe_tags")
def dedupe_and_sort_tags(nam: Name, cfg: LintConfig) -> Iterable[str]:
    original_tags = list(nam.type_tags)
    organ_tags_to_merge = defaultdict(list)
    all_tags = set()
    for tag in nam.type_tags:
        if (
            isinstance(tag, TypeTag.Organ)
            and tag.organ in CHECKED_ORGANS
            and not tag.condition
        ):
            organ_tags_to_merge[tag.organ].append(tag)
        else:
            all_tags.add(tag)
    for organ, group in organ_tags_to_merge.items():
        detail = []
        for tag in group:
            if tag.detail:
                detail.append(tag.detail)
        all_tags.add(TypeTag.Organ(organ, ", ".join(detail), ""))
    tags = sorted(all_tags)
    if tags != original_tags:
        if set(tags) != set(original_tags):
            print(f"changing tags for {nam}")
            getinput.print_diff(sorted(original_tags), tags)
        if cfg.autofix:
            nam.type_tags = tags  # type: ignore
    return []


def fix_type_specimen_link(url: str) -> str:
    if url.startswith((
        "http://arctos.database.museum/",
        "http://researcharchive.calacademy.org/",
        "http://ucmpdb.berkeley.edu/cgi/",
    )):
        return url.replace("http://", "https://")
    return url


@make_linter("type_designations", disabled=True)
def check_type_designations_present(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.genus_type_kind is TypeSpeciesDesignation.subsequent_designation:
        if not any(
            tag.type == nam.type
            for tag in nam.get_tags(nam.type_tags, TypeTag.TypeDesignation)
        ):
            yield "missing a reference for type species designation"
    if (
        nam.species_type_kind is SpeciesGroupType.lectotype
        and nam.type_specimen is not None
    ):
        if not any(
            tag.lectotype == nam.type_specimen
            for tag in nam.get_tags(nam.type_tags, TypeTag.LectotypeDesignation)
        ):
            yield "missing a reference for lectotype designation"
    if (
        nam.species_type_kind is SpeciesGroupType.neotype
        and nam.type_specimen is not None
    ):
        if not any(
            tag.neotype == nam.type_specimen
            for tag in nam.get_tags(nam.type_tags, TypeTag.NeotypeDesignation)
        ):
            yield "missing a reference for neotype designation"


MULTIPLE_ID = 366
BMNH_MAMMALS_ID = 1471
BMNH_COLLECTION_ID = 5


class RepositoryKind(enum.Enum):
    repository = 1
    former = 2
    extra = 3
    future = 4


def _get_repositories(nam: Name) -> set[tuple[RepositoryKind, Collection]]:
    repos = set()
    if nam.collection is not None and nam.collection.id != MULTIPLE_ID:
        repos.add((RepositoryKind.repository, nam.collection))
    repo: Collection
    for tag in nam.type_tags:
        match tag:
            case TypeTag.Repository(repo):
                repos.add((RepositoryKind.repository, repo))
            case TypeTag.FormerRepository(repo):
                repos.add((RepositoryKind.former, repo))
            case TypeTag.ExtraRepository(repo):
                repos.add((RepositoryKind.extra, repo))
            case TypeTag.FutureRepository(repo):
                repos.add((RepositoryKind.future, repo))
    return repos


@dataclass
class Specimen:
    text: str
    comment: str | None = None
    future_texts: Sequence[str] = field(default_factory=list)
    extra_texts: Sequence[str] = field(default_factory=list)
    former_texts: Sequence[str] = field(default_factory=list)

    def stringify(self) -> str:
        text = self.text
        if self.comment is not None:
            text += f" ({self.comment}!)"
        text += "".join(f" (=> {future})" for future in sorted(self.future_texts))
        text += "".join(f" (+ {extra})" for extra in sorted(self.extra_texts))
        text += "".join(f" (= {former})" for former in sorted(self.former_texts))
        return text

    def sort_key(self) -> tuple[object, ...]:
        numeric_match = re.fullmatch(r"(.+) (\d+)", self.text)
        return (
            1,
            numeric_match is not None,
            (
                (numeric_match.group(1), int(numeric_match.group(2)))
                if numeric_match is not None
                else self.text
            ),
            self.comment or "",
            tuple(sorted(self.future_texts)),
            tuple(sorted(self.extra_texts)),
            tuple(sorted(self.former_texts)),
        )


@dataclass
class SpecialSpecimen:
    collection: str
    label: str
    comment: str | None = None
    former_texts: Sequence[str] = field(default_factory=list)

    def stringify(self) -> str:
        text = f"{self.collection} ({self.label})"
        if self.comment is not None:
            text += f" ({self.comment}!)"
        text += "".join(f" (= {former})" for former in sorted(self.former_texts))
        return text

    def sort_key(self) -> tuple[object, ...]:
        return (
            2,
            self.collection,
            self.label,
            self.comment or "",
            tuple(sorted(self.former_texts)),
        )


@dataclass
class SpecimenRange:
    start: Specimen
    end: Specimen

    def stringify(self) -> str:
        return f"{self.start.stringify()} through {self.end.stringify()}"

    def sort_key(self) -> tuple[object, ...]:
        return (0, self.start.sort_key(), self.end.sort_key())


AnySpecimen = Specimen | SpecialSpecimen | SpecimenRange


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


@make_linter("type_specimen_order")
def check_type_specimen_order(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # reported elsewhere
    expected_text = ", ".join(
        spec.stringify() for spec in sorted(specs, key=lambda spec: spec.sort_key())
    )
    if nam.type_specimen == expected_text:
        return
    message = (
        f"Incorrectly formatted type specimen: got {nam.type_specimen!r}, expected"
        f" {expected_text!r}"
    )
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type_specimen = expected_text
    else:
        yield message


def parse_type_specimen(text: str) -> list[AnySpecimen]:
    specs: list[AnySpecimen] = []
    for chunk in _split_type_spec_string(text):
        if " through " in chunk:
            left, right = chunk.split(" through ", maxsplit=1)
            left_spec = _parse_single_specimen(left)
            if not isinstance(left_spec, Specimen):
                raise ValueError(
                    f"range must contain a simple specimen, not {left_spec}"
                )
            right_spec = _parse_single_specimen(right)
            if not isinstance(right_spec, Specimen):
                raise ValueError(
                    f"range must contain a simple specimen, not {right_spec}"
                )
            specs.append(SpecimenRange(left_spec, right_spec))
        else:
            specs.append(_parse_single_specimen(chunk))
    return specs


_SPECIAL_SUFFIXES = (
    " (unnumbered)",
    " (no number given)",
    " (no numbers given)",
    " (lost)",
)


def _parse_single_specimen(text: str) -> Specimen | SpecialSpecimen:
    formers: list[str] = []
    futures: list[str] = []
    extras: list[str] = []
    comment = None
    while text.endswith(")"):
        if text.endswith(_SPECIAL_SUFFIXES):
            text, end = text.rsplit(" (", maxsplit=1)
            if futures or extras:
                raise ValueError(
                    f"Special specimen {text} cannot have future or extra location"
                )
            return SpecialSpecimen(
                text, end.removesuffix(")"), comment=comment, former_texts=formers
            )
        elif " (" in text:
            text, end = text.rsplit(" (", maxsplit=1)
            tail = end.removesuffix(")")
            if tail.endswith("!"):
                if comment is not None:
                    raise ValueError(f"cannot have two comments in {text}")
                comment = tail.rstrip("!")
            elif tail.startswith("=>"):
                futures.append(tail.removeprefix("=>").strip())
            elif tail.startswith("+"):
                extras.append(tail.removeprefix("+").strip())
            elif tail.startswith("="):
                formers.append(tail.removeprefix("=").strip())
            else:
                raise ValueError(f"invalid parenthesized text in {text!r}")
        else:
            raise ValueError(f"invalid parenthesized text in {text!r}")
    return Specimen(
        text,
        comment=comment,
        former_texts=formers,
        future_texts=futures,
        extra_texts=extras,
    )


@make_linter("type_specimen")
def check_type_specimen(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError as e:
        yield f"cannot parse type specimen string {nam.type_specimen}: {e}"
        return
    for spec in specs:
        if isinstance(spec, SpecimenRange):
            yield from _check_specimen(spec.start, nam)
            yield from _check_specimen(spec.end, nam)
        elif isinstance(spec, SpecialSpecimen):
            continue
        else:
            yield from _check_specimen(spec, nam)


def _validate_specimen(
    text: str, repos: set[tuple[RepositoryKind, Collection]], kinds: set[RepositoryKind]
) -> str | None:
    messages = []
    for kind, repo in repos:
        if kind not in kinds:
            continue
        message = repo.validate_specimen(text)
        if message is None:
            return None
        messages.append(message)
    return "; ".join(messages)


def _check_specimen(spec: Specimen, nam: Name) -> Iterable[str]:
    repos = _get_repositories(nam)
    if message := _validate_specimen(spec.text, repos, {RepositoryKind.repository}):
        yield f"{spec.text!r} does not match: {message}"
    for former in spec.former_texts:
        if former.startswith('"') and former.endswith('"'):
            continue
        if message := _validate_specimen(
            former, repos, {RepositoryKind.repository, RepositoryKind.former}
        ):
            yield (
                f"former specimen reference {former!r} does not match any collection"
                f" ({message})"
            )
    for future in spec.future_texts:
        if message := _validate_specimen(
            future, repos, {RepositoryKind.repository, RepositoryKind.future}
        ):
            yield (
                f"future specimen reference {future!r} does not match any collection"
                f" ({message})"
            )
    for extra in spec.extra_texts:
        if message := _validate_specimen(
            extra, repos, {RepositoryKind.repository, RepositoryKind.extra}
        ):
            yield (
                f"extra specimen reference {extra!r} does not match any collection"
                f" ({message})"
            )


_BMNH_REGEXES = [
    (r"^BMNH ([MR])[\- \.]*(\d+[a-z]?)$", r"BMNH \1 \2"),  # M1234 -> M 1234
    (r"^BMNH (\d)(\.\d+\.\d+\.\d+)$", r"BMNH 190\1\2"),  # 2.1.1.1 -> 1902.1.1.1
    (r"^BMNH ([4-9]\d)(\.\d+\.\d+\.\d+)$", r"BMNH 18\1\2"),  # 55.1.1.1 -> 1855.1.1.1
    (r"^BMNH ([1-2]\d)(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 11.1.1.1 -> 1911.1.1.1
    (r"^BMNH (\d{4,5})$", r"BMNH OR \1"),  # BMNH 12345 -> BMNH OR 12345
    (r"^BMNH (3[0-5])(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 33.1.1.1 -> 1933.1.1.1
    (r"^BMNH (3[7-9])(\.\d+\.\d+\.\d+)$", r"BMNH 19\1\2"),  # 38.1.1.1 -> 1838.1.1.1
]


@make_linter("bmnh_types")
def check_bmnh_type_specimens(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Fix some simple issues with BMNH type specimens."""
    if nam.type_specimen is None:
        return
    if nam.collection is None or nam.collection.id != BMNH_COLLECTION_ID:
        return
    try:
        specs = parse_type_specimen(nam.type_specimen)
    except ValueError:
        return  # other check will complain
    for spec in specs:
        if not isinstance(spec, Specimen):
            continue
        if not spec.text.startswith("BMNH"):
            continue
        new_spec = clean_up_bmnh_type(spec.text)
        if new_spec != spec.text:
            message = f"replace {spec.text!r} with {new_spec!r}"
            if cfg.autofix:
                print(f"{nam}: {message}")
                nam.type_specimen = nam.type_specimen.replace(spec.text, new_spec)
            else:
                yield message


def get_all_type_specimen_texts(nam: Name) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    for spec in parse_type_specimen(nam.type_specimen):
        if isinstance(spec, Specimen):
            yield spec.text
        elif isinstance(spec, SpecimenRange):
            yield spec.start.text
            yield spec.end.text


@make_linter("child_collection")
def check_general_collection(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.collection is None:
        return
    if not nam.collection.must_use_children():
        return
    if nam.type_specimen is not None:
        for tag in nam.collection.tags:
            if (
                isinstance(tag, CollectionTag.ChildRule)
                and all(
                    re.fullmatch(tag.regex, spec)
                    for spec in get_all_type_specimen_texts(nam)
                )
                and (tag.age is None or nam.taxon.age == tag.age)
                and (tag.taxon is None or nam.taxon.is_child_of(tag.taxon))
            ):
                message = f"should use collection {tag.collection} based on rule {tag}"
                if cfg.autofix:
                    print(f"{nam}: {message}")
                    nam.collection = tag.collection
                else:
                    yield message
                return
    yield f"should use child collection of {nam.collection}"


@make_linter("type_specimen_link")
def check_must_have_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # TODO: cover multiple, ExtraRepository etc. here
    # After replacing all TypeSpecimenLink tags. Then we should be able to associate every TypeSpecimenLinkFor tag with some part of the type_specimen text.
    if nam.collection is None or not nam.collection.must_have_specimen_links(nam):
        return
    if nam.type_specimen is None:
        return
    num_expected = sum(
        not isinstance(spec, SpecialSpecimen)
        for spec in parse_type_specimen(nam.type_specimen)
    )
    num_actual = sum(
        isinstance(tag, (TypeTag.TypeSpecimenLink, TypeTag.TypeSpecimenLinkFor))
        and nam.collection is not None
        and nam.collection.is_valid_specimen_link(tag.url)
        for tag in nam.type_tags
    )
    if num_actual < num_expected:
        yield (
            f"has {num_actual} type specimen links, but expected at least"
            f" {num_expected}"
        )


@make_linter("duplicate_type_specimen_links")
def check_duplicate_type_specimen_links(nam: Name, cfg: LintConfig) -> Iterable[str]:
    tags_with_specimens = {
        tag.url for tag in nam.type_tags if isinstance(tag, TypeTag.TypeSpecimenLinkFor)
    }
    if not tags_with_specimens:
        return
    new_tags = tuple(
        tag
        for tag in nam.type_tags
        if not isinstance(tag, TypeTag.TypeSpecimenLink)
        or tag.url not in tags_with_specimens
    )
    if nam.type_tags != new_tags:
        removed = set(nam.type_tags) - set(new_tags)
        message = f"remove redundant tags: {removed}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.type_tags = new_tags  # type: ignore
        else:
            yield message


@make_linter("replace_simple_type_specimen_link")
def replace_simple_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    if "," in nam.type_specimen or "(" in nam.type_specimen:
        return
    num_ts_links = sum(
        isinstance(tag, TypeTag.TypeSpecimenLink) for tag in nam.type_tags
    )
    if num_ts_links != 1:
        return
    new_tags = tuple(
        (
            TypeTag.TypeSpecimenLinkFor(tag.url, nam.type_specimen)
            if isinstance(tag, TypeTag.TypeSpecimenLink)
            and nam.type_specimen is not None
            else tag
        )
        for tag in nam.type_tags
    )
    message = "replace TypeSpecimenLink tag with TypeSpecimenLinkFor"
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type_tags = new_tags  # type: ignore
    else:
        yield message


def _parse_specimen_detail(text: str) -> dict[str, str] | None:
    match = re.fullmatch(r"(.*) \[at (https?://.*)\]", text)
    if match is None:
        return None
    rest = match.group(1)
    url = match.group(2)
    out = {"at": url}
    for piece in rest.split(" ... "):
        match = re.fullmatch(r"\[([^\]]+)\] (.*)", piece)
        if match:
            out[match.group(1)] = match.group(2)
    return out


@make_linter("replace_type_specimen_link")
def replace_type_specimen_link(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    if not any(isinstance(tag, TypeTag.TypeSpecimenLink) for tag in nam.type_tags):
        return
    from_spec_details: dict[str, str] = {}
    for tag in nam.type_tags:
        if not isinstance(tag, TypeTag.SpecimenDetail):
            continue
        parsed = _parse_specimen_detail(tag.text)
        if not parsed:
            continue
        url = fix_type_specimen_link(parsed["at"])
        if "data.nhm.ac.uk" in url:
            catno = parsed["catalogNumber"]
            catno = (
                "BMNH "
                + catno.replace("NHMUK", "")
                .replace("ZD", "")
                .replace("GMCM", "")
                .replace("GERM", "")
                .strip()
            )
            catno = clean_up_bmnh_type(catno).replace("BMNH ", "BMNH Mammals ")
            catno = re.sub(r"\.\[[a-zA-Z]\]$", "", catno)
            catno = re.sub(r"\.([a-z])$", r"\1", catno)
        elif "coldb.mnhn.fr" in url:
            code = url.split("/")[-2].upper()
            catno = f"MNHN-{code}-{parsed['catalogNumber']}"
        elif "institutioncode" in parsed and "catalognumber" in parsed:
            catno = f"{parsed['institutioncode']} {parsed['catalognumber']}"
            catno = (
                catno.replace("UCMP ", "UCMP:V:")
                .replace("FMNH ", "FMNH Mammals ")
                .replace("UCLA ", "UCLA Mammals ")
                .replace("UF ", "UF:VP:")
            )
            catno = re.sub(r"^([A-Z]+) \1", r"\1", catno)
        else:
            continue
        from_spec_details[url] = catno

    new_tags = []
    messages = []
    possible_types = set(get_possible_type_specimens(nam))
    for tag in nam.type_tags:
        if not isinstance(tag, TypeTag.TypeSpecimenLink):
            new_tags.append(tag)
            continue
        specimen = None
        if tag.url in from_spec_details:
            specimen = from_spec_details[tag.url]
        elif tag.url.startswith("https://mczbase.mcz.harvard.edu/guid/"):
            specimen = tag.url.removeprefix("https://mczbase.mcz.harvard.edu/guid/")
        elif match := re.fullmatch(
            r"https?://portal\.vertnet\.org/o/ucla/mammals\?id=urn-catalog-ucla-mammals-(\d+)",
            tag.url,
        ):
            specimen = f"UCLA Mammals {match.group(1)}"
        elif match := re.fullmatch(
            r"http://coldb\.mnhn\.fr/catalognumber/mnhn/([a-z]+)/([a-z\-]+\d+)([a-z])",
            tag.url,
        ):
            specimen = f"MNHN-{match.group(1).upper()}-{match.group(2).upper()}{match.group(3)}"
        elif match := re.fullmatch(
            r"http://portal\.vertnet\.org/o/amnh/mammals\?id=urn-catalog-amnh-mammals-([a-z\d\-]+)",
            tag.url,
        ):
            specimen = f"AMNH {match.group(1).upper()}"

        if specimen is not None and specimen in possible_types:
            new_tag = TypeTag.TypeSpecimenLinkFor(tag.url, specimen)
            messages.append(f"{tag} -> {new_tag}")
        else:
            if specimen is not None:
                print(
                    f"Reject {specimen!r} (not in {possible_types} for {nam}, URL"
                    f" {tag.url})"
                )
            yield f"replace TypeSpecimenLink tag: {tag}"
            new_tag = tag
        new_tags.append(new_tag)

    if not messages:
        return
    message = (
        f"replace TypeSpecimenLink tag with TypeSpecimenLinkFor: {', '.join(messages)}"
    )
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.type_tags = new_tags  # type: ignore
    else:
        yield message


def get_possible_type_specimens(nam: Name) -> Iterable[str]:
    if nam.type_specimen is None:
        return
    for spec in parse_type_specimen(nam.type_specimen):
        match spec:
            case SpecialSpecimen(
                collection=collection, label=label, former_texts=former_texts
            ):
                yield f"{collection} ({label})"
                yield from former_texts
            case SpecimenRange(start=start, end=end):
                yield from _get_possible_type_specimens_from_specimen(start)
                yield from _get_possible_type_specimens_from_specimen(end)
            case Specimen():
                yield from _get_possible_type_specimens_from_specimen(spec)


def _get_possible_type_specimens_from_specimen(spec: Specimen) -> Iterable[str]:
    yield spec.text
    yield from spec.former_texts
    yield from spec.future_texts
    yield from spec.extra_texts


def clean_up_bmnh_type(text: str) -> str:
    for rgx, replacement in _BMNH_REGEXES:
        text = re.sub(rgx, replacement, text)
    return text


@make_linter("tags")
def check_tags_for_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Looks at all tags set on names and applies related changes."""
    try:
        tags = nam.tags
    except Exception:
        yield "could not deserialize tags"
        return
    if not tags:
        return

    status_to_priority = {}
    for priority, statuses in enumerate(NomenclatureStatus.hierarchy()):
        for status in statuses:
            status_to_priority[status] = priority

    def maybe_adjust_status(nam: Name, status: NomenclatureStatus, tag: object) -> None:
        current_priority = status_to_priority[nam.nomenclature_status]
        new_priority = status_to_priority[status]
        if current_priority > new_priority:
            comment = (
                f"Status automatically changed from {nam.nomenclature_status.name} to"
                f" {status.name} because of {tag}"
            )
            print(f"changing status of {nam} and adding comment {comment!r}")
            if cfg.autofix:
                nam.add_static_comment(CommentKind.automatic_change, comment)
                nam.nomenclature_status = status  # type: ignore

    for tag in tags:
        if isinstance(tag, NameTag.PreoccupiedBy):
            maybe_adjust_status(nam, NomenclatureStatus.preoccupied, tag)
            senior_name = tag.name
            if nam.group != senior_name.group:
                yield (
                    f"is of a different group than supposed senior name {senior_name}"
                )
            if senior_name.nomenclature_status is NomenclatureStatus.subsequent_usage:
                for senior_name_tag in senior_name.get_tags(
                    senior_name.tags, NameTag.SubsequentUsageOf
                ):
                    senior_name = senior_name_tag.name
            if senior_name.nomenclature_status is NomenclatureStatus.name_combination:
                for senior_name_tag in senior_name.get_tags(
                    senior_name.tags, NameTag.NameCombinationOf
                ):
                    senior_name = senior_name_tag.name
            if nam.get_date_object() < senior_name.get_date_object():
                yield f"predates supposed senior name {senior_name}"
            # TODO apply this check to species too by handling gender endings correctly.
            if nam.group is not Group.species:
                if nam.root_name != tag.name.root_name:
                    yield (
                        "has a different root name than supposed senior name"
                        f" {senior_name}"
                    )
        elif isinstance(
            tag,
            (
                NameTag.UnjustifiedEmendationOf,
                NameTag.IncorrectSubsequentSpellingOf,
                NameTag.VariantOf,
                NameTag.NomenNovumFor,
                NameTag.JustifiedEmendationOf,
                NameTag.SubsequentUsageOf,
                NameTag.NameCombinationOf,
            ),
        ):
            for status, tag_cls in STATUS_TO_TAG.items():
                if isinstance(tag, tag_cls):
                    maybe_adjust_status(nam, status, tag)
            if nam.get_date_object() < tag.name.get_date_object():
                yield f"predates supposed original name {tag.name}"
            if isinstance(tag, NameTag.SubsequentUsageOf):
                if (
                    nam.group is Group.species
                    and nam.taxon == tag.name.taxon
                    and nam.corrected_original_name != tag.name.corrected_original_name
                ):
                    yield (
                        f"{nam} should be a name combination instead of a subsequent"
                        " usage, because it is assigned to the same taxon as its"
                        f" target {tag.name}"
                    )
                    if (
                        cfg.autofix
                        and nam.nomenclature_status
                        is NomenclatureStatus.subsequent_usage
                    ):
                        new_tags = [
                            *[t for t in nam.tags if t != tag],
                            NameTag.NameCombinationOf(tag.name, tag.comment),
                        ]
                        nam.tags = new_tags  # type: ignore
                        nam.nomenclature_status = NomenclatureStatus.name_combination  # type: ignore
            else:
                if nam.taxon != tag.name.taxon:
                    yield f"{nam} is not assigned to the same name as {tag.name}"
        elif isinstance(tag, NameTag.PartiallySuppressedBy):
            maybe_adjust_status(nam, NomenclatureStatus.partially_suppressed, tag)
        elif isinstance(tag, NameTag.FullySuppressedBy):
            maybe_adjust_status(nam, NomenclatureStatus.fully_suppressed, tag)
        elif isinstance(tag, NameTag.Conserved):
            if nam.nomenclature_status not in (
                NomenclatureStatus.available,
                NomenclatureStatus.as_emended,
                NomenclatureStatus.nomen_novum,
                NomenclatureStatus.preoccupied,
            ):
                yield f"{nam} is on the Official List, but is not marked as available."
        # haven't handled TakesPriorityOf, NomenOblitum, MandatoryChangeOf


@make_linter("required_tags")
def check_required_tags(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.nomenclature_status not in STATUS_TO_TAG:
        return
    tag_cls = STATUS_TO_TAG[nam.nomenclature_status]
    tags = list(nam.get_tags(nam.tags, tag_cls))
    if not tags:
        yield (f"has status {nam.nomenclature_status.name} but no corresponding tag")


@make_linter("lsid")
def check_for_lsid(nam: Name, cfg: LintConfig) -> Iterable[str]:
    # ICZN Art. 8.5.1: ZooBank is relevant to availability only starting in 2012
    if (
        nam.numeric_year() < 2012
        or nam.corrected_original_name is None
        or nam.original_citation is None
    ):
        return
    try:
        zoobank_data_list = get_zoobank_data(nam.corrected_original_name)
    except requests.exceptions.HTTPError as e:
        print(f"Error retrieving ZooBank data: {e!r}")
        return
    if not zoobank_data_list:
        return
    type_tags = []
    art_tags = []
    art = nam.original_citation
    name_lsids = {tag.text for tag in nam.get_tags(nam.type_tags, TypeTag.LSIDName)}
    rejected_lsids = {
        tag.text for tag in nam.get_tags(nam.type_tags, TypeTag.RejectedLSIDName)
    }
    art_lsids = {tag.text for tag in art.get_tags(art.tags, ArticleTag.LSIDArticle)}
    for zoobank_data in zoobank_data_list:
        if zoobank_data.name_lsid in rejected_lsids:
            continue
        if zoobank_data.name_lsid not in name_lsids:
            type_tags.append(TypeTag.LSIDName(zoobank_data.name_lsid))
        if zoobank_data.citation_lsid and zoobank_data.citation_lsid not in art_lsids:
            art_tag = ArticleTag.LSIDArticle(
                zoobank_data.citation_lsid,
                present_in_article=PresenceStatus.to_be_determined,
            )
            art_tags.append(art_tag)
    if not type_tags and not art_tags:
        return
    message = f"Inferred ZooBank data: {type_tags}, {art_tags}"
    if cfg.autofix:
        print(f"{nam}: {message}")
        for tag in type_tags:
            nam.add_type_tag(tag)
        for tag in art_tags:
            nam.original_citation.add_tag(tag)
    else:
        yield message


@make_linter("year")
def check_year(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.year is None:
        return
    if not helpers.is_valid_date(nam.year):
        yield f"has invalid year {nam.year!r}"
    if helpers.is_date_range(nam.year):
        yield "year is a range"


@make_linter("year_matches")
def check_year_matches(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None:
        return

    if nam.original_citation.has_tag(
        ArticleTag.UnavailableElectronic
    ) and nam.nomenclature_status not in (
        NomenclatureStatus.unpublished_electronic,
        NomenclatureStatus.unpublished_pending,
    ):
        yield (
            "was published in unavailable electronic-only publication and should be"
            " marked as unpublished_electronic"
        )
        if cfg.autofix:
            nam.nomenclature_status = NomenclatureStatus.unpublished_electronic  # type: ignore
    elif nam.nomenclature_status in (
        NomenclatureStatus.unpublished_electronic,
        NomenclatureStatus.unpublished_pending,
    ) and not nam.original_citation.has_tag(ArticleTag.UnavailableElectronic):
        yield (
            "is marked as unpublished_electronic, but original citation is not marked"
            " accordingly"
        )

    if (
        nam.original_citation.type is ArticleType.THESIS
        and nam.nomenclature_status is not NomenclatureStatus.unpublished_thesis
    ):
        yield "was published in a thesis and should be marked as unpublished_thesis"
        if cfg.autofix:
            nam.nomenclature_status = NomenclatureStatus.unpublished_thesis  # type: ignore
    elif (
        nam.nomenclature_status is NomenclatureStatus.unpublished_thesis
        and nam.original_citation.type is not ArticleType.THESIS
    ):
        yield "is marked as unpublished_thesis, but was not published in a thesis"

    if nam.year != nam.original_citation.year:
        if cfg.autofix and helpers.is_more_specific_date(
            nam.original_citation.year, nam.year
        ):
            print(f"{nam}: fixing date {nam.year} -> {nam.original_citation.year}")
            nam.year = nam.original_citation.year
        else:
            yield (
                f"year mismatch: {nam.year} (name) vs."
                f" {nam.original_citation.year} (article)"
            )


ATTRIBUTES_BY_GROUP = {
    "name_complex": (Group.genus,),
    "species_name_complex": (Group.species,),
    "type": (Group.family, Group.genus),
    "type_locality": (Group.species,),
    "type_specimen": (Group.species,),
    "collection": (Group.species,),
    "genus_type_kind": (Group.genus,),
    "species_type_kind": (Group.species,),
}


@make_linter("disallowed_attributes")
def check_disallowed_attributes(nam: Name, cfg: LintConfig) -> Iterable[str]:
    for field_name, groups in ATTRIBUTES_BY_GROUP.items():
        if nam.group not in groups:
            value = getattr(nam, field_name)
            if value is not None:
                yield f"should not have attribute {field_name} (value {value})"
    if (
        nam.species_name_complex is not None
        and not nam.nomenclature_status.requires_name_complex()
    ):
        message = (
            f"is of status {nam.nomenclature_status.name} and should not have a"
            " name complex"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_data(
                "species_name_complex",
                nam.species_name_complex.id,
                concat_duplicate=True,
            )
            nam.species_name_complex = None
        else:
            yield message


def _make_con_messsage(nam: Name, text: str) -> str:
    return f"corrected original name {nam.corrected_original_name!r} {text}"


@make_linter("corrected_original_name")
def check_corrected_original_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that corrected_original_names are correct."""
    if nam.corrected_original_name is None:
        return
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    inferred = nam.infer_corrected_original_name()
    if inferred is not None and inferred != nam.corrected_original_name:
        yield _make_con_messsage(
            nam,
            f"inferred name {inferred!r} does not match current name"
            f" {nam.corrected_original_name!r}",
        )
    if not re.match(r"^[A-Z][a-z ]+$", nam.corrected_original_name):
        yield _make_con_messsage(nam, "contains unexpected characters")
        return
    if nam.group in (Group.high, Group.genus):
        if " " in nam.corrected_original_name:
            yield _make_con_messsage(nam, "contains whitespace")
        elif nam.corrected_original_name != nam.root_name:
            emended = nam.get_tag_target(NameTag.AsEmendedBy)
            if emended is not None and emended.root_name == nam.root_name:
                return
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")
    elif nam.group is Group.family:
        if nam.nomenclature_status is NomenclatureStatus.not_based_on_a_generic_name:
            possibilities = {
                f"{nam.root_name}{suffix}" for suffix in helpers.VALID_SUFFIXES
            }
            if nam.corrected_original_name not in {nam.root_name} | possibilities:
                yield _make_con_messsage(
                    nam, f"does not match root_name {nam.root_name!r}"
                )
        elif not nam.corrected_original_name.endswith(tuple(helpers.VALID_SUFFIXES)):
            yield _make_con_messsage(
                nam, "does not end with a valid family-group suffix"
            )
    elif nam.group is Group.species:
        parts = nam.corrected_original_name.split(" ")
        if len(parts) not in (2, 3, 4):
            yield _make_con_messsage(nam, "is not a valid species or subspecies name")
        elif parts[-1] != nam.root_name:
            emended = nam.get_tag_target(NameTag.AsEmendedBy)
            if emended is not None and emended.root_name == nam.root_name:
                return
            if nam.species_name_complex is not None:
                try:
                    forms = list(nam.species_name_complex.get_forms(nam.root_name))
                except ValueError as e:
                    yield _make_con_messsage(nam, f"has invalid name complex: {e!r}")
                    return
                if parts[-1] in forms:
                    return
            yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")


def _make_rn_message(nam: Name, text: str) -> str:
    return f"root name {nam.root_name!r} {text}"


@make_linter("root_name")
def check_root_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that root_names are correct."""
    if nam.nomenclature_status.permissive_corrected_original_name():
        return
    if nam.group in (Group.high, Group.genus, Group.family):
        if not re.match(r"^[A-Z][a-z]+$", nam.root_name):
            yield _make_rn_message(nam, "contains unexpected characters")
    elif nam.group is Group.species:
        if not re.match(r"^[a-z]+$", nam.root_name):
            yield _make_rn_message(nam, "contains unexpected characters")
            return
        yield from _check_species_name_gender(nam, cfg)


def _check_rn_matches_original(
    nam: Name, corrected_original_name: str, cfg: LintConfig, reason: str
) -> Iterable[str]:
    con_root = corrected_original_name.split()[-1]
    if con_root == nam.root_name:
        return
    message = _make_con_messsage(
        nam, f"does not match root_name {nam.root_name!r} ({reason})"
    )
    if cfg.autofix:
        print(f"{nam}: {message}")
        nam.root_name = con_root
    else:
        yield message


def _check_species_name_gender(nam: Name, cfg: LintConfig) -> Iterable[str]:
    emending_name = nam.get_tag_target(NameTag.AsEmendedBy)
    if emending_name is not None:
        corrected_original_name = emending_name.corrected_original_name
    else:
        corrected_original_name = nam.corrected_original_name
    if corrected_original_name is None:
        return
    con_root = corrected_original_name.split()[-1]
    # For nomina dubia we always follow the original name
    if nam.status in (Status.nomen_dubium, Status.species_inquirenda):
        yield from _check_rn_matches_original(
            nam, corrected_original_name, cfg, nam.status.name
        )
        return
    # If there is no name complex, the root_name should match exactly
    if nam.species_name_complex is None:
        yield from _check_rn_matches_original(
            nam, corrected_original_name, cfg, "no name complex"
        )
        return
    if nam.species_name_complex.kind is not SpeciesNameKind.adjective:
        yield from _check_rn_matches_original(
            nam, corrected_original_name, cfg, "not an adjective"
        )
        return
    if nam.species_name_complex.is_invariant_adjective():
        yield from _check_rn_matches_original(
            nam, corrected_original_name, cfg, "invariant adjective"
        )
        return
    # Now we have an adjective that needs to agree in gender with its genus, so we
    # have to find the genus. But first we check whether the name even makes sense.
    try:
        forms = list(nam.species_name_complex.get_forms(nam.root_name))
    except ValueError as e:
        yield _make_con_messsage(nam, f"has invalid name complex: {e!r}")
        return
    if con_root not in forms:
        yield _make_con_messsage(nam, f"does not match root_name {nam.root_name!r}")
        return

    taxon = nam.taxon
    genus = taxon.get_logical_genus() or taxon.get_nominal_genus()
    if genus is None or genus.name_complex is None:
        return

    genus_gender = genus.name_complex.gender
    expected_form = nam.species_name_complex.get_form(con_root, genus_gender)
    if expected_form != nam.root_name:
        message = _make_rn_message(
            nam,
            f"does not match expected form {expected_form!r} for"
            f" {genus_gender.name} genus {genus}",
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            comment = (
                f"Name changed from {nam.root_name!r} to {expected_form!r} to agree in"
                f" gender with {genus_gender.name} genus {genus} ({{n#{genus.id}}})"
            )
            nam.add_static_comment(CommentKind.automatic_change, comment)
            nam.root_name = expected_form
        else:
            yield message


@make_linter("family_root_name")
def check_family_root_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group is not Group.family or nam.type is None:
        return
    if nam.is_unavailable():
        return
    try:
        stem_name = nam.type.get_stem()
    except ValueError:
        yield f"{nam.type} has bad name complex: {nam.type.name_complex}"
        return
    if stem_name is None:
        return
    if nam.root_name == stem_name:
        return
    if nam.root_name + "id" == stem_name:
        # The Code allows eliding -id- from the stem.
        return
    for stripped in helpers.name_with_suffixes_removed(nam.root_name):
        if stripped == stem_name or stripped + "i" == stem_name:
            print(f"{nam}: Autocorrecting root name: {nam.root_name} -> {stem_name}")
            if cfg.autofix:
                nam.root_name = stem_name
            break
    if nam.root_name != stem_name:
        if nam.has_type_tag(TypeTag.IncorrectGrammar):
            return
        yield f"Stem mismatch: {nam.root_name} vs. {stem_name}"


@make_linter("type_taxon")
def correct_type_taxon(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check that a name's type belongs to a child of the name's taxon."""
    if nam.group not in (Group.genus, Group.family):
        return
    if nam.type is None:
        return
    if nam.taxon == nam.type.taxon:
        return
    expected_taxon = nam.type.taxon.parent
    while (
        expected_taxon is not None
        and expected_taxon.base_name.group != nam.group
        and expected_taxon != nam.taxon
    ):
        expected_taxon = expected_taxon.parent
    if expected_taxon is None:
        return
    if nam.taxon != expected_taxon:
        message = f"expected taxon to be {expected_taxon} not {nam.taxon}"
        if cfg.autofix and expected_taxon.is_child_of(nam.taxon):
            print(f"{nam}: {message}")
            nam.taxon = expected_taxon
        else:
            yield message


@make_linter("verbatim")
def clean_up_verbatim(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.verbatim_type is not None and (
        nam.type is not None or "type" not in nam.get_required_fields()
    ):
        message = f"cleaning up verbatim type: {nam.type}, {nam.verbatim_type}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
        else:
            yield message
    if (
        nam.group is Group.species
        and nam.verbatim_type is not None
        and nam.type_specimen is not None
    ):
        message = f"cleaning up verbatim type: {nam.type_specimen}, {nam.verbatim_type}"
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_data("verbatim_type", nam.verbatim_type, concat_duplicate=True)
            nam.verbatim_type = None
        else:
            yield message
    if nam.verbatim_citation is not None and nam.original_citation is not None:
        message = (
            f"cleaning up verbatim citation: {nam.original_citation.name},"
            f" {nam.verbatim_citation}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.add_data(
                "verbatim_citation", nam.verbatim_citation, concat_duplicate=True
            )
            nam.verbatim_citation = None
        else:
            yield message
    if nam.citation_group is not None and nam.original_citation is not None:
        message = (
            f"cleaning up citation group: {nam.original_citation.name},"
            f" {nam.citation_group}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.citation_group = None
        else:
            yield message


@make_linter("status")
def check_correct_status(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.status.is_base_name() and nam != nam.taxon.base_name:
        yield f"is of status {nam.status!r} and should be base name of {nam.taxon}"
    if (
        nam.status is Status.valid
        and nam.taxon.parent is not None
        and nam.taxon.parent.base_name.status is not Status.valid
    ):
        yield (
            f"is valid, but parent {nam.taxon.parent} is of status"
            f" {nam.taxon.parent.base_name.status.name}"
        )


def _check_names_match(nam: Name, other: Name) -> Iterable[str]:
    if nam.author_tags != other.author_tags:
        yield f"authors do not match {other}"
    if nam.year != other.year:
        yield f"year does not match {other}"
    if nam.original_citation != other.original_citation:
        yield f"original_citation does not match {other}"
    if nam.verbatim_citation != other.verbatim_citation:
        yield f"verbatim_citation does not match {other}"
    if nam.citation_group != other.citation_group:
        yield f"citation_group does not match {other}"


def _check_as_emended_name(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.nomenclature_status not in (
        NomenclatureStatus.nomen_novum,
        NomenclatureStatus.preoccupied,
        NomenclatureStatus.as_emended,
    ):
        yield "expected status to be as_emended"
    as_emended_target = nam.get_tag_target(NameTag.AsEmendedBy)
    if as_emended_target is None:
        yield "as_emended without an AsEmendedBy tag"
        return
    if as_emended_target.taxon != nam.taxon:
        yield f"target {as_emended_target} does not belong to the same taxon"
    if as_emended_target.root_name != nam.root_name:
        yield (
            f"root name {nam.root_name} does not match target"
            f" {as_emended_target.root_name}"
        )
    if (
        as_emended_target.nomenclature_status
        is not NomenclatureStatus.justified_emendation
    ):
        yield f"target {as_emended_target} is not a justified_emendation"
        return
    original = as_emended_target.get_tag_target(NameTag.JustifiedEmendationOf)
    if original is None:
        yield (
            f"as_emended target {as_emended_target} lacks a justified emendation tag"
        )
        return
    if original != nam:
        yield f"incorrect original spelling traces back to {original}, not this name"


@make_linter("justified_emendation")
def check_justified_emendations(nam: Name, cfg: LintConfig) -> Iterable[str]:
    """Check for issues around justified emendations.

    Justified emendations are complex to handle because they involve multiple Names
    that are coupled together and require a very specific set of tags. See
    docs/name.md for an explanation of how these names should be organized.

    Some of the errors produced by this linter apply to a different name than the
    one that requires the change, or the same lint may be emitted multiple times for
    different names. This is to ensure we don't miss any issues while keeping the
    code relatively simple.

    """
    if nam.nomenclature_status is NomenclatureStatus.as_emended:
        yield from _check_as_emended_name(nam, cfg)
    elif nam.nomenclature_status is NomenclatureStatus.justified_emendation:
        target = nam.get_tag_target(NameTag.JustifiedEmendationOf)
        if target is None:
            yield "justified_emendation without a JustifiedEmendationOf tag"
            return
        if target.taxon != nam.taxon:
            yield f"target {target} does not belong to the same taxon"
        if (
            target.nomenclature_status is NomenclatureStatus.as_emended
            or target.get_tag_target(NameTag.AsEmendedBy)
        ):
            # Now we must have an JE/as_emended pair.
            if (
                target.corrected_original_name != None
                and target.corrected_original_name.split()[-1] == nam.root_name
            ):
                yield (
                    f"supposed incorrect spelling {target} has identical root"
                    f" name {nam.root_name}"
                )
            yield from _check_as_emended_name(target, cfg)
        else:
            # Else it should be a justified emendation for something straightforward
            # (e.g., removing diacritics), so the root_name should match.
            # But the CON may not match exactly, because the species may have moved genera etc.
            if nam.root_name != target.root_name:
                yield (
                    f"root name {nam.root_name} does not match emended name {target}"
                )
    elif nam.nomenclature_status is NomenclatureStatus.incorrect_original_spelling:
        ios_target = nam.get_tag_target(NameTag.IncorrectOriginalSpellingOf)
        if ios_target is None:
            yield "missing IncorrectOriginalSpellingOf tag"
            return
        # Incorrect original spellings are used where there are multiple spellings
        # in the original publication, and one is selected as valid. Then both names
        # should have the same author etc.
        yield from _check_names_match(nam, ios_target)


@make_linter("autoset_original_rank")
def autoset_original_rank(nam: Name, cfg: LintConfig) -> Iterable[str]:
    nam.autoset_original_rank(dry_run=not cfg.autofix)
    return []


@make_linter("corrected_original_name")
def autoset_corrected_original_name(
    nam: Name, cfg: LintConfig, aggressive: bool = False
) -> Iterable[str]:
    if nam.original_name is None or nam.corrected_original_name is not None:
        return
    if "corrected_original_name" not in nam.get_required_fields():
        return
    inferred = nam.infer_corrected_original_name(aggressive=aggressive)
    if inferred:
        message = (
            f"inferred corrected_original_name to be {inferred!r} from"
            f" {nam.original_name!r}"
        )
        if cfg.autofix:
            print(f"{nam}: {message}")
            nam.corrected_original_name = inferred
        else:
            yield message
    else:
        yield (f"could not infer corrected original name from {nam.original_name!r}")


@make_linter("fill_data_level")
def check_fill_data_level(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None:
        return
    level, reason = nam.fill_data_level()
    if level > FillDataLevel.missing_required_fields:
        return
    if (
        nam.original_citation.has_tag(ArticleTag.NeedsTranslation)
        or nam.original_citation.is_non_original()
    ):
        return
    yield f"missing basic data: {reason}"


@make_linter("citation_group")
def check_citation_group(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.citation_group is None or nam.year is None:
        return
    if message := nam.citation_group.is_year_in_range(nam.numeric_year()):
        yield message


@make_linter("matches_citation")
def check_matches_citation(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None or nam.page_described is None:
        return
    art = nam.original_citation
    if art.type not in (ArticleType.JOURNAL, ArticleType.CHAPTER, ArticleType.PART):
        return
    start_page = art.numeric_start_page()
    end_page = art.numeric_end_page()
    if not start_page or not end_page:
        return
    page_range = range(start_page, end_page + 1)
    for page in extract_pages(nam.page_described):
        try:
            numeric_page = int(page)
        except ValueError:
            continue
        if numeric_page not in page_range:
            yield (f"{nam.page_described} is not in {start_page}–{end_page} for {art}")


def extract_pages(page_described: str) -> Iterable[str]:
    page_described = re.sub(r" \([^\)]+\)(?=, |$)", "", page_described)
    parts = page_described.split(", ")
    for part in parts:
        part = re.sub(r" \[as [0-9]+\]$", "", part)
        yield part


@make_linter("page_described")
def check_page_described(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.page_described is None:
        return
    # For now ignore names without a citation
    if nam.original_citation is None:
        return
    for part in extract_pages(nam.page_described):
        if part.isdecimal() and part.isascii():
            continue
        if re.fullmatch(r"[0-9]+-[0-9]+", part):
            continue
        if part.startswith("pl. "):
            number = part.removeprefix("pl. ")
            if helpers.is_valid_roman_numeral(number):
                continue
            if re.fullmatch(r"[A-Z]?[0-9]+[A-Za-z]*", number):
                continue
        if helpers.is_valid_roman_numeral(part):
            continue
        # Pretty common to see "S40" or "40A"
        if re.fullmatch(r"[A-Z]?[0-9]+[A-Z]?", part):
            continue
        if part in (
            "unnumbered",
            "cover",
            "foldout",
            "erratum",
            "addenda",
            "table of contents",
        ):
            continue
        yield f"invalid part {part!r} in {nam.page_described!r}"


_JG2015 = "{Australia (Jackson & Groves 2015).pdf}"
_JG2015_RE = re.compile(rf"\[From {re.escape(_JG2015)}: [^\[\]]+ \[([A-Za-z\s\d]+)\]\]")
_JG2015_RE2 = re.compile(rf" \[([A-Za-z\s\d]+)\]\ \[from {re.escape(_JG2015)}\]")


@make_linter("extract_date_from_verbatim")
def extract_date_from_verbatim(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.original_citation is None or not nam.data:
        return
    # Extract precise dates from references from Jackson & Groves (2015). This sometimes
    # produces incorrect results if the names aren't associated correctly. In that case,
    # edit the name that produced the bad tag to put "[error]" in its data field so the
    # regexes above don't match.
    try:
        verbatim = nam.get_data("verbatim_citation")
    except (KeyError, TypeError, json.JSONDecodeError):
        return
    if isinstance(verbatim, str):
        verbatim = [verbatim]
    for option in verbatim:
        for regex in _JG2015_RE, _JG2015_RE2:
            match = regex.search(option)
            if not match:
                continue
            date = match.group(1)
            parsed = parse_date(date)
            if parsed is None:
                if not date.startswith("Published before "):
                    yield f"cannot parse date: {verbatim}"
            else:
                yield from _maybe_add_publication_date(nam, parsed, date, _JG2015, cfg)


def _maybe_add_publication_date(
    nam: Name, parsed: str, raw_date: str, source: str, cfg: LintConfig
) -> Iterator[str]:
    article = nam.original_citation
    if article is None:
        return
    tag = ArticleTag.PublicationDate(
        DateSource.external, parsed, f'"{raw_date}" {source}'
    )
    if tag in (article.tags or ()):
        return
    message = f'inferred date for {article} from raw date "{raw_date}": {parsed}'
    if cfg.autofix:
        print(f"{nam}: {message}")
        article.add_tag(tag)
    else:
        yield message


USNM_RGX = re.compile(
    r"""
    ,\s(ordered\spublished\s)?
    ((?P<day>\d{1,2})\s)?
    ((?P<month>[A-Z][a-z]+)\s)?
    (?P<year>\d{4})[a-z]?\.(\s|$)
    """,
    re.VERBOSE,
)
AMNH_RGX = re.compile(
    r"""
    ^([A-Za-z\.,"\s]+)\s\d+:\s?\d+(-\d+)?,\s
    (?P<month>[A-Z][a-z]+)\.?\s
    ((?P<day>\d+),\s?)?
    (?P<year>\d{4})\.
    """,
    re.VERBOSE,
)


@make_linter("extract_date_from_structured_quote")
def extract_date_from_structured_quotes(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.original_citation:
        return
    for comment in nam.comments.filter(
        NameComment.source
        << (
            34056,  # {Mammalia-USNM types (Fisher & Ludwig 2015).pdf}
            33916,  # {Anomaluromorpha, Hystricomorpha, Myomorpha-USNM types.pdf}
            29833,  # {Ferungulata-USNM types.pdf}
            15513,  # {Castorimorpha, Sciuromorpha-USNM types.pdf}
            9585,  # Mammalia-AMNH types (Lawrence 1993).pdf
        ),
        NameComment.kind == CommentKind.structured_quote,
    ):
        if any(
            tag.comment is not None and comment.source.name in tag.comment
            for tag in nam.original_citation.get_tags(
                nam.original_citation.tags, ArticleTag.PublicationDate
            )
        ):
            continue

        cite = json.loads(comment.text)["verbatim_citation"]
        if comment.source.id == 9585:
            match = AMNH_RGX.search(cite)
        else:
            match = USNM_RGX.search(cite)
        if not match:
            if comment.source.id != 9585:
                yield (
                    f"cannot match verbatim citation (ref {nam.original_citation}):"
                    f" {cite!r}"
                )
            continue
        try:
            date = helpers.parse_date(
                match.group("year"), match.group("month"), match.group("day")
            )
        except ValueError as e:
            yield f"invalid date in {cite!r}: {e}"
            continue
        yield from _maybe_add_publication_date(
            nam, date, cite, f"{{{comment.source.name}}}", cfg
        )


def parse_date(date_str: str) -> str | None:
    for month in ("%b", "%B"):
        try:
            dt = datetime.strptime(date_str, f"{month} %Y")
        except ValueError:
            pass
        else:
            return f"{dt.year}-{dt.month:02d}"
        for prefix in ("", "0"):
            try:
                dt = datetime.strptime(date_str, f"{prefix}%d {month} %Y")
            except ValueError:
                pass
            else:
                return f"{dt.year}-{dt.month:02d}-{dt.day:02d}"
    return None


@make_linter("data")
def check_data(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.data:
        return
    try:
        data = json.loads(nam.data)
    except json.JSONDecodeError:
        yield f"invalid data field: {nam.data!r}"
        return
    if not isinstance(data, dict):
        yield f"invalid data field: {nam.data!r}"
        return
    if "verbatim_citation" in data:
        if not isinstance(data["verbatim_citation"], (str, list)):
            yield f"invalid verbatim_citation data: {data['verbatim_citation']}"


@make_linter("specific_authors")
def check_specific_authors(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if not nam.original_citation and not nam.verbatim_citation:
        return
    for i, author in enumerate(nam.get_authors()):
        if (
            author.get_level() is not PersonLevel.family_name_only
            or author.naming_convention is not NamingConvention.unspecified
        ):
            continue
        if nam.original_citation is not None:
            yield (
                "has original citation, but has family name-only author"
                f" {author} (position {i})"
            )
            if cfg.interactive and "specific_authors" not in get_ignored_lints(nam):
                author.edit_tag_sequence_on_object(
                    nam, "author_tags", AuthorTag.Author, "names"
                )
        elif nam.verbatim_citation is not None and helpers.simplify_string(
            author.family_name
        ) in helpers.simplify_string(nam.verbatim_citation):
            yield f"author {author} (position {i}) appears in verbatim citation"
            if cfg.interactive and "specific_authors" not in get_ignored_lints(nam):
                author.edit_tag_sequence_on_object(
                    nam, "author_tags", AuthorTag.Author, "names"
                )


@make_linter("required_fields")
def check_required_fields(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.verbatim_citation and not nam.citation_group:
        yield "has verbatim citation but no citation group"
    if (
        nam.original_citation
        and not nam.page_described
        and not nam.original_citation.is_non_original()
    ):
        yield "has original citation but no page_described"
    if (
        nam.numeric_year() > 1970
        and not nam.verbatim_citation
        and not nam.original_citation
    ):
        yield "recent name must have verbatim citation"
    if nam.type_specimen is not None and nam.species_type_kind is None:
        yield "has type_specimen but no species_type_kind"


@make_linter("synonym_group")
def check_synonym_group(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.group not in (Group.genus, Group.species):
        return
    if nam.taxon.base_name.group is not nam.group:
        yield (
            f"taxon is of group {nam.taxon.base_name.group.name} but name is of group"
            f" {nam.group.name}"
        )


@make_linter("composites")
def check_composites(nam: Name, cfg: LintConfig) -> Iterable[str]:
    if nam.status not in (Status.composite, Status.hybrid):
        return
    children = list(nam.taxon.children)
    if children:
        different_status = [
            child for child in children if child.status is not nam.status
        ]
        if different_status:
            yield (
                f"is of status {nam.status}, but has children of different status"
                f" {different_status}"
            )
    else:
        tags = list(nam.get_tags(nam.type_tags, TypeTag.PartialTaxon))
        if len(tags) < 2:
            yield (
                f"is of status {nam.status} and must have at least two PartialTaxon"
                f" tags (got {tags})"
            )


def run_linters(
    nam: Name, cfg: LintConfig, *, include_disabled: bool = False
) -> Iterable[str]:
    if include_disabled:
        linters = [*LINTERS, *DISABLED_LINTERS]
    else:
        linters = [*LINTERS]

    used_ignores = set()
    for linter in linters:
        used_ignores |= yield from linter(nam, cfg)
    actual_ignores = get_ignored_lints(nam)
    unused = actual_ignores - used_ignores
    if unused:
        if cfg.autofix:
            tags = nam.type_tags or ()
            new_tags = []
            for tag in tags:
                if isinstance(tag, TypeTag.IgnoreLintName) and tag.label in unused:
                    print(f"{nam}: removing unused IgnoreLint tag: {tag}")
                else:
                    new_tags.append(tag)
            nam.type_tags = new_tags  # type: ignore
        else:
            yield f"{nam}: has unused IgnoreLint tags {', '.join(unused)}"
