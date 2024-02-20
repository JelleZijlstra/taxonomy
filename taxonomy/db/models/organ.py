"""

Code for parsing organ texts.

"""

from __future__ import annotations

import itertools
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal, assert_never, cast, get_args

from .. import helpers
from ..constants import SpecimenOrgan

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
    "hemapophysis",
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
