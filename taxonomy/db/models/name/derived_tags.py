"""Extraction of derived type data from source quotations.

``certain`` marks forms suitable for inference; other forms are only
used to compare existing data.
"""

import datetime
import re
from dataclasses import dataclass
from decimal import Decimal

from taxonomy.db import helpers
from taxonomy.db.constants import (
    AltitudeUnit,
    SpecimenAge,
    SpecimenGender,
    SpecimenOrgan,
)

from .name import TypeTag


@dataclass(frozen=True)
class Evidence[T]:
    value: T
    certain: bool
    alternatives: tuple[T, ...] = ()


_UNSAFE = re.compile(
    r"\b(?:paratypes?|paratopotypes?|paralectotypes?|syntypes?|allotypes?|cotypes?|cotipos?|"
    r"specimens|skins|skulls|males|females|embryos|foetuses|fetuses|pregnant|"
    r"probably|possibly|perhaps|apparently|presumably|uncertain|"
    r"erroneously|incorrectly|formerly|originally|not|without|missing|lost|"
    r"destroyed|except|rather than|but|versus|compared|series|two|three|or|together with|another|since then)\b|\?",
    re.IGNORECASE,
)
_CATALOG = re.compile(r"(?:^| \.\.\. )\[([a-zA-Z]+)\] (.*?)(?= \.\.\. | \[at |$)")


def catalog_fields(text: str) -> dict[str, str]:
    return {m[1].lower(): m[2].strip() for m in _CATALOG.finditer(text)}


def specimen_text_is_safe(text: str) -> bool:
    return _UNSAFE.search(text) is None


def allows_inference(text: str) -> bool:
    fields = catalog_fields(text)
    if fields:
        return not any(
            _UNSAFE.search(fields.get(key, ""))
            for key in ("typestatus", "occurrenceremarks")
        )
    return specimen_text_is_safe(text)


def _field_or_prose(text: str, field: str) -> tuple[str, bool]:
    fields = catalog_fields(text)
    if fields:
        if _UNSAFE.search(fields.get("typestatus", "")) or _UNSAFE.search(
            fields.get("occurrenceremarks", "")
        ):
            return "", True
        return fields.get(field, ""), True
    return text, False


def _type_description(text: str) -> bool:
    return bool(
        re.search(
            r"\b(?:(?:holo|lecto|neo)?type|typus|tipo|holotypus)\b", text, re.IGNORECASE
        )
    ) or bool(
        re.match(
            r"^(?:(?:old|young|adult|subadult|juvenile|ad\.)\s*)*(?:male|female|[♂♀])|^(?:adult|subadult|sub-adult|juvenile)\b|^juv\.",
            text,
            re.IGNORECASE,
        )
    )


def extract_gender(text: str) -> list[Evidence[TypeTag]]:
    text, field = _field_or_prose(text, "sex")
    if not specimen_text_is_safe(text):
        return []
    values = set()
    for gender, pattern in (
        (SpecimenGender.male, r"\bmale\b|♂"),
        (SpecimenGender.female, r"\bfemale\b|♀"),
    ):
        if re.search(pattern, text, re.IGNORECASE):
            values.add(gender)
    if field and text.lower() in {"m", "f"}:
        values.add(
            SpecimenGender.male if text.lower() == "m" else SpecimenGender.female
        )
    # Both sexes in a single account are not a single-specimen assertion.
    if len(values) != 1:
        return []
    return [
        Evidence(TypeTag.Gender(values.pop()), certain=field or _type_description(text))
    ]


def extract_age(text: str) -> list[Evidence[TypeTag]]:
    text, field = _field_or_prose(text, "lifestage")
    if not specimen_text_is_safe(text):
        return []
    # German catalogs abbreviate subadult as 's. ad.'; do not match the
    # embedded 'ad.' as adult.
    text = re.sub(r"\bs\.\s*ad\.", "subadult", text, flags=re.IGNORECASE)
    if re.search(r"\b(?:young|nearly|almost)\s+adult\b", text, re.IGNORECASE):
        return [
            Evidence(
                TypeTag.Age(SpecimenAge.subadult),
                certain=False,
                alternatives=(TypeTag.Age(SpecimenAge.adult),),
            )
        ]
    if re.search(r"\bimmature\b", text, re.IGNORECASE):
        return [
            Evidence(
                TypeTag.Age(SpecimenAge.subadult),
                certain=False,
                alternatives=(TypeTag.Age(SpecimenAge.juvenile),),
            )
        ]
    values = set()
    for age, pattern in (
        (SpecimenAge.adult, r"(?<!sub-)(?<!sub )\badult\b|\bad\."),
        (SpecimenAge.subadult, r"\bsub[ -]?adult\b|\bsubad\."),
        (SpecimenAge.juvenile, r"\bjuvenile\b|\bjuv\."),
    ):
        if re.search(pattern, text, re.IGNORECASE):
            values.add(age)
    if len(values) == 1:
        return [
            Evidence(
                TypeTag.Age(values.pop()), certain=field or _type_description(text)
            )
        ]
    if not values and re.search(r"\byoung\b", text, re.IGNORECASE):
        return [
            Evidence(
                TypeTag.Age(SpecimenAge.juvenile),
                certain=False,
                alternatives=(TypeTag.Age(SpecimenAge.subadult),),
            )
        ]
    return []


_MONTH = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)
_DATE = re.compile(
    rf"\b(?:(?P<day>\d{{1,2}})(?:st|nd|rd|th)?\s+(?P<month>{_MONTH})\.?[,]?\s+"
    rf"|(?P<month_first>{_MONTH})\.?\s+(?P<day_last>\d{{1,2}})(?:st|nd|rd|th)?[,]?\s+)"
    r"(?P<year>[12]\d{3})\b",
    re.IGNORECASE,
)
_MONTHS = {
    month.lower(): i
    for i, month in enumerate(
        (
            "",
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        )
    )
    if month
}


def _format_date(year: int, month: int, day: int) -> str | None:
    try:
        date = datetime.date(year, month, day)
    except ValueError:
        return None
    return date.strftime("%-d %B %Y")


def _catalog_date_is_certain(date: str, fields: dict[str, str]) -> bool:
    # Normalized catalog dates can be acquisition dates or padded partial dates.
    # Require the verbatim field to corroborate their meaning and precision.
    if re.search(
        r"\b(?:purchased|received|accessioned|registered|died|captive)\b",
        fields.get("occurrenceremarks", ""),
        re.IGNORECASE,
    ):
        return False
    try:
        return helpers.standardize_date(fields.get("verbatimeventdate", "")) == date
    except ValueError:
        return False


def allows_date_inference(text: str) -> bool:
    """Acquisition/death accounts require review, even if a catalog has a date.

    A verbatim eventDate corroborates transcription, not the kind of event.
    Apply this veto across sources before adding any collection date.
    """
    return not re.search(
        r"\b(?:purchased|purchase|bought|acquired|acquisition|received|accessioned|"
        r"registered|catalogued|cataloged|died|death|captive)\b",
        text,
        re.IGNORECASE,
    )


def extract_date(text: str) -> list[Evidence[TypeTag]]:
    fields = catalog_fields(text)
    text, field = _field_or_prose(text, "eventdate")
    if not specimen_text_is_safe(text):
        return []
    if field:
        if match := re.fullmatch(r"([12]\d{3})(?:-(\d{2})(?:-(\d{2}))?)?", text):
            year, month, day = match.groups()
            if month is None:
                return [
                    Evidence(
                        TypeTag.Date(year),
                        certain=_catalog_date_is_certain(year, fields),
                    )
                ]
            date = _format_date(int(year), int(month), int(day or 1))
            if date is not None:
                if day is None:
                    date = date.split(" ", 1)[1]
                return [
                    Evidence(
                        TypeTag.Date(date),
                        certain=_catalog_date_is_certain(date, fields),
                    )
                ]
        return []
    # Museum cataloging and transfer sections can contain several dates, with
    # semicolons between them. Their punctuation does not start a new capture.
    text = re.sub(r"\[MC:[^\]]*\]", "", text, flags=re.IGNORECASE)
    text = re.split(r"\bEx:\s*", text, maxsplit=1, flags=re.IGNORECASE)[0]
    result = []
    for match in _DATE.finditer(text):
        month = match["month"] or match["month_first"]
        day = match["day"] or match["day_last"]
        date = _format_date(int(match["year"]), _MONTHS[month[:3].lower()], int(day))
        if date is None:
            continue
        prefix = text[: match.start()]
        # Do not take one endpoint from a date range or a museum transfer.
        if re.search(
            r"\d\s*[-–]\s*$|\b(?:to|and)\s*$|\bEx:\s*[^;]*$", prefix, re.IGNORECASE
        ):
            continue
        # A named month is safe from accession-number confusion, but only an
        # explicit capture event authorizes adding it (never a receipt date).
        certain = bool(
            re.search(
                r"\b(?:collected|obtained|caught|killed|taken)(?:\s+on)?\s*$",
                prefix,
                re.IGNORECASE,
            )
        )
        if re.search(
            r"\b(?:purchased|bought|acquired|received|returned|presented|registered|accessioned|catalogued|cataloged|died|lived)\b[^;]*$",
            prefix,
            re.IGNORECASE,
        ):
            continue
        result.append(Evidence(TypeTag.Date(date), certain))
    return result


_ORGANS = {
    "skin": SpecimenOrgan.skin,
    "skull": SpecimenOrgan.skull,
    "skeleton": SpecimenOrgan.skeleton,
    "postcranial skeleton": SpecimenOrgan.postcranial_skeleton,
    "in alcohol": SpecimenOrgan.in_alcohol,
    "in spirit": SpecimenOrgan.in_alcohol,
    "baculum": SpecimenOrgan.baculum,
}
_PREPARATION = re.compile(
    r"(?:study )?skin(?:, study)?|skull|(?:postcranial )?skeleton|in (?:alcohol|spirit)|baculum",
    re.IGNORECASE,
)


def extract_organ(text: str) -> list[Evidence[TypeTag]]:
    text, _ = _field_or_prose(text, "preparations")
    if not specimen_text_is_safe(text):
        return []
    # Extract complete, simple preparation clauses. Merely mentioning a skull
    # measurement or a skin colour does not establish a preserved preparation.
    result = []
    for clause in re.split(r"[.;]", text):
        clause = clause.strip()
        if re.search(
            r"\bskin\s+in\s+(?:alcohol|spirit)\b|^with\s+skin\b", clause, re.IGNORECASE
        ):
            continue
        clause = re.sub(
            r"^(?:the )?(?:holo|lecto|neo)?type\s*[:—–-]?\s*",
            "",
            clause,
            flags=re.IGNORECASE,
        )
        clause = re.sub(r"^(?:a |an )?", "", clause, flags=re.IGNORECASE)
        matches = list(_PREPARATION.finditer(clause))
        if not matches:
            continue
        rest = _PREPARATION.sub("", clause)
        if not re.fullmatch(r"(?:[\s,&/+|]|\band\b|\bwith\b)*", rest, re.IGNORECASE):
            continue
        for match in matches:
            key = match[0].lower().replace("study ", "").replace(", study", "")
            result.append(Evidence(TypeTag.Organ(_ORGANS[key]), certain=True))
    # A catalog field is preparation data throughout. Unknown or qualified
    # clauses remain unparsed, rather than being turned into bare Organ tags.
    if any(item.value.organ is SpecimenOrgan.skull for item in result):
        result = [
            (
                Evidence(
                    TypeTag.Organ(SpecimenOrgan.postcranial_skeleton),
                    certain=item.certain,
                )
                if item.value.organ is SpecimenOrgan.skeleton
                else item
            )
            for item in result
        ]
    return list(dict.fromkeys(result))


_NUMBER = r"-?(?:\d{1,3}(?:[, \u00a0\u202f]\d{3})+|\d+)(?:\.\d+)?"
_ALTITUDE = re.compile(
    rf"(?<![\w.,°′'–-])(?P<value>{_NUMBER}(?:\s*[-–]\s*{_NUMBER})?)\s*"
    r"(?P<unit>metres?|meters?|m|feet|foot|ft)\b\.?",
    re.IGNORECASE,
)


def extract_altitude(text: str) -> list[Evidence[TypeTag]]:
    # Accounts of a species' range or a mountain's height need interpretation.
    text = re.split(r"\brange\s*[.:—–-]", text, maxsplit=1, flags=re.IGNORECASE)[0]
    if re.search(
        r"\b(?:rises|summit reaches|restricted|erroneous|incorrect|not|probably)\b",
        text,
        re.IGNORECASE,
    ):
        return []
    result = []
    for match in _ALTITUDE.finditer(text):
        before, after = text[: match.start()], text[match.end() :]
        # A malformed grouping must not turn the final digits into an altitude.
        if re.search(r"\d[\s,]+$", before):
            continue
        # Distances, depths, measurements and coordinate minutes are not elevation.
        if re.match(
            r"\s*(?:[NSEW]\b|north|south|east|west|from|of|below|deep|depth|long|high|tall)",
            after,
            re.IGNORECASE,
        ):
            continue
        if re.search(
            r"(?:depth|deep|below|distance)\W*(?:of\s*)?$", before, re.IGNORECASE
        ):
            continue
        explicit = bool(
            re.search(
                r"(?:alt(?:itude)?|elev(?:ation)?)\.?\s*[:=,]?\s*(?:of\s*)?$",
                before,
                re.IGNORECASE,
            )
        )
        explicit |= bool(
            re.match(
                r"\s*(?:a\.?\s*s\.?\s*l\.?|above sea[ -]level|alt(?:itude)?\b|elev(?:ation)?\b)",
                after,
                re.IGNORECASE,
            )
        )
        value = re.sub(r"\s|,", "", match["value"]).replace("–", "-")
        # A dot before three digits may be a thousands separator (e.g. Spanish
        # '1.800 m'). Do not silently reinterpret either decimal convention.
        if re.search(r"\.\d{3}(?:$|-)", value):
            continue
        if re.search(r"\d\s*(?:and|to|or)\s*$", before, re.IGNORECASE):
            continue
        if re.match(
            r"\s*\)?\s*(?:\([^)]*\)\s*)?above\s+(?!sea[ -]level)", after, re.IGNORECASE
        ):
            continue
        unit = (
            AltitudeUnit.m if match["unit"].lower().startswith("m") else AltitudeUnit.ft
        )
        # Bare '500 m' is useful for validation, but can be a horizontal offset.
        certain = explicit and not re.search(
            r"\b(?:about|approximately|ca|circa|between|to|or|over|under|nearly|common|occurs?|distributed|distribution|ranges?|ranging|inhabits?)\b|[~<>?]",
            text,
            re.IGNORECASE,
        )
        # Keep inference within the existing Altitude tag's accepted syntax.
        certain = bool(
            certain and re.fullmatch(r"-?\d+(?:[-.]\d+)?", value) and value != "000"
        )
        result.append(Evidence(TypeTag.Altitude(value, unit), certain))
    return result


def altitude_interval(tag: TypeTag) -> tuple[Decimal, Decimal] | None:
    if not isinstance(tag, TypeTag.Altitude):
        return None
    if match := re.fullmatch(r"(-?\d+(?:\.\d+)?)(?:-(-?\d+(?:\.\d+)?))?", tag.altitude):
        factor = Decimal("0.3048") if tag.unit is AltitudeUnit.ft else Decimal(1)
        low, high = sorted(
            (Decimal(match[1]) * factor, Decimal(match[2] or match[1]) * factor)
        )
        return low, high
    return None


def tags_compatible(left: TypeTag, right: TypeTag) -> bool:
    if isinstance(left, TypeTag.Altitude) and isinstance(right, TypeTag.Altitude):
        a, b = altitude_interval(left), altitude_interval(right)
        if a is None or b is None:
            return True  # Malformed tags are handled by the syntax lint.
        tolerance = max(Decimal(1), max(abs(n) for n in (*a, *b)) * Decimal("0.005"))
        return a[0] <= b[1] + tolerance and b[0] <= a[1] + tolerance
    if isinstance(left, TypeTag.Date) and isinstance(right, TypeTag.Date):
        # The shorter date can be a source's less precise statement of the same
        # event. Do not overwrite it merely to increase precision.
        a, b = left.date.split(), right.date.split()
        return a[-min(len(a), len(b)) :] == b[-min(len(a), len(b)) :]
    return left == right


def altitudes_equivalent(left: TypeTag, right: TypeTag) -> bool:
    """Inference requires equivalent ranges, rather than mere overlap."""
    a, b = altitude_interval(left), altitude_interval(right)
    if a is None or b is None:
        return False
    tolerance = max(Decimal(1), max(abs(n) for n in (*a, *b)) * Decimal("0.005"))
    return all(abs(x - y) <= tolerance for x, y in zip(a, b, strict=True))


def extract_collectors(text: str) -> list[Evidence[str]]:
    value, field = _field_or_prose(text, "recordedby")
    if field:
        value = re.sub(r"^Collector\(s\):\s*", "", value, flags=re.IGNORECASE)
    else:
        if not specimen_text_is_safe(text):
            return []
        match = re.search(
            r"(?:\b(?:collected|obtained|caught|killed|taken)\s+"
            r"(?:(?!\b(?:by|presented|donated|described|purchased)\b)(?:[^.;\n]|\.(?=\s*\d))){0,70}?\bby\s+"
            r"|\bcollector\s*[:,]\s*)"
            r"(?P<names>[^\n]+)",
            text,
            re.IGNORECASE,
        )
        if match is None:
            return []
        value = match["names"]
        value = re.sub(r"\b(?:Dr|Mr|Mrs|Miss|Prof|Capt|Col|Major)\.?\s+", "", value)
        if comma := re.search(r",\s*(.*)", value):
            # A comma can introduce another collector, not just a date. Do not
            # silently retain only the first member of an unparsed team.
            if not re.match(
                r"\d|(?:on|at|in|original|field|and presented|and donated)\b",
                comma[1],
                re.IGNORECASE,
            ):
                return [Evidence(value, certain=False)]
        value = re.split(r"(?<=[a-z]{2})\.\s|,|\s+(?:on|at|in)\s+", value, maxsplit=1)[
            0
        ]
        # Do not confuse a subsequent donor with a member of the field team.
        if re.search(r"\b(?:presented|donated|purchased)\b", value, re.IGNORECASE):
            return []
    if not specimen_text_is_safe(value):
        return []
    parts = re.split(r"\s+(?:and|&)\s+|\s*[;|]\s*", value)
    result = []
    for part in parts:
        part = re.sub(
            r"^(?:(?:Dr|Mr|Mrs|Miss|Prof|Capt|Col|Major)\.?\s+)+",
            "",
            part,
            flags=re.IGNORECASE,
        ).strip(" .")
        if not re.fullmatch(r"[^\W\d_][\w. '\u2019-]+", part) or len(part.split()) < 2:
            return [Evidence(value, certain=False)] if value else []
        result.append(Evidence(part, certain=True))
    return result


def normalize_person_name(text: str) -> str:
    # Keep accents and hyphens: fuzzy matches are not safe person associations.
    return re.sub(r"[.\s]+", "", text).casefold()
