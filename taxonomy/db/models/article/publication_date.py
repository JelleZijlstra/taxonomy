"""Extract publication-date evidence from the opening pages of Article PDFs."""

from __future__ import annotations

import dataclasses
import datetime
import difflib
import re
import unicodedata
from typing import Literal, cast

MONTH_NAMES = {
    1: ("january", "jan", "januar", "janvier", "janeiro", "enero", "gennaio"),
    2: (
        "february",
        "feb",
        "februar",
        "februari",
        "fevrier",
        "febrero",
        "fevereiro",
        "febbraio",
    ),
    3: ("march", "mar", "marz", "maart", "mars", "marzo", "marco"),
    4: ("april", "apr", "avril", "abril", "aprile"),
    5: ("may", "mai", "mei", "mayo", "maggio"),
    6: ("june", "jun", "juni", "junie", "juin", "junio", "giugno", "junho"),
    7: ("july", "jul", "juli", "juillet", "julio", "luglio", "julho"),
    8: ("august", "aug", "augustus", "aout", "agosto"),
    9: ("september", "sep", "sept", "septembre", "septiembre", "setembro"),
    10: (
        "october",
        "oct",
        "oktober",
        "okt",
        "octobre",
        "octubre",
        "ottobre",
        "outubro",
    ),
    11: ("november", "nov", "novembre", "noviembre", "novembro"),
    12: (
        "december",
        "dec",
        "dezember",
        "dez",
        "decembre",
        "diciembre",
        "dicembre",
        "dezembro",
        "dic",
    ),
}

MONTH_LOOKUP = {name: number for number, names in MONTH_NAMES.items() for name in names}
MONTH_PATTERN = (
    "(?:"
    + "|".join(
        sorted((re.escape(name) for name in MONTH_LOOKUP), key=len, reverse=True)
    )
    + r")\.?"
)
YEAR_PATTERN = r"(?:1[5-9]\d{2}|20\d{2})"
DAY_PATTERN = r"(?:3[01]|[12]\d|0?[1-9])"
DATE_PATTERN = (
    r"(?:"
    rf"(?:(?P<ocr_leading_one>[il])\s*(?P<ocr_ones>\d)|"
    rf"(?P<ocr_tens>[12])\s*(?P<ocr_trailing_one>[il]))"
    rf"(?:[.,]\s*|\s+)(?P<ocr_month>{MONTH_PATTERN})\s*,?\s*"
    rf"(?P<ocr_year>{YEAR_PATTERN})|"
    rf"(?P<dmy_day>{DAY_PATTERN})(?:st|nd|rd|th|er)?"
    rf"(?:[.,]\s*|\s+)(?:de\s+)?"
    rf"(?P<dmy_month>{MONTH_PATTERN})\s*,?\s*(?P<dmy_year>{YEAR_PATTERN})|"
    rf"(?P<mdy_month>{MONTH_PATTERN})\s+(?P<mdy_day>{DAY_PATTERN})"
    rf"(?:st|nd|rd|th)?\s*,?\s*(?P<mdy_year>{YEAR_PATTERN})|"
    rf"(?P<iso_year>{YEAR_PATTERN})[-/.](?P<iso_month>\d{{1,2}})"
    rf"[-/.](?P<iso_day>{DAY_PATTERN})|"
    rf"(?P<num_day>{DAY_PATTERN})[-/.](?P<num_month>\d{{1,2}})[-/.]"
    rf"(?P<num_year>{YEAR_PATTERN})|"
    rf"(?P<year_month_year>{YEAR_PATTERN})[-/](?P<year_month>0[1-9]|1[0-2])|"
    rf"(?P<chinese_year>{YEAR_PATTERN})\s*年\s*(?P<chinese_month>\d{{1,2}})\s*月|"
    rf"(?P<month_only>{MONTH_PATTERN})\s*,?\s*(?P<month_year>{YEAR_PATTERN})"
    r")"
)
DATE = re.compile(rf"(?<![\w]){DATE_PATTERN}(?![\w])")
MONTH_YEAR_DATE = re.compile(
    rf"(?<![\w])(?P<month_only>{MONTH_PATTERN})\s*,?\s*"
    rf"(?P<month_year>{YEAR_PATTERN})(?![\w])"
)
INTERRUPTED_DMY_DATE = re.compile(
    rf"(?<![\w])(?P<dmy_day>{DAY_PATTERN})(?:st|nd|rd|th|er)?[.,]?\s*"
    rf"(?:no\.?|nr\.?)\s*\d+\.?\s*"
    rf"(?P<dmy_month>{MONTH_PATTERN})\s*,?\s*(?P<dmy_year>{YEAR_PATTERN})(?![\w])"
)
NUMBER_ONE_DATE = re.compile(
    rf"(?<![\w])(?:number|no\.?|nr\.?)\s*\d+\s+"
    rf"(?P<ocr_single_one>[il])\s+(?P<ocr_month>{MONTH_PATTERN})\s*,?\s*"
    rf"(?P<ocr_year>{YEAR_PATTERN})(?![\w])"
)
PUBLICATION_LABEL_TEXT = (
    r"(?:"
    r"published(?:\s+(?:online|on\s+line|in\s+print))?|"
    r"publication\s+date(?:\s*\((?:online|print)\))?|"
    r"issue\s+date|[ilt]ssued|advance\s+access(?:\s+publication)?|"
    r"online(?:\s+publication)?|on\s+line|"
    r"publiee?|publication\s+en\s+ligne|publicado|publicada|publicacao|"
    r"erschienen|ausgegeben"
    r")"
)
EXPLICIT_DATE = re.compile(
    rf"(?P<label>{PUBLICATION_LABEL_TEXT})\s*"
    rf"(?:on|le|el|em|am|:|-)?\s*{DATE_PATTERN}"
)
NEGATIVE_LABEL = re.compile(
    r"(?:received|accepted(?:\s+for\s+publication)?|submitted|revised|"
    r"accessed|retrieved|downloaded|"
    r"collected|collection|read|communicated|presented|meeting|copyright|"
    r"recibido|aceptado|recebido|aceito|soumis|accepte|eingegangen)\s*"
    rf"(?:on|le|el|em|am|:|-)?\s*"
    rf"(?:{DAY_PATTERN}(?:st|nd|rd|th|er)?\s*[.-]?\s*)?$"
)
REPOSITORY_COVER = re.compile(
    r"(?:this content downloaded|www\.jstor\.org/stable|"
    r"researchgate\.net/publication|"
    r"see discussions, stats, and author profiles for this publication|"
    r"all content following this page was uploaded|"
    r"\bcall\s*#|\bborrower:|\blending string:|\brapid[:;]|"
    r"\berstellungsdatum:)"
)
HEADER_LINE_CONTEXT = re.compile(
    r"(?:issn|volume|\bvol\.?|\bband\b|\btome\b|journal|revue|proceedings|"
    r"bulletin|abhandlungen|beitr\.?|livraison)"
)
HEADER_BLOCK_CONTEXT = re.compile(
    r"(?:issn|volume|\bvol\.?|\bband\b|\btome\b|journal|revue|proceedings|"
    r"bulletin|museum|novitates|soc[il]ety|societe|abhandlungen|beitr\.?|livraison|"
    r"\bnumber\s*\d|\bno\.?\s*\d|\bnr\.?\s*\d)"
)
VOLUME_LABEL = re.compile(
    r"(?:\bvolume|\bvol\.?|\bband|\btome|\bissue|\bpart|\bnumber|\bno\.?|"
    r"\bnr\.?|\bbulletin|\bpaper)\s*$"
)
NUMBER_RANGE_PREFIX = re.compile(
    r"(?:\bissue|\bpart|\bnumber|\bno\.?|\bnr\.?)\s*\d+\s*[-–—]\s*$"
)
PRODUCTION_TIMESTAMP = re.compile(
    r"(?:\bdtpro\b|\ballen press\b|\bfile\s*#|"
    r"\b\d{1,2}:\d{2}\s*(?:(?:am|pm)\b|uhr\b))"
)
LIBRARY_STAMP = re.compile(
    r"(?:shelve with bound volumes|univ(?:ersity)?\.?\s+of\s+\w+\s+libraries|"
    r"state university.{0,100}\blibrary|\bexchange\b|library date stamp|\bvkt\b)"
)
DATE_NUMBER_LAYOUT = re.compile(r"\bbulletin\s+(?:volume\s+\d+\s+)?$")
BODY_PROSE_DATE = re.compile(
    r"(?:during the month of|voyez\s+bulletin|journal des savants|"
    r"meeting.{0,80}\bin\s+|recent issues in this series|"
    r"numismatic chronicle|westminster review|literary gazette|"
    r"read and confirmed|revue des deux mondes|comptes rendus|"
    r"sit.{0,20}ber(?:icht|ieht)|gesellschaft.{0,100}\bvom\b)"
)

EvidenceKind = Literal["explicit publication statement", "bibliographic header"]
DateRole = Literal["online", "print", "unspecified"]


@dataclasses.dataclass(frozen=True, slots=True)
class PublicationDateEvidence:
    date: str
    page_number: int
    matched_text: str
    context: str
    kind: EvidenceKind
    role: DateRole = "unspecified"


@dataclasses.dataclass(frozen=True, slots=True)
class FoldedText:
    text: str
    raw: str
    raw_offsets: tuple[int, ...]

    def raw_span(self, start: int, end: int) -> str:
        raw_start = self.raw_offsets[start]
        raw_end = self.raw_offsets[end - 1] + 1
        return re.sub(r"\s+", " ", self.raw[raw_start:raw_end]).strip()

    def context(self, start: int, end: int, radius: int = 130) -> str:
        raw_start = self.raw_offsets[max(0, start - radius)]
        raw_end = self.raw_offsets[min(len(self.raw_offsets) - 1, end + radius)] + 1
        return re.sub(r"\s+", " ", self.raw[raw_start:raw_end]).strip()


def fold_text(raw: str) -> FoldedText:
    """Case-fold OCR text while retaining offsets into the source text."""
    output: list[str] = []
    offsets: list[int] = []
    previous_was_space = False
    for index, character in enumerate(raw):
        folded = "".join(
            part
            for part in unicodedata.normalize("NFKD", character.casefold())
            if not unicodedata.combining(part)
        )
        for part in folded:
            if part.isspace():
                if output and not previous_was_space:
                    output.append(" ")
                    offsets.append(index)
                previous_was_space = True
            else:
                output.append(part)
                offsets.append(index)
                previous_was_space = False
    return FoldedText("".join(output), raw, tuple(offsets))


def _clean_month(value: str) -> int:
    return MONTH_LOOKUP[value.rstrip(".")]


def _parse_date(match: re.Match[str]) -> str | None:
    groups = match.groupdict()
    try:
        if groups.get("ocr_month"):
            if groups.get("ocr_single_one"):
                day = 1
            elif groups.get("ocr_leading_one"):
                day = 10 + int(cast(str, groups["ocr_ones"]))
            else:
                day = int(cast(str, groups["ocr_tens"])) * 10 + 1
            date = datetime.date(
                int(cast(str, groups["ocr_year"])),
                _clean_month(cast(str, groups["ocr_month"])),
                day,
            )
            return date.isoformat()
        if groups.get("dmy_day"):
            date = datetime.date(
                int(cast(str, groups["dmy_year"])),
                _clean_month(cast(str, groups["dmy_month"])),
                int(cast(str, groups["dmy_day"])),
            )
            return date.isoformat()
        if groups.get("mdy_day"):
            date = datetime.date(
                int(cast(str, groups["mdy_year"])),
                _clean_month(cast(str, groups["mdy_month"])),
                int(cast(str, groups["mdy_day"])),
            )
            return date.isoformat()
        if groups.get("iso_day"):
            date = datetime.date(
                int(cast(str, groups["iso_year"])),
                int(cast(str, groups["iso_month"])),
                int(cast(str, groups["iso_day"])),
            )
            return date.isoformat()
        if groups.get("num_day"):
            day = int(cast(str, groups["num_day"]))
            month = int(cast(str, groups["num_month"]))
            # Without a named month, 03/04/1994 is not safe independent evidence.
            if day <= 12 and month <= 12:
                return None
            date = datetime.date(int(cast(str, groups["num_year"])), month, day)
            return date.isoformat()
        if groups.get("year_month_year"):
            year = int(cast(str, groups["year_month_year"]))
            month = int(cast(str, groups["year_month"]))
            datetime.date(year, month, 1)
            return f"{year:04d}-{month:02d}"
        if groups.get("chinese_year"):
            year = int(cast(str, groups["chinese_year"]))
            month = int(cast(str, groups["chinese_month"]))
            datetime.date(year, month, 1)
            return f"{year:04d}-{month:02d}"
        year = int(cast(str, groups["month_year"]))
        month = _clean_month(cast(str, groups["month_only"]))
        result = f"{year:04d}-{month:02d}"
    except ValueError, KeyError:
        return None
    return result


def _label_role(label: str) -> DateRole:
    if "online" in label or "on line" in label or "advance access" in label:
        return "online"
    if "print" in label:
        return "print"
    return "unspecified"


def _line_number(raw: str, raw_offset: int) -> int:
    return raw.count("\n", 0, raw_offset) + 1


def _label_before(text: str, start: int, pattern: re.Pattern[str]) -> bool:
    return pattern.search(text[max(0, start - 95) : start]) is not None


def _mentions_another_month_nearby(text: FoldedText, start: int) -> bool:
    prefix = text.text[max(0, start - 45) : start]
    other_month = (
        "(?:"
        + "|".join(
            re.escape(name)
            for names in MONTH_NAMES.values()
            for name in names
            if len(name) >= 4
        )
        + ")"
    )
    return bool(
        re.search(
            rf"(?<!\w){other_month}\.?(?!\w)\s*"
            rf"(?:(?:and|e|et|y|und|&)\s+|[-–—]\s*)$",
            prefix,
        )
    )


def _is_day_range_endpoint(text: FoldedText, start: int) -> bool:
    prefix = text.text[max(0, start - 45) : start]
    return bool(re.search(rf"(?<!\d){DAY_PATTERN}\s*[-–—]\s*$", prefix))


def _is_day_range_before_month(text: FoldedText, start: int) -> bool:
    prefix = text.text[max(0, start - 55) : start]
    match = re.search(rf"(?<!\d){DAY_PATTERN}\s*[-–—]\s*{DAY_PATTERN}\s*$", prefix)
    if match is None:
        return False
    before_range = prefix[: match.start()]
    return not bool(
        re.search(r"(?:\bissue|\bpart|\bnumber|\bno\.?|\bnr\.?)\s*$", before_range)
    )


def _is_header_match(
    text: FoldedText, match: re.Match[str], title_position: int
) -> bool:
    if _label_before(text.text, match.start(), NEGATIVE_LABEL):
        return False
    if _mentions_another_month_nearby(text, match.start()):
        return False
    if _is_day_range_before_month(text, match.start()):
        return False
    if match.groupdict().get("dmy_day"):
        prefix = text.text[max(0, match.start() - 30) : match.start()]
        if (
            VOLUME_LABEL.search(prefix)
            or NUMBER_RANGE_PREFIX.search(prefix)
            or DATE_NUMBER_LAYOUT.search(prefix)
            or _is_day_range_endpoint(text, match.start())
        ):
            return False
    raw_offset = text.raw_offsets[match.start()]
    line_number = _line_number(text.raw, raw_offset)
    raw_lines = text.raw.splitlines()
    if not 1 <= line_number <= len(raw_lines):
        return False
    folded_line = fold_text(raw_lines[line_number - 1]).text
    if PRODUCTION_TIMESTAMP.search(folded_line):
        return False
    block_start = max(0, line_number - 3)
    block_end = min(len(raw_lines), line_number + 2)
    folded_block = fold_text("\n".join(raw_lines[block_start:block_end])).text
    nearby = text.text[max(0, match.start() - 180) : match.end() + 100]
    date_and_following_text = text.text[match.start() : match.end() + 80]
    if (
        LIBRARY_STAMP.search(nearby)
        or PRODUCTION_TIMESTAMP.search(date_and_following_text)
        or BODY_PROSE_DATE.search(nearby)
    ):
        return False
    return (
        match.start() <= 800
        and match.start() < title_position
        and line_number <= 20
        and (
            bool(HEADER_LINE_CONTEXT.search(folded_line))
            or bool(HEADER_BLOCK_CONTEXT.search(folded_block))
            or bool(HEADER_BLOCK_CONTEXT.search(nearby))
        )
    )


def _upgrade_month_date_from_nearby_title_word(
    date: str, text: FoldedText, match: re.Match[str], title: str | None
) -> str:
    if len(date) != 7 or not title:
        return date
    prefix = text.text[max(0, match.start() - 45) : match.start()]
    title_word_day = re.search(
        rf"(?<!\d)(?P<day>{DAY_PATTERN})\s*(?P<word>[a-z]{{4,24}})\s*$", prefix
    )
    ocr_template_day = re.search(
        rf"(?<!\d)(?P<day>{DAY_PATTERN})\s*(?:x{{2,}}\s*)+$", prefix
    )
    if ocr_template_day is not None:
        later_year = re.match(
            rf"\s*(?P<year>{YEAR_PATTERN})(?!\d)",
            text.text[match.end() : match.end() + 12],
        )
        if later_year is not None and date.startswith("2010-"):
            date = f"{later_year.group('year')}-{date[5:]}"
        day = int(ocr_template_day.group("day"))
    elif (
        title_word_day is not None
        and title_word_day.group("word") in fold_text(title).text
    ):
        day = int(title_word_day.group("day"))
    else:
        return date
    try:
        parsed = datetime.date.fromisoformat(f"{date}-{day:02d}")
    except ValueError:
        return date
    return parsed.isoformat()


def _is_early_metadata_match(text: FoldedText, match: re.Match[str]) -> bool:
    raw_offset = text.raw_offsets[match.start()]
    return match.start() <= 1_200 and _line_number(text.raw, raw_offset) <= 40


def _article_title_position(page: str, title: str | None) -> int | None:
    if not title:
        return None
    folded_title = fold_text(title).text
    folded_page = fold_text(page[:6_000]).text
    if len(folded_title) < 20:
        return None
    if folded_title in folded_page:
        return folded_page.index(folded_title)
    longest = difflib.SequenceMatcher(
        None, folded_title, folded_page, autojunk=False
    ).find_longest_match()
    if longest.size >= 20 and longest.size / len(folded_title) >= 0.55:
        return longest.b
    return None


def _deduplicate_evidence(
    evidence: list[PublicationDateEvidence],
) -> tuple[PublicationDateEvidence, ...]:
    """Keep one witness per date and discard prefix-only dates when possible."""
    by_date: dict[str, PublicationDateEvidence] = {}
    for item in evidence:
        by_date.setdefault(item.date, item)
    dates = set(by_date)
    less_precise = {
        date
        for date in dates
        if any(other.startswith(f"{date}-") for other in dates if other != date)
    }
    return tuple(by_date[date] for date in sorted(dates - less_precise))


def extract_publication_date_evidence(
    pages: list[str], title: str | None
) -> tuple[PublicationDateEvidence, ...]:
    """Return high-confidence dates from the first five PDF pages.

    Explicit publication statements take precedence over unlabelled bibliographic
    headers. The function deliberately has no stored-date parameter: its output is
    independent evidence that callers can compare exactly with database tags.
    """
    explicit: list[PublicationDateEvidence] = []
    headers: list[PublicationDateEvidence] = []
    saw_online_date = False
    for page_number, page in enumerate(pages[:5], start=1):
        if not page.strip():
            continue
        folded = fold_text(page)
        # Repository-generated covers contain catalog or upload metadata rather
        # than evidence printed as part of the published work.
        if REPOSITORY_COVER.search(folded.text):
            continue
        title_position = _article_title_position(page, title)
        for match in EXPLICIT_DATE.finditer(folded.text):
            if not _is_early_metadata_match(folded, match):
                continue
            if page_number > 1 and title_position is None:
                continue
            role = _label_role(cast(str, match.group("label")))
            # A retrospective online release is not evidence for the print
            # publication date recorded by an internal PublicationDate tag.
            if role == "online":
                saw_online_date = True
                continue
            date = _parse_date(match)
            if date is None:
                continue
            explicit.append(
                PublicationDateEvidence(
                    date,
                    page_number,
                    folded.raw_span(match.start(), match.end()),
                    folded.context(match.start(), match.end()),
                    "explicit publication statement",
                    role,
                )
            )
        if title_position is None:
            continue
        matches = sorted(
            (
                *DATE.finditer(folded.text),
                *MONTH_YEAR_DATE.finditer(folded.text),
                *INTERRUPTED_DMY_DATE.finditer(folded.text),
                *NUMBER_ONE_DATE.finditer(folded.text),
            ),
            key=lambda match: (match.start(), match.end()),
        )
        for match in matches:
            if not _is_header_match(folded, match, title_position):
                continue
            date = _parse_date(match)
            if date is None:
                continue
            date = _upgrade_month_date_from_nearby_title_word(
                date, folded, match, title
            )
            headers.append(
                PublicationDateEvidence(
                    date,
                    page_number,
                    folded.raw_span(match.start(), match.end()),
                    folded.context(match.start(), match.end()),
                    "bibliographic header",
                )
            )
    # Publisher cover sheets commonly combine a retrospective online-release
    # date with the nominal date of an issue that actually appeared later. If
    # there is no explicit print-publication statement, neither date safely
    # establishes the print publication date.
    return _deduplicate_evidence(explicit if explicit or saw_online_date else headers)
