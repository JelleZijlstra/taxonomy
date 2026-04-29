import argparse
import csv
import importlib
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

lib = importlib.import_module("data_import.lib")
refparse = importlib.import_module("taxonomy.refmatch.parse")

SCIENTIFIC_DESCRIPTIONS = "Scientific Descriptions"
GENERAL_LIST = "General List"
SECTION_HEADERS = {
    "References of Scientific Descriptions": SCIENTIFIC_DESCRIPTIONS,
    "General List of References": GENERAL_LIST,
}

DEFAULT_PDF = lib.DATA_DIR / "chiroptera-hmw-refs.pdf"
DEFAULT_OUTPUT = lib.DATA_DIR / "chiroptera-hmw-refs.csv"
DEFAULT_PARSED_OUTPUT = lib.DATA_DIR / "chiroptera-hmw-refs-parsed.csv"
YEAR_RE = r"\d{4}(?:–\d{4})?[a-z]*"
AUTHOR_YEAR_RE = re.compile(
    rf"^(?P<authors>.+?) \((?P<year>{YEAR_RE})\)\. (?P<body>.*)$"
)
JOURNAL_TAIL_RE = re.compile(
    r"^(?P<prefix>.+) (?P<volume>\d+(?:–\d+)?[A-Za-z]?)"
    r"(?P<issue>(?:\s*\([^)]*\))*): (?P<pages>.+)$"
)
JOURNAL_ISSUE_ONLY_RE = re.compile(
    r"^(?P<prefix>.+) (?P<issue>\([^)]*\)): (?P<pages>.+)$"
)
SCIENTIFIC_CITATION_RE = re.compile(
    r"^(?P<prefix>.+) (?P<volume>\d+(?:–\d+)?[A-Za-z]?)"
    r"(?P<issue>(?:\s*\([^)]*\))*): (?P<pages>.+)$"
)
SCIENTIFIC_NO_VOLUME_RE = re.compile(r"^(?P<prefix>.+): (?P<pages>.+)$")
CHAPTER_RE = re.compile(
    r"^(?P<title>.+?)\. (?P<pages>Pp\. .+?|Page \d+) in: (?P<book>.*)$"
)
EDITED_BOOK_RE = re.compile(
    rf"^(?P<editors>.+?) eds?\. \((?P<book_year>{YEAR_RE})\)\. (?P<book>.*)$"
)
CROSS_REF_BOOK_RE = re.compile(rf"^(?P<book_ref>.+?) \((?P<book_year>{YEAR_RE})\)\.?$")
URL_RE = re.compile(
    r"(?:URL:\s*)?(?P<url>(?:https?://|www\.).*?)(?= \(download |\.$|$)"
)
ACCESSED_RE = re.compile(r"\(download (?P<accessed>[^)]+)\)")
LOWERCASE_AUTHOR_PREFIXES = ("d'", "d’", "de ", "del ", "dos ", "du ", "van ", "von ")

# The reference list is printed in three columns. These are the approximate
# left edges of the text columns in pdftotext TSV and pdftohtml XML units.
PLAIN_COLUMN_LEFTS = (36.0, 214.0, 392.0)
FORMATTED_COLUMN_LEFTS = (55.0, 322.0, 588.0)
ITALIC_RUN_RE = re.compile(r"<i>.*?</i>(?:\s*<i>.*?</i>)*")
TAG_RE = re.compile(r"</?i>")
JOURNAL_ABBREV_TOKEN = "J."  # noqa: S105
SERIES_RE = re.compile(
    r"^(?P<container>.+?),? (?P<series>(?:\d+(?:st|nd|rd|th|[ae])?) S[ée]r\.)$"
)


@dataclass(frozen=True)
class Word:
    page: int
    line_key: tuple[int, int, int, int]
    left: float
    top: float
    width: float
    text: str


@dataclass(frozen=True)
class TextSegment:
    page: int
    left: float
    top: float
    width: float
    text: str
    is_italic: bool


@dataclass(frozen=True)
class TextLine:
    page: int
    column: int
    column_left: float
    left: float
    top: float
    text: str
    formatted_text: str


@dataclass(frozen=True)
class Reference:
    section: str
    text: str
    formatted_text: str


def match_author_year(text: str) -> re.Match[str] | None:
    match = AUTHOR_YEAR_RE.match(text)
    if match is None:
        return None
    authors = match.group("authors")
    if authors.startswith("Version "):
        return None
    if any(
        marker in authors
        for marker in (" Pp. ", " Page ", " in: ", " URL:", "http", ": ")
    ):
        return None
    if re.search(r"\. [A-ZÁÄÅÉÍÓÖÜØ]", authors):
        return None
    if (
        authors
        and authors[0].islower()
        and not authors.startswith(LOWERCASE_AUTHOR_PREFIXES)
    ):
        return None
    return match


def has_author_year(text: str) -> bool:
    return match_author_year(text) is not None


def has_printed_author_year(text: str) -> bool:
    return AUTHOR_YEAR_RE.match(text) is not None


def is_continuation_fragment(text: str) -> bool:
    return text.startswith(("Version ", "URL:", "http://", "https://"))


def expects_book_citation_continuation(text: str) -> bool:
    text = text.rstrip()
    if text.endswith(" in:"):
        return True
    if " in:" not in text:
        return False
    tail = text.rsplit(" in:", maxsplit=1)[1].strip()
    return len(tail) < 80 and tail.endswith((",", "&"))


def find_embedded_reference_start(text: str) -> int | None:
    for match in re.finditer(r" (?=[A-ZÁÄÅÉÍÓÖÜØ][^()]{1,250} \()", text):
        start = match.start() + 1
        if text[:start].rstrip().endswith(" in:"):
            continue
        author_year_match = match_author_year(text[start:])
        if author_year_match is None:
            continue
        authors = author_year_match.group("authors")
        if "URL:" in authors or "http" in authors or "download " in authors:
            continue
        if authors.startswith("Version "):
            continue
        return start
    return None


def normalize_references(references: Iterable[Reference]) -> list[Reference]:
    refs = list(references)
    normalized: list[Reference] = []
    index = 0
    while index < len(refs):
        reference = refs[index]
        if (
            (
                normalized
                and normalized[-1].section == reference.section
                and expects_book_citation_continuation(normalized[-1].text)
            )
            or is_continuation_fragment(reference.text)
            or not has_author_year(reference.text)
        ):
            consumed_next = False
            embedded_start = find_embedded_reference_start(reference.text)
            continuation = reference
            embedded_reference = None
            if embedded_start is not None:
                continuation, embedded_reference = split_reference(
                    reference, embedded_start
                )
            if normalized and normalized[-1].section == reference.section:
                normalized[-1] = append_reference(
                    normalized[-1], continuation.text, continuation.formatted_text
                )
            elif index + 1 < len(refs) and refs[index + 1].section == reference.section:
                next_reference = refs[index + 1]
                normalized.append(
                    Reference(
                        reference.section,
                        append_reference_line(continuation.text, next_reference.text),
                        append_formatted_reference_line(
                            continuation.formatted_text, next_reference.formatted_text
                        ),
                    )
                )
                consumed_next = True
            else:
                normalized.append(continuation)
            if embedded_reference is not None:
                refs.insert(index + 1, embedded_reference)
            index += 2 if consumed_next else 1
        else:
            normalized.append(reference)
            index += 1
    return normalized


def run_pdftotext(pdf: Path) -> str:
    if shutil.which("pdftotext") is None:
        raise RuntimeError("pdftotext is required; install Poppler to run this script")
    result = subprocess.run(
        ["pdftotext", "-tsv", str(pdf), "-"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def run_pdftohtml(pdf: Path) -> str:
    if shutil.which("pdftohtml") is None:
        raise RuntimeError("pdftohtml is required; install Poppler to run this script")
    result = subprocess.run(
        ["pdftohtml", "-xml", "-i", "-stdout", str(pdf)],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def iter_words(tsv_text: str) -> Iterable[Word]:
    reader = csv.DictReader(StringIO(tsv_text), delimiter="\t")
    for row in reader:
        if row["level"] != "5" or row["text"].startswith("###"):
            continue
        text = row["text"]
        left = float(row["left"])
        if left < 30 and text.isdigit():
            continue
        page = int(row["page_num"])
        yield Word(
            page=page,
            line_key=(
                page,
                int(row["par_num"]),
                int(row["block_num"]),
                int(row["line_num"]),
            ),
            left=left,
            top=float(row["top"]),
            width=float(row["width"]),
            text=text,
        )


def join_words(words: Sequence[Word]) -> str:
    words = sorted(words, key=lambda word: word.left)
    pieces: list[str] = []
    last_right: float | None = None
    for word in words:
        if last_right is not None and word.left - last_right > 0.8:
            pieces.append(" ")
        pieces.append(word.text)
        last_right = max(last_right or 0, word.left + word.width)
    return "".join(pieces)


def xml_text(element: ET.Element) -> str:
    return "".join(element.itertext())


def is_italic(element: ET.Element) -> bool:
    return any(child.tag == "i" for child in element.iter())


def iter_text_segments(xml_text_content: str) -> Iterable[TextSegment]:
    root = ET.fromstring(xml_text_content)
    for page_element in root.iter("page"):
        page = int(page_element.attrib["number"])
        for text_element in page_element.iter("text"):
            text = xml_text(text_element)
            if text.startswith("###"):
                continue
            left = float(text_element.attrib["left"])
            if left < 45 and text.strip().isdigit():
                continue
            yield TextSegment(
                page=page,
                left=left,
                top=float(text_element.attrib["top"]),
                width=float(text_element.attrib["width"]),
                text=text,
                is_italic=is_italic(text_element),
            )


def format_segment(segment: TextSegment) -> str:
    if segment.is_italic:
        return f"<i>{segment.text}</i>"
    return segment.text


def join_segments(segments: Sequence[TextSegment], *, formatted: bool) -> str:
    segments = sorted(segments, key=lambda segment: segment.left)
    pieces: list[str] = []
    last_right: float | None = None
    for segment in segments:
        if not segment.text:
            continue
        text = format_segment(segment) if formatted else segment.text
        if (
            last_right is not None
            and segment.left - last_right > 1.2
            and pieces
            and not strip_italic_tags(pieces[-1]).endswith(" ")
            and not text.startswith(" ")
        ):
            pieces.append(" ")
        pieces.append(text)
        last_right = max(last_right or 0, segment.left + segment.width)
    return "".join(pieces)


def get_column(left: float, column_lefts: Sequence[float], gap: float) -> int:
    for index, next_left in enumerate(column_lefts[1:], start=1):
        if left < next_left - gap:
            return index - 1
    return len(column_lefts) - 1


def extract_formatted_lines(xml_text_content: str) -> list[TextLine]:
    segments_by_line: dict[tuple[int, int, int], list[TextSegment]] = defaultdict(list)
    for segment in iter_text_segments(xml_text_content):
        column = get_column(segment.left, FORMATTED_COLUMN_LEFTS, 7)
        segments_by_line[(segment.page, column, round(segment.top))].append(segment)

    lines = []
    for (page, column, _top), segments in segments_by_line.items():
        left = min(segment.left for segment in segments)
        top = min(segment.top for segment in segments)
        lines.append(
            TextLine(
                page=page,
                column=column,
                column_left=FORMATTED_COLUMN_LEFTS[column],
                left=left,
                top=top,
                text=join_segments(segments, formatted=False).strip(),
                formatted_text=join_segments(segments, formatted=True).strip(),
            )
        )
    return sorted(lines, key=lambda line: (line.page, line.column, line.top, line.left))


def get_plain_column(left: float) -> int:
    return min(
        range(len(PLAIN_COLUMN_LEFTS)),
        key=lambda index: abs(left - PLAIN_COLUMN_LEFTS[index]),
    )


def extract_plain_lines(tsv_text: str) -> list[TextLine]:
    words_by_line: dict[tuple[int, int, int, int], list[Word]] = defaultdict(list)
    for word in iter_words(tsv_text):
        words_by_line[word.line_key].append(word)

    lines = []
    for words in words_by_line.values():
        left = min(word.left for word in words)
        top = min(word.top for word in words)
        text = join_words(words).strip()
        lines.append(
            TextLine(
                page=words[0].page,
                column=get_plain_column(left),
                column_left=PLAIN_COLUMN_LEFTS[get_plain_column(left)],
                left=left,
                top=top,
                text=text,
                formatted_text=text,
            )
        )
    return sorted(lines, key=lambda line: (line.page, line.column, line.top, line.left))


def is_reference_start(line: TextLine) -> bool:
    return line.left <= line.column_left + 6


def should_skip_line(line: TextLine, current_section: str | None) -> bool:
    text = line.text
    if not text or current_section is None:
        return True
    if line.top < 50:
        return True
    if text == "Contents" or text.startswith("References of Handbook"):
        return True
    if "................................................................" in text:
        return True
    if text in SECTION_HEADERS:
        return True
    return False


def append_reference_line(text: str, line: str) -> str:
    line = line.strip()
    if text.endswith("-"):
        return text[:-1] + line
    return f"{text} {line}"


def strip_italic_tags(text: str) -> str:
    return TAG_RE.sub("", text)


def trim_trailing_markup_hyphen(text: str) -> str:
    return re.sub(r"-(</i>)?$", r"\1", text)


def append_formatted_reference_line(text: str, line: str) -> str:
    line = line.strip()
    if strip_italic_tags(text).endswith("-"):
        return trim_trailing_markup_hyphen(text) + line
    return f"{text} {line}"


def split_formatted_at_plain_index(text: str, plain_index: int) -> tuple[str, str]:
    pieces = re.split(r"(</?i>)", text)
    before: list[str] = []
    after: list[str] = []
    plain_position = 0
    split_done = False
    in_italic = False
    split_in_italic = False
    for piece in pieces:
        if piece == "<i>":
            in_italic = True
            (after if split_done else before).append(piece)
            continue
        if piece == "</i>":
            in_italic = False
            (after if split_done else before).append(piece)
            continue
        if split_done:
            after.append(piece)
            continue
        next_position = plain_position + len(piece)
        if next_position < plain_index:
            before.append(piece)
            plain_position = next_position
            continue
        offset = max(0, plain_index - plain_position)
        before.append(piece[:offset])
        after.append(piece[offset:])
        split_done = True
        split_in_italic = in_italic
        plain_position = next_position
    if not split_done:
        return text, ""
    before_text = "".join(before)
    after_text = "".join(after)
    if split_in_italic:
        before_text += "</i>"
        after_text = "<i>" + after_text
    return before_text, after_text


def append_reference(reference: Reference, line: str, formatted_line: str) -> Reference:
    return Reference(
        reference.section,
        append_reference_line(reference.text, line),
        append_formatted_reference_line(reference.formatted_text, formatted_line),
    )


def split_reference(
    reference: Reference, plain_index: int
) -> tuple[Reference, Reference]:
    first_formatted, second_formatted = split_formatted_at_plain_index(
        reference.formatted_text, plain_index
    )
    return (
        Reference(
            reference.section,
            reference.text[:plain_index].strip(),
            first_formatted.strip(),
        ),
        Reference(
            reference.section,
            reference.text[plain_index:].strip(),
            second_formatted.strip(),
        ),
    )


def canonical_reference_text(text: str) -> str:
    text = strip_italic_tags(text).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def merge_formatted_references(
    plain_references: Iterable[Reference], formatted_references: Iterable[Reference]
) -> list[Reference]:
    formatted_by_text = {
        canonical_reference_text(reference.text): reference.formatted_text
        for reference in formatted_references
    }
    merged = []
    for reference in plain_references:
        formatted_text = formatted_by_text.get(
            canonical_reference_text(reference.text), reference.text
        )
        merged.append(Reference(reference.section, reference.text, formatted_text))
    return merged


def extract_references(lines: Iterable[TextLine]) -> list[Reference]:
    references: list[Reference] = []
    current_section: str | None = None
    current_reference: str | None = None
    current_formatted_reference: str | None = None
    current_reference_section: str | None = None

    for line in lines:
        if line.text in SECTION_HEADERS:
            if current_reference is not None:
                assert current_reference_section is not None
                assert current_formatted_reference is not None
                references.append(
                    Reference(
                        current_reference_section,
                        current_reference,
                        current_formatted_reference,
                    )
                )
                current_reference = None
                current_formatted_reference = None
            current_section = SECTION_HEADERS[line.text]
            continue
        if should_skip_line(line, current_section):
            continue

        if (
            current_reference is not None
            and not has_printed_author_year(current_reference)
            and line.text not in SECTION_HEADERS
        ):
            current_reference = append_reference_line(current_reference, line.text)
            assert current_formatted_reference is not None
            current_formatted_reference = append_formatted_reference_line(
                current_formatted_reference, line.formatted_text
            )
        elif is_reference_start(line) and (
            current_reference is None or has_printed_author_year(line.text)
        ):
            if current_reference is not None:
                assert current_reference_section is not None
                assert current_formatted_reference is not None
                references.append(
                    Reference(
                        current_reference_section,
                        current_reference,
                        current_formatted_reference,
                    )
                )
            current_reference = line.text
            current_formatted_reference = line.formatted_text
            current_reference_section = current_section
        elif (
            is_reference_start(line)
            and current_reference is not None
            and current_reference.rstrip().endswith(".")
        ):
            assert current_reference_section is not None
            assert current_formatted_reference is not None
            references.append(
                Reference(
                    current_reference_section,
                    current_reference,
                    current_formatted_reference,
                )
            )
            current_reference = line.text
            current_formatted_reference = line.formatted_text
            current_reference_section = current_section
        elif current_reference is None:
            current_reference = line.text
            current_formatted_reference = line.formatted_text
            current_reference_section = current_section
        else:
            current_reference = append_reference_line(current_reference, line.text)
            assert current_formatted_reference is not None
            current_formatted_reference = append_formatted_reference_line(
                current_formatted_reference, line.formatted_text
            )

    if current_reference is not None:
        assert current_reference_section is not None
        assert current_formatted_reference is not None
        references.append(
            Reference(
                current_reference_section,
                current_reference,
                current_formatted_reference,
            )
        )
    return references


def write_csv(references: Iterable[Reference], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as f:
        writer = csv.DictWriter(f, ["section", "reference", "formatted_reference"])
        writer.writeheader()
        for reference in references:
            writer.writerow(
                {
                    "section": reference.section,
                    "reference": reference.text,
                    "formatted_reference": reference.formatted_text,
                }
            )


PARSED_FIELDS = refparse.STAGE2_FIELDS


def year_suffix(year: str) -> str:
    match = re.match(r"\d{4}(?:–\d{4})?(?P<suffix>[a-z]*)$", year)
    return match.group("suffix") if match else ""


def split_sentence_tail(text: str) -> tuple[str, str]:
    quoted_separator = text.rfind(".” ")
    if quoted_separator != -1:
        return text[: quoted_separator + 2], text[quoted_separator + 3 :].rstrip(".")
    before, separator, after = text.rpartition(". ")
    if not separator:
        return text.rstrip("."), ""
    return before, after.rstrip(".")


def parse_publisher_place(text: str) -> tuple[str, str]:
    if not text:
        return "", ""
    publisher, separator, place = text.rpartition(", ")
    if not separator:
        return text, ""
    return publisher, place


def clean_url(url: str) -> str:
    return re.sub(r"\s+", "", url).rstrip(".")


def get_language_note(text: str) -> str:
    match = re.search(r"((?:In|With) [A-Z][^.]+\.)$", text)
    return match.group(1) if match else ""


def get_page_count(text: str) -> str:
    match = re.search(r"(\d+ pp\.)$", text)
    return match.group(1) if match else ""


def is_series_text(text: str) -> bool:
    return (
        re.fullmatch(r"(?:\d+(?:st|nd|rd|th)|\d+e) S[ée]r\.", text.strip()) is not None
    )


def split_series(container: str) -> tuple[str, str]:
    match = SERIES_RE.match(container)
    if match is None:
        return container, ""
    return match.group("container"), match.group("series")


def split_in_authors(prefix: str) -> tuple[str, str]:
    if not prefix.startswith("In: "):
        return "", prefix
    inner = prefix.removeprefix("In: ")
    comma_matches = list(re.finditer(r", ", inner))
    for match in reversed(comma_matches):
        container = inner[match.end() :]
        if is_series_text(container):
            continue
        if not looks_like_container_title(container):
            continue
        return inner[: match.start()], container
    if comma_matches:
        match = comma_matches[0]
        return inner[: match.start()], inner[match.end() :]
    return "", prefix


def looks_like_container_title(container: str) -> bool:
    container = container.strip().strip(":")
    if not container:
        return False
    first = container.split()[0].rstrip(",")
    known_one_word = {
        "Mammalia",
        "Naturwissenschaften",
        "Vespertilio",
        "Zootaxa",
        "Beaufortia",
        "Oryx",
        "Therya",
    }
    if first in known_one_word:
        return True
    return "." in container and len(container.split()) <= 12


def split_volume_issue(volume: str, issue: str) -> tuple[str, str]:
    issue = issue.strip()
    match = re.match(r"^(?P<volume>\d+(?:–\d+)?[A-Za-z]?)(?P<issue>\([^)]*\))$", volume)
    if match is None:
        return volume, issue
    volume = match.group("volume")
    embedded_issue = match.group("issue")
    if issue:
        issue = f"{embedded_issue} {issue}"
    else:
        issue = embedded_issue
    return volume, issue


def set_journal_parts(
    row: dict[str, str], container: str, volume: str, issue: str, pages: str
) -> None:
    in_authors, container = split_in_authors(container)
    container = container.rstrip(":")
    container, series = split_series(container)
    volume, issue = split_volume_issue(volume, issue)
    row["in_authors"] = in_authors
    row["container_title"] = container
    row["series"] = series
    row["volume"] = volume
    row["issue"] = issue.strip()
    row["pages"] = pages.rstrip(".")


def looks_like_journal_container(container: str) -> bool:
    return journal_container_score(container) >= 4


def journal_container_score(container: str) -> int:
    tokens = container.split()
    if not tokens:
        return 0
    if len(tokens) >= 2 and tokens[0] == "S." and tokens[1] == "Afr.":
        return 8
    if (
        len(tokens) >= 3
        and tokens[0] == "West."
        and tokens[1] == "N."
        and tokens[2] == "Am."
    ):
        return 8
    first_token = tokens[0].rstrip(",")
    first_base = first_token.rstrip(".")
    likely_first_tokens = {
        "Acta",
        "Afr",
        "Am",
        "Ann",
        "Arch",
        "Aust",
        "Bat",
        "Beaufortia",
        "Bijdr",
        "Biol",
        "Bull",
        "Can",
        "Chiroptera",
        "Compt",
        "Conserv",
        "Fieldiana",
        "Folia",
        "J",
        "Jpn",
        "Mammal",
        "Mammalia",
        "Mem",
        "Misc",
        "Mol",
        "Nature",
        "New",
        "Occ",
        "PLoS",
        "Proc",
        "Rec",
        "Rev",
        "Science",
        "Smithson",
        "Spec",
        "Turk",
        "Vespertilio",
        "Vestn",
        "Z",
        "Zool",
        "Zootaxa",
        "eLife",
        "eZool",
        "North-West",
        "NorthWest",
    }
    likely_last_tokens = {
        "Biol",
        "Bull",
        "Conserv",
        "Ecol",
        "Entomol",
        "Hist",
        "Lett",
        "Mammal",
        "Mammalia",
        "Monit",
        "Nat",
        "Proc",
        "Res",
        "Rev",
        "Sci",
        "Stud",
        "Theriol",
        "Z",
        "Zool",
    }
    allowed_lowercase = {
        "and",
        "de",
        "del",
        "der",
        "des",
        "do",
        "et",
        "for",
        "fur",
        "in",
        "of",
        "the",
        "und",
        "voor",
        "y",
    }
    score = 0
    if first_base in likely_first_tokens:
        score += 4
    abbreviation_count = 0
    lowercase_penalty = 0
    for token in tokens:
        stripped = token.strip(",;:()[]")
        if not stripped:
            continue
        base = stripped.rstrip(".")
        if stripped.endswith(".") or (
            len(base) <= 4 and base and base[0].isupper() and base.isalpha()
        ):
            abbreviation_count += 1
        if stripped[0].islower() and stripped.lower() not in allowed_lowercase:
            lowercase_penalty += 1
    score += min(abbreviation_count, 4)
    if any(token == JOURNAL_ABBREV_TOKEN for token in tokens):
        score += 2
    last_base = tokens[-1].strip(",;:()[]").rstrip(".")
    if last_base in likely_last_tokens:
        score += 3
    if len(tokens) >= 2 and tokens[-2] == JOURNAL_ABBREV_TOKEN:
        score += 1
    score -= lowercase_penalty * 3
    if len(tokens) > 8:
        score -= len(tokens) - 8
    return score


def split_journal_prefix(prefix: str) -> tuple[str, str] | None:
    candidates: list[tuple[str, str]] = []
    for match in re.finditer(r"[.?!][’”'\"]? ", prefix):
        title = prefix[: match.end()].strip().rstrip(".")
        container = prefix[match.end() :].strip()
        first_token = container.split(" ", maxsplit=1)[0].rstrip(".")
        if not title or not container:
            continue
        if not first_token or not first_token[0].isupper():
            continue
        if first_token in {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"}:
            continue
        if len(container.split()) > 12:
            continue
        candidates.append((title, container))
    if candidates:
        ranked = sorted(
            candidates,
            key=lambda item: (
                journal_container_score(item[1]),
                len(item[1].split()),
                -len(item[0]),
            ),
            reverse=True,
        )
        if journal_container_score(ranked[0][1]) >= 4:
            return ranked[0]
        return candidates[0]
    title, separator, container = prefix.rpartition(". ")
    if not separator:
        return None
    return title, container


def get_formatted_body(reference: Reference, body: str) -> str:
    body_start = len(reference.text) - len(body)
    _, formatted_body = split_formatted_at_plain_index(
        reference.formatted_text, body_start
    )
    return formatted_body.strip()


def split_journal_prefix_by_italics(
    prefix: str, formatted_prefix: str
) -> tuple[str, str] | None:
    matches = list(ITALIC_RUN_RE.finditer(formatted_prefix))
    if not matches:
        return None
    journal_start = len(strip_italic_tags(formatted_prefix[: matches[-1].start()]))
    title = prefix[:journal_start].strip().rstrip(".")
    container = prefix[journal_start:].strip()
    if not title or not container:
        return None
    if not container[0].isupper():
        return None
    return title, container


def split_journal_tail(
    body: str, formatted_body: str = ""
) -> tuple[str, str, str, str, str] | None:
    match = JOURNAL_TAIL_RE.match(body)
    has_volume = True
    if match is None:
        match = JOURNAL_ISSUE_ONLY_RE.match(body)
        has_volume = False
    if match is None:
        return None
    prefix = match.group("prefix")
    formatted_prefix = ""
    if formatted_body:
        formatted_prefix, _ = split_formatted_at_plain_index(
            formatted_body, len(prefix)
        )
    split = (
        split_journal_prefix_by_italics(prefix, formatted_prefix)
        if formatted_prefix
        else None
    )
    if split is None:
        split = split_journal_prefix(prefix)
    if split is None:
        return None
    title, container = split
    volume = match.group("volume") if has_volume else ""
    issue = match.group("issue") or ""
    pages = match.group("pages")
    return title, container, volume, issue, pages


def parse_author_year(reference: Reference) -> dict[str, str]:
    row = dict.fromkeys(PARSED_FIELDS, "")
    row["section"] = reference.section
    row["raw_reference"] = reference.text
    row["formatted_reference"] = reference.formatted_text
    match = match_author_year(reference.text)
    if match is None:
        row["reference_type"] = "unparsed"
        row["unparsed"] = reference.text
        return row
    authors = match.group("authors")
    if authors.endswith(" eds."):
        authors = authors.removesuffix(" eds.")
        row["author_role"] = "editors"
    elif authors.endswith(" ed."):
        authors = authors.removesuffix(" ed.")
        row["author_role"] = "editor"
    row["authors"] = authors
    row["year"] = match.group("year")
    row["year_suffix"] = year_suffix(row["year"])
    row["unparsed"] = match.group("body")
    return row


def is_taxon_bracket(body: str, start: int) -> bool:
    before = body[:start].rstrip()
    if before.endswith(":"):
        return True
    context = before.rsplit(":", maxsplit=1)[-1].rsplit(",", maxsplit=1)[-1].strip()
    return (
        re.search(
            r"(?:^|\s)(?:[A-Z]+|\d+[a-z]?|\d+–\d+|[A-Z]\d+|pl\. ?\d+[a-z]?|ftn\.|unno\. pp\.?(?: and pl\.)?)$",
            context,
        )
        is not None
    )


def get_taxon_brackets(body: str) -> list[re.Match[str]]:
    return [
        match
        for match in re.finditer(r"\[([^]]+)\]", body)
        if is_taxon_bracket(body, match.start())
    ]


def remove_taxon_brackets(body: str, matches: Sequence[re.Match[str]]) -> str:
    pieces = []
    last_end = 0
    for match in matches:
        pieces.append(body[last_end : match.start()])
        last_end = match.end()
    pieces.append(body[last_end:])
    return "".join(pieces)


def parse_scientific_description(reference: Reference) -> dict[str, str]:
    row = parse_author_year(reference)
    if row["reference_type"] == "unparsed":
        return row
    body = row.pop("unparsed")
    taxon_matches = get_taxon_brackets(body)
    taxa = [match.group(1) for match in taxon_matches]
    if not taxa and (
        match := re.match(r"^(?P<citation>.*?: \d+) (?P<taxa>[^]]+)\]\.?$", body)
    ):
        body = f"{match.group('citation')} [{match.group('taxa')}]"
        taxon_matches = get_taxon_brackets(body)
        taxa = [match.group("taxa")]
    row["reference_type"] = "scientific_description"
    row["described_taxa"] = " | ".join(taxa)
    citation = remove_taxon_brackets(body, taxon_matches).rstrip(" ,.")
    citation = re.sub(r"\s+,", ",", citation)
    row["citation_detail"] = citation
    journal_match = SCIENTIFIC_CITATION_RE.match(citation)
    if journal_match is not None:
        set_journal_parts(
            row,
            journal_match.group("prefix"),
            journal_match.group("volume"),
            journal_match.group("issue") or "",
            journal_match.group("pages"),
        )
    elif no_volume_match := SCIENTIFIC_NO_VOLUME_RE.match(citation):
        set_journal_parts(
            row, no_volume_match.group("prefix"), "", "", no_volume_match.group("pages")
        )
    else:
        row["container_title"] = citation
    row["unparsed"] = ""
    return row


def parse_web(row: dict[str, str], body: str) -> dict[str, str]:
    title, _, rest = body.partition(". In: ")
    row["reference_type"] = "web"
    row["title"] = title
    url_match = URL_RE.search(body)
    accessed_match = ACCESSED_RE.search(body)
    row["url"] = clean_url(url_match.group("url")) if url_match else ""
    row["accessed"] = accessed_match.group("accessed") if accessed_match else ""
    container = rest
    if " URL:" in container:
        container = container.split(" URL:", maxsplit=1)[0]
    row["container_title"] = container.rstrip(".")
    row["unparsed"] = ""
    return row


def parse_chapter(row: dict[str, str], body: str) -> dict[str, str]:
    match = CHAPTER_RE.match(body)
    if match is None:
        return row
    row["reference_type"] = "book_chapter"
    row["title"] = match.group("title")
    row["pages"] = match.group("pages").removeprefix("Pp. ").removeprefix("Page ")
    book = match.group("book")
    chapter_pages = row["pages"]
    if edited_book_match := EDITED_BOOK_RE.match(book):
        row["editors"] = edited_book_match.group("editors")
        row["book_year"] = edited_book_match.group("book_year")
        book = edited_book_match.group("book")
        row["book_title"], publisher_place = split_sentence_tail(book)
        row["publisher"], row["place"] = parse_publisher_place(publisher_place)
    elif cross_ref_match := CROSS_REF_BOOK_RE.match(book):
        row["book_title"] = cross_ref_match.group("book_ref")
        row["book_year"] = cross_ref_match.group("book_year")
    elif (article_match := AUTHOR_YEAR_RE.match(book)) and (
        journal_tail := split_journal_tail(article_match.group("body"))
    ):
        article_title, container, volume, issue, container_pages = journal_tail
        row["in_authors"] = article_match.group("authors")
        row["book_year"] = article_match.group("year")
        row["book_title"] = article_title
        set_journal_parts(row, container, volume, issue, container_pages)
        row["citation_detail"] = row["pages"]
        row["pages"] = chapter_pages
    else:
        row["book_title"], publisher_place = split_sentence_tail(book)
        row["publisher"], row["place"] = parse_publisher_place(publisher_place)
    if not row["container_title"]:
        row["container_title"] = row["book_title"]
    row["unparsed"] = ""
    return row


def parse_thesis(row: dict[str, str], body: str) -> dict[str, str]:
    match = re.match(
        r"^(?P<title>.+?)\. (?P<kind>[A-Z][A-Za-z]+ (?:thesis|dissertation)), (?P<rest>.*)$",
        body,
    )
    if match is None:
        return row
    row["reference_type"] = "thesis"
    row["title"] = match.group("title")
    row["thesis_type"] = match.group("kind")
    row["institution"], row["place"] = parse_publisher_place(
        match.group("rest").rstrip(".")
    )
    row["unparsed"] = ""
    return row


def parse_journal(
    row: dict[str, str], body: str, formatted_body: str
) -> dict[str, str]:
    journal_tail = split_journal_tail(body, formatted_body)
    if journal_tail is None:
        return row
    title, container, volume, issue, pages = journal_tail
    tail = ""
    for tail_pattern in (r"\d+ pp\.", r"(?:In|With) [A-Z][^.]+\."):
        tail_match = re.search(rf"\. ({tail_pattern})$", pages)
        if tail_match is not None:
            tail = tail_match.group(1)
            pages = pages[: tail_match.start()].rstrip(".")
            break
    row["reference_type"] = "journal_article"
    row["title"] = title
    set_journal_parts(row, container, volume, issue, pages)
    row["page_count"] = get_page_count(tail)
    row["language_note"] = get_language_note(tail)
    row["unparsed"] = "" if row["page_count"] or row["language_note"] else tail
    return row


def parse_book_or_report(row: dict[str, str], body: str) -> dict[str, str]:
    lower_body = body.lower()
    is_report = "unpublished" in lower_body or "report" in lower_body
    row["reference_type"] = "report" if is_report else "book"
    row["page_count"] = get_page_count(body)
    row["language_note"] = get_language_note(body)
    if row["language_note"]:
        body = body.removesuffix(" " + row["language_note"])
    body = re.sub(r"\s+\d+ pp\.$", "", body)
    title, publisher_place = split_sentence_tail(body)
    row["title"] = title
    row["publisher"], row["place"] = parse_publisher_place(publisher_place)
    row["unparsed"] = "" if publisher_place else body
    return row


def parse_general_reference(reference: Reference) -> dict[str, str]:
    row = parse_author_year(reference)
    if row["reference_type"] == "unparsed":
        return row
    body = row["unparsed"]
    formatted_body = get_formatted_body(reference, body)
    if "URL:" in body or ". In: The IUCN Red List" in body:
        return parse_web(row, body)
    if CHAPTER_RE.match(body) is not None:
        return parse_chapter(row, body)
    if re.search(r"\b(?:thesis|dissertation),", body):
        return parse_thesis(row, body)
    row = parse_journal(row, body, formatted_body)
    if row["reference_type"] == "journal_article":
        return row
    return parse_book_or_report(row, body)


def parse_reference(reference: Reference) -> dict[str, str]:
    if reference.section == SCIENTIFIC_DESCRIPTIONS:
        return parse_scientific_description(reference)
    return parse_general_reference(reference)


def write_parsed_csv(references: Iterable[Reference], output: Path) -> None:
    refparse.write_stage2_csv(
        (parse_reference(reference) for reference in references), output
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract the HMW Chiroptera reference lists to raw and parsed CSVs."
    )
    parser.add_argument(
        "pdf",
        nargs="?",
        type=Path,
        default=DEFAULT_PDF,
        help=f"PDF to extract from (default: {DEFAULT_PDF})",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV to write (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--parsed-output",
        type=Path,
        default=DEFAULT_PARSED_OUTPUT,
        help=f"Parsed CSV to write (default: {DEFAULT_PARSED_OUTPUT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    plain_references = normalize_references(
        extract_references(extract_plain_lines(run_pdftotext(args.pdf)))
    )
    formatted_references = normalize_references(
        extract_references(extract_formatted_lines(run_pdftohtml(args.pdf)))
    )
    references = merge_formatted_references(plain_references, formatted_references)
    write_csv(references, args.output)
    write_parsed_csv(references, args.parsed_output)
    print(f"Wrote {len(references)} references to {args.output}")
    print(f"Wrote parsed references to {args.parsed_output}")


if __name__ == "__main__":
    main()
