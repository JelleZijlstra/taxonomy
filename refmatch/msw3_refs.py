import argparse
import csv
import importlib
import re
import shutil
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

lib = importlib.import_module("data_import.lib")
hmw_refs = importlib.import_module("refmatch.chiroptera_hmw_refs")
refparse = importlib.import_module("taxonomy.refmatch.parse")

SECTION = "MSW3 publications"
DEFAULT_DOC = Path(
    "/Users/jelle/Dropbox/c/Mammalia/Inter-group/High taxonomy/MSW3/"
    "Mammalia-MSW3 references.doc"
)
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
DEFAULT_OUTPUT = OUTPUT_DIR / "msw3-refs.csv"
DEFAULT_PARSED_OUTPUT = OUTPUT_DIR / "msw3-refs-parsed.csv"

YEAR_RE = r"\d{4}(?:[-–]\d{2,4})?[a-z]?"
MONTH_RE = (
    r"(?:January|February|March|April|May|June|July|August|September|October|"
    r"November|December)"
)
PRINTED_YEAR_RE = rf"{YEAR_RE}(?: +\[[0-9?]{{4}}\])?"
AUTHOR_YEAR_RE = re.compile(
    rf"^(?P<authors>.+?)\. +(?:{MONTH_RE} +)?(?P<year>{YEAR_RE})"
    r"(?: +\[[0-9?]{4}\])?\. +"
    r"(?P<body>.*)$"
)
CHAPTER_RE = re.compile(
    r"^(?P<title>.+?)\. +Pp\. +(?P<pages>[^,.;]+(?:[-–][^,.;]+)?),? +in:? +(?P<book>.*)$",
    re.IGNORECASE,
)
JOURNAL_TAIL_RE = re.compile(
    r"^(?P<prefix>.+?)[, ]+"
    r"(?P<volume>\d+(?:[-–]\d+)?[A-Za-z]?)"
    r"(?P<issue>(?:\s*\([^)]*\)|\(\d+[-/]\d+\))*)"
    r":\s*(?P<pages>.+)$"
)
URL_RE = re.compile(r"(?P<url>https?://\S+|www\.\S+)")
TRAILING_NOTE_RE = re.compile(r"\s*(?P<note>\[[^]]+\]|\([^)]+\))\s*$")


@dataclass(frozen=True)
class Reference:
    section: str
    text: str
    formatted_text: str


PARSED_FIELDS = refparse.STAGE2_FIELDS


def run_textutil(doc: Path) -> str:
    if shutil.which("textutil") is None:
        raise RuntimeError("textutil is required to extract text from the MSW3 .doc")
    result = subprocess.run(
        ["textutil", "-convert", "txt", "-stdout", str(doc)],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return result.stdout


def normalize_reference_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"\bSEQ +CHAPTER +\\h +\\r +\d+", " ", text)
    text = re.sub(r'\bHYPERLINK +"[^"]*"\s*(?:\\[a-z] +"[^"]*"\s*)?', "", text)
    text = re.sub(r'\bHYPERLINK +""(?P<url>https?://[^"]+)""', r"\g<url>", text)
    text = re.sub(rf"\.({MONTH_RE} +{YEAR_RE}\.)", r". \1", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_reference_start(text: str) -> bool:
    match = AUTHOR_YEAR_RE.match(text)
    return match is not None and likely_reference_line_start_authors(
        match.group("authors")
    )


def split_reference(reference: Reference, split_at: int) -> tuple[Reference, Reference]:
    return (
        Reference(
            reference.section,
            reference.text[:split_at].rstrip(),
            reference.formatted_text[:split_at].rstrip(),
        ),
        Reference(
            reference.section,
            reference.text[split_at:].lstrip(),
            reference.formatted_text[split_at:].lstrip(),
        ),
    )


def likely_embedded_reference_authors(authors: str) -> bool:
    if not authors or authors[0].islower():
        return False
    if any(marker in authors for marker in ("http", "://", " Pp. ", " pp.")):
        return False
    if len(authors) > 220:
        return False
    without_initials = re.sub(r"\([^)]*\.\)", "", authors)
    without_initials = re.sub(
        r"\b(?:[A-ZÁÉÍÓÚÄÖÜØ][A-Za-z]?|McT|Ph|St|v)\.", "", without_initials
    )
    without_initials = re.sub(r"\bJr\.", "", without_initials)
    if "." in without_initials:
        return False
    return True


def starts_with_initials(authors: str) -> bool:
    if authors.startswith("St. "):
        return False
    return re.match(r"^(?:[A-ZÁÉÍÓÚÄÖÜØ][A-Za-z]?\.\s*)+,?", authors) is not None


def surname_words_are_plausible(authors: str) -> bool:
    surname = authors.split(",", maxsplit=1)[0]
    for word in re.findall(r"[^\W\d_]+(?:-[^\W\d_]+)*", surname):
        if word in hmw_refs.LOWERCASE_AUTHOR_PREFIXES or word in {
            "de",
            "del",
            "van",
            "von",
        }:
            continue
        if word[0].islower():
            return False
    return True


def likely_reference_line_start_authors(authors: str) -> bool:
    if not authors:
        return False
    if authors[0].islower() and not authors.startswith(
        hmw_refs.LOWERCASE_AUTHOR_PREFIXES
    ):
        return False
    if any(marker in authors for marker in ("http", "://", " Pp. ", " pp.")):
        return False
    if len(authors) > 300 or starts_with_initials(authors):
        return False
    if "," in authors and not surname_words_are_plausible(authors):
        return False
    return True


def likely_embedded_reference_start_authors(authors: str) -> bool:
    if not likely_embedded_reference_authors(authors):
        return False
    if starts_with_initials(authors):
        return False
    if not re.search(r",| and |&", authors):
        return False
    return surname_words_are_plausible(authors)


def prefix_ends_with_initial(prefix: str) -> bool:
    if re.search(r"\b[^\W\d_]\.-[^\W\d_]\.$", prefix):
        return True
    match = re.search(r"\b(?P<token>[^\W\d_]+)\.$", prefix)
    return bool(
        match
        and len(match.group("token")) <= 3
        and match.group("token").lower() not in {"pls", "pp"}
    )


def find_embedded_reference_start(text: str) -> int | None:
    for match in re.finditer(r" (?=[A-ZÁÉÍÓÚÄÖÜØ])", text):
        start = match.start() + 1
        prefix = text[:start].rstrip()
        if not prefix.endswith("."):
            continue
        if prefix_ends_with_initial(prefix):
            continue
        author_year_match = AUTHOR_YEAR_RE.match(text[start:])
        if author_year_match is None:
            continue
        if likely_embedded_reference_start_authors(author_year_match.group("authors")):
            return start
    return None


def split_embedded_references(reference: Reference) -> list[Reference]:
    references = [reference]
    index = 0
    while index < len(references):
        embedded_start = find_embedded_reference_start(references[index].text)
        if embedded_start is None:
            index += 1
            continue
        before, after = split_reference(references[index], embedded_start)
        references[index : index + 1] = [before, after]
    return references


def extract_references(text: str) -> list[Reference]:
    references: list[Reference] = []
    current: str | None = None
    saw_header = False
    for line in text.splitlines():
        line = normalize_reference_text(line)
        if not line:
            continue
        if not saw_header:
            saw_header = line == SECTION
            continue
        if is_reference_start(line) and (
            current is None or is_reference_start(current)
        ):
            if current is not None:
                references.append(Reference(SECTION, current, current))
            current = line
        elif current is not None:
            current = hmw_refs.append_reference_line(current, line)
        else:
            current = line
    if current is not None:
        references.append(Reference(SECTION, current, current))
    return [
        split_reference
        for reference in references
        for split_reference in split_embedded_references(reference)
    ]


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


def empty_parsed_row(reference: Reference) -> dict[str, str]:
    row = dict.fromkeys(PARSED_FIELDS, "")
    row["section"] = reference.section
    row["raw_reference"] = reference.text
    row["formatted_reference"] = reference.formatted_text
    return row


def parse_author_year(reference: Reference) -> dict[str, str]:
    row = empty_parsed_row(reference)
    match = AUTHOR_YEAR_RE.match(reference.text)
    if match is None:
        row["reference_type"] = "unparsed"
        row["unparsed"] = reference.text
        return row
    row["authors"] = match.group("authors")
    row["year"] = match.group("year")
    row["year_suffix"] = hmw_refs.year_suffix(row["year"])
    row["unparsed"] = match.group("body")
    return row


def clean_url(url: str) -> str:
    return hmw_refs.clean_url(url.strip(" <>[]()"))


def pop_trailing_note(text: str) -> tuple[str, str]:
    notes = []
    while match := TRAILING_NOTE_RE.search(text):
        note = match.group("note").strip()
        if not re.search(
            r"\b(?:in|with|translated|chinese|english|russian|japanese|abstract|summary|reprint)\b",
            note,
            re.IGNORECASE,
        ):
            break
        notes.append(note)
        text = text[: match.start()].rstrip(" .")
    return text, " ".join(reversed(notes))


def set_journal_parts(
    row: dict[str, str], container: str, volume: str, issue: str, pages: str
) -> None:
    hmw_refs.set_journal_parts(row, container.rstrip(" ,"), volume, issue, pages)


def split_journal_prefix(prefix: str) -> tuple[str, str] | None:
    split = hmw_refs.split_journal_prefix(prefix)
    if split is not None:
        return split
    if prefix.startswith("[") and "] " in prefix:
        title, container = prefix.split("] ", maxsplit=1)
        return title + "]", container
    if ". " in prefix:
        left, right = prefix.rsplit(". ", maxsplit=1)
        return left, right
    return "", prefix


def parse_journal(row: dict[str, str], body: str) -> dict[str, str]:
    body, note = pop_trailing_note(body.rstrip("."))
    match = JOURNAL_TAIL_RE.match(body)
    if match is None:
        return row
    split = split_journal_prefix(match.group("prefix").rstrip(" ,"))
    if split is None:
        return row
    title, container = split
    volume = match.group("volume")
    issue = match.group("issue") or ""
    pages = match.group("pages")
    if not re.search(r"\d", pages):
        return row
    row["reference_type"] = "journal_article"
    row["title"] = title
    set_journal_parts(row, container, volume, issue, pages)
    row["language_note"] = note
    row["unparsed"] = ""
    return row


def parse_chapter(row: dict[str, str], body: str) -> dict[str, str]:
    body, note = pop_trailing_note(body.rstrip("."))
    match = CHAPTER_RE.match(body)
    if match is None:
        return row
    row["reference_type"] = "book_chapter"
    row["title"] = match.group("title")
    row["pages"] = match.group("pages").strip().rstrip(".")
    book = match.group("book").strip()
    row["language_note"] = note

    editors_match = re.search(r"\((?P<editors>[^()]+,\s+eds?\.?)\)", book)
    if editors_match is None:
        editors_match = re.search(r"\((?P<editors>[^()]+,\s+ed\.?)\)", book)
    if editors_match is not None:
        row["editors"] = editors_match.group("editors").rstrip(".")
        row["book_title"] = book[: editors_match.start()].strip().rstrip(".,")
        publisher_place = book[editors_match.end() :].strip().lstrip(". ")
    else:
        row["book_title"], publisher_place = hmw_refs.split_sentence_tail(book)

    row["publisher"], row["place"] = hmw_refs.parse_publisher_place(publisher_place)
    row["container_title"] = row["book_title"]
    row["unparsed"] = ""
    return row


def parse_thesis(row: dict[str, str], body: str) -> dict[str, str]:
    parsed = hmw_refs.parse_thesis(row, body)
    if parsed["reference_type"] == "thesis":
        return parsed
    match = re.match(
        r"^(?P<title>.+?)\. (?P<kind>[^.]*thesis|[^.]*dissertation)\. (?P<rest>.*)$",
        body,
        re.IGNORECASE,
    )
    if match is None:
        return row
    row["reference_type"] = "thesis"
    row["title"] = match.group("title")
    row["thesis_type"] = match.group("kind")
    row["institution"], row["place"] = hmw_refs.parse_publisher_place(
        match.group("rest").rstrip(".")
    )
    row["unparsed"] = ""
    return row


def parse_web(row: dict[str, str], body: str) -> dict[str, str]:
    match = URL_RE.search(body)
    if match is None:
        return row
    row["reference_type"] = "web"
    row["url"] = clean_url(match.group("url"))
    before = body[: match.start()].strip().rstrip(".")
    row["title"] = before
    row["unparsed"] = body[match.end() :].strip()
    return row


def parse_book_or_report(row: dict[str, str], body: str) -> dict[str, str]:
    body, note = pop_trailing_note(body.rstrip("."))
    row["reference_type"] = (
        "report"
        if re.search(
            r"\b(?:report|unpublished|proceedings|occasional paper)\b",
            body,
            re.IGNORECASE,
        )
        else "book"
    )
    row["language_note"] = note
    row["page_count"] = hmw_refs.get_page_count(body)
    body = re.sub(
        r"\s+(?:\d+\s*(?:pp\.|pages)|[ivxlcdm]+ ?\+ ?\d+ pp\.)$",
        "",
        body,
        flags=re.IGNORECASE,
    )
    title, publisher_place = hmw_refs.split_sentence_tail(body)
    row["title"] = title
    row["publisher"], row["place"] = hmw_refs.parse_publisher_place(publisher_place)
    row["unparsed"] = "" if publisher_place or row["page_count"] else body
    return row


def parse_reference(reference: Reference) -> dict[str, str]:
    row = parse_author_year(reference)
    if row["reference_type"] == "unparsed":
        return row
    body = row["unparsed"]
    for parser in (parse_web, parse_chapter, parse_thesis, parse_journal):
        parsed = parser(row, body)
        if parsed["reference_type"]:
            return parsed
    return parse_book_or_report(row, body)


def write_parsed_csv(references: Iterable[Reference], output: Path) -> None:
    refparse.write_stage2_csv(
        (parse_reference(reference) for reference in references), output
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract the MSW3 reference list to raw and parsed CSVs."
    )
    parser.add_argument(
        "doc",
        nargs="?",
        type=Path,
        default=DEFAULT_DOC,
        help=f"Word .doc to extract from (default: {DEFAULT_DOC})",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Raw Stage 1 CSV to write (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--parsed-output",
        type=Path,
        default=DEFAULT_PARSED_OUTPUT,
        help=f"Parsed Stage 2 CSV to write (default: {DEFAULT_PARSED_OUTPUT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    references = extract_references(run_textutil(args.doc))
    write_csv(references, args.output)
    write_parsed_csv(references, args.parsed_output)
    print(f"Wrote {len(references)} references to {args.output}")
    print(f"Wrote parsed references to {args.parsed_output}")


if __name__ == "__main__":
    main()
