import argparse
import csv
import importlib
import shutil
import subprocess
import sys
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

lib = importlib.import_module("data_import.lib")


SCIENTIFIC_DESCRIPTIONS = "Scientific Descriptions"
GENERAL_LIST = "General List"
SECTION_HEADERS = {
    "References of Scientific Descriptions": SCIENTIFIC_DESCRIPTIONS,
    "General List of References": GENERAL_LIST,
}

DEFAULT_PDF = lib.DATA_DIR / "chiroptera-hmw-refs.pdf"
DEFAULT_OUTPUT = lib.DATA_DIR / "chiroptera-hmw-refs.csv"

# The reference list is printed in three columns. These are the approximate
# left edges of the text columns in PDF points.
COLUMN_LEFTS = (36.0, 214.0, 392.0)


@dataclass(frozen=True)
class Word:
    page: int
    line_key: tuple[int, int, int, int]
    left: float
    top: float
    width: float
    text: str


@dataclass(frozen=True)
class TextLine:
    page: int
    column: int
    left: float
    top: float
    text: str


@dataclass(frozen=True)
class Reference:
    section: str
    text: str


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


def get_column(left: float) -> int:
    return min(
        range(len(COLUMN_LEFTS)), key=lambda index: abs(left - COLUMN_LEFTS[index])
    )


def extract_lines(tsv_text: str) -> list[TextLine]:
    words_by_line: dict[tuple[int, int, int, int], list[Word]] = defaultdict(list)
    for word in iter_words(tsv_text):
        words_by_line[word.line_key].append(word)

    lines = []
    for words in words_by_line.values():
        left = min(word.left for word in words)
        top = min(word.top for word in words)
        lines.append(
            TextLine(
                page=words[0].page,
                column=get_column(left),
                left=left,
                top=top,
                text=join_words(words).strip(),
            )
        )
    return sorted(lines, key=lambda line: (line.page, line.column, line.top, line.left))


def is_reference_start(line: TextLine) -> bool:
    return line.left <= COLUMN_LEFTS[line.column] + 6


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


def extract_references(lines: Iterable[TextLine]) -> list[Reference]:
    references: list[Reference] = []
    current_section: str | None = None
    current_reference: str | None = None
    current_reference_section: str | None = None

    for line in lines:
        if line.text in SECTION_HEADERS:
            if current_reference is not None:
                assert current_reference_section is not None
                references.append(
                    Reference(current_reference_section, current_reference)
                )
                current_reference = None
            current_section = SECTION_HEADERS[line.text]
            continue
        if should_skip_line(line, current_section):
            continue

        if is_reference_start(line):
            if current_reference is not None:
                assert current_reference_section is not None
                references.append(
                    Reference(current_reference_section, current_reference)
                )
            current_reference = line.text
            current_reference_section = current_section
        elif current_reference is None:
            current_reference = line.text
            current_reference_section = current_section
        else:
            current_reference = append_reference_line(current_reference, line.text)

    if current_reference is not None:
        assert current_reference_section is not None
        references.append(Reference(current_reference_section, current_reference))
    return references


def write_csv(references: Iterable[Reference], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as f:
        writer = csv.DictWriter(f, ["section", "reference"])
        writer.writeheader()
        for reference in references:
            writer.writerow({"section": reference.section, "reference": reference.text})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract the raw HMW Chiroptera reference lists to CSV."
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    references = extract_references(extract_lines(run_pdftotext(args.pdf)))
    write_csv(references, args.output)
    print(f"Wrote {len(references)} references to {args.output}")


if __name__ == "__main__":
    main()
