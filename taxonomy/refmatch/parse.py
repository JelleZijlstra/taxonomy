import csv
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path

STAGE2_FIELDS = [
    "section",
    "reference_type",
    "authors",
    "author_role",
    "in_authors",
    "year",
    "year_suffix",
    "title",
    "container_title",
    "series",
    "volume",
    "issue",
    "pages",
    "page_count",
    "editors",
    "book_year",
    "book_title",
    "publisher",
    "place",
    "thesis_type",
    "institution",
    "url",
    "accessed",
    "language_note",
    "described_taxa",
    "citation_detail",
    "unparsed",
    "raw_reference",
    "formatted_reference",
]

Stage2Parser = Callable[[str], Mapping[str, str]]


def make_empty_stage2_row() -> dict[str, str]:
    return dict.fromkeys(STAGE2_FIELDS, "")


def normalize_stage2_row(
    parsed: Mapping[str, str], stage1_row: Mapping[str, str] | None = None
) -> dict[str, str]:
    unknown = set(parsed) - set(STAGE2_FIELDS)
    if unknown:
        raise ValueError(f"Unknown Stage 2 field(s): {', '.join(sorted(unknown))}")
    row = make_empty_stage2_row()
    if stage1_row is not None:
        row["section"] = stage1_row.get("section", "")
        row["raw_reference"] = stage1_row.get("reference", "")
        row["formatted_reference"] = stage1_row.get(
            "formatted_reference", row["raw_reference"]
        )
    row.update(parsed)
    return row


def iter_stage2_rows(
    stage1_rows: Iterable[Mapping[str, str]],
    parser: Stage2Parser,
    *,
    reference_field: str = "reference",
) -> Iterable[dict[str, str]]:
    for stage1_row in stage1_rows:
        raw_reference = stage1_row[reference_field]
        yield normalize_stage2_row(parser(raw_reference), stage1_row)


def write_stage2_csv(rows: Iterable[Mapping[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="") as f:
        writer = csv.DictWriter(f, STAGE2_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(normalize_stage2_row(row))


def parse_stage1_csv(
    input_path: Path,
    output_path: Path,
    parser: Stage2Parser,
    *,
    reference_field: str = "reference",
) -> None:
    with input_path.open() as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    write_stage2_csv(
        iter_stage2_rows(rows, parser, reference_field=reference_field), output_path
    )
