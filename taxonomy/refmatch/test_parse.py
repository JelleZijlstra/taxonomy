import csv
from pathlib import Path

import pytest

from taxonomy.refmatch import parse


def test_parse_stage1_csv_fills_defaults(tmp_path: Path) -> None:
    input_path = tmp_path / "stage1.csv"
    output_path = tmp_path / "stage2.csv"
    with input_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, ["section", "reference", "formatted_reference"])
        writer.writeheader()
        writer.writerow(
            {
                "section": "References",
                "reference": "Smith, J. 2001. A title.",
                "formatted_reference": "<i>Smith</i>, J. 2001. A title.",
            }
        )

    def parser(raw_reference: str) -> dict[str, str]:
        assert raw_reference == "Smith, J. 2001. A title."
        return {"reference_type": "book", "authors": "Smith, J.", "year": "2001"}

    parse.parse_stage1_csv(input_path, output_path, parser)

    with output_path.open() as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    assert row["section"] == "References"
    assert row["reference_type"] == "book"
    assert row["authors"] == "Smith, J."
    assert row["raw_reference"] == "Smith, J. 2001. A title."
    assert row["formatted_reference"] == "<i>Smith</i>, J. 2001. A title."
    assert set(row) == set(parse.STAGE2_FIELDS)


def test_parse_stage1_csv_rejects_unknown_stage2_fields(tmp_path: Path) -> None:
    input_path = tmp_path / "stage1.csv"
    output_path = tmp_path / "stage2.csv"
    input_path.write_text("reference\nSmith, J. 2001. A title.\n")

    with pytest.raises(ValueError, match="not_a_field"):
        parse.parse_stage1_csv(
            input_path, output_path, lambda _raw: {"not_a_field": "value"}
        )
