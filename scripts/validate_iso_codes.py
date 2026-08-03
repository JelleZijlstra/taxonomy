"""
Validate COUNTRIES codes in scripts.mdd_taxa against an ISO 3166 CSV.

Usage:
  /Users/jelle/py/venvs/taxonomy314/bin/python scripts/validate_iso_codes.py [iso_csv_path]

The CSV is expected to have at least columns: name, alpha-2
"""

import csv
import sys
from pathlib import Path

from scripts.mdd_taxa import COUNTRIES


def load_iso(csv_path: Path) -> tuple[dict[str, str], dict[str, str]]:
    name_to_code: dict[str, str] = {}
    code_to_name: dict[str, str] = {}
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("name") or "").strip()
            code = (row.get("alpha-2") or "").strip().upper()
            if not name or len(code) != 2:
                continue
            name_to_code[name] = code
            code_to_name[code] = name
    return name_to_code, code_to_name


def main() -> None:
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("iso3166.csv")
    if not csv_path.exists():
        print(f"ISO CSV not found at {csv_path}", file=sys.stderr)
        sys.exit(1)
    name_to_code, code_to_name = load_iso(csv_path)

    unknown_codes = 0
    code_mismatches = 0
    name_differences = 0

    for name, info in sorted(COUNTRIES.items(), key=lambda kv: kv[0].casefold()):
        code = info.code
        if not code:
            continue  # Only validate entries with a declared code

        if code not in code_to_name:
            print(f"UNKNOWN CODE: {name}: code={code} not present in ISO CSV")
            unknown_codes += 1
            continue

        # If the country name is present in ISO CSV, its code must match
        if name in name_to_code and name_to_code[name] != code:
            print(f"CODE MISMATCH: {name}: ours={code} ISO={name_to_code[name]}")
            code_mismatches += 1

        # Also note when our name differs from ISO canonical name for this code
        iso_name = code_to_name[code]
        if iso_name != name:
            print(f"NAME DIFF: code={code}: ours='{name}' ISO='{iso_name}'")
            name_differences += 1

    print(
        f"\nSummary: unknown_codes={unknown_codes}, code_mismatches={code_mismatches}, name_differences={name_differences}"
    )


if __name__ == "__main__":
    main()
