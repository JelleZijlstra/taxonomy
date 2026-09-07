"""Export a database-only literature most-wanted list for a taxon.

A Name is included exactly when it has no original-citation Article, no
AuthorityPageLink, and no URL on its StructuredVerbatimCitation. The script does not
perform availability searches or consult side data.
"""

from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

from taxonomy.db.models import Name, Taxon
from taxonomy.db.models.name import TypeTag

CSV_FIELDS = (
    "requested_taxon",
    "name_id",
    "name_url",
    "current_taxon",
    "original_name",
    "corrected_original_name",
    "authority",
    "year",
    "page_described",
    "citation_group",
    "structured_series",
    "structured_volume",
    "structured_issue",
    "structured_start_page",
    "structured_end_page",
    "verbatim_citation",
    "motivation",
)


@dataclass(frozen=True)
class Candidate:
    requested_taxon: str
    name_id: int
    name_url: str
    current_taxon: str
    original_name: str
    corrected_original_name: str
    authority: str
    year: str
    numeric_year: int | None
    page_described: str
    citation_group: str
    structured_series: str
    structured_volume: str
    structured_issue: str
    structured_start_page: str
    structured_end_page: str
    verbatim_citation: str
    sort_author: str

    @property
    def display_name(self) -> str:
        return (
            self.corrected_original_name or self.original_name or f"Name {self.name_id}"
        )

    @property
    def motivation(self) -> str:
        return f"Original citation of {self.display_name}."

    def csv_row(self) -> dict[str, str | int]:
        return {
            field: getattr(self, field) for field in CSV_FIELDS if field != "motivation"
        } | {"motivation": self.motivation}


def normalized_sort_text(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text.casefold())
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def is_candidate(name: Name) -> bool:
    if name.original_citation is not None:
        return False
    if any(name.get_tags(name.type_tags, TypeTag.AuthorityPageLink)):
        return False
    structured = name.get_type_tag(TypeTag.StructuredVerbatimCitation)
    return structured is None or not structured.url


def candidate_from_name(name: Name, requested_taxon: str) -> Candidate:
    structured = name.get_type_tag(TypeTag.StructuredVerbatimCitation)
    citation_group = name.get_citation_group()
    authors = name.get_authors()
    current_taxon = name.taxon.valid_name if name.taxon is not None else ""
    return Candidate(
        requested_taxon=requested_taxon,
        name_id=name.id,
        name_url=name.get_absolute_url(),
        current_taxon=current_taxon,
        original_name=name.original_name or "",
        corrected_original_name=name.corrected_original_name or "",
        authority=name.taxonomic_authority(),
        year=name.year or "",
        numeric_year=name.valid_numeric_year(),
        page_described=name.page_described or "",
        citation_group=str(citation_group or ""),
        structured_series=(structured.series or "") if structured else "",
        structured_volume=(structured.volume or "") if structured else "",
        structured_issue=(structured.issue or "") if structured else "",
        structured_start_page=(structured.start_page or "") if structured else "",
        structured_end_page=(structured.end_page or "") if structured else "",
        verbatim_citation=name.verbatim_citation or "",
        sort_author=(
            authors[0].get_transliterated_family_name()
            if authors
            else name.taxonomic_authority()
        ),
    )


def candidate_sort_key(candidate: Candidate) -> tuple[object, ...]:
    return (
        normalized_sort_text(candidate.sort_author),
        candidate.numeric_year if candidate.numeric_year is not None else 9999,
        normalized_sort_text(candidate.verbatim_citation),
        normalized_sort_text(candidate.display_name),
        candidate.name_id,
    )


def get_candidates(taxon: Taxon) -> list[Candidate]:
    return sorted(
        (
            candidate_from_name(name, taxon.valid_name)
            for name in taxon.all_names_lazy()
            if is_candidate(name)
        ),
        key=candidate_sort_key,
    )


def write_csv(path: Path, candidates: Iterable[Candidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(candidate.csv_row() for candidate in candidates)


def join_names(names: Sequence[str]) -> str:
    rendered = [f"*{name}*" for name in names]
    if len(rendered) == 1:
        return rendered[0]
    if len(rendered) == 2:
        return " and ".join(rendered)
    return ", ".join(rendered[:-1]) + f", and {rendered[-1]}"


def bibliography_entries(
    candidates: Iterable[Candidate],
) -> list[tuple[tuple[object, ...], str, list[str]]]:
    groups: dict[str, list[Candidate]] = defaultdict(list)
    for candidate in candidates:
        key = candidate.verbatim_citation or f"name:{candidate.name_id}"
        groups[key].append(candidate)
    entries = []
    for members in groups.values():
        members.sort(key=candidate_sort_key)
        citation = members[0].verbatim_citation.strip() or "[Citation not recorded]"
        if citation[-1] not in ".!?…":
            citation += "."
        names = list(dict.fromkeys(member.display_name for member in members))
        entries.append((candidate_sort_key(members[0]), citation, names))
    return sorted(entries, key=lambda entry: entry[0])


def write_bibliography(
    path: Path, taxon: Taxon, candidates: Sequence[Candidate]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Literature most wanted: {taxon.valid_name}",
        "",
        (
            "Names listed here have no original-citation Article, AuthorityPageLink, "
            "or URL in StructuredVerbatimCitation."
        ),
        "",
    ]
    for index, (_, citation, names) in enumerate(
        bibliography_entries(candidates), start=1
    ):
        lines.append(f"{index}. {citation} (Original citation of {join_names(names)}.)")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def filename_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", normalized_sort_text(value)).strip("_")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("taxon", help="Exact valid Taxon name, for example Chiroptera")
    parser.add_argument(
        "--output-dir", type=Path, default=Path("recs/reports"), help="Output directory"
    )
    parser.add_argument("--csv", type=Path, help="Override the CSV output path")
    parser.add_argument(
        "--bibliography", type=Path, help="Override the Markdown bibliography path"
    )
    args = parser.parse_args()

    taxon = Taxon.getter("valid_name")(args.taxon)
    if taxon is None:
        raise SystemExit(f"Could not find valid Taxon {args.taxon!r}")
    stem = f"{filename_slug(taxon.valid_name)}_literature_most_wanted"
    csv_path = args.csv or args.output_dir / f"{stem}.csv"
    bibliography_path = args.bibliography or args.output_dir / f"{stem}.md"

    candidates = get_candidates(taxon)
    write_csv(csv_path, candidates)
    write_bibliography(bibliography_path, taxon, candidates)
    print(
        f"Wrote {len(candidates)} Names and "
        f"{len(bibliography_entries(candidates))} bibliography entries."
    )
    print(f"CSV: {csv_path}")
    print(f"Bibliography: {bibliography_path}")


if __name__ == "__main__":
    main()
