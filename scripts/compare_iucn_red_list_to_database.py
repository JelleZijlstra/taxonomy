"""Compare an IUCN Red List taxonomy export with the current database.

Matching first uses exact, case-sensitive equality between IUCN
``scientificName`` and the database ``Taxon.valid_name``. Remaining IUCN names
are then looked up among binomial names recorded in the database ``Name``
table. A Name-table match is accepted only when it resolves to exactly one
current species in the filtered database scope; ambiguous mappings are
reported without choosing a species.

Example for bats only::

    python scripts/compare_iucn_red_list_to_database.py \
        /tmp/iucn2026 \
        --order CHIROPTERA \
        --output /tmp/iucn2026-vs-database-chiroptera.csv

The IUCN input may be a taxonomy CSV or a directory containing taxonomy.csv.
If assessments.csv is available beside the taxonomy file, assessment metadata
is included automatically.
"""

import argparse
import csv
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from taxonomy.db.constants import Rank
from taxonomy.db.models.name.name import Name
from taxonomy.db.models.taxon.taxon import Taxon
from taxonomy.db.nomenclature_book import get_taxa

STATUS_ORDER = {
    "ambiguous_name_table": 0,
    "iucn_only": 1,
    "database_only": 2,
    "name_table_match": 3,
    "exact_match": 4,
}
BINOMIAL_RE = re.compile(r"^[A-Z][A-Za-z-]+ [a-z][a-z-]+$")
REQUIRED_IUCN_COLUMNS = {
    "internalTaxonId",
    "scientificName",
    "orderName",
    "familyName",
    "authority",
    "taxonomicNotes",
}
FIELDNAMES = (
    "match_status",
    "scientific_name",
    "database_valid_name",
    "iucn_internal_taxon_id",
    "iucn_assessment_id",
    "iucn_order",
    "iucn_family",
    "iucn_authority",
    "iucn_redlist_category",
    "iucn_year_published",
    "database_taxon_id",
    "database_name_id",
    "database_order",
    "database_family",
    "database_age",
    "database_authority",
    "database_year",
    "name_table_name_ids",
    "name_table_candidate_count",
    "name_table_candidate_valid_names",
    "comparison_note",
    "iucn_taxonomic_notes_if_unmatched",
    "iucn_source",
    "database_scope",
)

IUCNRow = dict[str, str]
ReportRow = dict[str, str | int]


@dataclass(frozen=True)
class DatabaseSpecies:
    scientific_name: str
    taxon_id: int
    name_id: int
    order: str
    family: str
    age: str
    authority: str
    year: str


@dataclass(frozen=True)
class NameTableTarget:
    species: DatabaseSpecies
    name_ids: tuple[int, ...]


NameTableIndex = Mapping[str, tuple[NameTableTarget, ...]]


@dataclass(frozen=True)
class Comparison:
    iucn_count: int
    database_count: int
    rows: tuple[ReportRow, ...]
    counts: Counter[str]


def resolve_taxonomy_path(path: Path) -> Path:
    if path.is_file():
        return path
    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_dir():
        raise ValueError(f"Not a file or directory: {path}")
    for filename in ("taxonomy.csv", "taxonomy_with_html.csv"):
        candidate = path / filename
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"Could not find taxonomy.csv or taxonomy_with_html.csv in {path}"
    )


def resolve_assessments_path(
    taxonomy_path: Path, explicit_path: Path | None
) -> Path | None:
    if explicit_path is not None:
        if not explicit_path.is_file():
            raise FileNotFoundError(explicit_path)
        return explicit_path
    candidate = taxonomy_path.with_name("assessments.csv")
    return candidate if candidate.is_file() else None


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        return list(reader.fieldnames), list(reader)


def read_iucn_taxonomy(path: Path) -> list[IUCNRow]:
    fieldnames, rows = read_csv(path)
    missing = REQUIRED_IUCN_COLUMNS - set(fieldnames)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Missing columns in {path}: {missing_list}")
    _validate_unique(rows, "internalTaxonId", path)
    _validate_unique(rows, "scientificName", path)
    return rows


def read_assessments(path: Path | None) -> dict[str, dict[str, str]]:
    if path is None:
        return {}
    fieldnames, rows = read_csv(path)
    required = {"internalTaxonId", "assessmentId"}
    missing = required - set(fieldnames)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Missing columns in {path}: {missing_list}")
    _validate_unique(rows, "internalTaxonId", path)
    return {row["internalTaxonId"]: row for row in rows}


def _validate_unique(
    rows: Iterable[dict[str, str]], field: str, path: Path | None = None
) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        value = row[field]
        if not value:
            raise ValueError(f"Empty {field} in {path or 'rows'}")
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        raise ValueError(
            f"Duplicate {field} values in {path or 'rows'}: {duplicate_list}"
        )


def filter_iucn_by_order(
    rows: Iterable[IUCNRow], order_name: str | None
) -> list[IUCNRow]:
    if order_name is None:
        return list(rows)
    normalized = _normalize_order(order_name)
    return [row for row in rows if row["orderName"].strip().casefold() == normalized]


def filter_database_by_order(
    species: Iterable[DatabaseSpecies], order_name: str | None
) -> list[DatabaseSpecies]:
    if order_name is None:
        return list(species)
    normalized = _normalize_order(order_name)
    return [sp for sp in species if sp.order.strip().casefold() == normalized]


def filter_name_table_by_order(
    index: NameTableIndex, order_name: str | None
) -> NameTableIndex:
    if order_name is None:
        return index
    normalized = _normalize_order(order_name)
    return {
        name: filtered
        for name, targets in index.items()
        if (
            filtered := tuple(
                target
                for target in targets
                if target.species.order.strip().casefold() == normalized
            )
        )
    }


def _normalize_order(order_name: str) -> str:
    normalized = order_name.strip().casefold()
    if not normalized:
        raise ValueError("Order filter must not be empty")
    return normalized


def _higher_name(taxon: Taxon, rank_name: str) -> str:
    parent = taxon.get_derived_field(rank_name)
    return parent.valid_name if parent is not None else ""


def _clean_name(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("_", " ").strip())


def _possible_binomials(name: Name) -> set[str]:
    candidates: set[str] = set()
    for candidate in (
        name.corrected_original_name,
        name.original_name,
        name.get_default_valid_name(),
    ):
        if candidate is None:
            continue
        cleaned = _clean_name(candidate)
        if BINOMIAL_RE.fullmatch(cleaned):
            candidates.add(cleaned)
    return candidates


def load_database() -> tuple[list[DatabaseSpecies], NameTableIndex]:
    mammalia = Taxon.getter("valid_name")("Mammalia")
    assert mammalia is not None
    species: list[DatabaseSpecies] = []
    name_targets: defaultdict[str, defaultdict[int, set[int]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for taxon in get_taxa(mammalia):
        if taxon.rank is not Rank.species:
            continue
        base_name = taxon.base_name
        database_species = DatabaseSpecies(
            scientific_name=taxon.valid_name,
            taxon_id=taxon.id,
            name_id=base_name.id,
            order=_higher_name(taxon, "order"),
            family=_higher_name(taxon, "family"),
            age=taxon.age.name,
            authority=base_name.taxonomic_authority(),
            year=str(base_name.numeric_year()),
        )
        species.append(database_species)
        for name in taxon.all_names_lazy():
            for candidate in _possible_binomials(name):
                name_targets[candidate][taxon.id].add(name.id)
    _validate_unique(
        [{"scientificName": species_row.scientific_name} for species_row in species],
        "scientificName",
    )
    species_by_id = {species_row.taxon_id: species_row for species_row in species}
    name_index = {
        name: tuple(
            NameTableTarget(species_by_id[taxon_id], tuple(sorted(name_ids)))
            for taxon_id, name_ids in sorted(targets.items())
        )
        for name, targets in name_targets.items()
    }
    return species, name_index


def compare(
    iucn_rows: Sequence[IUCNRow],
    database_species: Sequence[DatabaseSpecies],
    name_table_index: NameTableIndex,
    assessments: dict[str, dict[str, str]],
    *,
    iucn_source: Path,
    database_scope: str,
) -> Comparison:
    _validate_unique(iucn_rows, "scientificName")
    database_by_name = {sp.scientific_name: sp for sp in database_species}
    if len(database_by_name) != len(database_species):
        raise ValueError("Duplicate scientific names in database species")
    rows: list[ReportRow] = []
    counts: Counter[str] = Counter()
    matched_database_taxon_ids: set[int] = set()
    for iucn in iucn_rows:
        name = iucn["scientificName"]
        species = database_by_name.get(name)
        name_targets: tuple[NameTableTarget, ...] = ()
        name_ids = ""
        if species is not None:
            status = "exact_match"
            note = "Exact case-sensitive match: IUCN scientificName == Taxon.valid_name"
            matched_database_taxon_ids.add(species.taxon_id)
        else:
            name_targets = name_table_index.get(name, ())
            if len(name_targets) == 1:
                status = "name_table_match"
                species = name_targets[0].species
                name_ids = ";".join(
                    str(name_id) for name_id in name_targets[0].name_ids
                )
                note = (
                    "Unique Name-table match: the IUCN scientificName is recorded "
                    "on the matched current species"
                )
                matched_database_taxon_ids.add(species.taxon_id)
            elif len(name_targets) > 1:
                status = "ambiguous_name_table"
                note = (
                    "Ambiguous Name-table match: the IUCN scientificName is recorded "
                    "on multiple current species; no target selected"
                )
            else:
                status = "iucn_only"
                note = "No exact or Name-table match among current database species"
        counts[status] += 1

        assessment = assessments.get(iucn["internalTaxonId"], {})
        rows.append(
            _make_report_row(
                status=status,
                name=name,
                iucn=iucn,
                species=species,
                assessment=assessment,
                name_targets=name_targets,
                name_ids=name_ids,
                note=note,
                iucn_source=iucn_source,
                database_scope=database_scope,
            )
        )

    for species in database_species:
        if species.taxon_id in matched_database_taxon_ids:
            continue
        status = "database_only"
        counts[status] += 1
        rows.append(
            _make_report_row(
                status=status,
                name=species.scientific_name,
                iucn=None,
                species=species,
                assessment={},
                name_targets=(),
                name_ids="",
                note="No exact or unique Name-table match among the filtered IUCN scientific names",
                iucn_source=iucn_source,
                database_scope=database_scope,
            )
        )

    rows.sort(
        key=lambda row: (
            STATUS_ORDER[str(row["match_status"])],
            str(row["scientific_name"]).casefold(),
        )
    )
    return Comparison(
        iucn_count=len(iucn_rows),
        database_count=len(database_species),
        rows=tuple(rows),
        counts=counts,
    )


def _make_report_row(
    *,
    status: str,
    name: str,
    iucn: IUCNRow | None,
    species: DatabaseSpecies | None,
    assessment: dict[str, str],
    name_targets: tuple[NameTableTarget, ...],
    name_ids: str,
    note: str,
    iucn_source: Path,
    database_scope: str,
) -> ReportRow:
    candidate_names = ";".join(
        f"{target.species.scientific_name} [taxon_id={target.species.taxon_id}]"
        for target in name_targets
    )
    return {
        "match_status": status,
        "scientific_name": name,
        "database_valid_name": species.scientific_name if species is not None else "",
        "iucn_internal_taxon_id": iucn["internalTaxonId"] if iucn is not None else "",
        "iucn_assessment_id": assessment.get("assessmentId", ""),
        "iucn_order": iucn["orderName"] if iucn is not None else "",
        "iucn_family": iucn["familyName"] if iucn is not None else "",
        "iucn_authority": iucn["authority"] if iucn is not None else "",
        "iucn_redlist_category": assessment.get("redlistCategory", ""),
        "iucn_year_published": assessment.get("yearPublished", ""),
        "database_taxon_id": species.taxon_id if species is not None else "",
        "database_name_id": species.name_id if species is not None else "",
        "database_order": species.order if species is not None else "",
        "database_family": species.family if species is not None else "",
        "database_age": species.age if species is not None else "",
        "database_authority": species.authority if species is not None else "",
        "database_year": species.year if species is not None else "",
        "name_table_name_ids": name_ids,
        "name_table_candidate_count": len(name_targets),
        "name_table_candidate_valid_names": candidate_names,
        "comparison_note": note,
        "iucn_taxonomic_notes_if_unmatched": (
            iucn["taxonomicNotes"]
            if status in {"iucn_only", "ambiguous_name_table"} and iucn is not None
            else ""
        ),
        "iucn_source": str(iucn_source) if iucn is not None else "",
        "database_scope": database_scope if species is not None or name_targets else "",
    }


def write_report(path: Path, rows: Iterable[ReportRow]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    row_list = list(rows)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(row_list)
    return len(row_list)


def print_summary(comparison: Comparison, output: Path) -> None:
    print(f"IUCN species: {comparison.iucn_count:,}")
    print(f"Database species: {comparison.database_count:,}")
    print(f"Exact name matches: {comparison.counts['exact_match']:,}")
    print(f"Unique Name-table matches: {comparison.counts['name_table_match']:,}")
    print(
        f"Ambiguous Name-table mappings: {comparison.counts['ambiguous_name_table']:,}"
    )
    print(f"IUCN only: {comparison.counts['iucn_only']:,}")
    print(f"Database only: {comparison.counts['database_only']:,}")
    print(f"Wrote {len(comparison.rows):,} rows to {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "iucn",
        type=Path,
        help="IUCN taxonomy CSV, or directory containing taxonomy.csv",
    )
    parser.add_argument(
        "-o", "--output", required=True, type=Path, help="Output CSV path"
    )
    parser.add_argument(
        "--assessments",
        type=Path,
        help="Optional assessments CSV (auto-detected beside taxonomy.csv)",
    )
    parser.add_argument(
        "--order",
        help=(
            "Restrict both IUCN and database species to one order; matching is case-insensitive (for example: CHIROPTERA)"
        ),
    )
    args = parser.parse_args()

    taxonomy_path = resolve_taxonomy_path(args.iucn)
    assessments_path = resolve_assessments_path(taxonomy_path, args.assessments)
    iucn_rows = filter_iucn_by_order(read_iucn_taxonomy(taxonomy_path), args.order)
    all_database_species, all_name_table_index = load_database()
    database_species = filter_database_by_order(all_database_species, args.order)
    name_table_index = filter_name_table_by_order(all_name_table_index, args.order)
    scope = "Mammalia; valid species; extant or recently_extinct"
    if args.order is not None:
        scope += f"; order={args.order.strip().upper()}"
        print(f"Order filter: {args.order.strip().upper()}")
    comparison = compare(
        iucn_rows,
        database_species,
        name_table_index,
        read_assessments(assessments_path),
        iucn_source=taxonomy_path,
        database_scope=scope,
    )
    write_report(args.output, comparison.rows)
    print_summary(comparison, args.output)


if __name__ == "__main__":
    main()
