from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from taxonomy.db.constants import AgeClass, Rank, Status
from taxonomy.db.models import Article, ClassificationEntry, Name, Taxon
from taxonomy.upsheeter import upsheet

# Keep these article names in sync with taxonomy.db.nomenclature_book.PAST_TREATMENTS.
TREATMENT_TO_ARTICLES = {
    "MSW3": ("Mammalia-review (MSW3)",),
    "HMW": (
        "Rodentia (HMW7)",
        "Placentalia-HMW 8",
        "Chiroptera (HMW)",
        "Marsupialia, Monotremata (HMW)",
        "Primates (HMW)",
        "Glires (HMW)",
        "Mammalia-marine (HMW)",
        "Ungulata (HMW)",
        "Carnivora (HMW)",
    ),
}
TREATMENT_ARTICLE_NAMES = {
    article_name
    for article_names in TREATMENT_TO_ARTICLES.values()
    for article_name in article_names
}
CURRENT_AGES = (AgeClass.extant, AgeClass.recently_extinct)
BATNAMES_PATH = (
    Path(__file__).parent.parent / "notes" / "batnames_species_subspecies.csv"
)
DEFAULT_OUTPUT_PATH = Path("subspecies_classification_summary.csv")
SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1RvgywdUIf5q1hWWXlr-3YwzIHhn9WPJQDqHC2YXP2Xg/edit?gid=0#gid=0"
)
SHEET_GID = 0


@dataclass(frozen=True)
class Summary:
    label: str
    species: int
    species_with_subspecies: int
    subspecies: int


@dataclass(frozen=True)
class OtherTreatment:
    article: Article
    subspecies: tuple[str, ...]
    species: tuple[str, ...]

    def format_summary(self) -> str:
        parts = []
        if self.subspecies:
            parts.append(format_epithets(self.subspecies))
        if self.species:
            parts.append(f"species={format_epithets(self.species)}")
        return f"{'; '.join(parts)} ({format_short_ref(self.article)})"

    def format_reference(self) -> str:
        return (
            f"{format_short_ref(self.article)}: {normalize_space(self.article.cite())}"
        )


@dataclass(frozen=True)
class ClassificationData:
    subspecies: dict[Taxon, set[str]]
    species: dict[Taxon, set[str]]


@dataclass(frozen=True)
class ClassificationForComparison:
    label: str
    subspecies: Iterable[str]
    species: Iterable[str]


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def format_short_ref(article: Article) -> str:
    authors = article.get_authors()
    if len(authors) > 2:
        author_text = f"{authors[0].taxonomic_authority()} et al."
        year = article.get_year_only() or str(article.numeric_year())
    else:
        author_text, year = article.taxonomic_authority()
    return f"{author_text} {year}"


def format_epithets(epithets: Iterable[str]) -> str:
    return "|".join(sorted(set(epithets), key=str.casefold))


def normalize_subspecies_for_comparison(
    species: Taxon, epithets: Iterable[str]
) -> frozenset[str]:
    epithet_set = frozenset(epithets)
    if epithet_set == {species.valid_name.split()[-1]}:
        return frozenset()
    return epithet_set


def format_comparison_value(species: Taxon, epithets: Iterable[str]) -> str:
    normalized = normalize_subspecies_for_comparison(species, epithets)
    return format_epithets(normalized) if normalized else "(none)"


def format_current_value(epithets: Iterable[str]) -> str:
    epithet_set = set(epithets)
    return format_epithets(epithet_set) if epithet_set else "(none)"


def get_comparison_sets(
    species: Taxon, current: Iterable[str], listed_subspecies: Iterable[str]
) -> tuple[frozenset[str], frozenset[str]]:
    species_epithet = species.valid_name.split()[-1]
    current_normalized = normalize_subspecies_for_comparison(species, current)
    subspecies_normalized = normalize_subspecies_for_comparison(
        species, listed_subspecies
    )
    return (
        current_normalized - {species_epithet},
        subspecies_normalized - {species_epithet},
    )


def classification_matches(
    species: Taxon,
    current: Iterable[str],
    listed_subspecies: Iterable[str],
    listed_species: Iterable[str],
) -> bool:
    current_non_nominotypical, subspecies_non_nominotypical = get_comparison_sets(
        species, current, listed_subspecies
    )
    if subspecies_non_nominotypical == current_non_nominotypical:
        return True
    if not subspecies_non_nominotypical <= current_non_nominotypical:
        return False
    return current_non_nominotypical - subspecies_non_nominotypical <= frozenset(
        listed_species
    )


def format_mismatch(
    species: Taxon,
    current: Iterable[str],
    listed_subspecies: Iterable[str],
    listed_species: Iterable[str],
) -> str:
    current_non_nominotypical, subspecies_non_nominotypical = get_comparison_sets(
        species, current, listed_subspecies
    )
    if not current_non_nominotypical:
        return format_comparison_value(species, listed_subspecies)

    added = subspecies_non_nominotypical - current_non_nominotypical
    removed = (
        current_non_nominotypical
        - subspecies_non_nominotypical
        - frozenset(listed_species)
    )
    parts = []
    if added:
        parts.append(f"add {format_epithets(added)}")
    if removed:
        parts.append(f"remove {format_epithets(removed)}")
    return ", ".join(parts) or format_comparison_value(species, listed_subspecies)


def find_unresolved_issue(
    *,
    species: Taxon,
    current: tuple[str, ...],
    classifications: Iterable[ClassificationForComparison],
    other: Iterable[OtherTreatment],
) -> str:
    mismatch_to_labels: dict[str, list[str]] = {}

    all_classifications = list(classifications)
    all_classifications.extend(
        ClassificationForComparison(
            label=format_short_ref(treatment.article),
            subspecies=treatment.subspecies,
            species=treatment.species,
        )
        for treatment in other
    )

    for classification in all_classifications:
        if not classification_matches(
            species, current, classification.subspecies, classification.species
        ):
            mismatch = format_mismatch(
                species, current, classification.subspecies, classification.species
            )
            mismatch_to_labels.setdefault(mismatch, []).append(classification.label)

    if not mismatch_to_labels:
        return ""
    mismatches = (
        f"{', '.join(labels)}={mismatch}"
        for mismatch, labels in mismatch_to_labels.items()
    )
    return f"current={format_current_value(current)}; " + "; ".join(mismatches)


def get_treatment_entries(
    article_names: Iterable[str],
) -> Iterable[ClassificationEntry]:
    for art_name in article_names:
        art = Article.getter("name")(art_name)
        assert art is not None, art_name
        yield from art.get_classification_entries_with_children()


def summarize_treatment(label: str, article_names: Iterable[str]) -> Summary:
    species_entries: list[ClassificationEntry] = []
    subspecies_count = 0
    for ce in get_treatment_entries(article_names):
        if ce.rank is Rank.species:
            species_entries.append(ce)
        elif ce.rank is Rank.subspecies:
            subspecies_count += 1
    species_with_subspecies = sum(
        1 for ce in species_entries if any(ce.get_children_of_rank(Rank.subspecies))
    )
    return Summary(
        label=label,
        species=len(species_entries),
        species_with_subspecies=species_with_subspecies,
        subspecies=subspecies_count,
    )


def get_current_species() -> list[Taxon]:
    mammalia = Taxon.getter("valid_name")("Mammalia")
    assert mammalia is not None
    return sorted(
        (
            taxon
            for taxon in mammalia.children_of_rank(Rank.species)
            if taxon.age in CURRENT_AGES and taxon.base_name.status is Status.valid
        ),
        key=lambda taxon: taxon.valid_name,
    )


def current_subspecies_for_species(species: Taxon) -> tuple[str, ...]:
    return tuple(
        sorted(
            (
                child.base_name.root_name
                for child in species.children_of_rank(Rank.subspecies)
                if child.age in CURRENT_AGES and child.base_name.status is Status.valid
            ),
            key=str.casefold,
        )
    )


def summarize_current() -> Summary:
    species = get_current_species()
    subspecies_counts = [
        len(current_subspecies_for_species(taxon)) for taxon in species
    ]
    return Summary(
        label="Current",
        species=len(species),
        species_with_subspecies=sum(1 for count in subspecies_counts if count),
        subspecies=sum(subspecies_counts),
    )


def display_summary(summary: Summary) -> None:
    print(
        f"{summary.label}: species={summary.species}, "
        f"species_with_subspecies={summary.species_with_subspecies}, "
        f"subspecies={summary.subspecies}"
    )


def mapped_species_from_name(
    nam: Name, current_species: Mapping[str, Taxon]
) -> Taxon | None:
    taxon = nam.taxon
    if taxon.rank is Rank.species:
        species = taxon
    elif taxon.has_parent_of_rank(Rank.species):
        species = taxon.parent_of_rank(Rank.species)
    else:
        return None
    return current_species.get(species.valid_name)


def corrected_name_epithet(ce: ClassificationEntry) -> str | None:
    corrected_name = ce.get_corrected_name()
    if corrected_name is None:
        return None
    parts = corrected_name.split()
    if not parts:
        return None
    return parts[-1]


def current_species_for_classification_entry(
    ce: ClassificationEntry, current_species: Mapping[str, Taxon]
) -> Taxon | None:
    if ce.mapped_name is not None:
        species = mapped_species_from_name(ce.mapped_name, current_species)
        if species is not None:
            return species
    parent_species = ce.parent_of_rank(Rank.species)
    if parent_species is None:
        return None
    if parent_species.mapped_name is not None:
        species = mapped_species_from_name(parent_species.mapped_name, current_species)
        if species is not None:
            return species
    parent_name = parent_species.get_corrected_name()
    if parent_name is not None:
        return current_species.get(parent_name)
    return None


def collect_treatment_data(
    article_names: Iterable[str], current_species: Mapping[str, Taxon]
) -> ClassificationData:
    subspecies: dict[Taxon, set[str]] = defaultdict(set)
    species_names: dict[Taxon, set[str]] = defaultdict(set)
    for ce in get_treatment_entries(article_names):
        if ce.rank not in (Rank.species, Rank.subspecies):
            continue
        species = current_species_for_classification_entry(ce, current_species)
        epithet = corrected_name_epithet(ce)
        if species is None or epithet is None:
            continue
        if ce.rank is Rank.species:
            species_names[species].add(epithet)
        else:
            subspecies[species].add(epithet)
    return ClassificationData(subspecies=subspecies, species=species_names)


def get_batnames_data(current_species: Mapping[str, Taxon]) -> ClassificationData:
    subspecies: dict[Taxon, set[str]] = defaultdict(set)
    species_names: dict[Taxon, set[str]] = defaultdict(set)
    batnames_species_candidates: dict[str, tuple[Taxon, str]] = {}
    for species in current_species.values():
        genus_name = species.parent_of_rank(Rank.genus).valid_name
        for child in species.children_of_rank(Rank.subspecies):
            if child.age in CURRENT_AGES and child.base_name.status is Status.valid:
                epithet = child.base_name.root_name
                batnames_species_candidates[f"{genus_name} {epithet}"] = (
                    species,
                    epithet,
                )

    unmatched_species: list[str] = []
    with BATNAMES_PATH.open() as f:
        for row in csv.DictReader(f):
            maybe_species = current_species.get(row["species_name"])
            if maybe_species is not None:
                species_names[maybe_species].add(maybe_species.valid_name.split()[-1])
                subspecies[maybe_species].update(
                    epithet for epithet in row["subspecies_names"].split("|") if epithet
                )
                continue
            species_and_epithet = batnames_species_candidates.get(row["species_name"])
            if species_and_epithet is not None:
                species, epithet = species_and_epithet
                species_names[species].add(epithet)
            else:
                unmatched_species.append(row["species_name"])
    if unmatched_species:
        print(
            "BatNames species not matched to current taxonomy: "
            f"{len(unmatched_species)}",
            file=sys.stderr,
        )
    return ClassificationData(subspecies=subspecies, species=species_names)


def is_other_classification_article(article: Article, min_year: int) -> bool:
    if article.name in TREATMENT_ARTICLE_NAMES:
        return False
    return article.numeric_year() >= min_year


def collect_other_treatments(
    current_species: Mapping[str, Taxon], min_year: int
) -> dict[Taxon, list[OtherTreatment]]:
    subspecies: dict[tuple[Taxon, Article], set[str]] = defaultdict(set)
    species_names: dict[tuple[Taxon, Article], set[str]] = defaultdict(set)
    for ce in ClassificationEntry.select_valid().filter(
        (ClassificationEntry.rank == Rank.species)
        | (ClassificationEntry.rank == Rank.subspecies)
    ):
        article = ce.article
        if not is_other_classification_article(article, min_year):
            continue
        species = current_species_for_classification_entry(ce, current_species)
        epithet = corrected_name_epithet(ce)
        if species is None or epithet is None:
            continue
        if ce.rank is Rank.species:
            species_names[(species, article)].add(epithet)
        else:
            subspecies[(species, article)].add(epithet)

    by_species: dict[Taxon, list[OtherTreatment]] = defaultdict(list)
    all_keys = subspecies.keys() | species_names.keys()
    for species, article in all_keys:
        species_epithets = species_names[(species, article)]
        current_non_nominotypical = {
            epithet
            for epithet in current_subspecies_for_species(species)
            if epithet != species.valid_name.split()[-1]
        }
        if not subspecies[(species, article)] and not (
            species_epithets & current_non_nominotypical
        ):
            continue
        by_species[species].append(
            OtherTreatment(
                article=article,
                subspecies=tuple(subspecies[(species, article)]),
                species=tuple(species_epithets),
            )
        )
    for treatments in by_species.values():
        treatments.sort(
            key=lambda treatment: (
                treatment.article.numeric_year(),
                treatment.article.concise_citation(),
            )
        )
    return by_species


def build_rows(other_min_year: int) -> list[dict[str, str]]:
    species = get_current_species()
    current_species = {taxon.valid_name: taxon for taxon in species}
    treatment_data = {
        label: collect_treatment_data(article_names, current_species)
        for label, article_names in TREATMENT_TO_ARTICLES.items()
    }
    batnames_data = get_batnames_data(current_species)
    other_treatments = collect_other_treatments(current_species, other_min_year)

    rows = []
    for taxon in species:
        current = current_subspecies_for_species(taxon)
        msw3 = treatment_data["MSW3"].subspecies.get(taxon, set())
        hmw = treatment_data["HMW"].subspecies.get(taxon, set())
        batnames = batnames_data.subspecies.get(taxon, set())
        other = other_treatments.get(taxon, [])
        classifications = [
            ClassificationForComparison(
                label="MSW3",
                subspecies=msw3,
                species=treatment_data["MSW3"].species.get(taxon, set()),
            ),
            ClassificationForComparison(
                label="HMW",
                subspecies=hmw,
                species=treatment_data["HMW"].species.get(taxon, set()),
            ),
        ]
        if taxon.parent_of_rank(Rank.order).valid_name == "Chiroptera":
            classifications.append(
                ClassificationForComparison(
                    label="BatNames",
                    subspecies=batnames,
                    species=batnames_data.species.get(taxon, set()),
                )
            )
        unresolved_issue = find_unresolved_issue(
            species=taxon, current=current, classifications=classifications, other=other
        )
        rows.append(
            {
                "species_name": taxon.valid_name,
                "order": taxon.parent_of_rank(Rank.order).valid_name,
                "family": taxon.parent_of_rank(Rank.family).valid_name,
                "unresolved_issue": unresolved_issue,
                "current_subspecies": format_epithets(current),
                "msw3_subspecies": format_epithets(msw3),
                "hmw_subspecies": format_epithets(hmw),
                "batnames_subspecies": format_epithets(batnames),
                "other_subspecies": "; ".join(
                    treatment.format_summary() for treatment in other
                ),
                "other_refs": "\n".join(
                    treatment.format_reference() for treatment in other
                ),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise ValueError("No rows to write")
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Path for the local CSV output.",
    )
    parser.add_argument(
        "--sheet",
        action="store_true",
        help="Update the Google Sheet instead of writing a local CSV.",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print the aggregate treatment summaries that this script used to show.",
    )
    parser.add_argument(
        "--other-min-year",
        type=int,
        default=2010,
        help="Earliest year for the aggregated 'other' classifications.",
    )
    args = parser.parse_args()

    if args.summary:
        for label in ("MSW3", "HMW"):
            display_summary(summarize_treatment(label, TREATMENT_TO_ARTICLES[label]))
        display_summary(summarize_current())

    rows = build_rows(args.other_min_year)
    if args.sheet:
        upsheet(
            sheet_url=SHEET_URL,
            worksheet_gid=SHEET_GID,
            data=rows,
            matching_column="species_name",
            backup_path_name="subspecies_classification_summary",
        )
    else:
        write_csv(args.output, rows)
        print(f"Wrote {len(rows)} rows to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
