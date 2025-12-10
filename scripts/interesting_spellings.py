import argparse
import csv
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from taxonomy.db.constants import AgeClass, NomenclatureStatus, Rank, Status
from taxonomy.db.models.article.article import Article
from taxonomy.db.models.classification_entry.ce import ClassificationEntry
from taxonomy.db.models.name.name import Name
from taxonomy.db.models.taxon.taxon import Taxon
from taxonomy.upsheeter import upsheet


def get_extant_mammals() -> list[Taxon]:
    species = Taxon.select_valid().filter(
        Taxon.age.is_in((AgeClass.extant, AgeClass.recently_extinct)),
        Taxon.rank == Rank.species,
    )
    return [
        spec
        for spec in species
        if spec.base_name.status is Status.valid
        and spec.get_derived_field("class_").valid_name == "Mammalia"
    ]


def is_major(art: Article) -> bool:
    return "HMW" in art.name or "Red List" in art.name or "MSW" in art.name


@dataclass
class InterestingName:
    taxon: Taxon
    reason: str

    def get_alternative_spellings(self) -> set[str]:
        spellings = set()
        if self.taxon.base_name.nomenclature_status is NomenclatureStatus.as_emended:
            original_epithet = self.taxon.base_name.corrected_original_name.split()[-1]
            spellings.add(original_epithet)
        ces = get_relevant_ces(self.taxon.base_name)
        for ce in ces:
            corrected_name = ce.get_corrected_name()
            if corrected_name:
                epithet = corrected_name.split()[-1]
                spellings.add(epithet)
        if self.taxon.base_name.numeric_year() > 2000:
            spellings.add(self.taxon.base_name.corrected_original_name.split()[-1])
        spellings.discard(self.taxon.base_name.root_name)
        return spellings

    def get_major_classifications(self) -> list[ClassificationEntry]:
        ces = get_relevant_ces(self.taxon.base_name)
        return [ce for ce in ces if is_major(ce.article) and ce.rank is Rank.species]

    def to_csv(self) -> dict[str, str]:
        return {
            "valid_name": self.taxon.valid_name,
            "alternative_spellings": ", ".join(
                sorted(self.get_alternative_spellings())
            ),
            "original_combination": self.taxon.base_name.original_name or "",
            "authority": self.taxon.base_name.taxonomic_authority(),
            "year": str(self.taxon.base_name.numeric_year()),
            "citation": (
                self.taxon.base_name.original_citation.cite()
                if self.taxon.base_name.original_citation
                else ""
            ),
            "reason": self.reason,
            "major_classifications": "; ".join(
                repr(ce) for ce in self.get_major_classifications()
            ),
        }


def get_relevant_ces(nam: Name) -> list[ClassificationEntry]:
    resolved = nam.resolve_variant()
    root_name = nam.root_name
    ces = []
    for possible_variant in Name.select_valid().filter(Name.taxon == nam.taxon):
        if possible_variant.resolve_name() == resolved:
            ces.extend(get_and_filter_ces(possible_variant, root_name))
    return ces


def get_and_filter_ces(nam: Name, root_name: str) -> list[ClassificationEntry]:
    ces = nam.get_classification_entries()

    def is_interesting_name(corrected_name: str | None) -> bool:
        return corrected_name is not None and corrected_name.split()[-1] != root_name

    return [
        ce
        for ce in ces
        if (not ce.rank.is_synonym)
        and ce.article.numeric_year() >= 2000
        and is_interesting_name(ce.get_corrected_name())
    ]


def find_interesting_spellings(taxa: list[Taxon]) -> Iterable[InterestingName]:
    for taxon in taxa:
        if taxon.base_name.nomenclature_status is NomenclatureStatus.as_emended:
            original_epithet = taxon.base_name.corrected_original_name.split()[-1]
            yield InterestingName(taxon, f"as emended from '{original_epithet}'")
            continue
        if (
            taxon.base_name.numeric_year() > 2000
            and taxon.base_name.root_name
            != taxon.base_name.corrected_original_name.split()[-1]
        ):
            yield InterestingName(
                taxon,
                f"changed spelling in original description from '{taxon.base_name.corrected_original_name}' to '{taxon.base_name.root_name}'",
            )
            continue
        ces = get_relevant_ces(taxon.base_name)
        if ces:
            yield InterestingName(
                taxon, f"changed spelling in {len(ces)} classification entries: {ces}"
            )
            continue


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sheet", help="Update a sheet", action="store_true")
    args = parser.parse_args()

    taxa = get_extant_mammals()
    interesting = list(find_interesting_spellings(taxa))
    if not interesting:
        print("No interesting spellings found.", file=sys.stderr)
        return
    if args.sheet:
        upsheet(
            sheet_name="interesting_spellings",
            worksheet_gid=1446599367,
            data=[item.to_csv() for item in interesting],
            matching_column="valid_name",
            backup_path_name="interesting_spellings",
        )

    else:
        with Path("interesting_spellings.csv").open("w") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "valid_name",
                    "alternative_spellings",
                    "original_combination",
                    "authority",
                    "year",
                    "citation",
                    "reason",
                    "major_classifications",
                ],
            )
            writer.writeheader()
            for item in interesting:
                print(
                    f"Found interesting spelling: {item.taxon.valid_name} (or {', '.join(item.get_alternative_spellings())}) ({item.reason})",
                    file=sys.stderr,
                )
                writer.writerow(item.to_csv())


if __name__ == "__main__":
    main()
