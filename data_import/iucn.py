import argparse
import csv
import json
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank
from taxonomy.db.models.name.name import Name
from taxonomy.db.models.taxon.taxon import Taxon

SOURCES = {
    "2024-1": (
        lib.Source(
            "redlist_species_data_dc7cc691-eef5-4f49-9553-e5e0705d199c/taxonomy.csv",
            "Mammalia-Red List 2024-1.csv",
        ),
        lib.Source(
            "redlist_species_data_dc7cc691-eef5-4f49-9553-e5e0705d199c/assessments.csv",
            "Mammalia-Red List 2024-1.csv",
        ),
    )
}


HIGHER_RANKS = "kingdomName,phylumName,className,orderName,familyName,genusName".split(
    ","
)


def _get_rank(rank: str) -> Rank:
    return Rank[rank.lower().removesuffix("name").replace("class", "class_")]


def extract_names(t_source: lib.Source, a_source: lib.Source) -> Iterable[lib.CEDict]:
    seen_names: set[tuple[Rank, str]] = set()
    art = a_source.get_source()

    taxon_id_to_assessment_row = {}
    with (lib.DATA_DIR / a_source.inputfile).open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            assert row["internalTaxonId"] not in taxon_id_to_assessment_row, row
            taxon_id_to_assessment_row[row["internalTaxonId"]] = row

    with (lib.DATA_DIR / t_source.inputfile).open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            assessment = taxon_id_to_assessment_row[row["internalTaxonId"]]
            page_no = f"https://www.iucnredlist.org/species/{row['internalTaxonId']}/{assessment['assessmentId']}"
            name = row["scientificName"]
            if not name:
                continue
            previous_parent = None
            previous_parent_rank = None
            for rank_column in HIGHER_RANKS:
                value = row[rank_column].title()
                assert value.isalpha(), row
                rank_enum = _get_rank(rank_column)
                if (rank_enum, value) not in seen_names:
                    yield {
                        "name": value,
                        "page": page_no,
                        "rank": rank_enum,
                        "article": art,
                        "parent": previous_parent,
                        "parent_rank": previous_parent_rank,
                    }
                    seen_names.add((rank_enum, value))
                previous_parent = value
                previous_parent_rank = rank_enum

            yield {
                "name": name,
                "page": page_no,
                "rank": Rank.species,
                "article": art,
                "parent": previous_parent,
                "parent_rank": previous_parent_rank,
                "authority": row["authority"],
                "raw_data": json.dumps(
                    {"taxnomy": row, "assessment": assessment}, separators=(",", ":")
                ),
            }


def combination_exists(sci_name: str) -> bool:
    for _ in Name.select_valid().filter(Name.corrected_original_name == sci_name):
        return True
    return False


def taxon_exists(sci_name: str) -> bool:
    return bool(Taxon.select_valid().filter(Taxon.valid_name == sci_name).count())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("version")
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--missing-combinations", action="store_true", default=False)
    args = parser.parse_args()
    t_source, a_source = SOURCES[args.version]
    names = extract_names(t_source, a_source)
    if args.missing_combinations:
        for name in names:
            if (
                name["rank"] is Rank.species
                and not taxon_exists(name["name"])
                and not combination_exists(name["name"])
            ):
                print(name["name"])
    else:
        names = lib.validate_ce_parents(names)
        names = lib.add_classification_entries(names, dry_run=args.dry_run)
        lib.print_ce_summary(names)
        lib.format_ces(a_source)


if __name__ == "__main__":
    main()
