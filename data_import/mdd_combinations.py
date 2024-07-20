import argparse
import csv
from collections.abc import Iterable

from data_import import lib
from taxonomy.db.constants import Rank
from taxonomy.db.models.name.name import Name
from taxonomy.db.models.taxon.taxon import Taxon

SOURCES = {
    "1.0": lib.Source("MDD_v1_6495species_JMamm.csv", "Mammalia-MDD 1.0"),
    "1.1": lib.Source("MDD_v1.1_6526species.csv", "Mammalia-MDD 1.1"),
}


HIGHER_RANKS = "MajorType,MajorSubtype,Order,Family,Subfamily,Tribe,Genus".split(",")


def _get_rank(rank: str) -> tuple[Rank, str | None]:
    if rank in ("MajorType", "\ufeffMajorType"):
        return Rank.other, "MajorType"
    elif rank == "MajorSubtype":
        return Rank.other, "MajorSubtype"
    else:
        return Rank[rank.lower()], None


def extract_names(source: lib.Source) -> Iterable[lib.CEDict]:
    if source.source == "Mammalia-MDD 1.0":
        id_column = "ID_number"
        author_column = "Authority_author"
        year_column = "Authority_year"
        higher_ranks = [
            "\ufeff" + rank if i == 0 else rank for i, rank in enumerate(HIGHER_RANKS)
        ]
    else:
        id_column = "id"
        author_column = "Authority_sp_author"
        year_column = "Authority_sp_year"
        higher_ranks = HIGHER_RANKS
    seen_names: set[tuple[Rank, str]] = set()
    with (lib.DATA_DIR / source.inputfile).open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            page_no = f"ID #{row[id_column]}"
            name = row["SciName"].replace("_", " ")
            if not name:
                continue
            previous_parent = None
            previous_parent_rank = None
            for rank in higher_ranks:
                value = row[rank].title()
                if value in ("Na", "Incertae", "Sedis") or value.isdigit():
                    value = None
                if value is not None:
                    assert value.isalpha(), row
                    rank_enum, textual_rank = _get_rank(rank)
                    if (rank_enum, value) not in seen_names:
                        yield {
                            "name": value,
                            "page": page_no,
                            "rank": rank_enum,
                            "article": source.get_source(),
                            "textual_rank": textual_rank,
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
                "article": source.get_source(),
                "parent": previous_parent,
                "parent_rank": previous_parent_rank,
                "authority": row[author_column],
                "year": row[year_column],
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
    source = SOURCES[args.version]
    names = extract_names(source)
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
        lib.format_ces(source)


if __name__ == "__main__":
    main()
