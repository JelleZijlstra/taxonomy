import argparse
import csv
import sys
from pathlib import Path

from scripts import mdd_diff
from taxonomy import getinput
from taxonomy.db import export
from taxonomy.db.constants import AgeClass, Group, Rank
from taxonomy.db.models import Taxon

COLUMNS = [
    "Action",
    "Jelle_Comments",
    "Connor_Comments",
    "match_status",
    "spelling_status",
    "author_status",
    "author_suggested",
    "year_status",
    "Hesp_root_name",
    "Hesp_authority",
    "Hesp_year",
    "MDD_root_name",
    "MDD_author",
    "MDD_year",
    "Hesp_species",
    "MDD_species",
    "Hesp_status",
    "Hesp_nomenclature_status",
    "Hesp_id",
    "MDD_syn_ID",
    "Hesp_link",
    "Hesp_age_class",
    "Hesp_class_",
    "Hesp_order",
    "Hesp_family",
    "Hesp_genus",
    "Hesp_subspecies",
    "Hesp_taxon_name",
    "Hesp_taxon_link",
    "Hesp_original_name",
    "Hesp_corrected_original_name",
    "Hesp_original_rank",
    "Hesp_author_links",
    "Hesp_publication_date",
    "Hesp_page_described",
    "Hesp_original_citation",
    "Hesp_original_citation_link",
    "Hesp_verbatim_citation",
    "Hesp_citation_group",
    "Hesp_citation_group_link",
    "Hesp_type_locality",
    "Hesp_type_locality_detail",
    "Hesp_type_specimen",
    "Hesp_species_type_kind",
    "Hesp_collection_link",
    "Hesp_type_specimen_detail",
    "Hesp_species_name_complex",
    "Hesp_species_name_complex_link",
    "Hesp_tags",
    "MDD_order",
    "MDD_family",
    "MDD_genus",
    "MDD_specificEpithet",
    "MDD_subspecificEpithet",
    "MDD_original_combination",
    "MDD_authority_citation",
    "MDD_authority_link",
    "MDD_holotype",
    "MDD_type_locality",
    "MDD_type_country",
    "MDD_type_subregion",
    "MDD_type_latitude",
    "MDD_type_longitude",
    "MDD_validTaxon",
    "MDD_nominalName",
    "MDD_lapsus",
    "MDD_partum",
    "MDD_nameCombination",
    "MDD_currentSpeciesCombination",
    "MDD_currentSubpeciesCombination",
    "MDD_preoccupied",
    "MDD_unavailableName",
    "MDD_nomenNudum",
    "MDD_nomenDubium",
    "MDD_nomenOblitum",
    "MDD_comments",
    "MDD_synonymCitations",
]

EXTRA_COLUMNS = ["Action", "Jelle_Comments", "Connor_Comments"]
EMPTY: dict[str, object] = {}


def run(mdd_csv: Path, match_csv: Path, output_csv: Path, taxon: Taxon) -> None:
    with mdd_csv.open() as f:
        reader = csv.DictReader(f)
        mdd_rows = list(reader)
    with match_csv.open() as f:
        reader = csv.DictReader(f)
        match_rows = list(reader)
    hesp_names = export.get_names_for_export(
        taxon,
        ages={AgeClass.extant, AgeClass.recently_extinct},
        group=Group.species,
        min_rank_for_age_filtering=Rank.species,
    )

    hesp_id_to_mdd_ids: dict[int, set[str]] = {}
    mdd_id_to_extra: dict[str, dict[str, str]] = {}
    hesp_id_to_extra: dict[int, dict[str, str]] = {}
    for row in match_rows:
        mdd_id = row["MDD_syn_ID"]
        if mdd_id:
            for column in EXTRA_COLUMNS:
                if column in row and row[column]:
                    mdd_id_to_extra.setdefault(mdd_id, {})[column] = row[column]
        hesp_id_str = row["Hesp_id"]
        if not hesp_id_str:
            hesp_id = 0
        else:
            hesp_id = int(hesp_id_str)
        if hesp_id:
            for column in EXTRA_COLUMNS:
                if column in row and row[column]:
                    hesp_id_to_extra.setdefault(hesp_id, {})[column] = row[column]
        if not mdd_id:
            continue
        if hesp_id == 0:
            continue
        hesp_id_to_mdd_ids.setdefault(hesp_id, set()).add(mdd_id)
    mdd_id_to_row = {row["MDD_syn_ID"]: row for row in mdd_rows}
    used_mdd_ids = set()

    num_mdd_names = len(mdd_rows)
    num_hesp_names = len(hesp_names)
    num_simple_matches = 0
    num_multi_mdd = 0
    prev_match = 0
    num_hesp_only = 0
    num_mdd_only = 0
    num_author_diffs = 0
    num_spelling_diffs = 0
    num_year_diffs = 0

    with output_csv.open("w") as f:
        writer = csv.DictWriter(f, COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for name in getinput.print_every_n(hesp_names, label="Hesperomys names"):
            data = export.data_for_name(name)
            row = {f"Hesp_{k}": v for k, v in data.items()}
            row.update(hesp_id_to_extra.get(name.id, EMPTY))
            if name.id in hesp_id_to_mdd_ids:
                mdd_ids = sorted(hesp_id_to_mdd_ids[name.id])
                if len(mdd_ids) == 1:
                    num_simple_matches += 1
                else:
                    num_multi_mdd += 1
                    row["match_status"] = (
                        f"matched to {len(mdd_ids)} MDD names: {mdd_ids}"
                    )
                for mdd_id in mdd_ids:
                    single_row = {**row}
                    if mdd_id in used_mdd_ids:
                        single_row["match_status"] = (
                            f"matched MDD id {mdd_id}, which already matched another"
                            " Hesp id"
                        )
                        num_multi_mdd += 1
                        writer.writerow(single_row)
                        continue
                    used_mdd_ids.add(mdd_id)
                    if mdd_id not in mdd_id_to_row:
                        single_row["match_status"] = (
                            f"previously matched MDD id {mdd_id}"
                        )
                        prev_match += 1
                        writer.writerow(single_row)
                        continue
                    mdd_row = mdd_id_to_row[mdd_id]
                    single_row.update(mdd_row)
                    single_row.update(mdd_id_to_extra.get(mdd_id, EMPTY))
                    author_diffs = list(
                        mdd_diff.compare_authors_to_name(
                            name, mdd_id, mdd_row["MDD_author"]
                        )
                    )
                    if author_diffs:
                        single_row["author_status"] = "; ".join(
                            diff.to_markdown(concise=True) for diff in author_diffs
                        )
                        num_author_diffs += 1
                    if name.root_name != mdd_row["MDD_root_name"]:
                        single_row["spelling_status"] = (
                            f"{name.root_name} (H) / {mdd_row['MDD_root_name']} (M)"
                        )
                        num_spelling_diffs += 1
                    if data["year"] != mdd_row["MDD_year"]:
                        single_row["year_status"] = (
                            f"{data['year']} (H) / {mdd_row['MDD_year']} (M)"
                        )
                        num_year_diffs += 1
                    writer.writerow(single_row)
            else:
                num_hesp_only += 1
                row["match_status"] = "no_mdd_match"
                writer.writerow(row)
        for row in getinput.print_every_n(mdd_rows, label="MDD names"):
            if row["MDD_syn_ID"] in used_mdd_ids:
                continue
            num_mdd_only += 1
            out_row = {**row, "match_status": "no_hesp_match"}
            out_row.update(mdd_id_to_extra.get(row["MDD_syn_ID"], EMPTY))
            writer.writerow(out_row)

    print("Report:")
    print(f"{num_mdd_names} MDD names")
    print(f"{num_hesp_names} Hesperomys names")
    print(f"{num_simple_matches} clean matches")
    print(f"{num_multi_mdd} matches that are not one-to-one")
    print(f"{prev_match} matched MDD ids that no longer exist")
    print(f"{num_hesp_only} Hesperomys-only names")
    print(f"{num_mdd_only} MDD-only names")
    print(f"{num_spelling_diffs} spelling differences")
    print(f"{num_author_diffs} author differences")
    print(f"{num_year_diffs} year differences")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mdd_csv", help="CSV file with MDD synonym list")
    parser.add_argument("match_csv", help="CSV file with MDD/Hesp matching")
    parser.add_argument("output_csv", help="Output file to produce")
    parser.add_argument("taxon", default="Mammalia")
    args = parser.parse_args()
    root = Taxon.getter("valid_name")(args.taxon)
    if root is None:
        print("Invalid taxon", args.taxon)
        sys.exit(1)
    run(Path(args.mdd_csv), Path(args.match_csv), Path(args.output_csv), root)


if __name__ == "__main__":
    main()
