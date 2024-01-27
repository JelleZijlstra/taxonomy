import argparse
import csv
import functools
import sys
from pathlib import Path

import Levenshtein

from scripts import mdd_diff
from taxonomy import getinput
from taxonomy.db import export
from taxonomy.db.constants import AgeClass, Group, Rank, Status
from taxonomy.db.models import Name, Taxon

COLUMNS = [
    "MDD_syn_ID",
    "Hesp_id",
    "Jelle_TODO",
    "Connor_TODO",
    "Jelle_Comments",
    "Connor_Comments",
    "MDD_change",
    "match_status",
    "species_status",
    "spelling_status",
    "nomenclature_status",
    "author_status",
    "author_exact_status",
    "year_status",
    "Hesp_root_name",
    "Hesp_authority",
    "Hesp_year",
    "Hesp_nomenclature_status",
    "MDD_root_name",
    "MDD_author",
    "MDD_year",
    "MDD_nomenclature_status",
    "Hesp_original_citation",
    "Hesp_species",
    "MDD_species",
    "Hesp_status",
    "MDD_validity",
    "Hesp_class_",
    "Hesp_order",
    "Hesp_family",
    "Hesp_genus",
    "Hesp_subspecies",
    "Hesp_taxon_name",
    "Hesp_original_name",
    "Hesp_corrected_original_name",
    "Hesp_page_described",
    "Hesp_verbatim_citation",
    "Hesp_citation_group",
    "Hesp_type_locality",
    "Hesp_type_specimen",
    "Hesp_species_type_kind",
    "MDD_order",
    "MDD_family",
    "MDD_genus",
    "MDD_specificEpithet",
    "MDD_subspecificEpithet",
    "MDD_original_combination",
    "MDD_authority_citation",
    "MDD_authority_page",
    "MDD_authority_link",
    "MDD_holotype",
    "MDD_type_locality",
    "MDD_type_latitude",
    "MDD_type_longitude",
    "MDD_type_country",
    "MDD_type_subregion",
    "MDD_comments",
]

EXTRA_COLUMNS = [
    "Action",
    "Change",
    "Jelle_Comments",
    "Connor_Comments",
    "Jelle_TODO",
    "Connor_TODO",
]
EMPTY: dict[str, object] = {}


@functools.cache
def resolve_hesp_id(hesp_id_str: str) -> int | None:
    if hesp_id_str:
        hesp_id = int(hesp_id_str)
        row = Name.select().filter(Name.id == hesp_id).first()
        if row is None:
            return None
        while row.target is not None:
            row = row.target
        return row.id
    return None


def root_name_matches(name: Name, mdd_root_name: str) -> bool:
    mdd_root_name = mdd_root_name.replace("-", "")
    if name.root_name == mdd_root_name:
        return True
    if name.species_name_complex is not None:
        try:
            forms = list(name.species_name_complex.get_forms(name.root_name))
        except ValueError:
            pass
        else:
            return mdd_root_name in forms
    return False


def get_mdd_like_species_name(name: Name) -> str:
    if name.taxon.base_name.status is not Status.valid:
        try:
            genus = name.taxon.parent_of_rank(Rank.genus).valid_name
        except ValueError:
            return "incertae_sedis incertae_sedis"
        else:
            return f"{genus} incertae_sedis"
    try:
        return name.taxon.parent_of_rank(Rank.species).valid_name
    except ValueError:
        return ""


def run(
    mdd_csv: Path,
    match_csv: Path,
    output_csv: Path,
    match_override_csv: Path | None,
    taxon: Taxon,
) -> None:
    with mdd_csv.open() as f:
        reader = csv.DictReader(f)
        mdd_rows = list(reader)
    with match_csv.open() as f:
        reader = csv.DictReader(f)
        match_rows = list(reader)

    hesp_id_to_mdd_ids: dict[int, set[str]] = {}
    mdd_id_to_extra: dict[str, dict[str, str]] = {}
    hesp_id_to_extra: dict[int, dict[str, str]] = {}
    hesp_ids_with_overrides: set[int] = set()
    hesp_ids_with_null_overrides: set[int] = set()
    mdd_ids_with_overrides: set[str] = set()
    mdd_ids_with_null_overrides: set[str] = set()

    if match_override_csv is not None:
        with match_override_csv.open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                hesp_id = resolve_hesp_id(row["Hesp_id"])
                mdd_id = row["MDD_syn_ID"]
                if hesp_id:
                    hesp_ids_with_overrides.add(hesp_id)
                    for column in EXTRA_COLUMNS:
                        if value := row.get(column):
                            hesp_id_to_extra.setdefault(hesp_id, {})[column] = value
                    if not mdd_id:
                        hesp_ids_with_null_overrides.add(hesp_id)

                if mdd_id:
                    mdd_ids_with_overrides.add(mdd_id)
                    for column in EXTRA_COLUMNS:
                        if value := row.get(column):
                            mdd_id_to_extra.setdefault(mdd_id, {})[column] = value
                    if not hesp_id:
                        mdd_ids_with_null_overrides.add(mdd_id)

                if hesp_id and mdd_id:
                    hesp_id_to_mdd_ids.setdefault(hesp_id, set()).add(mdd_id)

    for row in mdd_rows:
        hesp_id = resolve_hesp_id(row["Hesp_id"])
        if hesp_id is None:
            continue
        if (
            row["MDD_syn_ID"] in mdd_ids_with_overrides
            or hesp_id in hesp_ids_with_overrides
        ):
            continue
        hesp_id_to_mdd_ids.setdefault(hesp_id, set()).add(row["MDD_syn_ID"])

    mdd_id_to_row = {row["MDD_syn_ID"]: row for row in mdd_rows}

    for row in match_rows:
        mdd_id = row["MDD_syn_ID"]
        if mdd_id:
            mdd_id_to_row[mdd_id].update(row)
            del mdd_id_to_row[mdd_id]["match_status"]
            for column in EXTRA_COLUMNS:
                if value := row.get(column):
                    mdd_id_to_extra.setdefault(mdd_id, {})[column] = value
        if hesp_id := resolve_hesp_id(row["Hesp_id"]):
            for column in EXTRA_COLUMNS:
                if value := row.get(column):
                    hesp_id_to_extra.setdefault(hesp_id, {})[column] = value

    used_mdd_ids = set()

    hesp_names = export.get_names_for_export(
        taxon,
        ages={AgeClass.extant, AgeClass.recently_extinct},
        group=Group.species,
        min_rank_for_age_filtering=Rank.species,
    )
    need_initials = mdd_diff.get_need_initials_authors(hesp_names)

    num_mdd_names = len(mdd_rows)
    num_hesp_names = len(hesp_names)
    num_simple_matches = 0
    num_multi_mdd = 0
    prev_match = 0
    num_hesp_only = 0
    num_mdd_only = 0
    num_species_diffs = 0
    num_author_diffs = 0
    num_exact_author_diffs = 0
    num_spelling_diffs = 0
    num_year_diffs = 0
    num_nomenclature_diffs = 0

    with output_csv.open("w") as f:
        writer = csv.DictWriter(f, COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for name in getinput.print_every_n(hesp_names, label="Hesperomys names"):
            data = export.data_for_name(name)
            row = {f"Hesp_{k}": v for k, v in data.items()}
            mdd_style_author = mdd_diff.get_mdd_style_authority(name, need_initials)
            row["Hesp_authority"] = mdd_style_author
            hesp_extra = hesp_id_to_extra.get(name.id, EMPTY)
            if name.id in hesp_id_to_mdd_ids:
                mdd_ids = sorted(hesp_id_to_mdd_ids[name.id])
                if len(mdd_ids) == 1:
                    num_simple_matches += 1
                else:
                    num_multi_mdd += 1
                    row["match_status"] = (
                        f"matched to {len(mdd_ids)} MDD names: {mdd_ids}"
                    )
                    print("=== match that is not 1-to-1: ", name, mdd_ids, "===")
                    for mdd_id in mdd_ids:
                        print(mdd_id, mdd_id_to_row[mdd_id])
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
                    # prefer Hesp data over mdd_row, which contains data from the matched sheet
                    single_row = {**mdd_row, **single_row, **hesp_extra}
                    extra = mdd_id_to_extra.get(mdd_id, EMPTY)
                    if extra.get("Change"):
                        single_row["Connor_Comments"] = extra["Change"]
                    single_row.update(extra)
                    mdd_species = (
                        f"{mdd_row['MDD_genus']} {mdd_row['MDD_specificEpithet']}"
                    )
                    hesp_species = get_mdd_like_species_name(name)
                    if mdd_species != hesp_species:
                        distance = Levenshtein.distance(hesp_species, mdd_species)
                        single_row["species_status"] = (
                            f"{distance}: {hesp_species} (H) / {mdd_species} (M)"
                        )
                        num_species_diffs += 1
                    else:
                        single_row["species_status"] = ""
                    if name.nomenclature_status.name != mdd_row[
                        "MDD_nomenclature_status"
                    ].replace("spelling_error", "incorrect_subsequent_spelling"):
                        single_row["nomenclature_status"] = (
                            f"{name.nomenclature_status.name} (H) /"
                            f" {mdd_row['MDD_nomenclature_status']} (M)"
                        )
                        num_nomenclature_diffs += 1
                    else:
                        single_row["nomenclature_status"] = ""
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
                    else:
                        single_row["author_status"] = ""
                    if mdd_style_author != mdd_row["MDD_author"]:
                        distance = Levenshtein.distance(
                            mdd_style_author, mdd_row["MDD_author"]
                        )
                        single_row["author_exact_status"] = (
                            f"{distance}: {mdd_style_author} (H) /"
                            f" {mdd_row['MDD_author']} (M)"
                        )
                        num_exact_author_diffs += 1
                    else:
                        single_row["author_exact_status"] = ""
                    if not root_name_matches(name, mdd_row["MDD_root_name"]):
                        distance = Levenshtein.distance(
                            name.root_name, mdd_row["MDD_root_name"]
                        )
                        single_row["spelling_status"] = (
                            f"{distance}: {name.root_name} (H) /"
                            f" {mdd_row['MDD_root_name']} (M)"
                        )
                        num_spelling_diffs += 1
                    else:
                        single_row["spelling_status"] = ""
                    if data["year"] != mdd_row["MDD_year"]:
                        try:
                            distance = abs(int(data["year"]) - int(mdd_row["MDD_year"]))
                        except ValueError:
                            distance = 1000
                        single_row["year_status"] = (
                            f"{distance}: {data['year']} (H) /"
                            f" {mdd_row['MDD_year']} (M)"
                        )
                        num_year_diffs += 1
                    else:
                        single_row["year_status"] = ""
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
            extra = mdd_id_to_extra.get(row["MDD_syn_ID"], EMPTY)
            if not extra.get("Jelle_Comments"):
                print("MDD-only name:", row)
            if extra.get("Change"):
                out_row["Connor_Comments"] = extra["Change"]
            out_row.update(extra)
            writer.writerow(out_row)

    print("Report:")
    print(f"{num_mdd_names} MDD names")
    print(f"{num_hesp_names} Hesperomys names")
    print(f"{num_simple_matches} clean matches")
    print(f"{num_multi_mdd} matches that are not one-to-one")
    print(f"{prev_match} matched MDD ids that no longer exist")
    print(f"{num_hesp_only} Hesperomys-only names")
    print(f"{num_mdd_only} MDD-only names")
    print(f"{num_species_diffs} species differences")
    print(f"{num_nomenclature_diffs} nomenclature status differences")
    print(f"{num_spelling_diffs} spelling differences")
    print(f"{num_author_diffs} author differences")
    print(f"{num_exact_author_diffs} exact author differences")
    print(f"{num_year_diffs} year differences")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mdd_csv", help="CSV file with MDD synonym list")
    parser.add_argument("match_csv", help="CSV file with MDD/Hesp matching")
    parser.add_argument("output_csv", help="Output file to produce")
    parser.add_argument(
        "match_override_csv",
        nargs="?",
        help="CSV file with MDD/Hesp matching",
        default=None,
    )
    parser.add_argument("--taxon", nargs="?", default="Mammalia")
    args = parser.parse_args()
    root = Taxon.getter("valid_name")(args.taxon)
    if root is None:
        print("Invalid taxon", args.taxon)
        sys.exit(1)
    run(
        Path(args.mdd_csv),
        Path(args.match_csv),
        Path(args.output_csv),
        Path(args.match_override_csv) if args.match_override_csv else None,
        root,
    )


if __name__ == "__main__":
    main()
