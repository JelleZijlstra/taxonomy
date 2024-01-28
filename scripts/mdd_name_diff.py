import argparse
import csv
import functools
import re
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
    "match_status",
    "spelling_status",
    "author_status",
    "author_exact_status",
    "year_status",
    "nomenclature_status",
    "species_status",
    "validity_status",
    "original_name_status",
    "citation_status",
    "type_locality_status",
    "type_specimen_status",
    # core data
    "Hesp_root_name",
    "Hesp_authority",
    "Hesp_year",
    "MDD_root_name",
    "MDD_author",
    "MDD_year",
    # nomenclature status
    "Hesp_nomenclature_status",
    "MDD_nomenclature_status",
    # species
    "Hesp_species",
    "MDD_species",
    # status
    "Hesp_status",
    "MDD_validity",
    # original name
    "Hesp_original_name",
    "Hesp_corrected_original_name",
    "MDD_original_combination",
    # citation
    "Hesp_original_citation",
    "Hesp_page_described",
    "Hesp_verbatim_citation",
    "Hesp_citation_group",
    "MDD_authority_citation",
    "MDD_authority_page",
    "MDD_authority_link",
    # type locality
    "Hesp_type_locality",
    "MDD_type_locality",
    "MDD_type_latitude",
    "MDD_type_longitude",
    "MDD_type_country",
    "MDD_type_subregion",
    # type specimen
    "Hesp_type_specimen",
    "Hesp_species_type_kind",
    "MDD_holotype",
    # taxonomic context
    "Hesp_order",
    "Hesp_family",
    "Hesp_genus",
    "Hesp_subspecies",
    "Hesp_taxon_name",
    "MDD_order",
    "MDD_family",
    "MDD_genus",
    "MDD_specificEpithet",
    "MDD_subspecificEpithet",
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


def get_mdd_status(name: Name) -> str:
    match name.status:
        case Status.valid:
            match name.taxon.rank:
                case Rank.species:
                    return "species"
                case Rank.subspecies:
                    if name.taxon.parent.base_name == name:
                        return "species"
                    return "synonym"
                case _:
                    return f"unexpected ({name.taxon.rank})"
        case Status.synonym:
            match name.taxon.base_name.status:
                case Status.valid:
                    return "synonym"
                case _:
                    return name.taxon.base_name.status.name
        case _:
            return name.status.name


def get_original_name_from_mdd(row: dict[str, str]) -> str | None:
    if original_combination := row["MDD_original_combination"]:
        if "_" not in original_combination:
            return None  # probably just the root name
        return original_combination.replace("_", " ")
    return None


def get_mdd_style_type_specimen(name: Name) -> str | None:
    spec = name.type_specimen
    if spec is None:
        return None
    spec = re.sub(r"([A-Z]+):[A-Za-z]+:", r"\1 ", spec)
    spec = re.sub(r"([A-Z]+) [A-Za-z]+ ", r"\1 ", spec)
    spec = re.sub(r"([A-Z]+)\.[A-Za-z]+\.", r"\1 ", spec)
    spec = re.sub(r"AMNH [A-Z]+\-", "AMNH ", spec)
    spec = spec.replace("MNHN-ZM-MO-", "MNHN ")
    spec = spec.replace("BMNH ", "BM ")
    return spec


def normalize_original_name(name: str) -> str:
    return (
        name.lower()
        .replace(" [sic]", "")
        .replace(".", "")
        .replace("[", "")
        .replace("]", "")
    )


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
    num_validity_diffs = 0
    num_original_name_diffs = 0
    num_original_name_diffs_normalized = 0
    original_name_missing_hesp = 0
    original_name_missing_mdd = 0
    citation_missing_hesp = 0
    citation_missing_mdd = 0
    tl_missing_hesp = 0
    tl_missing_hesp_not_required = 0
    tl_missing_mdd = 0
    num_type_specimen_diffs = 0
    type_specimen_missing_hesp = 0
    type_specimen_missing_mdd = 0

    with output_csv.open("w") as f:
        writer = csv.DictWriter(f, COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for name in getinput.print_every_n(hesp_names, label="Hesperomys names"):
            data = export.data_for_name(name)
            row = {f"Hesp_{k}": v for k, v in data.items()}
            mdd_style_author = mdd_diff.get_mdd_style_authority(name, need_initials)
            row["Hesp_authority"] = mdd_style_author
            if (
                name.status is Status.synonym
                and name.taxon.base_name.status is not Status.valid
            ):
                row["Hesp_status"] = name.taxon.base_name.status.name
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

                    # species_status
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

                    # nomenclature_status
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

                    # author_status, author_exact_status
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

                    # spelling_status
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

                    # year_status
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

                    # validity_status
                    expected_mdd_status = get_mdd_status(name)
                    if expected_mdd_status != mdd_row["MDD_validity"]:
                        single_row["validity_status"] = (
                            f"{expected_mdd_status} (H) / {mdd_row['MDD_validity']} (M)"
                        )
                        num_validity_diffs += 1
                    else:
                        single_row["validity_status"] = ""

                    # original_name_status
                    mdd_orig = get_original_name_from_mdd(mdd_row)
                    hesp_orig = name.original_name
                    if hesp_orig is not None and mdd_orig is not None:
                        if hesp_orig != mdd_orig:
                            normalized_matches = normalize_original_name(
                                hesp_orig
                            ) == normalize_original_name(mdd_orig)
                            prefix = (
                                "(normalized name matches) "
                                if normalized_matches
                                else ""
                            )
                            diff = f"{prefix}{hesp_orig} (H) / {mdd_orig} (M)"
                            single_row["original_name_status"] = diff
                            if normalized_matches:
                                num_original_name_diffs += 1
                            else:
                                print(f"{name}: {diff}")
                                num_original_name_diffs_normalized += 1
                        else:
                            single_row["original_name_status"] = ""
                    elif hesp_orig is not None:
                        single_row["original_name_status"] = "H only"
                        original_name_missing_mdd += 1
                    elif mdd_orig is not None:
                        single_row["original_name_status"] = "M only"
                        original_name_missing_hesp += 1
                    else:
                        single_row["original_name_status"] = ""

                    # citation_status
                    hesp_has_cite = (
                        name.original_citation is not None
                        or name.verbatim_citation is not None
                    )
                    mdd_has_cite = bool(mdd_row["MDD_authority_citation"])
                    if hesp_has_cite != mdd_has_cite:
                        if hesp_has_cite:
                            single_row["citation_status"] = "H only"
                            citation_missing_mdd += 1
                        else:
                            single_row["citation_status"] = "M only"
                            citation_missing_hesp += 1
                    else:
                        single_row["citation_status"] = ""

                    # type_locality_status
                    hesp_has_tl = name.type_locality is not None
                    mdd_has_tl = bool(mdd_row["MDD_type_locality"])
                    if hesp_has_tl != mdd_has_tl:
                        if hesp_has_tl:
                            single_row["type_locality_status"] = "H only"
                            tl_missing_mdd += 1
                        else:
                            if "type_locality" in name.get_required_fields():
                                single_row["type_locality_status"] = "M only"
                                tl_missing_hesp += 1
                            else:
                                single_row["type_locality_status"] = (
                                    "M only (not required)"
                                )
                                tl_missing_hesp_not_required += 1
                    else:
                        single_row["type_locality_status"] = ""

                    # type_specimen_status
                    hesp_type = get_mdd_style_type_specimen(name)
                    mdd_type = mdd_row["MDD_holotype"]
                    if hesp_type and mdd_type:
                        if hesp_type != mdd_type:
                            diff = f"{hesp_type} (H) / {mdd_type} (M)"
                            single_row["type_specimen_status"] = diff
                            num_type_specimen_diffs += 1
                        else:
                            single_row["type_specimen_status"] = ""
                    elif hesp_type:
                        single_row["type_specimen_status"] = "H only"
                        type_specimen_missing_mdd += 1
                    elif mdd_type:
                        single_row["type_specimen_status"] = "M only"
                        type_specimen_missing_hesp += 1
                    else:
                        single_row["type_specimen_status"] = ""

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
    print(f"{num_validity_diffs} validity differences")
    print(f"{citation_missing_hesp} Hesperomys names missing citation")
    print(f"{citation_missing_mdd} MDD names missing citation")
    print(f"{tl_missing_hesp} Hesperomys names missing type locality")
    print(
        f"{tl_missing_hesp_not_required} Hesperomys names missing type locality"
        " (type locality not required by status)"
    )
    print(f"{tl_missing_mdd} MDD names missing type locality")
    print(f"{num_type_specimen_diffs} type specimen differences")
    print(f"{type_specimen_missing_hesp} Hesperomys names missing type specimen")
    print(f"{type_specimen_missing_mdd} MDD names missing type specimen")
    print(
        f"{num_original_name_diffs} original name differences (normalized name matches)"
    )
    print(
        f"{num_original_name_diffs_normalized} original name differences (normalized"
        " name does not match)"
    )
    print(f"{original_name_missing_hesp} Hesperomys names missing original name")
    print(f"{original_name_missing_mdd} MDD names missing original name")


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
