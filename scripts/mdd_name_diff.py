import argparse
import csv
import functools
import re
import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import Levenshtein

from scripts import mdd_diff
from taxonomy import getinput
from taxonomy.db import export
from taxonomy.db.constants import AgeClass, Group, Rank, RegionKind, Status
from taxonomy.db.models import Name, Taxon
from taxonomy.db.models.name import TypeTag

COLUMNS = [
    "MDD_syn_ID",
    "Hesp_id",
    "Jelle_TODO",
    "Connor_TODO",
    "Jelle_Comments",
    "Connor_Comments",
    "match_status",
    "spelling_status",
    "author_exact_status",
    "year_status",
    "nomenclature_status",
    "species_status",
    "validity_status",
    "original_name_status",
    "authority_citation_status",
    "authority_page_status",
    "unchecked_authority_citation_status",
    "original_type_locality_status",
    "type_country_status",
    "type_subregion_status",
    "type_specimen_status",
    "species_type_kind_status",
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
    "MDD_unchecked_authority_citation",
    "MDD_authority_link",
    # type locality
    "Hesp_type_locality_region",
    "Hesp_type_locality",
    "Hesp_original_type_locality_verbatim",
    "Hesp_emended_type_locality_verbatim",
    "Hesp_type_latitude",
    "Hesp_type_longitude",
    "MDD_old_type_locality",
    "MDD_original_type_locality",
    "MDD_unchecked_type_locality",
    "MDD_emended_type_locality",
    "MDD_type_latitude",
    "MDD_type_longitude",
    "MDD_type_country",
    "MDD_type_subregion",
    # type specimen
    "Hesp_type_specimen",
    "Hesp_species_type_kind",
    "MDD_holotype",
    "MDD_type_kind",
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
SKIP_MDD_ONLY: Final = True


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
            genus = name.taxon.parent_of_rank(Rank.genus)
        except ValueError:
            return "incertae_sedis incertae_sedis"
        else:
            if genus.base_name.status is Status.valid:
                return f"{genus.valid_name} incertae_sedis"
            else:
                return "incertae_sedis incertae_sedis"
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


def get_original_name_from_mdd(row: dict[str, str]) -> str:
    if original_combination := row["MDD_original_combination"]:
        if "_" not in original_combination:
            return ""  # probably just the root name
        return original_combination.replace("_", " ")
    return ""


def normalize_original_name(name: str) -> str:
    return (
        name.lower()
        .replace(" [sic]", "")
        .replace(".", "")
        .replace("[", "")
        .replace("]", "")
    )


def get_type_locality_region(nam: Name) -> str:
    if nam.type_locality is None:
        return ""
    region = nam.type_locality.region
    regions = [region.name]
    while region is not None and region.kind not in (
        RegionKind.planet,
        RegionKind.continent,
        RegionKind.country,
    ):
        region = region.parent
        regions.append(region.name)
    return ": ".join(reversed(regions))


def get_hesp_row(name: Name, need_initials: set[str]) -> dict[str, Any]:
    data = export.data_for_name(name)
    row = {f"Hesp_{k}": v for k, v in data.items()}
    mdd_style_author = mdd_diff.get_mdd_style_authority(name, need_initials)
    row["Hesp_authority"] = mdd_style_author
    if (
        name.status is Status.synonym
        and name.taxon.base_name.status is not Status.valid
    ):
        row["Hesp_status"] = name.taxon.base_name.status.name
    verbatim_tl = []
    emended_tl = []
    for tag in name.type_tags:
        if isinstance(tag, TypeTag.LocationDetail):
            if tag.source == name.original_citation:
                verbatim_tl.append(tag.text)
            else:
                citation = ", ".join(tag.source.taxonomic_authority())
                emended_tl.append(f'"{tag.text}" ({citation})')
        elif isinstance(tag, TypeTag.Coordinates):
            row["Hesp_type_latitude"] = tag.latitude
            row["Hesp_type_longitude"] = tag.longitude
    if verbatim_tl:
        row["Hesp_original_type_locality_verbatim"] = " | ".join(verbatim_tl)
    if emended_tl:
        row["Hesp_emended_type_locality_verbatim"] = " | ".join(emended_tl)
    row["Hesp_type_locality_region"] = get_type_locality_region(name)
    return row


@dataclass
class ComputedValue:
    value: str


def compare_column(
    single_row: dict[str, str],
    *,
    hesp_column: str | ComputedValue,
    mdd_column: str | ComputedValue,
    target_column: str,
    compare_func: Callable[[str, str], object] | None = Levenshtein.distance,
    counts: dict[str, int],
) -> bool:
    if isinstance(hesp_column, ComputedValue):
        hesp_value = hesp_column.value
    else:
        hesp_value = single_row[hesp_column]
    if isinstance(mdd_column, ComputedValue):
        mdd_value = mdd_column.value
    else:
        mdd_value = single_row[mdd_column]
    match (bool(hesp_value), bool(mdd_value)):
        case (True, False):
            single_row[target_column] = "H only"
            counts[f"{target_column} missing in MDD"] += 1
            return True
        case (False, True):
            single_row[target_column] = "M only"
            counts[f"{target_column} missing in Hesperomys"] += 1
            return True
        case (True, True):
            if hesp_value != mdd_value:
                comparison = f"{hesp_value} (H) / {mdd_value} (M)"
                if compare_func is not None:
                    extra = compare_func(hesp_value, mdd_value)
                    comparison = f"{extra}: {comparison}"
                single_row[target_column] = comparison
                counts[f"{target_column} differences"] += 1
                return True
            else:
                single_row[target_column] = ""
        case (False, False):
            single_row[target_column] = ""
    return False


def compare_year(hesp_year: str, mdd_year: str) -> int:
    try:
        return abs(int(hesp_year) - int(mdd_year))
    except ValueError:
        return 1000


COLUMN_RENAMES = {"MDD original combination": "MDD_original_combination"}
REMOVED_COLUMNS = {"citation_status", "author_status", "type_locality_status"}


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

    for raw_row in match_rows:
        mdd_id = raw_row["MDD_syn_ID"]
        row = {COLUMN_RENAMES.get(key, key): value for key, value in raw_row.items()}
        for key, value in row.items():
            if value and key not in COLUMNS and key not in REMOVED_COLUMNS:
                print(f"warning: unknown column: {key}")
        if mdd_id:
            if mdd_id in mdd_id_to_row:
                mdd_id_to_row[mdd_id].update(row)
            else:
                hesp_id = resolve_hesp_id(row["Hesp_id"])
                if hesp_id is not None:
                    hesp_id_to_mdd_ids.setdefault(hesp_id, set()).add(mdd_id)
                mdd_id_to_row[mdd_id] = row
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
    num_spelling_diffs = 0
    num_nomenclature_diffs = 0
    num_original_name_diffs = 0
    num_original_name_diffs_normalized = 0
    num_count_diffs = 0
    num_number_diffs = 0
    num_text_diffs = 0
    counts: dict[str, int] = Counter()

    def compare_type_specimen_text(hesp_type: str, mdd_type: str) -> str:
        hesp_comma_count = hesp_type.count(",")
        mdd_comma_count = mdd_type.count(",")
        if hesp_comma_count != mdd_comma_count:
            nonlocal num_count_diffs
            num_count_diffs += 1
            return f"count: ({hesp_comma_count + 1}/{mdd_comma_count + 1})"
        else:
            hesp_stripped = re.sub(r"[^0-9]", " ", hesp_type).strip()
            mdd_stripped = re.sub(r"[^0-9]+", " ", mdd_type)
            mdd_stripped = re.sub(r" +", " ", mdd_stripped).strip()
            if hesp_stripped != mdd_stripped:
                nonlocal num_number_diffs
                num_number_diffs += 1
                return f"number: ({hesp_stripped}/{mdd_stripped})"
            else:
                nonlocal num_text_diffs
                num_text_diffs += 1
                return "text"

    def compare_original_name(hesp_orig: str, mdd_orig: str) -> str:
        normalized_matches = normalize_original_name(
            hesp_orig
        ) == normalize_original_name(mdd_orig)
        distance = Levenshtein.distance(hesp_orig, mdd_orig)
        if normalized_matches:
            nonlocal num_original_name_diffs
            num_original_name_diffs += 1
            return f"(normalized name matches) {distance}"
        else:
            nonlocal num_original_name_diffs_normalized
            num_original_name_diffs_normalized += 1
            return str(distance)

    with output_csv.open("w") as f:
        writer = csv.DictWriter(f, COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for name in getinput.print_every_n(hesp_names, label="Hesperomys names"):
            row = get_hesp_row(name, need_initials)
            mdd_style_author = row["Hesp_authority"]
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

                    # author_exact_status
                    compare_column(
                        single_row,
                        hesp_column=ComputedValue(mdd_style_author),
                        mdd_column="MDD_author",
                        target_column="author_exact_status",
                        counts=counts,
                    )

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
                    compare_column(
                        single_row,
                        hesp_column="Hesp_year",
                        mdd_column="MDD_year",
                        target_column="year_status",
                        compare_func=compare_year,
                        counts=counts,
                    )

                    # validity_status
                    expected_mdd_status = get_mdd_status(name)
                    compare_column(
                        single_row,
                        hesp_column=ComputedValue(expected_mdd_status),
                        mdd_column="MDD_validity",
                        target_column="validity_status",
                        counts=counts,
                        compare_func=None,
                    )

                    # original_name_status
                    mdd_orig = get_original_name_from_mdd(mdd_row)
                    compare_column(
                        single_row,
                        hesp_column="Hesp_original_name",
                        mdd_column=ComputedValue(mdd_orig),
                        target_column="original_name_status",
                        compare_func=compare_original_name,
                        counts=counts,
                    )

                    # citation_status
                    compare_column(
                        single_row,
                        hesp_column="Hesp_original_citation",
                        mdd_column="MDD_authority_citation",
                        target_column="authority_citation_status",
                        counts=counts,
                    )
                    compare_column(
                        single_row,
                        hesp_column="Hesp_page_described",
                        mdd_column="MDD_authority_page",
                        target_column="authority_page_status",
                        counts=counts,
                    )
                    compare_column(
                        single_row,
                        hesp_column="Hesp_verbatim_citation",
                        mdd_column="MDD_unchecked_authority_citation",
                        target_column="unchecked_authority_citation_status",
                        counts=counts,
                    )

                    # type_locality_status
                    compare_column(
                        single_row,
                        hesp_column="Hesp_original_type_locality_verbatim",
                        mdd_column="MDD_original_type_locality",
                        target_column="original_type_locality_status",
                        counts=counts,
                    )
                    region_parts = single_row["Hesp_type_locality_region"].split(":")
                    compare_column(
                        single_row,
                        hesp_column=ComputedValue(
                            region_parts[0].strip() if region_parts else ""
                        ),
                        mdd_column="MDD_type_country",
                        target_column="type_country_status",
                        counts=counts,
                    )
                    compare_column(
                        single_row,
                        hesp_column=ComputedValue(
                            region_parts[1].strip() if len(region_parts) > 1 else ""
                        ),
                        mdd_column="MDD_type_subregion",
                        target_column="type_subregion_status",
                        counts=counts,
                    )

                    # species_type_kind_status
                    compare_column(
                        single_row,
                        hesp_column="Hesp_species_type_kind",
                        mdd_column="MDD_type_kind",
                        target_column="species_type_kind_status",
                        counts=counts,
                    )

                    # type_specimen_status
                    compare_column(
                        single_row,
                        hesp_column="Hesp_type_specimen",
                        mdd_column="MDD_holotype",
                        target_column="type_specimen_status",
                        compare_func=compare_type_specimen_text,
                        counts=counts,
                    )

                    writer.writerow(single_row)
            else:
                num_hesp_only += 1
                row["match_status"] = "no_mdd_match"
                writer.writerow(row)
        if not SKIP_MDD_ONLY:
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
    for key, count in sorted(counts.items()):
        print(f"{count} {key}")
    print(f"{num_species_diffs} species differences")
    print(f"{num_nomenclature_diffs} nomenclature status differences")
    print(f"{num_spelling_diffs} spelling differences")
    print(
        f"type specimen differences: {num_count_diffs} counts, {num_number_diffs} specimen numbers, {num_text_diffs} text"
    )
    print(
        f"{num_original_name_diffs} original name differences (normalized name matches)"
    )
    print(
        f"{num_original_name_diffs_normalized} original name differences (normalized"
        " name does not match)"
    )


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
