"""Match up data with NOW.

Needs an export file from NOW containing its locality and species data.

"""

import csv
import functools
from typing import TypedDict

from data_import.lib import DATA_DIR
from taxonomy.db.models import Name, Taxon

INPUT_FILE = DATA_DIR / "now_export_locsp_public_2023-06-19T15#55#32+0000.csv"


class NowRow(TypedDict):
    LIDNUM: str
    NAME: str
    LATSTR: str
    LONGSTR: str
    LAT: str
    LONG: str
    ALTITUDE: str
    MAX_AGE: str
    BFA_MAX: str
    BFA_MAX_ABS: str
    FRAC_MAX: str
    MIN_AGE: str
    BFA_MIN: str
    BFA_MIN_ABS: str
    FRAC_MIN: str
    CHRON: str
    AGE_COMM: str
    BASIN: str
    SUBBASIN: str
    COUNTRY: str
    STATE: str
    COUNTY: str
    APNUMSPM: str
    GENERAL: str
    LOC_SYNONYMS: str
    MEAN_HYPSODONTY: str
    ESTIMATE_PRECIP: str
    ESTIMATE_TEMP: str
    ESTIMATE_NPP: str
    PERS_WOODY_COVER: str
    PERS_POLLEN_AP: str
    PERS_POLLEN_NAP: str
    PERS_POLLEN_OTHER: str
    SIDNUM: str
    ORDER: str
    FAMILY: str
    GENUS: str
    SPECIES: str
    SUBCLASSORSUPERORDER: str
    SUBORDERORSUPERFAMILY: str
    SUBFAMILY: str
    UNIQUE: str
    TAXON_STATUS: str
    ID_STATUS: str
    ADD_INFO: str
    SOURCE_NAME: str
    LS_MICROWEAR: str
    LS_MESOWEAR: str
    LS_MESOWEAR_SCORE: str
    LS_MW_OR_HIGH: str
    LS_MW_OR_LOW: str
    LS_MW_CS_SHARP: str
    LS_MW_CS_ROUND: str
    LS_MW_CS_BLUNT: str
    SVLENGTH: str
    BODYMASS: str
    SXDIMSZE: str
    SXDIMDIS: str
    TSHM: str
    TCRWNHT: str
    HORIZODONTY: str
    CROWNTYP: str
    CUSP_SHAPE: str
    CUSP_COUNT_BUCCAL: str
    CUSP_COUNT_LINGUAL: str
    LOPH_COUNT_LON: str
    LOPH_COUNT_TRS: str
    FCT_AL: str
    FCT_OL: str
    FCT_SF: str
    FCT_OT: str
    FCT_CM: str
    MICROWEAR: str
    MESOWEAR: str
    MESOWEAR_SCORE: str
    MW_OR_HIGH: str
    MW_OR_LOW: str
    MW_CS_SHARP: str
    MW_CS_ROUND: str
    MW_CS_BLUNT: str
    DIET_1: str
    DIET_2: str
    DIET_3: str
    LOCOMO1: str
    LOCOMO2: str
    LOCOMO3: str
    SPCOMMENT: str
    SP_SYNONYM: str
    SP_SYNONYM_COMMENT: str
    HOMININ_BONES: str
    BIPEDAL_FOOTPRINTS: str
    CUTMARKS: str
    STONE_TOOLS: str
    TECHNOLOGICAL_MODE_1: str
    TECHNOLOGICAL_MODE_2: str
    TECHNOLOGICAL_MODE_3: str
    CULTURAL_STAGE_1: str
    CULTURAL_STAGE_2: str
    CULTURAL_STAGE_3: str
    REGIONAL_CULTURE_1: str
    REGIONAL_CULTURE_2: str
    REGIONAL_CULTURE_3: str


def read_file() -> list[NowRow]:
    with INPUT_FILE.open() as f:
        rows: list[NowRow] = list(csv.DictReader(f))  # type: ignore
    return sorted(
        rows,
        key=lambda row: (row["ORDER"], row["FAMILY"], row["GENUS"], row["SPECIES"]),
    )


@functools.cache
def match_up_species(genus_name: str, species_name: str) -> list[Taxon]:
    # First, try to find by valid name
    full_name = f"{genus_name} {species_name}"
    taxa = list(Taxon.select_valid().filter(Taxon.valid_name == full_name))
    if taxa:
        return taxa

    # Try with "
    alternative_full_name = f'"{genus_name}" {species_name}'
    taxa = list(Taxon.select_valid().filter(Taxon.valid_name == alternative_full_name))
    if taxa:
        return taxa

    # Next, try by original name for synonyms
    names = Name.select_valid().filter(Name.corrected_original_name == full_name)
    return list({nam.taxon for nam in names})


@functools.cache
def match_up_and_report(genus_name: str, species_name: str) -> None:
    if species_name in ("indet.", "sp.") or "/" in species_name:
        # TODO find the genus for these
        return
    result = match_up_species(genus_name, species_name)
    if not result:
        print(f"{genus_name} {species_name}: no match found")
    elif len(result) > 1:
        print(f"{genus_name} {species_name}: multiple matches found: {result}")


def run() -> None:
    for line in read_file():
        match_up_and_report(line["GENUS"], line["SPECIES"])


if __name__ == "__main__":
    run()
